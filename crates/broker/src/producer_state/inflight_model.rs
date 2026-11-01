//! Exhaustive stateright model of the idempotent-producer window between a
//! produce's dedup check and its tracker commit.
//!
//! `producer_state_model` checks the decision kernel over one serialized
//! submit sequence: each submit is checked and committed in one step. The
//! broker does not run a produce that way. Three steps of one `acks=all`
//! produce are separate, and requests from one producer can interleave between
//! them:
//!
//! 1. `handle_duplicate` (`handlers/produce/producer_checks.rs`) classifies the
//!    batch with [`ProducerState::check_batch`](super::ProducerState), which
//!    is [`check_retained`] over the tracked entry, under the tracker's lock,
//!    and releases the lock.
//! 2. The partition writer appends the batch. Neither the writer nor the log
//!    checks the sequence again.
//! 3. `PendingAck::finish` (`handlers/produce/append.rs`) waits for the
//!    high-watermark gate, then `AppendCommit::record` commits the append into
//!    the tracker through [`ProducerState::commit`](super::ProducerState), on a
//!    replicated gate and on a timeout alike, and only then answers.
//!
//! The produce holds only a shared read guard of the partition's transition
//! barrier (`ProduceTransition`) through all three, so two requests of one
//! producer can be in flight together. A client whose request outlives its
//! `request.timeout.ms`, or whose connection drops, sends the batch again with
//! the same producer id, epoch and sequence over a new connection while the
//! first copy still waits at the gate.
//!
//! Kafka answers the same retry as a duplicate: `UnifiedLog.append` checks the
//! batch and updates the producer state under the log lock at append time, so
//! a retry sees every batch already in the log.
//!
//! # State
//!
//! - The partition log, one single-record batch per slot, each slot's offset
//!   its index. A slot carries the batch's producer id, epoch and sequence.
//! - The host tracker's [`ProducerEntry`] for each producer.
//! - A ghost of Kafka's producer state for each producer: the last sequence
//!   and the retained batches, with offsets, at most five, written the plain
//!   way and updated when the log appends.
//! - The requests the broker admitted for append and has not answered, each
//!   admitted or appended at an offset.
//! - Each client's next sequence, the sends it made of that batch, and
//!   whether the broker refused it.
//!
//! # Actions
//!
//! - `Send` and `Retry`: the client sends its next batch, or sends its
//!   outstanding batch again, and the broker admits it: [`check_retained`]
//!   over the tracked entry. A retry is enabled whenever a send of the batch
//!   is outstanding and the client has attempts left, which covers both a
//!   client-side timeout and a `REQUEST_TIMED_OUT` answer. A duplicate answers
//!   at once and appends nothing.
//! - `Append`: the writer appends an admitted batch at the log's end, and the
//!   ghost takes it as Kafka's log would.
//! - `Complete`: the gate resolves, replicated or timed out, the broker
//!   commits the append through [`committed_entry`], which is the whole of
//!   [`ProducerState::commit`](super::ProducerState) apart from its lock, and
//!   then answers. Nothing that another request reads changes between the
//!   gate and the commit, so the two are one step. A replicated gate
//!   acknowledges the batch, and the client moves to its next one.
//!
//! Two producers with distinct ids share the partition. The first sends two
//! batches and the second one. Every batch is sent at most twice.
//!
//! # Properties
//!
//! - `no_duplicate_append` (always): no producer id, epoch and sequence is in
//!   the log twice.
//! - `decision_matches_kafka` (always): the host classifies every admitted
//!   batch as Kafka would over the batches already in the log, at admission
//!   and again when the writer appends it. A duplicate must name the offset
//!   Kafka names.
//! - Witnesses (sometimes): a retry admitted while an earlier send of its
//!   batch waits at the gate; a retry answered as a duplicate; a retry
//!   answered as a duplicate after the other producer's batch was appended
//!   behind the original; and every batch acknowledged.
//!
//! # Known failures
//!
//! Both `always` properties fail on the current code. This is Jepsen's
//! "Duplicate records" known failure (krabka-io/krabka-broker#1932): under
//! `acks=all`, a paused or partitioned follower holds the gate, the client
//! retries, and the leader appends the retry again rather than answering it
//! with the original's offset. The tracker learns of an append only at its
//! commit, so a retry admitted between the original's append and its commit
//! finds no entry, or an older one, and appends a second copy.
//!
//! - `decision_matches_kafka` at depth 3: `Send { producer: 0 }`, `Append {
//!   producer: 0, sequence: 0 }`, `Retry { producer: 0 }`. The host admits the
//!   retry for append, and Kafka answers it as a duplicate of offset 0.
//! - `no_duplicate_append` at depth 4: `Send { producer: 0 }`, `Retry {
//!   producer: 0 }`, `Append { producer: 0, sequence: 0 }`, `Append {
//!   producer: 0, sequence: 0 }`. Both sends pass the check before either
//!   appends, so even a tracker updated at append time would not stop this
//!   one: the check and the append must be one step, as in Kafka.
//!
//! The fix is being made separately. When it merges, it deletes the
//! [`KnownFailure`] entries, re-pins the unique-state count, and the two
//! properties are then checked as every other property is.

use {
    super::{
        Decision, ProducerEntry, committed_entry,
        decision::{Checked, SequenceContext, check_retained},
        entry::NUM_BATCHES_TO_RETAIN,
    },
    crate::{
        api_catalog::UnstableApiVersions,
        model_check::{KnownFailure, check_model_with_known_failures},
        partition::LogOffset,
    },
    stateright::{Model, Property},
    std::collections::VecDeque,
};

const MAX_DEPTH: usize = 40;
const MAX_STATES: usize = 2_000_000;

// The exact unique-state count of the exhaustive single-threaded BFS. A changed
// count is a changed model: a dropped action, a transition that starts
// returning `None`, or a field that the derived `Hash` stops reading. The fix
// for the known failures changes the reachable set, so it re-pins this count.
const PINNED_UNIQUE_STATES: usize = 42_912;

/// The producer ids, one per producer index.
const PRODUCER_IDS: [i64; 2] = [11, 22];
/// The one epoch every batch carries.
const EPOCH: i16 = 0;

struct InflightModel {
    /// The batches each producer sends, sequences `0..batches`.
    batches: [i32; 2],
    /// The sends of one batch a client makes, its first send included.
    max_attempts: u8,
}

/// One single-record batch in the log. Its offset is its slot's index.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Slot {
    producer_id: i64,
    epoch: i16,
    sequence: i32,
}

/// The ghost: Kafka's producer state for one producer, which
/// `UnifiedLog.append` updates as it appends.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Window {
    last_sequence: i32,
    /// The sequence and offset of each retained batch, oldest first, at most
    /// five.
    retained: VecDeque<(i32, LogOffset)>,
}

impl Window {
    /// The window after Kafka appends `sequence` at `offset`.
    fn after_append(window: Option<&Self>, sequence: i32, offset: LogOffset) -> Self {
        let mut window = window.cloned().unwrap_or(Self {
            last_sequence: -1,
            retained: VecDeque::new(),
        });
        window.last_sequence = sequence;
        window.retained.push_back((sequence, offset));
        if window.retained.len() > NUM_BATCHES_TO_RETAIN {
            window.retained.pop_front();
        }
        window
    }
}

/// How a batch is answered.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Answer {
    Append,
    /// A duplicate of the retained batch with this sequence, at this offset.
    Duplicate {
        sequence: i32,
        offset: LogOffset,
    },
    OutOfOrder,
    Fenced,
}

impl Answer {
    fn of(checked: Checked) -> Option<Self> {
        Some(match checked.decision {
            Decision::Append => Self::Append,
            Decision::Duplicate { base_offset } => {
                let batch = checked.duplicate?;
                (batch.base_offset == base_offset).then_some(Self::Duplicate {
                    sequence: batch.base_sequence,
                    offset: base_offset,
                })?
            }
            Decision::OutOfOrder => Self::OutOfOrder,
            Decision::Fenced => Self::Fenced,
        })
    }
}

/// Kafka's answer to a single-record batch at [`EPOCH`] over the producer's
/// state: `ProducerStateEntry.findDuplicateBatch` among the retained batches
/// first, then `ProducerAppendInfo.checkSequence`. Every batch carries one
/// epoch, so no batch is fenced, and a producer with no state starts anywhere,
/// as Kafka 4.3.1 lets it.
fn kafka_decision(window: Option<&Window>, sequence: i32) -> Answer {
    let Some(window) = window else {
        return Answer::Append;
    };
    if let Some(&(_, offset)) = window
        .retained
        .iter()
        .find(|(retained, _)| *retained == sequence)
    {
        return Answer::Duplicate { sequence, offset };
    }
    if sequence == window.last_sequence + 1 {
        Answer::Append
    } else {
        Answer::OutOfOrder
    }
}

/// A request the broker admitted for append and has not answered yet.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct Request {
    producer: usize,
    sequence: i32,
    /// The offset the writer appended the batch at, or `None` while it waits
    /// for the writer.
    appended: Option<LogOffset>,
}

/// One producer client.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Client {
    /// The sequence of the batch the client is producing.
    next: i32,
    /// The sends of that batch the client made. Zero until its first send.
    attempts: u8,
    /// The broker refused a batch, out of order or fenced, and the client
    /// stopped.
    refused: bool,
}

impl Client {
    const START: Self = Self {
        next: 0,
        attempts: 0,
        refused: false,
    };

    fn acknowledge(&mut self, sequence: i32) {
        if self.attempts > 0 && self.next == sequence {
            self.next += 1;
            self.attempts = 0;
        }
    }
}

/// Where the host's answer was compared with Kafka's.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Stage {
    Admit,
    Append,
}

/// A batch the host and Kafka answered differently.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Violation {
    stage: Stage,
    producer: usize,
    sequence: i32,
    host: Answer,
    kafka: Answer,
}

/// A retry outcome a `sometimes` witness looks for. It is recorded only on
/// the transition that produced it and cleared by the next. One transition can
/// produce several, so the state holds them as a set.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Witness {
    /// A retry was admitted while an earlier send of its batch was appended
    /// and waiting at the gate: the window the Jepsen failure needs.
    AdmittedDuringPendingCommit,
    /// A retry answered as a duplicate.
    Deduplicated,
    /// A retry answered as a duplicate of a batch that the other producer's
    /// batch was appended behind.
    DeduplicatedAfterOtherProducer,
}

/// The witnesses of one transition.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct Witnesses(u8);

impl Witnesses {
    fn mark(&mut self, witness: Witness) { self.0 |= 1 << witness as u8; }

    fn seen(self, witness: Witness) -> bool { self.0 & (1 << witness as u8) != 0 }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct InflightState {
    log: Vec<Slot>,
    tracked: [Option<ProducerEntry>; 2],
    kafka: [Option<Window>; 2],
    /// Sorted, so two states that differ only in the order requests were
    /// admitted are one state.
    inflight: Vec<Request>,
    clients: [Client; 2],
    witnesses: Witnesses,
    /// The first batch the host and Kafka answered differently.
    violation: Option<Violation>,
}

/// Whether the gate a [`Action::Complete`] resolves saw the batch replicated.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Gate {
    Replicated,
    TimedOut,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Action {
    /// The client sends its next batch, and the broker admits it.
    Send { producer: usize },
    /// The client sends its outstanding batch again, and the broker admits
    /// it.
    Retry { producer: usize },
    /// The writer appends an admitted batch.
    Append { producer: usize, sequence: i32 },
    /// The gate of an appended batch resolves, the broker commits the append
    /// into the tracker and answers.
    Complete {
        producer: usize,
        sequence: i32,
        offset: LogOffset,
        gate: Gate,
    },
}

impl InflightState {
    /// The broker admits the client's current batch of `producer`.
    fn admit(&mut self, producer: usize, retry: bool) {
        let sequence = self.clients[producer].next;
        let context = SequenceContext {
            log_empty: self.log.is_empty(),
            unstable: UnstableApiVersions::Disabled,
        };
        let host = Answer::of(check_retained(
            self.tracked[producer].as_ref(),
            context,
            EPOCH,
            sequence,
            0,
        ))
        .expect("a duplicate names the retained batch at its offset");
        let kafka = kafka_decision(self.kafka[producer].as_ref(), sequence);
        self.compare(Stage::Admit, producer, sequence, host, kafka);
        self.clients[producer].attempts += 1;
        if retry
            && self.inflight.iter().any(|request| {
                request.producer == producer
                    && request.sequence == sequence
                    && request.appended.is_some()
            })
        {
            self.witnesses.mark(Witness::AdmittedDuringPendingCommit);
        }
        match host {
            Answer::Append => {
                self.inflight.push(Request {
                    producer,
                    sequence,
                    appended: None,
                });
                self.inflight.sort_unstable();
            }
            Answer::Duplicate { offset, .. } => {
                if retry {
                    // An offset is its slot's index.
                    let after = usize::try_from(offset).map_or(&[][..], |index| {
                        self.log.get(index + 1..).unwrap_or_default()
                    });
                    self.witnesses.mark(Witness::Deduplicated);
                    if after
                        .iter()
                        .any(|slot| slot.producer_id != PRODUCER_IDS[producer])
                    {
                        self.witnesses.mark(Witness::DeduplicatedAfterOtherProducer);
                    }
                }
                self.clients[producer].acknowledge(sequence);
            }
            Answer::OutOfOrder | Answer::Fenced => self.clients[producer].refused = true,
        }
    }

    fn compare(
        &mut self,
        stage: Stage,
        producer: usize,
        sequence: i32,
        host: Answer,
        kafka: Answer,
    ) {
        if host != kafka {
            self.violation.get_or_insert(Violation {
                stage,
                producer,
                sequence,
                host,
                kafka,
            });
        }
    }

    /// The writer appends the admitted batch `sequence` of `producer`.
    fn append(&mut self, producer: usize, sequence: i32) -> Option<()> {
        let request = self.inflight.iter_mut().find(|request| {
            request.producer == producer
                && request.sequence == sequence
                && request.appended.is_none()
        })?;
        let offset = LogOffset::try_from(self.log.len()).ok()?;
        request.appended = Some(offset);
        self.inflight.sort_unstable();
        let kafka = kafka_decision(self.kafka[producer].as_ref(), sequence);
        self.compare(Stage::Append, producer, sequence, Answer::Append, kafka);
        if kafka == Answer::Append {
            self.kafka[producer] = Some(Window::after_append(
                self.kafka[producer].as_ref(),
                sequence,
                offset,
            ));
        }
        self.log.push(Slot {
            producer_id: PRODUCER_IDS[producer],
            epoch: EPOCH,
            sequence,
        });
        Some(())
    }

    /// The gate of the batch appended at `offset` resolves, and the broker
    /// commits it and answers.
    fn complete(
        &mut self,
        producer: usize,
        sequence: i32,
        offset: LogOffset,
        gate: Gate,
    ) -> Option<()> {
        let request = Request {
            producer,
            sequence,
            appended: Some(offset),
        };
        let index = self
            .inflight
            .iter()
            .position(|pending| *pending == request)?;
        self.inflight.remove(index);
        if let Some(entry) = committed_entry(
            self.tracked[producer],
            (PRODUCER_IDS[producer], EPOCH),
            (sequence, 0),
            (offset, 0, false),
        ) {
            self.tracked[producer] = Some(entry);
        }
        if gate == Gate::Replicated {
            self.clients[producer].acknowledge(sequence);
        }
        Some(())
    }

    fn duplicate_in_log(&self) -> bool {
        self.log
            .iter()
            .enumerate()
            .any(|(index, slot)| self.log[..index].contains(slot))
    }
}

impl Model for InflightModel {
    type Action = Action;
    type State = InflightState;

    fn init_states(&self) -> Vec<InflightState> {
        vec![InflightState {
            log: Vec::new(),
            tracked: [None; 2],
            kafka: [None, None],
            inflight: Vec::new(),
            clients: [Client::START; 2],
            witnesses: Witnesses::default(),
            violation: None,
        }]
    }

    fn actions(&self, s: &InflightState, actions: &mut Vec<Action>) {
        for (producer, client) in s.clients.iter().enumerate() {
            if client.refused {
                continue;
            }
            if client.attempts == 0 && client.next < self.batches[producer] {
                actions.push(Action::Send { producer });
            }
            if client.attempts > 0 && client.attempts < self.max_attempts {
                actions.push(Action::Retry { producer });
            }
        }
        for request in &s.inflight {
            let Request {
                producer,
                sequence,
                appended,
            } = *request;
            match appended {
                None => actions.push(Action::Append { producer, sequence }),
                Some(offset) => {
                    for gate in [Gate::Replicated, Gate::TimedOut] {
                        actions.push(Action::Complete {
                            producer,
                            sequence,
                            offset,
                            gate,
                        });
                    }
                }
            }
        }
        actions.dedup();
    }

    fn next_state(&self, last: &InflightState, action: Action) -> Option<InflightState> {
        let mut s = InflightState {
            witnesses: Witnesses::default(),
            ..last.clone()
        };
        match action {
            Action::Send { producer } => s.admit(producer, false),
            Action::Retry { producer } => s.admit(producer, true),
            Action::Append { producer, sequence } => s.append(producer, sequence)?,
            Action::Complete {
                producer,
                sequence,
                offset,
                gate,
            } => s.complete(producer, sequence, offset, gate)?,
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_duplicate_append", |_, s: &InflightState| {
                !s.duplicate_in_log()
            }),
            Property::always("decision_matches_kafka", |_, s: &InflightState| {
                s.violation.is_none()
            }),
            Property::sometimes("retry_during_pending_commit", |_, s: &InflightState| {
                s.witnesses.seen(Witness::AdmittedDuringPendingCommit)
            }),
            Property::sometimes("retry_deduplicated", |_, s: &InflightState| {
                s.witnesses.seen(Witness::Deduplicated)
            }),
            Property::sometimes(
                "retry_deduplicated_after_other_producer",
                |_, s: &InflightState| s.witnesses.seen(Witness::DeduplicatedAfterOtherProducer),
            ),
            Property::sometimes(
                "every_batch_acknowledged",
                |model: &Self, s: &InflightState| {
                    s.clients
                        .iter()
                        .zip(model.batches)
                        .all(|(client, batches)| client.next == batches)
                },
            ),
        ]
    }
}

#[test]
fn a_retry_during_a_pending_commit_appends_again() {
    check_model_with_known_failures(
        InflightModel {
            batches: [2, 1],
            max_attempts: 2,
        },
        "producer_inflight",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES,
        &[
            KnownFailure {
                property: "decision_matches_kafka",
                depth: 3,
            },
            KnownFailure {
                property: "no_duplicate_append",
                depth: 4,
            },
        ],
    );
}
