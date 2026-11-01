//! Exhaustive stateright model of the idempotent-producer window between a
//! produce's dedup check and its tracker commit.
//!
//! `producer_state_model` checks the decision kernel over one serialized
//! submit sequence: each submit is checked and committed in one step. The
//! broker does not run a produce that way. Four steps of one `acks=all`
//! produce are separate, and requests from one producer can interleave between
//! them:
//!
//! 1. `handle_duplicate` (`handlers/produce/producer_checks.rs`) classifies the
//!    batch with [`ProducerState::check_batch`](super::ProducerState), which
//!    is [`check_retained`] over the tracked entry, under the tracker's lock,
//!    and releases the lock. This early gate answers a duplicate, an
//!    out-of-order batch or a fenced one at once, and passes the rest on.
//! 2. The partition writer, under the log's append lock, looks the batch up
//!    among its producer's retained batches (`duplicate_of` in
//!    `partition_writer/append.rs`, which is
//!    [`Log::find_duplicate_batch`](krabka_log::Log::find_duplicate_batch)).
//!    A repeat is answered with the retained batch's offset and is not
//!    appended. Anything else is appended, and the log records it in its own
//!    producer state as it appends.
//! 3. `finalize_ack` (`handlers/produce/append.rs`) commits the writer's
//!    answer into the tracker through `AppendCommit::record` as soon as the
//!    writer answers, before any `acks=all` wait.
//! 4. `PendingAck::finish` waits for the high-watermark gate and answers:
//!    acknowledged when the batch replicated, `REQUEST_TIMED_OUT` otherwise.
//!
//! The produce holds only a shared read guard of the partition's transition
//! barrier (`ProduceTransition`) through all four, so two requests of one
//! producer can be in flight together. A client whose request outlives its
//! `request.timeout.ms`, or whose connection drops, sends the batch again with
//! the same producer id, epoch and sequence over a new connection while the
//! first copy still waits.
//!
//! Kafka answers the same retry as a duplicate: `UnifiedLog.append` checks the
//! batch and updates the producer state under the log lock at append time, so
//! a retry sees every batch already in the log. Step 2 is that check.
//!
//! # State
//!
//! - The partition log, one single-record batch per slot, each slot's offset
//!   its index. A slot carries the batch's producer id, epoch and sequence.
//!   The log's own producer state is not stored: [`LoggedProducer`] replays
//!   the producer's slots through the log's update,
//!   [`ProducerSnapshotEntry::add_data_batch`](krabka_log::ProducerSnapshotEntry::add_data_batch),
//!   whenever the writer looks a batch up.
//! - The host tracker's [`ProducerEntry`] for each producer.
//! - A ghost of Kafka's producer state for each producer: the last sequence
//!   and the retained batches, with offsets, at most five, written the plain
//!   way and updated when Kafka would append.
//! - The requests the broker admitted and has not answered: each waits for
//!   the writer, or holds the writer's answer and whether the tracker has
//!   committed it.
//! - Each client's next sequence, the sends it made of that batch, and
//!   whether the broker refused it.
//!
//! # Actions
//!
//! - `Send` and `Retry`: the client sends its next batch, or sends its
//!   outstanding batch again, and the early gate classifies it with
//!   [`check_retained`]. A retry is enabled whenever a send of the batch is
//!   outstanding and the client has attempts left, which covers both a
//!   client-side timeout and a `REQUEST_TIMED_OUT` answer. A duplicate answers
//!   at once and appends nothing.
//! - `Append`: the writer takes an admitted batch. It answers a repeat of a
//!   retained batch with that batch's offset through
//!   [`LoggedProducer::duplicate_of`], which runs
//!   [`ProducerSnapshotEntry::find_duplicate_batch`](krabka_log::ProducerSnapshotEntry::find_duplicate_batch),
//!   and appends anything else at the log's end. The ghost takes the batch
//!   if Kafka would append it.
//! - `Commit`: the request commits the writer's answer into the tracker
//!   through [`committed_entry`], which is the whole of
//!   [`ProducerState::commit`](super::ProducerState) apart from its lock.
//! - `Complete`: the gate resolves, replicated or timed out, and the broker
//!   answers. A replicated gate acknowledges the batch, and the client moves
//!   to its next one.
//!
//! Two producers with distinct ids share the partition. The first sends two
//! batches and the second one. Every batch is sent at most twice.
//!
//! # Properties
//!
//! - `no_duplicate_append` (always): no producer id, epoch and sequence is in
//!   the log twice.
//! - `decision_matches_kafka` (always): the host answers every batch as Kafka
//!   would over the batches already in the log. An answer the early gate
//!   gives is compared when it gives it. A batch the gate passes is compared
//!   when the writer answers it. A duplicate must name the offset Kafka names.
//! - Witnesses (sometimes): a retry admitted while an earlier copy of its
//!   batch has a writer's answer that the tracker has not committed; a retry
//!   the early gate answered as a duplicate, once with the other producer's
//!   batch appended behind the original; a copy the gate passed that the
//!   writer answered as a duplicate, once with the other producer's batch
//!   appended behind the original, which is the shape of Jepsen's `t9-1`;
//!   and every batch acknowledged.
//!
//! # RED regression
//!
//! [`Orchestration::GateOnly`] is the orchestration before #1947: the writer
//! looks nothing up, and the tracker commits only after the `acks=all` gate
//! resolves, so `Complete` commits and there is no `Commit`. It reproduces
//! Jepsen's "Duplicate records" failure (krabka-io/krabka-broker#1932), and
//! its test pins both counterexamples, so a change that brings the old
//! orchestration back, or a model that stops reaching the failure, fails it:
//!
//! - `no_duplicate_append` at depth 4: `Send { producer: 0 }`, `Retry {
//!   producer: 0 }`, `Append { producer: 0, sequence: 0 }`, `Append {
//!   producer: 0, sequence: 0 }`. Both sends pass the gate before either
//!   appends, and the writer appends both.
//! - `decision_matches_kafka` at depth 4, on the same path: the writer
//!   appends the second copy, and Kafka answers it as a duplicate of
//!   offset 0.

use {
    super::{
        Decision, LoggedProducer, ProducerEntry, committed_entry,
        decision::{Checked, SequenceContext, check_retained},
        entry::NUM_BATCHES_TO_RETAIN,
    },
    crate::{
        api_catalog::UnstableApiVersions,
        model_check::{KnownFailure, check_model, check_model_with_known_failures},
        partition::LogOffset,
    },
    stateright::{Model, Property},
    std::collections::VecDeque,
};

const MAX_DEPTH: usize = 40;
const MAX_STATES: usize = 2_000_000;

// The exact unique-state counts of the exhaustive single-threaded BFS, one per
// orchestration. A changed count is a changed model: a dropped action, a
// transition that starts returning `None`, or a field that the derived `Hash`
// stops reading.
const PINNED_UNIQUE_STATES: usize = 8_175;
const PINNED_UNIQUE_STATES_GATE_ONLY: usize = 15_315;

/// The producer ids, one per producer index.
const PRODUCER_IDS: [i64; 2] = [11, 22];
/// The one epoch every batch carries.
const EPOCH: i16 = 0;

/// How the broker orders the steps of one produce.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Orchestration {
    /// The code since #1947: the writer looks every client batch up among
    /// its producer's retained batches, and the tracker commits the writer's
    /// answer before the `acks=all` wait.
    WriterLookup,
    /// The code before #1947: only the early gate checks, and the tracker
    /// commits after the `acks=all` gate resolves. The RED regression.
    GateOnly,
}

struct InflightModel {
    /// The batches each producer sends, sequences `0..batches`.
    batches: [i32; 2],
    /// The sends of one batch a client makes, its first send included.
    max_attempts: u8,
    orchestration: Orchestration,
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

/// A request the early gate passed and the broker has not answered yet.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct Request {
    producer: usize,
    sequence: i32,
    /// The offset the writer answered the batch with, where it appended it
    /// or where the retained batch it repeats starts, or `None` while it
    /// waits for the writer.
    answered: Option<LogOffset>,
    /// Whether the tracker committed the writer's answer. Under
    /// [`Orchestration::GateOnly`] it commits as the request completes, so
    /// this stays `false`.
    committed: bool,
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
    /// The early gate answered the batch.
    Admit,
    /// The writer answered a batch the gate passed.
    Write,
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
    /// A retry was admitted while an earlier copy of its batch had a writer's
    /// answer that the tracker had not committed: the window the Jepsen
    /// failure needs.
    AdmittedDuringPendingCommit,
    /// The early gate answered a retry as a duplicate.
    GateDeduplicated,
    /// The early gate answered a retry as a duplicate of a batch that the
    /// other producer's batch was appended behind.
    GateDeduplicatedAfterOtherProducer,
    /// The writer answered a copy the gate passed as a duplicate.
    WriterDeduplicated,
    /// The writer answered a copy the gate passed as a duplicate of a batch
    /// that the other producer's batch was appended behind.
    WriterDeduplicatedAfterOtherProducer,
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
    /// The client sends its next batch, and the early gate classifies it.
    Send { producer: usize },
    /// The client sends its outstanding batch again, and the early gate
    /// classifies it.
    Retry { producer: usize },
    /// The writer takes a batch the gate passed: it appends the batch, or
    /// answers it as a duplicate of a retained batch.
    Append { producer: usize, sequence: i32 },
    /// The request commits the writer's answer into the tracker. Under
    /// [`Orchestration::GateOnly`], [`Action::Complete`] commits instead.
    Commit {
        producer: usize,
        sequence: i32,
        offset: LogOffset,
    },
    /// The gate of an answered batch resolves, and the broker answers.
    Complete {
        producer: usize,
        sequence: i32,
        offset: LogOffset,
        gate: Gate,
    },
}

impl InflightState {
    /// The early gate classifies the client's current batch of `producer`.
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
        self.clients[producer].attempts += 1;
        if retry
            && self.inflight.iter().any(|request| {
                request.producer == producer
                    && request.sequence == sequence
                    && request.answered.is_some()
                    && !request.committed
            })
        {
            self.witnesses.mark(Witness::AdmittedDuringPendingCommit);
        }
        if host == Answer::Append {
            // The writer gives this batch its answer.
            self.inflight.push(Request {
                producer,
                sequence,
                answered: None,
                committed: false,
            });
            self.inflight.sort_unstable();
            return;
        }
        let kafka = kafka_decision(self.kafka[producer].as_ref(), sequence);
        self.compare(Stage::Admit, producer, sequence, host, kafka);
        if let Answer::Duplicate { offset, .. } = host {
            if retry {
                self.witnesses.mark(Witness::GateDeduplicated);
                if self.appended_behind(producer, offset) {
                    self.witnesses
                        .mark(Witness::GateDeduplicatedAfterOtherProducer);
                }
            }
            self.clients[producer].acknowledge(sequence);
        } else {
            self.clients[producer].refused = true;
        }
    }

    /// Whether the other producer appended a batch behind `offset`.
    fn appended_behind(&self, producer: usize, offset: LogOffset) -> bool {
        // An offset is its slot's index.
        usize::try_from(offset)
            .map_or(&[][..], |index| {
                self.log.get(index + 1..).unwrap_or_default()
            })
            .iter()
            .any(|slot| slot.producer_id != PRODUCER_IDS[producer])
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

    /// The producer state the log holds for `producer`.
    fn logged(&self, producer: usize) -> LoggedProducer {
        LoggedProducer::replay(
            PRODUCER_IDS[producer],
            EPOCH,
            // An offset is its slot's index.
            self.log
                .iter()
                .zip(0..)
                .filter(|(slot, _)| slot.producer_id == PRODUCER_IDS[producer])
                .map(|(slot, offset)| (slot.sequence, offset)),
        )
    }

    /// The writer takes the batch `sequence` of `producer` that the gate
    /// passed.
    fn write(
        &mut self,
        producer: usize,
        sequence: i32,
        orchestration: Orchestration,
    ) -> Option<()> {
        let log_end = LogOffset::try_from(self.log.len()).ok()?;
        let duplicate = match orchestration {
            Orchestration::WriterLookup => self.logged(producer).duplicate_of(EPOCH, sequence),
            Orchestration::GateOnly => None,
        };
        let request = self.inflight.iter_mut().find(|request| {
            request.producer == producer
                && request.sequence == sequence
                && request.answered.is_none()
        })?;
        request.answered = Some(duplicate.unwrap_or(log_end));
        self.inflight.sort_unstable();
        let host = duplicate.map_or(Answer::Append, |offset| Answer::Duplicate {
            sequence,
            offset,
        });
        let kafka = kafka_decision(self.kafka[producer].as_ref(), sequence);
        self.compare(Stage::Write, producer, sequence, host, kafka);
        if kafka == Answer::Append {
            self.kafka[producer] = Some(Window::after_append(
                self.kafka[producer].as_ref(),
                sequence,
                log_end,
            ));
        }
        if let Some(offset) = duplicate {
            self.witnesses.mark(Witness::WriterDeduplicated);
            if self.appended_behind(producer, offset) {
                self.witnesses
                    .mark(Witness::WriterDeduplicatedAfterOtherProducer);
            }
        } else {
            self.log.push(Slot {
                producer_id: PRODUCER_IDS[producer],
                epoch: EPOCH,
                sequence,
            });
        }
        Some(())
    }

    /// The tracker takes the batch `sequence` of `producer` at `offset`.
    fn commit_tracker(&mut self, producer: usize, sequence: i32, offset: LogOffset) {
        if let Some(entry) = committed_entry(
            self.tracked[producer],
            (PRODUCER_IDS[producer], EPOCH),
            (sequence, 0),
            (offset, 0, false),
        ) {
            self.tracked[producer] = Some(entry);
        }
    }

    /// The request for `sequence` of `producer` that the writer answered with
    /// `offset` commits that answer.
    fn commit(&mut self, producer: usize, sequence: i32, offset: LogOffset) -> Option<()> {
        let request = self.inflight.iter_mut().find(|request| {
            **request
                == Request {
                    producer,
                    sequence,
                    answered: Some(offset),
                    committed: false,
                }
        })?;
        request.committed = true;
        self.inflight.sort_unstable();
        self.commit_tracker(producer, sequence, offset);
        Some(())
    }

    /// The gate of the batch answered with `offset` resolves, and the broker
    /// answers.
    fn complete(
        &mut self,
        producer: usize,
        sequence: i32,
        (offset, gate): (LogOffset, Gate),
        orchestration: Orchestration,
    ) -> Option<()> {
        let request = Request {
            producer,
            sequence,
            answered: Some(offset),
            committed: orchestration == Orchestration::WriterLookup,
        };
        let index = self
            .inflight
            .iter()
            .position(|pending| *pending == request)?;
        self.inflight.remove(index);
        if orchestration == Orchestration::GateOnly {
            self.commit_tracker(producer, sequence, offset);
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
                answered,
                committed,
            } = *request;
            match answered {
                None => actions.push(Action::Append { producer, sequence }),
                Some(offset) if self.orchestration == Orchestration::WriterLookup && !committed => {
                    actions.push(Action::Commit {
                        producer,
                        sequence,
                        offset,
                    });
                }
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
            Action::Append { producer, sequence } => {
                s.write(producer, sequence, self.orchestration)?;
            }
            Action::Commit {
                producer,
                sequence,
                offset,
            } => s.commit(producer, sequence, offset)?,
            Action::Complete {
                producer,
                sequence,
                offset,
                gate,
            } => s.complete(producer, sequence, (offset, gate), self.orchestration)?,
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
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
                s.witnesses.seen(Witness::GateDeduplicated)
            }),
            Property::sometimes(
                "retry_deduplicated_after_other_producer",
                |_, s: &InflightState| {
                    s.witnesses
                        .seen(Witness::GateDeduplicatedAfterOtherProducer)
                },
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
        ];
        // Before #1947 the writer answers nothing as a duplicate.
        if self.orchestration == Orchestration::WriterLookup {
            properties.extend([
                Property::sometimes("writer_deduplicated", |_, s: &InflightState| {
                    s.witnesses.seen(Witness::WriterDeduplicated)
                }),
                Property::sometimes(
                    "writer_deduplicated_after_other_producer",
                    |_, s: &InflightState| {
                        s.witnesses
                            .seen(Witness::WriterDeduplicatedAfterOtherProducer)
                    },
                ),
            ]);
        }
        properties
    }
}

/// The broker answers a retry that passes the early gate while its first copy
/// waits for `acks=all` as Kafka does, and appends no batch twice.
#[test]
fn a_retry_during_a_pending_commit_answers_as_a_duplicate() {
    check_model(
        InflightModel {
            batches: [2, 1],
            max_attempts: 2,
            orchestration: Orchestration::WriterLookup,
        },
        "producer_inflight",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES,
    );
}

/// RED: the orchestration before #1947 appends a retry a second time. See the
/// module's "RED regression".
#[test]
fn before_the_writer_lookup_a_retry_appends_again() {
    check_model_with_known_failures(
        InflightModel {
            batches: [2, 1],
            max_attempts: 2,
            orchestration: Orchestration::GateOnly,
        },
        "producer_inflight_gate_only",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES_GATE_ONLY,
        &[
            KnownFailure {
                property: "decision_matches_kafka",
                depth: 4,
            },
            KnownFailure {
                property: "no_duplicate_append",
                depth: 4,
            },
        ],
    );
}
