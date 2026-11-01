//! Exhaustive stateright model of one idempotent produce, step by step, while
//! its client retries.
//!
//! `producer_state_model` checks the decision kernel over one serialized
//! submit sequence: each submit is checked and committed in one step. The
//! broker does not run a produce that way. The steps of one `acks=all` produce
//! are separate, and requests from one producer can interleave between them:
//!
//! 1. `handle_duplicate` (`handlers/produce/producer_checks.rs`) classifies the
//!    batch with [`ProducerState::check_batch`], which is `check_retained`
//!    over the tracked entry, under the tracker's lock, and releases the lock.
//! 2. The partition writer takes the batch under the log's append lock. It
//!    first looks the batch up among its producer's retained batches in the
//!    log (`duplicate_of` in `partition_writer/append.rs`, which calls
//!    `krabka_log::Log::find_duplicate_batch`) and answers a copy with the
//!    retained batch's offset. Otherwise it appends, and the log adds the
//!    batch to the producer's retained batches. The writer does not check the
//!    sequence again.
//! 3. `finalize_ack` (`handlers/produce/append.rs`) commits the writer's
//!    answer into the tracker through [`ProducerState::commit`].
//! 4. `PendingAck::finish` waits for the high-watermark gate and answers.
//!
//! The produce holds only a shared read guard of the partition's transition
//! barrier (`ProduceTransition`) through all of them, so two requests of one
//! producer can be in flight together. A client whose request outlives its
//! `request.timeout.ms`, or whose connection drops, sends the batch again with
//! the same producer id, epoch and sequence over a new connection while the
//! first copy is still at any of these steps.
//!
//! Kafka checks the batch and updates the producer state under the log lock
//! at append time (`UnifiedLog.append`), so a retry sees every batch already
//! in the log. The writer's lookup is that check; the tracker gate in front of
//! it only answers early what the log already holds.
//!
//! # State
//!
//! - The partition log, one single-record batch per slot, each slot's offset
//!   its index. A slot carries the batch's producer id, epoch and sequence.
//! - The log's producer state for each producer: its
//!   [`ProducerSnapshotEntry`] and earlier retained batches, as
//!   `krabka_log::Log` keeps them.
//! - The host tracker's [`ProducerEntry`] for each producer.
//! - A ghost of Kafka's producer state for each producer: the last sequence
//!   and the retained batches, with offsets, at most five, written the plain
//!   way and updated when the log appends.
//! - The requests on the broker, each at one step.
//! - Each client's next sequence, the sends it made of that batch, and
//!   whether the broker refused it.
//!
//! # Actions
//!
//! Every step calls the production code it models. `Admit` and `Commit` run
//! the real async [`ProducerState`] methods on a tracker that holds the
//! state's entries, and `Append` runs
//! [`ProducerSnapshotEntry::find_duplicate_batch`] and
//! [`ProducerSnapshotEntry::add_data_batch`], the bodies of the log's own
//! lookup and update.
//!
//! - `Send` and `Retry`: the client sends its next batch, or sends its
//!   outstanding batch again. A retry is enabled whenever the batch is
//!   unacknowledged and the client has attempts left, which covers a
//!   client-side timeout, a dropped connection and a `REQUEST_TIMED_OUT`
//!   answer, wherever the earlier copies are.
//! - `Admit`: the dedup gate. A duplicate answers at once and appends
//!   nothing; an out-of-order or fenced batch is refused.
//! - `Append`: the writer looks the batch up and answers a copy, or appends it
//!   at the log's end. The ghost takes an append as Kafka's log would.
//! - `Commit`: the request commits the writer's answer into the tracker.
//! - `GateResolve`: the gate resolves, replicated or timed out, and the broker
//!   answers. A replicated gate acknowledges the batch, and the client moves
//!   to its next one.
//! - `Drop`: the request goes away. One that passed the gate is already in the
//!   writer's queue, so the writer still takes it, and nothing commits or
//!   answers it.
//!
//! Two producers with distinct ids share the partition. The first sends two
//! batches and the second one. Every batch is sent at most twice.
//!
//! # Properties
//!
//! - `no_duplicate_append` (always): no producer id, epoch and sequence is in
//!   the log twice.
//! - `decision_matches_kafka` (always): every answer the broker decides is the
//!   one Kafka gives over the batches already in the log at that step. The gate
//!   decides a duplicate or a refusal, and the writer decides the rest. A
//!   duplicate must name the offset Kafka names. A batch the gate lets through
//!   is not compared at the gate: the tracker may lag the log there, and the
//!   writer answers it.
//! - Witnesses (sometimes): a retry admitted by the gate while an earlier copy
//!   of its batch is appended and not yet committed; a retry answered as a
//!   duplicate by the gate; a copy answered as a duplicate by the writer, and
//!   one after the other producer's batch was appended behind the original; a
//!   dropped request whose append lands; and every batch acknowledged.
//!
//! # Without the writer's lookup
//!
//! [`WriterLookup::Off`] restates the broker before the lookup (#1947). Both
//! `always` properties then fail, pinned as [`KnownFailure`]s at six actions:
//! the client sends and retries, both copies pass the gate before either
//! commits, and the writer appends both. This is the duplicate Jepsen found
//! (krabka-io/krabka-broker#1932). A tracker committed at append time does
//! not stop it: only the check under the append lock does.

use {
    super::{
        Checked, Decision, ProducerEntry, ProducerState, decision::SequenceContext,
        entry::NUM_BATCHES_TO_RETAIN,
    },
    crate::{
        api_catalog::UnstableApiVersions,
        model_check::{KnownFailure, check_model_with_known_failures},
        partition::LogOffset,
    },
    futures_util::FutureExt as _,
    krabka_ids::PartitionIndex,
    krabka_log::{Offset, ProducerBatchMetadata, ProducerId, ProducerSnapshotEntry},
    stateright::{Model, Property},
    std::{collections::VecDeque, future::Future},
};

const MAX_DEPTH: usize = 40;
const MAX_STATES: usize = 4_000_000;

// The exact unique-state count of each exhaustive single-threaded BFS. A
// changed count is a changed model: a dropped action, a transition that starts
// returning `None`, or a field that the derived `Hash` stops reading.
const PINNED_UNIQUE_STATES: usize = 60_139;
const PINNED_UNIQUE_STATES_WITHOUT_LOOKUP: usize = 492_900;

/// The producer ids, one per producer index.
const PRODUCER_IDS: [i64; 2] = [11, 22];
/// The one epoch every batch carries.
const EPOCH: i16 = 0;
/// The tracker's topic and partition.
const TOPIC: &str = "t";
const PARTITION: PartitionIndex = PartitionIndex(0);

/// Whether the writer looks a batch up among the log's retained batches
/// before it appends.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WriterLookup {
    /// It does, under the append lock, as production does.
    UnderLock,
    /// It appends every batch the gate lets through, as the broker did before
    /// #1947.
    Off,
}

struct InflightModel {
    /// The batches each producer sends, sequences `0..batches`.
    batches: [i32; 2],
    /// The sends of one batch a client makes, its first send included.
    max_attempts: u8,
    lookup: WriterLookup,
}

/// One single-record batch in the log. Its offset is its slot's index.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Slot {
    producer_id: i64,
    epoch: i16,
    sequence: i32,
}

/// One producer's state in the log: its entry, whose last batch is the
/// producer's last one, and its earlier retained batches, oldest first.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct LogProducer {
    entry: ProducerSnapshotEntry,
    earlier: VecDeque<ProducerBatchMetadata>,
}

impl LogProducer {
    fn new(producer: usize) -> Self {
        Self {
            entry: ProducerSnapshotEntry {
                producer_id: ProducerId(PRODUCER_IDS[producer]),
                producer_epoch: EPOCH,
                last_sequence: -1,
                last_offset: Offset(-1),
                offset_delta: 0,
                timestamp: -1,
                coordinator_epoch: -1,
                current_txn_first_offset: None,
            },
            earlier: VecDeque::new(),
        }
    }
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

/// Where one copy of a batch is on the broker.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Stage {
    /// Received, ahead of the gate.
    Received,
    /// The gate let it through, and it waits in the writer's queue.
    Admitted,
    /// As `Admitted`, but the request went away. The writer still takes it,
    /// and nothing commits or answers it.
    Orphaned,
    /// The writer answered with the batch at this offset, and the tracker
    /// commit is next.
    Answered(LogOffset),
    /// Committed, and waiting for the gate on the batch at this offset.
    Waiting(LogOffset),
}

/// One copy of a batch on the broker.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct Request {
    producer: usize,
    sequence: i32,
    stage: Stage,
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

/// Which step decided the answer that was compared with Kafka's.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Step {
    Gate,
    Writer,
}

/// A batch the host and Kafka answered differently.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Violation {
    step: Step,
    producer: usize,
    sequence: i32,
    host: Answer,
    kafka: Answer,
}

/// An outcome a `sometimes` witness looks for. It is recorded only on the
/// transition that produced it and cleared by the next. One transition can
/// produce several, so the state holds them as a set.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Witness {
    /// The gate let a retry through while an earlier copy of its batch was
    /// appended and not yet committed: the window the Jepsen failure needs.
    AdmittedDuringPendingCommit,
    /// The gate answered a retry as a duplicate.
    GateDeduplicated,
    /// The writer answered a copy as a duplicate.
    WriterDeduplicated,
    /// The writer answered a copy as a duplicate of a batch that the other
    /// producer's batch was appended behind.
    WriterDeduplicatedAfterOtherProducer,
    /// A dropped request's append landed in the log.
    OrphanAppended,
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
    log_producers: [Option<LogProducer>; 2],
    tracked: [Option<ProducerEntry>; 2],
    kafka: [Option<Window>; 2],
    /// Sorted, so two states that differ only in the order requests arrived
    /// are one state.
    requests: Vec<Request>,
    clients: [Client; 2],
    witnesses: Witnesses,
    /// The first batch the host and Kafka answered differently.
    violation: Option<Violation>,
}

/// Whether the gate a [`Action::GateResolve`] resolves saw the batch
/// replicated.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Gate {
    Replicated,
    TimedOut,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Action {
    /// The client sends its next batch.
    Send { producer: usize },
    /// The client sends its outstanding batch again.
    Retry { producer: usize },
    /// The gate checks the request at this index.
    Admit(usize),
    /// The writer takes the request at this index.
    Append(usize),
    /// The request at this index commits the writer's answer.
    Commit(usize),
    /// The gate of the request at this index resolves, and the broker answers.
    GateResolve(usize, Gate),
    /// The request at this index goes away.
    Drop(usize),
}

/// Run a tracker call to completion. The tracker of a step is new and nothing
/// else holds its lock, so every call is ready on its first poll.
fn now<F: Future>(future: F) -> F::Output {
    future
        .now_or_never()
        .expect("an uncontended tracker call is ready at once")
}

/// A tracker that holds `entries`, by producer.
fn tracker(entries: &[Option<ProducerEntry>; 2]) -> ProducerState {
    let state = ProducerState::new();
    state
        .handle(TOPIC, PARTITION)
        .try_lock()
        .expect("a new tracker is unlocked")
        .entries = PRODUCER_IDS
        .iter()
        .zip(entries)
        .filter_map(|(id, entry)| Some((ProducerId(*id), (*entry)?)))
        .collect();
    state
}

/// The entries `state` holds, by producer.
fn tracked(state: &ProducerState) -> [Option<ProducerEntry>; 2] {
    let snapshot = now(state.snapshot(TOPIC, PARTITION));
    PRODUCER_IDS.map(|id| {
        snapshot
            .iter()
            .find(|(tracked, _)| *tracked == id)
            .map(|(_, entry)| *entry)
    })
}

impl InflightState {
    fn compare(&mut self, step: Step, request: Request, host: Answer, kafka: Answer) {
        if host != kafka {
            self.violation.get_or_insert(Violation {
                step,
                producer: request.producer,
                sequence: request.sequence,
                host,
                kafka,
            });
        }
    }

    /// The client of `producer` sends its current batch.
    fn send(&mut self, producer: usize) {
        let sequence = self.clients[producer].next;
        self.clients[producer].attempts += 1;
        self.requests.push(Request {
            producer,
            sequence,
            stage: Stage::Received,
        });
    }

    /// The gate checks the request at `index` against the tracker.
    fn admit(&mut self, index: usize) -> Option<()> {
        let request = *self.requests.get(index)?;
        if request.stage != Stage::Received {
            return None;
        }
        let context = SequenceContext {
            log_empty: self.log.is_empty(),
            unstable: UnstableApiVersions::Disabled,
        };
        let host = Answer::of(now(tracker(&self.tracked).check_batch(
            TOPIC,
            PARTITION,
            context,
            (PRODUCER_IDS[request.producer], EPOCH),
            (request.sequence, 0),
        )))
        .expect("a duplicate names the retained batch at its offset");
        match host {
            Answer::Append => {
                if self.requests.iter().any(|other| {
                    other.producer == request.producer
                        && other.sequence == request.sequence
                        && matches!(other.stage, Stage::Answered(_))
                }) {
                    self.witnesses.mark(Witness::AdmittedDuringPendingCommit);
                }
                self.requests[index].stage = Stage::Admitted;
            }
            Answer::Duplicate { .. } => {
                let kafka = kafka_decision(self.kafka[request.producer].as_ref(), request.sequence);
                self.compare(Step::Gate, request, host, kafka);
                self.witnesses.mark(Witness::GateDeduplicated);
                self.requests.remove(index);
                self.clients[request.producer].acknowledge(request.sequence);
            }
            Answer::OutOfOrder | Answer::Fenced => {
                let kafka = kafka_decision(self.kafka[request.producer].as_ref(), request.sequence);
                self.compare(Step::Gate, request, host, kafka);
                self.requests.remove(index);
                self.clients[request.producer].refused = true;
            }
        }
        Some(())
    }

    /// The writer takes the request at `index`.
    fn append(&mut self, index: usize, lookup: WriterLookup) -> Option<()> {
        let request = *self.requests.get(index)?;
        if !matches!(request.stage, Stage::Admitted | Stage::Orphaned) {
            return None;
        }
        let producer = request.producer;
        let kafka = kafka_decision(self.kafka[producer].as_ref(), request.sequence);
        let retained = self.log_producers[producer]
            .as_ref()
            .filter(|_| lookup == WriterLookup::UnderLock)
            .and_then(|log| {
                log.entry
                    .find_duplicate_batch(&log.earlier, EPOCH, (request.sequence, 0))
            });
        let offset = if let Some(retained) = retained {
            let offset = retained.base_offset().0;
            self.compare(
                Step::Writer,
                request,
                Answer::Duplicate {
                    sequence: request.sequence,
                    offset,
                },
                kafka,
            );
            self.witnesses.mark(Witness::WriterDeduplicated);
            // An offset is its slot's index.
            let after = usize::try_from(offset)
                .map_or(&[][..], |slot| self.log.get(slot + 1..).unwrap_or_default());
            if after
                .iter()
                .any(|slot| slot.producer_id != PRODUCER_IDS[producer])
            {
                self.witnesses
                    .mark(Witness::WriterDeduplicatedAfterOtherProducer);
            }
            offset
        } else {
            let offset = LogOffset::try_from(self.log.len()).ok()?;
            self.compare(Step::Writer, request, Answer::Append, kafka);
            if kafka == Answer::Append {
                self.kafka[producer] = Some(Window::after_append(
                    self.kafka[producer].as_ref(),
                    request.sequence,
                    offset,
                ));
            }
            self.log.push(Slot {
                producer_id: PRODUCER_IDS[producer],
                epoch: EPOCH,
                sequence: request.sequence,
            });
            let log =
                self.log_producers[producer].get_or_insert_with(|| LogProducer::new(producer));
            log.entry.add_data_batch(
                &mut log.earlier,
                EPOCH,
                ProducerBatchMetadata {
                    last_sequence: request.sequence,
                    last_offset: Offset(offset),
                    offset_delta: 0,
                    timestamp: 0,
                },
                false,
            );
            if request.stage == Stage::Orphaned {
                self.witnesses.mark(Witness::OrphanAppended);
            }
            offset
        };
        if request.stage == Stage::Orphaned {
            self.requests.remove(index);
        } else {
            self.requests[index].stage = Stage::Answered(offset);
        }
        Some(())
    }

    /// The request at `index` commits the writer's answer into the tracker.
    fn commit(&mut self, index: usize) -> Option<()> {
        let request = *self.requests.get(index)?;
        let Stage::Answered(offset) = request.stage else {
            return None;
        };
        let tracker = tracker(&self.tracked);
        now(tracker.commit(
            TOPIC,
            PARTITION,
            (PRODUCER_IDS[request.producer], EPOCH),
            (request.sequence, 0),
            (offset, 0, false),
        ));
        self.tracked = tracked(&tracker);
        self.requests[index].stage = Stage::Waiting(offset);
        Some(())
    }

    /// The gate of the request at `index` resolves, and the broker answers.
    fn resolve(&mut self, index: usize, gate: Gate) -> Option<()> {
        let request = *self.requests.get(index)?;
        let Stage::Waiting(_) = request.stage else {
            return None;
        };
        self.requests.remove(index);
        if gate == Gate::Replicated {
            self.clients[request.producer].acknowledge(request.sequence);
        }
        Some(())
    }

    /// The request at `index` goes away.
    fn drop_request(&mut self, index: usize) -> Option<()> {
        let request = self.requests.get_mut(index)?;
        match request.stage {
            Stage::Orphaned => return None,
            Stage::Admitted => request.stage = Stage::Orphaned,
            Stage::Received | Stage::Answered(_) | Stage::Waiting(_) => {
                self.requests.remove(index);
            }
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
            log_producers: [None, None],
            tracked: [None; 2],
            kafka: [None, None],
            requests: Vec::new(),
            clients: [Client::START; 2],
            witnesses: Witnesses::default(),
            violation: None,
        }]
    }

    fn actions(&self, s: &InflightState, actions: &mut Vec<Action>) {
        for (producer, client) in s.clients.iter().enumerate() {
            if client.refused || client.next >= self.batches[producer] {
                continue;
            }
            if client.attempts == 0 {
                actions.push(Action::Send { producer });
            } else if client.attempts < self.max_attempts {
                actions.push(Action::Retry { producer });
            }
        }
        for (index, request) in s.requests.iter().enumerate() {
            match request.stage {
                Stage::Received => actions.push(Action::Admit(index)),
                Stage::Admitted | Stage::Orphaned => actions.push(Action::Append(index)),
                Stage::Answered(_) => actions.push(Action::Commit(index)),
                Stage::Waiting(_) => {
                    actions.push(Action::GateResolve(index, Gate::Replicated));
                    actions.push(Action::GateResolve(index, Gate::TimedOut));
                }
            }
            if request.stage != Stage::Orphaned {
                actions.push(Action::Drop(index));
            }
        }
    }

    fn next_state(&self, last: &InflightState, action: Action) -> Option<InflightState> {
        let mut s = InflightState {
            witnesses: Witnesses::default(),
            ..last.clone()
        };
        match action {
            Action::Send { producer } | Action::Retry { producer } => s.send(producer),
            Action::Admit(index) => s.admit(index)?,
            Action::Append(index) => s.append(index, self.lookup)?,
            Action::Commit(index) => s.commit(index)?,
            Action::GateResolve(index, gate) => s.resolve(index, gate)?,
            Action::Drop(index) => s.drop_request(index)?,
        }
        s.requests.sort_unstable();
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
            Property::sometimes("retry_deduplicated_by_gate", |_, s: &InflightState| {
                s.witnesses.seen(Witness::GateDeduplicated)
            }),
            Property::sometimes("dropped_request_appends", |_, s: &InflightState| {
                s.witnesses.seen(Witness::OrphanAppended)
            }),
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
        // Only the writer's lookup answers a copy as a duplicate.
        if self.lookup == WriterLookup::UnderLock {
            properties.extend([
                Property::sometimes("copy_deduplicated_by_writer", |_, s: &InflightState| {
                    s.witnesses.seen(Witness::WriterDeduplicated)
                }),
                Property::sometimes(
                    "copy_deduplicated_by_writer_after_other_producer",
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

/// The model's bounds, with the writer's lookup as given.
fn model(lookup: WriterLookup) -> InflightModel {
    InflightModel {
        batches: [2, 1],
        max_attempts: 2,
        lookup,
    }
}

#[test]
fn a_retry_never_appends_a_batch_twice() {
    check_model_with_known_failures(
        model(WriterLookup::UnderLock),
        "producer_inflight",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES,
        &[],
    );
}

#[test]
fn without_the_writer_lookup_a_retry_appends_again() {
    // Send, Retry, Admit, Admit, Append, Append: both copies pass the gate
    // before either commits, and the writer appends both. The writer's second
    // append is also the answer Kafka gives as a duplicate.
    check_model_with_known_failures(
        model(WriterLookup::Off),
        "producer_inflight_without_lookup",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES_WITHOUT_LOOKUP,
        &[
            KnownFailure {
                property: "decision_matches_kafka",
                depth: 6,
            },
            KnownFailure {
                property: "no_duplicate_append",
                depth: 6,
            },
        ],
    );
}
