//! The request half of the producer-state model: every step one idempotent
//! produce request takes on the broker, for two producers that retry.
//!
//! A request is not one step. The produce path runs the dedup gate against
//! the broker's tracker ([`ProducerState::check_batch`]), outside the log's
//! append lock. The partition writer then appends under that lock, and it
//! first looks the batch up among its producer's retained batches in the log
//! (`krabka_log::Log::find_duplicate_batch`, from `duplicate_of` in
//! `partition_writer/append.rs`). The request commits the batch to the tracker
//! once the writer answers ([`ProducerState::commit`], from `finalize_ack` in
//! `handlers/produce/append.rs`), and then waits for `acks=all`. A client that
//! times out does not wait for the broker to answer: it resends the batch on a
//! new connection while the first copy is still at any of these steps, and a
//! request can go away at any of them.
//!
//! So the model splits each request into `Admit` (the gate), `Append` (the
//! writer), `Commit` (the tracker) and `GateResolve` (the `acks=all` wait, which
//! answers or times out), and adds `ClientRetry`, enabled while the batch is
//! unacknowledged, and `Drop`. A request dropped after the gate is already in
//! the writer's queue, so its append still lands; nothing commits it.
//!
//! Each step calls the production code it models. `Admit` and `Commit` run the
//! real async [`ProducerState`] methods on a tracker rebuilt from the state's
//! entries, and `Append` keeps the log's retained batches with
//! [`ProducerSnapshotEntry::find_duplicate_batch`] and
//! [`ProducerSnapshotEntry::add_data_batch`], which `krabka_log::Log` runs
//! under its append lock.
//!
//! The `always` property `no_duplicate_in_log` requires that no
//! `(producer id, epoch, sequence)` is stored twice. The model has room for
//! every batch and one more, so a second copy always fits. With the writer's
//! lookup switched off, the search finds the Jepsen counterexample: a copy and
//! its retry both pass the gate before either commits, and both append.
//!
//! Every batch holds one record at one epoch. Epochs, the five-batch window,
//! and multi-record ranges are the [classification half](super::classification)'s
//! concern. Offsets are log positions, and follower replication, truncation,
//! and transactions remain outside the model.

use {
    crate::{
        api_catalog::UnstableApiVersions,
        model_check::{check_model, shortest_counterexample},
        producer_state::{Checked, Decision, ProducerEntry, ProducerState, SequenceContext},
    },
    futures_util::FutureExt as _,
    krabka_ids::PartitionIndex,
    krabka_log::{Offset, ProducerBatchMetadata, ProducerId, ProducerSnapshotEntry},
    stateright::{Model, Property},
    std::{collections::VecDeque, future::Future},
};

const TOPIC: &str = "t";
const PARTITION: PartitionIndex = PartitionIndex(0);
const EPOCH: i16 = 0;

const MAX_STATES: usize = 20_000_000;
const MAX_DEPTH: usize = 40;

// The exact unique-state count of the exhaustive BFS over each config below.
// See the classification half for why the count is pinned.
const PINNED_UNIQUE_STATES_BASIC: usize = 6_607;
const PINNED_UNIQUE_STATES_WIDE: usize = 1_045_623;

// The longest path the counterexample search tries with the writer's lookup
// switched off. The shortest counterexample is shorter.
const COUNTEREXAMPLE_LENGTH: usize = 8;

/// The bounds of one run.
#[derive(Clone, bon::Builder)]
struct ProducerRequests {
    /// How many producers write to the partition.
    #[builder(default = 2)]
    producers: u8,
    /// How many batches each producer sends, at sequences from 0.
    #[builder(default = 1)]
    batches: i32,
    /// How many copies of one batch a client sends: the first and its retries.
    #[builder(default = 2)]
    sends: u8,
    /// Whether the writer looks a batch up among the log's retained batches
    /// before it appends. Production always does; a run without it shows the
    /// bug the lookup fixes.
    #[builder(default = true)]
    writer_lookup: bool,
}

/// Where one copy of a batch is on the broker.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Stage {
    /// Received, ahead of the dedup gate.
    Received,
    /// The gate let it through, and it waits in the writer's queue.
    Admitted,
    /// As `Admitted`, but the request went away. The writer still appends it,
    /// and nothing commits or answers it.
    Orphaned,
    /// The writer answered with the batch at this offset. The tracker commit
    /// is next.
    Appended(i64),
    /// Waiting for `acks=all` on the batch at this offset.
    Waiting(i64),
}

/// One copy of a batch that a client sent.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct Request {
    producer: u8,
    sequence: i32,
    stage: Stage,
}

/// A batch in the log. Its offset is its position.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Stored {
    producer: u8,
    epoch: i16,
    sequence: i32,
}

/// One producer's state in the log: its entry, whose last batch is the
/// producer's last one, and its earlier retained batches, oldest first, as
/// `krabka_log::Log` keeps them.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct LogProducer {
    entry: ProducerSnapshotEntry,
    earlier: VecDeque<ProducerBatchMetadata>,
}

impl LogProducer {
    fn new(producer: u8) -> Self {
        Self {
            entry: ProducerSnapshotEntry {
                producer_id: ProducerId(producer_id(producer)),
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

/// One producer client. It sends its batches in order and moves on only when
/// the current one is acknowledged.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Client {
    /// The offset each acknowledged batch was answered with, by sequence. The
    /// current batch's sequence is the count of acknowledged ones.
    acked: Vec<i64>,
    /// The copies of the current batch sent so far.
    sends: u8,
}

impl Client {
    fn sequence(&self) -> i32 { i32::try_from(self.acked.len()).expect("a small sequence") }
}

/// A step whose outcome a `sometimes` witness looks for. It is recorded only
/// on the transition that produced it and cleared by the next.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Witness {
    /// A client resent a batch while an earlier copy was still on the broker.
    RetryInFlight,
    /// The gate answered a copy as a duplicate from the tracker.
    GateDuplicate,
    /// The writer answered a copy that passed the gate as a duplicate.
    WriterDuplicate,
    /// As `WriterDuplicate`, with another batch appended after the original.
    WriterDuplicateBehindLaterBatch,
    /// A dropped request's append landed in the log.
    OrphanAppended,
    /// The `acks=all` wait timed out on the batch the client waits for.
    TimedOut,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct RequestState {
    log: Vec<Stored>,
    /// The log's retained batches, by producer.
    log_producers: Vec<Option<LogProducer>>,
    /// The tracker's entries, by producer.
    tracker: Vec<Option<ProducerEntry>>,
    clients: Vec<Client>,
    /// The copies on the broker, kept sorted so that the same set is one
    /// state.
    requests: Vec<Request>,
    /// Whether the gate ever refused a copy as out of order or fenced. Kafka
    /// refuses none of these clients' batches.
    refused: bool,
    witness: Option<Witness>,
}

/// How the `acks=all` wait ends.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Gate {
    Ok,
    Timeout,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Action {
    /// The client sends the first copy of its current batch.
    Send(u8),
    /// The client sends another copy of its unacknowledged batch.
    ClientRetry(u8),
    /// The gate checks the request at this index against the tracker.
    Admit(usize),
    /// The writer takes the request at this index.
    Append(usize),
    /// The request at this index commits to the tracker.
    Commit(usize),
    /// The `acks=all` wait of the request at this index ends.
    GateResolve(usize, Gate),
    /// The request at this index goes away.
    Drop(usize),
}

fn producer_id(producer: u8) -> i64 { i64::from(producer) + 1 }

/// Run a tracker call to completion. The tracker in a model step is new and
/// nothing else holds its lock, so every call is ready on its first poll.
fn now<F: Future>(future: F) -> F::Output {
    future
        .now_or_never()
        .expect("an uncontended tracker call is ready at once")
}

/// A tracker that holds `entries`, by producer.
fn tracker(entries: &[Option<ProducerEntry>]) -> ProducerState {
    let state = ProducerState::new();
    state
        .handle(TOPIC, PARTITION)
        .try_lock()
        .expect("a new tracker is unlocked")
        .entries = (0..)
        .zip(entries)
        .filter_map(|(producer, entry)| Some((ProducerId(producer_id(producer)), (*entry)?)))
        .collect();
    state
}

/// The entries `state` holds, by producer.
fn entries(state: &ProducerState, producers: u8) -> Vec<Option<ProducerEntry>> {
    let snapshot = now(state.snapshot(TOPIC, PARTITION));
    (0..producers)
        .map(|producer| {
            snapshot
                .iter()
                .find(|(id, _)| *id == producer_id(producer))
                .map(|(_, entry)| *entry)
        })
        .collect()
}

impl ProducerRequests {
    /// Room for every batch and one more, so a second copy always fits.
    fn log_capacity(&self) -> usize {
        usize::from(self.producers) * usize::try_from(self.batches).expect("a small batch count")
            + 1
    }

    fn admit(s: &mut RequestState, index: usize) -> Option<()> {
        let request = s.requests[index];
        if request.stage != Stage::Received {
            return None;
        }
        let context = SequenceContext {
            log_empty: s.log.is_empty(),
            unstable: UnstableApiVersions::Disabled,
        };
        let Checked { decision, .. } = now(tracker(&s.tracker).check_batch(
            TOPIC,
            PARTITION,
            context,
            (producer_id(request.producer), EPOCH),
            (request.sequence, 0),
        ));
        match decision {
            Decision::Append => s.requests[index].stage = Stage::Admitted,
            Decision::Duplicate { base_offset } => {
                s.requests[index].stage = Stage::Waiting(base_offset);
                s.witness = Some(Witness::GateDuplicate);
            }
            Decision::OutOfOrder | Decision::Fenced => {
                s.requests.remove(index);
                s.refused = true;
            }
        }
        Some(())
    }

    fn append(&self, s: &mut RequestState, index: usize) -> Option<()> {
        let request = s.requests[index];
        if !matches!(request.stage, Stage::Admitted | Stage::Orphaned) {
            return None;
        }
        let producer = usize::from(request.producer);
        let retained = s.log_producers[producer]
            .as_ref()
            .filter(|_| self.writer_lookup)
            .and_then(|log| {
                log.entry
                    .find_duplicate_batch(&log.earlier, EPOCH, (request.sequence, 0))
            });
        let base_offset = if let Some(retained) = retained {
            let base_offset = retained.base_offset().0;
            let behind = usize::try_from(base_offset).expect("a log position") + 1 < s.log.len();
            s.witness = Some(if behind {
                Witness::WriterDuplicateBehindLaterBatch
            } else {
                Witness::WriterDuplicate
            });
            base_offset
        } else {
            if s.log.len() >= self.log_capacity() {
                return None;
            }
            let offset = i64::try_from(s.log.len()).expect("a small log");
            s.log.push(Stored {
                producer: request.producer,
                epoch: EPOCH,
                sequence: request.sequence,
            });
            let log =
                s.log_producers[producer].get_or_insert_with(|| LogProducer::new(request.producer));
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
            offset
        };
        if request.stage == Stage::Orphaned {
            s.requests.remove(index);
            s.witness = Some(Witness::OrphanAppended);
        } else {
            s.requests[index].stage = Stage::Appended(base_offset);
        }
        Some(())
    }

    fn commit(&self, s: &mut RequestState, index: usize) -> Option<()> {
        let request = s.requests[index];
        let Stage::Appended(base_offset) = request.stage else {
            return None;
        };
        let tracker = tracker(&s.tracker);
        now(tracker.commit(
            TOPIC,
            PARTITION,
            (producer_id(request.producer), EPOCH),
            (request.sequence, 0),
            (base_offset, 0, false),
        ));
        s.tracker = entries(&tracker, self.producers);
        s.requests[index].stage = Stage::Waiting(base_offset);
        Some(())
    }

    fn resolve(s: &mut RequestState, index: usize, gate: Gate) -> Option<()> {
        let request = s.requests[index];
        let Stage::Waiting(base_offset) = request.stage else {
            return None;
        };
        s.requests.remove(index);
        let client = &mut s.clients[usize::from(request.producer)];
        // A client ignores an answer for a batch it already moved past.
        if client.sequence() != request.sequence {
            return Some(());
        }
        match gate {
            Gate::Ok => {
                client.acked.push(base_offset);
                client.sends = 0;
            }
            Gate::Timeout => s.witness = Some(Witness::TimedOut),
        }
        Some(())
    }

    fn send(&self, s: &mut RequestState, producer: u8) -> Option<()> {
        let in_flight = s.requests.iter().any(|request| {
            request.producer == producer
                && request.sequence == s.clients[usize::from(producer)].sequence()
        });
        let client = &mut s.clients[usize::from(producer)];
        if client.sequence() >= self.batches || client.sends >= self.sends {
            return None;
        }
        if in_flight {
            s.witness = Some(Witness::RetryInFlight);
        }
        client.sends += 1;
        let sequence = client.sequence();
        s.requests.push(Request {
            producer,
            sequence,
            stage: Stage::Received,
        });
        Some(())
    }
}

impl Model for ProducerRequests {
    type Action = Action;
    type State = RequestState;

    fn init_states(&self) -> Vec<Self::State> {
        let producers = usize::from(self.producers);
        vec![RequestState {
            log: Vec::new(),
            log_producers: vec![None; producers],
            tracker: vec![None; producers],
            clients: vec![
                Client {
                    acked: Vec::new(),
                    sends: 0,
                };
                producers
            ],
            requests: Vec::new(),
            refused: false,
            witness: None,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        for producer in 0..self.producers {
            let client = &s.clients[usize::from(producer)];
            if client.sequence() < self.batches && client.sends < self.sends {
                actions.push(if client.sends == 0 {
                    Action::Send(producer)
                } else {
                    Action::ClientRetry(producer)
                });
            }
        }
        for (index, request) in s.requests.iter().enumerate() {
            match request.stage {
                Stage::Received => actions.push(Action::Admit(index)),
                Stage::Admitted => actions.push(Action::Append(index)),
                Stage::Orphaned => {
                    actions.push(Action::Append(index));
                    continue;
                }
                Stage::Appended(_) => actions.push(Action::Commit(index)),
                Stage::Waiting(_) => {
                    actions.push(Action::GateResolve(index, Gate::Ok));
                    actions.push(Action::GateResolve(index, Gate::Timeout));
                }
            }
            actions.push(Action::Drop(index));
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = RequestState {
            witness: None,
            ..last.clone()
        };
        match action {
            Action::Send(producer) | Action::ClientRetry(producer) => self.send(&mut s, producer),
            Action::Admit(index) => Self::admit(&mut s, index),
            Action::Append(index) => self.append(&mut s, index),
            Action::Commit(index) => self.commit(&mut s, index),
            Action::GateResolve(index, gate) => Self::resolve(&mut s, index, gate),
            Action::Drop(index) => {
                if s.requests[index].stage == Stage::Admitted {
                    s.requests[index].stage = Stage::Orphaned;
                } else {
                    s.requests.remove(index);
                }
                Some(())
            }
        }?;
        s.requests.sort_unstable();
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: one batch is stored at most once, whatever the client
            // resends and wherever its earlier copies are.
            Property::always("no_duplicate_in_log", |_, s: &RequestState| {
                s.log
                    .iter()
                    .enumerate()
                    .all(|(offset, batch)| !s.log[..offset].contains(batch))
            }),
            // Safety: a batch is acknowledged with the offset that holds it.
            Property::always("acked_offset_holds_the_batch", |_, s: &RequestState| {
                (0..).zip(&s.clients).all(|(producer, client)| {
                    (0..).zip(&client.acked).all(|(sequence, offset)| {
                        usize::try_from(*offset)
                            .ok()
                            .and_then(|offset| s.log.get(offset))
                            == Some(&Stored {
                                producer,
                                epoch: EPOCH,
                                sequence,
                            })
                    })
                })
            }),
            // Safety: the gate never refuses a batch Kafka accepts, however far
            // the tracker lags the log.
            Property::always("no_batch_refused", |_, s: &RequestState| !s.refused),
            Property::sometimes("retry_while_first_copy_in_flight", |_, s: &RequestState| {
                s.witness == Some(Witness::RetryInFlight)
            }),
            Property::sometimes("gate_answers_duplicate", |_, s: &RequestState| {
                s.witness == Some(Witness::GateDuplicate)
            }),
            Property::sometimes("writer_answers_duplicate", |_, s: &RequestState| {
                s.witness == Some(Witness::WriterDuplicate)
            }),
            Property::sometimes(
                "writer_answers_duplicate_behind_later_batch",
                |_, s: &RequestState| s.witness == Some(Witness::WriterDuplicateBehindLaterBatch),
            ),
            Property::sometimes("dropped_request_appends", |_, s: &RequestState| {
                s.witness == Some(Witness::OrphanAppended)
            }),
            Property::sometimes("acks_all_wait_times_out", |_, s: &RequestState| {
                s.witness == Some(Witness::TimedOut)
            }),
            Property::sometimes("every_batch_acked", |model: &Self, s: &RequestState| {
                s.clients
                    .iter()
                    .all(|client| client.sequence() == model.batches)
            }),
        ]
    }
}

#[test]
fn producer_requests_basic() {
    // Two producers with one batch each, sent up to twice.
    check_model(
        ProducerRequests::builder().build(),
        "producer_requests_basic",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn producer_requests_wide() {
    // Two producers with two batches each, each sent up to twice.
    check_model(
        ProducerRequests::builder().batches(2).build(),
        "producer_requests_wide",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES_WIDE,
    );
}

#[test]
fn without_the_writer_lookup_a_retry_appends_twice() {
    // The broker as it was before the writer looked a batch up: a copy and
    // its retry both pass the gate before either commits, and both append.
    let model = ProducerRequests::builder().writer_lookup(false).build();
    let actions = shortest_counterexample(&model, "no_duplicate_in_log", COUNTEREXAMPLE_LENGTH)
        .expect("a second copy reaches the log");
    // Send, retry, two admits and two appends, with no commit between them.
    assert2::assert!(actions.len() == 6, "{actions:?}");
    assert2::assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::Commit(_))),
        "{actions:?}"
    );
}
