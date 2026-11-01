//! Exhaustive model of transaction completion: a transaction that never gets
//! its marker.
//!
//! The Jepsen workload found a transaction that stays open on a partition
//! after brokers are killed, although the producer's `transaction.timeout.ms`
//! is one second. Its partition's last stable offset stays behind it, and a
//! `read_committed` consumer reads nothing past it. The models beside this one
//! (`decision_model`, `two_pc_model`, `eos_composition_model`) persist and
//! fan out in one step, over one partition, and never reload the coordinator,
//! so none of them reaches that state.
//!
//! # Shape
//!
//! One transactional id at `TV_2`, a Jepsen producer that writes one
//! transaction over two data partitions, and the coordinator of its
//! `__transaction_state` partition. The producer adds a partition, produces to
//! it once verified, and sends `EndTxn` (commit or abort), at most
//! [`MAX_END_ATTEMPTS`] times. The coordinator runs one operation at a time,
//! split at its `await`s:
//!
//! - A state write appends its record above the high watermark and waits.
//!   `Commit` moves the high watermark over the tail and wakes the waiter.
//!   While the partition is `Stall`ed, the waiter gives up after the
//!   transaction timeout (`TimedOut`, the `REQUEST_TIMED_OUT` of
//!   `await_state_committed`) and its record stays in the log: it can still
//!   commit later, or be truncated by a failover. That is the indeterminate
//!   persist, and the split between the cache and the log.
//! - `Failover` moves the state partition: the cache, the operation in flight
//!   and the completion queue are gone, and the new leader holds the tail
//!   (`true`) or truncates it (`false`). `Load` rebuilds the cache from the
//!   last record of the new leader's log, as `replay_partition` reads it to the
//!   log end, and queues a `Prepare*` entry for completion.
//! - A marker fan-out tries each partition once. A leader answers `NONE`. An
//!   old leader whose partition `MoveLeader` moved, or whose partition writer
//!   is gone, answers `NOT_LEADER_OR_FOLLOWER`, with the marker kept or lost.
//!   In the [`CompletionModel::refusals`] variant a leader can also answer
//!   `UNKNOWN_SERVER_ERROR` as a fault. Then `EndTxn`, the reaper, or the
//!   completion task reacts as it does in production.
//! - `ReaperTick` is one sweep, `CompletionRun` one completion attempt, and
//!   `Tick` moves the clock past the open transaction's timeout.
//!
//! The faults are `Stall`, `Failover`, `MoveLeader`, a refused marker and
//! `Abandon` (the test kills the producer), at most two of them, and `Heal`,
//! after which there are none.
//!
//! # Driven production code
//!
//! - `AddPartitionsToTxn`: `registration_decision` and `stage_registration`.
//! - Produce verification: `verification_code`.
//! - `EndTxn`: `end_txn_decision`, `decide_phase1_transition`,
//!   `apply_end_txn_prepare`, `prepare_completion_identities_with_fresh`,
//!   `fan_out_outcome` and Phase 3's `decide_end_txn_completion` and
//!   `apply_completion`.
//! - The fan-out: `MarkerFanOut::fail` keeps the failure that decides the
//!   attempt (`record_deciding_failure`), `classify_marker_failure` classifies
//!   it, and `remove_marked_partitions` and `remove_marked_partitions_from`
//!   drop the written partitions.
//! - The reaper: `reaper_step` (with `should_abort_idle_txn`),
//!   `apply_prepare_abort`, `prepare_server_abort_identities_with_fresh`,
//!   `complete_abort_decision`, and `apply_completion` to `CompleteAbort`,
//!   which is all `apply_complete_abort` does.
//! - The completion task: `completion_for`, `fan_out_failure_attempt`,
//!   `completion_decision`, `apply_completion`.
//!
//! Restated: the load's last-value fold (`RecoveredTransactions::apply_value`)
//! and its queueing of a `Prepare*` entry (`install_recovered`); the reaction
//! to each state-write result in `prepare_transaction`, `EndTxn`'s handler,
//! `prepare_abort`, `complete_abort` and `complete_prepared_transaction`; and
//! a data partition's log, which holds one batch and the first marker after
//! it.
//!
//! # Properties
//!
//! - [`CONVERGED`]: once the faults have ended and no step can move the
//!   system (see [`Quiescence`]), every partition that holds the
//!   transaction's data has its marker, and its last stable offset is its
//!   high watermark.
//! - [`ONE_OUTCOME`]: no transaction commits on one partition and aborts on
//!   another.
//! - `projection_exact`: every entry a kernel returned fits the model.
//! - Witnesses: an indeterminate persist, a log ahead of the cache, a reload
//!   of a prepared transaction, a reaper abort, a marker to a moved leader, a
//!   converged run after `Heal`, and, with refusals, a fenced-or-unexpected
//!   marker answer.
//!
//! # Known failures
//!
//! Both properties fail on the current code, with every marker leader
//! answering. Both failures share one cause: a coordinator load publishes
//! records its new leader has not committed. `replay_partition` reads the log
//! to its end, not to the high watermark, and the coordinator acts on what it
//! read before the record commits. A second move of the state partition, to a
//! replica that never had the record, then takes it back.
//!
//! [`CONVERGED`] fails at depth 10:
//!
//! ```text
//! Add(0), Failover(true), Load, Add(0), Produce(0), End(true),
//! Failover(false), Load, End(true), Heal
//! ```
//!
//! The registration of partition 0 is appended but not committed when the
//! state partition moves. The new leader's load publishes the uncommitted
//! `Ongoing` record. The producer's retry of the registration finds the
//! partition registered and gets `NONE` without a write (`PersistRetry`), and
//! the producer writes its batch. The second move truncates the record, and
//! the next load finds the transaction `Empty`. The reaper aborts only an
//! `Ongoing` transaction it holds, so the batch on partition 0 stays open for
//! good. That is the Jepsen symptom: the abort the timeout should bring never
//! comes. An `EndTxn(abort)` does not help: an abort of an `Empty`
//! transaction marks no partition.
//!
//! [`ONE_OUTCOME`] fails at depth 16:
//!
//! ```text
//! Add(0), Commit, Produce(0), Add(1), Commit, End(true), Tick,
//! Failover(true), Load, CompletionRun, Marker(0, Written), Failover(false),
//! Load, ReaperTick, Commit, Marker(1, Written)
//! ```
//!
//! The load publishes an uncommitted `PrepareCommit` and queues it, and the
//! completion task writes the COMMIT marker of partition 0. The second move
//! truncates the record, the next load finds the transaction `Ongoing`, and
//! the reaper aborts it: partition 1 gets an ABORT marker.
//!
//! With refusals, [`CONVERGED`] fails sooner, at depth 7:
//!
//! ```text
//! Add(0), Commit, Produce(0), End(true), Commit, Marker(0, Refused), Heal
//! ```
//!
//! The `PrepareCommit` is durable, and its one marker answers an unexpected
//! code. `fan_out_outcome` gives up, so `EndTxn` answers `NONE` and queues
//! nothing (the `GivenUp` arm of `handlers/end_txn.rs`); the completion task
//! gives up the same way (`fan_out_failure_attempt`); and the reaper resumes
//! only a `PrepareAbort` (`reaper_step`). Nothing writes the marker until the
//! state partition moves again. A `PrepareAbort` in the same place recovers,
//! because the reaper sends its markers again, and since #1941 a retriable
//! partition in the same attempt keeps it queued. This path matches Kafka,
//! which also stops on an unexpected code, and after #1941 only a leader in
//! an illegal state answers one (see [`Res::Refused`]).
//!
//! # Not covered
//!
//! One coordinator operation runs at a time, so the model does not interleave
//! an `EndTxn` fan-out with the reaper's or the completion task's, and a
//! fan-out of the old coordinator ends with the failover. A stale coordinator
//! fenced by a newer one is therefore never modeled, and a fenced marker
//! answer stands in the same action as an unexpected one, since both stop the
//! fan-out. A data partition's own replication is not modeled: a produce and
//! a written marker are committed when they happen. Only `TV_2` is covered,
//! with no epoch exhaustion, no 2PC, and no `InitProducerId`.

use {
    super::{
        coordinator::{
            ReaperStep, apply_prepare_abort, complete_abort_decision,
            completion::{
                CompletionAttempt, PreparedCompletion, apply_completion, completion_decision,
                completion_for, fan_out_failure_attempt,
            },
            produce_verification::{HeldTransaction, verification_code},
            reaper_step, registration_decision, remove_marked_partitions,
            remove_marked_partitions_from, stage_registration,
        },
        decision::{
            CompletionDecision, CompletionExpectation, apply_end_txn_prepare,
            decide_end_txn_completion, decide_phase1_transition,
        },
        handlers::end_txn::{
            EndTxnDecision, MarkerFanOut, MarkerFanOutOutcome, TableRules,
            client_producer_identity, completion_producer_identity, end_txn_decision,
            fan_out_outcome, prepare_completion_identities_with_fresh,
            prepare_server_abort_identities_with_fresh,
        },
        marker::{MarkerType, classify_marker_failure},
        state::{TopicPartition, TxnEntry, TxnState},
        two_pc::should_abort_idle_txn,
        util::ProducerIdentity,
        version::TxnVersion,
    },
    crate::{
        codes,
        error::BrokerError,
        model_check::{KnownFailure, Quiescence, check_model_with_known_failures},
    },
    krabka_ids::PartitionIndex,
    krabka_log::ProducerId,
    krabka_verified::transaction::{
        TransactionReaperCompletionDecision, TransactionRegistrationDecision,
    },
    stateright::{Model, Property},
};

const TID: &str = "tid";
const PID: i64 = 1000;
/// The Jepsen producer's `transaction.timeout.ms`.
const TIMEOUT_MS: i32 = 1000;
const TOPIC: &str = "data";
const PARTITIONS: u8 = 2;
/// The `EndTxn` requests the producer sends before it gives up.
const MAX_END_ATTEMPTS: u8 = 2;

const MAX_DEPTH: usize = 40;
const TARGET_STATE_COUNT: usize = 2_000_000;
const MAX_UNIQUE_STATES: usize = 500_000;

// The exact unique-state count of each exhaustive search below. A changed
// count is a changed model; see `model_check::assert_pinned_count`.
const PINNED_UNIQUE_STATES_REFUSALS: usize = 104_970;
const PINNED_UNIQUE_STATES_NO_REFUSALS: usize = 89_478;

/// The liveness property this model exists for. It fails on the current code.
const CONVERGED: &str = "every_transaction_ends_when_quiescent";
/// The atomicity property. It fails on the current code too.
const ONE_OUTCOME: &str = "one_outcome_per_transaction";

// The number of actions in each shortest counterexample. The module
// documentation gives each trace.
const CONVERGED_DEPTH_REFUSALS: usize = 7;
const CONVERGED_DEPTH_NO_REFUSALS: usize = 10;
const ONE_OUTCOME_DEPTH: usize = 16;

/// One partition's bit in a partition set.
fn bit(partition: u8) -> u8 { 1 << partition }

fn topic_partition(partition: u8) -> TopicPartition {
    TopicPartition {
        topic: TOPIC.to_owned(),
        partition: PartitionIndex(i32::from(partition)),
    }
}

fn topic_partitions(set: u8) -> Vec<TopicPartition> {
    (0..PARTITIONS)
        .filter(|&partition| set & bit(partition) != 0)
        .map(topic_partition)
        .collect()
}

/// A hashable projection of the one `TxnEntry` this model holds. Every field
/// the model can change is here; [`Ent::entry`] rebuilds the production value
/// that the decision kernels read, and [`project`] maps it back.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Ent {
    /// `TxnState::to_kafka_status`.
    state: i8,
    epoch: i16,
    last_epoch: i16,
    /// The transaction's partitions, one bit each.
    parts: u8,
    start_ms: i64,
    updated_ms: i64,
    client_tv: i16,
}

impl Ent {
    /// The entry `InitProducerId` leaves: `Empty` at epoch 0.
    fn initial() -> Self {
        project(&TxnEntry::new_empty(
            TID.to_owned(),
            ProducerId(PID),
            0,
            TIMEOUT_MS,
            0,
        ))
        .expect("a fresh entry has a projection")
    }

    fn state(self) -> TxnState {
        TxnState::from_kafka_status(self.state).expect("a model state is a modeled TxnState")
    }

    fn entry(self) -> TxnEntry {
        let mut entry =
            TxnEntry::new_empty(TID.to_owned(), ProducerId(PID), self.epoch, TIMEOUT_MS, 0);
        entry.state = self.state();
        entry.last_producer_epoch = self.last_epoch;
        entry.partitions = topic_partitions(self.parts).into_iter().collect();
        entry.start_ms = self.start_ms;
        entry.last_update_ms = self.updated_ms;
        entry.client_transaction_version = self.client_tv;
        entry
    }
}

/// The projection of `entry`, or `None` when it holds a value the model has no
/// field for (a rotated producer id, a staged identity, another partition).
fn project(entry: &TxnEntry) -> Option<Ent> {
    let mut parts = 0;
    for partition in &entry.partitions {
        let index = u8::try_from(partition.partition.get()).ok()?;
        parts |= bit(index);
    }
    let ent = Ent {
        state: entry.state.to_kafka_status(),
        epoch: entry.producer_epoch,
        last_epoch: entry.last_producer_epoch,
        parts,
        start_ms: entry.start_ms,
        updated_ms: entry.last_update_ms,
        client_tv: entry.client_transaction_version,
    };
    (ent.entry() == *entry).then_some(ent)
}

/// A data partition: at most one data batch of the transaction, then at most
/// one control marker. Every record in it is committed, so its high watermark
/// is its log end.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct Part {
    data: bool,
    /// `Some(true)` for a COMMIT marker, `Some(false)` for an ABORT one.
    marker: Option<bool>,
    /// The partition's leadership moved, and the coordinator's next marker
    /// for it reaches the old leader.
    moved: bool,
}

impl Part {
    fn high_watermark(self) -> u8 { u8::from(self.data) + u8::from(self.marker.is_some()) }

    /// Kafka's first unstable offset: the transaction's data batch while no
    /// marker ends it, else the log end.
    fn last_stable_offset(self) -> u8 {
        if self.data && self.marker.is_none() {
            0
        } else {
            self.high_watermark()
        }
    }
}

/// Where the producer's `EndTxn` stands.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum End {
    NotSent,
    /// An `EndTxn` waits for its answer.
    Waiting {
        commit: bool,
        attempts: u8,
    },
    /// The last `EndTxn` answered an error.
    Failed {
        commit: bool,
        attempts: u8,
    },
    /// `EndTxn` answered `NONE`.
    Done,
}

/// The Jepsen producer: one transaction over two partitions at `TV_2`, so a
/// produce is verified against the coordinator before its batch is appended.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Client {
    epoch: i16,
    added: u8,
    produced: u8,
    end: End,
    /// False once the producer is fenced, or killed by the test.
    alive: bool,
}

/// Who waits for a `__transaction_state` append.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Writer {
    /// `register_partitions` for one partition.
    Add(u8),
    /// `EndTxn` Phase 1, `prepare_transaction`.
    EndPrepare,
    /// `EndTxn` Phase 3, answering the producer with `epoch`.
    EndComplete { epoch: i16 },
    /// The reaper's `prepare_abort`.
    ReaperPrepare,
    /// The reaper's `complete_abort`.
    ReaperComplete,
    /// `complete_prepared_transaction`.
    Completion,
}

/// Who runs a marker fan-out, and so how its outcome is read.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Owner {
    EndTxn,
    Reaper,
    Completion,
}

/// The one coordinator operation in flight.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Op {
    /// `writer` waits for `record`, which sits in the uncommitted tail.
    Persist { writer: Writer, record: Ent },
    /// The markers of `snapshot`: the partitions not yet tried, those
    /// written, and the error code of the failure that decides the attempt.
    FanOut {
        owner: Owner,
        snapshot: Ent,
        untried: u8,
        written: u8,
        failure: Option<i16>,
    },
}

/// Ghost bits for the witnesses no state field shows.
const SEEN_REFUSED: u8 = 1;
const SEEN_REAPER_ABORT: u8 = 2;
/// A kernel returned an entry the projection cannot hold.
const SEEN_BROKEN: u8 = 4;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct St {
    client: Client,
    /// The coordinator's in-memory entry. `None` from a failover until the
    /// next load.
    cache: Option<Ent>,
    /// The last committed `__transaction_state` record. A replay folds the
    /// log to the last value of the transactional id, so only it matters.
    committed: Ent,
    /// Records the leader appended above its high watermark, oldest first.
    tail: Vec<Ent>,
    /// The state partition's high watermark does not move.
    stalled: bool,
    /// The completion task holds the transactional id.
    queued: bool,
    op: Option<Op>,
    now_ms: i64,
    parts: [Part; PARTITIONS as usize],
    faults: u8,
    healed: bool,
    seen: u8,
}

/// The result of one marker write to one partition.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Res {
    /// The leader appended the marker and it committed: `NONE`.
    Written,
    /// The old leader answered `NOT_LEADER_OR_FOLLOWER`, and the marker it
    /// appended survived on the new leader.
    MovedLanded,
    /// The old leader answered `NOT_LEADER_OR_FOLLOWER`, and nothing landed.
    MovedLost,
    /// The leader answered `UNKNOWN_SERVER_ERROR`, a code Kafka's completion
    /// handler throws on. Since #1941 only a leader in an illegal state
    /// answers one: a malformed generation (`RejectMalformed`) or a poisoned
    /// log lock, each a `BrokerError::Txn`. A gone partition writer answers
    /// `NOT_LEADER_OR_FOLLOWER` now, which `MovedLost` stands for.
    Refused,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Act {
    // ----- the producer -----
    Add(u8),
    Produce(u8),
    End(bool),
    // ----- the coordinator and the clock -----
    /// The state partition's high watermark covers its tail.
    Commit,
    /// A stalled append's caller gives up after `transaction.timeout.ms`.
    TimedOut,
    Load,
    ReaperTick,
    CompletionRun,
    Marker(u8, Res),
    /// The clock passes the open transaction's timeout.
    Tick,
    // ----- faults -----
    Stall,
    /// The state partition moves to another broker, which holds the tail
    /// (`true`) or truncates it (`false`).
    Failover(bool),
    MoveLeader(u8),
    /// The test kills the producer.
    Abandon,
    /// The faults end.
    Heal,
}

struct CompletionModel {
    fault_budget: u8,
    /// Whether a marker leader can answer a code that stops the fan-out.
    refusals: bool,
}

/// The `WriteTxnMarkers` refusal a remote leader answers with `code`.
fn refused(code: i16) -> BrokerError {
    BrokerError::MarkerWriteRefused {
        code,
        message: String::new(),
    }
}

fn refusal_code(error: &BrokerError) -> i16 {
    match error {
        BrokerError::MarkerWriteRefused { code, .. } => *code,
        other => codes::from_broker_error(other),
    }
}

/// The one of `failure` and `code` that decides the attempt, as
/// `MarkerFanOut::fail` keeps it (`record_deciding_failure`).
fn deciding(failure: Option<i16>, code: i16) -> Option<i16> {
    let mut fan_out = MarkerFanOut::default();
    if let Some(existing) = failure {
        fan_out.fail(refused(existing));
    }
    fan_out.fail(refused(code));
    fan_out.failure.as_ref().map(refusal_code)
}

impl St {
    fn cache_entry(&self) -> TxnEntry { self.cache.expect("the coordinator is loaded").entry() }

    /// The entry a load builds: the last record of the new leader's log,
    /// which `replay_partition` reads to its log end.
    fn log_end_record(&self) -> Ent { self.tail.last().copied().unwrap_or(self.committed) }

    fn set_cache(&mut self, entry: &TxnEntry) {
        match project(entry) {
            Some(ent) => self.cache = Some(ent),
            None => self.seen |= SEEN_BROKEN,
        }
    }

    /// Append `entry` above the high watermark and wait for it as `writer`.
    fn append(&mut self, writer: Writer, entry: &TxnEntry) {
        let Some(record) = project(entry) else {
            self.seen |= SEEN_BROKEN;
            return;
        };
        // A replay reads only the last record, and the tail commits or is
        // truncated as a whole, so an earlier copy of the record changes
        // nothing: keep the tail free of repeats.
        self.tail.retain(|earlier| *earlier != record);
        self.tail.push(record);
        self.op = Some(Op::Persist { writer, record });
    }

    fn answer_end(&mut self, ok: Option<i16>) {
        let End::Waiting { commit, attempts } = self.client.end else {
            return;
        };
        match ok {
            Some(epoch) => {
                self.client.end = End::Done;
                self.client.epoch = epoch;
            }
            None => self.client.end = End::Failed { commit, attempts },
        }
    }

    fn complete_epoch(snapshot: &TxnEntry) -> i16 { completion_producer_identity(snapshot).1 }

    /// The waiting `writer`'s append committed (`ok`) or failed.
    fn resume(&mut self, writer: Writer, record: Ent, ok: bool) {
        self.op = None;
        match (writer, ok) {
            (Writer::Add(partition), true) => {
                self.cache = Some(record);
                self.client.added |= bit(partition);
            }
            (Writer::EndPrepare, true) => {
                self.cache = Some(record);
                self.start_fan_out(Owner::EndTxn, record);
            }
            // `prepare_transaction` answers the append error.
            (Writer::EndPrepare, false) => self.answer_end(None),
            (Writer::EndComplete { epoch }, true) => {
                self.cache = Some(record);
                self.answer_end(Some(epoch));
            }
            // `handle` queues the transaction and still answers `NONE`.
            (Writer::EndComplete { epoch }, false) => {
                self.queued = true;
                self.answer_end(Some(epoch));
            }
            (Writer::ReaperPrepare, true) => {
                self.cache = Some(record);
                self.seen |= SEEN_REAPER_ABORT;
                self.start_fan_out(Owner::Reaper, record);
            }
            (Writer::ReaperComplete | Writer::Completion, true) => self.cache = Some(record),
            // `complete_prepared_transaction` answers `Retry`.
            (Writer::Completion, false) => self.queued = true,
            (Writer::Add(_) | Writer::ReaperPrepare | Writer::ReaperComplete, false) => {}
        }
    }

    fn start_fan_out(&mut self, owner: Owner, snapshot: Ent) {
        if snapshot.parts == 0 {
            self.finish_fan_out(owner, snapshot, 0, None);
        } else {
            self.op = Some(Op::FanOut {
                owner,
                snapshot,
                untried: snapshot.parts,
                written: 0,
                failure: None,
            });
        }
    }

    /// `dispatch_transaction_markers` returned: drop the written partitions,
    /// then react as `owner` does.
    fn finish_fan_out(&mut self, owner: Owner, snapshot: Ent, written: u8, failure: Option<i16>) {
        self.op = None;
        let written = topic_partitions(written);
        let mut prepared = snapshot.entry();
        let mut live = self.cache_entry();
        remove_marked_partitions(&mut live, &prepared, &written);
        remove_marked_partitions_from(&mut prepared, &written);
        self.set_cache(&live);
        let class = failure.map(|code| classify_marker_failure(&refused(code)));
        match owner {
            Owner::EndTxn => match fan_out_outcome(class) {
                MarkerFanOutOutcome::Complete => self.end_txn_phase3(&prepared),
                MarkerFanOutOutcome::Retry => {
                    self.queued = true;
                    self.answer_end(Some(Self::complete_epoch(&prepared)));
                }
                MarkerFanOutOutcome::GivenUp => {
                    self.answer_end(Some(Self::complete_epoch(&prepared)));
                }
            },
            // `dispatch_abort_markers` queues a failed fan-out.
            Owner::Reaper if class.is_some() => self.queued = true,
            Owner::Reaper => self.reaper_complete(&prepared),
            Owner::Completion => match class.map(fan_out_failure_attempt) {
                Some(CompletionAttempt::Retry) => self.queued = true,
                Some(CompletionAttempt::NothingToComplete | CompletionAttempt::Completed) => {}
                None => self.completion_complete(&prepared),
            },
        }
    }

    /// `EndTxn` Phase 3 after a complete fan-out.
    fn end_txn_phase3(&mut self, prepared: &TxnEntry) {
        let entry = self.cache_entry();
        let Some(PreparedCompletion { complete, .. }) = completion_for(prepared.state) else {
            self.seen |= SEEN_BROKEN;
            return;
        };
        let (completion_pid, completion_epoch) = completion_producer_identity(prepared);
        let expectation = CompletionExpectation {
            prepared: ProducerIdentity {
                producer_id: prepared.producer_id,
                producer_epoch: prepared.producer_epoch,
            },
            completion: ProducerIdentity {
                producer_id: completion_pid,
                producer_epoch: completion_epoch,
            },
            prepare: prepared.state,
            complete,
        };
        match decide_end_txn_completion(&entry, &expectation) {
            CompletionDecision::Proceed {
                next_state,
                response_pid,
                response_epoch,
            } => {
                let mut staged = entry;
                apply_completion(
                    &mut staged,
                    next_state,
                    (response_pid, response_epoch),
                    self.now_ms,
                );
                self.append(
                    Writer::EndComplete {
                        epoch: response_epoch,
                    },
                    &staged,
                );
            }
            CompletionDecision::AlreadyComplete { response_epoch, .. } => {
                self.answer_end(Some(response_epoch));
            }
            CompletionDecision::Reject(_) => self.answer_end(None),
        }
    }

    /// The reaper's `complete_abort`. Its `apply_complete_abort` is
    /// `apply_completion` to `CompleteAbort`, called here directly.
    fn reaper_complete(&mut self, prepared: &TxnEntry) {
        let entry = self.cache_entry();
        match complete_abort_decision(&entry, prepared) {
            TransactionReaperCompletionDecision::Proceed => {
                let mut staged = entry;
                let identity = completion_producer_identity(&staged);
                apply_completion(&mut staged, TxnState::CompleteAbort, identity, self.now_ms);
                self.append(Writer::ReaperComplete, &staged);
            }
            TransactionReaperCompletionDecision::AlreadyComplete
            | TransactionReaperCompletionDecision::RejectMalformed
            | TransactionReaperCompletionDecision::RejectStaleIdentity
            | TransactionReaperCompletionDecision::RejectChangedPreparedState => {}
        }
    }

    /// `complete_prepared_transaction` after a complete fan-out.
    fn completion_complete(&mut self, prepared: &TxnEntry) {
        let entry = self.cache_entry();
        let Some(PreparedCompletion { complete, .. }) = completion_for(prepared.state) else {
            return;
        };
        match completion_decision(&entry, prepared, (prepared.state, complete)) {
            TransactionReaperCompletionDecision::Proceed => {
                let mut staged = entry;
                let identity = completion_producer_identity(&staged);
                apply_completion(&mut staged, complete, identity, self.now_ms);
                self.append(Writer::Completion, &staged);
            }
            TransactionReaperCompletionDecision::AlreadyComplete
            | TransactionReaperCompletionDecision::RejectMalformed
            | TransactionReaperCompletionDecision::RejectStaleIdentity
            | TransactionReaperCompletionDecision::RejectChangedPreparedState => {}
        }
    }

    /// A marker of `commit` lands on `partition`. The first marker after the
    /// data decides what a `read_committed` consumer makes of it; a later one
    /// changes nothing there.
    fn land(&mut self, partition: u8, commit: bool) {
        let part = &mut self.parts[usize::from(partition)];
        if part.marker.is_none() {
            part.marker = Some(commit);
        }
    }

    fn converged(&self) -> bool {
        self.parts.iter().all(|part| {
            (!part.data || part.marker.is_some())
                && part.last_stable_offset() == part.high_watermark()
        })
    }
}

impl CompletionModel {
    fn faults_enabled(&self, s: &St) -> bool { !s.healed && s.faults < self.fault_budget }

    fn client_actions(s: &St, actions: &mut Vec<Act>) {
        if !s.client.alive || s.cache.is_none() {
            return;
        }
        let idle = s.op.is_none();
        for partition in 0..PARTITIONS {
            if s.client.end == End::NotSent {
                if idle && s.client.added & bit(partition) == 0 {
                    actions.push(Act::Add(partition));
                }
                if s.client.added & bit(partition) != 0 && s.client.produced & bit(partition) == 0 {
                    actions.push(Act::Produce(partition));
                }
            }
        }
        if !idle {
            return;
        }
        match s.client.end {
            End::NotSent if s.client.produced != 0 => {
                actions.push(Act::End(true));
                actions.push(Act::End(false));
            }
            End::Failed { commit, attempts } if attempts < MAX_END_ATTEMPTS => {
                actions.push(Act::End(commit));
            }
            End::NotSent | End::Failed { .. } | End::Waiting { .. } | End::Done => {}
        }
    }

    fn add(s: &mut St, partition: u8) -> Option<()> {
        let entry = s.cache_entry();
        let requested = [topic_partition(partition)];
        match registration_decision(&entry, TID, (ProducerId(PID), s.client.epoch), &requested) {
            TransactionRegistrationDecision::PersistRetry => s.client.added |= bit(partition),
            TransactionRegistrationDecision::PersistRegistration => {
                let staged =
                    stage_registration(&entry, requested, s.now_ms, TxnVersion::Verified.level());
                s.append(Writer::Add(partition), &staged);
            }
            TransactionRegistrationDecision::RejectProducerEpoch => s.client.alive = false,
            // `CONCURRENT_TRANSACTIONS` and the rest: the producer retries
            // later, against a coordinator that has not changed.
            TransactionRegistrationDecision::RejectNotCoordinator
            | TransactionRegistrationDecision::RejectUnknownProducer
            | TransactionRegistrationDecision::RejectProducerId
            | TransactionRegistrationDecision::RejectPendingTransition
            | TransactionRegistrationDecision::RejectState => return None,
        }
        Some(())
    }

    /// A produce at `TV_2`: the leader verifies the partition with the
    /// coordinator (`verification_code`), then appends the batch, which the
    /// partition's producer state refuses after a marker.
    fn produce(s: &mut St, partition: u8) -> Option<()> {
        let cache = s.cache?;
        let code = verification_code(
            HeldTransaction {
                identity: ProducerIdentity {
                    producer_id: ProducerId(PID),
                    producer_epoch: cache.epoch,
                },
                state: cache.state(),
            },
            cache.parts & bit(partition) != 0,
            ProducerIdentity {
                producer_id: ProducerId(PID),
                producer_epoch: s.client.epoch,
            },
        );
        let part = &mut s.parts[usize::from(partition)];
        if code == codes::PRODUCER_FENCED {
            s.client.alive = false;
        } else if code == codes::NONE && part.marker.is_none() {
            part.data = true;
            s.client.produced |= bit(partition);
        } else {
            return None;
        }
        Some(())
    }

    /// `EndTxn` Phases 1: the state table, then `prepare_transaction`.
    fn end(s: &mut St, commit: bool) -> Option<()> {
        let attempts = match s.client.end {
            End::NotSent => 1,
            End::Failed { attempts, .. } => attempts + 1,
            End::Waiting { .. } | End::Done => return None,
        };
        s.client.end = End::Waiting { commit, attempts };
        let entry = s.cache_entry();
        let decision = end_txn_decision(
            &entry,
            ProducerIdentity {
                producer_id: ProducerId(PID),
                producer_epoch: s.client.epoch,
            },
            commit,
            TableRules {
                verified: true,
                trunk_rules: false,
            },
        );
        match decision {
            EndTxnDecision::Prepare {
                no_partition_added, ..
            } => {
                let mut staged = entry;
                if no_partition_added {
                    staged.partitions.clear();
                    staged.start_ms = s.now_ms;
                    apply_end_txn_prepare(&mut staged, commit);
                } else if decide_phase1_transition(&mut staged, commit).is_err() {
                    s.answer_end(None);
                    return Some(());
                }
                prepare_completion_identities_with_fresh(&mut staged, TxnVersion::Verified, None)
                    .expect("model epochs never reach the rotation boundary");
                staged.last_update_ms = s.now_ms;
                staged.client_transaction_version = TxnVersion::Verified.level();
                s.append(Writer::EndPrepare, &staged);
            }
            EndTxnDecision::AlreadyComplete => {
                s.answer_end(Some(client_producer_identity(&entry).1));
            }
            EndTxnDecision::Refuse(code)
                if code == codes::PRODUCER_FENCED || code == codes::INVALID_PRODUCER_ID_MAPPING =>
            {
                s.client.end = End::Done;
                s.client.alive = false;
            }
            EndTxnDecision::Refuse(_) => s.answer_end(None),
        }
        Some(())
    }

    /// One sweep of the reaper over the one transactional id.
    fn reaper_tick(s: &mut St) -> Option<()> {
        let entry = s.cache_entry();
        match reaper_step(&entry, s.now_ms) {
            ReaperStep::Skip => return None,
            ReaperStep::ResumeAbort => s.start_fan_out(Owner::Reaper, s.cache?),
            ReaperStep::PrepareAbort => {
                let mut staged = entry;
                apply_prepare_abort(&mut staged, s.now_ms);
                prepare_server_abort_identities_with_fresh(&mut staged, TxnVersion::Verified, None)
                    .expect("model epochs never reach the rotation boundary");
                s.append(Writer::ReaperPrepare, &staged);
            }
        }
        Some(())
    }

    /// The completion task takes the queued id and tries it once.
    fn completion_run(s: &mut St) {
        s.queued = false;
        let Some(cache) = s.cache else {
            return;
        };
        if completion_for(cache.state()).is_some() {
            s.start_fan_out(Owner::Completion, cache);
        }
    }

    fn marker(s: &mut St, partition: u8, result: Res) -> Option<()> {
        let Some(Op::FanOut {
            owner,
            snapshot,
            untried,
            mut written,
            mut failure,
        }) = s.op
        else {
            return None;
        };
        let commit = completion_for(snapshot.state())?.marker == MarkerType::Commit;
        match result {
            Res::Written => {
                s.land(partition, commit);
                written |= bit(partition);
            }
            Res::MovedLanded | Res::MovedLost => {
                if result == Res::MovedLanded {
                    s.land(partition, commit);
                }
                s.parts[usize::from(partition)].moved = false;
                failure = deciding(failure, codes::NOT_LEADER_OR_FOLLOWER);
            }
            Res::Refused => {
                s.faults += 1;
                s.seen |= SEEN_REFUSED;
                failure = deciding(failure, codes::UNKNOWN_SERVER_ERROR);
            }
        }
        let untried = untried & !bit(partition);
        if untried == 0 {
            s.finish_fan_out(owner, snapshot, written, failure);
        } else {
            s.op = Some(Op::FanOut {
                owner,
                snapshot,
                untried,
                written,
                failure,
            });
        }
        Some(())
    }

    fn failover(s: &mut St, keep_tail: bool) {
        s.faults += 1;
        let client_waits = matches!(
            s.op,
            Some(
                Op::Persist {
                    writer: Writer::EndPrepare | Writer::EndComplete { .. },
                    ..
                } | Op::FanOut {
                    owner: Owner::EndTxn,
                    ..
                }
            )
        );
        if client_waits {
            // The old coordinator answers the request `NOT_COORDINATOR`.
            s.answer_end(None);
        }
        s.op = None;
        s.cache = None;
        s.queued = false;
        if !keep_tail {
            s.tail.clear();
        }
    }

    /// The new coordinator's load: `install_recovered` publishes the replayed
    /// entry and queues it when `completion_for` says it is prepared.
    fn load(s: &mut St) {
        let record = s.log_end_record();
        s.cache = Some(record);
        if completion_for(record.state()).is_some() {
            s.queued = true;
        }
    }
}

impl Model for CompletionModel {
    type Action = Act;
    type State = St;

    fn init_states(&self) -> Vec<St> {
        let initial = Ent::initial();
        vec![St {
            client: Client {
                epoch: initial.epoch,
                added: 0,
                produced: 0,
                end: End::NotSent,
                alive: true,
            },
            cache: Some(initial),
            committed: initial,
            tail: Vec::new(),
            stalled: false,
            queued: false,
            op: None,
            now_ms: 0,
            parts: [Part::default(); PARTITIONS as usize],
            faults: 0,
            healed: false,
            seen: 0,
        }]
    }

    fn actions(&self, s: &St, actions: &mut Vec<Act>) {
        Self::client_actions(s, actions);
        if !s.stalled && !s.tail.is_empty() {
            actions.push(Act::Commit);
        }
        let waiting = matches!(s.op, Some(Op::Persist { .. }));
        if s.stalled && waiting {
            actions.push(Act::TimedOut);
        }
        if s.op.is_none() {
            if s.cache.is_none() {
                actions.push(Act::Load);
            } else {
                actions.push(Act::ReaperTick);
            }
            if s.queued {
                actions.push(Act::CompletionRun);
            }
        }
        if let Some(cache) = s.cache
            && cache.state() == TxnState::Ongoing
            && !should_abort_idle_txn(cache.state(), TIMEOUT_MS, cache.start_ms, s.now_ms)
        {
            actions.push(Act::Tick);
        }
        let faults = self.faults_enabled(s);
        if let Some(Op::FanOut { untried, .. }) = s.op {
            for partition in 0..PARTITIONS {
                if untried & bit(partition) == 0 {
                    continue;
                }
                if s.parts[usize::from(partition)].moved {
                    actions.push(Act::Marker(partition, Res::MovedLanded));
                    actions.push(Act::Marker(partition, Res::MovedLost));
                } else {
                    actions.push(Act::Marker(partition, Res::Written));
                    if faults && self.refusals {
                        actions.push(Act::Marker(partition, Res::Refused));
                    }
                }
            }
        }
        if faults {
            if !s.stalled {
                actions.push(Act::Stall);
            }
            actions.push(Act::Failover(true));
            if !s.tail.is_empty() {
                actions.push(Act::Failover(false));
            }
            for partition in 0..PARTITIONS {
                if !s.parts[usize::from(partition)].moved {
                    actions.push(Act::MoveLeader(partition));
                }
            }
            if s.client.alive && s.client.end != End::Done {
                actions.push(Act::Abandon);
            }
        }
        if !s.healed {
            actions.push(Act::Heal);
        }
    }

    fn next_state(&self, last: &St, action: Act) -> Option<St> {
        let mut s = last.clone();
        match action {
            Act::Add(partition) => Self::add(&mut s, partition)?,
            Act::Produce(partition) => Self::produce(&mut s, partition)?,
            Act::End(commit) => Self::end(&mut s, commit)?,
            Act::Commit => {
                s.committed = s.log_end_record();
                s.tail.clear();
                if let Some(Op::Persist { writer, record }) = s.op {
                    s.resume(writer, record, true);
                }
            }
            Act::TimedOut => {
                let Some(Op::Persist { writer, record }) = s.op else {
                    return None;
                };
                s.resume(writer, record, false);
            }
            Act::Load => Self::load(&mut s),
            Act::ReaperTick => Self::reaper_tick(&mut s)?,
            Act::CompletionRun => Self::completion_run(&mut s),
            Act::Marker(partition, result) => Self::marker(&mut s, partition, result)?,
            Act::Tick => {
                let cache = s.cache?;
                s.now_ms = cache.start_ms + i64::from(TIMEOUT_MS) + 1;
            }
            Act::Stall => {
                s.faults += 1;
                s.stalled = true;
            }
            Act::Failover(keep_tail) => Self::failover(&mut s, keep_tail),
            Act::MoveLeader(partition) => {
                s.faults += 1;
                s.parts[usize::from(partition)].moved = true;
            }
            Act::Abandon => {
                s.faults += 1;
                s.client.alive = false;
            }
            Act::Heal => {
                s.healed = true;
                s.stalled = false;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            // HEADLINE: once the faults end and nothing but a new fault could
            // move the system, every partition that holds the transaction's
            // data has its marker, and its last stable offset is its high
            // watermark.
            Property::<Self>::always(CONVERGED, |model: &Self, s: &St| {
                !(s.healed && model.quiescent(s)) || s.converged()
            }),
            // Every partition's first marker ends the one transaction the
            // same way: never committed on one partition and aborted on
            // another.
            Property::<Self>::always(ONE_OUTCOME, |_, s: &St| {
                let mut outcomes = s.parts.iter().filter_map(|part| part.marker);
                let first = outcomes.next();
                outcomes.all(|outcome| Some(outcome) == first)
            }),
            // Every entry the production kernels returned fits the model's
            // projection, so no transition dropped a field.
            Property::<Self>::always("projection_exact", |_, s: &St| s.seen & SEEN_BROKEN == 0),
            // ----- non-vacuity witnesses -----
            // An append whose caller saw an error is still in the log.
            Property::<Self>::sometimes("indeterminate_persist", |_, s: &St| {
                s.op.is_none() && !s.tail.is_empty()
            }),
            // The log committed a record the coordinator's cache never took.
            Property::<Self>::sometimes("log_ahead_of_cache", |_, s: &St| {
                s.op.is_none() && s.tail.is_empty() && s.cache.is_some_and(|c| c != s.committed)
            }),
            // A coordinator reloads a prepared transaction from the log.
            Property::<Self>::sometimes("reload_of_a_prepared_transaction", |_, s: &St| {
                s.cache.is_none() && completion_for(s.log_end_record().state()).is_some()
            }),
            Property::<Self>::sometimes("reaper_abort", |_, s: &St| {
                s.seen & SEEN_REAPER_ABORT != 0
            }),
            // An old leader answers a marker `NOT_LEADER_OR_FOLLOWER`.
            Property::<Self>::sometimes("moved_marker_leader", |_, s: &St| {
                s.parts.iter().any(|part| part.moved) && matches!(s.op, Some(Op::FanOut { .. }))
            }),
            // The property is not vacuous: healed runs do converge.
            Property::<Self>::sometimes("converged_after_heal", |model: &Self, s: &St| {
                s.healed && model.quiescent(s) && s.converged() && s.parts[0].data
            }),
        ];
        if self.refusals {
            properties.push(Property::<Self>::sometimes(
                "fenced_or_unexpected_marker",
                |_, s: &St| s.seen & SEEN_REFUSED != 0,
            ));
        }
        properties
    }
}

impl Quiescence for CompletionModel {
    fn is_fault(&self, action: &Act) -> bool {
        matches!(
            action,
            Act::Marker(_, Res::Refused)
                | Act::Stall
                | Act::Failover(_)
                | Act::MoveLeader(_)
                | Act::Abandon
                | Act::Heal
        )
    }
}

/// A marker leader that refuses a `PrepareCommit`'s marker strands it: the
/// shortest path to a transaction that never ends. The atomicity failure
/// below is reachable here too.
#[test]
fn a_refused_marker_strands_a_prepared_commit() {
    check_model_with_known_failures(
        CompletionModel {
            fault_budget: 2,
            refusals: true,
        },
        "txn_completion",
        (MAX_DEPTH, TARGET_STATE_COUNT, MAX_UNIQUE_STATES),
        PINNED_UNIQUE_STATES_REFUSALS,
        &[
            KnownFailure {
                property: CONVERGED,
                depth: CONVERGED_DEPTH_REFUSALS,
            },
            KnownFailure {
                property: ONE_OUTCOME,
                depth: ONE_OUTCOME_DEPTH,
            },
        ],
    );
}

/// With every marker leader answering, a coordinator that loads an
/// uncommitted registration, and then loses it to a second failover, still
/// leaves a transaction open for good, and an uncommitted `Prepare*` that a
/// load completes can still split the outcome.
#[test]
fn a_load_of_an_uncommitted_record_loses_the_transaction() {
    check_model_with_known_failures(
        CompletionModel {
            fault_budget: 2,
            refusals: false,
        },
        "txn_completion_no_refusals",
        (MAX_DEPTH, TARGET_STATE_COUNT, MAX_UNIQUE_STATES),
        PINNED_UNIQUE_STATES_NO_REFUSALS,
        &[
            KnownFailure {
                property: CONVERGED,
                depth: CONVERGED_DEPTH_NO_REFUSALS,
            },
            KnownFailure {
                property: ONE_OUTCOME,
                depth: ONE_OUTCOME_DEPTH,
            },
        ],
    );
}
