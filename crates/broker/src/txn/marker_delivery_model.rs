//! Exhaustive stateright model of transaction-marker delivery under faults.
//!
//! The model runs one transactional id over two data partitions through every
//! interleaving of a transaction-version-2 producer, the coordinator's
//! timeout reaper and completion task, the partition leaders, and a bounded
//! set of faults: a `__transaction_state` write that commits, or fails its
//! commit wait and lands later, or is lost when the coordinator crashes; a
//! coordinator crash and the reload of its state partition; a partition that
//! answers a marker with a retriable code while it is short of replicas or
//! changes leader; and a partition that answers a code Kafka's
//! `TransactionMarkerRequestCompletionHandler` throws on. A `Heal` action ends
//! the faults: every later write commits and every later marker reaches its
//! leader, except at a partition that answered an unexpected code, which keeps
//! answering it.
//!
//! Headline liveness, as a safety property over quiescent states,
//! `every_marker_delivered`: **once the cluster has healed and no background
//! action can make progress, every partition holding a transaction's data has
//! that transaction's marker**, so its last stable offset has reached its high
//! watermark. The partition that answered an unexpected code is the one
//! exception, as in Kafka, where its transaction waits for the coordinator's
//! next load. "Background" is everything that runs without the client: the
//! reaper, the completion task, a coordinator reload, the late commit of a
//! write, and a partition leader finishing an in-flight `Produce`. The client
//! is free to stop at any point, so an abandoned transaction must still close.
//!
//! Jepsen's `subscribe-txn-kill` found a commit whose fan-out met an
//! unexpected code on one partition while another was short of replicas. The
//! attempt gave up, nothing retries a `PrepareCommit`, and the second
//! partition kept its batch open for good. Restoring the old failure order in
//! `record_deciding_failure` (an unexpected code over a retriable one) makes
//! `every_marker_delivered` fail on that trace.
//!
//! Secondary safety, `outcome_is_atomic`: the markers that close one
//! generation's data on the two partitions are all COMMIT or all ABORT.
//!
//! What is DRIVEN (production code on every transition):
//!
//! - `EndTxn` Phase 1: `decide_phase1_transition` and
//!   `prepare_completion_identities_with_fresh`.
//! - The reaper: `should_abort_idle_txn` and
//!   `prepare_server_abort_identities_with_fresh`.
//! - The fan-out: `completion_for`, the per-attempt `MarkerFanOut` that keeps
//!   the deciding failure, `classify_marker_failure` and its `stops_fan_out`,
//!   then `completion_decision`, `completion_producer_identity` and
//!   `apply_completion` for the `Complete*` record.
//! - The partition leader's marker check: `transaction_marker_equal_epoch_fenced`
//!   and `transaction_marker_materialization_decision`.
//! - Registration's state gate `TxnState::can_transition_to(Ongoing)` and the
//!   recorded version `TxnVersion::for_add_partitions_to_txn`.
//!
//! What is MODELED (hand-written, mirroring the handlers):
//!
//! - The `__transaction_state` log keeps its last committed record and its
//!   last uncommitted one. A reload reads the newest record the new leader
//!   holds, as Kafka's `loadTransactionMetadata` reads to the log end, and
//!   queues a `Prepare*` transaction for completion.
//! - Registration from `Produce` v12: the epoch check, the state gate, the
//!   `PersistRetry` shortcut for a partition already in the transaction, and
//!   the leader's append after it, which refuses a batch below the
//!   partition's producer epoch (`StaleProducerEpoch`).
//! - The reaper's sweep prepares the abort of a timed-out transaction and
//!   queues it for completion, and queues an outstanding `PrepareAbort` again;
//!   it never fans markers out itself. `EndTxn`'s inline fan-out is the
//!   completion task's first attempt.
//! - The client learns the entry's epoch once its transaction ended, which
//!   stands for an `EndTxn` retry or an `InitProducerId`.
//!
//! Bounds: at most one partition ever answers an unexpected code, and the
//! producer epoch stops at 2, which leaves room for two transactions. `marker_delivery_basic`
//! allows one failed commit wait and one coordinator crash, and
//! `marker_delivery_wide` two of each.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` and `target_state_count`.

use {
    super::{
        coordinator::{
            completion::{
                PreparedCompletion, apply_completion, completion_decision, completion_for,
            },
            produce_verification::INTERNAL_REGISTRATION_VERSION,
        },
        decision::decide_phase1_transition,
        handlers::end_txn::{
            MarkerFanOut, completion_producer_identity, prepare_completion_identities_with_fresh,
            prepare_server_abort_identities_with_fresh,
        },
        marker::{MarkerType, classify_marker_failure},
        state::{TopicPartition, TxnEntry, TxnState},
        two_pc::should_abort_idle_txn,
        version::TxnVersion,
    },
    crate::{codes, error::BrokerError, model_check::check_model},
    krabka_ids::PartitionIndex,
    krabka_log::ProducerId,
    krabka_verified::transaction::{
        TransactionMarkerMaterializationDecision as MarkerDecision,
        TransactionMarkerPartitionState, TransactionMarkerRequest,
        TransactionReaperCompletionDecision, transaction_marker_equal_epoch_fenced,
    },
    stateright::{Model, Property},
    std::collections::{BTreeMap, BTreeSet},
};

const MAX_STATES: usize = 4_000_000;
const MAX_DEPTH: usize = 60;
const MAX_UNIQUE: usize = 4_000_000;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set into a failure instead of a silently
// smaller search that still passes the upper bound. The *generated* count is
// not pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_BASIC: usize = 67_094;
const PINNED_UNIQUE_STATES_WIDE: usize = 362_541;

const PID: ProducerId = ProducerId(1000);
/// The Jepsen workload's `transaction.timeout.ms`.
const TIMEOUT_MS: i32 = 1_000;
const TOPIC: &str = "t";
const PARTITIONS: usize = 2;

struct MarkerDeliveryModel {
    max_epoch: i16,
    /// Writes that may fail their commit wait.
    timeouts: u8,
    /// Coordinator crashes.
    crashes: u8,
}

/// A `__transaction_state` record, as the fields the model's decisions read.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Rec {
    state: i8, // TxnState::to_kafka_status()
    epoch: i16,
    last_epoch: i16,
    /// Bit `i` set: data partition `i` is in the transaction.
    partitions: u8,
    version: i16,
}

/// What a data partition's leader holds for the model's producer.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Part {
    producer_epoch: i16,
    coordinator_epoch: i32,
    /// The epoch of the open transaction's data, when one is open.
    open: Option<i16>,
    /// The leader answered a code Kafka's completion handler throws on, and
    /// keeps answering it.
    poisoned: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Violation {
    /// The markers of one generation's data differ between partitions.
    MixedOutcome,
    /// A partition leader found its own state malformed.
    Malformed,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Witness {
    CommitMarker,
    AbortMarker,
    /// A marker reached a partition from a reloaded coordinator.
    MarkerAfterReload,
    /// An attempt in which one partition answered an unexpected code and
    /// another a retriable one stayed queued.
    RetriedPastUnexpected,
    /// A partition got its marker while the other partition of the
    /// transaction was poisoned and still outstanding.
    ClosedBesidePoisoned,
    /// A leader refused an in-flight batch below the partition's epoch.
    StaleAppendRefused,
    /// The coordinator's cache and its durable record differed after the
    /// heal.
    StaleCacheHealed,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct MdState {
    /// The newest committed `__transaction_state` record.
    durable: Rec,
    /// A newer record whose commit wait failed. It commits later, or is lost
    /// when the coordinator crashes.
    tail: Option<Rec>,
    /// The coordinator's in-memory entry, `None` while it is down.
    cache: Option<Rec>,
    coordinator_epoch: i32,
    /// The completion task holds the transactional id.
    queued: bool,
    parts: [Part; PARTITIONS],
    /// Registered partitions whose batch the leader has not appended yet.
    in_flight: u8,
    in_flight_epoch: i16,
    client_epoch: i16,
    healed: bool,
    timeouts_left: u8,
    crashes_left: u8,
    unexpected_left: u8,
    /// Ghost: the marker type that closed each generation's data.
    outcomes: BTreeMap<i16, bool>,
    violations: BTreeSet<Violation>,
    witnesses: BTreeSet<Witness>,
}

/// How a `__transaction_state` write ends.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Persist {
    Committed,
    /// The commit wait failed; the record stays in the log.
    TimedOut,
}

/// What a partition's leader does with a marker.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Answer {
    /// The marker reaches the leader, which decides it.
    Reach,
    /// `NOT_ENOUGH_REPLICAS`, a leader change, or an unreachable leader.
    Retriable,
    /// A code Kafka's completion handler throws on.
    Unexpected,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum MdAction {
    /// Client: a `Produce` v12 registers partition `i` with the coordinator.
    Register(usize, Persist),
    /// Client: `EndTxn(committed)` Phase 1.
    EndTxn(bool, Persist),
    /// Client: learns the entry's epoch after its transaction ended.
    Resync,
    /// Background: the leader of partition `i` appends the registered batch.
    Append(usize),
    /// Background: a reaper sweep prepares the abort of a timed-out
    /// transaction, or queues an outstanding one again.
    Reap(Persist),
    /// Background: one completion attempt, a marker fan-out and then the
    /// `Complete*` write.
    Dispatch([Answer; PARTITIONS], Persist),
    /// Background: the uncommitted record commits.
    LateCommit,
    /// Background: a new coordinator loads the state partition.
    Reload,
    /// Fault: the coordinator dies, keeping or losing its uncommitted record.
    Crash(bool),
    /// The faults end.
    Heal,
}

impl MdAction {
    fn is_background(self) -> bool {
        matches!(
            self,
            Self::Append(_) | Self::Reap(_) | Self::Dispatch(..) | Self::LateCommit | Self::Reload
        )
    }
}

fn st(id: i8) -> TxnState { TxnState::from_kafka_status(id).expect("valid TxnState id in model") }

fn tp(index: usize) -> TopicPartition {
    TopicPartition {
        topic: TOPIC.to_owned(),
        partition: PartitionIndex(i32::try_from(index).expect("model partition index")),
    }
}

fn bit(index: usize) -> u8 { 1 << index }

/// Rebuilds the real entry, with fixed clocks, so the production decisions
/// behave as in a live run and two equal records rebuild equal entries.
fn rebuild(rec: Rec) -> TxnEntry {
    let mut entry = TxnEntry::new_empty("tid".to_owned(), PID, rec.epoch, TIMEOUT_MS, 0);
    entry.state = st(rec.state);
    entry.last_producer_epoch = rec.last_epoch;
    entry.client_transaction_version = rec.version;
    entry.start_ms = 0;
    entry.partitions = (0..PARTITIONS)
        .filter(|index| rec.partitions & bit(*index) != 0)
        .map(tp)
        .collect();
    entry
}

fn project(entry: &TxnEntry) -> Rec {
    Rec {
        state: entry.state.to_kafka_status(),
        epoch: entry.producer_epoch,
        last_epoch: entry.last_producer_epoch,
        partitions: (0..PARTITIONS)
            .filter(|index| entry.partitions.contains(&tp(*index)))
            .fold(0, |bits, index| bits | bit(index)),
        version: entry.client_transaction_version,
    }
}

fn refused(code: i16) -> BrokerError {
    BrokerError::MarkerWriteRefused {
        code,
        message: format!("model code {code}"),
    }
}

/// Appends `rec` as the coordinator does, or refuses the action when `how` is
/// not possible. Returns whether the write committed.
fn persist(s: &mut MdState, rec: Rec, how: Persist) -> Option<bool> {
    match how {
        Persist::Committed => {
            s.durable = rec;
            s.tail = None;
            s.cache = Some(rec);
            Some(true)
        }
        Persist::TimedOut => {
            if s.healed || s.timeouts_left == 0 {
                return None;
            }
            s.timeouts_left -= 1;
            s.tail = Some(rec);
            Some(false)
        }
    }
}

/// The part of the state a background action has to change to count as
/// progress. The completion queue flag is left out: a reaper sweep that
/// queues a transaction the completion task then drops again is the benign
/// cycle of a partition that keeps answering an unexpected code.
fn progress(s: &MdState) -> impl PartialEq + use<> {
    (
        s.durable,
        s.tail,
        s.cache,
        s.coordinator_epoch,
        s.parts,
        s.in_flight,
    )
}

/// Whether no background action can make progress from `s`.
fn quiescent(model: &MarkerDeliveryModel, s: &MdState) -> bool {
    let mut actions = Vec::new();
    model.actions(s, &mut actions);
    let here = progress(s);
    actions
        .into_iter()
        .filter(|action| action.is_background())
        .filter_map(|action| model.next_state(s, action))
        .all(|next| progress(&next) == here)
}

impl MarkerDeliveryModel {
    fn register(s: &mut MdState, index: usize, how: Persist) -> Option<()> {
        let cache = s.cache?;
        if s.in_flight != 0 {
            return None;
        }
        let mut entry = rebuild(cache);
        if entry.producer_epoch != s.client_epoch {
            return None;
        }
        if entry.state == TxnState::Ongoing && entry.partitions.contains(&tp(index)) {
            if how != Persist::Committed {
                return None;
            }
        } else {
            if !entry.state.can_transition_to(TxnState::Ongoing) {
                return None;
            }
            if matches!(
                entry.state,
                TxnState::CompleteCommit | TxnState::CompleteAbort
            ) {
                entry.partitions.clear();
            }
            entry.state = TxnState::Ongoing;
            entry.partitions.insert(tp(index));
            entry.client_transaction_version =
                TxnVersion::for_add_partitions_to_txn(INTERNAL_REGISTRATION_VERSION).level();
            if !persist(s, project(&entry), how)? {
                return Some(());
            }
        }
        s.in_flight = bit(index);
        s.in_flight_epoch = s.client_epoch;
        Some(())
    }

    fn end_txn(s: &mut MdState, committed: bool, how: Persist) -> Option<()> {
        let cache = s.cache?;
        let mut entry = rebuild(cache);
        if entry.producer_epoch != s.client_epoch || s.in_flight != 0 {
            return None;
        }
        decide_phase1_transition(&mut entry, committed).ok()?;
        prepare_completion_identities_with_fresh(&mut entry, TxnVersion::Verified, None)
            .expect("model epochs never reach the rotation boundary");
        entry.client_transaction_version = TxnVersion::Verified.level();
        if persist(s, project(&entry), how)? {
            s.queued = true;
            s.client_epoch = completion_producer_identity(&entry).1;
        }
        Some(())
    }

    fn resync(s: &mut MdState) -> Option<()> {
        let cache = s.cache?;
        let ended = matches!(
            st(cache.state),
            TxnState::Empty | TxnState::CompleteCommit | TxnState::CompleteAbort
        );
        if !ended || cache.epoch == s.client_epoch {
            return None;
        }
        s.client_epoch = cache.epoch;
        Some(())
    }

    fn append(s: &mut MdState, index: usize) -> Option<()> {
        if s.in_flight & bit(index) == 0 {
            return None;
        }
        s.in_flight &= !bit(index);
        let epoch = s.in_flight_epoch;
        let part = &mut s.parts[index];
        if epoch < part.producer_epoch {
            s.witnesses.insert(Witness::StaleAppendRefused);
            return Some(());
        }
        part.open.get_or_insert(epoch);
        part.producer_epoch = epoch;
        Some(())
    }

    /// One reaper sweep over the transactional id: `prepare_abort`, then the
    /// hand-off to the completion task.
    fn reap(s: &mut MdState, how: Persist) -> Option<()> {
        let cache = s.cache?;
        let mut entry = rebuild(cache);
        if entry.state == TxnState::PrepareAbort {
            // An abort still outstanding is queued again.
            if how != Persist::Committed {
                return None;
            }
            s.queued = true;
            return Some(());
        }
        // The transaction started at 0 and the sweep runs past its timeout.
        let now_ms = i64::from(TIMEOUT_MS) + 1;
        if !should_abort_idle_txn(entry.state, entry.txn_timeout_ms, entry.start_ms, now_ms) {
            return None;
        }
        entry.state = TxnState::PrepareAbort;
        prepare_server_abort_identities_with_fresh(&mut entry, TxnVersion::Verified, None)
            .expect("model epochs never reach the rotation boundary");
        if persist(s, project(&entry), how)? {
            s.queued = true;
        }
        Some(())
    }

    /// The leader of partition `index` decides `marker`, and appends it when
    /// it may. Returns the failure it answers, if any.
    fn deliver(
        s: &mut MdState,
        index: usize,
        prepared: &TxnEntry,
        marker: MarkerType,
    ) -> Option<BrokerError> {
        let coordinator_epoch = s.coordinator_epoch;
        let part = s.parts[index];
        if transaction_marker_equal_epoch_fenced(
            prepared.client_transaction_version,
            prepared.producer_epoch,
            part.producer_epoch,
            part.open.is_some(),
        ) {
            return Some(refused(codes::INVALID_PRODUCER_EPOCH));
        }
        let decision = krabka_verified::transaction_marker_materialization_decision(
            TransactionMarkerRequest {
                producer_id: PID.get(),
                producer_epoch: prepared.producer_epoch,
                coordinator_epoch,
                is_commit: marker == MarkerType::Commit,
                is_offsets_partition: false,
            },
            TransactionMarkerPartitionState {
                producer_epoch: part.producer_epoch,
                coordinator_epoch: part.coordinator_epoch,
                has_pending_transaction: part.open.is_some(),
            },
        );
        match decision {
            MarkerDecision::RejectMalformed => {
                s.violations.insert(Violation::Malformed);
                Some(refused(codes::UNKNOWN_SERVER_ERROR))
            }
            MarkerDecision::RejectProducerEpoch => Some(refused(codes::INVALID_PRODUCER_EPOCH)),
            MarkerDecision::RejectCoordinatorEpoch => {
                Some(refused(codes::TRANSACTION_COORDINATOR_FENCED))
            }
            MarkerDecision::Retry => None,
            MarkerDecision::AppendWithoutOffsetPublication
            | MarkerDecision::AppendAndPublishOffsets => {
                let commit = marker == MarkerType::Commit;
                if let Some(generation) = s.parts[index].open
                    && *s.outcomes.entry(generation).or_insert(commit) != commit
                {
                    s.violations.insert(Violation::MixedOutcome);
                }
                s.witnesses.insert(if commit {
                    Witness::CommitMarker
                } else {
                    Witness::AbortMarker
                });
                if coordinator_epoch > 0 {
                    s.witnesses.insert(Witness::MarkerAfterReload);
                }
                let part = &mut s.parts[index];
                part.open = None;
                part.producer_epoch = prepared.producer_epoch;
                part.coordinator_epoch = coordinator_epoch;
                None
            }
        }
    }

    /// Checks that `answers` is one the faults allow, and poisons a partition
    /// that answers an unexpected code for the first time.
    fn admit_answers(
        s: &mut MdState,
        outstanding: u8,
        answers: [Answer; PARTITIONS],
    ) -> Option<()> {
        for (index, answer) in answers.into_iter().enumerate() {
            if outstanding & bit(index) == 0 {
                if answer != Answer::Reach {
                    return None;
                }
                continue;
            }
            let poisoned = s.parts[index].poisoned;
            // A poisoned partition answers only its unexpected code; the
            // healed cluster answers no retriable one and poisons nothing.
            let allowed = match answer {
                Answer::Reach => !poisoned,
                Answer::Retriable => !poisoned && !s.healed,
                Answer::Unexpected => poisoned || (!s.healed && s.unexpected_left > 0),
            };
            if !allowed {
                return None;
            }
            if answer == Answer::Unexpected && !poisoned {
                s.unexpected_left -= 1;
                s.parts[index].poisoned = true;
            }
        }
        Some(())
    }

    fn dispatch(s: &mut MdState, answers: [Answer; PARTITIONS], how: Persist) -> Option<()> {
        if !s.queued {
            return None;
        }
        let cache = s.cache?;
        let prepared = rebuild(cache);
        let Some(PreparedCompletion { marker, complete }) = completion_for(prepared.state) else {
            // The completion task finds nothing to complete and drops the id.
            if answers == [Answer::Reach; PARTITIONS] && how == Persist::Committed {
                s.queued = false;
                return Some(());
            }
            return None;
        };
        Self::admit_answers(s, cache.partitions, answers)?;
        let mut fan_out = MarkerFanOut::default();
        let mut retriable = false;
        let mut unexpected = false;
        for (index, answer) in answers.into_iter().enumerate() {
            if cache.partitions & bit(index) == 0 {
                continue;
            }
            let failure = match answer {
                Answer::Retriable => {
                    retriable = true;
                    Some(refused(codes::NOT_ENOUGH_REPLICAS))
                }
                Answer::Unexpected => {
                    unexpected = true;
                    Some(refused(codes::UNKNOWN_SERVER_ERROR))
                }
                Answer::Reach => Self::deliver(s, index, &prepared, marker),
            };
            match failure {
                Some(error) => fan_out.fail(error),
                None => fan_out.written.push(tp(index)),
            }
        }
        // `remove_marked_partitions`: the cache drops each written partition.
        let mut live = prepared.clone();
        for written in &fan_out.written {
            live.partitions.remove(written);
        }
        if live.partitions.len() < prepared.partitions.len()
            && (0..PARTITIONS)
                .any(|index| live.partitions.contains(&tp(index)) && s.parts[index].poisoned)
        {
            s.witnesses.insert(Witness::ClosedBesidePoisoned);
        }
        s.cache = Some(project(&live));
        if let Some(error) = fan_out.failure {
            if how != Persist::Committed {
                return None;
            }
            if classify_marker_failure(&error).stops_fan_out() {
                s.queued = false;
            } else if retriable && unexpected {
                s.witnesses.insert(Witness::RetriedPastUnexpected);
            }
            return Some(());
        }
        let decision = completion_decision(&live, &live, (live.state, complete));
        if decision != TransactionReaperCompletionDecision::Proceed {
            return None;
        }
        let mut completed = live;
        let identity = completion_producer_identity(&completed);
        apply_completion(&mut completed, complete, identity, 0);
        if persist(s, project(&completed), how)? {
            s.queued = false;
        }
        Some(())
    }

    fn reload(s: &mut MdState) -> Option<()> {
        if s.cache.is_some() {
            return None;
        }
        // The new leader's log holds the uncommitted record, and its term
        // commits it.
        if let Some(tail) = s.tail.take() {
            s.durable = tail;
        }
        s.cache = Some(s.durable);
        s.coordinator_epoch += 1;
        s.queued = completion_for(st(s.durable.state)).is_some();
        Some(())
    }

    fn crash(s: &mut MdState, keep: bool) -> Option<()> {
        if s.healed || s.crashes_left == 0 || s.cache.is_none() {
            return None;
        }
        s.crashes_left -= 1;
        s.cache = None;
        s.queued = false;
        if !keep {
            s.tail = None;
        }
        Some(())
    }

    fn heal(s: &mut MdState) -> Option<()> {
        if s.healed {
            return None;
        }
        s.healed = true;
        if let Some(tail) = s.tail.take() {
            s.durable = tail;
        }
        Some(())
    }
}

impl Model for MarkerDeliveryModel {
    type Action = MdAction;
    type State = MdState;

    fn init_states(&self) -> Vec<Self::State> {
        let initial = project(&TxnEntry::new_empty(
            "tid".to_owned(),
            PID,
            0,
            TIMEOUT_MS,
            0,
        ));
        let part = Part {
            producer_epoch: -1,
            coordinator_epoch: -1,
            open: None,
            poisoned: false,
        };
        vec![MdState {
            durable: initial,
            tail: None,
            cache: Some(initial),
            coordinator_epoch: 0,
            queued: false,
            parts: [part; PARTITIONS],
            in_flight: 0,
            in_flight_epoch: -1,
            client_epoch: 0,
            healed: false,
            timeouts_left: self.timeouts,
            crashes_left: self.crashes,
            unexpected_left: 1,
            outcomes: BTreeMap::new(),
            violations: BTreeSet::new(),
            witnesses: BTreeSet::new(),
        }]
    }

    fn actions(&self, _: &Self::State, actions: &mut Vec<Self::Action>) {
        // Every action is offered in every state; `next_state` refuses the
        // ones that do not apply.
        let answers = [Answer::Reach, Answer::Retriable, Answer::Unexpected];
        for how in [Persist::Committed, Persist::TimedOut] {
            for index in 0..PARTITIONS {
                actions.push(MdAction::Register(index, how));
            }
            actions.push(MdAction::EndTxn(true, how));
            actions.push(MdAction::EndTxn(false, how));
            actions.push(MdAction::Reap(how));
            for first in answers {
                for second in answers {
                    actions.push(MdAction::Dispatch([first, second], how));
                }
            }
        }
        for index in 0..PARTITIONS {
            actions.push(MdAction::Append(index));
        }
        actions.extend([
            MdAction::Resync,
            MdAction::LateCommit,
            MdAction::Reload,
            MdAction::Crash(true),
            MdAction::Crash(false),
            MdAction::Heal,
        ]);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            MdAction::Register(index, how) => Self::register(&mut s, index, how)?,
            MdAction::EndTxn(committed, how) => Self::end_txn(&mut s, committed, how)?,
            MdAction::Resync => Self::resync(&mut s)?,
            MdAction::Append(index) => Self::append(&mut s, index)?,
            MdAction::Reap(how) => Self::reap(&mut s, how)?,
            MdAction::Dispatch(answers, how) => Self::dispatch(&mut s, answers, how)?,
            MdAction::LateCommit => {
                s.durable = s.tail.take()?;
            }
            MdAction::Reload => Self::reload(&mut s)?,
            MdAction::Crash(keep) => Self::crash(&mut s, keep)?,
            MdAction::Heal => Self::heal(&mut s)?,
        }
        if s.healed && s.cache.is_some_and(|cache| cache != s.durable) {
            s.witnesses.insert(Witness::StaleCacheHealed);
        }
        (s != *last).then_some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: after the heal, a quiet cluster has marked every
            // partition that holds data, except one that keeps answering a
            // code Kafka's completion handler throws on.
            Property::always("every_marker_delivered", |model, s: &MdState| {
                !s.healed
                    || s.parts
                        .iter()
                        .all(|part| part.poisoned || part.open.is_none())
                    || !quiescent(model, s)
            }),
            Property::always("outcome_is_atomic", |_, s: &MdState| {
                !s.violations.contains(&Violation::MixedOutcome)
            }),
            Property::always("leader_state_well_formed", |_, s: &MdState| {
                !s.violations.contains(&Violation::Malformed)
            }),
            // Non-vacuity.
            Property::sometimes("commit_marker_written", |_, s: &MdState| {
                s.witnesses.contains(&Witness::CommitMarker)
            }),
            Property::sometimes("abort_marker_written", |_, s: &MdState| {
                s.witnesses.contains(&Witness::AbortMarker)
            }),
            Property::sometimes("marker_after_reload", |_, s: &MdState| {
                s.witnesses.contains(&Witness::MarkerAfterReload)
            }),
            Property::sometimes("retried_past_unexpected", |_, s: &MdState| {
                s.witnesses.contains(&Witness::RetriedPastUnexpected)
            }),
            Property::sometimes("closed_beside_poisoned", |_, s: &MdState| {
                s.witnesses.contains(&Witness::ClosedBesidePoisoned)
            }),
            Property::sometimes("stale_append_refused", |_, s: &MdState| {
                s.witnesses.contains(&Witness::StaleAppendRefused)
            }),
            Property::sometimes("stale_cache_healed", |_, s: &MdState| {
                s.witnesses.contains(&Witness::StaleCacheHealed)
            }),
            // Non-vacuity of the headline: a quiet, healed cluster after two
            // generations' data got their markers.
            Property::sometimes("quiet_after_heal", |model, s: &MdState| {
                s.healed && s.outcomes.len() >= 2 && quiescent(model, s)
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.client_epoch <= self.max_epoch && s.durable.epoch <= self.max_epoch
    }
}

#[test]
fn marker_delivery_basic() {
    check_model(
        MarkerDeliveryModel {
            max_epoch: 2,
            timeouts: 1,
            crashes: 1,
        },
        "marker_delivery_basic",
        (MAX_DEPTH, MAX_STATES, MAX_UNIQUE),
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn marker_delivery_wide() {
    check_model(
        MarkerDeliveryModel {
            max_epoch: 2,
            timeouts: 2,
            crashes: 2,
        },
        "marker_delivery_wide",
        (MAX_DEPTH, MAX_STATES, MAX_UNIQUE),
        PINNED_UNIQUE_STATES_WIDE,
    );
}
