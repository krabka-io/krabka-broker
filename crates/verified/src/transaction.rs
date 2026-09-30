//! Transaction-completion fencing after the `EndTxn` marker fan-out.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Classification of the actual first control-record key, shared by live
/// append and recovery. Versions are ignored as in Kafka's marker type parser.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum LogBatchKind {
    Data,
    Abort,
    Commit,
    Barrier,
    OtherControl,
}

#[ensures((result == LogBatchKind::Data) == !is_control)]
#[ensures((result == LogBatchKind::Abort) ==
    (is_control && key@.len() >= 4 && key@[2]@ == 0 && key@[3]@ == 0))]
#[ensures((result == LogBatchKind::Commit) ==
    (is_control && key@.len() >= 4 && key@[2]@ == 0 && key@[3]@ == 1))]
#[ensures((result == LogBatchKind::Barrier) ==
    (is_control && key@.len() >= 4 && key@[2]@ == 3 && key@[3]@ == 232))]
#[ensures((result == LogBatchKind::OtherControl) == (is_control
    && !(key@.len() >= 4 && ((key@[2]@ == 0 && (key@[3]@ == 0 || key@[3]@ == 1))
        || (key@[2]@ == 3 && key@[3]@ == 232)))))]
#[must_use]
pub fn log_batch_kind(is_control: bool, key: &[u8]) -> LogBatchKind {
    if !is_control {
        return LogBatchKind::Data;
    }
    if key.len() < 4 {
        return LogBatchKind::OtherControl;
    }
    match (key[2], key[3]) {
        (0, 0) => LogBatchKind::Abort,
        (0, 1) => LogBatchKind::Commit,
        (3, 232) => LogBatchKind::Barrier,
        _ => LogBatchKind::OtherControl,
    }
}

/// Whether one transaction record may install its live producer identities.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TransactionPidInstallDecision {
    RejectWrongPartition,
    RejectCurrentIdentity,
    RejectStagedIdentity,
    RejectCollision,
    Apply,
}

/// Whether one transaction marker may append and publish committed offsets.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TransactionMarkerMaterializationDecision {
    RejectMalformed,
    RejectProducerEpoch,
    RejectCoordinatorEpoch,
    Retry,
    AppendWithoutOffsetPublication,
    AppendAndPublishOffsets,
}

/// One transaction marker as it reaches one data partition.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionMarkerRequest {
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// The sending coordinator's epoch; `-1` names no coordinator generation.
    pub coordinator_epoch: i32,
    /// The marker is a COMMIT rather than an ABORT.
    pub is_commit: bool,
    /// The partition is a `__consumer_offsets` partition.
    pub is_offsets_partition: bool,
}

/// The partition's latest producer state for the marker's producer ID.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionMarkerPartitionState {
    /// The latest producer epoch, or `-1` when the partition has none.
    pub producer_epoch: i16,
    /// The latest coordinator epoch, or `-1` when the partition has none.
    pub coordinator_epoch: i32,
    /// The producer has an open transaction on this partition.
    pub has_pending_transaction: bool,
}

/// The marker names a nonnegative producer identity, every epoch is at least
/// the `-1` sentinel, and a pending transaction has a real producer epoch.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn marker_well_formed(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> bool {
    pearlite! {
        request.producer_id@ >= 0
            && request.producer_epoch@ >= 0
            && request.coordinator_epoch@ >= -1
            && current.producer_epoch@ >= -1
            && current.coordinator_epoch@ >= -1
            && (!current.has_pending_transaction || current.producer_epoch@ >= 0)
    }
}

/// Neither the producer nor the coordinator generation of the marker is
/// older than the partition's (`ProducerAppendInfo.checkProducerEpoch` and
/// `appendEndTxnMarker`'s coordinator-epoch check).
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn marker_generation_current(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> bool {
    pearlite! {
        request.producer_epoch@ >= current.producer_epoch@
            && request.coordinator_epoch@ >= current.coordinator_epoch@
    }
}

/// The marker repeats the exact generation that already closed the
/// producer's transaction on this partition.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn marker_completed_retry(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> bool {
    pearlite! {
        !current.has_pending_transaction
            && request.producer_epoch@ == current.producer_epoch@
            && request.coordinator_epoch@ == current.coordinator_epoch@
    }
}

/// Only a COMMIT that closes a pending transaction on `__consumer_offsets`
/// publishes the transaction's offset commits.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn marker_publishes_offsets(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> bool {
    pearlite! {
        current.has_pending_transaction && request.is_commit && request.is_offsets_partition
    }
}

/// Fence a transaction marker against the partition's latest producer and
/// coordinator generations, suppress an exact completed retry, and publish
/// offsets only for a pending commit on `__consumer_offsets`.
#[ensures((result == TransactionMarkerMaterializationDecision::RejectMalformed)
    == !marker_well_formed(request, current))]
#[ensures((result == TransactionMarkerMaterializationDecision::RejectProducerEpoch)
    == (marker_well_formed(request, current)
        && request.producer_epoch@ < current.producer_epoch@))]
#[ensures((result == TransactionMarkerMaterializationDecision::RejectCoordinatorEpoch)
    == (marker_well_formed(request, current)
        && request.producer_epoch@ >= current.producer_epoch@
        && request.coordinator_epoch@ < current.coordinator_epoch@))]
#[ensures((result == TransactionMarkerMaterializationDecision::Retry)
    == (marker_well_formed(request, current) && marker_completed_retry(request, current)))]
#[ensures((result == TransactionMarkerMaterializationDecision::AppendAndPublishOffsets)
    == (marker_well_formed(request, current)
        && marker_generation_current(request, current)
        && marker_publishes_offsets(request, current)))]
#[ensures((result == TransactionMarkerMaterializationDecision::AppendWithoutOffsetPublication)
    == (marker_well_formed(request, current)
        && marker_generation_current(request, current)
        && !marker_completed_retry(request, current)
        && !marker_publishes_offsets(request, current)))]
#[must_use]
pub fn transaction_marker_materialization_decision(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> TransactionMarkerMaterializationDecision {
    if request.producer_id < 0
        || request.producer_epoch < 0
        || request.coordinator_epoch < -1
        || current.producer_epoch < -1
        || current.coordinator_epoch < -1
        || (current.has_pending_transaction && current.producer_epoch == -1)
    {
        TransactionMarkerMaterializationDecision::RejectMalformed
    } else if request.producer_epoch < current.producer_epoch {
        TransactionMarkerMaterializationDecision::RejectProducerEpoch
    } else if request.coordinator_epoch < current.coordinator_epoch {
        TransactionMarkerMaterializationDecision::RejectCoordinatorEpoch
    } else if !current.has_pending_transaction
        && request.producer_epoch == current.producer_epoch
        && request.coordinator_epoch == current.coordinator_epoch
    {
        TransactionMarkerMaterializationDecision::Retry
    } else if current.has_pending_transaction && request.is_commit && request.is_offsets_partition {
        TransactionMarkerMaterializationDecision::AppendAndPublishOffsets
    } else {
        TransactionMarkerMaterializationDecision::AppendWithoutOffsetPublication
    }
}

/// Whether the idle reaper or the completion task may publish one prepared
/// completion.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TransactionReaperCompletionDecision {
    /// A snapshot names a negative producer ID or epoch.
    RejectMalformed,
    /// The live entry no longer holds the prepared producer identity.
    RejectStaleIdentity,
    /// The live entry holds the prepared identity but is no longer the exact
    /// prepared snapshot.
    RejectChangedPreparedState,
    /// The live entry already holds the intended completion.
    AlreadyComplete,
    /// The live entry is the exact prepared snapshot.
    Proceed,
}

/// `snapshot` names a producer identity: its PID and epoch are nonnegative.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn is_identity(snapshot: TransactionSnapshot) -> bool {
    pearlite! { snapshot.pid@ >= 0 && snapshot.epoch@ >= 0 }
}

/// `snapshot` holds the producer identity `identity`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn has_identity(snapshot: TransactionSnapshot, identity: TransactionIdentity) -> bool {
    pearlite! { snapshot.pid == identity.pid && snapshot.epoch == identity.epoch }
}

/// Two snapshots agree on producer identity and state.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn snapshot_eq(left: TransactionSnapshot, right: TransactionSnapshot) -> bool {
    pearlite! {
        left.pid == right.pid && left.epoch == right.epoch && left.state == right.state
    }
}

/// Recheck a prepared snapshot after the abort- or commit-marker fan-out.
///
/// This is [`transaction_completion_decision`] with two additions: every
/// snapshot must name a nonnegative producer identity, because the live entry
/// and the prepared one come from the replayed transaction log; and
/// `exact_prepared_snapshot` must hold for `Proceed`. The host computes it
/// from equality over the complete persisted transaction entry, including its
/// staged identity, partition set, timeout, and timestamps, so a registration
/// or recovery that kept the identity and state still blocks the completion.
///
/// `prepared.state` and `completion.state` are the prepare and complete tags
/// of one Kafka completion, which the host takes from a fixed
/// `Prepare* -> Complete*` pairing, so they differ.
#[requires(prepared.state != completion.state)]
#[ensures((result == TransactionReaperCompletionDecision::RejectMalformed)
    == !(is_identity(current) && is_identity(prepared) && is_identity(completion)))]
#[ensures((result == TransactionReaperCompletionDecision::AlreadyComplete)
    == (is_identity(current)
        && is_identity(prepared)
        && is_identity(completion)
        && snapshot_eq(current, completion)))]
#[ensures((result == TransactionReaperCompletionDecision::RejectStaleIdentity)
    == (is_identity(current)
        && is_identity(prepared)
        && is_identity(completion)
        && !snapshot_eq(current, completion)
        && !(current.pid == prepared.pid && current.epoch == prepared.epoch)))]
#[ensures((result == TransactionReaperCompletionDecision::RejectChangedPreparedState)
    == (is_identity(current)
        && is_identity(prepared)
        && is_identity(completion)
        && !snapshot_eq(current, completion)
        && current.pid == prepared.pid
        && current.epoch == prepared.epoch
        && !(current.state == prepared.state && exact_prepared_snapshot)))]
#[ensures((result == TransactionReaperCompletionDecision::Proceed)
    == (is_identity(current)
        && is_identity(prepared)
        && is_identity(completion)
        && snapshot_eq(current, prepared)
        && exact_prepared_snapshot))]
#[must_use]
pub fn transaction_reaper_completion_decision(
    current: TransactionSnapshot,
    prepared: TransactionSnapshot,
    completion: TransactionSnapshot,
    exact_prepared_snapshot: bool,
) -> TransactionReaperCompletionDecision {
    if current.pid < 0
        || current.epoch < 0
        || prepared.pid < 0
        || prepared.epoch < 0
        || completion.pid < 0
        || completion.epoch < 0
    {
        return TransactionReaperCompletionDecision::RejectMalformed;
    }
    match transaction_completion_decision(
        current,
        prepared.identity(),
        completion.identity(),
        prepared.state,
        completion.state,
    ) {
        TransactionCompletionDecision::AlreadyComplete => {
            TransactionReaperCompletionDecision::AlreadyComplete
        }
        TransactionCompletionDecision::RejectStaleIdentity => {
            TransactionReaperCompletionDecision::RejectStaleIdentity
        }
        TransactionCompletionDecision::RejectState => {
            TransactionReaperCompletionDecision::RejectChangedPreparedState
        }
        TransactionCompletionDecision::Proceed => {
            if exact_prepared_snapshot {
                TransactionReaperCompletionDecision::Proceed
            } else {
                TransactionReaperCompletionDecision::RejectChangedPreparedState
            }
        }
    }
}

/// Admit only a well-formed, uniquely owned producer-ID pair from the
/// transaction log partition selected by its transactional ID.
#[ensures((result == TransactionPidInstallDecision::RejectWrongPartition)
    == !partition_matches)]
#[ensures((result == TransactionPidInstallDecision::RejectCurrentIdentity)
    == (partition_matches && (producer_id@ < 0 || producer_epoch@ < 0)))]
#[ensures((result == TransactionPidInstallDecision::RejectStagedIdentity)
    == (partition_matches
        && producer_id@ >= 0
        && producer_epoch@ >= 0
        && !((next_producer_id@ == -1 && next_producer_epoch@ == -1)
            || (next_producer_id@ >= 0 && next_producer_epoch@ >= 0))))]
#[ensures((result == TransactionPidInstallDecision::RejectCollision)
    == (partition_matches
        && producer_id@ >= 0
        && producer_epoch@ >= 0
        && ((next_producer_id@ == -1 && next_producer_epoch@ == -1)
            || (next_producer_id@ >= 0 && next_producer_epoch@ >= 0))
        && (!current_owner_matches
            || (next_producer_id@ >= 0 && !next_owner_matches))))]
#[ensures((result == TransactionPidInstallDecision::Apply)
    == (partition_matches
        && producer_id@ >= 0
        && producer_epoch@ >= 0
        && ((next_producer_id@ == -1 && next_producer_epoch@ == -1)
            || (next_producer_id@ >= 0 && next_producer_epoch@ >= 0))
        && current_owner_matches
        && (next_producer_id@ < 0 || next_owner_matches)))]
#[must_use]
pub fn transaction_pid_install_decision(
    partition_matches: bool,
    producer_id: i64,
    producer_epoch: i16,
    next_producer_id: i64,
    next_producer_epoch: i16,
    current_owner_matches: bool,
    next_owner_matches: bool,
) -> TransactionPidInstallDecision {
    if !partition_matches {
        TransactionPidInstallDecision::RejectWrongPartition
    } else if producer_id < 0 || producer_epoch < 0 {
        TransactionPidInstallDecision::RejectCurrentIdentity
    } else if !((next_producer_id == -1 && next_producer_epoch == -1)
        || (next_producer_id >= 0 && next_producer_epoch >= 0))
    {
        TransactionPidInstallDecision::RejectStagedIdentity
    } else if !current_owner_matches || (next_producer_id >= 0 && !next_owner_matches) {
        TransactionPidInstallDecision::RejectCollision
    } else {
        TransactionPidInstallDecision::Apply
    }
}

/// Whether one transaction generation may persist a partition registration.
///
/// The ordering mirrors Kafka's `TransactionCoordinator.handleAddPartitionsToTransaction`:
/// a pending transition (`pendingTransitionInProgress`) is checked first so it
/// can complete before any identity check runs, then the producer id, then the
/// producer epoch, then the `PrepareCommit`/`PrepareAbort` state check. Every
/// one of those four rejections is retriable on Kafka's wire
/// (`CONCURRENT_TRANSACTIONS`, `INVALID_PRODUCER_ID_MAPPING`, or
/// `PRODUCER_FENCED`), which the host maps; this module only orders the facts.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TransactionRegistrationDecision {
    RejectNotCoordinator,
    RejectUnknownProducer,
    RejectPendingTransition,
    RejectProducerId,
    RejectProducerEpoch,
    RejectState,
    PersistRetry,
    PersistRegistration,
}

/// Facts used to fence one transaction partition registration.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionRegistrationFacts {
    pub ownership: TransactionRegistrationOwnershipFacts,
    pub identity: TransactionRegistrationIdentityFacts,
    pub state: TransactionRegistrationStateFacts,
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionRegistrationOwnershipFacts {
    pub is_coordinator: bool,
    pub producer_id_valid: bool,
    pub entry_exists: bool,
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionRegistrationIdentityFacts {
    /// Kafka's `txnMetadata.pendingTransitionInProgress`: a staged producer
    /// identity is waiting on an older transaction to complete first.
    pub pending_transition: bool,
    pub matching: TransactionRegistrationIdentityMatchFacts,
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionRegistrationIdentityMatchFacts {
    pub transactional_id_matches: bool,
    pub producer_id_matches: bool,
    pub producer_epoch_matches: bool,
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionRegistrationStateFacts {
    pub state_allows_registration: bool,
    /// Kafka's `txnMetadata.state == ONGOING`. The retry optimization below
    /// applies only in that exact state, never against a stale partition set
    /// left over from a completed or not-yet-started transaction.
    pub state_is_ongoing: bool,
    pub exact_partitions_registered: bool,
}

/// Fence a partition registration against coordinator ownership and one exact
/// transactional-id, producer-id, and producer-epoch generation.
#[ensures((result == TransactionRegistrationDecision::RejectNotCoordinator)
    == !facts.ownership.is_coordinator)]
#[ensures((result == TransactionRegistrationDecision::RejectUnknownProducer)
    == (facts.ownership.is_coordinator
        && (!facts.ownership.producer_id_valid
            || !facts.ownership.entry_exists
            || !facts.identity.matching.transactional_id_matches)))]
#[ensures((result == TransactionRegistrationDecision::RejectPendingTransition)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.matching.transactional_id_matches
        && facts.identity.pending_transition))]
#[ensures((result == TransactionRegistrationDecision::RejectProducerId)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.matching.transactional_id_matches
        && !facts.identity.pending_transition
        && !facts.identity.matching.producer_id_matches))]
#[ensures((result == TransactionRegistrationDecision::RejectProducerEpoch)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.matching.transactional_id_matches
        && !facts.identity.pending_transition
        && facts.identity.matching.producer_id_matches
        && !facts.identity.matching.producer_epoch_matches))]
#[ensures((result == TransactionRegistrationDecision::RejectState)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.matching.transactional_id_matches
        && !facts.identity.pending_transition
        && facts.identity.matching.producer_id_matches
        && facts.identity.matching.producer_epoch_matches
        && !facts.state.state_allows_registration))]
#[ensures((result == TransactionRegistrationDecision::PersistRetry)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.matching.transactional_id_matches
        && !facts.identity.pending_transition
        && facts.identity.matching.producer_id_matches
        && facts.identity.matching.producer_epoch_matches
        && facts.state.state_allows_registration
        && facts.state.state_is_ongoing
        && facts.state.exact_partitions_registered))]
#[ensures((result == TransactionRegistrationDecision::PersistRegistration)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.matching.transactional_id_matches
        && !facts.identity.pending_transition
        && facts.identity.matching.producer_id_matches
        && facts.identity.matching.producer_epoch_matches
        && facts.state.state_allows_registration
        && !(facts.state.state_is_ongoing && facts.state.exact_partitions_registered)))]
#[must_use]
pub fn transaction_partition_registration(
    facts: TransactionRegistrationFacts,
) -> TransactionRegistrationDecision {
    if !facts.ownership.is_coordinator {
        TransactionRegistrationDecision::RejectNotCoordinator
    } else if !facts.ownership.producer_id_valid
        || !facts.ownership.entry_exists
        || !facts.identity.matching.transactional_id_matches
    {
        TransactionRegistrationDecision::RejectUnknownProducer
    } else if facts.identity.pending_transition {
        TransactionRegistrationDecision::RejectPendingTransition
    } else if !facts.identity.matching.producer_id_matches {
        TransactionRegistrationDecision::RejectProducerId
    } else if !facts.identity.matching.producer_epoch_matches {
        TransactionRegistrationDecision::RejectProducerEpoch
    } else if !facts.state.state_allows_registration {
        TransactionRegistrationDecision::RejectState
    } else if facts.state.state_is_ongoing && facts.state.exact_partitions_registered {
        TransactionRegistrationDecision::PersistRetry
    } else {
        TransactionRegistrationDecision::PersistRegistration
    }
}

/// Whether an `EndTxn` caller may finalize the entry it prepared.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TransactionCompletionDecision {
    Proceed,
    AlreadyComplete,
    RejectStaleIdentity,
    RejectState,
}

/// Sentinel persisted for a transaction controlled by an external 2PC owner.
pub const NO_TRANSACTION_TIMEOUT_MS: i32 = i32::MAX;

/// Select the first unstable transaction offset, or the log end when no
/// transaction is open. A pending start beyond the log end is rejected.
///
/// `Some` and `None` partition the inputs: the result is `Some` exactly when
/// every start is at or below the log end.
#[ensures(match result {
    Some(lso) => lso@ <= log_end@
        && (forall<i: Int> 0 <= i && i < starts@.len() ==> starts@[i]@ <= log_end@)
        && ((starts@.len() == 0 && lso@ == log_end@)
            || (starts@.len() > 0
                && (exists<i: Int> 0 <= i && i < starts@.len() && lso@ == starts@[i]@)
                && (forall<i: Int> 0 <= i && i < starts@.len() ==> lso@ <= starts@[i]@))),
    None => exists<i: Int> 0 <= i && i < starts@.len() && starts@[i]@ > log_end@,
})]
#[must_use]
pub fn first_unstable_offset(starts: &[i64], log_end: i64) -> Option<i64> {
    let mut lso = log_end;
    let mut index = 0usize;
    #[invariant(index@ <= starts@.len())]
    #[invariant(lso@ <= log_end@)]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> starts@[i]@ <= log_end@)]
    #[invariant(index@ == 0 ==> lso@ == log_end@)]
    #[invariant(index@ > 0 ==> exists<i: Int> 0 <= i && i < index@ && lso@ == starts@[i]@)]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> lso@ <= starts@[i]@)]
    #[variant(starts@.len() - index@)]
    while index < starts.len() {
        let start = starts[index];
        if start > log_end {
            return None;
        }
        if start < lso {
            lso = start;
        }
        index += 1;
    }
    Some(lso)
}

/// A valid COMMIT or ABORT marker closes state only for its matching pending
/// producer.
#[ensures(result == ((is_abort || is_commit) && !(is_abort && is_commit) && has_pending))]
#[must_use]
pub fn transaction_marker_closes(is_abort: bool, is_commit: bool, has_pending: bool) -> bool {
    (is_abort || is_commit) && !(is_abort && is_commit) && has_pending
}

/// Construct one aborted transaction's inclusive interval only from a live,
/// nonnegative producer and ordered marker bounds.
#[ensures(match result {
    Some((start, last)) => producer_id@ >= 0
        && pending_start == Some(start)
        && last@ == marker_last@
        && start@ <= last@,
    None => producer_id@ < 0
        || pending_start == None
        || match pending_start { Some(start) => start@ > marker_last@, None => false },
})]
#[must_use]
pub fn aborted_transaction_interval(
    pending_start: Option<i64>,
    marker_last: i64,
    producer_id: i64,
) -> Option<(i64, i64)> {
    if producer_id < 0 {
        return None;
    }
    let start = pending_start?;
    if start > marker_last {
        return None;
    }
    Some((start, marker_last))
}

/// Whether a valid inclusive aborted interval intersects a nonempty half-open
/// Fetch range.
#[ensures(result == (entry_start@ <= entry_last@
    && query_start@ < query_end@
    && entry_start@ < query_end@
    && entry_last@ >= query_start@))]
#[must_use]
pub fn aborted_transaction_overlaps(
    entry_start: i64,
    entry_last: i64,
    query_start: i64,
    query_end: i64,
) -> bool {
    entry_start <= entry_last
        && query_start < query_end
        && entry_start < query_end
        && entry_last >= query_start
}

/// State fact needed by the idle-transaction reaper.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum IdleTransactionState {
    Ongoing,
    Other,
}

/// Whether the idle reaper may abort one persisted transaction.
///
/// This is Kafka's `TransactionStateManager.timedOutTransactions`: the
/// transaction is `ONGOING`, it is not a KIP-939 two-phase-commit transaction
/// (`TransactionMetadata.isDistributedTwoPhaseCommitTxn`, a timeout of
/// `Integer.MAX_VALUE`), and `txnStartTimestamp + txnTimeoutMs < now`. The
/// comparison is strict, so a transaction is reapable only once its timeout
/// has passed, not when it is reached. Kafka evaluates the sum in a Java
/// `long`; the kernel compares exactly, without overflow.
///
/// The decision is total over every persisted timeout. Kafka's
/// `TransactionLog.read` accepts any `TransactionTimeoutMs`, and
/// `InitProducerId` refuses a timeout that is not positive
/// (`validateTransactionTimeoutMs`), so a zero or negative timeout reaches the
/// reaper only from a transaction-log record another writer produced. Kafka
/// treats that timeout arithmetically, and so does this kernel.
///
/// A backwards clock never aborts a transaction with a nonnegative timeout,
/// which is every timeout `InitProducerId` persists.
#[ensures(result == (state == IdleTransactionState::Ongoing
    && txn_timeout_ms@ != NO_TRANSACTION_TIMEOUT_MS@
    && start_ms@ + txn_timeout_ms@ < now_ms@))]
#[ensures(now_ms@ <= start_ms@ && txn_timeout_ms@ >= 0 ==> !result)]
#[must_use]
pub fn should_abort_idle_transaction(
    state: IdleTransactionState,
    txn_timeout_ms: i32,
    start_ms: i64,
    now_ms: i64,
) -> bool {
    let ongoing = match state {
        IdleTransactionState::Ongoing => true,
        IdleTransactionState::Other => false,
    };
    // Saturation keeps the order against any `i32` timeout: an elapsed time
    // above `i64::MAX` exceeds it, and one below `i64::MIN` does not.
    ongoing
        && txn_timeout_ms != NO_TRANSACTION_TIMEOUT_MS
        && now_ms.saturating_sub(start_ms) > i64::from(txn_timeout_ms)
}

/// The persisted identity and state observed after marker fan-out.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionSnapshot {
    pub pid: i64,
    pub epoch: i16,
    pub state: i8,
}

/// A producer identity captured before marker fan-out.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionIdentity {
    pub pid: i64,
    pub epoch: i16,
}

impl TransactionSnapshot {
    /// The producer identity this snapshot holds.
    #[ensures(result.pid == self.pid && result.epoch == self.epoch)]
    #[must_use]
    pub fn identity(self) -> TransactionIdentity {
        TransactionIdentity {
            pid: self.pid,
            epoch: self.epoch,
        }
    }
}

/// Choose the producer identity exposed after transaction completion.
///
/// Verified normal completion reserves `i16::MAX` for the transaction marker,
/// while a staged recovery identity may use that epoch once before rotating.
#[ensures(!verified ==> result == Some((pid, epoch)))]
#[ensures(verified && !recovery && epoch@ < i16::MAX@ - 1 ==>
    match result {
        Some((result_pid, result_epoch)) => result_pid == pid && result_epoch@ == epoch@ + 1,
        None => false,
    })]
#[ensures(verified && recovery && epoch@ < i16::MAX@ ==>
    match result {
        Some((result_pid, result_epoch)) => result_pid == pid && result_epoch@ == epoch@ + 1,
        None => false,
    })]
#[ensures(verified
    && ((!recovery && epoch@ >= i16::MAX@ - 1) || (recovery && epoch@ >= i16::MAX@)) ==>
    match (result, fresh) {
        (Some((result_pid, result_epoch)), Some(fresh_pid)) =>
            result_pid == fresh_pid && result_epoch@ == 0,
        (None, None) => true,
        _ => false,
    })]
#[must_use]
pub fn next_producer_identity(
    verified: bool,
    recovery: bool,
    pid: i64,
    epoch: i16,
    fresh: Option<i64>,
) -> Option<(i64, i16)> {
    if !verified {
        return Some((pid, epoch));
    }
    let can_increment = if recovery {
        epoch < i16::MAX
    } else {
        epoch < i16::MAX - 1
    };
    if can_increment {
        Some((pid, epoch + 1))
    } else {
        fresh.map(|fresh_pid| (fresh_pid, 0))
    }
}

/// What `InitProducerId` does with the producer identity a caller supplies.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum InitProducerIdIdentityDecision {
    /// The caller names no identity. Kafka bumps the epoch and records no
    /// last epoch (`prepareIncrementProducerEpoch` with an empty expected
    /// epoch).
    BumpWithoutIdentity,
    /// The caller names the entry's live epoch. Kafka bumps the epoch and
    /// records the epoch it held as the last epoch.
    Bump,
    /// The caller names the epoch the entry held before its last bump. Kafka
    /// treats this as a retry of the call that made that bump: it answers the
    /// entry's identity and writes nothing.
    Retry,
    /// Kafka answers `PRODUCER_FENCED`.
    Fenced,
}

/// The epoch at and above which Kafka treats a producer epoch as exhausted
/// (`TransactionMetadata.isEpochExhausted`). The coordinator keeps one epoch
/// in hand to fence the producer with, so it never hands out `i16::MAX`.
pub const EXHAUSTED_PRODUCER_EPOCH: i16 = i16::MAX - 1;

/// What an `InitProducerId` caller's producer identity may do to the entry it
/// names (KIP-360).
///
/// A request producer id of `-1` supplies no identity, which every
/// `InitProducerId` below v3 and every first initialisation does.
///
/// Kafka admits a supplied identity in `TransactionCoordinator`'s
/// `isValidProducerId`: the identity names the entry's producer id, whatever
/// its epoch, or it names the producer id from before the last rotation
/// together with an exhausted epoch. The epoch then decides the outcome in
/// `TransactionMetadata.prepareIncrementProducerEpoch`: the entry's own epoch
/// bumps it, the epoch before the last bump is a retry of that bump, and
/// every other epoch is fenced.
///
/// The retry rule also covers a failed epoch fence: that path records the
/// epoch the producer still holds as the last epoch, so the producer that
/// owns the transaction is the one the rule admits.
///
/// Kafka's `isValidProducerId` has a third admission clause, which this
/// kernel does not model: `txnMetadata.producerEpoch ==
/// RecordBatch.NO_PRODUCER_EPOCH` admits every supplied identity, and
/// `prepareIncrementProducerEpoch` then bumps. Only the metadata that
/// `handleInitProducerId` has just created for an unknown transactional ID
/// carries that epoch. The host covers the clause: it answers a transactional
/// ID with no entry by allocating a fresh identity without calling this
/// kernel, and every entry it does pass here has a nonnegative epoch, because
/// allocation hands out nonnegative epochs and
/// [`transaction_pid_install_decision`] rejects a replayed entry with a
/// negative current or staged epoch.
#[ensures((result == InitProducerIdIdentityDecision::BumpWithoutIdentity)
    == (request_pid@ == -1))]
#[ensures((result == InitProducerIdIdentityDecision::Bump)
    == (request_pid@ != -1
        && request_pid@ == entry_pid@
        && request_epoch@ == entry_epoch@))]
#[ensures((result == InitProducerIdIdentityDecision::Retry)
    == (request_pid@ != -1
        && (request_pid@ == entry_pid@
            || (request_pid@ == prev_pid@
                && request_epoch@ >= EXHAUSTED_PRODUCER_EPOCH@))
        && !(request_pid@ == entry_pid@ && request_epoch@ == entry_epoch@)
        && request_epoch@ == last_epoch@))]
#[ensures((result == InitProducerIdIdentityDecision::Fenced)
    == (request_pid@ != -1
        && (!(request_pid@ == entry_pid@
            || (request_pid@ == prev_pid@
                && request_epoch@ >= EXHAUSTED_PRODUCER_EPOCH@))
            || (!(request_pid@ == entry_pid@ && request_epoch@ == entry_epoch@)
                && request_epoch@ != last_epoch@))))]
#[must_use]
pub fn init_producer_id_identity_decision(
    entry_pid: i64,
    entry_epoch: i16,
    last_epoch: i16,
    prev_pid: i64,
    request_pid: i64,
    request_epoch: i16,
) -> InitProducerIdIdentityDecision {
    if request_pid == -1 {
        return InitProducerIdIdentityDecision::BumpWithoutIdentity;
    }
    let admitted = request_pid == entry_pid
        || (request_pid == prev_pid && request_epoch >= EXHAUSTED_PRODUCER_EPOCH);
    if !admitted {
        return InitProducerIdIdentityDecision::Fenced;
    }
    if request_pid == entry_pid && request_epoch == entry_epoch {
        return InitProducerIdIdentityDecision::Bump;
    }
    if request_epoch == last_epoch {
        return InitProducerIdIdentityDecision::Retry;
    }
    InitProducerIdIdentityDecision::Fenced
}

/// Revalidate the transaction entry after the marker fan-out released its lock.
///
/// `expected` and `prepare_state` are what the caller prepared; `completion`
/// and `complete_state` are the completion it intends to write. The live entry
/// already holding that completion is an idempotent success; otherwise the
/// entry must still hold the prepared identity (else it was fenced) and the
/// prepared state (else another caller moved it).
///
/// The two state tags are one Kafka `Prepare* -> Complete*` pairing, which the
/// host takes from a fixed mapping, so they differ.
#[requires(prepare_state != complete_state)]
#[ensures((result == TransactionCompletionDecision::AlreadyComplete)
    == (has_identity(current, completion) && current.state == complete_state))]
#[ensures((result == TransactionCompletionDecision::RejectStaleIdentity)
    == (!(has_identity(current, completion) && current.state == complete_state)
        && !has_identity(current, expected)))]
#[ensures((result == TransactionCompletionDecision::RejectState)
    == (!(has_identity(current, completion) && current.state == complete_state)
        && has_identity(current, expected)
        && current.state != prepare_state))]
#[ensures((result == TransactionCompletionDecision::Proceed)
    == (has_identity(current, expected) && current.state == prepare_state))]
#[must_use]
pub fn transaction_completion_decision(
    current: TransactionSnapshot,
    expected: TransactionIdentity,
    completion: TransactionIdentity,
    prepare_state: i8,
    complete_state: i8,
) -> TransactionCompletionDecision {
    if current.pid == completion.pid
        && current.epoch == completion.epoch
        && current.state == complete_state
    {
        return TransactionCompletionDecision::AlreadyComplete;
    }
    if current.pid != expected.pid || current.epoch != expected.epoch {
        return TransactionCompletionDecision::RejectStaleIdentity;
    }
    if current.state == prepare_state {
        TransactionCompletionDecision::Proceed
    } else {
        TransactionCompletionDecision::RejectState
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    // Kafka `TransactionState` ids.
    const ONGOING: i8 = 1;
    const PREPARE_COMMIT: i8 = 2;
    const PREPARE_ABORT: i8 = 3;
    const COMPLETE_COMMIT: i8 = 4;
    const COMPLETE_ABORT: i8 = 5;

    #[test]
    fn pid_install_rejects_malformed_misplaced_and_colliding_records() {
        use TransactionPidInstallDecision::{
            Apply, RejectCollision, RejectCurrentIdentity, RejectStagedIdentity,
            RejectWrongPartition,
        };

        for (arguments, expected) in [
            ((false, 1, 0, -1, -1, true, true), RejectWrongPartition),
            ((true, -1, 0, -1, -1, true, true), RejectCurrentIdentity),
            ((true, 1, -1, -1, -1, true, true), RejectCurrentIdentity),
            ((true, 1, 0, 2, -1, true, true), RejectStagedIdentity),
            ((true, 1, 0, -1, 0, true, true), RejectStagedIdentity),
            ((true, 1, 0, 1, 0, true, true), Apply),
            ((true, 1, 0, -1, -1, false, true), RejectCollision),
            ((true, 1, 0, 2, 0, true, false), RejectCollision),
            ((true, 1, 0, -1, -1, true, true), Apply),
            ((true, 0, 0, -1, -1, true, true), Apply),
            ((true, 1, 0, 2, 0, true, true), Apply),
        ] {
            assert!(
                transaction_pid_install_decision(
                    arguments.0,
                    arguments.1,
                    arguments.2,
                    arguments.3,
                    arguments.4,
                    arguments.5,
                    arguments.6,
                ) == expected
            );
        }
    }

    fn marker(
        producer_epoch: i16,
        coordinator_epoch: i32,
        is_commit: bool,
        is_offsets_partition: bool,
    ) -> TransactionMarkerRequest {
        TransactionMarkerRequest {
            producer_id: 1,
            producer_epoch,
            coordinator_epoch,
            is_commit,
            is_offsets_partition,
        }
    }

    fn partition(
        producer_epoch: i16,
        coordinator_epoch: i32,
        has_pending_transaction: bool,
    ) -> TransactionMarkerPartitionState {
        TransactionMarkerPartitionState {
            producer_epoch,
            coordinator_epoch,
            has_pending_transaction,
        }
    }

    /// Kafka `ProducerStateManager`'s `appendEndTxnMarker` fencing, plus the
    /// exact-retry and offset-publication rules.
    #[test]
    fn marker_materialization_fences_retries_and_offset_publication() {
        use TransactionMarkerMaterializationDecision::{
            AppendAndPublishOffsets, AppendWithoutOffsetPublication, RejectCoordinatorEpoch,
            RejectMalformed, RejectProducerEpoch, Retry,
        };

        let cases = [
            // A negative producer ID names no producer.
            (
                TransactionMarkerRequest {
                    producer_id: -1,
                    ..marker(0, 0, true, true)
                },
                partition(-1, -1, true),
                RejectMalformed,
            ),
            // A negative marker epoch.
            (
                marker(-1, 0, true, true),
                partition(0, 0, true),
                RejectMalformed,
            ),
            // A coordinator epoch below the `-1` sentinel.
            (
                marker(0, -2, false, false),
                partition(0, -1, true),
                RejectMalformed,
            ),
            // Partition state below the sentinels.
            (
                marker(0, 0, true, true),
                partition(-2, -1, true),
                RejectMalformed,
            ),
            (
                marker(0, 0, true, true),
                partition(0, -2, false),
                RejectMalformed,
            ),
            // A pending transaction without a producer epoch.
            (
                marker(0, 0, true, true),
                partition(-1, 0, true),
                RejectMalformed,
            ),
            // `ProducerFencedException`: the marker's epoch is older.
            (
                marker(2, 9, true, true),
                partition(3, 8, true),
                RejectProducerEpoch,
            ),
            // `TransactionCoordinatorFencedException`: an older coordinator.
            (
                marker(3, 7, true, true),
                partition(3, 8, true),
                RejectCoordinatorEpoch,
            ),
            // The same generation already closed the transaction: append
            // nothing.
            (marker(3, 8, true, true), partition(3, 8, false), Retry),
            (marker(0, 0, true, true), partition(0, 0, false), Retry),
            // A pending COMMIT on `__consumer_offsets` publishes its offsets.
            (
                marker(3, 8, true, true),
                partition(3, 7, true),
                AppendAndPublishOffsets,
            ),
            // An ABORT, a data partition, or no pending transaction appends
            // without publishing.
            (
                marker(3, 8, false, true),
                partition(3, 7, true),
                AppendWithoutOffsetPublication,
            ),
            (
                marker(3, 8, true, false),
                partition(3, 7, true),
                AppendWithoutOffsetPublication,
            ),
            (
                marker(0, -1, false, false),
                partition(0, -1, true),
                AppendWithoutOffsetPublication,
            ),
            // A newer generation than the closed one is not a retry.
            (
                marker(4, 9, true, true),
                partition(3, 8, false),
                AppendWithoutOffsetPublication,
            ),
            (
                marker(0, 0, true, true),
                partition(-1, 0, false),
                AppendWithoutOffsetPublication,
            ),
        ];
        for (request, current, expected) in cases {
            assert!(
                transaction_marker_materialization_decision(request, current) == expected,
                "request={request:?}, current={current:?}"
            );
        }
    }

    fn snapshot(pid: i64, epoch: i16, state: i8) -> TransactionSnapshot {
        TransactionSnapshot { pid, epoch, state }
    }

    #[test]
    fn reaper_completion_requires_the_exact_prepared_snapshot() {
        use TransactionReaperCompletionDecision::{
            AlreadyComplete, Proceed, RejectChangedPreparedState, RejectMalformed,
            RejectStaleIdentity,
        };

        let prepared = snapshot(7, 3, PREPARE_ABORT);
        let completion = snapshot(7, 4, COMPLETE_ABORT);
        // (current, prepared, completion, exact snapshot, expected).
        let cases = [
            // The entry is exactly as the reaper prepared it.
            (prepared, prepared, completion, true, Proceed),
            (
                snapshot(0, 0, PREPARE_ABORT),
                snapshot(0, 0, PREPARE_ABORT),
                snapshot(0, 1, COMPLETE_ABORT),
                true,
                Proceed,
            ),
            // Same identity and state, but another field of the entry moved,
            // such as a late partition registration.
            (
                prepared,
                prepared,
                completion,
                false,
                RejectChangedPreparedState,
            ),
            // Same identity, different state: another caller moved it.
            (
                snapshot(7, 3, ONGOING),
                prepared,
                completion,
                true,
                RejectChangedPreparedState,
            ),
            // An `InitProducerId` bumped the epoch or rotated the PID.
            (
                snapshot(7, 5, PREPARE_ABORT),
                prepared,
                completion,
                true,
                RejectStaleIdentity,
            ),
            (
                snapshot(9, 3, PREPARE_ABORT),
                prepared,
                completion,
                false,
                RejectStaleIdentity,
            ),
            // The completion identity at the prepare state is not the
            // completion.
            (
                snapshot(7, 4, PREPARE_ABORT),
                prepared,
                completion,
                true,
                RejectStaleIdentity,
            ),
            // The intended completion is already durable, whatever the
            // snapshot comparison says.
            (completion, prepared, completion, false, AlreadyComplete),
            (completion, prepared, completion, true, AlreadyComplete),
            // A negative PID or epoch in any snapshot.
            (
                snapshot(-1, 3, PREPARE_ABORT),
                prepared,
                completion,
                true,
                RejectMalformed,
            ),
            (
                snapshot(7, -1, PREPARE_ABORT),
                prepared,
                completion,
                true,
                RejectMalformed,
            ),
            (
                prepared,
                snapshot(-1, 3, PREPARE_ABORT),
                completion,
                true,
                RejectMalformed,
            ),
            (
                prepared,
                snapshot(7, -1, PREPARE_ABORT),
                completion,
                true,
                RejectMalformed,
            ),
            (
                prepared,
                prepared,
                snapshot(-1, 4, COMPLETE_ABORT),
                true,
                RejectMalformed,
            ),
            (
                prepared,
                prepared,
                snapshot(7, -1, COMPLETE_ABORT),
                true,
                RejectMalformed,
            ),
        ];
        for (current, prepared, completion, exact, expected) in cases {
            assert!(
                transaction_reaper_completion_decision(current, prepared, completion, exact)
                    == expected,
                "current={current:?}, prepared={prepared:?}, completion={completion:?}, \
                 exact={exact}"
            );
        }
    }

    #[test]
    fn partition_registration_fences_generation_and_retries_exactly() {
        use TransactionRegistrationDecision::{
            PersistRegistration, PersistRetry, RejectNotCoordinator, RejectPendingTransition,
            RejectProducerEpoch, RejectProducerId, RejectState, RejectUnknownProducer,
        };

        let admitted = TransactionRegistrationFacts {
            ownership: TransactionRegistrationOwnershipFacts {
                is_coordinator: true,
                producer_id_valid: true,
                entry_exists: true,
            },
            identity: TransactionRegistrationIdentityFacts {
                pending_transition: false,
                matching: TransactionRegistrationIdentityMatchFacts {
                    transactional_id_matches: true,
                    producer_id_matches: true,
                    producer_epoch_matches: true,
                },
            },
            state: TransactionRegistrationStateFacts {
                state_allows_registration: true,
                state_is_ongoing: true,
                exact_partitions_registered: false,
            },
        };

        let mut facts = admitted;
        facts.ownership.is_coordinator = false;
        assert!(transaction_partition_registration(facts) == RejectNotCoordinator);
        for malformed in [
            (false, true, true),
            (true, false, true),
            (true, true, false),
        ] {
            let mut facts = admitted;
            facts.ownership.producer_id_valid = malformed.0;
            facts.ownership.entry_exists = malformed.1;
            facts.identity.matching.transactional_id_matches = malformed.2;
            assert!(transaction_partition_registration(facts) == RejectUnknownProducer);
        }

        // The pending-transition check runs ahead of the producer id and
        // epoch checks, so it wins even when both of those also mismatch.
        let mut facts = admitted;
        facts.identity.pending_transition = true;
        facts.identity.matching.producer_id_matches = false;
        facts.identity.matching.producer_epoch_matches = false;
        assert!(transaction_partition_registration(facts) == RejectPendingTransition);

        let mut facts = admitted;
        facts.identity.matching.producer_id_matches = false;
        assert!(transaction_partition_registration(facts) == RejectProducerId);

        let mut facts = admitted;
        facts.identity.matching.producer_epoch_matches = false;
        assert!(transaction_partition_registration(facts) == RejectProducerEpoch);

        let mut facts = admitted;
        facts.state.state_allows_registration = false;
        assert!(transaction_partition_registration(facts) == RejectState);

        let mut facts = admitted;
        facts.state.exact_partitions_registered = true;
        assert!(transaction_partition_registration(facts) == PersistRetry);

        // The retry optimization requires the current state to be exactly
        // Ongoing; a stale exact match left over from a completed or
        // not-yet-started transaction must still persist.
        let mut facts = admitted;
        facts.state.exact_partitions_registered = true;
        facts.state.state_is_ongoing = false;
        assert!(transaction_partition_registration(facts) == PersistRegistration);

        assert!(transaction_partition_registration(admitted) == PersistRegistration);
    }

    #[test]
    fn local_lso_marker_and_aborted_interval_decisions_fail_closed() {
        assert2::assert!(first_unstable_offset(&[], 20) == Some(20));
        assert2::assert!(first_unstable_offset(&[20], 20) == Some(20));
        assert2::assert!(first_unstable_offset(&[9, 3, 14], 20) == Some(3));
        assert2::assert!(first_unstable_offset(&[9, 21], 20).is_none());
        // A start beyond the log end rejects even behind a lower start.
        assert2::assert!(first_unstable_offset(&[5, 30], 20).is_none());
        assert2::assert!(transaction_marker_closes(true, false, true));
        assert2::assert!(!transaction_marker_closes(false, false, true));
        assert2::assert!(!transaction_marker_closes(true, false, false));
        assert2::assert!(aborted_transaction_interval(Some(7), 7, 0) == Some((7, 7)));
        assert2::assert!(aborted_transaction_interval(Some(3), 7, 1) == Some((3, 7)));
        assert2::assert!(aborted_transaction_interval(Some(8), 7, 1).is_none());
        assert2::assert!(aborted_transaction_interval(Some(3), 7, -1).is_none());
        assert2::assert!(aborted_transaction_interval(None, 7, 1).is_none());
        assert2::assert!(aborted_transaction_overlaps(10, 14, 0, 11));
        assert2::assert!(!aborted_transaction_overlaps(10, 14, 0, 10));
        assert2::assert!(!aborted_transaction_overlaps(14, 10, 0, 20));
        assert2::assert!(!aborted_transaction_overlaps(10, 14, 20, 20));
        assert2::assert!(!aborted_transaction_overlaps(10, 14, 12, 12));
    }

    /// Kafka `TransactionStateManager.timedOutTransactions`.
    #[test]
    fn idle_transaction_reaper_matches_kafka_timed_out_transactions() {
        use IdleTransactionState::{Ongoing, Other};

        // (state, timeout, start, now, expected).
        let cases = [
            // `txnStartTimestamp + txnTimeoutMs < now` is strict.
            (Ongoing, 60_000, 0, 60_001, true),
            (Ongoing, 60_000, 0, 60_000, false),
            (Ongoing, 60_000, 0, 59_999, false),
            (Ongoing, 1, 10, 11, false),
            (Ongoing, 1, 10, 12, true),
            // Only an ONGOING transaction times out.
            (Other, 1, 0, i64::MAX, false),
            // KIP-939: a two-phase-commit transaction never times out.
            (Ongoing, NO_TRANSACTION_TIMEOUT_MS, 0, i64::MAX, false),
            (
                Ongoing,
                NO_TRANSACTION_TIMEOUT_MS,
                i64::MIN,
                i64::MAX,
                false,
            ),
            // A backwards clock never aborts a nonnegative timeout.
            (Ongoing, 60_000, 100_000, 0, false),
            (Ongoing, 0, 10, 9, false),
            (Ongoing, 0, 10, 10, false),
            (Ongoing, 0, i64::MAX, i64::MIN, false),
            // A zero timeout expires one millisecond after the start.
            (Ongoing, 0, 10, 11, true),
            // A negative timeout from a foreign log record is arithmetic too.
            (Ongoing, -5, 10, 6, true),
            (Ongoing, -5, 10, 5, false),
            // The elapsed time does not wrap at the `i64` edges.
            (Ongoing, 60_000, i64::MIN, i64::MAX, true),
            (Ongoing, i32::MAX - 1, i64::MAX, i64::MIN, false),
        ];
        for (state, timeout, start, now, expected) in cases {
            assert!(
                should_abort_idle_transaction(state, timeout, start, now) == expected,
                "state={state:?}, timeout={timeout}, start={start}, now={now}"
            );
        }
    }

    #[test]
    fn producer_identity_boundary_table() {
        let cases = [
            (false, false, i16::MAX, None, Some((7, i16::MAX))),
            (false, true, i16::MAX, Some(11), Some((7, i16::MAX))),
            (true, false, i16::MAX - 2, None, Some((7, i16::MAX - 1))),
            (true, false, i16::MAX - 1, None, None),
            (true, false, i16::MAX - 1, Some(11), Some((11, 0))),
            (true, true, i16::MAX - 1, None, Some((7, i16::MAX))),
            (true, true, i16::MAX, None, None),
            (true, true, i16::MAX, Some(11), Some((11, 0))),
        ];
        for (verified, recovery, epoch, fresh, expected) in cases {
            assert!(
                next_producer_identity(verified, recovery, 7, epoch, fresh) == expected,
                "verified={verified}, recovery={recovery}, epoch={epoch}, fresh={fresh:?}"
            );
        }
    }

    /// Kafka `isValidProducerId` and `prepareIncrementProducerEpoch`.
    #[test]
    fn init_producer_id_identity_bumps_retries_or_fences() {
        use InitProducerIdIdentityDecision::{Bump, BumpWithoutIdentity, Fenced, Retry};

        // (entry pid, entry epoch, last epoch, prev pid, request pid,
        //  request epoch, expected).
        let cases = [
            (
                7_i64,
                4_i16,
                -1_i16,
                -1_i64,
                -1_i64,
                -1_i16,
                BumpWithoutIdentity,
            ),
            (7, 4, -1, -1, -1, 4, BumpWithoutIdentity),
            (7, 4, -1, -1, 7, 4, Bump),
            (7, 5, 4, -1, 7, 4, Retry),
            (7, 5, 4, -1, 7, 3, Fenced),
            (7, 4, -1, -1, 7, 5, Fenced),
            (7, 4, -1, -1, 9, 4, Fenced),
            // A producer id rotated at the epoch ceiling: the old id retries
            // with its exhausted epoch and gets the rotated identity back.
            (
                11,
                0,
                EXHAUSTED_PRODUCER_EPOCH,
                7,
                7,
                EXHAUSTED_PRODUCER_EPOCH,
                Retry,
            ),
            (11, 0, EXHAUSTED_PRODUCER_EPOCH, 7, 7, i16::MAX, Fenced),
            (11, 0, EXHAUSTED_PRODUCER_EPOCH, 7, 7, 5, Fenced),
            (11, 0, -1, 7, 7, EXHAUSTED_PRODUCER_EPOCH, Fenced),
        ];
        for (entry_pid, entry_epoch, last_epoch, prev_pid, pid, epoch, expected) in cases {
            assert!(
                init_producer_id_identity_decision(
                    entry_pid,
                    entry_epoch,
                    last_epoch,
                    prev_pid,
                    pid,
                    epoch,
                ) == expected,
                "entry=({entry_pid}, {entry_epoch}), last={last_epoch}, \
                 prev={prev_pid}, request=({pid}, {epoch})"
            );
        }
    }

    #[test]
    fn completion_requires_the_prepared_identity_and_state() {
        use TransactionCompletionDecision::{Proceed, RejectStaleIdentity, RejectState};

        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 7,
                    epoch: 3,
                    state: PREPARE_COMMIT,
                },
                TransactionIdentity { pid: 7, epoch: 3 },
                TransactionIdentity { pid: 7, epoch: 4 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == Proceed
        );
        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 7,
                    epoch: 4,
                    state: PREPARE_COMMIT,
                },
                TransactionIdentity { pid: 7, epoch: 3 },
                TransactionIdentity { pid: 7, epoch: 4 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == RejectStaleIdentity
        );
        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 7,
                    epoch: 3,
                    state: ONGOING,
                },
                TransactionIdentity { pid: 7, epoch: 3 },
                TransactionIdentity { pid: 7, epoch: 4 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == RejectState
        );
    }

    #[test]
    fn only_the_intended_completion_is_idempotent() {
        use TransactionCompletionDecision::{AlreadyComplete, RejectState};

        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 11,
                    epoch: 0,
                    state: COMPLETE_COMMIT,
                },
                TransactionIdentity {
                    pid: 7,
                    epoch: i16::MAX,
                },
                TransactionIdentity { pid: 11, epoch: 0 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == AlreadyComplete
        );
        assert!(
            transaction_completion_decision(
                TransactionSnapshot {
                    pid: 7,
                    epoch: i16::MAX,
                    state: COMPLETE_COMMIT,
                },
                TransactionIdentity {
                    pid: 7,
                    epoch: i16::MAX,
                },
                TransactionIdentity { pid: 11, epoch: 0 },
                PREPARE_COMMIT,
                COMPLETE_COMMIT,
            ) == RejectState
        );
    }
}
