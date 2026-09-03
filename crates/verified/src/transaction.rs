//! Transaction-completion fencing after the `EndTxn` marker fan-out.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

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

/// Whether the idle reaper may publish one prepared abort completion.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TransactionReaperCompletionDecision {
    RejectMalformed,
    RejectStaleIdentity,
    RejectChangedPreparedState,
    AlreadyComplete,
    Proceed,
}

/// Recheck the exact prepared snapshot after abort-marker dispatch.
///
/// `exact_prepared_snapshot` is supplied by the host from equality over the
/// complete persisted transaction entry, including its staged identity,
/// partition set, timeout, and timestamps.
#[ensures((result == TransactionReaperCompletionDecision::RejectMalformed)
    == (current.0@ < 0
        || current.1@ < 0
        || prepared.0@ < 0
        || prepared.1@ < 0
        || completion.0@ < 0
        || completion.1@ < 0
        || prepared.2 == completion.2))]
#[ensures((result == TransactionReaperCompletionDecision::AlreadyComplete)
    == (current.0@ >= 0
        && current.1@ >= 0
        && prepared.0@ >= 0
        && prepared.1@ >= 0
        && completion.0@ >= 0
        && completion.1@ >= 0
        && prepared.2 != completion.2
        && current.0@ == completion.0@
        && current.1@ == completion.1@
        && current.2 == completion.2))]
#[ensures((result == TransactionReaperCompletionDecision::Proceed)
    == (current.0@ >= 0
        && current.1@ >= 0
        && prepared.0@ >= 0
        && prepared.1@ >= 0
        && completion.0@ >= 0
        && completion.1@ >= 0
        && prepared.2 != completion.2
        && !(current.0@ == completion.0@
            && current.1@ == completion.1@
            && current.2 == completion.2)
        && current.0@ == prepared.0@
        && current.1@ == prepared.1@
        && current.2 == prepared.2
        && exact_prepared_snapshot))]
#[must_use]
pub fn transaction_reaper_completion_decision(
    current: (i64, i16, i8),
    prepared: (i64, i16, i8),
    completion: (i64, i16, i8),
    exact_prepared_snapshot: bool,
) -> TransactionReaperCompletionDecision {
    let (current_pid, current_epoch, current_state) = current;
    let (prepared_pid, prepared_epoch, prepare_state) = prepared;
    let (completion_pid, completion_epoch, complete_state) = completion;
    if current_pid < 0
        || current_epoch < 0
        || prepared_pid < 0
        || prepared_epoch < 0
        || completion_pid < 0
        || completion_epoch < 0
        || prepare_state == complete_state
    {
        TransactionReaperCompletionDecision::RejectMalformed
    } else if current_pid == completion_pid
        && current_epoch == completion_epoch
        && current_state == complete_state
    {
        TransactionReaperCompletionDecision::AlreadyComplete
    } else if current_pid != prepared_pid || current_epoch != prepared_epoch {
        TransactionReaperCompletionDecision::RejectStaleIdentity
    } else if current_state != prepare_state || !exact_prepared_snapshot {
        TransactionReaperCompletionDecision::RejectChangedPreparedState
    } else {
        TransactionReaperCompletionDecision::Proceed
    }
}

/// Fence a transaction marker against the partition's latest producer and
/// coordinator generations, suppress an exact completed retry, and publish
/// offsets only for a pending commit on `__consumer_offsets`.
#[ensures((result == TransactionMarkerMaterializationDecision::RejectMalformed)
    == (request.0@ < 0
        || request.1@ < 0
        || request.2@ < -1
        || current.0@ < -1
        || current.1@ < -1
        || (current.2 && current.0@ == -1)))]
#[ensures((result == TransactionMarkerMaterializationDecision::RejectProducerEpoch)
    == (request.0@ >= 0
        && request.1@ >= 0
        && request.2@ >= -1
        && current.0@ >= -1
        && current.1@ >= -1
        && (!current.2 || current.0@ >= 0)
        && request.1@ < current.0@))]
#[ensures((result == TransactionMarkerMaterializationDecision::RejectCoordinatorEpoch)
    == (request.0@ >= 0
        && request.1@ >= 0
        && request.2@ >= -1
        && current.0@ >= -1
        && current.1@ >= -1
        && (!current.2 || current.0@ >= 0)
        && request.1@ >= current.0@
        && request.2@ < current.1@))]
#[ensures((result == TransactionMarkerMaterializationDecision::Retry)
    == (request.0@ >= 0
        && request.1@ >= 0
        && request.2@ >= -1
        && request.1@ >= current.0@
        && request.2@ >= current.1@
        && current.0@ >= -1
        && current.1@ >= -1
        && !current.2
        && request.1@ == current.0@
        && request.2@ == current.1@))]
#[ensures((result == TransactionMarkerMaterializationDecision::AppendAndPublishOffsets)
    == (request.0@ >= 0
        && request.1@ >= 0
        && request.2@ >= -1
        && current.0@ >= -1
        && current.1@ >= -1
        && (!current.2 || current.0@ >= 0)
        && request.1@ >= current.0@
        && request.2@ >= current.1@
        && current.2
        && marker.0
        && marker.1))]
#[ensures((result == TransactionMarkerMaterializationDecision::AppendWithoutOffsetPublication)
    == (request.0@ >= 0
        && request.1@ >= 0
        && request.2@ >= -1
        && current.0@ >= -1
        && current.1@ >= -1
        && (!current.2 || current.0@ >= 0)
        && request.1@ >= current.0@
        && request.2@ >= current.1@
        && !(!current.2
            && request.1@ == current.0@
            && request.2@ == current.1@)
        && !(current.2 && marker.0 && marker.1)))]
#[must_use]
pub fn transaction_marker_materialization_decision(
    request: (i64, i16, i32),
    current: (i16, i32, bool),
    marker: (bool, bool),
) -> TransactionMarkerMaterializationDecision {
    let (producer_id, producer_epoch, coordinator_epoch) = request;
    let (current_producer_epoch, current_coordinator_epoch, has_pending_transaction) = current;
    let (is_commit, is_offsets_partition) = marker;
    if producer_id < 0
        || producer_epoch < 0
        || coordinator_epoch < -1
        || current_producer_epoch < -1
        || current_coordinator_epoch < -1
        || (has_pending_transaction && current_producer_epoch == -1)
    {
        TransactionMarkerMaterializationDecision::RejectMalformed
    } else if producer_epoch < current_producer_epoch {
        TransactionMarkerMaterializationDecision::RejectProducerEpoch
    } else if coordinator_epoch < current_coordinator_epoch {
        TransactionMarkerMaterializationDecision::RejectCoordinatorEpoch
    } else if !has_pending_transaction
        && producer_epoch == current_producer_epoch
        && coordinator_epoch == current_coordinator_epoch
    {
        TransactionMarkerMaterializationDecision::Retry
    } else if has_pending_transaction && is_commit && is_offsets_partition {
        TransactionMarkerMaterializationDecision::AppendAndPublishOffsets
    } else {
        TransactionMarkerMaterializationDecision::AppendWithoutOffsetPublication
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
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TransactionRegistrationDecision {
    RejectNotCoordinator,
    RejectUnknownProducer,
    RejectStagedIdentity,
    RejectStaleIdentity,
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
    pub transactional_id_matches: bool,
    pub staged_identity: bool,
    pub producer_identity_matches: bool,
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TransactionRegistrationStateFacts {
    pub state_allows_registration: bool,
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
            || !facts.identity.transactional_id_matches)))]
#[ensures((result == TransactionRegistrationDecision::RejectStagedIdentity)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.transactional_id_matches
        && facts.identity.staged_identity))]
#[ensures((result == TransactionRegistrationDecision::RejectStaleIdentity)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.transactional_id_matches
        && !facts.identity.staged_identity
        && !facts.identity.producer_identity_matches))]
#[ensures((result == TransactionRegistrationDecision::RejectState)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.transactional_id_matches
        && !facts.identity.staged_identity
        && facts.identity.producer_identity_matches
        && !facts.state.state_allows_registration))]
#[ensures((result == TransactionRegistrationDecision::PersistRetry)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.transactional_id_matches
        && !facts.identity.staged_identity
        && facts.identity.producer_identity_matches
        && facts.state.state_allows_registration
        && facts.state.exact_partitions_registered))]
#[ensures((result == TransactionRegistrationDecision::PersistRegistration)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.transactional_id_matches
        && !facts.identity.staged_identity
        && facts.identity.producer_identity_matches
        && facts.state.state_allows_registration
        && !facts.state.exact_partitions_registered))]
#[must_use]
pub fn transaction_partition_registration(
    facts: TransactionRegistrationFacts,
) -> TransactionRegistrationDecision {
    if !facts.ownership.is_coordinator {
        TransactionRegistrationDecision::RejectNotCoordinator
    } else if !facts.ownership.producer_id_valid
        || !facts.ownership.entry_exists
        || !facts.identity.transactional_id_matches
    {
        TransactionRegistrationDecision::RejectUnknownProducer
    } else if facts.identity.staged_identity {
        TransactionRegistrationDecision::RejectStagedIdentity
    } else if !facts.identity.producer_identity_matches {
        TransactionRegistrationDecision::RejectStaleIdentity
    } else if !facts.state.state_allows_registration {
        TransactionRegistrationDecision::RejectState
    } else if facts.state.exact_partitions_registered {
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
#[ensures(match result {
    Some(lso) => lso@ <= log_end@
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

/// Resolve the persisted timeout without colliding with the 2PC sentinel.
#[requires(0 < min_timeout_ms@)]
#[requires(min_timeout_ms@ <= max_timeout_ms@)]
#[requires(max_timeout_ms@ < i32::MAX@)]
#[ensures(enable_2pc ==> result@ == i32::MAX@)]
#[ensures(!enable_2pc ==> min_timeout_ms@ <= result@ && result@ <= max_timeout_ms@)]
#[ensures(!enable_2pc && requested_ms@ < min_timeout_ms@ ==> result@ == min_timeout_ms@)]
#[ensures(!enable_2pc && requested_ms@ > max_timeout_ms@ ==> result@ == max_timeout_ms@)]
#[ensures(!enable_2pc && min_timeout_ms@ <= requested_ms@ && requested_ms@ <= max_timeout_ms@
    ==> result@ == requested_ms@)]
#[must_use]
pub fn resolve_transaction_timeout(
    enable_2pc: bool,
    requested_ms: i32,
    min_timeout_ms: i32,
    max_timeout_ms: i32,
) -> i32 {
    if enable_2pc {
        NO_TRANSACTION_TIMEOUT_MS
    } else if requested_ms < min_timeout_ms {
        min_timeout_ms
    } else if requested_ms > max_timeout_ms {
        max_timeout_ms
    } else {
        requested_ms
    }
}

/// Whether the idle reaper may abort one persisted transaction.
#[requires(0 < txn_timeout_ms@)]
#[ensures(state != IdleTransactionState::Ongoing ==> !result)]
#[ensures(txn_timeout_ms@ == i32::MAX@ ==> !result)]
#[ensures(result ==> state == IdleTransactionState::Ongoing
    && txn_timeout_ms@ != i32::MAX@
    && now_ms@ - start_ms@ >= txn_timeout_ms@)]
#[ensures(state == IdleTransactionState::Ongoing
    && txn_timeout_ms@ != i32::MAX@
    && now_ms@ - start_ms@ >= txn_timeout_ms@ ==> result)]
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
    if !ongoing || txn_timeout_ms == NO_TRANSACTION_TIMEOUT_MS || now_ms < start_ms {
        return false;
    }
    now_ms.saturating_sub(start_ms) >= i64::from(txn_timeout_ms)
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

/// Choose the producer identity exposed after transaction completion.
///
/// Verified normal completion reserves `i16::MAX` for the transaction marker,
/// while a staged recovery identity may use that epoch once before rotating.
#[cfg_attr(creusot, ensures(!verified ==> result == Some((pid, epoch))))]
#[cfg_attr(
    creusot,
    ensures(
        verified && !recovery && epoch@ < i16::MAX@ - 1 ==>
            match result {
                Some((result_pid, result_epoch)) =>
                    result_pid == pid && result_epoch@ == epoch@ + 1,
                None => false,
            }
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        verified && recovery && epoch@ < i16::MAX@ ==>
            match result {
                Some((result_pid, result_epoch)) =>
                    result_pid == pid && result_epoch@ == epoch@ + 1,
                None => false,
            }
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        verified
            && ((!recovery && epoch@ >= i16::MAX@ - 1)
                || (recovery && epoch@ >= i16::MAX@)) ==>
            match (result, fresh) {
                (Some((result_pid, result_epoch)), Some(fresh_pid)) =>
                    result_pid == fresh_pid && result_epoch@ == 0,
                (None, None) => true,
                _ => false,
            }
    )
)]
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

/// Whether an `InitProducerId` caller's supplied producer identity may
/// re-initialise the transactional id it names.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum InitProducerIdFencingDecision {
    NoIdentity,
    Admit,
    Fenced,
}

/// Fence a stale `(producer_id, producer_epoch)` on `InitProducerId`
/// (KIP-360).
///
/// A request producer id of `-1` supplies no identity, which every
/// `InitProducerId` below v3 and every first initialisation does; such a
/// caller is neither admitted nor fenced on identity grounds. An identity that
/// is supplied must name the entry's live producer id, and carry either the
/// entry's live epoch or, when an epoch fence failed after it was prepared,
/// the epoch the entry held before that fence.
#[ensures((result == InitProducerIdFencingDecision::NoIdentity) == (request_pid@ == -1))]
#[ensures((result == InitProducerIdFencingDecision::Admit)
    == (request_pid@ != -1
        && request_pid@ == entry_pid@
        && (request_epoch@ == entry_epoch@
            || (has_failed_epoch_fence && request_epoch@ == last_epoch@))))]
#[ensures((result == InitProducerIdFencingDecision::Fenced)
    == (request_pid@ != -1
        && !(request_pid@ == entry_pid@
            && (request_epoch@ == entry_epoch@
                || (has_failed_epoch_fence && request_epoch@ == last_epoch@)))))]
#[must_use]
pub fn init_producer_id_fencing_decision(
    entry_pid: i64,
    entry_epoch: i16,
    last_epoch: i16,
    has_failed_epoch_fence: bool,
    request_pid: i64,
    request_epoch: i16,
) -> InitProducerIdFencingDecision {
    if request_pid == -1 {
        return InitProducerIdFencingDecision::NoIdentity;
    }
    let epoch_valid =
        request_epoch == entry_epoch || (has_failed_epoch_fence && request_epoch == last_epoch);
    if request_pid == entry_pid && epoch_valid {
        InitProducerIdFencingDecision::Admit
    } else {
        InitProducerIdFencingDecision::Fenced
    }
}

/// Revalidate the transaction entry after the marker fan-out released its lock.
#[cfg_attr(creusot, requires(prepare_state != complete_state))]
#[cfg_attr(
    creusot,
    ensures(
        current.pid == completion.pid
            && current.epoch == completion.epoch
            && current.state == complete_state
            ==> result == TransactionCompletionDecision::AlreadyComplete
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        result == TransactionCompletionDecision::AlreadyComplete
            ==> current.pid == completion.pid
                && current.epoch == completion.epoch
                && current.state == complete_state
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        current.pid == expected.pid
            && current.epoch == expected.epoch
            && current.state == prepare_state
            ==> result == TransactionCompletionDecision::Proceed
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        result == TransactionCompletionDecision::Proceed
            ==> current.pid == expected.pid
                && current.epoch == expected.epoch
                && current.state == prepare_state
                && !(current.pid == completion.pid
                    && current.epoch == completion.epoch
                    && current.state == complete_state)
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        !(current.pid == completion.pid
            && current.epoch == completion.epoch
            && current.state == complete_state)
            && (current.pid != expected.pid || current.epoch != expected.epoch)
            ==> result == TransactionCompletionDecision::RejectStaleIdentity
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        result == TransactionCompletionDecision::RejectStaleIdentity
            ==> !(current.pid == completion.pid
                && current.epoch == completion.epoch
                && current.state == complete_state)
                && (current.pid != expected.pid || current.epoch != expected.epoch)
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        current.pid == expected.pid
            && current.epoch == expected.epoch
            && current.state != prepare_state
            && !(current.pid == completion.pid
                && current.epoch == completion.epoch
                && current.state == complete_state)
            ==> result == TransactionCompletionDecision::RejectState
    )
)]
#[cfg_attr(
    creusot,
    ensures(
        result == TransactionCompletionDecision::RejectState
            ==> current.pid == expected.pid
                && current.epoch == expected.epoch
                && current.state != prepare_state
                && !(current.pid == completion.pid
                    && current.epoch == completion.epoch
                    && current.state == complete_state)
    )
)]
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

    const PREPARE_COMMIT: i8 = 2;
    const COMPLETE_COMMIT: i8 = 4;

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

    #[test]
    fn marker_materialization_fences_retries_and_offset_publication() {
        use TransactionMarkerMaterializationDecision::{
            AppendAndPublishOffsets, AppendWithoutOffsetPublication, RejectCoordinatorEpoch,
            RejectMalformed, RejectProducerEpoch, Retry,
        };

        assert!(
            transaction_marker_materialization_decision((-1, 0, 0), (-1, -1, true), (true, true))
                == RejectMalformed
        );
        assert!(
            transaction_marker_materialization_decision((1, 0, 0), (-2, -1, true), (true, true))
                == RejectMalformed
        );
        assert!(
            transaction_marker_materialization_decision((1, 0, -2), (0, -1, true), (false, false))
                == RejectMalformed
        );
        assert!(
            transaction_marker_materialization_decision((1, 0, -1), (0, -1, true), (false, false))
                == AppendWithoutOffsetPublication
        );
        assert!(
            transaction_marker_materialization_decision((1, 2, 9), (3, 8, true), (true, true))
                == RejectProducerEpoch
        );
        assert!(
            transaction_marker_materialization_decision((1, 3, 7), (3, 8, true), (true, true))
                == RejectCoordinatorEpoch
        );
        assert!(
            transaction_marker_materialization_decision((1, 3, 8), (3, 8, false), (true, true))
                == Retry
        );
        assert!(
            transaction_marker_materialization_decision((1, 3, 8), (3, 7, true), (true, true))
                == AppendAndPublishOffsets
        );
        assert!(
            transaction_marker_materialization_decision((1, 3, 8), (3, 7, true), (false, true))
                == AppendWithoutOffsetPublication
        );
        assert!(
            transaction_marker_materialization_decision((1, 4, 9), (3, 8, false), (true, true))
                == AppendWithoutOffsetPublication
        );
    }

    #[test]
    fn reaper_completion_requires_the_exact_prepared_snapshot() {
        use TransactionReaperCompletionDecision::{
            AlreadyComplete, Proceed, RejectChangedPreparedState, RejectMalformed,
            RejectStaleIdentity,
        };

        assert!(
            transaction_reaper_completion_decision((7, 3, 3), (7, 3, 3), (7, 4, 5), true)
                == Proceed
        );
        assert!(
            transaction_reaper_completion_decision((7, 3, 3), (7, 3, 3), (7, 4, 5), false)
                == RejectChangedPreparedState
        );
        assert!(
            transaction_reaper_completion_decision((7, 4, 3), (7, 3, 3), (7, 4, 5), false)
                == RejectStaleIdentity
        );
        assert!(
            transaction_reaper_completion_decision((7, 4, 5), (7, 3, 3), (7, 4, 5), false)
                == AlreadyComplete
        );
        assert!(
            transaction_reaper_completion_decision((-1, 0, 3), (7, 3, 3), (7, 4, 5), false)
                == RejectMalformed
        );
    }

    #[test]
    fn partition_registration_fences_generation_and_retries_exactly() {
        use TransactionRegistrationDecision::{
            PersistRegistration, PersistRetry, RejectNotCoordinator, RejectStagedIdentity,
            RejectStaleIdentity, RejectState, RejectUnknownProducer,
        };

        let admitted = TransactionRegistrationFacts {
            ownership: TransactionRegistrationOwnershipFacts {
                is_coordinator: true,
                producer_id_valid: true,
                entry_exists: true,
            },
            identity: TransactionRegistrationIdentityFacts {
                transactional_id_matches: true,
                staged_identity: false,
                producer_identity_matches: true,
            },
            state: TransactionRegistrationStateFacts {
                state_allows_registration: true,
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
            facts.identity.transactional_id_matches = malformed.2;
            assert!(transaction_partition_registration(facts) == RejectUnknownProducer);
        }

        let mut facts = admitted;
        facts.identity.staged_identity = true;
        assert!(transaction_partition_registration(facts) == RejectStagedIdentity);

        let mut facts = admitted;
        facts.identity.producer_identity_matches = false;
        assert!(transaction_partition_registration(facts) == RejectStaleIdentity);

        let mut facts = admitted;
        facts.state.state_allows_registration = false;
        assert!(transaction_partition_registration(facts) == RejectState);

        let mut facts = admitted;
        facts.state.exact_partitions_registered = true;
        assert!(transaction_partition_registration(facts) == PersistRetry);

        assert!(transaction_partition_registration(admitted) == PersistRegistration);
    }

    #[test]
    fn local_lso_marker_and_aborted_interval_decisions_fail_closed() {
        assert2::assert!(first_unstable_offset(&[], 20) == Some(20));
        assert2::assert!(first_unstable_offset(&[9, 3, 14], 20) == Some(3));
        assert2::assert!(first_unstable_offset(&[9, 21], 20).is_none());
        assert2::assert!(transaction_marker_closes(true, false, true));
        assert2::assert!(!transaction_marker_closes(false, false, true));
        assert2::assert!(!transaction_marker_closes(true, false, false));
        assert2::assert!(aborted_transaction_interval(Some(3), 7, 1) == Some((3, 7)));
        assert2::assert!(aborted_transaction_interval(Some(8), 7, 1).is_none());
        assert2::assert!(aborted_transaction_interval(Some(3), 7, -1).is_none());
        assert2::assert!(aborted_transaction_interval(None, 7, 1).is_none());
        assert2::assert!(aborted_transaction_overlaps(10, 14, 0, 11));
        assert2::assert!(!aborted_transaction_overlaps(10, 14, 0, 10));
        assert2::assert!(!aborted_transaction_overlaps(14, 10, 0, 20));
        assert2::assert!(!aborted_transaction_overlaps(10, 14, 20, 20));
    }

    #[test]
    fn two_pc_timeout_and_reaper_are_fail_closed() {
        assert2::assert!(resolve_transaction_timeout(true, -1, 2_000, 8_000) == i32::MAX);
        assert2::assert!(resolve_transaction_timeout(false, -1, 2_000, 8_000) == 2_000);
        assert2::assert!(resolve_transaction_timeout(false, i32::MAX, 2_000, 8_000) == 8_000);
        assert2::assert!(!should_abort_idle_transaction(
            IdleTransactionState::Ongoing,
            i32::MAX,
            0,
            i64::MAX,
        ));
        assert2::assert!(!should_abort_idle_transaction(
            IdleTransactionState::Other,
            1,
            0,
            i64::MAX,
        ));
        assert2::assert!(!should_abort_idle_transaction(
            IdleTransactionState::Ongoing,
            1,
            10,
            9,
        ));
        assert2::assert!(should_abort_idle_transaction(
            IdleTransactionState::Ongoing,
            1,
            10,
            11,
        ));
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

    #[test]
    fn init_producer_id_fencing_admits_only_the_live_or_failed_fence_identity() {
        use InitProducerIdFencingDecision::{Admit, Fenced, NoIdentity};

        // (entry pid, entry epoch, last epoch, failed fence, request pid,
        //  request epoch, expected).
        let cases = [
            (7_i64, 4_i16, -1_i16, false, -1_i64, -1_i16, NoIdentity),
            (7, 4, -1, false, -1, 4, NoIdentity),
            (7, 4, -1, false, 7, 4, Admit),
            (7, 4, -1, false, 7, 3, Fenced),
            (7, 4, -1, false, 7, 5, Fenced),
            (7, 4, -1, false, 9, 4, Fenced),
            (7, 5, 4, true, 7, 4, Admit),
            (7, 5, 4, true, 7, 5, Admit),
            (7, 5, 4, false, 7, 4, Fenced),
            (7, 5, 4, true, 7, 3, Fenced),
            (7, 5, 4, true, 9, 4, Fenced),
        ];
        for (entry_pid, entry_epoch, last_epoch, failed_fence, pid, epoch, expected) in cases {
            assert!(
                init_producer_id_fencing_decision(
                    entry_pid,
                    entry_epoch,
                    last_epoch,
                    failed_fence,
                    pid,
                    epoch,
                ) == expected,
                "entry=({entry_pid}, {entry_epoch}), last={last_epoch}, \
                 failed_fence={failed_fence}, request=({pid}, {epoch})"
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
                    state: 1,
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
