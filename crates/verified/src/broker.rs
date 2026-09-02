//! Pure, safety-critical decision kernels used by `krabka-broker`.
//!
//! Keeping these small arithmetic decisions here lets Creusot prove the exact
//! executable bodies used by the asynchronous broker.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Visibility bounds and response watermarks for one Fetch partition.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FetchVisibility {
    pub out_of_range: bool,
    pub empty: bool,
    pub limit_offset: i64,
    pub effective_lso: i64,
    pub read_committed_aborts: bool,
    pub response_hw: i64,
    pub response_lso: i64,
}

/// The partition offsets one Fetch visibility decision reads.
///
/// They are one struct because they are five `i64` values with five different
/// meanings, and a transposed call site would compile.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FetchWatermarks {
    /// First offset the log still holds. Below it a fetch is out of range.
    pub log_start: i64,
    /// High watermark: the exclusive bound of what the ISR has replicated.
    pub hw: i64,
    /// Last stable offset: the first offset an open transaction may cover.
    pub lso: i64,
    /// Log end offset: the exclusive bound of what the leader holds.
    pub log_end: i64,
    /// KFC-1 delivery watermark: the first offset that is not due yet.
    pub deliverable: i64,
}

/// The only direct mutation class selected from one follower Fetch response.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ReplicaFetchMutation {
    /// The response is not for the exact live request target.
    Reject,
    /// The response is fenced or otherwise unsuccessful; error handling may
    /// retry or enter a separately guarded recovery path.
    Retry,
    /// Apply the KIP-320 divergence boundary and return without appending.
    Truncate,
    /// Apply the successful response batches, then its high watermark.
    Append,
}

/// Fence one follower Fetch response against its topic, partition, in-flight
/// request epoch, current metadata target, and any reported leader identity;
/// then select one exclusive response action.
#[ensures((result == ReplicaFetchMutation::Reject) == (
    !identity.0
        || !identity.1
        || epochs.0@ != epochs.1@
        || !target.0
        || !target.1
))]
#[ensures((result == ReplicaFetchMutation::Retry) == (
    identity.0
        && identity.1
        && epochs.0@ == epochs.1@
        && target.0
        && target.1
        && !outcome.0
))]
#[ensures((result == ReplicaFetchMutation::Truncate) == (
    identity.0
        && identity.1
        && epochs.0@ == epochs.1@
        && target.0
        && target.1
        && outcome.0
        && outcome.1
))]
#[ensures((result == ReplicaFetchMutation::Append) == (
    identity.0
        && identity.1
        && epochs.0@ == epochs.1@
        && target.0
        && target.1
        && outcome.0
        && !outcome.1
))]
#[must_use]
pub fn replica_fetch_mutation(
    identity: (bool, bool),
    epochs: (i32, i32),
    target: (bool, bool),
    outcome: (bool, bool),
) -> ReplicaFetchMutation {
    let (topic_matches, partition_matches) = identity;
    let (request_leader_epoch, current_leader_epoch) = epochs;
    let (target_matches, reported_target_matches) = target;
    let (response_success, has_divergence) = outcome;
    if !topic_matches
        || !partition_matches
        || request_leader_epoch != current_leader_epoch
        || !target_matches
        || !reported_target_matches
    {
        ReplicaFetchMutation::Reject
    } else if !response_success {
        ReplicaFetchMutation::Retry
    } else if has_divergence {
        ReplicaFetchMutation::Truncate
    } else {
        ReplicaFetchMutation::Append
    }
}

/// Admit one preferred-leader rebalance batch only when the scan is
/// nonempty, internally consistent, and at the exact configured threshold.
#[ensures(result == (
    total_partitions@ > 0
        && selected_changes@ > 0
        && selected_changes@ <= total_partitions@
        && changes_unique
        && all_preferred_eligible
        && threshold_met
))]
#[must_use]
pub fn preferred_rebalance_admission(
    total_partitions: u64,
    selected_changes: u64,
    changes_unique: bool,
    all_preferred_eligible: bool,
    threshold_met: bool,
) -> bool {
    total_partitions > 0
        && selected_changes > 0
        && selected_changes <= total_partitions
        && changes_unique
        && all_preferred_eligible
        && threshold_met
}

/// Compute Kafka's consumer/follower Fetch visibility window.
///
/// [`FetchWatermarks::deliverable`] is KFC-1's delivery watermark: the first
/// offset a consumer may not see yet, because the batch that starts there has
/// not reached its activation time. It caps a consumer, and it caps nothing
/// else:
///
/// - A follower is never gated. Replication carries a scheduled record to the
///   ISR, and it counts toward the high watermark, long before any consumer can
///   read it.
/// - `response_hw` and `response_lso` do not move. The broker reports the true
///   high watermark and last stable offset, so consumer lag stays honest and
///   KIP-227 watermark monotonicity is untouched.
///
/// The precondition bounds the delivery watermark inside `[log_start, hw]`,
/// which is what the caller clamps it to. The body still takes the minimum
/// against the bound it caps, so a caller that breaks the precondition gets a
/// narrower window and never a dirty read.
#[requires(0 <= w.log_start@ && w.log_start@ <= w.hw@ && w.hw@ <= w.log_end@)]
#[requires(w.log_start@ <= w.deliverable@ && w.deliverable@ <= w.hw@)]
#[ensures(result.out_of_range == (fetch_offset@ < w.log_start@))]
#[ensures(result.empty == (!(fetch_offset@ < w.log_start@)
    && fetch_offset@ >= if is_follower { w.log_end@ } else { w.deliverable@ }))]
#[ensures(result.effective_lso@ == if read_committed && !is_follower {
    if w.lso@ < w.hw@ { w.lso@ } else { w.hw@ }
} else { w.lso@ })]
#[ensures(result.read_committed_aborts == (read_committed && !is_follower))]
#[ensures(result.response_hw@ == if is_follower { w.log_end@ } else { w.hw@ })]
#[ensures(result.response_lso@ == if read_committed && !is_follower {
    if w.lso@ < w.hw@ { w.lso@ } else { w.hw@ }
} else if is_follower { w.log_end@ } else { w.hw@ })]
#[ensures(result.limit_offset@ == if is_follower { w.log_end@ } else if read_committed {
    if w.lso@ < w.deliverable@ { w.lso@ } else { w.deliverable@ }
} else { w.deliverable@ })]
#[ensures(is_follower ==> result.limit_offset@ == w.log_end@)]
#[ensures(!is_follower ==> result.limit_offset@ <= w.deliverable@)]
#[must_use]
pub fn fetch_visibility(
    is_follower: bool,
    read_committed: bool,
    w: FetchWatermarks,
    fetch_offset: i64,
) -> FetchVisibility {
    // The delivery watermark caps a consumer and never a follower.
    let visible = if w.deliverable < w.hw {
        w.deliverable
    } else {
        w.hw
    };
    let upper_bound = if is_follower { w.log_end } else { visible };
    let effective_lso = if read_committed && !is_follower {
        if w.lso < w.hw { w.lso } else { w.hw }
    } else {
        w.lso
    };
    let response_hw = if is_follower { w.log_end } else { w.hw };
    let response_lso = if read_committed && !is_follower {
        effective_lso
    } else if is_follower {
        w.log_end
    } else {
        w.hw
    };
    let limit_offset = if is_follower {
        w.log_end
    } else if read_committed {
        if effective_lso < visible {
            effective_lso
        } else {
            visible
        }
    } else {
        visible
    };
    let out_of_range = fetch_offset < w.log_start;
    FetchVisibility {
        out_of_range,
        empty: !out_of_range && fetch_offset >= upper_bound,
        limit_offset,
        effective_lso,
        read_committed_aborts: read_committed && !is_follower,
        response_hw,
        response_lso,
    }
}

/// Offset facts used to admit one `DeleteRecords` trim.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct DeleteRecordsTrimFacts {
    pub requested: i64,
    pub high_watermark: i64,
    pub log_end: i64,
    pub current_start: i64,
    pub has_delivery_watermark: bool,
    pub delivery_watermark: i64,
}

/// The complete boundary decision for one `DeleteRecords` trim.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum DeleteRecordsTrimDecision {
    RejectMalformed,
    RejectOutOfRange,
    Noop { frontier: i64 },
    Apply { frontier: i64 },
}

/// Admit a trim and cap it at every logical deletion frontier.
///
/// `-1` means the current high watermark. Explicit requests may name the
/// uncommitted tail, so the committed high watermark still caps them. A
/// scheduled topic adds its delivery watermark as a second cap. Stale and
/// repeated requests return the current start and never move it backwards.
#[must_use]
#[ensures({
    let malformed = facts.requested@ < -1
        || facts.current_start@ < 0
        || facts.high_watermark@ < facts.current_start@
        || facts.log_end@ < facts.high_watermark@
        || (facts.has_delivery_watermark
            && facts.delivery_watermark@ < facts.current_start@);
    let out_of_range = !malformed && facts.requested@ > facts.log_end@;
    let resolved = if facts.requested@ == -1 {
        facts.high_watermark@
    } else {
        facts.requested@
    };
    let committed = if resolved < facts.high_watermark@ {
        resolved
    } else {
        facts.high_watermark@
    };
    let bounded = if facts.has_delivery_watermark
        && facts.delivery_watermark@ < committed
    {
        facts.delivery_watermark@
    } else {
        committed
    };
    match result {
        DeleteRecordsTrimDecision::RejectMalformed => malformed,
        DeleteRecordsTrimDecision::RejectOutOfRange => out_of_range,
        DeleteRecordsTrimDecision::Noop { frontier } => {
            !malformed && !out_of_range
                && bounded <= facts.current_start@
                && frontier@ == facts.current_start@
        }
        DeleteRecordsTrimDecision::Apply { frontier } => {
            !malformed && !out_of_range
                && bounded > facts.current_start@
                && frontier@ == bounded
                && frontier@ <= facts.high_watermark@
                && frontier@ <= facts.log_end@
                && (!facts.has_delivery_watermark
                    || frontier@ <= facts.delivery_watermark@)
        }
    }
})]
pub const fn delete_records_trim_decision(
    facts: DeleteRecordsTrimFacts,
) -> DeleteRecordsTrimDecision {
    if facts.requested < -1
        || facts.current_start < 0
        || facts.high_watermark < facts.current_start
        || facts.log_end < facts.high_watermark
        || (facts.has_delivery_watermark && facts.delivery_watermark < facts.current_start)
    {
        return DeleteRecordsTrimDecision::RejectMalformed;
    }
    if facts.requested > facts.log_end {
        return DeleteRecordsTrimDecision::RejectOutOfRange;
    }
    let resolved = if facts.requested == -1 {
        facts.high_watermark
    } else {
        facts.requested
    };
    let committed = if resolved < facts.high_watermark {
        resolved
    } else {
        facts.high_watermark
    };
    let bounded = if facts.has_delivery_watermark {
        if committed < facts.delivery_watermark {
            committed
        } else {
            facts.delivery_watermark
        }
    } else {
        committed
    };
    if bounded <= facts.current_start {
        DeleteRecordsTrimDecision::Noop {
            frontier: facts.current_start,
        }
    } else {
        DeleteRecordsTrimDecision::Apply { frontier: bounded }
    }
}

/// The next idempotent step while reconciling WAL and local trim frontiers.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum DeleteRecordsTrimApplication {
    RejectMalformed,
    TrimWal { frontier: i64 },
    TrimLocal { frontier: i64 },
    Complete { frontier: i64 },
}

/// Choose one monotonic trim step, with WAL ordered before the local log.
///
/// Re-evaluating this function after a failed step is retry-safe: the chosen
/// frontier is the maximum of the request and both observed frontiers, so no
/// retry can regress either store. Completion requires exact equality.
#[must_use]
#[ensures({
    let frontier = if requested@ > wal_start@ {
        if requested@ > local_start@ { requested@ } else { local_start@ }
    } else if wal_start@ > local_start@ {
        wal_start@
    } else {
        local_start@
    };
    match result {
        DeleteRecordsTrimApplication::RejectMalformed => {
            requested@ < 0 || wal_start@ < 0 || local_start@ < 0
        }
        DeleteRecordsTrimApplication::TrimWal { frontier: next } => {
            requested@ >= 0 && wal_start@ >= 0 && local_start@ >= 0
                && wal_start@ < frontier && next@ == frontier
        }
        DeleteRecordsTrimApplication::TrimLocal { frontier: next } => {
            requested@ >= 0 && wal_start@ >= 0 && local_start@ >= 0
                && wal_start@ == frontier && local_start@ < frontier
                && next@ == frontier
        }
        DeleteRecordsTrimApplication::Complete { frontier: done } => {
            requested@ >= 0 && wal_start@ >= 0 && local_start@ >= 0
                && wal_start@ == frontier && local_start@ == frontier
                && done@ == frontier
        }
    }
})]
pub const fn delete_records_trim_application(
    requested: i64,
    wal_start: i64,
    local_start: i64,
) -> DeleteRecordsTrimApplication {
    if requested < 0 || wal_start < 0 || local_start < 0 {
        return DeleteRecordsTrimApplication::RejectMalformed;
    }
    let request_or_wal = if requested > wal_start {
        requested
    } else {
        wal_start
    };
    let frontier = if request_or_wal > local_start {
        request_or_wal
    } else {
        local_start
    };
    if wal_start < frontier {
        DeleteRecordsTrimApplication::TrimWal { frontier }
    } else if local_start < frontier {
        DeleteRecordsTrimApplication::TrimLocal { frontier }
    } else {
        DeleteRecordsTrimApplication::Complete { frontier }
    }
}

/// Non-negative KIP-932 backlog above the effective share start offset.
#[cfg(creusot)]
#[logic]
#[cfg_attr(test, mutants::skip)]
pub fn effective_share_backlog_model(hwm: i64, spso: i64, log_start: i64) -> Int {
    pearlite! {
        let base = if spso@ >= 0 && spso@ > log_start@ { spso@ } else { log_start@ };
        let difference = hwm@ - base;
        if difference <= 0 {
            0
        } else if difference > 9223372036854775807 {
            9223372036854775807
        } else {
            difference
        }
    }
}

#[ensures(result@ == effective_share_backlog_model(hwm, spso, log_start))]
#[must_use]
pub fn effective_share_backlog(hwm: i64, spso: i64, log_start: i64) -> i64 {
    let base = if spso >= 0 && spso > log_start {
        spso
    } else {
        log_start
    };
    let difference = hwm.saturating_sub(base);
    if difference > 0 { difference } else { 0 }
}

/// The complete per-key admission outcome for `FindCoordinator`.
///
/// The allow variants preserve the key type for the host adapter. Denials carry
/// the Kafka authorization domain, while malformed SHARE keys and unknown wire
/// discriminants fail closed as invalid requests.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FindCoordinatorAdmission {
    AllowGroup,
    AllowTransaction,
    AllowShare,
    DenyGroup,
    DenyTransaction,
    DenyCluster,
    InvalidRequest,
}

/// Decide whether one `FindCoordinator` key may proceed to coordinator lookup.
///
/// Kafka key types are GROUP=0, TRANSACTION=1, and SHARE=2. SHARE was added in
/// API version 6, uses `ClusterAction` authorization, and carries a composite key
/// that the host validates before calling this kernel. `share_key_valid` is
/// ignored for the two non-SHARE key types. Unknown key types never inherit an
/// allow result.
#[ensures(key_type@ == 0 ==> result == if acl_allowed {
    FindCoordinatorAdmission::AllowGroup
} else {
    FindCoordinatorAdmission::DenyGroup
})]
#[ensures(key_type@ == 1 ==> result == if acl_allowed {
    FindCoordinatorAdmission::AllowTransaction
} else {
    FindCoordinatorAdmission::DenyTransaction
})]
#[ensures(key_type@ == 2 ==> result == if api_version@ < 6 || !share_key_valid {
    FindCoordinatorAdmission::InvalidRequest
} else if acl_allowed {
    FindCoordinatorAdmission::AllowShare
} else {
    FindCoordinatorAdmission::DenyCluster
})]
#[ensures(key_type@ < 0 || key_type@ > 2
    ==> result == FindCoordinatorAdmission::InvalidRequest)]
#[must_use]
pub fn find_coordinator_admission(
    api_version: i16,
    key_type: i8,
    acl_allowed: bool,
    share_key_valid: bool,
) -> FindCoordinatorAdmission {
    match key_type {
        0 if acl_allowed => FindCoordinatorAdmission::AllowGroup,
        0 => FindCoordinatorAdmission::DenyGroup,
        1 if acl_allowed => FindCoordinatorAdmission::AllowTransaction,
        1 => FindCoordinatorAdmission::DenyTransaction,
        2 if api_version < 6 || !share_key_valid => FindCoordinatorAdmission::InvalidRequest,
        2 if acl_allowed => FindCoordinatorAdmission::AllowShare,
        2 => FindCoordinatorAdmission::DenyCluster,
        _ => FindCoordinatorAdmission::InvalidRequest,
    }
}

/// Admit an unclean-election commit only against the exact partition snapshot
/// used to select its winner.
#[ensures(result == (selected_partition_epoch@ == current_partition_epoch@
    && !current_leader_alive
    && selected_replicas@ == current_replicas@
    && (exists<i: Int> 0 <= i && i < current_replicas@.len()
        && current_replicas@[i] == winner)))]
#[must_use]
pub fn unclean_recovery_commit_admission(
    selected_partition_epoch: i32,
    current_partition_epoch: i32,
    selected_replicas: &[u64],
    current_replicas: &[u64],
    winner: u64,
    current_leader_alive: bool,
) -> bool {
    if selected_partition_epoch != current_partition_epoch
        || current_leader_alive
        || selected_replicas.len() != current_replicas.len()
    {
        return false;
    }

    let mut winner_assigned = false;
    let mut i = 0usize;
    #[cfg_attr(creusot, invariant(i@ <= selected_replicas@.len()))]
    #[cfg_attr(creusot, invariant(selected_replicas@.len() == current_replicas@.len()))]
    #[cfg_attr(creusot, invariant(forall<k: Int> 0 <= k && k < i@
        ==> selected_replicas@[k] == current_replicas@[k]))]
    #[cfg_attr(creusot, invariant(winner_assigned == (exists<k: Int>
        0 <= k && k < i@ && current_replicas@[k] == winner)))]
    #[cfg_attr(creusot, variant(selected_replicas@.len() - i@))]
    while i < selected_replicas.len() {
        if selected_replicas[i] != current_replicas[i] {
            return false;
        }
        if current_replicas[i] == winner {
            winner_assigned = true;
        }
        i += 1;
    }
    winner_assigned
}

/// Java `String.hashCode` over the first `limit` UTF-16 code units.
#[cfg(creusot)]
#[logic]
#[requires(0 <= limit && limit <= units.len())]
#[variant(limit)]
pub fn java_string_hash_prefix_model(units: Seq<u16>, limit: Int) -> i32 {
    pearlite! {
        if limit <= 0 {
            0i32
        } else {
            java_string_hash_prefix_model(units, limit - 1) * 31i32
                + units[limit - 1] as i32
        }
    }
}

#[cfg(creusot)]
#[logic]
fn java_string_abs_model(hash: i32) -> i32 {
    pearlite! {
        if hash == i32::MIN {
            0i32
        } else if hash < 0i32 {
            -hash
        } else {
            hash
        }
    }
}

#[cfg(creusot)]
#[logic]
#[requires(partition_count@ > 0)]
fn java_string_hash_partition_model(units: Seq<u16>, partition_count: i32) -> Int {
    pearlite! {
        java_string_abs_model(java_string_hash_prefix_model(units, units.len()))@
            % partition_count@
    }
}

/// Java `String.hashCode` over UTF-16 code units, followed by Kafka's
/// `Utils.abs(hash) % partition_count` coordinator selection.
///
/// The host supplies `str::encode_utf16()` output so non-ASCII group ids use
/// the same surrogate-pair semantics as the JVM. Java's `Integer.MIN_VALUE`
/// absolute-value corner maps to zero, matching `Utils.abs`.
#[cfg_attr(
    creusot,
    ensures(partition_count@ > 0 ==>
        exists<partition: i32> result == Some(partition)
            && partition@ == java_string_hash_partition_model(units@, partition_count))
)]
#[ensures(result == None ==> partition_count@ <= 0)]
#[ensures(partition_count@ <= 0 ==> result == None)]
#[ensures(forall<partition: i32> result == Some(partition) ==>
    0 <= partition@ && partition@ < partition_count@)]
#[must_use]
pub fn java_string_hash_partition(units: &[u16], partition_count: i32) -> Option<i32> {
    if partition_count <= 0 {
        return None;
    }

    let mut hash = 0_i32;
    let mut index = 0_usize;
    #[invariant(index@ <= units@.len())]
    #[cfg_attr(creusot, invariant(hash == java_string_hash_prefix_model(units@, index@)))]
    while index < units.len() {
        hash = hash.wrapping_mul(31).wrapping_add(i32::from(units[index]));
        index += 1;
    }
    let positive = if hash == i32::MIN {
        0
    } else if hash < 0 {
        -hash
    } else {
        hash
    };
    Some(positive % partition_count)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn replica_fetch_mutation_fences_every_input_and_selects_one_action() {
        use ReplicaFetchMutation::{Append, Reject, Retry, Truncate};

        assert!(
            replica_fetch_mutation(
                (true, true),
                (i32::MIN, i32::MIN),
                (true, true),
                (true, true),
            ) == Truncate
        );
        assert!(
            replica_fetch_mutation(
                (true, true),
                (i32::MAX, i32::MAX),
                (true, true),
                (true, false),
            ) == Append
        );
        assert!(
            replica_fetch_mutation((true, true), (4, 4), (true, true), (false, false)) == Retry
        );

        for rejected in [
            replica_fetch_mutation((false, true), (4, 4), (true, true), (true, false)),
            replica_fetch_mutation((true, false), (4, 4), (true, true), (true, false)),
            replica_fetch_mutation(
                (true, true),
                (i32::MIN, i32::MAX),
                (true, true),
                (true, false),
            ),
            replica_fetch_mutation((true, true), (4, 4), (false, true), (true, false)),
            replica_fetch_mutation((true, true), (4, 4), (true, false), (true, false)),
        ] {
            assert!(rejected == Reject);
        }
    }

    #[test]
    fn preferred_rebalance_admission_requires_every_batch_fact() {
        assert!(preferred_rebalance_admission(10, 1, true, true, true));
        for denied in [
            preferred_rebalance_admission(0, 0, true, true, true),
            preferred_rebalance_admission(10, 0, true, true, true),
            preferred_rebalance_admission(10, 11, true, true, true),
            preferred_rebalance_admission(10, 1, false, true, true),
            preferred_rebalance_admission(10, 1, true, false, true),
            preferred_rebalance_admission(10, 1, true, true, false),
        ] {
            assert!(!denied);
        }
    }

    #[test]
    fn fetch_visibility_covers_consumer_and_follower_bounds() {
        // Nothing is held back: the delivery watermark sits at the high
        // watermark, so every bound is the one Kafka computes today.
        assert2::assert!(
            fetch_visibility(
                false,
                true,
                FetchWatermarks {
                    log_start: 2,
                    hw: 8,
                    lso: 6,
                    log_end: 10,
                    deliverable: 8,
                },
                3
            ) == FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 6,
                effective_lso: 6,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 6,
            }
        );

        assert2::assert!(
            fetch_visibility(
                false,
                true,
                FetchWatermarks {
                    log_start: 2,
                    hw: 8,
                    lso: 9,
                    log_end: 10,
                    deliverable: 8,
                },
                3
            ) == FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 8,
                effective_lso: 8,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 8,
            }
        );

        // A follower reads to the log end even where the whole log is waiting
        // to be delivered.
        assert2::assert!(
            fetch_visibility(
                true,
                false,
                FetchWatermarks {
                    log_start: 2,
                    hw: 8,
                    lso: 6,
                    log_end: 10,
                    deliverable: 2,
                },
                10
            ) == FetchVisibility {
                out_of_range: false,
                empty: true,
                limit_offset: 10,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 10,
                response_lso: 10,
            }
        );
    }

    #[test]
    fn fetch_visibility_caps_a_consumer_at_the_delivery_watermark() {
        // read_uncommitted: the cap is the delivery watermark, not the high
        // watermark, and the reported watermarks do not move with it.
        assert2::assert!(
            fetch_visibility(
                false,
                false,
                FetchWatermarks {
                    log_start: 0,
                    hw: 8,
                    lso: 8,
                    log_end: 10,
                    deliverable: 5,
                },
                3
            ) == FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 5,
                effective_lso: 8,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 8,
            }
        );

        // A consumer parked exactly at the watermark reads nothing, which is
        // what parks it in a long poll until the batch there comes due.
        assert2::assert!(
            fetch_visibility(
                false,
                false,
                FetchWatermarks {
                    log_start: 0,
                    hw: 8,
                    lso: 8,
                    log_end: 10,
                    deliverable: 5,
                },
                5
            ) == FetchVisibility {
                out_of_range: false,
                empty: true,
                limit_offset: 5,
                effective_lso: 8,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 8,
            }
        );

        // read_committed takes the lowest of the three. The abort-scan ceiling
        // stays `lso.min(hw)`, because a wider scan only lists aborts the
        // consumer already knows how to drop.
        assert2::assert!(
            fetch_visibility(
                false,
                true,
                FetchWatermarks {
                    log_start: 0,
                    hw: 8,
                    lso: 6,
                    log_end: 10,
                    deliverable: 4,
                },
                0
            ) == FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 4,
                effective_lso: 6,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 6,
            }
        );
        // The last stable offset still wins where it is the lowest of the three.
        assert2::assert!(
            fetch_visibility(
                false,
                true,
                FetchWatermarks {
                    log_start: 0,
                    hw: 8,
                    lso: 3,
                    log_end: 10,
                    deliverable: 4,
                },
                0
            )
            .limit_offset
                == 3
        );
    }

    #[test]
    fn broker_arithmetic_edges_are_explicit() {
        assert!(
            delete_records_trim_decision(DeleteRecordsTrimFacts {
                requested: -1,
                high_watermark: 7,
                log_end: 9,
                current_start: 2,
                has_delivery_watermark: false,
                delivery_watermark: 0,
            }) == DeleteRecordsTrimDecision::Apply { frontier: 7 }
        );
        assert!(effective_share_backlog(12, -1, 4) == 8);
        assert!(effective_share_backlog(5, 9, 4) == 0);
        assert!(effective_share_backlog(i64::MAX, i64::MIN, i64::MIN) == i64::MAX);
    }

    #[test]
    fn delete_records_application_orders_retries_wal_first() {
        use DeleteRecordsTrimApplication::{Complete, RejectMalformed, TrimLocal, TrimWal};

        assert!(delete_records_trim_application(-1, 0, 0) == RejectMalformed);
        assert!(delete_records_trim_application(8, 2, 2) == TrimWal { frontier: 8 });
        assert!(delete_records_trim_application(8, 8, 2) == TrimLocal { frontier: 8 });
        assert!(delete_records_trim_application(8, 8, 8) == Complete { frontier: 8 });
        // A retry repairs either side at the highest frontier and never
        // regresses a partially applied trim.
        assert!(delete_records_trim_application(5, 8, 3) == TrimLocal { frontier: 8 });
        assert!(delete_records_trim_application(5, 3, 8) == TrimWal { frontier: 8 });
        assert!(delete_records_trim_application(i64::MAX, 0, 0) == TrimWal { frontier: i64::MAX });
    }

    #[test]
    fn find_coordinator_admission_is_exhaustive_and_fail_closed() {
        use FindCoordinatorAdmission::{
            AllowGroup, AllowShare, AllowTransaction, DenyCluster, DenyGroup, DenyTransaction,
            InvalidRequest,
        };

        for share_key_valid in [false, true] {
            assert!(find_coordinator_admission(0, 0, false, share_key_valid) == DenyGroup);
            assert!(find_coordinator_admission(0, 0, true, share_key_valid) == AllowGroup);
            assert!(find_coordinator_admission(0, 1, false, share_key_valid) == DenyTransaction);
            assert!(find_coordinator_admission(0, 1, true, share_key_valid) == AllowTransaction);
        }
        for version in [i16::MIN, 0, 5] {
            assert!(find_coordinator_admission(version, 2, false, true) == InvalidRequest);
            assert!(find_coordinator_admission(version, 2, true, true) == InvalidRequest);
        }
        assert!(find_coordinator_admission(6, 2, false, false) == InvalidRequest);
        assert!(find_coordinator_admission(6, 2, true, false) == InvalidRequest);
        assert!(find_coordinator_admission(6, 2, false, true) == DenyCluster);
        assert!(find_coordinator_admission(6, 2, true, true) == AllowShare);

        for unknown in [i8::MIN, -1, 3, i8::MAX] {
            for acl_allowed in [false, true] {
                for share_key_valid in [false, true] {
                    assert!(
                        find_coordinator_admission(6, unknown, acl_allowed, share_key_valid)
                            == InvalidRequest
                    );
                }
            }
        }
    }

    #[test]
    fn unclean_recovery_commit_requires_the_selection_snapshot() {
        assert!(unclean_recovery_commit_admission(
            7,
            7,
            &[1, 2],
            &[1, 2],
            2,
            false
        ));
        assert!(!unclean_recovery_commit_admission(
            7,
            8,
            &[1, 2],
            &[1, 2],
            2,
            false
        ));
        assert!(!unclean_recovery_commit_admission(
            7,
            7,
            &[1, 2],
            &[2, 1],
            2,
            false
        ));
        assert!(!unclean_recovery_commit_admission(
            7,
            7,
            &[1, 2],
            &[1, 2, 3],
            2,
            false
        ));
        assert!(!unclean_recovery_commit_admission(
            7,
            7,
            &[1, 2],
            &[1, 2],
            3,
            false
        ));
        assert!(!unclean_recovery_commit_admission(
            7,
            7,
            &[1, 2],
            &[1, 2],
            2,
            true
        ));
    }

    #[test]
    fn java_string_hash_partition_matches_jvm_goldens() {
        for (key, partitions, expected) in [
            ("g:BQUFBQUFBQUFBQUFBQUFBQ:0", 50, 2),
            ("consumer-group", 50, 38),
            ("🦀:BQUFBQUFBQUFBQUFBQUFBQ:7", 17, 8),
            // This is the canonical Java String whose hashCode is
            // Integer.MIN_VALUE. Kafka Utils.abs maps that corner to zero.
            ("polygenelubricants", 50, 0),
        ] {
            let units: Vec<u16> = key.encode_utf16().collect();
            assert!(java_string_hash_partition(&units, partitions) == Some(expected));
        }
        assert!(java_string_hash_partition(&[], 0) == None);
        assert!(java_string_hash_partition(&[], -1) == None);
    }

    #[test]
    fn fetch_visibility_matches_the_complete_decision_table() {
        for is_follower in [false, true] {
            for read_committed in [false, true] {
                for log_start in [0, 2] {
                    for hw in [2, 5] {
                        for lso in [1, 4, 7] {
                            for log_end in [5, 9] {
                                // The table walks `deliverable` past both ends
                                // of the proved domain `[log_start, hw]` as
                                // well as through it, because the precondition
                                // is a proof obligation on the caller and does
                                // not run. The oracle takes the same minimum
                                // the body does.
                                for deliverable in [0, 2, 3, 5, 9] {
                                    for fetch_offset in [0, 2, 4, 5, 10] {
                                        let got = fetch_visibility(
                                            is_follower,
                                            read_committed,
                                            FetchWatermarks {
                                                log_start,
                                                hw,
                                                lso,
                                                log_end,
                                                deliverable,
                                            },
                                            fetch_offset,
                                        );
                                        let visible = hw.min(deliverable);
                                        let upper = if is_follower { log_end } else { visible };
                                        let effective_lso = if read_committed && !is_follower {
                                            lso.min(hw)
                                        } else {
                                            lso
                                        };
                                        let response_lso = if is_follower {
                                            log_end
                                        } else if read_committed {
                                            lso.min(hw)
                                        } else {
                                            hw
                                        };
                                        let limit = if is_follower {
                                            log_end
                                        } else if read_committed {
                                            effective_lso.min(visible)
                                        } else {
                                            visible
                                        };
                                        let out_of_range = fetch_offset < log_start;

                                        assert2::assert!(
                                            got == FetchVisibility {
                                                out_of_range,
                                                empty: !out_of_range && fetch_offset >= upper,
                                                limit_offset: limit,
                                                effective_lso,
                                                read_committed_aborts: read_committed
                                                    && !is_follower,
                                                response_hw: if is_follower { log_end } else { hw },
                                                response_lso,
                                            }
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn broker_arithmetic_matches_wide_integer_oracles() {
        let values = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX];
        for hwm in values {
            for spso in values {
                for log_start in values {
                    let base = if spso >= 0 {
                        spso.max(log_start)
                    } else {
                        log_start
                    };
                    let expected = i64::try_from(
                        (i128::from(hwm) - i128::from(base)).clamp(0, i128::from(i64::MAX)),
                    )
                    .expect("oracle is clamped to the i64 range");
                    assert!(effective_share_backlog(hwm, spso, log_start) == expected);
                }
            }
        }
    }
}
