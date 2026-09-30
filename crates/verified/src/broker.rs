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

/// What a follower knows about one row of a Fetch response when it decides
/// what that row may change.
///
/// The row's topic and partition identity is not among them: the host looks
/// the row up by that identity (`ResponseIndex::locate` in the broker's
/// replicator) and never hands the kernel a row it could not attribute to
/// exactly one followed partition.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ReplicaFetchFacts {
    /// The leader epoch this follower sent for the partition in the request
    /// that the row answers.
    pub request_leader_epoch: i32,
    /// The leader epoch of the partition's current replication target.
    pub current_leader_epoch: i32,
    /// The replication target is the one the request was sent to.
    pub target_matches: bool,
    /// The row's `current_leader` is either absent or exactly the current
    /// target's leader and epoch.
    pub reported_target_matches: bool,
    /// The row's Kafka error code; `0` is `NONE`.
    pub error_code: i16,
    /// The row's KIP-320 `DivergingEpoch.Epoch`; `-1`, the schema default,
    /// means the row carries no divergence. Kafka's
    /// `FetchResponse.isDivergingEpoch` tests the epoch, not the end offset.
    pub diverging_epoch: i32,
}

/// A row answers a request this follower no longer stands behind: the epoch
/// moved while it was in flight, the replication target changed, or the leader
/// names a different target than the one this follower fetched from.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn replica_fetch_fenced(facts: ReplicaFetchFacts) -> bool {
    pearlite! {
        facts.request_leader_epoch@ != facts.current_leader_epoch@
            || !facts.target_matches
            || !facts.reported_target_matches
    }
}

/// Fence one follower Fetch response row against its in-flight request epoch,
/// its current metadata target and any reported leader identity; then select
/// one exclusive response action.
///
/// A follower's request covers every partition it follows on one leader, so
/// the caller applies this once per row of the answer, with that row's own
/// partition configuration and the epoch it was asked under. The guarantee is
/// therefore per partition and does not weaken as a response carries more of
/// them: a fenced row mutates nothing, whatever the rows beside it say. An
/// unfenced row is retried on an error, truncates on a KIP-320 divergence
/// (which Kafka sends with no records), and appends otherwise.
#[ensures(result == if replica_fetch_fenced(facts) {
    ReplicaFetchMutation::Reject
} else if facts.error_code@ != 0 {
    ReplicaFetchMutation::Retry
} else if facts.diverging_epoch@ >= 0 {
    ReplicaFetchMutation::Truncate
} else {
    ReplicaFetchMutation::Append
})]
#[must_use]
pub fn replica_fetch_mutation(facts: ReplicaFetchFacts) -> ReplicaFetchMutation {
    if facts.request_leader_epoch != facts.current_leader_epoch
        || !facts.target_matches
        || !facts.reported_target_matches
    {
        ReplicaFetchMutation::Reject
    } else if facts.error_code != 0 {
        ReplicaFetchMutation::Retry
    } else if facts.diverging_epoch >= 0 {
        ReplicaFetchMutation::Truncate
    } else {
        ReplicaFetchMutation::Append
    }
}

/// What a preferred-leader rebalance scan observed about one change it wants
/// to submit.
///
/// The host reads every fact off the change record itself and the scan's
/// liveness and witness snapshots, not off the election that produced it, so
/// the kernel checks the election's output rather than restating it.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct PreferredLeaderChange {
    /// The leader the change installs.
    pub new_leader: u64,
    /// The partition's preferred replica, `replicas[0]`, or `None` for an
    /// empty assignment.
    pub preferred_replica: Option<u64>,
    /// The new leader is in the change's ISR.
    pub leader_in_isr: bool,
    /// The new leader is alive in the scan's liveness snapshot.
    pub leader_alive: bool,
    /// The new leader carries the witness role, so it may never lead.
    pub leader_is_witness: bool,
}

/// KIP-460's preferred election: leadership moves to the first assigned
/// replica, and only when that replica is alive, in the ISR, and able to lead.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn preferred_change_eligible(change: PreferredLeaderChange) -> bool {
    pearlite! {
        change.preferred_replica == Some(change.new_leader)
            && change.leader_in_isr
            && change.leader_alive
            && !change.leader_is_witness
    }
}

/// Admit one preferred-leader rebalance batch only when it is nonempty, within
/// the per-tick election cap, and every change in it is a KIP-460 preferred
/// election.
///
/// There is no imbalance-ratio threshold among the facts. That gate is the
/// `ZooKeeper` controller's `leader.imbalance.per.broker.percentage`; the
/// `KRaft` controller krabka follows restores every partition whose preferred
/// replica is eligible, and bounds the work by a per-tick election count
/// (`QuorumController.MAX_ELECTIONS_PER_IMBALANCE`), which is `max_changes`.
#[ensures(result == (
    0 < changes@.len()
        && changes@.len() <= max_changes@
        && forall<i: Int> 0 <= i && i < changes@.len()
            ==> preferred_change_eligible(changes@[i])
))]
#[must_use]
pub fn preferred_rebalance_admission(
    changes: &[PreferredLeaderChange],
    max_changes: usize,
) -> bool {
    // `len` rather than `is_empty`: Creusot specifies `<[T]>::len` only.
    let count = changes.len();
    if count == 0 || count > max_changes {
        return false;
    }
    let mut index = 0_usize;
    #[invariant(index@ <= changes@.len())]
    #[invariant(forall<k: Int> 0 <= k && k < index@ ==> preferred_change_eligible(changes@[k]))]
    #[variant(changes@.len() - index@)]
    while index < changes.len() {
        let change = changes[index];
        let installs_preferred = match change.preferred_replica {
            Some(preferred) => preferred == change.new_leader,
            None => false,
        };
        if !installs_preferred
            || !change.leader_in_isr
            || !change.leader_alive
            || change.leader_is_witness
        {
            return false;
        }
        index += 1;
    }
    true
}

/// The lower of two offsets.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn offset_min(a: Int, b: Int) -> Int {
    pearlite! { if a <= b { a } else { b } }
}

/// The exclusive upper offset one fetch may read.
///
/// A follower reads to the leader's log end (Kafka's `FetchIsolation.LOG_END`).
/// A consumer reads below the high watermark (`HIGH_WATERMARK`), below the
/// last stable offset as well under `read_committed` (`TXN_COMMITTED`), and,
/// on a scheduled topic, below KFC-1's delivery watermark too.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn fetch_limit_model(is_follower: bool, read_committed: bool, w: FetchWatermarks) -> Int {
    pearlite! {
        if is_follower {
            w.log_end@
        } else if read_committed {
            offset_min(offset_min(w.hw@, w.deliverable@), w.lso@)
        } else {
            offset_min(w.hw@, w.deliverable@)
        }
    }
}

/// Compute Kafka's consumer/follower Fetch visibility window.
///
/// The reported watermarks are the partition's own, for every fetcher:
/// `response_hw` is the high watermark and `response_lso` is the last stable
/// offset, capped at the high watermark as Kafka's `UnifiedLog.lastStableOffset`
/// caps it. Kafka's `Partition.readRecords` reads both before the fetch and
/// puts them in the `LogReadInfo` whatever the isolation level and whoever is
/// fetching, so a follower learns the leader's committed bound rather than its
/// log end. That is what keeps a KIP-392 follower from exposing an offset the
/// leader may still truncate.
///
/// [`FetchWatermarks::deliverable`] is KFC-1's delivery watermark: the first
/// offset a consumer may not see yet, because the batch that starts there has
/// not reached its activation time. It caps a consumer, and it caps nothing
/// else:
///
/// - A follower is never gated. Replication carries a scheduled record to the
///   ISR, and it counts toward the high watermark, long before any consumer can
///   read it.
/// - `response_hw` and `response_lso` do not move with it, so consumer lag
///   stays honest and KIP-227 watermark monotonicity is untouched.
///
/// There is no precondition. The consumer bound is proved outright: whatever
/// the caller passes, a consumer never reads at or past the high watermark, the
/// delivery watermark, or, under `read_committed`, the last stable offset.
#[ensures(result.out_of_range == (fetch_offset@ < w.log_start@))]
#[ensures(result.empty == (!result.out_of_range
    && fetch_offset@ >= if is_follower { w.log_end@ } else { offset_min(w.hw@, w.deliverable@) }))]
#[ensures(result.limit_offset@ == fetch_limit_model(is_follower, read_committed, w))]
#[ensures(!is_follower ==> result.limit_offset@ <= w.hw@
    && result.limit_offset@ <= w.deliverable@
    && (read_committed ==> result.limit_offset@ <= w.lso@))]
#[ensures(result.read_committed_aborts == (read_committed && !is_follower))]
#[ensures(result.effective_lso@ == if result.read_committed_aborts {
    offset_min(w.lso@, w.hw@)
} else {
    w.lso@
})]
#[ensures(result.response_hw == w.hw)]
#[ensures(result.response_lso@ == offset_min(w.lso@, w.hw@))]
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
    let stable = if w.lso < w.hw { w.lso } else { w.hw };
    let read_committed_aborts = read_committed && !is_follower;
    let limit_offset = if is_follower {
        w.log_end
    } else if read_committed && stable < visible {
        stable
    } else {
        visible
    };
    let upper_bound = if is_follower { w.log_end } else { visible };
    let out_of_range = fetch_offset < w.log_start;
    FetchVisibility {
        out_of_range,
        empty: !out_of_range && fetch_offset >= upper_bound,
        limit_offset,
        effective_lso: if read_committed_aborts { stable } else { w.lso },
        read_committed_aborts,
        response_hw: w.hw,
        response_lso: stable,
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
/// `-1` means the current high watermark, which is always admitted. An
/// explicit request above the high watermark is refused
/// `RejectOutOfRange`, matching `UnifiedLog.maybeIncrementLogStartOffset`
/// on Kafka trunk: an explicit offset never enters the uncommitted tail,
/// even when it is still below the log end offset. A scheduled topic adds
/// its delivery watermark as a second cap on top of the high watermark.
/// Stale and repeated requests return the current start and never move it
/// backwards.
#[must_use]
#[ensures({
    let malformed = facts.requested@ < -1
        || facts.current_start@ < 0
        || facts.high_watermark@ < facts.current_start@
        || facts.log_end@ < facts.high_watermark@
        || (facts.has_delivery_watermark
            && facts.delivery_watermark@ < facts.current_start@);
    let out_of_range = !malformed
        && facts.requested@ != -1
        && facts.requested@ > facts.high_watermark@;
    let resolved = if facts.requested@ == -1 {
        facts.high_watermark@
    } else {
        facts.requested@
    };
    let bounded = if facts.has_delivery_watermark
        && facts.delivery_watermark@ < resolved
    {
        facts.delivery_watermark@
    } else {
        resolved
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
    if facts.requested != -1 && facts.requested > facts.high_watermark {
        return DeleteRecordsTrimDecision::RejectOutOfRange;
    }
    let resolved = if facts.requested == -1 {
        facts.high_watermark
    } else {
        facts.requested
    };
    let bounded = if facts.has_delivery_watermark {
        if resolved < facts.delivery_watermark {
            resolved
        } else {
            facts.delivery_watermark
        }
    } else {
        resolved
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
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
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
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
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

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
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

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
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
    #[variant(units@.len() - index@)]
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

    /// A follower's row that nothing has fenced: the epoch it asked under is
    /// still the current one, and the leader named no other target.
    const LIVE_ROW: ReplicaFetchFacts = ReplicaFetchFacts {
        request_leader_epoch: 4,
        current_leader_epoch: 4,
        target_matches: true,
        reported_target_matches: true,
        error_code: 0,
        diverging_epoch: -1,
    };

    #[test]
    fn replica_fetch_mutation_fences_every_input_and_selects_one_action() {
        use ReplicaFetchMutation::{Append, Reject, Retry, Truncate};

        for (scenario, facts, expected) in [
            ("records for the live request", LIVE_ROW, Append),
            (
                "KIP-320 divergence at the widest epoch",
                ReplicaFetchFacts {
                    request_leader_epoch: i32::MAX,
                    current_leader_epoch: i32::MAX,
                    diverging_epoch: 0,
                    ..LIVE_ROW
                },
                Truncate,
            ),
            (
                "FENCED_LEADER_EPOCH goes to error handling",
                ReplicaFetchFacts {
                    error_code: 74,
                    ..LIVE_ROW
                },
                Retry,
            ),
            (
                "an error row with a divergence is still only an error",
                ReplicaFetchFacts {
                    error_code: 1,
                    diverging_epoch: 7,
                    ..LIVE_ROW
                },
                Retry,
            ),
            (
                "leader epoch bumped while the request was in flight",
                ReplicaFetchFacts {
                    current_leader_epoch: 5,
                    ..LIVE_ROW
                },
                Reject,
            ),
            (
                "the replication target moved to another leader",
                ReplicaFetchFacts {
                    target_matches: false,
                    ..LIVE_ROW
                },
                Reject,
            ),
            (
                "the leader reports a different current leader",
                ReplicaFetchFacts {
                    reported_target_matches: false,
                    diverging_epoch: 3,
                    ..LIVE_ROW
                },
                Reject,
            ),
            (
                "a fenced error row mutates nothing either",
                ReplicaFetchFacts {
                    request_leader_epoch: i32::MIN,
                    error_code: 6,
                    ..LIVE_ROW
                },
                Reject,
            ),
        ] {
            assert!(replica_fetch_mutation(facts) == expected, "{scenario}");
        }
    }

    /// Broker 1 takes its preferred partition back: it is `replicas[0]`,
    /// alive, in the ISR and not a witness.
    const PREFERRED_BACK: PreferredLeaderChange = PreferredLeaderChange {
        new_leader: 1,
        preferred_replica: Some(1),
        leader_in_isr: true,
        leader_alive: true,
        leader_is_witness: false,
    };

    #[test]
    fn preferred_rebalance_admits_only_capped_preferred_elections() {
        for (scenario, changes, cap, expected) in [
            (
                "one preferred election",
                std::vec![PREFERRED_BACK],
                1000,
                true,
            ),
            (
                "a batch exactly at the cap",
                std::vec![PREFERRED_BACK; 2],
                2,
                true,
            ),
            ("nothing to rebalance", std::vec![], 1000, false),
            (
                "a batch over the cap",
                std::vec![PREFERRED_BACK; 3],
                2,
                false,
            ),
            (
                "the change installs a non-preferred replica",
                std::vec![PreferredLeaderChange {
                    new_leader: 2,
                    ..PREFERRED_BACK
                }],
                1000,
                false,
            ),
            (
                "the partition has no assignment",
                std::vec![PreferredLeaderChange {
                    preferred_replica: None,
                    ..PREFERRED_BACK
                }],
                1000,
                false,
            ),
            (
                "the preferred replica fell out of the ISR",
                std::vec![
                    PREFERRED_BACK,
                    PreferredLeaderChange {
                        leader_in_isr: false,
                        ..PREFERRED_BACK
                    },
                ],
                1000,
                false,
            ),
            (
                "the preferred replica is not alive",
                std::vec![PreferredLeaderChange {
                    leader_alive: false,
                    ..PREFERRED_BACK
                }],
                1000,
                false,
            ),
            (
                "the preferred replica is a witness",
                std::vec![PreferredLeaderChange {
                    leader_is_witness: true,
                    ..PREFERRED_BACK
                }],
                1000,
                false,
            ),
        ] {
            assert!(
                preferred_rebalance_admission(&changes, cap) == expected,
                "{scenario}"
            );
        }
    }

    /// A partition with an open transaction at 6, a high watermark of 8 and
    /// two uncommitted records beyond it. Nothing is scheduled, so the delivery
    /// watermark sits at the high watermark.
    const OPEN_TXN: FetchWatermarks = FetchWatermarks {
        log_start: 2,
        hw: 8,
        lso: 6,
        log_end: 10,
        deliverable: 8,
    };

    #[test]
    fn fetch_visibility_matches_kafka_fetch_scenarios() {
        let open_txn = OPEN_TXN;
        // Every row reports the partition's own HW (8) and LSO (6), follower
        // or consumer: `Partition.readRecords` reads both whoever fetches.
        for (scenario, is_follower, read_committed, w, fetch_offset, expected) in [
            (
                "read_uncommitted consumer reads to the high watermark",
                false,
                false,
                open_txn,
                3,
                FetchVisibility {
                    out_of_range: false,
                    empty: false,
                    limit_offset: 8,
                    effective_lso: 6,
                    read_committed_aborts: false,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
            (
                "read_committed consumer stops at the open transaction",
                false,
                true,
                open_txn,
                3,
                FetchVisibility {
                    out_of_range: false,
                    empty: false,
                    limit_offset: 6,
                    effective_lso: 6,
                    read_committed_aborts: true,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
            (
                "follower reads to the log end but learns the committed bounds",
                true,
                false,
                open_txn,
                8,
                FetchVisibility {
                    out_of_range: false,
                    empty: false,
                    limit_offset: 10,
                    effective_lso: 6,
                    read_committed_aborts: false,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
            (
                "caught-up follower has nothing to read",
                true,
                false,
                open_txn,
                10,
                FetchVisibility {
                    out_of_range: false,
                    empty: true,
                    limit_offset: 10,
                    effective_lso: 6,
                    read_committed_aborts: false,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
            (
                "a fetch below the log start is OFFSET_OUT_OF_RANGE",
                false,
                false,
                open_txn,
                1,
                FetchVisibility {
                    out_of_range: true,
                    empty: false,
                    limit_offset: 8,
                    effective_lso: 6,
                    read_committed_aborts: false,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
        ] {
            assert!(
                fetch_visibility(is_follower, read_committed, w, fetch_offset) == expected,
                "{scenario}"
            );
        }

        // An LSO that has run ahead of the high watermark is capped at it, in
        // the report as well as the read_committed bound, as
        // `UnifiedLog.lastStableOffset` caps it.
        assert!(
            fetch_visibility(false, true, FetchWatermarks { lso: 9, ..open_txn }, 3)
                == FetchVisibility {
                    out_of_range: false,
                    empty: false,
                    limit_offset: 8,
                    effective_lso: 8,
                    read_committed_aborts: true,
                    response_hw: 8,
                    response_lso: 8,
                }
        );
    }

    #[test]
    fn fetch_visibility_caps_only_a_consumer_at_the_delivery_watermark() {
        let open_txn = OPEN_TXN;
        // As above, every row reports the partition's own HW (8) and LSO (6).
        for (scenario, is_follower, read_committed, w, fetch_offset, expected) in [
            (
                "a follower is not gated by a delivery watermark at the log start",
                true,
                false,
                FetchWatermarks {
                    deliverable: 2,
                    ..open_txn
                },
                3,
                FetchVisibility {
                    out_of_range: false,
                    empty: false,
                    limit_offset: 10,
                    effective_lso: 6,
                    read_committed_aborts: false,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
            (
                "a consumer is held below a batch that is not due yet",
                false,
                false,
                FetchWatermarks {
                    deliverable: 5,
                    ..open_txn
                },
                3,
                FetchVisibility {
                    out_of_range: false,
                    empty: false,
                    limit_offset: 5,
                    effective_lso: 6,
                    read_committed_aborts: false,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
            (
                "a consumer parked at the delivery watermark reads nothing",
                false,
                false,
                FetchWatermarks {
                    deliverable: 5,
                    ..open_txn
                },
                5,
                FetchVisibility {
                    out_of_range: false,
                    empty: true,
                    limit_offset: 5,
                    effective_lso: 6,
                    read_committed_aborts: false,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
            (
                "read_committed takes the delivery watermark where it is lowest",
                false,
                true,
                FetchWatermarks {
                    deliverable: 4,
                    ..open_txn
                },
                3,
                FetchVisibility {
                    out_of_range: false,
                    empty: false,
                    limit_offset: 4,
                    effective_lso: 6,
                    read_committed_aborts: true,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
            (
                "a delivery watermark above the high watermark exposes nothing uncommitted",
                false,
                false,
                FetchWatermarks {
                    deliverable: 10,
                    ..open_txn
                },
                8,
                FetchVisibility {
                    out_of_range: false,
                    empty: true,
                    limit_offset: 8,
                    effective_lso: 6,
                    read_committed_aborts: false,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
            (
                "a delivery watermark below the log start leaves an empty window",
                false,
                true,
                FetchWatermarks {
                    deliverable: 0,
                    ..open_txn
                },
                2,
                FetchVisibility {
                    out_of_range: false,
                    empty: true,
                    limit_offset: 0,
                    effective_lso: 6,
                    read_committed_aborts: true,
                    response_hw: 8,
                    response_lso: 6,
                },
            ),
        ] {
            assert!(
                fetch_visibility(is_follower, read_committed, w, fetch_offset) == expected,
                "{scenario}"
            );
        }
    }

    #[test]
    fn broker_arithmetic_edges_are_explicit() {
        use DeleteRecordsTrimDecision::{Apply, Noop, RejectMalformed, RejectOutOfRange};

        let facts = |requested, high_watermark, log_end, current_start, has_delivery, delivery| {
            DeleteRecordsTrimFacts {
                requested,
                high_watermark,
                log_end,
                current_start,
                has_delivery_watermark: has_delivery,
                delivery_watermark: delivery,
            }
        };

        // Malformed checks
        assert!(delete_records_trim_decision(facts(-2, 7, 9, 2, false, 0)) == RejectMalformed);
        assert!(delete_records_trim_decision(facts(5, 7, 9, -1, false, 0)) == RejectMalformed);
        assert!(delete_records_trim_decision(facts(5, 1, 9, 2, false, 0)) == RejectMalformed);
        assert!(delete_records_trim_decision(facts(5, 7, 6, 2, false, 0)) == RejectMalformed);
        assert!(delete_records_trim_decision(facts(5, 7, 9, 2, true, 1)) == RejectMalformed);
        assert!(delete_records_trim_decision(facts(5, 7, 9, 2, false, 1)) == Apply { frontier: 5 });

        // Out of range: an explicit offset above the log end, and one above
        // the high watermark but still within the log end (KIP-107: the
        // uncommitted tail is never a valid explicit target).
        assert!(delete_records_trim_decision(facts(10, 7, 9, 2, false, 0)) == RejectOutOfRange);
        assert!(delete_records_trim_decision(facts(8, 7, 9, 2, false, 0)) == RejectOutOfRange);
        assert!(delete_records_trim_decision(facts(9, 9, 9, 2, false, 0)) == Apply { frontier: 9 });

        // Requested = -1 resolves to high_watermark and is always admitted,
        // even though an explicit request for that same offset is refused.
        assert!(
            delete_records_trim_decision(facts(-1, 7, 9, 2, false, 0)) == Apply { frontier: 7 }
        );

        // Zero boundary
        assert!(delete_records_trim_decision(facts(0, 0, 0, 0, false, 0)) == Noop { frontier: 0 });
        assert!(delete_records_trim_decision(facts(0, 0, 0, 0, true, 0)) == Noop { frontier: 0 });

        // Clamping by delivery_watermark
        assert!(delete_records_trim_decision(facts(7, 7, 9, 2, true, 6)) == Apply { frontier: 6 });
        assert!(delete_records_trim_decision(facts(5, 7, 9, 2, true, 6)) == Apply { frontier: 5 });

        // Noop when bounded <= current_start
        assert!(delete_records_trim_decision(facts(2, 7, 9, 2, false, 0)) == Noop { frontier: 2 });
        assert!(delete_records_trim_decision(facts(1, 7, 9, 2, false, 0)) == Noop { frontier: 2 });

        assert!(effective_share_backlog(12, -1, 4) == 8);
        assert!(effective_share_backlog(5, 9, 4) == 0);
        assert!(effective_share_backlog(i64::MAX, i64::MIN, i64::MIN) == i64::MAX);
    }

    #[test]
    fn delete_records_application_orders_retries_wal_first() {
        use DeleteRecordsTrimApplication::{Complete, RejectMalformed, TrimLocal, TrimWal};

        assert!(delete_records_trim_application(-1, 0, 0) == RejectMalformed);
        assert!(delete_records_trim_application(0, -1, 0) == RejectMalformed);
        assert!(delete_records_trim_application(0, 0, -1) == RejectMalformed);
        assert!(delete_records_trim_application(0, 0, 0) == Complete { frontier: 0 });
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
