//! Classic Kafka ISR decisions: `AlterPartition` validation on the controller,
//! ISR maintenance and follower catch-up tracking on the partition leader, and
//! the high-watermark computation.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// How a proposed ISR relates to the partition's assignment and leader.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ProposedIsr {
    /// Kafka's `Replicas.validateIsr` fails: a member is negative, repeated,
    /// or not an assigned replica.
    Invalid,
    /// Every member is a distinct assigned replica, but the current leader is
    /// not among them.
    WithoutLeader,
    /// Every member is a distinct assigned replica, and the leader is one.
    Valid,
}

/// What the controller knows about one `AlterPartition` partition row, as
/// Kafka's `ReplicationControlManager.validateAlterPartitionData` reads it.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct AlterPartitionFacts {
    pub request_leader_epoch: i32,
    pub current_leader_epoch: i32,
    pub request_partition_epoch: i32,
    pub current_partition_epoch: i32,
    /// The request's `brokerId` is the partition's current leader.
    pub requester_is_leader: bool,
    pub proposed_isr: ProposedIsr,
    /// The requested leader recovery state is a known value, a `RECOVERING`
    /// request proposes at most one member, and a `RECOVERED` partition is not
    /// asked to go back to `RECOVERING`.
    pub recovery_state_valid: bool,
    /// Kafka's `ineligibleReplicasForIsr` is empty: every proposed member is
    /// registered, unfenced, not in controlled shutdown, and carries either
    /// the -1 sentinel or its registered broker epoch.
    pub replicas_eligible: bool,
}

/// The first `AlterPartition` validation failure of one partition row, or
/// `Admit`.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum IsrAdmission {
    /// `NOT_CONTROLLER`: the request carries a leader or partition epoch the
    /// controller has not seen, so this controller is likely stale.
    NotController,
    /// `FENCED_LEADER_EPOCH`: the request's leader epoch is older.
    FencedLeaderEpoch,
    /// `INVALID_UPDATE_VERSION`: the request's partition epoch is older.
    InvalidUpdateVersion,
    /// `INVALID_REQUEST`: the requester is not the leader, the proposed ISR is
    /// invalid or omits the leader, or the recovery-state change is illegal.
    InvalidRequest,
    /// `INELIGIBLE_REPLICA`: a proposed member may not join an ISR.
    IneligibleReplica,
    Admit,
}

/// Kafka's `ReplicationControlManager.validateAlterPartitionData`, check for
/// check, after its unknown-partition check.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_validate_alter_partition_data(facts: AlterPartitionFacts) -> IsrAdmission {
    pearlite! {
        if facts.request_leader_epoch@ > facts.current_leader_epoch@
            || facts.request_partition_epoch@ > facts.current_partition_epoch@
        {
            IsrAdmission::NotController
        } else if facts.request_leader_epoch@ < facts.current_leader_epoch@ {
            IsrAdmission::FencedLeaderEpoch
        } else if !facts.requester_is_leader {
            IsrAdmission::InvalidRequest
        } else if facts.request_partition_epoch@ < facts.current_partition_epoch@ {
            IsrAdmission::InvalidUpdateVersion
        } else {
            match facts.proposed_isr {
                ProposedIsr::Valid => {
                    if !facts.recovery_state_valid {
                        IsrAdmission::InvalidRequest
                    } else if !facts.replicas_eligible {
                        IsrAdmission::IneligibleReplica
                    } else {
                        IsrAdmission::Admit
                    }
                }
                _ => IsrAdmission::InvalidRequest,
            }
        }
    }
}

/// Apply Kafka's ordered `AlterPartition` checks to one partition row.
///
/// A row is admitted exactly when it names the controller's current leader and
/// partition epochs, comes from the leader, proposes a valid ISR that keeps
/// the leader, requests a legal recovery state, and names only eligible
/// replicas. In particular a row whose partition epoch is behind the
/// controller's never overwrites the newer ISR.
#[ensures(result == kafka_validate_alter_partition_data(facts))]
#[ensures((result == IsrAdmission::Admit) == (
    facts.request_leader_epoch@ == facts.current_leader_epoch@
        && facts.request_partition_epoch@ == facts.current_partition_epoch@
        && facts.requester_is_leader
        && facts.proposed_isr == ProposedIsr::Valid
        && facts.recovery_state_valid
        && facts.replicas_eligible
))]
#[must_use]
pub fn isr_admission(facts: AlterPartitionFacts) -> IsrAdmission {
    if facts.request_leader_epoch > facts.current_leader_epoch
        || facts.request_partition_epoch > facts.current_partition_epoch
    {
        return IsrAdmission::NotController;
    }
    if facts.request_leader_epoch < facts.current_leader_epoch {
        return IsrAdmission::FencedLeaderEpoch;
    }
    if !facts.requester_is_leader {
        return IsrAdmission::InvalidRequest;
    }
    if facts.request_partition_epoch < facts.current_partition_epoch {
        return IsrAdmission::InvalidUpdateVersion;
    }
    match facts.proposed_isr {
        ProposedIsr::Valid => {}
        ProposedIsr::Invalid | ProposedIsr::WithoutLeader => {
            return IsrAdmission::InvalidRequest;
        }
    }
    if !facts.recovery_state_valid {
        return IsrAdmission::InvalidRequest;
    }
    if facts.replicas_eligible {
        IsrAdmission::Admit
    } else {
        IsrAdmission::IneligibleReplica
    }
}

/// Which fetch time, if any, a follower fetch proves the follower was caught
/// up at.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum CaughtUpCredit {
    /// The fetch reached the leader's log end offset: caught up now.
    ThisFetch,
    /// The fetch reached the leader's log end offset as of the previous fetch:
    /// caught up at the previous fetch's time.
    PreviousFetch,
    /// The fetch proves nothing new.
    Unchanged,
}

/// Kafka's `Replica.updateFetchStateOrThrow` choice of `lastCaughtUpTimeMs`.
///
/// `fetch_offset` is the follower's fetch offset, `leader_log_end` the
/// leader's log end offset when the fetch arrived, and
/// `last_fetch_leader_log_end` the leader's log end offset recorded at the
/// follower's previous fetch (-1 before the first). The host keeps the later
/// of the credited time and the recorded one.
#[ensures((result == CaughtUpCredit::ThisFetch) == (fetch_offset@ >= leader_log_end@))]
#[ensures((result == CaughtUpCredit::PreviousFetch) == (
    fetch_offset@ < leader_log_end@ && fetch_offset@ >= last_fetch_leader_log_end@
))]
#[must_use]
pub fn follower_caught_up_credit(
    fetch_offset: i64,
    leader_log_end: i64,
    last_fetch_leader_log_end: i64,
) -> CaughtUpCredit {
    if fetch_offset >= leader_log_end {
        CaughtUpCredit::ThisFetch
    } else if fetch_offset >= last_fetch_leader_log_end {
        CaughtUpCredit::PreviousFetch
    } else {
        CaughtUpCredit::Unchanged
    }
}

/// Where one candidate stands in the partition the leader maintains.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum IsrMemberRole {
    /// A current ISR member that is no longer assigned.
    Unassigned,
    /// The assigned leader.
    Leader,
    /// An assigned follower in the current ISR.
    InSyncFollower,
    /// An assigned follower outside the current ISR.
    OutOfSyncFollower,
}

/// The inputs of [`isr_maintenance_selected`], superseded by
/// [`IsrCandidateFacts`]. Every time bound is `replica.lag.time.max.ms`,
/// measured from the scan's single timestamp.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct IsrMemberFacts {
    pub role: IsrMemberRole,
    /// The follower's log end offset equals the leader's at this scan.
    pub log_end_matches_leader: bool,
    /// The follower's last caught-up time is within the bound.
    pub caught_up_within_lag: bool,
    /// The follower's last fetch is within the bound.
    pub fetch_within_lag: bool,
}

/// Kafka's `ReplicaState.isCaughtUp`: the follower has the leader's log end
/// offset, or it last caught up within `replica.lag.time.max.ms`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_is_caught_up(facts: IsrMemberFacts) -> bool {
    pearlite! { facts.log_end_matches_leader || facts.caught_up_within_lag }
}

/// krabka's former ISR scan rule, superseded by [`isr_candidate_selected`].
///
/// The leader always stays. An in-sync follower stays exactly while Kafka's
/// `ReplicaState.isCaughtUp` holds. An out-of-sync follower is admitted when
/// it fetched and caught up within the bound, which is not Kafka's
/// `Partition.isFollowerInSync`. The broker no longer calls this; the one
/// remaining caller is the leader-failover model's expansion stand-in, and
/// this function and [`IsrMemberFacts`] go once that model moves to
/// [`isr_candidate_selected`].
#[ensures(match facts.role {
    IsrMemberRole::Unassigned => !result,
    IsrMemberRole::Leader => result,
    IsrMemberRole::InSyncFollower => result == kafka_is_caught_up(facts),
    IsrMemberRole::OutOfSyncFollower =>
        result == (facts.fetch_within_lag && facts.caught_up_within_lag),
})]
#[must_use]
pub fn isr_maintenance_selected(facts: IsrMemberFacts) -> bool {
    match facts.role {
        IsrMemberRole::Unassigned => false,
        IsrMemberRole::Leader => true,
        IsrMemberRole::InSyncFollower => facts.log_end_matches_leader || facts.caught_up_within_lag,
        IsrMemberRole::OutOfSyncFollower => facts.fetch_within_lag && facts.caught_up_within_lag,
    }
}

/// What the leader's metadata cache and the follower's last Fetch say about
/// whether one replica may join the ISR, the inputs of Kafka's
/// `Partition.isReplicaIsrEligible` (KIP-841).
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct IsrEligibilityFacts {
    /// Kafka's `metadataCache.isBrokerFenced`.
    pub fenced: bool,
    /// Kafka's `metadataCache.isBrokerShuttingDown`.
    pub shutting_down: bool,
    /// Kafka's `ReplicaState.brokerEpoch`: the broker epoch the follower's
    /// last Fetch to this leader carried, -1 when that Fetch carried none, and
    /// `None` before this leader received one.
    pub fetch_broker_epoch: Option<i64>,
    /// Kafka's `metadataCache.getAliveBrokerEpoch`: the broker's registered
    /// epoch while it is registered and unfenced, `None` otherwise.
    pub alive_broker_epoch: Option<i64>,
}

/// Kafka's `Partition.isReplicaIsrEligible`: the broker is neither fenced nor
/// in controlled shutdown, and `isBrokerEpochIsrEligible` holds -- both epochs
/// are known, and the Fetch carried either no epoch (-1) or the alive one.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_is_replica_isr_eligible(facts: IsrEligibilityFacts) -> bool {
    pearlite! {
        !facts.fenced && !facts.shutting_down
            && match facts.fetch_broker_epoch {
                Some(stored) => match facts.alive_broker_epoch {
                    Some(alive) => stored@ == -1 || stored@ == alive@,
                    None => false,
                },
                None => false,
            }
    }
}

/// Kafka's `ReplicaState.isCaughtUp`: the follower's log end offset is the
/// leader's, or it last caught up within `replica.lag.time.max.ms`. A
/// follower that has not fetched from this leader has log end -1, Kafka's
/// `UNKNOWN_OFFSET`, which no leader log end equals.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_replica_is_caught_up(
    follower_log_end: Int,
    leader_log_end: Int,
    caught_up_within_lag: bool,
) -> bool {
    pearlite! { follower_log_end == leader_log_end || caught_up_within_lag }
}

/// Decide whether one replica may join the ISR, Kafka's
/// `Partition.isReplicaIsrEligible`.
#[ensures(result == kafka_is_replica_isr_eligible(facts))]
#[must_use]
pub fn replica_isr_eligible(facts: IsrEligibilityFacts) -> bool {
    if facts.fenced || facts.shutting_down {
        return false;
    }
    match (facts.fetch_broker_epoch, facts.alive_broker_epoch) {
        (Some(stored), Some(alive)) => stored == -1 || stored == alive,
        _ => false,
    }
}

/// What the leader knows about one candidate at an ISR maintenance scan,
/// read once at the scan's single timestamp.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct IsrCandidateFacts {
    pub role: IsrMemberRole,
    /// Kafka's `ReplicaState.logEndOffset`: the follower's last fetch offset,
    /// -1 before it fetched from this leader.
    pub follower_log_end: i64,
    /// The leader's log end offset.
    pub leader_log_end: i64,
    /// The leader's high watermark.
    pub leader_high_watermark: i64,
    /// Kafka's `Partition.leaderEpochStartOffsetOpt`: the log end offset at
    /// which this leader's epoch began, `None` while unknown.
    pub leader_epoch_start: Option<i64>,
    /// The follower's last caught-up time is within
    /// `replica.lag.time.max.ms`.
    pub caught_up_within_lag: bool,
    pub eligibility: IsrEligibilityFacts,
}

/// Kafka's `Partition.isFollowerInSync`: the follower's log end offset has
/// reached both the leader's high watermark and the start of the leader's
/// current epoch. The second half keeps out a follower that could otherwise
/// be elected before it fetched committed data between the high watermark
/// and the leader's log end.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_is_follower_in_sync(facts: IsrCandidateFacts) -> bool {
    pearlite! {
        facts.follower_log_end@ >= facts.leader_high_watermark@
            && match facts.leader_epoch_start {
                Some(start) => facts.follower_log_end@ >= start@,
                None => false,
            }
    }
}

/// Decide whether one candidate belongs in the next ISR proposal.
///
/// The leader always stays, and a member that is no longer assigned leaves.
/// An in-sync follower stays exactly while Kafka's `ReplicaState.isCaughtUp`
/// holds, which is `Partition.getOutOfSyncReplicas`: a follower that keeps
/// fetching but never catches up leaves once its last caught-up time is older
/// than the bound. An out-of-sync follower joins exactly when Kafka's
/// `Partition.needsExpandIsr` would add it: it is ISR-eligible
/// ([`replica_isr_eligible`]) and in sync by `Partition.isFollowerInSync`.
#[ensures(match facts.role {
    IsrMemberRole::Unassigned => !result,
    IsrMemberRole::Leader => result,
    IsrMemberRole::InSyncFollower => result == kafka_replica_is_caught_up(
        facts.follower_log_end@, facts.leader_log_end@, facts.caught_up_within_lag),
    IsrMemberRole::OutOfSyncFollower => result == (
        kafka_is_replica_isr_eligible(facts.eligibility) && kafka_is_follower_in_sync(facts)),
})]
#[must_use]
pub fn isr_candidate_selected(facts: IsrCandidateFacts) -> bool {
    match facts.role {
        IsrMemberRole::Unassigned => false,
        IsrMemberRole::Leader => true,
        IsrMemberRole::InSyncFollower => {
            facts.follower_log_end == facts.leader_log_end || facts.caught_up_within_lag
        }
        IsrMemberRole::OutOfSyncFollower => {
            let in_sync = facts.follower_log_end >= facts.leader_high_watermark
                && match facts.leader_epoch_start {
                    Some(start) => facts.follower_log_end >= start,
                    None => false,
                };
            in_sync && replica_isr_eligible(facts.eligibility)
        }
    }
}

/// Report a proposal only when at least one unique member is added or removed.
#[ensures(result == (removed@ > 0 || added@ > 0))]
#[must_use]
pub fn isr_proposal_changed(removed: usize, added: usize) -> bool {
    removed > 0 || added > 0
}

/// One remote replica as Kafka's `Partition.maybeIncrementLeaderHW` weighs
/// it.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct HwmReplica {
    /// Kafka's `ReplicaState.logEndOffset`, -1 before the replica fetched from
    /// this leader.
    pub log_end: i64,
    /// The replica is in the leader's ISR.
    pub in_isr: bool,
    /// The replica's last caught-up time is within `replica.lag.time.max.ms`.
    pub caught_up_within_lag: bool,
    pub eligibility: IsrEligibilityFacts,
}

/// The partition-wide inputs of Kafka's `Partition.maybeIncrementLeaderHW`.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct HighWatermarkFacts {
    /// The leader's high watermark before this step.
    pub current: i64,
    /// The leader's log end offset.
    pub leader_log_end: i64,
    /// Kafka's `partitionState.isr.size`: the ISR, leader included.
    pub isr_size: usize,
    /// Kafka's `Partition.effectiveMinIsr`: `min.insync.replicas` capped by
    /// the replica count.
    pub effective_min_isr: usize,
}

/// Kafka's `Partition.isUnderMinIsr` on the leader.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_is_under_min_isr(facts: HighWatermarkFacts) -> bool {
    pearlite! { facts.isr_size@ < facts.effective_min_isr@ }
}

/// Whether `replica` holds the high watermark back: it is in the ISR, or, in
/// Kafka's `shouldWaitForReplicaToJoinIsr`, it is caught up and ISR-eligible,
/// so the watermark waits for it rather than run away from a follower that is
/// about to join.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_holds_watermark(replica: HwmReplica, leader_log_end: Int) -> bool {
    pearlite! {
        replica.in_isr
            || (kafka_replica_is_caught_up(replica.log_end@, leader_log_end, replica.caught_up_within_lag)
                && kafka_is_replica_isr_eligible(replica.eligibility))
    }
}

/// The candidate Kafka's `maybeIncrementLeaderHW` loop builds over the first
/// `n` replicas: the leader's log end, lowered to every log end of a replica
/// that holds the watermark back.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(n)]
fn kafka_watermark_candidate(leader_log_end: Int, replicas: Seq<HwmReplica>, n: Int) -> Int {
    pearlite! {
        if n <= 0 {
            leader_log_end
        } else {
            if kafka_holds_watermark(replicas[n - 1], leader_log_end)
                && replicas[n - 1].log_end@ < kafka_watermark_candidate(leader_log_end, replicas, n - 1)
            {
                replicas[n - 1].log_end@
            } else {
                kafka_watermark_candidate(leader_log_end, replicas, n - 1)
            }
        }
    }
}

/// Kafka's `Partition.maybeIncrementLeaderHW` followed by
/// `UnifiedLog.maybeIncrementHighWatermark`: nothing moves while the ISR is
/// under min ISR, and otherwise the watermark rises to the candidate if that
/// is higher and never falls.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_leader_high_watermark(facts: HighWatermarkFacts, replicas: Seq<HwmReplica>) -> Int {
    pearlite! {
        if kafka_is_under_min_isr(facts) {
            facts.current@
        } else if kafka_watermark_candidate(facts.leader_log_end@, replicas, replicas.len())
            > facts.current@
        {
            kafka_watermark_candidate(facts.leader_log_end@, replicas, replicas.len())
        } else {
            facts.current@
        }
    }
}

/// Compute the leader's next high watermark.
///
/// While the ISR is below the effective `min.insync.replicas` the watermark
/// stays where it is, so every record below it reached the HWM with at least
/// min ISR replicas holding it -- the fact KIP-966 builds the eligible-leader
/// set on. Otherwise it is the lowest log end among the leader, the ISR and
/// every caught-up ISR-eligible replica outside it, if that is higher than
/// the current watermark. A replica that has not fetched from this leader
/// (log end -1) in the ISR holds it where it is.
///
/// The caller keeps `current` at or below the leader's log end, as Kafka's
/// `UnifiedLog.truncateTo` does when it truncates.
#[requires(facts.current@ <= facts.leader_log_end@)]
#[ensures(result@ == kafka_leader_high_watermark(facts, replicas@))]
#[ensures(facts.current@ <= result@ && result@ <= facts.leader_log_end@)]
#[ensures(kafka_is_under_min_isr(facts) ==> result == facts.current)]
#[must_use]
pub fn leader_high_watermark(facts: HighWatermarkFacts, replicas: &[HwmReplica]) -> i64 {
    if facts.isr_size < facts.effective_min_isr {
        return facts.current;
    }
    let mut candidate = facts.leader_log_end;
    let mut i = 0;
    #[cfg_attr(creusot, invariant(i@ <= replicas@.len()))]
    #[cfg_attr(creusot, invariant(candidate@ <= facts.leader_log_end@))]
    #[cfg_attr(creusot, invariant(candidate@
        == kafka_watermark_candidate(facts.leader_log_end@, replicas@, i@)))]
    #[cfg_attr(creusot, variant(replicas@.len() - i@))]
    while i < replicas.len() {
        let replica = replicas[i];
        let caught_up = replica.log_end == facts.leader_log_end || replica.caught_up_within_lag;
        let holds = replica.in_isr || (caught_up && replica_isr_eligible(replica.eligibility));
        if holds && replica.log_end < candidate {
            candidate = replica.log_end;
        }
        i += 1;
    }
    if candidate > facts.current {
        candidate
    } else {
        facts.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A registered, unfenced broker at epoch 7 whose last Fetch carried it.
    const ELIGIBLE: IsrEligibilityFacts = IsrEligibilityFacts {
        fenced: false,
        shutting_down: false,
        fetch_broker_epoch: Some(7),
        alive_broker_epoch: Some(7),
    };

    /// `Partition.isReplicaIsrEligible` and `isBrokerEpochIsrEligible`.
    #[test]
    fn replica_isr_eligibility_follows_kip_841() {
        for (label, facts, expected) in [
            (
                "an alive broker whose fetch carried its epoch",
                ELIGIBLE,
                true,
            ),
            (
                "a fetch that carried no epoch skips the comparison",
                IsrEligibilityFacts {
                    fetch_broker_epoch: Some(-1),
                    ..ELIGIBLE
                },
                true,
            ),
            (
                "a fetch from the broker's previous incarnation",
                IsrEligibilityFacts {
                    fetch_broker_epoch: Some(6),
                    ..ELIGIBLE
                },
                false,
            ),
            (
                "no fetch reached this leader yet",
                IsrEligibilityFacts {
                    fetch_broker_epoch: None,
                    ..ELIGIBLE
                },
                false,
            ),
            (
                "a broker the metadata cache holds no alive epoch for",
                IsrEligibilityFacts {
                    fetch_broker_epoch: Some(-1),
                    alive_broker_epoch: None,
                    ..ELIGIBLE
                },
                false,
            ),
            (
                "a fenced broker",
                IsrEligibilityFacts {
                    fenced: true,
                    ..ELIGIBLE
                },
                false,
            ),
            (
                "a broker in controlled shutdown",
                IsrEligibilityFacts {
                    shutting_down: true,
                    ..ELIGIBLE
                },
                false,
            ),
        ] {
            assert2::check!(replica_isr_eligible(facts) == expected, "{label}");
        }
    }

    /// One scan of a leader whose log ends at 120, whose high watermark is
    /// 100, and whose current epoch began at offset 90. In-sync rows follow
    /// `Partition.getOutOfSyncReplicas`, out-of-sync rows
    /// `Partition.needsExpandIsr`.
    #[test]
    fn isr_candidates_follow_kafkas_shrink_and_expand_rules() {
        use IsrMemberRole::{InSyncFollower, Leader, OutOfSyncFollower, Unassigned};
        let at = |role, follower_log_end, caught_up_within_lag| IsrCandidateFacts {
            role,
            follower_log_end,
            leader_log_end: 120,
            leader_high_watermark: 100,
            leader_epoch_start: Some(90),
            caught_up_within_lag,
            eligibility: ELIGIBLE,
        };
        for (label, facts, expected) in [
            ("the leader stays", at(Leader, -1, false), true),
            (
                "a reassigned-away member leaves",
                at(Unassigned, 120, true),
                false,
            ),
            (
                "an idle follower at the leader's log end stays",
                at(InSyncFollower, 120, false),
                true,
            ),
            (
                "a follower that caught up recently stays",
                at(InSyncFollower, 50, true),
                true,
            ),
            (
                "a follower behind for longer than the bound leaves",
                at(InSyncFollower, 119, false),
                false,
            ),
            (
                "a follower that never fetched from this leader leaves after the bound",
                at(InSyncFollower, -1, false),
                false,
            ),
            (
                "a follower at the high watermark joins",
                at(OutOfSyncFollower, 100, false),
                true,
            ),
            (
                "a follower one short of the high watermark stays out",
                at(OutOfSyncFollower, 99, true),
                false,
            ),
            (
                "a follower past the high watermark but before the epoch start stays out",
                IsrCandidateFacts {
                    leader_epoch_start: Some(110),
                    ..at(OutOfSyncFollower, 105, true)
                },
                false,
            ),
            (
                "a leader that does not know its epoch start admits nobody",
                IsrCandidateFacts {
                    leader_epoch_start: None,
                    ..at(OutOfSyncFollower, 120, true)
                },
                false,
            ),
            (
                "a fenced follower at the log end stays out",
                IsrCandidateFacts {
                    eligibility: IsrEligibilityFacts {
                        fenced: true,
                        ..ELIGIBLE
                    },
                    ..at(OutOfSyncFollower, 120, true)
                },
                false,
            ),
            (
                "a follower that never fetched from this leader stays out",
                at(OutOfSyncFollower, -1, true),
                false,
            ),
        ] {
            assert2::check!(isr_candidate_selected(facts) == expected, "{label}");
        }
        assert2::check!(!isr_proposal_changed(0, 0));
        assert2::check!(isr_proposal_changed(1, 0));
        assert2::check!(isr_proposal_changed(0, 1));
        assert2::check!(isr_proposal_changed(usize::MAX, usize::MAX));
    }

    /// `Partition.maybeIncrementLeaderHW` for a leader at log end 100 and
    /// high watermark 40, with three assigned replicas and
    /// `min.insync.replicas` 2 unless a row says otherwise.
    #[test]
    fn leader_high_watermark_follows_kafka() {
        let facts = |isr_size, effective_min_isr| HighWatermarkFacts {
            current: 40,
            leader_log_end: 100,
            isr_size,
            effective_min_isr,
        };
        let in_isr = |log_end| HwmReplica {
            log_end,
            in_isr: true,
            caught_up_within_lag: false,
            eligibility: ELIGIBLE,
        };
        let outside = |log_end, caught_up_within_lag, eligibility| HwmReplica {
            log_end,
            in_isr: false,
            caught_up_within_lag,
            eligibility,
        };
        let fenced = IsrEligibilityFacts {
            fenced: true,
            ..ELIGIBLE
        };
        let cases: [(&str, HighWatermarkFacts, &[HwmReplica], i64); 10] = [
            (
                "the slowest in-sync follower sets the watermark",
                facts(3, 2),
                &[in_isr(80), in_isr(60)],
                60,
            ),
            (
                "a leader alone in its ISR advances to its log end",
                facts(1, 1),
                &[outside(10, false, ELIGIBLE), outside(20, false, ELIGIBLE)],
                100,
            ),
            (
                "under min ISR the watermark does not move",
                facts(1, 2),
                &[outside(100, true, ELIGIBLE), outside(100, true, ELIGIBLE)],
                40,
            ),
            (
                "exactly at min ISR it moves",
                facts(2, 2),
                &[in_isr(70), outside(10, false, ELIGIBLE)],
                70,
            ),
            (
                "a caught-up eligible replica outside the ISR holds it back",
                facts(2, 2),
                &[in_isr(90), outside(55, true, ELIGIBLE)],
                55,
            ),
            (
                "a replica outside the ISR that has not caught up does not",
                facts(2, 2),
                &[in_isr(90), outside(55, false, ELIGIBLE)],
                90,
            ),
            (
                "a caught-up fenced replica outside the ISR does not",
                facts(2, 2),
                &[in_isr(90), outside(55, true, fenced)],
                90,
            ),
            (
                "an ISR member that has not fetched from this leader holds it",
                facts(3, 2),
                &[in_isr(90), in_isr(-1)],
                40,
            ),
            (
                "the watermark never falls",
                facts(3, 2),
                &[in_isr(90), in_isr(30)],
                40,
            ),
            (
                "an ISR member at the log end with nothing else caught up",
                facts(2, 2),
                &[in_isr(100), outside(0, false, ELIGIBLE)],
                100,
            ),
        ];
        for (label, facts, replicas, expected) in cases {
            assert2::check!(
                leader_high_watermark(facts, replicas) == expected,
                "{label}"
            );
        }
    }

    /// The controller holds leader epoch 5 and partition epoch 10 for a
    /// partition led by the requester. Each row is one `AlterPartition` a
    /// Kafka controller answers as `expected`, per
    /// `ReplicationControlManager.validateAlterPartitionData`.
    #[test]
    fn isr_admission_follows_kafkas_alter_partition_validation() {
        use IsrAdmission::{
            Admit, FencedLeaderEpoch, IneligibleReplica, InvalidRequest, InvalidUpdateVersion,
            NotController,
        };
        let current = AlterPartitionFacts {
            request_leader_epoch: 5,
            current_leader_epoch: 5,
            request_partition_epoch: 10,
            current_partition_epoch: 10,
            requester_is_leader: true,
            proposed_isr: ProposedIsr::Valid,
            recovery_state_valid: true,
            replicas_eligible: true,
        };
        let cases = [
            ("an up-to-date shrink", current, Admit),
            (
                "a newer leader epoch than the controller has seen",
                AlterPartitionFacts {
                    request_leader_epoch: 6,
                    ..current
                },
                NotController,
            ),
            (
                "a newer partition epoch than the controller has seen",
                AlterPartitionFacts {
                    request_partition_epoch: 11,
                    ..current
                },
                NotController,
            ),
            (
                "a newer partition epoch outranks a fenced leader epoch",
                AlterPartitionFacts {
                    request_leader_epoch: 4,
                    request_partition_epoch: 11,
                    ..current
                },
                NotController,
            ),
            (
                "a leader from an older epoch",
                AlterPartitionFacts {
                    request_leader_epoch: 4,
                    request_partition_epoch: 9,
                    requester_is_leader: false,
                    ..current
                },
                FencedLeaderEpoch,
            ),
            (
                "a request from a broker that is not the leader",
                AlterPartitionFacts {
                    requester_is_leader: false,
                    request_partition_epoch: 9,
                    ..current
                },
                InvalidRequest,
            ),
            (
                "a proposal built on an ISR the controller has since replaced",
                AlterPartitionFacts {
                    request_partition_epoch: 9,
                    proposed_isr: ProposedIsr::Invalid,
                    replicas_eligible: false,
                    ..current
                },
                InvalidUpdateVersion,
            ),
            (
                "an ISR naming an unassigned replica",
                AlterPartitionFacts {
                    proposed_isr: ProposedIsr::Invalid,
                    replicas_eligible: false,
                    ..current
                },
                InvalidRequest,
            ),
            (
                "an ISR that drops the leader",
                AlterPartitionFacts {
                    proposed_isr: ProposedIsr::WithoutLeader,
                    ..current
                },
                InvalidRequest,
            ),
            (
                "a recovering leader proposing two members, one ineligible",
                AlterPartitionFacts {
                    recovery_state_valid: false,
                    replicas_eligible: false,
                    ..current
                },
                InvalidRequest,
            ),
            (
                "a fenced broker in the proposed ISR",
                AlterPartitionFacts {
                    replicas_eligible: false,
                    ..current
                },
                IneligibleReplica,
            ),
        ];
        for (label, facts, expected) in cases {
            assert2::check!(isr_admission(facts) == expected, "{label}");
        }
    }

    /// `Replica.updateFetchStateOrThrow` over a leader whose log end was 100
    /// at the follower's previous fetch and is 120 now.
    #[test]
    fn caught_up_credit_follows_kafkas_fetch_state_update() {
        for (label, fetch_offset, last_fetch_leader_log_end, expected) in [
            (
                "the fetch reaches the current log end",
                120,
                100,
                CaughtUpCredit::ThisFetch,
            ),
            (
                "the fetch reaches the previous fetch's log end",
                100,
                100,
                CaughtUpCredit::PreviousFetch,
            ),
            (
                "the fetch falls short of both",
                99,
                100,
                CaughtUpCredit::Unchanged,
            ),
            (
                "the first fetch after a leadership change",
                0,
                -1,
                CaughtUpCredit::PreviousFetch,
            ),
        ] {
            assert2::check!(
                follower_caught_up_credit(fetch_offset, 120, last_fetch_leader_log_end) == expected,
                "{label}"
            );
        }
    }

    /// `Partition.getOutOfSyncReplicas` for the in-sync rows; krabka's
    /// expansion rule for the out-of-sync rows.
    #[test]
    fn isr_maintenance_keeps_caught_up_followers_and_drops_the_rest() {
        use IsrMemberRole::{InSyncFollower, Leader, OutOfSyncFollower, Unassigned};
        let facts =
            |role, log_end_matches_leader, caught_up_within_lag, fetch_within_lag| IsrMemberFacts {
                role,
                log_end_matches_leader,
                caught_up_within_lag,
                fetch_within_lag,
            };
        for (label, member, expected) in [
            ("the leader stays", facts(Leader, false, false, false), true),
            (
                "a reassigned-away member leaves",
                facts(Unassigned, true, true, true),
                false,
            ),
            (
                "an idle follower at the leader's log end stays",
                facts(InSyncFollower, true, false, false),
                true,
            ),
            (
                "a follower that caught up recently stays",
                facts(InSyncFollower, false, true, true),
                true,
            ),
            (
                "a follower fetching but behind for too long leaves",
                facts(InSyncFollower, false, false, true),
                false,
            ),
            (
                "a follower that stopped fetching behind the leader leaves",
                facts(InSyncFollower, false, false, false),
                false,
            ),
            (
                "a follower that fetched and caught up rejoins",
                facts(OutOfSyncFollower, false, true, true),
                true,
            ),
            (
                "a follower caught up long ago but fetching again stays out",
                facts(OutOfSyncFollower, true, false, true),
                false,
            ),
            (
                "a follower that caught up but stopped fetching stays out",
                facts(OutOfSyncFollower, false, true, false),
                false,
            ),
        ] {
            assert2::check!(isr_maintenance_selected(member) == expected, "{label}");
        }
        assert2::check!(!isr_proposal_changed(0, 0));
        assert2::check!(isr_proposal_changed(1, 0));
        assert2::check!(isr_proposal_changed(0, 1));
        assert2::check!(isr_proposal_changed(usize::MAX, usize::MAX));
    }
}
