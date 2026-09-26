//! Uniform group-assignor kernels (KIP-848 `UniformAssignor`).
//!
//! Kafka's server-side `UniformAssignor`
//! (`org.apache.kafka.coordinator.group.assignor`) picks one of two builders.
//! `UniformHomogeneousAssignmentBuilder` runs when every member subscribes to
//! the same topics, and `UniformHeterogeneousAssignmentBuilder` runs
//! otherwise. Both rank balance ahead of stickiness, and neither consults
//! replica racks. This module holds the two decisions that the builders make
//! about members: the quota each member gets in the homogeneous builder, and
//! the least-loaded subscriber that receives an unassigned partition in the
//! heterogeneous builder. The host in `krabka-broker` owns the partition
//! bookkeeping around them.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Kafka's floor-and-remainder split of a partition total over the members.
#[cfg_attr(creusot, derive(Clone, Copy))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct UniformQuotaSplit {
    /// `minimumMemberQuota`: every member gets at least this many partitions.
    pub minimum_quota: usize,
    /// `remainingMembersToGetAnExtraPartition`: this many members get one
    /// partition more than `minimum_quota`.
    pub extra_quotas: usize,
}

/// Split `total_partitions` over `member_count` members, as
/// `UniformHomogeneousAssignmentBuilder.build` does.
///
/// The contract is the Euclidean division property, which names the floor
/// and the remainder uniquely: the quotas add up to the total, and fewer
/// members than there are members get the extra partition.
#[requires(member_count@ > 0)]
#[ensures(result.minimum_quota@ * member_count@ + result.extra_quotas@ == total_partitions@)]
#[ensures(result.extra_quotas@ < member_count@)]
#[must_use]
pub fn uniform_quota_split(total_partitions: usize, member_count: usize) -> UniformQuotaSplit {
    UniformQuotaSplit {
        minimum_quota: total_partitions / member_count,
        extra_quotas: total_partitions % member_count,
    }
}

/// One member's quota in the homogeneous builder.
#[cfg_attr(creusot, derive(Clone, Copy))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct HomogeneousMemberQuota {
    /// Whether the member's target is `minimum_quota + 1`, not
    /// `minimum_quota`.
    pub takes_extra: bool,
    /// How many partitions the member keeps from its current assignment.
    pub retain: usize,
    /// How many unassigned partitions the member receives.
    pub fill: usize,
}

/// Kafka's rule for the tentative extra partition in
/// `UniformHomogeneousAssignmentBuilder.maybeRevokePartitions`.
///
/// While extra partitions are left to hand out, a member keeps the extra slot
/// when it already owns more than the minimum. Otherwise it gives the slot up
/// for a later member, unless the members that are left, this one included,
/// are no more than the extra partitions that are left. Then it must take it.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn keeps_extra_quota(
    minimum_quota: Int,
    extras_left: Int,
    owned: Int,
    members_left: Int,
) -> bool {
    pearlite! { extras_left > 0 && (owned > minimum_quota || members_left <= extras_left) }
}

/// The extra partitions that are still unclaimed when the member at `index`
/// is visited. Members are visited in order, and each member that keeps the
/// extra slot claims one.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(index)]
pub fn extras_left_before(
    minimum_quota: Int,
    extra_quotas: Int,
    owned: Seq<usize>,
    index: Int,
) -> Int {
    pearlite! {
        if index <= 0 {
            extra_quotas
        } else {
            extras_left_before(minimum_quota, extra_quotas, owned, index - 1)
                - (if keeps_extra_quota(
                    minimum_quota,
                    extras_left_before(minimum_quota, extra_quotas, owned, index - 1),
                    owned[index - 1]@,
                    owned.len() - (index - 1),
                ) { 1 } else { 0 })
        }
    }
}

/// The partition target for a member: the minimum, plus one when it keeps
/// the extra slot.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn member_target(minimum_quota: Int, quota: HomogeneousMemberQuota) -> Int {
    pearlite! { minimum_quota + (if quota.takes_extra { 1 } else { 0 }) }
}

/// Every member's quota in the homogeneous builder, in member order.
///
/// `owned[i]` is how many partitions of the subscribed topics member `i`
/// holds in its current target assignment. The contract states Kafka's
/// balance-then-stickiness rule:
///
/// - Balance: each member's target is `minimum_quota` or `minimum_quota + 1`,
///   and exactly `extra_quotas` members get the larger target, because no
///   extra partition is left unclaimed after the last member.
/// - Stickiness: each member keeps as much of its current ownership as its
///   target allows, and receives only the difference.
/// - The extra slots go by [`keeps_extra_quota`], in member order.
#[requires(extra_quotas@ <= owned@.len())]
#[requires(minimum_quota@ < usize::MAX@)]
#[ensures(result@.len() == owned@.len())]
#[ensures(forall<i: Int> 0 <= i && i < owned@.len() ==> result@[i].takes_extra == keeps_extra_quota(
    minimum_quota@,
    extras_left_before(minimum_quota@, extra_quotas@, owned@, i),
    owned@[i]@,
    owned@.len() - i,
))]
#[ensures(forall<i: Int> 0 <= i && i < owned@.len()
    ==> result@[i].retain@ + result@[i].fill@ == member_target(minimum_quota@, result@[i]))]
#[ensures(forall<i: Int> 0 <= i && i < owned@.len()
    ==> result@[i].retain@ == (if owned@[i]@ < member_target(minimum_quota@, result@[i]) {
        owned@[i]@
    } else {
        member_target(minimum_quota@, result@[i])
    }))]
#[ensures(extras_left_before(minimum_quota@, extra_quotas@, owned@, owned@.len()) == 0)]
#[must_use]
pub fn homogeneous_member_quotas(
    minimum_quota: usize,
    extra_quotas: usize,
    owned: &[usize],
) -> Vec<HomogeneousMemberQuota> {
    let mut quotas: Vec<HomogeneousMemberQuota> = Vec::new();
    let mut extras_left = extra_quotas;
    let mut i = 0usize;
    #[invariant(i@ <= owned@.len())]
    #[invariant(quotas@.len() == i@)]
    #[invariant(extras_left@ == extras_left_before(minimum_quota@, extra_quotas@, owned@, i@))]
    #[invariant(extras_left@ <= owned@.len() - i@)]
    #[invariant(forall<k: Int> 0 <= k && k < i@ ==> quotas@[k].takes_extra == keeps_extra_quota(
        minimum_quota@,
        extras_left_before(minimum_quota@, extra_quotas@, owned@, k),
        owned@[k]@,
        owned@.len() - k,
    ))]
    #[invariant(forall<k: Int> 0 <= k && k < i@
        ==> quotas@[k].retain@ + quotas@[k].fill@ == member_target(minimum_quota@, quotas@[k]))]
    #[invariant(forall<k: Int> 0 <= k && k < i@
        ==> quotas@[k].retain@ == (if owned@[k]@ < member_target(minimum_quota@, quotas@[k]) {
            owned@[k]@
        } else {
            member_target(minimum_quota@, quotas@[k])
        }))]
    #[variant(owned@.len() - i@)]
    while i < owned.len() {
        let members_left = owned.len() - i;
        let takes_extra =
            extras_left > 0 && (owned[i] > minimum_quota || members_left <= extras_left);
        let target = if takes_extra {
            minimum_quota + 1
        } else {
            minimum_quota
        };
        let retain = if owned[i] < target { owned[i] } else { target };
        quotas.push(HomogeneousMemberQuota {
            takes_extra,
            retain,
            fill: target - retain,
        });
        if takes_extra {
            extras_left -= 1;
        }
        i += 1;
    }
    quotas
}

/// A subscriber's load while the heterogeneous builder assigns one topic.
#[cfg_attr(creusot, derive(Clone, Copy))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct SubscriberLoad {
    /// Partitions the member holds now, across every topic.
    pub assigned: usize,
    /// Partitions the member held when the builder started on this topic.
    pub assigned_at_topic_start: usize,
}

/// `a` goes before `b` in the heterogeneous builder's least-loaded order: the
/// smaller current load first, then the smaller load at the start of the
/// topic. The candidate position breaks the remaining ties.
///
/// `MemberAssignmentBalancer.nextLeastLoadedMember` sorts the subscribers once
/// per topic by `(load, member index)` and then fills them level by level in
/// that order. Among the members at the lowest current level, the next one it
/// picks is therefore the first by starting load, then by member index.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn load_precedes(a: SubscriberLoad, b: SubscriberLoad) -> bool {
    pearlite! {
        a.assigned@ < b.assigned@
            || (a.assigned@ == b.assigned@ && a.assigned_at_topic_start@ < b.assigned_at_topic_start@)
    }
}

/// Choose the subscriber that receives the next unassigned partition of a
/// topic in `UniformHeterogeneousAssignmentBuilder.assignRemainingPartitions`.
///
/// The host passes the topic's subscribers in ascending member order. The
/// result is `None` exactly for an empty slice. Otherwise no candidate
/// precedes the chosen one, and the chosen one strictly precedes every
/// earlier candidate, so the first of the least-loaded candidates wins.
#[ensures((result == None) == (candidates@.len() == 0))]
#[ensures(forall<chosen: usize> result == Some(chosen) ==> chosen@ < candidates@.len()
    && (forall<j: Int> 0 <= j && j < candidates@.len()
        ==> !load_precedes(candidates@[j], candidates@[chosen@]))
    && (forall<j: Int> 0 <= j && j < chosen@
        ==> load_precedes(candidates@[chosen@], candidates@[j])))]
#[must_use]
pub fn select_least_loaded(candidates: &[SubscriberLoad]) -> Option<usize> {
    let count = candidates.len();
    if count == 0 {
        return None;
    }
    let mut best = 0usize;
    let mut i = 1usize;
    #[invariant(1 <= i@ && i@ <= candidates@.len())]
    #[invariant(best@ < i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> !load_precedes(candidates@[j], candidates@[best@]))]
    #[invariant(forall<j: Int> 0 <= j && j < best@
        ==> load_precedes(candidates@[best@], candidates@[j]))]
    #[variant(candidates@.len() - i@)]
    while i < count {
        let candidate = candidates[i];
        let current = candidates[best];
        if candidate.assigned < current.assigned
            || (candidate.assigned == current.assigned
                && candidate.assigned_at_topic_start < current.assigned_at_topic_start)
        {
            best = i;
        }
        i += 1;
    }
    Some(best)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        HomogeneousMemberQuota, SubscriberLoad, UniformQuotaSplit, homogeneous_member_quotas,
        select_least_loaded, uniform_quota_split,
    };

    #[test]
    fn quota_split_is_kafkas_floor_and_remainder() {
        // Kafka's own example: 11 partitions over 3 members.
        let rows = [
            (
                11,
                3,
                UniformQuotaSplit {
                    minimum_quota: 3,
                    extra_quotas: 2,
                },
            ),
            (
                5,
                2,
                UniformQuotaSplit {
                    minimum_quota: 2,
                    extra_quotas: 1,
                },
            ),
            (
                2,
                3,
                UniformQuotaSplit {
                    minimum_quota: 0,
                    extra_quotas: 2,
                },
            ),
            (
                6,
                3,
                UniformQuotaSplit {
                    minimum_quota: 2,
                    extra_quotas: 0,
                },
            ),
            (
                0,
                4,
                UniformQuotaSplit {
                    minimum_quota: 0,
                    extra_quotas: 0,
                },
            ),
        ];
        for (total, members, expected) in rows {
            assert!(
                uniform_quota_split(total, members) == expected,
                "{total} over {members}"
            );
        }
    }

    fn quota(takes_extra: bool, retain: usize, fill: usize) -> HomogeneousMemberQuota {
        HomogeneousMemberQuota {
            takes_extra,
            retain,
            fill,
        }
    }

    struct QuotaRow {
        name: &'static str,
        minimum: usize,
        extras: usize,
        owned: &'static [usize],
        expected: Vec<HomogeneousMemberQuota>,
    }

    #[test]
    fn homogeneous_quotas_follow_kafka_scenarios() {
        // Rows are `UniformHomogeneousAssignmentBuilderTest` scenarios, with
        // the members in ascending member-ID order.
        let rows = [
            // testFirstAssignmentTwoMembersTwoTopicsNoMemberRacks: 5
            // partitions, nothing owned. A gives the extra slot up for B.
            QuotaRow {
                name: "first assignment, 5 over 2",
                minimum: 2,
                extras: 1,
                owned: &[0, 0],
                expected: vec![quota(false, 0, 2), quota(true, 0, 3)],
            },
            // testFirstAssignmentNumMembersGreaterThanTotalNumPartitions.
            QuotaRow {
                name: "first assignment, 2 over 3",
                minimum: 0,
                extras: 2,
                owned: &[0, 0, 0],
                expected: vec![quota(false, 0, 0), quota(true, 0, 1), quota(true, 0, 1)],
            },
            // testReassignmentForTwoMembersTwoTopicsGivenUnbalancedPrevAssignment:
            // A owns 4 of 6 and gives one back.
            QuotaRow {
                name: "unbalanced previous assignment",
                minimum: 3,
                extras: 0,
                owned: &[4, 2],
                expected: vec![quota(false, 3, 0), quota(false, 2, 1)],
            },
            // testReassignmentWhenPartitionsAreAddedForTwoMembersTwoTopics:
            // 11 partitions, each member owns 3.
            QuotaRow {
                name: "partitions added",
                minimum: 5,
                extras: 1,
                owned: &[3, 3],
                expected: vec![quota(false, 3, 2), quota(true, 3, 3)],
            },
            // testReassignmentWhenOneMemberAddedAfterInitialAssignmentWithTwoMembersTwoTopics.
            QuotaRow {
                name: "member added",
                minimum: 2,
                extras: 0,
                owned: &[3, 3, 0],
                expected: vec![quota(false, 2, 0), quota(false, 2, 0), quota(false, 0, 2)],
            },
            // A member already above the minimum keeps the extra slot even
            // when later members could take it.
            QuotaRow {
                name: "owner above minimum keeps the extra slot",
                minimum: 2,
                extras: 1,
                owned: &[3, 0, 0],
                expected: vec![quota(true, 3, 0), quota(false, 0, 2), quota(false, 0, 2)],
            },
        ];
        for row in rows {
            assert!(
                homogeneous_member_quotas(row.minimum, row.extras, row.owned) == row.expected,
                "{}",
                row.name
            );
        }
    }

    fn load(assigned: usize, assigned_at_topic_start: usize) -> SubscriberLoad {
        SubscriberLoad {
            assigned,
            assigned_at_topic_start,
        }
    }

    #[test]
    fn least_loaded_selection_orders_by_load_then_start_then_position() {
        let rows: [(&str, &[SubscriberLoad], Option<usize>); 6] = [
            ("no subscribers", &[], None),
            (
                "smallest current load",
                &[load(4, 4), load(1, 1), load(1, 1)],
                Some(1),
            ),
            ("first of equal loads", &[load(2, 2), load(2, 2)], Some(0)),
            // A member that started the topic lighter goes first at a level,
            // even behind a lower member index.
            (
                "starting load breaks a level tie",
                &[load(1, 1), load(1, 0)],
                Some(1),
            ),
            (
                "current load outranks starting load",
                &[load(2, 0), load(1, 1)],
                Some(1),
            ),
            (
                "maximum loads",
                &[load(usize::MAX, 0), load(usize::MAX, 0)],
                Some(0),
            ),
        ];
        for (name, candidates, expected) in rows {
            assert!(select_least_loaded(candidates) == expected, "{name}");
        }
    }
}
