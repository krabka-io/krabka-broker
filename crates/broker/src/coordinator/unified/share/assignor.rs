//! KIP-932 share-group assignor, a port of Kafka's `SimpleAssignor`.
//!
//! Share-group assignment is **non-exclusive**: when a group has more members
//! than partitions, several members share a partition. The assignor balances
//! first and keeps as much of the previous target assignment as balance
//! allows. A homogeneous group, where every member subscribes to the same
//! topics, balances all of its partitions together
//! (`SimpleHomogeneousAssignmentBuilder`). A heterogeneous group balances each
//! topic among the members that subscribe to it
//! (`SimpleHeterogeneousAssignmentBuilder`). Rack ids are ignored, as in
//! Kafka.
//!
//! Kafka walks Java hash maps and hash sets, whose order follows the hash of
//! each member id and partition. This port walks members in member-id order
//! and partitions in `(topic id, partition)` order, so it is deterministic.
//! Where Kafka's walk order decides between equally balanced results, the
//! member that gets a partition can differ.

use {
    crate::coordinator::unified::assignor::{
        Assignment, GroupSpec, SubscriptionType, TopicMetadata,
    },
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap, HashSet},
        ops::Bound,
    },
};

/// A topic id as an ordered key.
type TopicKey = [u8; 16];
/// A topic id and a partition as an ordered key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TopicPartition {
    topic: TopicKey,
    partition: i32,
}
/// One member's assignment, in key order.
type MemberAssignment = BTreeMap<TopicKey, BTreeSet<i32>>;

/// Kafka's `SimpleAssignor`, which share groups report as `simple`.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShareGroupAssignor;

impl ShareGroupAssignor {
    #[must_use]
    pub fn name(&self) -> &'static str { "simple" }

    /// The target assignment of `group` over `topics`.
    ///
    /// Each member's `assigned_partitions` is its current target assignment.
    /// `assignable` is Kafka's `GroupSpec.isPartitionAssignable` map: with
    /// `Some`, only the listed partitions are assigned; with `None`, every
    /// partition is.
    #[must_use]
    pub fn assign(
        &self,
        group: &GroupSpec,
        topics: &TopicMetadata,
        assignable: Option<&HashMap<Uuid, HashSet<i32>>>,
    ) -> Assignment {
        let mut members: Vec<_> = group.members.iter().collect();
        members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
        if members.is_empty() {
            return Assignment::new();
        }
        let subscriptions: Vec<BTreeSet<TopicKey>> = members
            .iter()
            .map(|m| m.subscribed_topic_ids.iter().map(|t| t.0).collect())
            .collect();
        let target_partitions = |topic: TopicKey| -> Vec<i32> {
            let count = topics
                .partitions_per_topic
                .get(&Uuid(topic))
                .copied()
                .unwrap_or(0);
            (0..count)
                .filter(|p| {
                    assignable.is_none_or(|a| a.get(&Uuid(topic)).is_some_and(|s| s.contains(p)))
                })
                .collect()
        };
        // A previous assignment keeps only partitions that are still target
        // partitions. Kafka's builders keep a partition that stopped being
        // assignable; that cannot happen in Kafka, where a topic never shrinks
        // and an initialized partition stays initialized, but here it would
        // hand out a partition the image no longer holds.
        let old: Vec<MemberAssignment> = members
            .iter()
            .map(|m| {
                let mut assignment: MemberAssignment = m
                    .assigned_partitions
                    .iter()
                    .map(|(t, parts)| (t.0, parts.iter().copied().collect()))
                    .collect();
                for (topic, parts) in &mut assignment {
                    let targets = target_partitions(*topic);
                    parts.retain(|p| targets.contains(p));
                }
                assignment
            })
            .collect();
        let assignments = match group.subscription_type {
            SubscriptionType::Homogeneous => {
                subscriptions.first().map_or_else(Vec::new, |subscribed| {
                    homogeneous(subscribed, old, target_partitions)
                })
            }
            SubscriptionType::Heterogeneous => {
                heterogeneous(&subscriptions, old, target_partitions)
            }
        };
        members
            .iter()
            .zip(assignments)
            .map(|(m, assignment)| {
                let assignment = assignment
                    .into_iter()
                    .filter(|(_, parts)| !parts.is_empty())
                    .map(|(t, parts)| (Uuid(t), parts.into_iter().collect()))
                    .collect();
                (m.member_id.clone(), assignment)
            })
            .collect()
    }
}

/// Kafka's `desiredSharing`: how many members each partition should have.
fn desired_sharing(members: usize, partitions: usize) -> usize {
    if partitions == 0 {
        0
    } else {
        members.div_ceil(partitions)
    }
}

/// Kafka's `desiredAssignmentCount` of the member at `index` of `members`:
/// the members split `sharing * partitions` assignments as evenly as the
/// ceiling steps allow. Kafka computes the steps in `double`; this computes
/// them exactly.
fn desired_count(index: usize, members: usize, sharing: usize, partitions: usize) -> usize {
    let total = sharing * partitions;
    (total * (index + 1)).div_ceil(members) - (total * index).div_ceil(members)
}

/// The next member after `cursor` in `set`, which is Java's
/// `Iterator.next` over a set that the loop removes from as it goes.
fn next_after(set: &BTreeSet<usize>, cursor: Option<usize>) -> Option<usize> {
    cursor.map_or_else(
        || set.iter().next().copied(),
        |c| {
            set.range((Bound::Excluded(c), Bound::Unbounded))
                .next()
                .copied()
        },
    )
}

/// The next unfilled member for `assignRemainingPartitions`: the one after
/// `cursor`, or, when the walk ran off the end and assigned something on this
/// pass, the first one again.
fn next_member(
    unfilled: &BTreeSet<usize>,
    cursor: Option<usize>,
    assigned_this_pass: &mut bool,
) -> Option<usize> {
    if let Some(member) = next_after(unfilled, cursor) {
        return Some(member);
    }
    if !*assigned_this_pass {
        return None;
    }
    *assigned_this_pass = false;
    next_after(unfilled, None)
}

/// Revoke excess holders in their original member order, then update the caller's indices.
fn revoke_overshared_partitions<P: Copy + Ord>(
    by_partition: &mut BTreeMap<P, BTreeSet<usize>>,
    sharing: usize,
    mut remove: impl FnMut(usize, P) -> bool,
    mut revoked: impl FnMut(usize, P),
) {
    for (&partition, holders) in by_partition {
        let mut count = holders.len();
        if count <= sharing {
            continue;
        }
        for member in holders.clone() {
            if remove(member, partition) {
                count -= 1;
                holders.remove(&member);
                revoked(member, partition);
            }
            if count <= sharing {
                break;
            }
        }
    }
}

/// Kafka's `SimpleHomogeneousAssignmentBuilder.build`.
fn homogeneous(
    subscribed: &BTreeSet<TopicKey>,
    old: Vec<MemberAssignment>,
    target_partitions: impl Fn(TopicKey) -> Vec<i32>,
) -> Vec<MemberAssignment> {
    let member_count = old.len();
    if subscribed.is_empty() {
        return vec![MemberAssignment::new(); member_count];
    }
    let targets: Vec<TopicPartition> = subscribed
        .iter()
        .flat_map(|&topic| {
            target_partitions(topic)
                .into_iter()
                .map(move |partition| TopicPartition { topic, partition })
        })
        .collect();
    let sharing = desired_sharing(member_count, targets.len());
    let desired: Vec<usize> = (0..member_count)
        .map(|i| desired_count(i, member_count, sharing, targets.len()))
        .collect();

    // revokeUnassignablePartitions: drop the topics no longer subscribed and
    // count what each member keeps.
    let mut assignment = old;
    let mut by_partition: BTreeMap<TopicPartition, BTreeSet<usize>> = BTreeMap::new();
    let mut by_member: Vec<BTreeSet<TopicPartition>> = vec![BTreeSet::new(); member_count];
    let mut unfilled: BTreeSet<usize> = BTreeSet::new();
    let mut overfilled: BTreeSet<usize> = BTreeSet::new();
    for (member, (member_assignment, (held, &want))) in assignment
        .iter_mut()
        .zip(by_member.iter_mut().zip(&desired))
        .enumerate()
    {
        member_assignment.retain(|topic, _| subscribed.contains(topic));
        for (&topic, parts) in member_assignment.iter() {
            for &p in parts {
                let tp = TopicPartition {
                    topic,
                    partition: p,
                };
                by_partition.entry(tp).or_default().insert(member);
                held.insert(tp);
            }
        }
        let count = held.len();
        if count < want {
            unfilled.insert(member);
        } else if count > want {
            overfilled.insert(member);
        }
    }

    // revokeOverfilledMembers.
    for &member in &overfilled {
        let (Some(held), Some(&want), Some(member_assignment)) = (
            by_member.get_mut(member),
            desired.get(member),
            assignment.get_mut(member),
        ) else {
            continue;
        };
        while held.len() > want {
            let Some(tp) = held.pop_first() else {
                break;
            };
            remove_partition(member_assignment, tp);
            if let Some(holders) = by_partition.get_mut(&tp) {
                holders.remove(&member);
            }
        }
    }

    // revokeOversharedPartitions.
    revoke_overshared_partitions(
        &mut by_partition,
        sharing,
        |member, tp| {
            assignment
                .get_mut(member)
                .is_some_and(|member_assignment| remove_partition(member_assignment, tp))
        },
        |member, tp| {
            if let Some(held) = by_member.get_mut(member) {
                held.remove(&tp);
            }
            unfilled.insert(member);
        },
    );

    // Add the target partitions nobody holds, then assignRemainingPartitions.
    for &tp in &targets {
        by_partition.entry(tp).or_default();
    }
    assign_remaining(&by_partition, sharing, &mut unfilled, |member, tp| {
        if let Some(member_assignment) = assignment.get_mut(member) {
            member_assignment
                .entry(tp.topic)
                .or_default()
                .insert(tp.partition);
        }
        let Some(held) = by_member.get_mut(member) else {
            return true;
        };
        held.insert(tp);
        desired.get(member).is_none_or(|&want| held.len() >= want)
    });
    assignment
}

/// Kafka's `SimpleHeterogeneousAssignmentBuilder.build`: each topic is
/// balanced among its own subscribers.
///
/// Kafka keeps a member's partitions of a topic that no member subscribes to
/// any more, because it only walks subscribed topics. This port drops them,
/// so a deleted or abandoned topic leaves the assignment.
fn heterogeneous(
    subscriptions: &[BTreeSet<TopicKey>],
    old: Vec<MemberAssignment>,
    target_partitions: impl Fn(TopicKey) -> Vec<i32>,
) -> Vec<MemberAssignment> {
    let member_count = old.len();
    let subscribed: BTreeSet<TopicKey> = subscriptions.iter().flatten().copied().collect();
    let mut assignment = old;
    for member_assignment in &mut assignment {
        member_assignment.retain(|topic, _| subscribed.contains(topic));
    }
    for &topic in &subscribed {
        let targets = target_partitions(topic);
        let subscribers: Vec<usize> = subscriptions
            .iter()
            .take(member_count)
            .enumerate()
            .filter(|(_, subscription)| subscription.contains(&topic))
            .map(|(member, _)| member)
            .collect();
        let sharing = desired_sharing(subscribers.len(), targets.len());
        let mut desired = vec![0; member_count];
        for (i, &member) in subscribers.iter().enumerate() {
            if let Some(want) = desired.get_mut(member) {
                *want = desired_count(i, subscribers.len(), sharing, targets.len());
            }
        }
        let desired_of = |member: usize| desired.get(member).copied().unwrap_or(0);

        // revokeUnassignablePartitions.
        let mut by_partition: BTreeMap<i32, BTreeSet<usize>> = BTreeMap::new();
        let mut by_member: BTreeMap<usize, BTreeSet<i32>> = BTreeMap::new();
        for (member, member_assignment) in assignment.iter().enumerate() {
            for &p in member_assignment.get(&topic).into_iter().flatten() {
                by_partition.entry(p).or_default().insert(member);
                by_member.entry(member).or_default().insert(p);
            }
        }

        // revokeOverfilledMembers.
        for (&member, held) in &mut by_member {
            while held.len() > desired_of(member) {
                let Some(p) = held.pop_first() else {
                    break;
                };
                if let Some(holders) = by_partition.get_mut(&p) {
                    holders.remove(&member);
                }
                if let Some(member_assignment) = assignment.get_mut(member) {
                    remove_partition(member_assignment, TopicPartition {
                        topic,
                        partition: p,
                    });
                }
            }
        }

        // revokeOversharedPartitions.
        revoke_overshared_partitions(
            &mut by_partition,
            sharing,
            |member, partition| {
                assignment.get_mut(member).is_some_and(|member_assignment| {
                    remove_partition(member_assignment, TopicPartition { topic, partition })
                })
            },
            |member, partition| {
                if let Some(held) = by_member.get_mut(&member) {
                    held.remove(&partition);
                }
            },
        );

        for &p in &targets {
            by_partition.entry(p).or_default();
        }

        // assignRemainingPartitions.
        let held = |by_member: &BTreeMap<usize, BTreeSet<i32>>, member: usize| {
            by_member.get(&member).map_or(0, BTreeSet::len)
        };
        let mut unfilled: BTreeSet<usize> = subscribers
            .iter()
            .copied()
            .filter(|&m| held(&by_member, m) < desired_of(m))
            .collect();
        assign_remaining(&by_partition, sharing, &mut unfilled, |member, p| {
            if let Some(member_assignment) = assignment.get_mut(member) {
                member_assignment.entry(topic).or_default().insert(p);
            }
            by_member.entry(member).or_default().insert(p);
            held(&by_member, member) >= desired_of(member)
        });
    }
    assignment
}

/// Fill undershared partitions in Kafka's member order. `assign` returns whether
/// the member reached its desired count.
fn assign_remaining<P: Copy>(
    by_partition: &BTreeMap<P, BTreeSet<usize>>,
    sharing: usize,
    unfilled: &mut BTreeSet<usize>,
    mut assign: impl FnMut(usize, P) -> bool,
) {
    let mut cursor = None;
    let mut assigned_this_pass = false;
    for (&partition, holders) in by_partition {
        if unfilled.is_empty() {
            break;
        }
        let mut to_make = sharing.saturating_sub(holders.len());
        while to_make > 0 {
            let Some(member) = next_member(unfilled, cursor, &mut assigned_this_pass) else {
                break;
            };
            cursor = Some(member);
            // Kafka checks the original holders and never adds the member it
            // just assigned. A repeat insertion is a no-op, but the count
            // still drops, so fewer members can hold it than `sharing`.
            if holders.contains(&member) {
                continue;
            }
            let filled = assign(member, partition);
            to_make -= 1;
            assigned_this_pass = true;
            if filled {
                unfilled.remove(&member);
            }
        }
    }
}

/// Removes `tp` from `assignment`, and says whether it was there.
fn remove_partition(assignment: &mut MemberAssignment, tp: TopicPartition) -> bool {
    assignment
        .get_mut(&tp.topic)
        .is_some_and(|parts| parts.remove(&tp.partition))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::assignor::MemberSubscription,
        assert2::{assert, check},
    };

    const T1: Uuid = Uuid([1; 16]);

    /// The partition count of a topic.
    struct TopicCount {
        topic: Uuid,
        partitions: i32,
    }

    fn count(topic: Uuid, partitions: i32) -> TopicCount { TopicCount { topic, partitions } }

    fn topics(counts: &[TopicCount]) -> TopicMetadata {
        TopicMetadata {
            partitions_per_topic: counts
                .iter()
                .map(|count| (count.topic, count.partitions))
                .collect(),
            ..TopicMetadata::default()
        }
    }

    fn spec(members: Vec<MemberSubscription>, subscription_type: SubscriptionType) -> GroupSpec {
        GroupSpec {
            members,
            subscription_type,
        }
    }

    /// The whole assignment for inputs from Kafka's `SimpleAssignorTest` and
    /// from the issue: balance over the group, sharing when members outnumber
    /// partitions, and stickiness.
    /// Kafka's `testIncrementalAssignment*MembersHomogeneous`: members join one
    /// at a time and then leave one at a time, every partition stays assigned,
    /// and the member counts stay balanced.
    #[test]
    fn incremental_membership_keeps_every_partition_assigned() {
        let partitions = 24;
        let topics = topics(&[count(T1, partitions)]);
        let mut current: Assignment = Assignment::new();
        let run = |ids: &[String], current: &Assignment| {
            let members = ids
                .iter()
                .map(|id| MemberSubscription {
                    member_id: id.clone(),
                    rack_id: None,
                    subscribed_topic_ids: vec![T1],
                    assigned_partitions: current.get(id).cloned().unwrap_or_default(),
                })
                .collect();
            ShareGroupAssignor.assign(&spec(members, SubscriptionType::Homogeneous), &topics, None)
        };
        let covered = |a: &Assignment| {
            a.values()
                .flat_map(|m| m.get(&T1).into_iter().flatten().copied())
                .collect::<HashSet<i32>>()
                .len()
        };
        let mut ids: Vec<String> = Vec::new();
        for i in 0..30 {
            ids.push(format!("M{i:02}"));
            let next = run(&ids, &current);
            check!(covered(&next) == 24, "{} members", ids.len());
            // Balance: member counts differ by at most one.
            let counts: Vec<usize> = next
                .values()
                .map(|m| m.get(&T1).map_or(0, Vec::len))
                .collect();
            check!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
            current = next;
        }
        while ids.len() > 1 {
            ids.pop();
            current = run(&ids, &current);
            check!(covered(&current) == 24, "{} members", ids.len());
        }
        assert!(current["M00"][&T1] == (0..24).collect::<Vec<_>>());
    }
    use {
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{given, scenarios, then, when},
    };
    struct ShareAssignmentInput {
        group: GroupSpec,
        topics: TopicMetadata,
        assignable: Option<HashMap<Uuid, HashSet<i32>>>,
    }
    #[derive(Default)]
    struct SharedPlacement {
        source: Option<ShareAssignmentInput>,
        assignment: Option<Assignment>,
    }
    #[rstest::fixture]
    fn shared_placement() -> SharedPlacement {
        // Start this scenario with fresh inputs and observations.
        SharedPlacement::default()
    }

    #[when("the Share simple assignor balances eligible partitions and retains prior ownership")]
    fn assign_share_partitions(shared_placement: &mut SharedPlacement) {
        let source = shared_placement
            .source
            .as_ref()
            .expect("group and topics prepared");
        shared_placement.assignment = Some(ShareGroupAssignor.assign(
            &source.group,
            &source.topics,
            source.assignable.as_ref(),
        ));
    }

    use crate::coordinator::unified::test_support::partition_assignments::{
        self, ExpectedMemberPartitions, MemberAssignmentInput, TopicPartitionsInput,
    };
    #[given("the Share subscription builder is {builder}")]
    fn subscription_policy(shared_placement: &mut SharedPlacement, builder: String) {
        shared_placement.source = Some(ShareAssignmentInput {
            group: GroupSpec {
                members: vec![],
                subscription_type: partition_assignments::subscription_type(&builder),
            },
            topics: TopicMetadata::default(),
            assignable: None,
        });
    }
    #[given("the Share topic partitions and replica racks are")]
    fn assignment_topic_metadata(
        shared_placement: &mut SharedPlacement,
        #[datatable] rows: Rows<TopicPartitionsInput>,
    ) {
        shared_placement
            .source
            .as_mut()
            .expect("subscription builder given")
            .topics = partition_assignments::topic_metadata(rows);
    }
    #[given("the Share members have these subscriptions and previous assignments")]
    fn assignment_member_subscriptions(
        shared_placement: &mut SharedPlacement,
        #[datatable] rows: Rows<MemberAssignmentInput>,
    ) {
        shared_placement
            .source
            .as_mut()
            .expect("subscription builder given")
            .group
            .members = partition_assignments::member_subscriptions(rows);
    }
    #[then("the complete Share assignment is")]
    fn complete_assignment(
        shared_placement: &SharedPlacement,
        #[datatable] rows: Rows<ExpectedMemberPartitions>,
    ) {
        let expected = partition_assignments::expected_assignment(rows);
        check!(shared_placement.assignment.as_ref() == Some(&expected));
    }

    #[given("the Share assignable partition set is {partitions}")]
    fn share_eligible_partitions(shared_placement: &mut SharedPlacement, partitions: String) {
        let eligible = serde_json::from_str::<Option<HashMap<String, Vec<i32>>>>(&partitions)
            .expect("eligible partitions object or null");
        shared_placement
            .source
            .as_mut()
            .expect("group and topics prepared")
            .assignable = eligible.map(|topics| {
            topics
                .into_iter()
                .map(|(topic, partitions)| {
                    (
                        Uuid([topic.parse::<u8>().expect("topic rank"); 16]),
                        partitions.into_iter().collect(),
                    )
                })
                .collect()
        });
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/share/assignor.feature",fixtures=[shared_placement:SharedPlacement]);
}
