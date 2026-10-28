use {
    super::{Balancer, UniformAssignor},
    crate::coordinator::unified::assignor::{
        Assignment, Assignor, GroupSpec, MemberSubscription, SubscriptionType, TopicIdPartition,
        TopicMetadata,
    },
    assert2::assert,
    krabka_protocol::primitives::uuid::Uuid,
    krabka_verified::uniform_assignor::{SubscriberLoad, select_least_loaded},
    proptest::prelude::*,
    std::collections::{HashMap, HashSet},
};

const HOMOGENEOUS: SubscriptionType = SubscriptionType::Homogeneous;
const HETEROGENEOUS: SubscriptionType = SubscriptionType::Heterogeneous;

/// A topic ID whose rank in Kafka's `Uuid` order is `rank`.
fn tid(rank: u8) -> Uuid { Uuid([rank; 16]) }

/// A topic and its partition count.
struct TopicCount {
    topic: Uuid,
    partitions: i32,
}

fn sized(topic: Uuid, partitions: i32) -> TopicCount { TopicCount { topic, partitions } }

/// A group together with the topic metadata it is assigned against.
#[derive(Debug)]
struct Case {
    spec: GroupSpec,
    topics: TopicMetadata,
}

fn group(subscription_type: SubscriptionType, members: Vec<MemberSubscription>) -> GroupSpec {
    GroupSpec {
        members,
        subscription_type,
    }
}

fn metadata(topics: &[TopicCount]) -> TopicMetadata {
    TopicMetadata {
        partitions_per_topic: topics
            .iter()
            .map(|count| (count.topic, count.partitions))
            .collect(),
        ..TopicMetadata::default()
    }
}

/// Kafka's `checkValidityAndBalance` validity half, plus completeness: each
/// partition of a subscribed topic goes to exactly one subscriber.
fn check_validity(group: &GroupSpec, topics: &TopicMetadata, actual: &Assignment) {
    let mut seen: HashSet<TopicIdPartition> = HashSet::new();
    for member in &group.members {
        for (topic, partitions) in &actual[&member.member_id] {
            assert!(member.subscribed_topic_ids.contains(topic));
            assert!(!partitions.is_empty());
            for partition in partitions {
                assert!(
                    seen.insert(TopicIdPartition {
                        topic_id: *topic,
                        partition: *partition,
                    }),
                    "{partition} assigned twice"
                );
            }
        }
    }
    let expected: HashSet<TopicIdPartition> = topics
        .partitions_per_topic
        .iter()
        .filter(|(topic, _)| {
            group
                .members
                .iter()
                .any(|m| m.subscribed_topic_ids.contains(topic))
        })
        .flat_map(|(topic, count)| {
            (0..*count).map(move |partition| TopicIdPartition {
                topic_id: *topic,
                partition,
            })
        })
        .collect();
    assert!(seen == expected);
}

fn sizes(actual: &Assignment) -> Vec<usize> {
    let mut sizes: Vec<usize> = actual
        .values()
        .map(|topics| topics.values().map(Vec::len).sum())
        .collect();
    sizes.sort_unstable();
    sizes
}

fn with_current(spec: &GroupSpec, current: &Assignment, order: &[&str]) -> GroupSpec {
    let members = order
        .iter()
        .map(|id| {
            let member = spec
                .members
                .iter()
                .find(|member| member.member_id == *id)
                .expect("member exists");
            MemberSubscription {
                assigned_partitions: current[*id].clone(),
                ..member.clone()
            }
        })
        .collect();
    group(spec.subscription_type, members)
}

/// Kafka's `testAssignmentReuse` and `testReassignmentStickiness`: feeding an
/// assignment back in returns it unchanged, whatever order the members come
/// in.
/// How many partitions of `previous` each member still holds in `next`.
fn retained(previous: &Assignment, next: &Assignment, member: &str) -> usize {
    previous[member]
        .iter()
        .map(|(topic, partitions)| {
            partitions
                .iter()
                .filter(|p| next[member].get(topic).is_some_and(|now| now.contains(p)))
                .count()
        })
        .sum()
}

prop_compose! {
    /// A homogeneous group with a random but valid current assignment: up
    /// to four topics of up to eight partitions, up to five members, and
    /// each partition owned by a random member or by nobody.
    fn homogeneous_case()(
        counts in prop::collection::vec(0i32..8, 1..4),
        member_count in 1usize..6,
    )(
        owners in prop::collection::vec(
            prop::option::of(0..member_count),
            usize::try_from(counts.iter().sum::<i32>()).expect("nonnegative"),
        ),
        counts in Just(counts),
        member_count in Just(member_count),
    ) -> Case {
        let topic_ids: Vec<Uuid> = (1..=counts.len())
            .map(|rank| tid(u8::try_from(rank).expect("few topics")))
            .collect();
        let mut current: Vec<HashMap<Uuid, Vec<i32>>> = vec![HashMap::new(); member_count];
        let mut owner = owners.into_iter();
        for (topic, count) in topic_ids.iter().zip(&counts) {
            for partition in 0..*count {
                if let Some(Some(member)) = owner.next() {
                    current[member].entry(*topic).or_default().push(partition);
                }
            }
        }
        let members = current
            .into_iter()
            .enumerate()
            .map(|(index, assigned_partitions)| MemberSubscription {
                member_id: format!("m{index}"),
                rack_id: None,
                subscribed_topic_ids: topic_ids.clone(),
                assigned_partitions,
            })
            .collect();
        let counts: Vec<TopicCount> = topic_ids
            .iter()
            .zip(&counts)
            .map(|(&topic, &partitions)| sized(topic, partitions))
            .collect();
        let topics = metadata(&counts);
        Case {
            spec: group(HOMOGENEOUS, members),
            topics,
        }
    }
}

proptest! {
    /// The homogeneous builder is valid, balanced within one partition, and
    /// sticky: each member keeps as many of its current partitions as its
    /// quota allows, and a second run changes nothing.
    #[test]
    fn homogeneous_is_valid_balanced_and_sticky(Case { spec, topics } in homogeneous_case()) {
        let actual = UniformAssignor.assign(&spec, &topics);
        check_validity(&spec, &topics, &actual);
        let sizes = sizes(&actual);
        prop_assert!(sizes[sizes.len() - 1] - sizes[0] <= 1);

        let previous: Assignment = spec
            .members
            .iter()
            .map(|m| (m.member_id.clone(), m.assigned_partitions.clone()))
            .collect();
        for member in &spec.members {
            let held: usize = member.assigned_partitions.values().map(Vec::len).sum();
            let quota: usize = actual[&member.member_id].values().map(Vec::len).sum();
            prop_assert!(retained(&previous, &actual, &member.member_id) == held.min(quota));
        }

        let order: Vec<&str> = spec.members.iter().rev().map(|m| m.member_id.as_str()).collect();
        let again = UniformAssignor.assign(&with_current(&spec, &actual, &order), &topics);
        prop_assert!(again == actual);
    }

    /// With no partition moved away, the balancer's least-loaded sequence
    /// is the proved kernel's lexicographic minimum of current load,
    /// starting load and member index. The heterogeneous builder relies on
    /// this when it assigns unassigned partitions through the kernel.
    #[test]
    fn kernel_selection_matches_the_balancer(
        start in prop::collection::vec(0usize..6, 1..6),
        picks in 1usize..30,
    ) {
        let members: Vec<usize> = (0..start.len()).collect();
        let mut balancer_sizes = start.clone();
        let mut kernel_sizes = start.clone();
        let mut balancer = Balancer::new(&members, &balancer_sizes).expect("members are non-empty");
        for _ in 0..picks {
            let from_balancer = balancer
                .next_least_loaded(&balancer_sizes)
                .expect("the least-loaded range holds a member");
            balancer_sizes[from_balancer] += 1;
            let loads: Vec<SubscriberLoad> = kernel_sizes
                .iter()
                .zip(&start)
                .map(|(&assigned, &assigned_at_topic_start)| SubscriberLoad {
                    assigned,
                    assigned_at_topic_start,
                })
                .collect();
            let from_kernel = select_least_loaded(&loads).expect("members are non-empty");
            kernel_sizes[from_kernel] += 1;
            prop_assert!(from_kernel == from_balancer);
        }
    }
}

use {
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{given, scenarios, then, when},
};
#[derive(Default)]
struct UniformPlacement {
    source: Option<Case>,
    actual: Option<Assignment>,
    heterogeneous: Option<Assignment>,
    reused: Vec<Assignment>,
}
#[rstest::fixture]
fn uniform_placement() -> UniformPlacement {
    // Start this scenario with fresh inputs and observations.
    UniformPlacement::default()
}

#[given("a homogeneous group has 100 three-partition topics and 49 subscribing members")]
fn large_uniform_group(uniform_placement: &mut UniformPlacement) {
    let topic_ids = (1..=100).map(tid).collect::<Vec<_>>();
    let members = (1..50)
        .map(|index| MemberSubscription {
            member_id: format!("member{index}"),
            rack_id: None,
            subscribed_topic_ids: topic_ids.clone(),
            assigned_partitions: HashMap::new(),
        })
        .collect();
    let topics = TopicMetadata {
        partitions_per_topic: topic_ids.iter().map(|id| (*id, 3)).collect(),
        ..TopicMetadata::default()
    };
    uniform_placement.source = Some(Case {
        spec: group(HOMOGENEOUS, members),
        topics,
    });
}

#[when("the uniform assignor balances the subscribed partitions")]
fn assign_uniform_group(uniform_placement: &mut UniformPlacement) {
    let source = uniform_placement
        .source
        .as_ref()
        .expect("group and topics prepared");
    uniform_placement.actual = Some(UniformAssignor.assign(&source.spec, &source.topics));
}
#[when("the same resolved subscriptions are assigned with the heterogeneous builder")]
fn assign_explicit_heterogeneous(uniform_placement: &mut UniformPlacement) {
    let source = uniform_placement.source.as_ref().expect("group prepared");
    let group = GroupSpec {
        subscription_type: HETEROGENEOUS,
        ..source.spec.clone()
    };
    uniform_placement.heterogeneous = Some(UniformAssignor.assign(&group, &source.topics));
}
#[when("the first assignment is reused in all six member orders")]
fn reuse_uniform_assignments(uniform_placement: &mut UniformPlacement) {
    let source = uniform_placement.source.as_ref().expect("group prepared");
    let first = uniform_placement
        .actual
        .as_ref()
        .expect("first assignment computed");
    for order in [
        ["A", "B", "C"],
        ["A", "C", "B"],
        ["B", "A", "C"],
        ["B", "C", "A"],
        ["C", "A", "B"],
        ["C", "B", "A"],
    ] {
        uniform_placement.reused.push(
            UniformAssignor.assign(&with_current(&source.spec, first, &order), &source.topics),
        );
    }
}
#[then("the declared homogeneous and explicit heterogeneous assignments are identical")]
fn uniform_builder_fallback(uniform_placement: &UniformPlacement) {
    assert!(uniform_placement.heterogeneous == uniform_placement.actual);
}
#[then("every subscribed partition is assigned once and member loads differ by at most one")]
fn balanced_large_assignment(uniform_placement: &UniformPlacement) {
    let source = uniform_placement.source.as_ref().expect("group prepared");
    let actual = uniform_placement
        .actual
        .as_ref()
        .expect("assignment computed");
    check_validity(&source.spec, &source.topics, actual);
    let sizes = sizes(actual);
    assert!(sizes[sizes.len() - 1] - sizes[0] <= 1);
}
#[then("the first assignment is valid and all six reordered assignments preserve every owner")]
fn sticky_reused_assignment(uniform_placement: &UniformPlacement) {
    let source = uniform_placement.source.as_ref().expect("group prepared");
    let first = uniform_placement
        .actual
        .as_ref()
        .expect("assignment computed");
    check_validity(&source.spec, &source.topics, first);
    assert!(uniform_placement.reused.len() == 6);
    for reused in &uniform_placement.reused {
        assert!(reused == first);
    }
}

use crate::coordinator::unified::test_support::partition_assignments::{
    self, ExpectedMemberPartitions, MemberAssignmentInput, TopicPartitionsInput,
};
#[given("the uniform subscription builder is {builder}")]
fn subscription_policy(uniform_placement: &mut UniformPlacement, builder: String) {
    uniform_placement.source = Some(Case {
        spec: GroupSpec {
            members: vec![],
            subscription_type: partition_assignments::subscription_type(&builder),
        },
        topics: TopicMetadata::default(),
    });
}
#[given("the uniform topic partitions and replica racks are")]
fn assignment_topic_metadata(
    uniform_placement: &mut UniformPlacement,
    #[datatable] rows: Rows<TopicPartitionsInput>,
) {
    uniform_placement
        .source
        .as_mut()
        .expect("subscription builder given")
        .topics = partition_assignments::topic_metadata(rows);
}
#[given("the uniform members have these subscriptions and previous assignments")]
fn assignment_member_subscriptions(
    uniform_placement: &mut UniformPlacement,
    #[datatable] rows: Rows<MemberAssignmentInput>,
) {
    uniform_placement
        .source
        .as_mut()
        .expect("subscription builder given")
        .spec
        .members = partition_assignments::member_subscriptions(rows);
}
#[then("the complete uniform assignment is")]
fn complete_assignment(
    uniform_placement: &UniformPlacement,
    #[datatable] rows: Rows<ExpectedMemberPartitions>,
) {
    let expected = partition_assignments::expected_assignment(rows);
    assert!(uniform_placement.actual.as_ref() == Some(&expected));
    let source = uniform_placement
        .source
        .as_ref()
        .expect("group and topics prepared");
    check_validity(
        &source.spec,
        &source.topics,
        uniform_placement
            .actual
            .as_ref()
            .expect("assignment computed"),
    );
}

scenarios!("tests/features/migrated/src/coordinator/unified/assignor/uniform/tests.feature",fixtures=[uniform_placement:UniformPlacement]);
