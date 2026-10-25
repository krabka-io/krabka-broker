//! `UniformAssignor`, the default of KIP-848. It ports Kafka's server-side
//! `org.apache.kafka.coordinator.group.assignor.UniformAssignor`.
//!
//! The assignor ranks balance ahead of stickiness. Every member ends with a
//! partition count within one of every other member's where the
//! subscriptions allow it, and a member keeps as much of its current target
//! assignment as that balance leaves room for. It does not consult replica
//! racks, as in Kafka.
//!
//! Two builders do the work, as in Kafka:
//!
//! - `homogeneous` ports `UniformHomogeneousAssignmentBuilder`. It runs when
//!   the group's [`SubscriptionType`] is homogeneous and every member's
//!   resolved topic IDs agree. Regex authorization is per member here, so a
//!   group that Kafka's rule calls homogeneous can still resolve to different
//!   topics, and then the heterogeneous builder keeps the result valid. The
//!   group-wide quota split and each member's quota come from the proved
//!   kernels [`uniform_quota_split`] and [`homogeneous_member_quotas`].
//! - `heterogeneous` ports `UniformHeterogeneousAssignmentBuilder`. The
//!   subscriber that receives each unassigned partition comes from the proved
//!   kernel [`select_least_loaded`]. The final balancing pass ports
//!   `MemberAssignmentBalancer` step for step.
//!
//! Kafka iterates members and homogeneous topics in hash order, so its tests
//! pin them with sorted maps. This port fixes both orders: members in
//! ascending member-ID order, and topics in ascending order of Kafka's
//! `Uuid.compareTo`.
//!
//! Kafka rejects a subscription to a topic that is missing from the metadata.
//! Here the reconciler resolves subscriptions from the same metadata
//! snapshot, so a subscribed topic that is missing, or that has a negative
//! partition count, is skipped instead. A current assignment entry outside a
//! member's subscribed topics or outside the partition range is dropped, and
//! a partition that two members claim stays with the first of them.

use {
    super::{Assignment, Assignor, GroupSpec, SubscriptionType, TopicMetadata},
    krabka_protocol::primitives::uuid::Uuid,
    krabka_verified::uniform_assignor::{
        SubscriberLoad, homogeneous_member_quotas, select_least_loaded, uniform_quota_split,
    },
    std::{cmp::Ordering, collections::HashMap},
};

#[derive(Debug)]
pub struct UniformAssignor;

impl Assignor for UniformAssignor {
    fn name(&self) -> &'static str { "uniform" }

    fn assign(&self, group: &GroupSpec, topics: &TopicMetadata) -> Assignment {
        let mut spec = Spec::new(group, topics);
        let homogeneous_group = group.subscription_type == SubscriptionType::Homogeneous
            && spec
                .subscriptions
                .windows(2)
                .all(|pair| matches!(pair, [first, second] if first == second));
        if homogeneous_group {
            homogeneous(&mut spec);
        } else {
            heterogeneous(&mut spec);
        }
        spec.into_assignment()
    }
}

/// Kafka's `Uuid.compareTo`: the most significant 64 bits as a signed long,
/// then the least significant 64 bits as a signed long.
fn kafka_uuid_order(a: &Uuid, b: &Uuid) -> Ordering {
    let halves = |uuid: &Uuid| {
        let [
            m0,
            m1,
            m2,
            m3,
            m4,
            m5,
            m6,
            m7,
            l0,
            l1,
            l2,
            l3,
            l4,
            l5,
            l6,
            l7,
        ] = uuid.0;
        (
            i64::from_be_bytes([m0, m1, m2, m3, m4, m5, m6, m7]),
            i64::from_be_bytes([l0, l1, l2, l3, l4, l5, l6, l7]),
        )
    };
    halves(a).cmp(&halves(b))
}

/// An existing topic and its partition count.
#[derive(Debug, Clone, Copy)]
struct TopicSize {
    topic_id: Uuid,
    partitions: usize,
    /// `partitions` as the metadata states it.
    partition_count: i32,
}

/// One partition, named by its topic's number in [`Spec::topics`] and its
/// index within that topic.
#[derive(Debug, Clone, Copy)]
struct PartitionSlot {
    topic: usize,
    partition: usize,
}

/// The assignor's working state. Members and topics are numbered by their
/// position in `member_ids` and `topics`.
#[derive(Debug)]
struct Spec<'a> {
    /// Member IDs in ascending order.
    member_ids: Vec<&'a str>,
    /// Existing topics with their partition counts, in Kafka `Uuid` order.
    topics: Vec<TopicSize>,
    /// Each member's subscribed topics that exist, as ascending topic numbers.
    subscriptions: Vec<Vec<usize>>,
    /// The owner of each partition of each topic, or `None` while unassigned.
    owners: Vec<Vec<Option<usize>>>,
}

impl<'a> Spec<'a> {
    fn new(group: &'a GroupSpec, metadata: &TopicMetadata) -> Self {
        let mut members: Vec<_> = group.members.iter().collect();
        members.sort_unstable_by(|a, b| a.member_id.cmp(&b.member_id));

        let mut topics: Vec<TopicSize> = metadata
            .partitions_per_topic
            .iter()
            .filter_map(|(id, count)| {
                Some(TopicSize {
                    topic_id: *id,
                    partitions: usize::try_from(*count).ok()?,
                    partition_count: *count,
                })
            })
            .collect();
        topics.sort_unstable_by(|a, b| kafka_uuid_order(&a.topic_id, &b.topic_id));
        let topic_numbers: HashMap<Uuid, usize> = topics
            .iter()
            .enumerate()
            .map(|(number, topic)| (topic.topic_id, number))
            .collect();

        let subscriptions: Vec<Vec<usize>> = members
            .iter()
            .map(|member| {
                let mut subscribed: Vec<usize> = member
                    .subscribed_topic_ids
                    .iter()
                    .filter_map(|id| topic_numbers.get(id).copied())
                    .collect();
                subscribed.sort_unstable();
                subscribed.dedup();
                subscribed
            })
            .collect();

        let mut owners: Vec<Vec<Option<usize>>> = topics
            .iter()
            .map(|topic| vec![None; topic.partitions])
            .collect();
        for (member, (subscription, spec)) in subscriptions.iter().zip(&members).enumerate() {
            for (topic_id, partitions) in &spec.assigned_partitions {
                let Some(&topic) = topic_numbers.get(topic_id) else {
                    continue;
                };
                if subscription.binary_search(&topic).is_err() {
                    continue;
                }
                for &partition in partitions {
                    let owner = usize::try_from(partition)
                        .ok()
                        .and_then(|index| owners.get_mut(topic)?.get_mut(index));
                    if let Some(owner @ None) = owner {
                        *owner = Some(member);
                    }
                }
            }
        }

        Spec {
            member_ids: members
                .iter()
                .map(|member| member.member_id.as_str())
                .collect(),
            topics,
            subscriptions,
            owners,
        }
    }

    /// The owners of `topic`'s partitions, empty for a topic number out of
    /// range.
    fn topic_owners(&self, topic: usize) -> &[Option<usize>] {
        self.owners.get(topic).map_or(&[], Vec::as_slice)
    }

    /// The owner cell of `slot`, `None` for a slot out of range.
    fn owner_mut(&mut self, slot: PartitionSlot) -> Option<&mut Option<usize>> {
        self.owners.get_mut(slot.topic)?.get_mut(slot.partition)
    }

    /// The partitions `member` owns, in topic order and then partition order.
    fn owned_by(&self, member: usize) -> Vec<PartitionSlot> {
        self.subscriptions
            .get(member)
            .into_iter()
            .flatten()
            .flat_map(|&topic| {
                self.topic_owners(topic)
                    .iter()
                    .enumerate()
                    .filter(move |(_, owner)| **owner == Some(member))
                    .map(move |(partition, _)| PartitionSlot { topic, partition })
            })
            .collect()
    }

    fn into_assignment(self) -> Assignment {
        let mut out: Assignment = self
            .member_ids
            .iter()
            .map(|id| ((*id).to_owned(), HashMap::new()))
            .collect();
        for (topic, owners) in self.topics.iter().zip(&self.owners) {
            for (partition, owner) in (0_i32..).zip(owners) {
                let Some(member_id) = owner.and_then(|member| self.member_ids.get(member)) else {
                    continue;
                };
                out.entry((*member_id).to_owned())
                    .or_default()
                    .entry(topic.topic_id)
                    .or_default()
                    .push(partition);
            }
        }
        out
    }
}

/// `UniformHomogeneousAssignmentBuilder.build`: every member subscribes to
/// the same topics.
///
/// Each member's quota is the floor or the ceiling of the partition total
/// over the members. A member keeps its current partitions up to its quota
/// and gives the rest back. The unassigned partitions, in topic order and
/// then partition order, followed by the given-back ones, go to the members
/// with room, in member order.
fn homogeneous(spec: &mut Spec<'_>) {
    let Some(subscribed) = spec.subscriptions.first().cloned() else {
        return;
    };
    let total: usize = subscribed
        .iter()
        .filter_map(|&topic| spec.topics.get(topic))
        .map(|topic| topic.partitions)
        .sum();
    let split = uniform_quota_split(total, spec.member_ids.len());

    // The unassigned partitions are listed before any is given back below.
    let mut unassigned: Vec<PartitionSlot> = Vec::new();
    unassigned.extend(subscribed.iter().flat_map(|&topic| {
        spec.topic_owners(topic)
            .iter()
            .enumerate()
            .filter(|(_, owner)| owner.is_none())
            .map(move |(partition, _)| PartitionSlot { topic, partition })
    }));
    let owned: Vec<Vec<PartitionSlot>> = (0..spec.member_ids.len())
        .map(|member| spec.owned_by(member))
        .collect();
    let counts: Vec<usize> = owned.iter().map(Vec::len).collect();
    let quotas = homogeneous_member_quotas(split.minimum_quota, split.extra_quotas, &counts);

    for (partitions, quota) in owned.iter().zip(&quotas) {
        for &slot in partitions.iter().skip(quota.retain) {
            if let Some(owner) = spec.owner_mut(slot) {
                *owner = None;
            }
            unassigned.push(slot);
        }
    }
    // The proved quotas add up to the partition total, and every partition
    // is either retained or unassigned here, so the fills use up the list.
    let mut next = unassigned.into_iter();
    for (member, quota) in quotas.iter().enumerate() {
        for slot in next.by_ref().take(quota.fill) {
            if let Some(owner) = spec.owner_mut(slot) {
                *owner = Some(member);
            }
        }
    }
}

/// The maximum number of passes of the final balancing phase, as in Kafka.
const MAX_ITERATION_COUNT: usize = 16;

/// `UniformHeterogeneousAssignmentBuilder.build`: members subscribe to
/// different topics.
///
/// Members keep every current partition of the topics they still subscribe
/// to. The unassigned partitions go, topic by topic, to the least-loaded
/// subscriber. A final pass then moves partitions from the most-loaded to the
/// least-loaded subscribers of each topic until no move improves balance.
fn heterogeneous(spec: &mut Spec<'_>) {
    let mut builder = Heterogeneous::new(spec);
    builder.assign_remaining_partitions();
    builder.balance();
}

#[derive(Debug)]
struct Heterogeneous<'s, 'a> {
    spec: &'s mut Spec<'a>,
    /// The subscribers of each topic, in ascending member order.
    topic_subscribers: Vec<Vec<usize>>,
    /// Each member's partition count across every topic.
    sizes: Vec<usize>,
}

impl<'s, 'a> Heterogeneous<'s, 'a> {
    fn new(spec: &'s mut Spec<'a>) -> Self {
        let mut topic_subscribers = vec![Vec::new(); spec.topics.len()];
        for (member, subscription) in spec.subscriptions.iter().enumerate() {
            for &topic in subscription {
                if let Some(subscribers) = topic_subscribers.get_mut(topic) {
                    subscribers.push(member);
                }
            }
        }
        let mut sizes = vec![0; spec.member_ids.len()];
        for owner in spec.owners.iter().flatten().flatten() {
            if let Some(size) = sizes.get_mut(*owner) {
                *size += 1;
            }
        }
        Heterogeneous {
            spec,
            topic_subscribers,
            sizes,
        }
    }

    /// The subscribers of `topic`, empty for a topic number out of range.
    fn subscribers(&self, topic: usize) -> &[usize] {
        self.topic_subscribers.get(topic).map_or(&[], Vec::as_slice)
    }

    /// `member`'s partition count across every topic.
    fn size(&self, member: usize) -> usize { self.sizes.get(member).copied().unwrap_or(0) }

    /// `sortTopicIds`: partitions per subscriber descending, then subscriber
    /// count ascending, then topic ID ascending.
    fn sort_topics(&self, topics: &mut [usize]) {
        let topic_id = |topic: usize| self.spec.topics.get(topic).map(|size| size.topic_id);
        let per_subscriber = |topic: usize| {
            let partitions = self
                .spec
                .topics
                .get(topic)
                .map_or(0.0, |size| f64::from(size.partition_count));
            let subscribers = self
                .subscribers(topic)
                .iter()
                .fold(0.0, |count: f64, _| count + 1.0);
            partitions / subscribers
        };
        topics.sort_by(|&a, &b| {
            per_subscriber(b)
                .total_cmp(&per_subscriber(a))
                .then_with(|| self.subscribers(a).len().cmp(&self.subscribers(b).len()))
                .then_with(|| match (topic_id(a), topic_id(b)) {
                    (Some(a), Some(b)) => kafka_uuid_order(&a, &b),
                    (a, b) => a.is_some().cmp(&b.is_some()),
                })
        });
    }

    fn assign_partition(&mut self, slot: PartitionSlot, member: usize) {
        let Some(owner) = self.spec.owner_mut(slot) else {
            return;
        };
        let previous = owner.replace(member);
        if let Some(size) = previous.and_then(|previous| self.sizes.get_mut(previous)) {
            *size -= 1;
        }
        if let Some(size) = self.sizes.get_mut(member) {
            *size += 1;
        }
    }

    /// `assignRemainingPartitions`.
    fn assign_remaining_partitions(&mut self) {
        let mut topics: Vec<usize> = (0..self.spec.topics.len())
            .filter(|&topic| {
                !self.subscribers(topic).is_empty()
                    && self.spec.topic_owners(topic).iter().any(Option::is_none)
            })
            .collect();
        self.sort_topics(&mut topics);
        let mut loads = Vec::new();
        for topic in topics {
            let subscribers = self.subscribers(topic).to_vec();
            let start: Vec<usize> = subscribers.iter().map(|&m| self.size(m)).collect();
            let unowned: Vec<usize> = self
                .spec
                .topic_owners(topic)
                .iter()
                .enumerate()
                .filter(|(_, owner)| owner.is_none())
                .map(|(partition, _)| partition)
                .collect();
            for partition in unowned {
                loads.clear();
                loads.extend(subscribers.iter().zip(&start).map(|(&member, &at_start)| {
                    SubscriberLoad {
                        assigned: self.size(member),
                        assigned_at_topic_start: at_start,
                    }
                }));
                let Some(&chosen) =
                    select_least_loaded(&loads).and_then(|chosen| subscribers.get(chosen))
                else {
                    break;
                };
                self.assign_partition(PartitionSlot { topic, partition }, chosen);
            }
        }
    }

    /// `balance` and `balanceTopics`: pass over the topics with two or more
    /// subscribers until a full pass moves nothing, or the pass limit.
    fn balance(&mut self) {
        let mut topics: Vec<usize> = (0..self.spec.topics.len())
            .filter(|&topic| self.subscribers(topic).len() >= 2)
            .collect();
        if topics.is_empty() {
            return;
        }
        self.sort_topics(&mut topics);
        let mut last_rebalanced: Option<usize> = None;
        for _ in 0..MAX_ITERATION_COUNT {
            for (index, &topic) in topics.iter().enumerate() {
                if last_rebalanced == Some(index) {
                    return;
                }
                if self.balance_topic(topic) > 0 || last_rebalanced.is_none() {
                    last_rebalanced = Some(index);
                }
            }
        }
    }

    /// `balanceTopic`: move the topic's partitions from its most-loaded
    /// subscribers to its least-loaded ones. Returns how many moved.
    fn balance_topic(&mut self, topic: usize) -> usize {
        let Some(mut balancer) = Balancer::new(self.subscribers(topic), &self.sizes) else {
            return 0;
        };
        if balancer.imbalance(&self.sizes) <= 1 {
            return 0;
        }

        // The topic's partitions grouped by owner, and each owner's range.
        let owners = self.spec.topic_owners(topic);
        let mut partitions: Vec<(Option<usize>, usize)> = owners
            .iter()
            .enumerate()
            .map(|(partition, &owner)| (owner, partition))
            .collect();
        partitions.sort_unstable();
        let mut start: HashMap<usize, usize> = HashMap::new();
        let mut end: HashMap<usize, usize> = HashMap::new();
        for (index, &(owner, _)) in partitions.iter().enumerate() {
            if let Some(owner) = owner {
                start.entry(owner).or_insert(index);
                end.insert(owner, index + 1);
            }
        }

        let mut moved = 0;
        while !balancer.is_balanced() {
            // The most-loaded member that still holds a partition of the
            // topic, and the end of its range.
            let most_loaded = loop {
                let Some(member) = balancer.next_most_loaded(&self.sizes) else {
                    break None;
                };
                match (start.get(&member), end.get(&member)) {
                    (Some(first), Some(last)) if last > first => break Some(member),
                    _ => balancer.exclude_most_loaded(),
                }
            };
            let Some(last) = most_loaded.and_then(|member| end.get_mut(&member)) else {
                break;
            };
            *last -= 1;
            let Some(&(_, partition)) = partitions.get(*last) else {
                break;
            };
            let Some(least_loaded) = balancer.next_least_loaded(&self.sizes) else {
                break;
            };
            if balancer.is_balanced() {
                break;
            }
            self.assign_partition(PartitionSlot { topic, partition }, least_loaded);
            moved += 1;
        }
        moved
    }
}

/// Kafka's `MemberAssignmentBalancer`, over one topic's subscribers.
///
/// It keeps two ranges of the subscribers sorted by load: the least-loaded
/// range grows to the right one level at a time, and the most-loaded range
/// grows to the left. Balancing ends when the two levels are within one.
#[derive(Debug)]
struct Balancer {
    sorted_members: Vec<usize>,
    /// `[0, least_loaded_range_end)` is the least-loaded range.
    least_loaded_range_end: usize,
    least_loaded_range_partition_count: i64,
    next_least_loaded_member: usize,
    /// `[most_loaded_range_start, most_loaded_range_end)` is the most-loaded
    /// range.
    most_loaded_range_start: usize,
    most_loaded_range_end: usize,
    most_loaded_range_partition_count: i64,
    /// The next position to take from, counting down. `None` is Kafka's -1.
    next_most_loaded_member: Option<usize>,
}

/// A load as a signed level, so that a level can sit one below zero. It
/// saturates, which no partition count reaches.
fn level(size: usize) -> i64 { i64::try_from(size).unwrap_or(i64::MAX) }

/// The load of `member` in `sizes`, as a level.
fn load(sizes: &[usize], member: usize) -> i64 { level(sizes.get(member).copied().unwrap_or(0)) }

impl Balancer {
    /// `MemberAssignmentBalancer.initialize`, or `None` for no members.
    fn new(members: &[usize], sizes: &[usize]) -> Option<Self> {
        let mut sorted_members = members.to_vec();
        sorted_members.sort_by_key(|&member| (sizes.get(member).copied(), member));
        let (&least, &most) = (sorted_members.first()?, sorted_members.last()?);
        let len = sorted_members.len();
        Some(Self {
            least_loaded_range_end: 0,
            least_loaded_range_partition_count: load(sizes, least) - 1,
            next_least_loaded_member: 0,
            most_loaded_range_start: len,
            most_loaded_range_end: len,
            most_loaded_range_partition_count: load(sizes, most) + 1,
            next_most_loaded_member: len.checked_sub(1),
            sorted_members,
        })
    }

    /// The load of the member at `position` of the sorted members.
    fn load_at(&self, sizes: &[usize], position: usize) -> Option<i64> {
        self.sorted_members
            .get(position)
            .map(|&member| load(sizes, member))
    }

    /// The load gap between the most and the least loaded member, which
    /// `initialize` returns.
    fn imbalance(&self, sizes: &[usize]) -> i64 {
        match (self.sorted_members.first(), self.sorted_members.last()) {
            (Some(&first), Some(&last)) => load(sizes, last) - load(sizes, first),
            _ => 0,
        }
    }

    /// `nextLeastLoadedMember`.
    fn next_least_loaded(&mut self, sizes: &[usize]) -> Option<usize> {
        if self.next_least_loaded_member >= self.least_loaded_range_end {
            self.least_loaded_range_partition_count += 1;
            while self.load_at(sizes, self.least_loaded_range_end)
                == Some(self.least_loaded_range_partition_count)
            {
                self.least_loaded_range_end += 1;
            }
            self.next_least_loaded_member = 0;
        }
        let member = self
            .sorted_members
            .get(self.next_least_loaded_member)
            .copied();
        self.next_least_loaded_member += 1;
        member
    }

    /// `nextMostLoadedMember`.
    fn next_most_loaded(&mut self, sizes: &[usize]) -> Option<usize> {
        if self
            .next_most_loaded_member
            .is_none_or(|next| next < self.most_loaded_range_start)
        {
            let below_start = self
                .most_loaded_range_start
                .checked_sub(1)
                .and_then(|position| self.load_at(sizes, position));
            match below_start {
                Some(level) if self.most_loaded_range_end <= self.most_loaded_range_start => {
                    self.most_loaded_range_partition_count = level;
                }
                _ => self.most_loaded_range_partition_count -= 1,
            }
            while let Some(position) = self.most_loaded_range_start.checked_sub(1)
                && self.load_at(sizes, position) == Some(self.most_loaded_range_partition_count)
            {
                self.most_loaded_range_start = position;
            }
            self.next_most_loaded_member = self.most_loaded_range_end.checked_sub(1);
        }
        let next = self.next_most_loaded_member?;
        self.next_most_loaded_member = next.checked_sub(1);
        self.sorted_members.get(next).copied()
    }

    /// `excludeMostLoadedMember`: swap the member last returned by
    /// `next_most_loaded` to the end of the most-loaded range, and shrink it.
    fn exclude_most_loaded(&mut self) {
        let returned = self.next_most_loaded_member.map_or(0, |next| next + 1);
        self.sorted_members
            .swap(returned, self.most_loaded_range_end - 1);
        self.most_loaded_range_end -= 1;
    }

    /// `isBalanced`.
    fn is_balanced(&self) -> bool {
        self.most_loaded_range_partition_count - self.least_loaded_range_partition_count <= 1
    }
}

#[cfg(test)]
mod tests;
