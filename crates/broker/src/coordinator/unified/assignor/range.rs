//! `RangeAssignor`: it assigns a contiguous partition range for each topic.
//!
//! It matches the classic `RangeAssignor` semantics, which co-partition across
//! topics that have equal partition counts.

use {
    super::{Assignment, Assignor, GroupSpec, TopicMetadata},
    std::collections::HashMap,
};

#[derive(Debug)]
pub struct RangeAssignor;

impl Assignor for RangeAssignor {
    fn name(&self) -> &'static str { "range" }

    fn assign(&self, group: &GroupSpec, topics: &TopicMetadata) -> Assignment {
        let members = &group.members;
        let mut out: Assignment = HashMap::new();
        for m in members {
            out.insert(m.member_id.clone(), HashMap::new());
        }
        let mut sorted_topics: Vec<_> = topics.partitions_per_topic.iter().collect();
        sorted_topics.sort_by_key(|(id, _)| id.0);
        for (topic_id, partition_count) in sorted_topics {
            let mut names: Vec<&str> = members
                .iter()
                .filter(|m| m.subscribed_topic_ids.contains(topic_id))
                .map(|m| m.member_id.as_str())
                .collect();
            names.sort_unstable();
            let subscribers: Vec<(i32, &str)> = (0_i32..).zip(names).collect();
            let Some(&(last, _)) = subscribers.last() else {
                continue;
            };
            let n = last + 1;
            let p = *partition_count;
            let per_member = p / n;
            let remainder = p % n;
            let mut cursor = 0;
            for &(i, sub) in &subscribers {
                let extra = i32::from(i < remainder);
                let take = per_member + extra;
                if take == 0 {
                    continue;
                }
                let range: Vec<i32> = (cursor..cursor + take).collect();
                out.entry(sub.to_owned())
                    .or_default()
                    .insert(*topic_id, range);
                cursor += take;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::assignor::{MemberSubscription, SubscriptionType},
        assert2::assert,
        krabka_bdd::Given,
        krabka_protocol::primitives::uuid::Uuid,
        std::{convert::Infallible, num::ParseIntError, str::FromStr},
    };

    fn tid(b: u8) -> Uuid { Uuid([b; 16]) }
    fn member(id: &str, topics: &[Uuid]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.into(),
            rack_id: None,
            subscribed_topic_ids: topics.to_vec(),
            assigned_partitions: HashMap::new(),
        }
    }

    fn group(members: Vec<MemberSubscription>) -> GroupSpec {
        GroupSpec {
            members,
            subscription_type: SubscriptionType::Heterogeneous,
        }
    }

    #[test]
    fn deterministic_under_input_order() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 6)].into(),
            ..Default::default()
        };
        let a1 = RangeAssignor.assign(
            &group(vec![member("m1", &[t]), member("m2", &[t])]),
            &topics,
        );
        let a2 = RangeAssignor.assign(
            &group(vec![member("m2", &[t]), member("m1", &[t])]),
            &topics,
        );
        assert!(a1 == a2);
    }

    /// Comma-separated names, as a scenario lists members or topics.
    struct IdList(Vec<String>);

    impl FromStr for IdList {
        type Err = Infallible;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            Ok(Self(text.split(',').map(String::from).collect()))
        }
    }

    /// The partitions of one topic that each member owns, as a scenario writes
    /// them: `m1=0,1 m2=none`.
    struct Owned(HashMap<String, Vec<i32>>);

    impl FromStr for Owned {
        type Err = String;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            let mut owned = HashMap::new();
            for entry in text.split_whitespace() {
                let (member, partitions) = entry
                    .split_once('=')
                    .ok_or_else(|| format!("`{entry}` is not member=partitions"))?;
                let partitions = if partitions == "none" {
                    Vec::new()
                } else {
                    partitions
                        .split(',')
                        .map(str::parse)
                        .collect::<Result<_, ParseIntError>>()
                        .map_err(|error| format!("`{entry}`: {error}"))?
                };
                owned.insert(member.to_owned(), partitions);
            }
            Ok(Self(owned))
        }
    }

    /// The topics and subscriptions of a group, and what the range assignor
    /// gives it.
    struct RangeAssignment {
        topics: HashMap<String, (Uuid, i32)>,
        members: Vec<MemberSubscription>,
        assignment: Given<Assignment>,
    }

    impl RangeAssignment {
        fn add_topic(&mut self, name: &str, partitions: i32) {
            let id = tid(u8::try_from(self.topics.len() + 1).expect("a few topics"));
            self.topics.insert(name.to_owned(), (id, partitions));
        }

        fn subscribe(&mut self, member_id: &str, topics: &[String]) {
            let ids: Vec<Uuid> = topics.iter().map(|name| self.topics[name].0).collect();
            self.members.push(member(member_id, &ids));
        }
    }

    #[rstest::fixture]
    fn range_assignment() -> RangeAssignment {
        RangeAssignment {
            topics: HashMap::new(),
            members: Vec::new(),
            assignment: Given::new(
                "assignment",
                "When the range assignor assigns the partitions",
            ),
        }
    }

    #[krabka_bdd::given("a topic {topic} with {partitions} partitions")]
    fn a_topic(range_assignment: &mut RangeAssignment, topic: String, partitions: i32) {
        range_assignment.add_topic(&topic, partitions);
    }

    #[krabka_bdd::given("topics {topics} with {partitions} partitions each")]
    fn equal_topics(range_assignment: &mut RangeAssignment, topics: IdList, partitions: i32) {
        for topic in &topics.0 {
            range_assignment.add_topic(topic, partitions);
        }
    }

    #[krabka_bdd::given("members {members} subscribe to {topics}")]
    fn members_subscribe(range_assignment: &mut RangeAssignment, members: IdList, topics: IdList) {
        for member_id in &members.0 {
            range_assignment.subscribe(member_id, &topics.0);
        }
    }

    #[krabka_bdd::given("member {member_id} subscribes to {topics}")]
    fn member_subscribes(
        range_assignment: &mut RangeAssignment,
        member_id: String,
        topics: IdList,
    ) {
        range_assignment.subscribe(&member_id, &topics.0);
    }

    #[krabka_bdd::given("member {member_id} has no subscription")]
    fn member_without_subscription(range_assignment: &mut RangeAssignment, member_id: String) {
        range_assignment.subscribe(&member_id, &[]);
    }

    #[krabka_bdd::when("the range assignor assigns the partitions")]
    fn assign_ranges(range_assignment: &mut RangeAssignment) {
        let topics = TopicMetadata {
            partitions_per_topic: range_assignment.topics.values().copied().collect(),
            ..Default::default()
        };
        let assignment = RangeAssignor.assign(&group(range_assignment.members.clone()), &topics);
        range_assignment.assignment.set(assignment);
    }

    #[krabka_bdd::then("the partitions of {topic} go to {owned}")]
    fn members_own(range_assignment: &RangeAssignment, topic: String, owned: Owned) {
        let id = range_assignment.topics[&topic].0;
        let actual: HashMap<String, Vec<i32>> = range_assignment
            .assignment
            .get()
            .iter()
            .map(|(member_id, topics)| {
                (
                    member_id.clone(),
                    topics.get(&id).cloned().unwrap_or_default(),
                )
            })
            .collect();
        assert!(actual == owned.0);
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/coordinator/unified/assignor/range.feature",
        fixtures = [range_assignment: RangeAssignment]
    );
}
