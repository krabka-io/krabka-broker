//! `UniformAssignor`, the default of KIP-848. It distributes partitions as
//! evenly as possible across the members that subscribe to each topic.
//!
//! The assignor is rack-aware. When the `TopicMetadata` of the coordinator
//! carries a `partition_racks` entry for `(topic_id, partition_index)`, the
//! assignor prefers subscribers whose `rack_id` matches one of the replica
//! racks of the partition. If no subscriber matches, or the partition has no
//! rack data, all subscribers are eligible. This is the original
//! non-rack-aware behavior.
//!
//! In the eligible pool, the assignor selects the member with the lowest
//! running partition count for this topic. It breaks ties by member-id lex
//! order. Without rack data, the running minimum reduces exactly to the
//! original `p % subscribers.len()` round-robin, so behavior does not change
//! for clusters that do not expose broker racks.

use std::collections::HashMap;

use krabka_verified::select_uniform_member;

use super::{Assignment, Assignor, MemberSubscription, TopicMetadata};

#[derive(Debug)]
pub struct UniformAssignor;

impl Assignor for UniformAssignor {
    fn name(&self) -> &'static str {
        "uniform"
    }

    fn assign(&self, members: &[MemberSubscription], topics: &TopicMetadata) -> Assignment {
        let mut out: Assignment = HashMap::new();
        for m in members {
            out.insert(m.member_id.clone(), HashMap::new());
        }
        for (topic_id, partition_count) in &topics.partitions_per_topic {
            let mut subscribers: Vec<&MemberSubscription> = members
                .iter()
                .filter(|m| m.subscribed_topic_ids.contains(topic_id))
                .collect();
            subscribers.sort_unstable_by_key(|member| member.member_id.as_str());
            if subscribers.is_empty() {
                continue;
            }
            // Per-member partition count for THIS topic, used to choose
            // the least-loaded member from the eligible pool. Reset per
            // topic — KIP-848 balances within-topic, not across topics.
            let mut count_by_member = vec![0usize; subscribers.len()];

            for p in 0..*partition_count {
                let partition_racks = topics.partition_racks.get(&(*topic_id, p));
                let candidates: Vec<(usize, bool)> = subscribers
                    .iter()
                    .zip(&count_by_member)
                    .map(|(member, &count)| {
                        (
                            count,
                            partition_racks.is_some_and(|racks| {
                                member
                                    .rack_id
                                    .as_ref()
                                    .is_some_and(|rack| racks.iter().any(|r| r == rack))
                            }),
                        )
                    })
                    .collect();
                let chosen = select_uniform_member(&candidates)
                    .expect("subscribers are non-empty, so candidates are non-empty");
                count_by_member[chosen] += 1;
                let member_id = &subscribers[chosen].member_id;
                out.get_mut(member_id)
                    .expect("inserted above")
                    .entry(*topic_id)
                    .or_default()
                    .push(p);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::primitives::uuid::Uuid;

    use super::{Assignor, MemberSubscription, TopicMetadata, UniformAssignor};

    fn tid(b: u8) -> Uuid {
        Uuid([b; 16])
    }

    fn member(id: &str, topics: &[Uuid]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.into(),
            rack_id: None,
            subscribed_topic_ids: topics.to_vec(),
        }
    }

    fn member_in_rack(id: &str, rack: &str, topics: &[Uuid]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.into(),
            rack_id: Some(rack.into()),
            subscribed_topic_ids: topics.to_vec(),
        }
    }

    #[test]
    fn single_member_gets_all_partitions() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
            ..Default::default()
        };
        let a = UniformAssignor.assign(&[member("m1", &[t])], &topics);
        assert!(a["m1"][&t] == vec![0, 1, 2, 3]);
    }

    #[test]
    fn two_members_split_round_robin() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
            ..Default::default()
        };
        let a = UniformAssignor.assign(&[member("m1", &[t]), member("m2", &[t])], &topics);
        assert!(a["m1"][&t] == vec![0, 2]);
        assert!(a["m2"][&t] == vec![1, 3]);
    }

    #[test]
    fn unsubscribed_member_gets_empty_for_topic() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 2)].into(),
            ..Default::default()
        };
        let a = UniformAssignor.assign(&[member("m1", &[t]), member("m2", &[])], &topics);
        assert!(a["m1"][&t] == vec![0, 1]);
        assert!(!a["m2"].contains_key(&t) || a["m2"][&t].is_empty());
    }

    #[test]
    fn zero_partitions_no_assignment() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 0)].into(),
            ..Default::default()
        };
        let a = UniformAssignor.assign(&[member("m1", &[t])], &topics);
        assert!(!a["m1"].contains_key(&t) || a["m1"][&t].is_empty());
    }

    #[test]
    fn deterministic_under_member_input_order() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 6)].into(),
            ..Default::default()
        };
        let a1 = UniformAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t]), member("m3", &[t])],
            &topics,
        );
        let a2 = UniformAssignor.assign(
            &[member("m3", &[t]), member("m1", &[t]), member("m2", &[t])],
            &topics,
        );
        assert!(a1 == a2);
    }

    #[test]
    fn deterministic_retry_is_complete_unique_and_balanced() {
        let t = tid(1);
        let members = [member("m3", &[t]), member("m1", &[t]), member("m2", &[t])];
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 7)].into(),
            ..Default::default()
        };

        let first = UniformAssignor.assign(&members, &topics);
        let retry = UniformAssignor.assign(&members, &topics);
        assert!(first == retry, "an exact retry must be deterministic");

        let mut partitions: Vec<i32> = first
            .values()
            .flat_map(|topics| topics.get(&t).into_iter().flatten().copied())
            .collect();
        partitions.sort_unstable();
        assert!(partitions == (0..7).collect::<Vec<_>>());

        let mut loads: Vec<usize> = first
            .values()
            .map(|topics| topics.get(&t).map_or(0, Vec::len))
            .collect();
        loads.sort_unstable();
        assert!(loads == vec![2, 2, 3]);
    }

    #[test]
    fn negative_partition_count_is_fail_closed() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, -1)].into(),
            ..Default::default()
        };
        let assignment = UniformAssignor.assign(&[member("m1", &[t])], &topics);
        assert!(assignment["m1"].is_empty());
    }

    #[test]
    fn empty_members_no_panic() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
            ..Default::default()
        };
        let a = UniformAssignor.assign(&[], &topics);
        assert!(a.is_empty());
    }

    // ── rack-aware ────────────────────────────────────────────────

    /// Builds a `TopicMetadata` with rack info on every partition of `t`.
    fn topics_with_racks(
        t: Uuid,
        partitions: i32,
        racks_per_partition: Vec<Vec<&str>>,
    ) -> TopicMetadata {
        let mut partition_racks = std::collections::HashMap::new();
        for (i, racks) in racks_per_partition.into_iter().enumerate() {
            partition_racks.insert(
                (t, i32::try_from(i).unwrap()),
                racks.into_iter().map(String::from).collect(),
            );
        }
        TopicMetadata {
            partitions_per_topic: [(t, partitions)].into(),
            partition_racks,
        }
    }

    #[test]
    fn rack_aware_prefers_collocated_member() {
        // Two members, two racks, two partitions each pinned to one rack.
        // Each member must own exactly the partition for its own rack.
        let t = tid(1);
        let topics = topics_with_racks(t, 2, vec![vec!["us-east-1a"], vec!["us-east-1b"]]);
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "us-east-1a", &[t]),
                member_in_rack("m2", "us-east-1b", &[t]),
            ],
            &topics,
        );
        assert!(a["m1"][&t] == vec![0], "m1 in us-east-1a takes partition 0");
        assert!(a["m2"][&t] == vec![1], "m2 in us-east-1b takes partition 1");
    }

    #[test]
    fn rack_match_wins_even_when_an_unmatched_member_has_less_load() {
        let t = tid(1);
        let topics = topics_with_racks(t, 2, vec![vec![], vec!["rack-a"]]);
        let assignment = UniformAssignor.assign(
            &[
                member_in_rack("m1", "rack-a", &[t]),
                member_in_rack("m2", "rack-b", &[t]),
            ],
            &topics,
        );

        assert!(assignment["m1"][&t] == vec![0, 1]);
        assert!(!assignment["m2"].contains_key(&t));
    }

    #[test]
    fn mixed_topics_assign_only_subscribed_rack_eligible_members() {
        let t1 = tid(1);
        let t2 = tid(2);
        let members = [
            member_in_rack("m1", "rack-a", &[t1, t2]),
            member_in_rack("m2", "rack-b", &[t1]),
            member_in_rack("m3", "rack-c", &[t2]),
            member_in_rack("m4", "rack-a", &[]),
        ];
        let topics = TopicMetadata {
            partitions_per_topic: [(t1, 2), (t2, 2)].into(),
            partition_racks: [
                ((t1, 0), vec!["rack-a".into()]),
                ((t1, 1), vec!["rack-b".into()]),
                ((t2, 0), vec!["rack-c".into()]),
                ((t2, 1), vec!["rack-a".into()]),
            ]
            .into(),
        };

        let assignment = UniformAssignor.assign(&members, &topics);
        assert!(assignment["m1"][&t1] == vec![0]);
        assert!(assignment["m2"][&t1] == vec![1]);
        assert!(assignment["m3"][&t2] == vec![0]);
        assert!(assignment["m1"][&t2] == vec![1]);
        assert!(assignment["m4"].is_empty());
    }

    #[test]
    fn rack_aware_falls_back_to_round_robin_when_no_rack_match() {
        // Both partitions are in us-east-1a; members are in us-east-1b
        // and us-east-1c. No subscriber matches → fall back to balanced
        // round-robin over all subscribers.
        let t = tid(1);
        let topics = topics_with_racks(t, 4, vec![vec!["us-east-1a"]; 4]);
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "us-east-1b", &[t]),
                member_in_rack("m2", "us-east-1c", &[t]),
            ],
            &topics,
        );
        // 4 partitions / 2 members → 2 each, distributed evenly.
        assert!(a["m1"][&t].len() == 2);
        assert!(a["m2"][&t].len() == 2);
        // Union covers all 4 partitions exactly once.
        let mut all: Vec<i32> = a["m1"][&t]
            .iter()
            .chain(a["m2"][&t].iter())
            .copied()
            .collect();
        all.sort_unstable();
        assert!(all == vec![0, 1, 2, 3]);
    }

    #[test]
    fn rack_aware_balances_within_rack_pool() {
        // Three partitions all in us-east-1a, two members both in
        // us-east-1a. Same rack → both eligible for every partition,
        // balanced 2/1 (3/2 rounded).
        let t = tid(1);
        let topics = topics_with_racks(t, 3, vec![vec!["us-east-1a"]; 3]);
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "us-east-1a", &[t]),
                member_in_rack("m2", "us-east-1a", &[t]),
            ],
            &topics,
        );
        assert!(
            a["m1"][&t].len() + a["m2"][&t].len() == 3,
            "all partitions assigned"
        );
        assert!(
            a["m1"][&t].len().abs_diff(a["m2"][&t].len()) <= 1,
            "balanced within ±1: {:?} vs {:?}",
            a["m1"][&t],
            a["m2"][&t],
        );
    }

    #[test]
    fn rack_aware_handles_partition_with_no_rack_data() {
        // Partition 0 has rack info, partition 1 does NOT (omitted).
        // Partition 0 goes to rack-matched m1; partition 1 falls back
        // to all-subscribers and load-balances to whoever has fewer.
        let t = tid(1);
        let mut partition_racks = std::collections::HashMap::new();
        partition_racks.insert((t, 0), vec!["rack-a".into()]);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 2)].into(),
            partition_racks,
        };
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "rack-a", &[t]),
                member_in_rack("m2", "rack-b", &[t]),
            ],
            &topics,
        );
        assert!(
            a["m1"][&t] == vec![0],
            "m1 wins rack-collocated partition 0"
        );
        assert!(
            a["m2"][&t] == vec![1],
            "partition 1 has no rack data → load-balanced to m2 (m1 already has 1)"
        );
    }

    #[test]
    fn rack_aware_empty_partition_racks_acts_like_non_rack_aware() {
        // partition_racks is empty → behavior must match the original
        // non-rack-aware path for backwards-compat with old test cases.
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
            partition_racks: std::collections::HashMap::new(),
        };
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "rack-a", &[t]),
                member_in_rack("m2", "rack-b", &[t]),
            ],
            &topics,
        );
        // Same as `two_members_split_round_robin` above.
        assert!(a["m1"][&t] == vec![0, 2]);
        assert!(a["m2"][&t] == vec![1, 3]);
    }

    #[test]
    fn rack_aware_no_subscriber_has_rack_id_acts_like_non_rack_aware() {
        // Partitions have rack info, but neither subscriber declares
        // rack_id. Eligible pool falls back to all subscribers.
        let t = tid(1);
        let topics = topics_with_racks(t, 4, vec![vec!["rack-a"]; 4]);
        let a = UniformAssignor.assign(&[member("m1", &[t]), member("m2", &[t])], &topics);
        assert!(a["m1"][&t] == vec![0, 2]);
        assert!(a["m2"][&t] == vec![1, 3]);
    }
}
