//! The KIP-496 decision table: one requested `(topic, partition)` tuple in,
//! one response row plus, when the deletion may proceed, one tombstone record
//! out.
//!
//! This is the whole per-partition outcome of an `OffsetDelete`, and it is
//! pure. The handler feeds it the ACL decisions, the live subscriptions and
//! the partition counts it gathered, so every branch of the specification is
//! testable without a broker.

use std::collections::HashSet;

use krabka_protocol::{
    owned::offset_delete_response::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic},
    records::Record,
};

use crate::{authorizer::AuthorizationResult, codes, coordinator::persistence::OffsetCommitValue};

/// Pure helper: build the per-topic / per-partition response rows for a
/// decoded `OffsetDeleteRequest`, plus the tombstone records that need to
/// be appended to the group's `__consumer_offsets` partition and the `(topic, partition)`
/// keys to remove from in-memory `Group.committed_offsets` once the
/// append succeeds.
///
/// It also returns the tombstone records to append to `__consumer_offsets-0`,
/// and the `(topic, partition)` keys to remove from the in-memory
/// `Group.committed_offsets` once the append succeeds.
///
/// The branches match KIP-496:
///   - `topic_decisions[name] == Deny` gives `TOPIC_AUTHORIZATION_FAILED` for
///     every partition in the topic.
///   - `subscribed_topics.contains(name)` gives `GROUP_SUBSCRIBED_TO_TOPIC`
///     for every partition in the topic.
///   - an absent `topic_partition_counts[name]`, or a `partition_index` out of
///     range, gives `UNKNOWN_TOPIC_OR_PARTITION`.
///   - otherwise the helper queues a tombstone and returns `NONE`.
pub(super) fn build_response_rows(
    group_id: &str,
    topics: &[krabka_protocol::owned::offset_delete_request::OffsetDeleteRequestTopic],
    topic_decisions: &std::collections::HashMap<&str, AuthorizationResult>,
    subscribed_topics: &HashSet<String>,
    topic_partition_counts: &std::collections::HashMap<&str, i32>,
) -> (
    Vec<OffsetDeleteResponseTopic>,
    Vec<Record>,
    Vec<(String, i32)>,
) {
    let mut topics_out: Vec<OffsetDeleteResponseTopic> = Vec::with_capacity(topics.len());
    let mut tombstones: Vec<Record> = Vec::new();
    let mut to_remove: Vec<(String, i32)> = Vec::new();
    let mut delta: i32 = 0;

    for topic in topics {
        let denied = topic_decisions
            .get(topic.name.as_str())
            .copied()
            .unwrap_or(AuthorizationResult::Deny)
            == AuthorizationResult::Deny;
        let mut partitions_out: Vec<OffsetDeleteResponsePartition> =
            Vec::with_capacity(topic.partitions.len());

        for part in &topic.partitions {
            let code = if denied {
                codes::TOPIC_AUTHORIZATION_FAILED
            } else if subscribed_topics.contains(&topic.name) {
                codes::GROUP_SUBSCRIBED_TO_TOPIC
            } else {
                match topic_partition_counts.get(topic.name.as_str()) {
                    Some(n) if part.partition_index >= 0 && part.partition_index < *n => {
                        tombstones.push(Record {
                            offset_delta: delta,
                            timestamp_delta: 0,
                            key: Some(OffsetCommitValue::encode_key(
                                group_id,
                                &topic.name,
                                part.partition_index,
                            )),
                            value: None, // null value = tombstone
                            ..Default::default()
                        });
                        delta += 1;
                        to_remove.push((topic.name.clone(), part.partition_index));
                        codes::NONE
                    }
                    _ => codes::UNKNOWN_TOPIC_OR_PARTITION,
                }
            };
            partitions_out.push(OffsetDeleteResponsePartition {
                partition_index: part.partition_index,
                error_code: code,
                ..Default::default()
            });
        }

        topics_out.push(OffsetDeleteResponseTopic {
            name: topic.name.clone(),
            partitions: partitions_out,
            ..Default::default()
        });
    }

    (topics_out, tombstones, to_remove)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert2::{assert, check};

    use super::*;
    use crate::handlers::offset_delete::test_support::{
        expected_row, expected_topic, req_with_topics,
    };

    #[test]
    fn build_rows_denied_topic_returns_topic_authorization_failed_per_partition() {
        let req = req_with_topics(&[("t1", &[0, 1])]);
        let mut decisions = HashMap::new();
        decisions.insert("t1", AuthorizationResult::Deny);
        let subscribed: HashSet<String> = HashSet::new();
        let mut counts = HashMap::new();
        counts.insert("t1", 4);

        let (out, tombs, to_remove) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        assert!(tombs.is_empty(), "no tombstones when denied");
        assert!(to_remove.is_empty());
        for p in &out[0].partitions {
            assert!(p.error_code == codes::TOPIC_AUTHORIZATION_FAILED);
        }
    }

    #[test]
    fn build_rows_subscribed_topic_returns_group_subscribed_per_partition() {
        let req = req_with_topics(&[("t1", &[0])]);
        let mut decisions = HashMap::new();
        decisions.insert("t1", AuthorizationResult::Allow);
        let mut subscribed: HashSet<String> = HashSet::new();
        subscribed.insert("t1".to_string());
        let mut counts = HashMap::new();
        counts.insert("t1", 4);

        let (out, tombs, _) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        assert!(tombs.is_empty(), "subscribed topic → no tombstone");
        assert!(out[0].partitions[0].error_code == codes::GROUP_SUBSCRIBED_TO_TOPIC);
    }

    #[test]
    fn build_rows_missing_topic_returns_unknown_topic_or_partition() {
        let req = req_with_topics(&[("ghost", &[0])]);
        let mut decisions = HashMap::new();
        decisions.insert("ghost", AuthorizationResult::Allow);
        let subscribed: HashSet<String> = HashSet::new();
        // counts is empty: topic doesn't exist in image.
        let counts: HashMap<&str, i32> = HashMap::new();

        let (out, tombs, _) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        assert!(tombs.is_empty());
        assert!(out[0].partitions[0].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn build_rows_partition_out_of_range_returns_unknown_topic_or_partition() {
        let req = req_with_topics(&[("t1", &[0, 99, -1])]);
        let mut decisions = HashMap::new();
        decisions.insert("t1", AuthorizationResult::Allow);
        let subscribed: HashSet<String> = HashSet::new();
        let mut counts = HashMap::new();
        counts.insert("t1", 2);

        let (out, tombs, to_remove) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        // p=0 succeeds; p=99 and p=-1 each fail with UNKNOWN_TOPIC_OR_PARTITION.
        let expected = vec![expected_topic(
            "t1",
            vec![
                expected_row(0, codes::NONE),
                expected_row(99, codes::UNKNOWN_TOPIC_OR_PARTITION),
                expected_row(-1, codes::UNKNOWN_TOPIC_OR_PARTITION),
            ],
        )];
        check!(out == expected);
        check!(tombs.len() == 1, "only p=0 queued");
        check!(to_remove == vec![("t1".to_string(), 0)]);
    }

    #[test]
    fn build_rows_happy_path_queues_tombstone_with_increasing_deltas() {
        let req = req_with_topics(&[("t1", &[0, 2]), ("t2", &[7])]);
        let mut decisions = HashMap::new();
        decisions.insert("t1", AuthorizationResult::Allow);
        decisions.insert("t2", AuthorizationResult::Allow);
        let subscribed: HashSet<String> = HashSet::new();
        let mut counts = HashMap::new();
        counts.insert("t1", 4);
        counts.insert("t2", 8);

        let (out, tombs, to_remove) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        // 3 partitions × 1 tombstone each; offset deltas increase
        // monotonically across the whole batch.
        let deltas: Vec<i32> = tombs.iter().map(|t| t.offset_delta).collect();
        assert!(deltas == vec![0, 1, 2]);
        // Tombstones carry null values.
        assert!(tombs.iter().all(|t| t.value.is_none()));
        for t in &out {
            for p in &t.partitions {
                assert!(p.error_code == codes::NONE);
            }
        }
        assert!(
            to_remove
                == vec![
                    ("t1".to_string(), 0),
                    ("t1".to_string(), 2),
                    ("t2".to_string(), 7),
                ]
        );
    }

    #[test]
    fn build_rows_missing_topic_in_decisions_treats_as_deny() {
        // ACL decisions map didn't include the topic → treat as Deny
        // (defensive default). Per-partition TOPIC_AUTHORIZATION_FAILED.
        let req = req_with_topics(&[("t1", &[0])]);
        let decisions: HashMap<&str, AuthorizationResult> = HashMap::new();
        let subscribed: HashSet<String> = HashSet::new();
        let mut counts = HashMap::new();
        counts.insert("t1", 4);

        let (out, tombs, _) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        assert!(tombs.is_empty());
        assert!(out[0].partitions[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED);
    }
}
