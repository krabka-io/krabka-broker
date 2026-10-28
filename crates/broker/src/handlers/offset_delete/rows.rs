//! The KIP-496 decision table: the requested `(topic, partition)` tuples in,
//! the response rows plus the keys whose offsets may be deleted out.
//!
//! This is the whole per-partition outcome of an `OffsetDelete`, and it is
//! pure. The handler feeds it the ACL decisions, the group's subscribed topics
//! and the partition counts it gathered, so every branch of the specification
//! is testable without a broker.

use {
    crate::{
        authorizer::AuthorizationResult, codes, coordinator::unified::actor::SubscribedTopics,
    },
    krabka_protocol::owned::{
        offset_delete_request::OffsetDeleteRequestTopic,
        offset_delete_response::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic},
    },
    std::collections::HashMap,
};

/// The per-partition rows of an `OffsetDelete` response, and the
/// `(topic, partition)` keys whose rows answer `NONE`. The group actor
/// tombstones those of them it holds an offset for.
pub(super) struct Rows {
    pub topics: Vec<OffsetDeleteResponseTopic>,
    pub to_delete: Vec<(String, i32)>,
}

/// A requested topic and the partitions of it that pass the broker's checks.
struct ValidTopic<'a> {
    name: &'a str,
    partitions: Vec<i32>,
}

/// Builds the response rows in Kafka's two steps.
///
/// `KafkaApis.handleOffsetDeleteRequest` answers first, per topic:
///   - a `Read` denial (`topic_decisions[name] == Deny`, or no decision) gives
///     `TOPIC_AUTHORIZATION_FAILED` for every partition in the topic;
///   - a topic absent from `topic_partition_counts` gives
///     `UNKNOWN_TOPIC_OR_PARTITION` for every partition in the topic;
///   - a `partition_index` out of range gives `UNKNOWN_TOPIC_OR_PARTITION`.
///
/// `OffsetMetadataManager.deleteOffsets` then answers the partitions left:
///   - a topic that the group subscribes to gives `GROUP_SUBSCRIBED_TO_TOPIC`
///     for every one of them;
///   - otherwise each answers `NONE` and joins `to_delete`. Whether it gets
///     a tombstone depends on the offsets the group holds, which only the
///     group actor knows.
///
/// `OffsetDeleteResponse.Builder.merge` appends the coordinator's partitions
/// to a topic that the first step already answered, and its topics after the
/// first step's.
pub(super) fn build_response_rows(
    topics: &[OffsetDeleteRequestTopic],
    topic_decisions: &HashMap<&str, AuthorizationResult>,
    subscribed_topics: &SubscribedTopics,
    topic_partition_counts: &HashMap<&str, i32>,
) -> Rows {
    let mut out: Vec<OffsetDeleteResponseTopic> = Vec::new();
    let mut valid: Vec<ValidTopic<'_>> = Vec::new();

    for topic in topics {
        let denied = topic_decisions
            .get(topic.name.as_str())
            .copied()
            .unwrap_or(AuthorizationResult::Deny)
            == AuthorizationResult::Deny;
        let partition_count = topic_partition_counts.get(topic.name.as_str()).copied();
        let mut valid_partitions = Vec::new();
        for part in &topic.partitions {
            let index = part.partition_index;
            let code = match partition_count {
                _ if denied => codes::TOPIC_AUTHORIZATION_FAILED,
                Some(n) if (0..n).contains(&index) => {
                    valid_partitions.push(index);
                    continue;
                }
                _ => codes::UNKNOWN_TOPIC_OR_PARTITION,
            };
            push_row(&mut out, &topic.name, index, code);
        }
        if !valid_partitions.is_empty() {
            valid.push(ValidTopic {
                name: topic.name.as_str(),
                partitions: valid_partitions,
            });
        }
    }

    let mut to_delete: Vec<(String, i32)> = Vec::new();
    for ValidTopic { name, partitions } in valid {
        let subscribed = subscribed_topics.contains(name);
        for index in partitions {
            let code = if subscribed {
                codes::GROUP_SUBSCRIBED_TO_TOPIC
            } else {
                to_delete.push((name.to_owned(), index));
                codes::NONE
            };
            push_row(&mut out, name, index, code);
        }
    }

    Rows {
        topics: out,
        to_delete,
    }
}

/// Adds one partition row under the topic named `name`, creating the topic
/// row at the end when it is not there yet, as Kafka's `getOrCreateTopic`.
fn push_row(out: &mut Vec<OffsetDeleteResponseTopic>, name: &str, index: i32, code: i16) {
    let row = OffsetDeleteResponsePartition {
        partition_index: index,
        error_code: code,
        ..Default::default()
    };
    if let Some(topic) = out.iter_mut().find(|t| t.name == name) {
        topic.partitions.push(row);
    } else {
        out.push(OffsetDeleteResponseTopic {
            name: name.to_owned(),
            partitions: vec![row],
            ..Default::default()
        });
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::offset_delete::test_support::{expected_row, expected_topic},
        assert2::check,
        std::collections::HashSet,
    };

    struct OffsetDeletionRowsScenario {
        requests: Vec<OffsetDeleteRequestTopic>,
        subscription: SubscribedTopics,
        rows: Option<Rows>,
    }
    #[rstest::fixture]
    fn offset_deletion_rows() -> OffsetDeletionRowsScenario {
        let subscription = SubscribedTopics::Named(HashSet::new());
        OffsetDeletionRowsScenario {
            requests: Vec::new(),
            subscription,
            rows: None,
        }
    }
    #[rstest_bdd_macros::given("offset deletion requests topic partitions {requests}")]
    fn offset_deletion_partitions(
        offset_deletion_rows: &mut OffsetDeletionRowsScenario,
        requests: String,
    ) {
        use krabka_protocol::owned::offset_delete_request::OffsetDeleteRequestPartition;
        offset_deletion_rows.requests = requests
            .split(';')
            .map(|entry| {
                let (name, partitions) = entry.split_once(':').unwrap();
                OffsetDeleteRequestTopic {
                    name: name.into(),
                    partitions: partitions
                        .split(',')
                        .map(|partition| OffsetDeleteRequestPartition {
                            partition_index: partition.parse().unwrap(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }
            })
            .collect();
    }
    #[rstest_bdd_macros::given("the group subscribes to {subscription}")]
    fn offset_deletion_subscription(
        offset_deletion_rows: &mut OffsetDeletionRowsScenario,
        subscription: String,
    ) {
        offset_deletion_rows.subscription = match subscription.as_str() {
            "all topics" => SubscribedTopics::All,
            "no topics" => SubscribedTopics::Named(HashSet::new()),
            _ => SubscribedTopics::Named(subscription.split(',').map(str::to_owned).collect()),
        };
    }
    #[rstest_bdd_macros::when(
        "offset deletion validates topics t1 with 4 partitions t2 with 8 denied with 4 and absent ghost"
    )]
    fn build_offset_deletion_rows(offset_deletion_rows: &mut OffsetDeletionRowsScenario) {
        let decisions = HashMap::from([
            ("t1", AuthorizationResult::Allow),
            ("t2", AuthorizationResult::Allow),
            ("ghost", AuthorizationResult::Allow),
            ("denied", AuthorizationResult::Deny),
        ]);
        let counts = HashMap::from([("t1", 4), ("t2", 8), ("denied", 4)]);
        offset_deletion_rows.rows = Some(build_response_rows(
            &offset_deletion_rows.requests,
            &decisions,
            &offset_deletion_rows.subscription,
            &counts,
        ));
    }
    #[rstest_bdd_macros::then("the ordered offset deletion rows are {expected}")]
    fn check_offset_deletion_rows(
        offset_deletion_rows: &OffsetDeletionRowsScenario,
        expected: String,
    ) {
        let want: Vec<_> = expected
            .split(';')
            .map(|entry| {
                let (topic, partitions) = entry.split_once(':').unwrap();
                expected_topic(
                    topic,
                    partitions
                        .split(',')
                        .map(|part| {
                            let (partition, code) = part.split_once('=').unwrap();
                            expected_row(partition.parse().unwrap(), code.parse().unwrap())
                        })
                        .collect(),
                )
            })
            .collect();
        check!(offset_deletion_rows.rows.as_ref().unwrap().topics == want);
    }
    #[rstest_bdd_macros::then("the only offset keys eligible for tombstones are {expected}")]
    fn check_deletable_offset_keys(
        offset_deletion_rows: &OffsetDeletionRowsScenario,
        expected: String,
    ) {
        let want: Vec<(String, i32)> = if expected == "none" {
            Vec::new()
        } else {
            expected
                .split(';')
                .map(|entry| {
                    let (topic, partition) = entry.split_once(':').unwrap();
                    (topic.to_owned(), partition.parse().unwrap())
                })
                .collect()
        };
        check!(offset_deletion_rows.rows.as_ref().unwrap().to_delete == want);
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_offset_deletion_rows",fixtures=[offset_deletion_rows:OffsetDeletionRowsScenario]);
}
