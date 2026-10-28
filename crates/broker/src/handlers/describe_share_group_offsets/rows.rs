//! The topic and partition rows of a `DescribeShareGroupOffsets` response.
//!
//! This is Kafka's `GroupCoordinatorService.describeShareGroupOffsets` and
//! `computeShareGroupLagAndBuildResponse`: the start offset, leader epoch and
//! delivery complete count come from the share-state summary, the lag from
//! the partition end offset its leader reports, and a topic the metadata
//! image does not hold is answered as having no data rather than as an
//! error. The `TOPIC_AUTHORIZATION_FAILED` rows of `KafkaApis` are built here
//! as well, so every row shape of the response lives in one place.

use {
    super::end_offsets::{
        EndOffset, ErrorAnswer, LatestOffsets, TopicPartition, UNKNOWN_SERVER_ERROR_MESSAGE,
        lookup_error,
    },
    crate::{
        codes,
        error::BrokerError,
        share_coordinator::{
            coordinator::{ShareStateSummary, UNINITIALIZED_START_OFFSET},
            persister_client::SharePersister,
        },
    },
    futures_util::future::join_all,
    krabka_protocol::{
        owned::{
            describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestTopic,
            describe_share_group_offsets_response::{
                DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
            },
        },
        primitives::uuid::Uuid,
    },
    std::collections::HashMap,
};

/// Kafka's message for `TOPIC_AUTHORIZATION_FAILED`
/// (`Errors.TOPIC_AUTHORIZATION_FAILED.message()`).
const TOPIC_AUTHORIZATION_FAILED_MESSAGE: &str = "Topic authorization failed.";

/// Kafka's `PartitionFactory.DEFAULT_LEADER_EPOCH`.
const DEFAULT_LEADER_EPOCH: i32 = 0;
/// Kafka's `PartitionFactory.UNINITIALIZED_DELIVERY_COMPLETE_COUNT`.
const UNINITIALIZED_DELIVERY_COMPLETE_COUNT: i32 = -1;
/// Kafka's `PartitionFactory.UNINITIALIZED_LAG`.
const UNINITIALIZED_LAG: i64 = -1;

/// Kafka's `Persister.readSummary`, one partition at a time.
pub(super) trait ShareSummaries {
    /// The share-state summary of `(group, topic_id, partition)`: `None` for
    /// a key with no state, or the error of the read.
    async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<ShareStateSummary>, BrokerError>;
}

impl ShareSummaries for SharePersister {
    async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<ShareStateSummary>, BrokerError> {
        self.read_state_summary(group, topic_id, partition).await
    }
}

/// Topics without a visible id use the same envelope, with caller-specific partition rows.
fn unidentified_topic(
    topic: DescribeShareGroupOffsetsRequestTopic,
    row: impl FnMut(i32) -> DescribeShareGroupOffsetsResponsePartition,
) -> DescribeShareGroupOffsetsResponseTopic {
    DescribeShareGroupOffsetsResponseTopic {
        topic_name: topic.topic_name,
        topic_id: Uuid::default(),
        partitions: topic.partitions.into_iter().map(row).collect(),
        ..Default::default()
    }
}

/// The response row of a topic the request named explicitly, but which the
/// caller may not `Describe`, as `KafkaApis.describeShareGroupOffsetsForGroup`
/// builds it.
///
/// Every partition the request named answers `TOPIC_AUTHORIZATION_FAILED`,
/// with start offset and lag `-1`, the leader epoch Kafka leaves at its
/// default of 0, and the all-zero topic id. An empty `partitions` list gives
/// a row with no partitions.
pub(super) fn unauthorized_topic(
    topic: DescribeShareGroupOffsetsRequestTopic,
) -> DescribeShareGroupOffsetsResponseTopic {
    unidentified_topic(topic, |partition_index| {
        DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: -1,
            leader_epoch: DEFAULT_LEADER_EPOCH,
            lag: -1,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some(TOPIC_AUTHORIZATION_FAILED_MESSAGE.to_owned()),
            ..Default::default()
        }
    })
}

/// A topic of the request the image holds, with the partitions it named.
struct FoundTopic {
    topic_id: uuid::Uuid,
    name: String,
    partitions: Vec<i32>,
}

/// The share-state summary of one partition, as
/// `DefaultStatePersister.readSummary` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionSummary {
    start_offset: i64,
    leader_epoch: i32,
    delivery_complete_count: i32,
    /// The read of the summary failed.
    failed: bool,
}

impl PartitionSummary {
    fn from_read(read: &Result<Option<ShareStateSummary>, BrokerError>) -> Self {
        match *read {
            Ok(Some((_, leader_epoch, start_offset, delivery_complete_count))) => Self {
                start_offset: start_offset.0,
                leader_epoch,
                delivery_complete_count,
                failed: false,
            },
            // `ShareCoordinatorShard.readStateSummary` for a key with no state.
            Ok(None) => Self {
                start_offset: UNINITIALIZED_START_OFFSET,
                leader_epoch: DEFAULT_LEADER_EPOCH,
                delivery_complete_count: UNINITIALIZED_DELIVERY_COMPLETE_COUNT,
                failed: false,
            },
            Err(_) => Self {
                start_offset: UNINITIALIZED_START_OFFSET,
                leader_epoch: DEFAULT_LEADER_EPOCH,
                delivery_complete_count: UNINITIALIZED_DELIVERY_COMPLETE_COUNT,
                failed: true,
            },
        }
    }

    /// Kafka's `shouldComputeSharePartitionLag`: the read succeeded and both
    /// the start offset and the delivery complete count are initialized.
    fn has_lag(self) -> bool {
        !self.failed
            && self.start_offset != UNINITIALIZED_START_OFFSET
            && self.delivery_complete_count != UNINITIALIZED_DELIVERY_COMPLETE_COUNT
    }
}

/// The topic rows of one group, as Kafka's
/// `GroupCoordinatorService.describeShareGroupOffsets` builds them.
///
/// A topic the image holds is read from the share state; its lag is the
/// partition end offset less the start offset and the delivery complete
/// count, and a partition whose end offset lookup fails carries that error.
/// A partition whose state is missing or cannot be read has start offset
/// `-1`, leader epoch 0 and lag `-1`, with no error. A topic the image does
/// not hold answers each partition it named with start offset `-1`, leader
/// epoch 0, lag `-1` and no error, and those rows follow the rows of the
/// topics the image holds.
///
/// A topic named twice is one row, and a partition named twice is one row,
/// as the maps Kafka collects them in make them. Kafka orders the rows of the
/// topics the image holds by the iteration order of those maps; they keep
/// the order the request first names them in here.
///
/// # Errors
///
/// Returns `UNKNOWN_SERVER_ERROR` with Kafka's message when
/// `DefaultStatePersister` refuses the read before it starts: an empty group
/// id, a topic the image holds named with no partitions, or a negative
/// partition.
pub(super) async fn describe_topics(
    summaries: &impl ShareSummaries,
    end_offsets: &impl LatestOffsets,
    image: &krabka_metadata::MetadataImage,
    group: &str,
    topics: Vec<DescribeShareGroupOffsetsRequestTopic>,
) -> Result<Vec<DescribeShareGroupOffsetsResponseTopic>, ErrorAnswer> {
    let mut found: Vec<FoundTopic> = Vec::new();
    let mut missing: Vec<DescribeShareGroupOffsetsResponseTopic> = Vec::new();
    for topic in topics {
        let Some(topic_id) = image.topic(&topic.topic_name).map(|t| t.topic_id) else {
            missing.push(missing_topic(topic));
            continue;
        };
        // `DefaultStatePersister.validate` checks each topic entry of the
        // read, before entries naming one topic are merged.
        if topic.partitions.is_empty() || topic.partitions.iter().any(|p| *p < 0) {
            return Err(ErrorAnswer {
                code: codes::UNKNOWN_SERVER_ERROR,
                message: UNKNOWN_SERVER_ERROR_MESSAGE,
            });
        }
        if let Some(entry) = found.iter_mut().find(|entry| entry.topic_id == topic_id) {
            for partition in topic.partitions {
                if !entry.partitions.contains(&partition) {
                    entry.partitions.push(partition);
                }
            }
        } else {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                if !partitions.contains(&partition) {
                    partitions.push(partition);
                }
            }
            found.push(FoundTopic {
                topic_id,
                name: topic.topic_name,
                partitions,
            });
        }
    }
    if found.is_empty() {
        return Ok(missing);
    }
    if group.is_empty() {
        return Err(ErrorAnswer {
            code: codes::UNKNOWN_SERVER_ERROR,
            message: UNKNOWN_SERVER_ERROR_MESSAGE,
        });
    }

    let reads = found.iter().flat_map(|topic| {
        topic
            .partitions
            .iter()
            .map(|partition| summaries.read_summary(group, topic.topic_id, *partition))
    });
    let mut read = join_all(reads)
        .await
        .into_iter()
        .map(|read| PartitionSummary::from_read(&read));
    let summaries: Vec<Vec<PartitionSummary>> = found
        .iter()
        .map(|topic| read.by_ref().take(topic.partitions.len()).collect())
        .collect();

    let lagging: Vec<TopicPartition> = found
        .iter()
        .zip(&summaries)
        .flat_map(|(topic, summaries)| {
            topic
                .partitions
                .iter()
                .zip(summaries)
                .filter(|(_, summary)| summary.has_lag())
                .map(|(partition, _)| TopicPartition {
                    topic: topic.name.clone(),
                    partition: *partition,
                })
        })
        .collect();
    let offsets = if lagging.is_empty() {
        HashMap::new()
    } else {
        end_offsets.latest_offsets(&lagging).await
    };

    let mut rows: Vec<DescribeShareGroupOffsetsResponseTopic> = found
        .into_iter()
        .zip(summaries)
        .map(|(topic, summaries)| {
            let partitions = topic
                .partitions
                .iter()
                .zip(summaries)
                .map(|(partition, summary)| {
                    let end_offset = summary.has_lag().then(|| {
                        offsets
                            .get(&TopicPartition {
                                topic: topic.name.clone(),
                                partition: *partition,
                            })
                            .copied()
                            .unwrap_or(Err(codes::UNKNOWN_SERVER_ERROR))
                    });
                    partition_row(*partition, summary, end_offset)
                })
                .collect();
            DescribeShareGroupOffsetsResponseTopic {
                topic_name: topic.name,
                topic_id: Uuid(*topic.topic_id.as_bytes()),
                partitions,
                ..Default::default()
            }
        })
        .collect();
    rows.extend(missing);
    Ok(rows)
}

/// The row of a topic the image does not hold: no data for each partition
/// the request named.
fn missing_topic(
    topic: DescribeShareGroupOffsetsRequestTopic,
) -> DescribeShareGroupOffsetsResponseTopic {
    unidentified_topic(topic, |partition_index| {
        DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: UNINITIALIZED_START_OFFSET,
            leader_epoch: DEFAULT_LEADER_EPOCH,
            lag: UNINITIALIZED_LAG,
            ..Default::default()
        }
    })
}

/// One partition row of a topic the image holds. `end_offset` is the lookup
/// of the partition end offset, made only for a partition with a lag.
fn partition_row(
    partition_index: i32,
    summary: PartitionSummary,
    end_offset: Option<EndOffset>,
) -> DescribeShareGroupOffsetsResponsePartition {
    match end_offset {
        None => DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: summary.start_offset,
            leader_epoch: summary.leader_epoch,
            lag: UNINITIALIZED_LAG,
            ..Default::default()
        },
        Some(Ok(end_offset)) => DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: summary.start_offset,
            leader_epoch: summary.leader_epoch,
            lag: end_offset - summary.start_offset - i64::from(summary.delivery_complete_count),
            ..Default::default()
        },
        // Kafka sets only the index and the error on this row, so the start
        // offset and the leader epoch keep the schema default of 0.
        Some(Err(code)) => {
            let ErrorAnswer {
                code: error_code,
                message,
            } = lookup_error(code);
            DescribeShareGroupOffsetsResponsePartition {
                partition_index,
                error_code,
                error_message: Some(message.to_owned()),
                ..Default::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_log::Offset,
        krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord},
        std::{collections::HashMap, future},
    };

    const ORDERS: uuid::Uuid = uuid::Uuid::from_u128(0xD5C0);
    const AUDIT: uuid::Uuid = uuid::Uuid::from_u128(0xA0D1);

    /// A share-state key: a topic id and a partition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct SummaryKey {
        topic_id: uuid::Uuid,
        partition: i32,
    }

    /// Share-state summaries keyed by `(topic id, partition)`: `Ok(None)`
    /// for a key with no entry, and an error for a key mapped to `None`.
    struct Summaries(HashMap<SummaryKey, Option<ShareStateSummary>>);

    impl ShareSummaries for Summaries {
        fn read_summary(
            &self,
            _group: &str,
            topic_id: uuid::Uuid,
            partition: i32,
        ) -> impl Future<Output = Result<Option<ShareStateSummary>, BrokerError>> {
            future::ready(
                match self.0.get(&SummaryKey {
                    topic_id,
                    partition,
                }) {
                    None => Ok(None),
                    Some(Some(summary)) => Ok(Some(*summary)),
                    Some(None) => Err(BrokerError::Share("share coordinator down".into())),
                },
            )
        }
    }

    /// End offsets keyed by partition; a partition with no entry fails with
    /// `NETWORK_EXCEPTION`.
    struct Ends(HashMap<TopicPartition, EndOffset>);

    impl LatestOffsets for Ends {
        fn latest_offsets(
            &self,
            partitions: &[TopicPartition],
        ) -> impl Future<Output = HashMap<TopicPartition, EndOffset>> {
            future::ready(
                partitions
                    .iter()
                    .map(|partition| {
                        let end = self
                            .0
                            .get(partition)
                            .copied()
                            .unwrap_or(Err(codes::NETWORK_EXCEPTION));
                        (partition.clone(), end)
                    })
                    .collect(),
            )
        }
    }

    fn image() -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (name, topic_id) in [("orders", ORDERS), ("audit", AUDIT)] {
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id,
                partitions: 4,
                replication_factor: 1,
            }));
        }
        image
    }

    fn topic(name: &str, partitions: &[i32]) -> DescribeShareGroupOffsetsRequestTopic {
        DescribeShareGroupOffsetsRequestTopic {
            topic_name: name.into(),
            partitions: partitions.to_vec(),
            ..Default::default()
        }
    }

    fn row(
        name: &str,
        topic_id: uuid::Uuid,
        partitions: Vec<DescribeShareGroupOffsetsResponsePartition>,
    ) -> DescribeShareGroupOffsetsResponseTopic {
        DescribeShareGroupOffsetsResponseTopic {
            topic_name: name.into(),
            topic_id: Uuid(*topic_id.as_bytes()),
            partitions,
            ..Default::default()
        }
    }

    #[derive(Default)]
    struct ShareOffsetRowsScenario {
        image: Option<MetadataImage>,
        summaries: HashMap<SummaryKey, Option<ShareStateSummary>>,
        ends: HashMap<TopicPartition, EndOffset>,
        result: Option<Result<Vec<DescribeShareGroupOffsetsResponseTopic>, ErrorAnswer>>,
    }
    #[rstest::fixture]
    fn share_offset_rows() -> ShareOffsetRowsScenario {
        let image = None;
        ShareOffsetRowsScenario {
            image,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given("orders UUID d5c0 and audit UUID a0d1 each have four partitions")]
    fn share_offset_topic_image(share_offset_rows: &mut ShareOffsetRowsScenario) {
        share_offset_rows.image = Some(image());
    }
    #[rstest_bdd_macros::given("share state stores these partition summaries")]
    fn stored_share_offset_summaries(
        share_offset_rows: &mut ShareOffsetRowsScenario,
        datatable: Vec<Vec<String>>,
    ) {
        for cells in datatable.into_iter().skip(1) {
            let topic_id =
                uuid::Uuid::from_u128(u128::from_str_radix(&cells[0], 16).expect("topic UUID"));
            let key = SummaryKey {
                topic_id,
                partition: cells[1].parse().expect("partition index"),
            };
            let summary = if cells[2] == "error" {
                None
            } else {
                Some((
                    cells[2].parse().expect("state epoch"),
                    cells[3].parse().expect("leader epoch"),
                    Offset(cells[4].parse().expect("start offset")),
                    cells[5].parse().expect("delivered count"),
                ))
            };
            share_offset_rows.summaries.insert(key, summary);
        }
    }
    #[rstest_bdd_macros::given("the end offset lookup for orders partition 0 returns {end}")]
    fn share_offset_end(share_offset_rows: &mut ShareOffsetRowsScenario, end: i64) {
        share_offset_rows.ends.insert(
            TopicPartition {
                topic: "orders".into(),
                partition: 0,
            },
            Ok(end),
        );
    }
    #[rstest_bdd_macros::when(
        "share offset description for group \"{group}\" requests these topic partitions"
    )]
    async fn describe_share_offset_rows(
        share_offset_rows: &mut ShareOffsetRowsScenario,
        group: String,
        datatable: Vec<Vec<String>>,
    ) {
        let group = if group == "<empty>" {
            String::new()
        } else {
            group
        };
        let topics = datatable
            .into_iter()
            .skip(1)
            .map(|cells| {
                topic(
                    &cells[0],
                    &cells[1]
                        .split(',')
                        .filter(|value| *value != "none")
                        .map(|value| value.parse().expect("partition index"))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        share_offset_rows.result = Some(
            describe_topics(
                &Summaries(share_offset_rows.summaries.clone()),
                &Ends(share_offset_rows.ends.clone()),
                share_offset_rows.image.as_ref().expect("topic metadata"),
                &group,
                topics,
            )
            .await,
        );
    }
    #[rstest_bdd_macros::then("the complete ordered share offset topic rows are")]
    fn described_share_offset_rows(
        share_offset_rows: &ShareOffsetRowsScenario,
        datatable: Vec<Vec<String>>,
    ) {
        let mut topics: Vec<DescribeShareGroupOffsetsResponseTopic> = Vec::new();
        for cells in datatable.into_iter().skip(1) {
            let id =
                uuid::Uuid::from_u128(u128::from_str_radix(&cells[1], 16).expect("topic UUID"));
            if topics
                .last()
                .is_none_or(|topic| topic.topic_name != cells[0])
            {
                topics.push(row(&cells[0], id, Vec::new()));
            }
            if cells[2] != "none" {
                topics.last_mut().expect("topic row").partitions.push(
                    DescribeShareGroupOffsetsResponsePartition {
                        partition_index: cells[2].parse().expect("partition index"),
                        start_offset: cells[3].parse().expect("start offset"),
                        leader_epoch: cells[4].parse().expect("leader epoch"),
                        lag: cells[5].parse().expect("lag"),
                        error_code: cells[6].parse().expect("partition error"),
                        error_message: (cells[7] != "null").then(|| cells[7].clone()),
                        ..Default::default()
                    },
                );
            }
        }
        assert!(
            share_offset_rows
                .result
                .as_ref()
                .expect("description result")
                == &Ok(topics)
        );
    }
    #[rstest_bdd_macros::then(
        "share offset description fails the whole group with UNKNOWN_SERVER_ERROR and Kafka's default message"
    )]
    fn refused_share_offset_rows(share_offset_rows: &ShareOffsetRowsScenario) {
        assert!(
            share_offset_rows
                .result
                .as_ref()
                .expect("description result")
                == &Err(ErrorAnswer {
                    code: codes::UNKNOWN_SERVER_ERROR,
                    message: UNKNOWN_SERVER_ERROR_MESSAGE
                })
        );
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "Lag subtracts start offset and completed deliveries"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_0(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "Missing partition state has no data or lag"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_1(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "Uninitialized delivery count suppresses lag"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_2(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "State read failure has no data and no partition error"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_3(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "Failed end lookup carries its own error"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_4(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "Unknown topics follow known topics"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_5(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "Unknown topics with empty partitions still have a topic row"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_6(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "Unknown topics need no group identity"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_7(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "Repeated topics merge and repeated partitions collapse in first-seen order"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_8(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "An empty partition list for a known topic fails the group"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_9(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "A negative known partition fails the group"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_10(share_offset_rows: ShareOffsetRowsScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_offset_rows.feature",
        name = "An empty group identity fails a known topic"
    )]
    #[tokio::test]
    async fn share_offset_rows_case_11(share_offset_rows: ShareOffsetRowsScenario) {}

    #[test]
    fn an_unauthorized_topic_answers_each_partition_it_named() {
        let denied = |partition_index| DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: -1,
            leader_epoch: 0,
            lag: -1,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some("Topic authorization failed.".into()),
            ..Default::default()
        };
        let actual = [
            unauthorized_topic(topic("secret", &[1, 0])),
            unauthorized_topic(topic("secret", &[])),
        ];
        let expected = [
            row("secret", uuid::Uuid::nil(), vec![denied(1), denied(0)]),
            row("secret", uuid::Uuid::nil(), vec![]),
        ];
        assert!(actual == expected);
    }
}
