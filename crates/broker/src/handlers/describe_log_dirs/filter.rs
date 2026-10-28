//! The topic and partition filter that a `DescribeLogDirs` request carries.
//!
//! The request's `topics` field is optional. A named topic contributes only
//! the partition indexes it lists, so an empty list contributes none, as
//! Kafka's `KafkaApis.handleDescribeLogDirsRequest` builds its partition set.
//! This module turns that wire shape into a single predicate, so the directory
//! scan in the parent module stays about directories.

use {
    krabka_protocol::owned::describe_log_dirs_request::DescribeLogDirsRequest,
    std::collections::BTreeSet,
};

/// A partition named by topic and index.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TopicPartition {
    topic: String,
    partition: i32,
}

/// Filter that the handler derives from the request `topics` field.
///
/// - `None`  → report every partition. This is the admin-client default
///   (`DescribeLogDirsRequest.isAllTopicPartitions`).
/// - `Some`  → report only the listed `(topic, partition)` pairs, the union
///   over every entry of the request.
pub(super) enum Filter {
    All,
    Partitions(BTreeSet<TopicPartition>),
}

impl Filter {
    pub(super) fn allows(&self, topic: &str, partition: i32) -> bool {
        match self {
            Self::All => true,
            Self::Partitions(set) => set.contains(&TopicPartition {
                topic: topic.to_owned(),
                partition,
            }),
        }
    }
}

pub(super) fn request_filter(req: DescribeLogDirsRequest) -> Filter {
    req.topics.map_or(Filter::All, |topics| {
        Filter::Partitions(
            topics
                .into_iter()
                .flat_map(|topic| {
                    let name = topic.topic;
                    topic
                        .partitions
                        .into_iter()
                        .map(move |partition| TopicPartition {
                            topic: name.clone(),
                            partition,
                        })
                })
                .collect(),
        )
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*, assert2::check,
        krabka_protocol::owned::describe_log_dirs_request::DescribableLogDirTopic,
    };

    struct LogDirectoryFilterScenario {
        request: Option<DescribeLogDirsRequest>,
        filter: Option<Filter>,
        allowed: Option<bool>,
    }
    #[rstest::fixture]
    fn directory_filter() -> LogDirectoryFilterScenario {
        let request = None;
        LogDirectoryFilterScenario {
            request,
            filter: None,
            allowed: None,
        }
    }
    #[rstest_bdd_macros::given("the log directory request lists {requested}")]
    fn requested_directory_partitions(
        directory_filter: &mut LogDirectoryFilterScenario,
        requested: String,
    ) {
        let topics = if requested == "all topics" {
            None
        } else {
            Some(
                requested
                    .split(';')
                    .map(|entry| {
                        let (topic, partitions) = entry.split_once(':').unwrap();
                        DescribableLogDirTopic {
                            topic: topic.into(),
                            partitions: if partitions == "none" {
                                Vec::new()
                            } else {
                                partitions.split(',').map(|p| p.parse().unwrap()).collect()
                            },
                            ..Default::default()
                        }
                    })
                    .collect(),
            )
        };
        directory_filter.request = Some(DescribeLogDirsRequest {
            topics,
            ..Default::default()
        });
    }
    #[rstest_bdd_macros::when(
        "the log directory filter checks topic {topic} partition {partition}"
    )]
    fn check_requested_directory_partition(
        directory_filter: &mut LogDirectoryFilterScenario,
        topic: String,
        partition: i32,
    ) {
        let filter = request_filter(directory_filter.request.take().unwrap());
        directory_filter.allowed = Some(filter.allows(&topic, partition));
        directory_filter.filter = Some(filter);
    }
    #[rstest_bdd_macros::then("that log partition is included {expected}")]
    fn assert_directory_filter(directory_filter: &LogDirectoryFilterScenario, expected: bool) {
        check!(directory_filter.allowed.unwrap() == expected);
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_log_directory_filter",fixtures=[directory_filter:LogDirectoryFilterScenario]);
}
