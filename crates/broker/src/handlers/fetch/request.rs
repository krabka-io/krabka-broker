//! Projection of a `Fetch` request, or of the partitions its session
//! already caches, into the per-topic shape the read path walks, together
//! with the session classification and the authorization that run before
//! any partition is read.

use {
    crate::{
        broker::Broker,
        fetch_session::{CachedPartitionState, FetchSessionKey, SessionDecision},
        handlers::{RequestContext, cluster_action_denied, denied_topics, requested_topic_name},
    },
    krabka_metadata::AclOperation,
    krabka_protocol::{owned::fetch_request::FetchRequest, primitives::uuid::Uuid as WireUuid},
    std::sync::Arc,
};

/// Projection of `FetchRequest::topics` or of the cached session partitions.
///
/// It holds the minimum that the read loop needs. The handler builds it once
/// at the top, from either source.
pub(super) struct EffectiveTopic {
    pub(super) topic: String,
    pub(super) topic_id: WireUuid,
    pub(super) partitions: Vec<EffectivePartition>,
}

pub(super) struct EffectivePartition {
    pub(super) partition: i32,
    pub(super) current_leader_epoch: i32,
    /// KIP-320: the leader epoch of the last fetched record, as the fetcher
    /// reports it. `-1` means "not set". This happens with v0–v11 fetchers and
    /// with session-cached partitions that never set the field.
    pub(super) last_fetched_epoch: i32,
    pub(super) fetch_offset: i64,
    /// The log start offset the fetcher reports, from Fetch v5. A follower
    /// sends its own local log start, which the leader's `DeleteRecords`
    /// waits on. `-1` means "not set".
    pub(super) log_start_offset: i64,
    pub(super) partition_max_bytes: i32,
}

/// The authorization decision for the partition rows of one fetch.
///
/// Kafka's `KafkaApis.handleFetchRequest` authorizes a follower fetch and a
/// consumer fetch in different ways. A follower fetch needs `ClusterAction` on
/// the cluster resource, and then no topic ACL. A consumer fetch needs `Read`
/// on each topic.
pub(super) enum FetchAuthorization {
    /// A follower fetch whose principal holds `ClusterAction`.
    Follower,
    /// A follower fetch whose principal does not hold `ClusterAction`. Kafka
    /// answers `TOPIC_AUTHORIZATION_FAILED` on every partition row, also on a
    /// row whose topic does not resolve, and reads nothing.
    FollowerDenied,
    /// A consumer fetch, with the names of the topics whose `Read` the
    /// authorizer denied.
    Consumer {
        denied_topics: std::collections::HashSet<String>,
    },
}

impl FetchAuthorization {
    /// Reports whether every partition row is refused before its topic
    /// resolves.
    pub(super) const fn refuses_every_row(&self) -> bool { matches!(self, Self::FollowerDenied) }

    /// Reports whether the rows of the resolved topic `topic_name` are refused.
    pub(super) fn refuses_topic(&self, topic_name: &str) -> bool {
        match self {
            Self::Follower => false,
            Self::FollowerDenied => true,
            Self::Consumer { denied_topics } => denied_topics.contains(topic_name),
        }
    }
}

pub(super) struct FetchPreparation {
    pub(super) decision: SessionDecision,
    pub(super) effective_topics: Vec<EffectiveTopic>,
    pub(super) image: Arc<krabka_metadata::MetadataImage>,
    pub(super) authorization: FetchAuthorization,
    pub(super) effective_replica_id: i32,
    pub(super) is_follower_fetch: bool,
    pub(super) read_committed: bool,
}

/// A fetch that its fetch session refuses before any read: Kafka's
/// `SessionErrorContext`.
pub(super) struct SessionRefusal {
    pub(super) code: i16,
    pub(super) is_follower_fetch: bool,
}

pub(super) fn prepare_fetch(
    broker: &Broker,
    request: &FetchRequest,
    version: i16,
    context: &RequestContext<'_>,
) -> Result<FetchPreparation, SessionRefusal> {
    let effective_replica_id = if request.replica_id >= 0 {
        request.replica_id
    } else {
        request.replica_state.replica_id
    };
    let is_follower_fetch = effective_replica_id >= 0;
    let decision = broker.fetch_session_cache.classify(request, version);
    if let SessionDecision::Error { code } = decision {
        return Err(SessionRefusal {
            code,
            is_follower_fetch,
        });
    }
    let effective_topics = match &decision {
        SessionDecision::Incremental { partitions, .. } => {
            group_cached_into_effective_topics(partitions)
        }
        _ => request
            .topics
            .iter()
            .map(|topic| EffectiveTopic {
                topic: topic.topic.clone(),
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| EffectivePartition {
                        partition: partition.partition,
                        current_leader_epoch: partition.current_leader_epoch,
                        last_fetched_epoch: partition.last_fetched_epoch,
                        fetch_offset: partition.fetch_offset,
                        log_start_offset: partition.log_start_offset,
                        partition_max_bytes: partition.partition_max_bytes,
                    })
                    .collect(),
            })
            .collect(),
    };
    let image = broker.controller.current_image();
    let authorization = if is_follower_fetch {
        if cluster_action_denied(broker.config.authorizer.as_ref(), &image, context) {
            FetchAuthorization::FollowerDenied
        } else {
            FetchAuthorization::Follower
        }
    } else {
        FetchAuthorization::Consumer {
            denied_topics: consumer_denied_topics(broker, &image, &effective_topics, context),
        }
    };
    Ok(FetchPreparation {
        decision,
        effective_topics,
        image,
        authorization,
        effective_replica_id,
        is_follower_fetch,
        read_committed: !is_follower_fetch && request.isolation_level == 1,
    })
}

/// The names of the topics whose `Read` the authorizer denies to a consumer.
///
/// A topic id that does not resolve gives the empty name.
fn consumer_denied_topics(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    effective_topics: &[EffectiveTopic],
    context: &RequestContext<'_>,
) -> std::collections::HashSet<String> {
    let names: Vec<String> = effective_topics
        .iter()
        .map(|topic| requested_topic_name(image, &topic.topic, topic.topic_id))
        .collect();
    denied_topics(
        broker.config.authorizer.as_ref(),
        image,
        context,
        AclOperation::Read,
        names.iter().map(String::as_str),
    )
}

/// Re-group the flat `(key, state)` list that `FetchSessionCache::classify`
/// returns into per-topic chunks, keeping the session's partition order.
///
/// Only consecutive partitions of one topic share a chunk, as Kafka's
/// `FetchResponse.toMessage` groups them, so a topic whose partitions are not
/// adjacent in the session appears once per run. The read loop spends the
/// response budget in this order, and the session moves a partition that sent
/// records to the end. Folding every partition of a topic into its first run
/// would undo that move: in `[A1, B0, A0]`, `A0` would be read ahead of `B0`
/// again, and a busy `A0` would starve `B0` for good.
///
/// The grouping key is the topic's whole identity, not its name. KIP-516 moved
/// a Fetch's topic identity to `topic_id` at v13, and a v13 request carries no
/// names at all, so a cache holding several id-only topics holds several keys
/// whose `topic_name` is the empty string. Grouping by name alone would fold
/// them into one entry, and the read loop would then serve every one of those
/// partitions out of that one topic.
fn group_cached_into_effective_topics(
    cached: &[(FetchSessionKey, CachedPartitionState)],
) -> Vec<EffectiveTopic> {
    let mut topics: Vec<EffectiveTopic> = Vec::new();
    for (k, s) in cached {
        let partition = EffectivePartition {
            partition: k.partition,
            current_leader_epoch: s.current_leader_epoch,
            last_fetched_epoch: s.last_fetched_epoch,
            fetch_offset: s.fetch_offset,
            log_start_offset: s.log_start_offset,
            partition_max_bytes: s.max_bytes,
        };
        match topics.last_mut() {
            Some(topic) if topic.topic_id == k.topic_id && topic.topic == k.topic_name => {
                topic.partitions.push(partition);
            }
            _ => topics.push(EffectiveTopic {
                topic: k.topic_name.clone(),
                topic_id: k.topic_id,
                partitions: vec![partition],
            }),
        }
    }
    topics
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    fn key(id: u8, name: &str, partition: i32) -> FetchSessionKey {
        FetchSessionKey {
            topic_id: WireUuid([id; 16]),
            topic_name: name.to_owned(),
            partition,
        }
    }

    fn state(fetch_offset: i64) -> CachedPartitionState {
        CachedPartitionState {
            fetch_offset,
            ..CachedPartitionState::default()
        }
    }

    /// A v13 session caches several topics with no name at all. Each must come
    /// back as its own topic, under its own id.
    #[test]
    fn nameless_cached_topics_do_not_collapse_into_one() {
        /// A partition and the offset the session cached for it.
        #[derive(Debug, PartialEq, Eq)]
        struct PartitionOffset {
            partition: i32,
            fetch_offset: i64,
        }

        /// A topic's id and its partitions.
        #[derive(Debug, PartialEq, Eq)]
        struct IdShape {
            topic_id: WireUuid,
            partitions: Vec<PartitionOffset>,
        }

        let cached = vec![
            (key(1, "", 0), state(10)),
            (key(2, "", 0), state(20)),
            (key(3, "", 0), state(30)),
        ];

        let grouped = group_cached_into_effective_topics(&cached);

        let shape: Vec<IdShape> = grouped
            .iter()
            .map(|topic| IdShape {
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| PartitionOffset {
                        partition: partition.partition,
                        fetch_offset: partition.fetch_offset,
                    })
                    .collect(),
            })
            .collect();
        assert!(
            shape
                == vec![
                    IdShape {
                        topic_id: WireUuid([1; 16]),
                        partitions: vec![PartitionOffset {
                            partition: 0,
                            fetch_offset: 10
                        }]
                    },
                    IdShape {
                        topic_id: WireUuid([2; 16]),
                        partitions: vec![PartitionOffset {
                            partition: 0,
                            fetch_offset: 20
                        }]
                    },
                    IdShape {
                        topic_id: WireUuid([3; 16]),
                        partitions: vec![PartitionOffset {
                            partition: 0,
                            fetch_offset: 30
                        }]
                    },
                ]
        );
    }

    /// A topic's name and the partition indexes one chunk groups.
    #[derive(Debug, PartialEq, Eq)]
    struct NameShape {
        topic: &'static str,
        partitions: Vec<i32>,
    }

    /// Consecutive partitions of one topic share a chunk, and a topic whose
    /// partitions the session does not keep together appears once per run,
    /// so the read order is the session's order.
    #[yare::parameterized(
        adjacent = {
            &[("orders", 2), ("orders", 0), ("", 0)],
            vec![
                NameShape { topic: "orders", partitions: vec![2, 0] },
                NameShape { topic: "", partitions: vec![0] },
            ],
        },
        split_by_another_topic = {
            &[("orders", 1), ("", 0), ("orders", 0)],
            vec![
                NameShape { topic: "orders", partitions: vec![1] },
                NameShape { topic: "", partitions: vec![0] },
                NameShape { topic: "orders", partitions: vec![0] },
            ],
        },
    )]
    fn cached_partitions_keep_the_session_order(
        rows: &[(&'static str, i32)],
        expected: Vec<NameShape>,
    ) {
        let cached: Vec<_> = rows
            .iter()
            .map(|&(name, partition)| {
                let id = if name.is_empty() { 2 } else { 1 };
                (key(id, name, partition), state(0))
            })
            .collect();

        let grouped = group_cached_into_effective_topics(&cached);

        let shape: Vec<NameShape> = grouped
            .iter()
            .map(|topic| NameShape {
                topic: rows
                    .iter()
                    .map(|&(name, _)| name)
                    .find(|name| *name == topic.topic)
                    .expect("a requested topic"),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| partition.partition)
                    .collect(),
            })
            .collect();
        assert2::assert!(shape == expected);
    }
}
