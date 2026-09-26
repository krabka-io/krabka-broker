//! The expansion of an `ElectLeaders` request into the exact list of partitions
//! it elects.
//!
//! KIP-460 lets a client name every partition of the cluster by omitting the
//! topic list, or name an exact set per topic. A topic sent with an empty
//! partition list names no partition: Kafka's
//! `ReplicationControlManager.electLeaders` loops over `topic.partitions()`
//! only, so it elects nothing for that topic and answers a topic row with no
//! partition results. Both shapes become one list of topics and partition
//! indices here, in the order the response carries them.

use krabka_protocol::owned::elect_leaders_request::ElectLeadersRequest;

pub(super) fn resolve_targets(
    image: &krabka_metadata::MetadataImage,
    request: &ElectLeadersRequest,
) -> Vec<(String, Vec<i32>)> {
    request.topic_partitions.as_ref().map_or_else(
        || {
            image
                .topics()
                .map(|topic| {
                    let partitions = image
                        .partitions_of(&topic.name)
                        .map(|partition| partition.partition)
                        .collect();
                    (topic.name.clone(), partitions)
                })
                .collect()
        },
        |topics| {
            topics
                .iter()
                .map(|topic| (topic.topic.clone(), topic.partitions.clone()))
                .collect()
        },
    )
}
