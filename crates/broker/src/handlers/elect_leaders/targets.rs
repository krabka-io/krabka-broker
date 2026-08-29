//! The expansion of an `ElectLeaders` request into the exact list of partitions
//! it elects.
//!
//! KIP-460 lets a client name every partition of the cluster by omitting the
//! topic list, name a whole topic by sending it with no partitions, or name an
//! exact set. All three shapes become one list of topics and partition indices
//! here, resolved against the metadata image.

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
                .map(|topic| {
                    let partitions = if topic.partitions.is_empty() {
                        image
                            .partitions_of(&topic.topic)
                            .map(|partition| partition.partition)
                            .collect()
                    } else {
                        topic.partitions.clone()
                    };
                    (topic.topic.clone(), partitions)
                })
                .collect()
        },
    )
}
