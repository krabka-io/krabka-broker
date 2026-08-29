//! Choosing which topics a `DescribeShareGroupOffsets` group row covers.
//!
//! KIP-932 lets the request omit the topic list entirely, which means "every
//! topic this group has share state for". Answering that needs the group's
//! share-state partition metadata and the metadata image to turn each stored
//! `topic_id` back into a name, so the choice is made here rather than inside
//! the row builders.

use krabka_protocol::owned::describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestTopic;

use crate::coordinator::unified::share::persistence::ShareGroupStatePartitionMetadataValue;

pub(super) fn requested_topics(
    requested: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
    metadata: Option<&ShareGroupStatePartitionMetadataValue>,
    image: &krabka_metadata::MetadataImage,
) -> Vec<DescribeShareGroupOffsetsRequestTopic> {
    let Some(metadata) = metadata else {
        return requested.unwrap_or_default();
    };
    if let Some(topics) = requested {
        return topics;
    }
    let mut topics: Vec<_> = metadata
        .initialized
        .iter()
        .filter_map(|(topic_id, partitions)| {
            image.topic_name_by_id(topic_id).map(|topic_name| {
                DescribeShareGroupOffsetsRequestTopic {
                    topic_name: topic_name.into(),
                    partitions: partitions.clone(),
                    ..Default::default()
                }
            })
        })
        .collect();
    topics.sort_by(|a, b| a.topic_name.cmp(&b.topic_name));
    topics
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};

    use super::*;
    use crate::handlers::describe_share_group_offsets::test_support::image_with_topic;

    #[test]
    fn null_topics_resolves_all_initialized_topic_partitions() {
        let alpha_id = uuid::Uuid::from_u128(1);
        let beta_id = uuid::Uuid::from_u128(2);
        let missing_id = uuid::Uuid::from_u128(3);
        let mut image = image_with_topic("beta", beta_id);
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "alpha".into(),
            topic_id: alpha_id,
            partitions: 2,
            replication_factor: 1,
        }));
        let metadata = crate::coordinator::unified::share::persistence::ShareGroupStatePartitionMetadataValue {
            initialized: vec![(beta_id, vec![0]), (missing_id, vec![7]), (alpha_id, vec![0, 1])],
            deleting: Vec::new(),
        };

        let topics = requested_topics(None, Some(&metadata), &image);

        assert!(
            topics
                .iter()
                .map(|topic| (topic.topic_name.as_str(), topic.partitions.as_slice()))
                .collect::<Vec<_>>()
                == vec![("alpha", &[0, 1][..]), ("beta", &[0][..])]
        );
    }
}
