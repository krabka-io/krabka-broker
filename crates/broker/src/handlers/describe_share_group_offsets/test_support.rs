//! Fixtures shared by the `DescribeShareGroupOffsets` test modules.
//!
//! Starting a broker with a chosen authorizer and share-group setting, and
//! building a metadata image that knows one topic, are each needed by more
//! than one of the test modules under this handler, so they live in one file
//! instead of once per module.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};

use crate::authorizer::Authorizer;

pub(super) async fn start_broker(
    authorizer: Arc<dyn Authorizer>,
    share_enabled: bool,
) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| {
        cfg.authorizer = authorizer;
        cfg.share_group.enable = share_enabled;
    })
    .await
}

pub(super) fn image_with_topic(name: &str, topic_id: uuid::Uuid) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id,
        partitions: 1,
        replication_factor: 1,
    }));
    image
}

/// Registers topic `name` with one partition, led by this broker, in the
/// broker's own metadata.
///
/// The share coordinator refuses to initialize state for a topic-partition its
/// metadata does not hold, as Kafka's `ShareCoordinatorService` does, so a test
/// registers each topic it seeds share state for.
pub(super) async fn register_topic(
    broker: &crate::broker::Broker,
    name: &str,
    topic_id: uuid::Uuid,
) {
    let leader = krabka_metadata::NodeId(broker.config.node_id.0);
    broker
        .controller
        .submit_change(vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                topic: name.into(),
                partition: 0,
                leader,
                replicas: vec![leader],
                isr: vec![leader],
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ])
        .await
        .expect("register topic");
}
