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
