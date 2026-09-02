//! The fixtures the `BrokerHeartbeat` controller-side tests share: a
//! `MetadataSource` that captures submitted records, a one-partition metadata
//! image, and a populated liveness state.
//!
//! The offline-log-dir failover tests and the controlled-shutdown drain tests
//! drive the same shapes, so the fixtures live in one module rather than once
//! per test file.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use uuid::Uuid;

use crate::{
    heartbeat::controller_state::ControllerLivenessState, test_support::FakeMetadataSource,
};

/// A metadata source over `image`, with this node as the controller leader so
/// that the failover and drain paths act rather than defer. What they submit
/// is captured for the assertions.
pub(super) fn fake_source(image: MetadataImage) -> Arc<FakeMetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(Some(NodeId(1)))
            .build(),
    )
}

pub(super) fn image_with_dir_partition(
    leader: NodeId,
    replicas: &[NodeId],
    isr: &[NodeId],
    dirs: &[Uuid],
) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    img.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: i16::try_from(replicas.len()).unwrap(),
    }));
    img.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: "t".into(),
        partition: 0,
        leader,
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        leader_epoch: krabka_metadata::LeaderEpoch(5),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: dirs.to_vec(),
        partition_epoch: 0,
    }));
    img
}

pub(super) async fn liveness_with(alive: &[NodeId]) -> Arc<ControllerLivenessState> {
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for &n in alive {
        l.record_heartbeat(n.0).await;
    }
    Arc::new(l)
}
