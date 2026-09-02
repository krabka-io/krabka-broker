//! Fixtures shared by this module's unit tests: metadata records to seed an
//! image, a real on-disk partition, a way to force a replica state, and the
//! `MetadataSource` the ISR code reads its image and leader from.

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use krabka_ids::{LeaderEpoch, PartitionIndex};
use krabka_log::Offset;
use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord};
use krabka_raft::NodeId;

use crate::{partition::Partition, test_support::FakeMetadataSource};

pub(super) fn reg(id: NodeId) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        node_id: id,
        broker_epoch: i64::try_from(id.0).unwrap(),
        incarnation_id: uuid::Uuid::nil(),
        host: format!("b{id}"),
        port: 9092,
        rack: None,
        log_dirs: vec![],
        endpoints: vec![],
        features: std::collections::BTreeMap::new(),
    })
}

pub(super) fn topic(name: &str, topic_id: uuid::Uuid) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.to_string(),
        topic_id,
        partitions: 1,
        replication_factor: 3,
    })
}

pub(super) fn fixture_partition(log_dir: &Path, topic: &str, partition: i32) -> Arc<Partition> {
    let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
    std::fs::create_dir_all(&part_dir).unwrap();
    let log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).unwrap();
    crate::broker::spawn_partition(
        topic.to_string(),
        PartitionIndex(partition),
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    )
}

pub(super) async fn set_replica_state(
    part: &Partition,
    isr: &[NodeId],
    replicas: &[NodeId],
    leader: NodeId,
    leader_epoch: i32,
    follower_ages: &[(NodeId, Duration, Duration)],
) {
    let now = Instant::now();
    let mut st = part.replica_state.lock().await;
    st.install_isr(isr, replicas, leader, now);
    st.current_leader_epoch = LeaderEpoch(leader_epoch);
    for &(follower, last_fetch_age, last_caught_up_age) in follower_ages {
        st.per_follower.insert(
            follower,
            crate::replica_state::FollowerStats {
                leo: Offset(0),
                last_fetch: now
                    .checked_sub(last_fetch_age)
                    .expect("test fetch age is representable"),
                last_caught_up: now
                    .checked_sub(last_caught_up_age)
                    .expect("test caught-up age is representable"),
            },
        );
    }
}

/// A metadata source over `image`, with `leader` as the controller leader.
pub(super) fn fake_source(image: MetadataImage, leader: Option<NodeId>) -> FakeMetadataSource {
    FakeMetadataSource::builder()
        .image(image)
        .leader(leader)
        .build()
}
