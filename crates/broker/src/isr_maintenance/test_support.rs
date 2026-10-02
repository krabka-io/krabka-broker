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
use krabka_metadata::{
    BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
};
use krabka_raft::NodeId;

use crate::{partition::Partition, test_support::FakeMetadataSource};

pub(super) fn reg(id: NodeId) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        fenced: false,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
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

/// Install `isr` and `replicas` with `leader` at `leader_epoch` on `part`.
/// Each `(follower, age)` in `stale_followers` has not fetched from this
/// leader and last caught up `age` ago.
pub(super) async fn set_replica_state(
    part: &Partition,
    isr: &[NodeId],
    replicas: &[NodeId],
    leader: NodeId,
    leader_epoch: i32,
    stale_followers: &[(NodeId, Duration)],
) {
    let now = Instant::now();
    let mut st = part.replica_state.lock().await;
    st.install_isr(isr, replicas, leader, now);
    st.current_leader_epoch = LeaderEpoch(leader_epoch);
    for &(follower, last_caught_up_age) in stale_followers {
        st.per_follower.insert(
            follower,
            crate::replica_state::FollowerStats {
                leo: Offset(0),
                last_fetch: None,
                last_fetch_leader_leo: Offset(-1),
                last_caught_up: Some(
                    now.checked_sub(last_caught_up_age)
                        .expect("test caught-up age is representable"),
                ),
                broker_epoch: None,
                fetched_since_isr_exit: false,
            },
        );
    }
}

/// Partition 0 of `topic` as the metadata image holds it.
pub(super) fn partition(
    topic: &str,
    isr: &[NodeId],
    replicas: &[NodeId],
    leader: NodeId,
    leader_epoch: i32,
    partition_epoch: i32,
) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_string(),
        partition: 0,
        leader,
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch,
    })
}

/// A metadata source over `image`, with `leader` as the controller leader.
pub(super) fn fake_source(image: MetadataImage, leader: Option<NodeId>) -> FakeMetadataSource {
    FakeMetadataSource::builder()
        .image(image)
        .leader(leader)
        .build()
}
