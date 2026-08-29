//! Fixtures shared by this module's unit tests: metadata records to seed an
//! image, a real on-disk partition, a way to force a replica state, and a
//! `MetadataSource` stub that answers only the image and leader reads the ISR
//! code makes.

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use krabka_ids::{LeaderEpoch, PartitionIndex};
use krabka_log::Offset;
use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord};
use krabka_raft::NodeId;
use tokio::sync::watch;

use crate::partition::Partition;

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

pub(super) struct TestMetadataSource {
    image_tx: watch::Sender<Arc<MetadataImage>>,
    leader_tx: watch::Sender<Option<NodeId>>,
}

impl TestMetadataSource {
    pub(super) fn new(image: MetadataImage, leader: Option<NodeId>) -> Self {
        let (image_tx, _) = watch::channel(Arc::new(image));
        let (leader_tx, _) = watch::channel(leader);
        Self {
            image_tx,
            leader_tx,
        }
    }
}

#[async_trait::async_trait]
impl crate::metadata_source::MetadataSource for TestMetadataSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        self.image_tx.borrow().clone()
    }

    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image_tx.subscribe()
    }

    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader_tx.subscribe()
    }

    fn quorum_state(&self) -> krabka_raft::QuorumState {
        unimplemented!("unused in isr_maintenance tests")
    }

    async fn submit_change(
        &self,
        _records: Vec<MetadataRecord>,
    ) -> Result<krabka_raft::SubmitChangeResult, krabka_raft::RaftError> {
        unimplemented!("unused in isr_maintenance tests")
    }

    async fn change_membership(
        &self,
        _new_voters: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), krabka_raft::RaftError> {
        unimplemented!("unused in isr_maintenance tests")
    }

    async fn add_learner(
        &self,
        _node_id: NodeId,
        _node: krabka_raft::Node,
    ) -> Result<(), krabka_raft::RaftError> {
        unimplemented!("unused in isr_maintenance tests")
    }

    fn controller_bound_addr(&self) -> std::net::SocketAddr {
        unimplemented!("unused in isr_maintenance tests")
    }

    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> krabka_raft::SnapshotRange {
        unimplemented!("unused in isr_maintenance tests")
    }

    async fn trigger_snapshot(&self) -> Result<(), krabka_raft::RaftError> {
        unimplemented!("unused in isr_maintenance tests")
    }

    async fn add_voter(
        &self,
        _req: krabka_raft::AddVoter,
    ) -> Result<krabka_raft::ReconfigOutcome, krabka_raft::RaftError> {
        unimplemented!("unused in isr_maintenance tests")
    }

    async fn remove_voter(
        &self,
        _req: krabka_raft::RemoveVoter,
    ) -> Result<krabka_raft::ReconfigOutcome, krabka_raft::RaftError> {
        unimplemented!("unused in isr_maintenance tests")
    }

    async fn update_voter(
        &self,
        _req: krabka_raft::UpdateVoter,
    ) -> Result<krabka_raft::ReconfigOutcome, krabka_raft::RaftError> {
        unimplemented!("unused in isr_maintenance tests")
    }

    async fn cancel(&self) {}
}
