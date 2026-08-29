//! The fixtures the `BrokerHeartbeat` controller-side tests share: a
//! `MetadataSource` double that captures submitted records, a one-partition
//! metadata image, and a populated liveness state.
//!
//! The offline-log-dir failover tests and the controlled-shutdown drain tests
//! drive the same shapes, so the fixtures live in one module rather than once
//! per test file.

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
use krabka_raft::{
    AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    UpdateVoter,
};
use tokio::sync::watch;
use uuid::Uuid;

use crate::heartbeat::controller_state::ControllerLivenessState;

/// Minimal `MetadataSource` that captures `submit_change` calls for
/// inspection. It returns a fixed image, and `watch_leader` always reports
/// `Some(1)`, so this node is the leader.
pub(super) struct MockSource {
    leader_rx: watch::Receiver<Option<NodeId>>,
    _leader_tx: watch::Sender<Option<NodeId>>,
    image: Arc<MetadataImage>,
    captured: Arc<Mutex<Vec<MetadataRecord>>>,
}

impl MockSource {
    pub(super) fn new(image: MetadataImage) -> (Self, Arc<Mutex<Vec<MetadataRecord>>>) {
        let (tx, rx) = watch::channel(Some(NodeId(1)));
        let captured = Arc::new(Mutex::new(Vec::new()));
        let source = Self {
            leader_rx: rx,
            _leader_tx: tx,
            image: Arc::new(image),
            captured: captured.clone(),
        };
        (source, captured)
    }
}

#[async_trait::async_trait]
impl crate::metadata_source::MetadataSource for MockSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        self.image.clone()
    }
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        unimplemented!()
    }
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader_rx.clone()
    }
    fn quorum_state(&self) -> QuorumState {
        unimplemented!()
    }
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<krabka_raft::SubmitChangeResult, RaftError> {
        self.captured.lock().unwrap().extend(records);
        Ok(krabka_raft::SubmitChangeResult::default())
    }
    async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        unimplemented!()
    }
    async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
        unimplemented!()
    }
    fn controller_bound_addr(&self) -> SocketAddr {
        unimplemented!()
    }
    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
        unimplemented!()
    }
    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        unimplemented!()
    }
    async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        unimplemented!()
    }
    async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        unimplemented!()
    }
    async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        unimplemented!()
    }
    async fn cancel(&self) {
        unimplemented!()
    }
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
