//! Behaviour tests for the manager's control flow: when recovery is not
//! needed, when no replica is eligible, and how a duplicate job for the same
//! partition is refused.

use std::{collections::BTreeSet, net::SocketAddr};

use assert2::assert;
use krabka_metadata::{
    BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
};
use krabka_raft::{
    AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    UpdateVoter,
};
use krabka_units::secs;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use super::*;
use crate::{
    config_keys::RecoveryStrategy, heartbeat::controller_state::ControllerLivenessState,
    metadata_source::MetadataSource,
};

/// Minimal `MetadataSource` that drives the control flow of
/// `run_recovery`. These paths exercise only `watch_leader`,
/// `current_image`, and `submit_change`, and never reach the rest.
struct MockSource {
    leader_rx: watch::Receiver<Option<NodeId>>,
    _leader_tx: watch::Sender<Option<NodeId>>,
    image: Arc<MetadataImage>,
}

impl MockSource {
    fn new(leader: Option<u64>, image: MetadataImage) -> Self {
        let (tx, rx) = watch::channel(leader.map(NodeId));
        Self {
            leader_rx: rx,
            _leader_tx: tx,
            image: Arc::new(image),
        }
    }
}

#[async_trait::async_trait]
impl MetadataSource for MockSource {
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
        _records: Vec<MetadataRecord>,
    ) -> Result<krabka_raft::SubmitChangeResult, RaftError> {
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

const NODE: u64 = 10;

fn image_with_partition(leader: u64, replicas: &[u64]) -> MetadataImage {
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
        leader: NodeId(leader),
        replicas: replicas.iter().copied().map(NodeId).collect(),
        isr: replicas.iter().copied().map(NodeId).collect(),
        leader_epoch: krabka_metadata::LeaderEpoch(5),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }));
    img
}

fn register_broker(img: &mut MetadataImage, node_id: u64, host: &str, port: u16) {
    img.apply(&MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            node_id: NodeId(node_id),
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::nil(),
            host: host.into(),
            port,
            rack: None,
            log_dirs: vec![],
            endpoints: vec![],
            features: std::collections::BTreeMap::new(),
        },
    ));
}

async fn liveness_with_alive(alive: &[u64]) -> Arc<ControllerLivenessState> {
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for &n in alive {
        l.record_heartbeat(n).await;
    }
    Arc::new(l)
}

fn manager(source: MockSource, liveness: Arc<ControllerLivenessState>) -> UncleanRecoveryManager {
    UncleanRecoveryManager {
        controller: Arc::new(source),
        liveness,
        node_id: NodeId(NODE),
        inter_broker_client: Arc::new(InterBrokerClient::new(None, None)),
        listener_protocol: krabka_security::ListenerProtocol::Plaintext,
        metrics: crate::metrics::BrokerMetrics::new(),
        policy: RecoveryPolicy {
            aggressive_deadline: secs(2),
            balanced_deadline: secs(30),
            queue_capacity: 256,
            listener_protocol: krabka_security::ListenerProtocol::Plaintext,
            inter_broker_server_name: "localhost".to_string(),
        },
        in_flight: Arc::new(Mutex::new(HashSet::new())),
    }
}

fn job() -> RecoveryJob {
    RecoveryJob {
        topic: "t".into(),
        partition: 0,
        strategy: RecoveryStrategy::None,
        reply: None,
    }
}

#[tokio::test]
async fn not_controller_leader_is_not_needed() {
    let mgr = manager(
        MockSource::new(Some(99), image_with_partition(1, &[1, 2])),
        liveness_with_alive(&[]).await,
    );
    assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NotNeeded);
}

#[tokio::test]
async fn missing_partition_is_not_needed() {
    let mgr = manager(
        MockSource::new(Some(NODE), MetadataImage::new(Uuid::nil())),
        liveness_with_alive(&[]).await,
    );
    assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NotNeeded);
}

#[tokio::test]
async fn live_leader_is_not_needed() {
    let mgr = manager(
        MockSource::new(Some(NODE), image_with_partition(1, &[1, 2])),
        liveness_with_alive(&[1]).await,
    );
    assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NotNeeded);
}

#[tokio::test]
async fn dead_leader_no_alive_replicas_is_no_eligible() {
    // Leader 1 is dead and no replica is alive: nothing to query.
    let mgr = manager(
        MockSource::new(Some(NODE), image_with_partition(1, &[1, 2])),
        liveness_with_alive(&[]).await,
    );
    assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NoEligibleReplica);
}

#[tokio::test]
async fn dead_leader_all_queries_fail_is_no_eligible() {
    // Replica 2 is alive but its endpoint refuses connections, so the
    // query returns no log info and no winner can be selected.
    let mut img = image_with_partition(1, &[1, 2]);
    register_broker(&mut img, 2, "127.0.0.1", 1);
    let mgr = manager(
        MockSource::new(Some(NODE), img),
        liveness_with_alive(&[2]).await,
    );
    assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NoEligibleReplica);
}

#[tokio::test]
async fn recover_one_dedups_in_flight_job() {
    let mgr = Arc::new(manager(
        MockSource::new(Some(NODE), image_with_partition(1, &[1, 2])),
        liveness_with_alive(&[]).await,
    ));
    // Pre-mark this partition as already recovering.
    mgr.in_flight.lock().await.insert(("t".to_string(), 0));
    let (tx, rx) = oneshot::channel();
    let j = RecoveryJob {
        topic: "t".into(),
        partition: 0,
        strategy: RecoveryStrategy::None,
        reply: Some(tx),
    };
    mgr.clone().recover_one(j).await;
    assert!(rx.await.unwrap() == RecoveryOutcome::InProgress);
}
