//! Fixtures shared by the replicator's unit tests: a static `MetadataSource`,
//! metadata-image builders for the leader and follower-throttle cases, a
//! `Config` over a temporary log dir, and `Fetch` response builders.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};

use krabka_ids::PartitionIndex;
use krabka_log::LogConfig;
use krabka_metadata::{
    MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord,
};
use krabka_protocol::{
    owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    primitives::uuid::Uuid as WireUuid,
};
use krabka_raft::{
    AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    UpdateVoter,
};
use krabka_security::ListenerProtocol;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::Config;
use crate::{
    config::ReplicationRuntimeConfig, partition_registry::PartitionRegistry,
    throttle::ThrottleState,
};
pub(super) const TOPIC: &str = "orders";
pub(super) const PARTITION: i32 = 0;
pub(super) const NODE_ID: NodeId = NodeId(2);
pub(super) const LEADER_ID: NodeId = NodeId(1);
pub(super) const WIRE_TOPIC_ID: WireUuid = WireUuid([7; 16]);

struct StaticMetadataSource {
    image: Arc<MetadataImage>,
    image_rx: watch::Receiver<Arc<MetadataImage>>,
    leader_rx: watch::Receiver<Option<NodeId>>,
}

impl StaticMetadataSource {
    fn new(image: MetadataImage) -> Self {
        let image = Arc::new(image);
        let (_image_tx, image_rx) = watch::channel(image.clone());
        let (_leader_tx, leader_rx) = watch::channel(None);
        Self {
            image,
            image_rx,
            leader_rx,
        }
    }
}

#[async_trait::async_trait]
impl crate::metadata_source::MetadataSource for StaticMetadataSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        self.image.clone()
    }

    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image_rx.clone()
    }

    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader_rx.clone()
    }

    fn quorum_state(&self) -> QuorumState {
        QuorumState {
            current_term: 0,
            last_applied_index: 0,
            current_leader: None,
            voters: Vec::new(),
            voter_nodes: BTreeMap::new(),
            per_voter_matched_index: BTreeMap::new(),
        }
    }

    async fn submit_change(
        &self,
        _records: Vec<MetadataRecord>,
    ) -> Result<krabka_raft::SubmitChangeResult, RaftError> {
        panic!("unused in replicator tests")
    }

    async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        panic!("unused in replicator tests")
    }

    async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
        panic!("unused in replicator tests")
    }

    fn controller_bound_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 0))
    }

    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
        SnapshotRange::NoSnapshot
    }

    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        panic!("unused in replicator tests")
    }

    async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        panic!("unused in replicator tests")
    }

    async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        panic!("unused in replicator tests")
    }

    async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        panic!("unused in replicator tests")
    }

    async fn cancel(&self) {}
}

pub(super) fn image_with_leader(leader: NodeId) -> MetadataImage {
    image_with_topic_id_and_leader(uuid::Uuid::from_bytes(WIRE_TOPIC_ID.0), leader)
}

pub(super) fn image_with_topic_id_and_leader(
    topic_id: uuid::Uuid,
    leader: NodeId,
) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.into(),
        topic_id,
        partitions: 1,
        replication_factor: 2,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.into(),
        partition: PARTITION,
        leader,
        replicas: vec![LEADER_ID, NODE_ID],
        isr: vec![LEADER_ID, NODE_ID],
        leader_epoch: krabka_metadata::LeaderEpoch(4),
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        directories: Vec::new(),
        partition_epoch: 0,
    }));
    image
}

pub(super) fn image_with_follower_throttle(value: &str) -> MetadataImage {
    let mut image = image_with_leader(LEADER_ID);
    let mut overrides = BTreeMap::new();
    overrides.insert(
        crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY.to_string(),
        value.to_string(),
    );
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: TOPIC.into(),
        overrides,
    }));
    image
}

pub(super) fn test_config(image: MetadataImage) -> (Config, tempfile::TempDir) {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
        node_id: NODE_ID,
        topic: TOPIC.into(),
        topic_id: WIRE_TOPIC_ID,
        partition: PartitionIndex(PARTITION),
        leader_node_id: LEADER_ID,
        leader_epoch: krabka_metadata::LeaderEpoch(4),
        leader_host: "127.0.0.1".into(),
        leader_port: 9,
        partitions: Arc::new(PartitionRegistry::new()),
        log_dirs: vec![log_dir.path().to_path_buf()],
        log_settings: LogConfig::default(),
        client_id: "replica-test".into(),
        shutdown: CancellationToken::new(),
        inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
        inter_broker_listener_protocol: ListenerProtocol::Plaintext,
        inter_broker_server_name: "localhost".into(),
        replication: ReplicationRuntimeConfig::default(),
        throttle_state: Arc::new(ThrottleState::new()),
        controller: Arc::new(StaticMetadataSource::new(image)),
        log_dir_status: crate::log_dir_status::LogDirRegistry::default(),
        producer_state: Arc::new(crate::producer_state::ProducerState::new()),
        metrics: crate::metrics::BrokerMetrics::default(),
    };
    (cfg, log_dir)
}

pub(super) fn fetch_response(
    topic: &str,
    topic_id: WireUuid,
    part: PartitionData,
) -> FetchResponse {
    FetchResponse {
        responses: vec![FetchableTopicResponse {
            topic: topic.into(),
            topic_id,
            partitions: vec![part],
            ..FetchableTopicResponse::default()
        }],
        ..FetchResponse::default()
    }
}

pub(super) fn partition_response(partition_index: i32, error_code: i16) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        ..PartitionData::default()
    }
}
