//! Fixtures shared by the supervisor's unit tests: metadata-record builders, a
//! static `MetadataSource`, a counting `AssignDirsReporter`, and a supervisor
//! built over a temporary log dir.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use assert2::assert;
use krabka_log::LogConfig;
use krabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord,
    TopicRecord,
};
use krabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest;
use krabka_raft::NodeId;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ReplicatorSupervisor, ReplicatorSupervisorConfig, dir_assignments::AssignDirsReporter,
    materialize::MaterializePartitionConfig,
};
pub(super) use crate::test_support::await_until;
use crate::{
    config::ReplicationRuntimeConfig, metadata_source::MetadataSource,
    partition_registry::PartitionRegistry, test_support::FakeMetadataSource,
    throttle::ThrottleState,
};

pub(super) fn image_with(records: &[MetadataRecord]) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    for r in records {
        img.apply(r);
    }
    img
}

pub(super) fn topic_record(name: &str, partitions: i32) -> MetadataRecord {
    topic_record_with_id(name, Uuid::new_v4(), partitions, 3)
}

pub(super) fn topic_record_with_id(
    name: &str,
    topic_id: Uuid,
    partitions: i32,
    replication_factor: i16,
) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id,
        partitions,
        replication_factor,
    })
}

pub(super) fn single_partition_image(topic: &str, topic_id: Uuid, leader: NodeId) -> MetadataImage {
    image_with(&[
        topic_record_with_id(topic, topic_id, 1, 1),
        partition_record(topic, 0, leader, vec![leader], 0),
    ])
}

/// One rf=3 topic and its partition, with the caller's exact replica list and epoch.
pub(super) fn replicated_partition_image(
    topic: &str,
    leader: NodeId,
    replicas: &[NodeId],
    epoch: i32,
) -> MetadataImage {
    image_with(&[
        topic_record(topic, 1),
        partition_record(topic, 0, leader, replicas.to_vec(), epoch),
    ])
}

/// The supervisor tests' rf=3 topic, with all three brokers as replicas.
pub(super) fn three_replica_image(leader: NodeId, epoch: i32) -> MetadataImage {
    replicated_partition_image("t", leader, &[NodeId(1), NodeId(2), NodeId(3)], epoch)
}

pub(super) fn follower_promotion_images() -> (MetadataImage, MetadataImage) {
    let replicas = vec![NodeId(1), NodeId(2)];
    let topic = topic_record("t", 1);
    (
        image_with(&[
            topic.clone(),
            partition_record("t", 0, NodeId(1), replicas.clone(), 3),
        ]),
        image_with(&[topic, partition_record("t", 0, NodeId(2), replicas, 7)]),
    )
}

#[derive(Default)]
pub(super) struct MaterializeFixture {
    log_dir_status: crate::log_dir_status::LogDirRegistry,
    producer_state: Arc<crate::producer_state::ProducerState>,
}

impl MaterializeFixture {
    pub(super) fn config<'a>(
        &'a self,
        partitions: &'a PartitionRegistry,
        topic: &'a str,
        log_dirs: &'a [PathBuf],
        log_config: &'a LogConfig,
    ) -> MaterializePartitionConfig<'a> {
        MaterializePartitionConfig {
            partitions,
            topic,
            topic_id: None,
            partition: 0,
            log_dirs,
            log_config,
            log_dir_status: &self.log_dir_status,
            producer_state: &self.producer_state,
            runtime: crate::partition::PartitionRuntimeConfig::new(
                (1_024, 64, 3),
                false,
                (None, None, None),
            ),
        }
    }

    pub(super) fn materialize(
        &self,
        partitions: &PartitionRegistry,
        topic: &str,
        log_dirs: &[PathBuf],
        log_config: &LogConfig,
        context: &str,
    ) {
        super::materialize::materialize_partition(
            self.config(partitions, topic, log_dirs, log_config),
        )
        .expect(context);
    }
}

/// Materialize `t-0` with the usual config, retaining its directory and registry.
pub(super) fn materialized_partition() -> (tempfile::TempDir, Arc<PartitionRegistry>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    MaterializeFixture::default().materialize(
        &partitions,
        "t",
        &[dir.path().to_path_buf()],
        &LogConfig::default(),
        "materialize",
    );
    (dir, partitions)
}

pub(super) fn partition_record(
    topic: &str,
    partition: i32,
    leader: NodeId,
    replicas: Vec<NodeId>,
    leader_epoch: i32,
) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.into(),
        partition,
        leader,
        replicas: replicas.clone(),
        isr: replicas,
        leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    })
}

pub(super) fn broker_record(node_id: NodeId) -> BrokerRegistrationRecord {
    BrokerRegistrationRecord {
        incarnation_id: Uuid::new_v4(),
        host: "legacy-host".into(),
        endpoints: vec![BrokerEndpoint {
            name: "INTERNAL".into(),
            host: "internal-host".into(),
            port: 19092,
            protocol: krabka_security::ListenerProtocol::Plaintext,
        }],
        ..crate::test_support::broker_registration(node_id)
    }
}

/// A metadata source over `image` with no controller leader elected, and a
/// loopback controller listener for the assign-dirs reporter to resolve
/// against.
pub(super) fn static_source(image: MetadataImage) -> FakeMetadataSource {
    FakeMetadataSource::static_image(image)
}

#[derive(Default)]
pub(super) struct CountingAssignDirsReporter {
    pub(super) calls: AtomicUsize,
}

#[async_trait::async_trait]
impl AssignDirsReporter for CountingAssignDirsReporter {
    async fn send(
        &self,
        _controller: &Arc<dyn MetadataSource>,
        _client_id: &str,
        req: AssignReplicasToDirsRequest,
    ) -> Result<(), String> {
        assert!(!req.directories.is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub(super) type SupervisorFixture = (
    ReplicatorSupervisor,
    Arc<PartitionRegistry>,
    Arc<CountingAssignDirsReporter>,
    tempfile::TempDir,
);

/// Reconcile once and retrieve `t-0`, retaining every supervisor fixture guard.
pub(super) async fn reconciled_partition(
    image: &MetadataImage,
    context: &str,
) -> (SupervisorFixture, Arc<crate::partition::Partition>) {
    let fixture = reconciled_supervisor(image).await;
    let partition = fixture
        .1
        .get("t", krabka_ids::PartitionIndex(0))
        .expect(context);
    (fixture, partition)
}

/// A supervisor fixture after the first metadata reconciliation, with every guard retained.
pub(super) async fn reconciled_supervisor(image: &MetadataImage) -> SupervisorFixture {
    let fixture = supervisor_fixture(image.clone());
    fixture.0.reconcile(image).await;
    fixture
}

pub(super) fn supervisor_fixture(image: MetadataImage) -> SupervisorFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    let reporter = Arc::new(CountingAssignDirsReporter::default());
    let mut supervisor = ReplicatorSupervisor::new(ReplicatorSupervisorConfig {
        node_id: NodeId(2),
        broker_id: 2,
        controller: Arc::new(static_source(image)),
        partitions: partitions.clone(),
        log_dirs: vec![dir.path().to_path_buf()],
        log_config: LogConfig::default(),
        unstable_api_versions: crate::api_catalog::UnstableApiVersions::Disabled,
        client_id: "supervisor-test".into(),
        shutdown: CancellationToken::new(),
        txn_coordinator: None,
        share_coordinator: None,
        inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
        inter_broker_listener_protocol: krabka_security::ListenerProtocol::Plaintext,
        inter_broker_server_name: "localhost".into(),
        inter_broker_listener_name: "INTERNAL".into(),
        controller_listener_protocol: krabka_security::ListenerProtocol::Plaintext,
        controller_server_name: "localhost".into(),
        controller_quorum_voters: Vec::new(),
        replication: ReplicationRuntimeConfig::default(),
        throttle_state: Arc::new(ThrottleState::new()),
        log_dir_status: crate::log_dir_status::LogDirRegistry::default(),
        producer_state: Arc::new(crate::producer_state::ProducerState::new()),
        max_produce_group: 1_024,
        partition_writer_queue_depth: 64,
        diskless_wal_local_replica_count: 3,
        metrics: crate::metrics::BrokerMetrics::default(),
        log_dir_ids: crate::log_dir_id::LogDirIds::resolve(&[dir.path().to_path_buf()]),
        hot_tail: Arc::new(crate::diskless::hot_tail::HotTailCache::default()),
        wal_shards: Arc::new(crate::wal::quorum::registry::WalShardRegistry::new(
            krabka_raft::NodeId(2),
        )),
    });
    supervisor.assign_dirs_reporter = reporter.clone();
    (supervisor, partitions, reporter, dir)
}
