//! Fixtures shared by the supervisor's unit tests: metadata-record builders, a
//! static `MetadataSource`, a counting `AssignDirsReporter`, and a supervisor
//! built over a temporary log dir.

use std::{
    net::SocketAddr,
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
use krabka_raft::NodeId;
use krabka_units::hours;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ReplicatorSupervisor, ReplicatorSupervisorConfig, dir_assignments::AssignDirsReporter,
};
use crate::{
    config::ReplicationRuntimeConfig, partition_registry::PartitionRegistry,
    test_support::FakeMetadataSource, throttle::ThrottleState,
};

/// Yield-poll until `cond` holds, with a bounded hang-guard. A real
/// stall then fails the test deterministically instead of spinning
/// forever.
pub(super) async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held: {what}");
}

pub(super) fn image_with(records: &[MetadataRecord]) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    for r in records {
        img.apply(r);
    }
    img
}

pub(super) fn topic_record(name: &str, partitions: i32) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: Uuid::new_v4(),
        partitions,
        replication_factor: 3,
    })
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
        node_id,
        broker_epoch: 0,
        incarnation_id: Uuid::new_v4(),
        host: "legacy-host".into(),
        port: 9092,
        rack: None,
        log_dirs: vec![],
        endpoints: vec![BrokerEndpoint {
            name: "INTERNAL".into(),
            host: "internal-host".into(),
            port: 19092,
            protocol: krabka_security::ListenerProtocol::Plaintext,
        }],
        features: std::collections::BTreeMap::new(),
    }
}

/// A metadata source over `image` with no controller leader elected, and a
/// loopback controller listener for the assign-dirs reporter to resolve
/// against.
pub(super) fn static_source(image: MetadataImage) -> FakeMetadataSource {
    FakeMetadataSource::builder()
        .image(image)
        .controller_bound_addr(SocketAddr::from(([127, 0, 0, 1], 0)))
        .build()
}

#[derive(Default)]
pub(super) struct CountingAssignDirsReporter {
    pub(super) calls: AtomicUsize,
}

#[async_trait::async_trait]
impl AssignDirsReporter for CountingAssignDirsReporter {
    async fn send(
        &self,
        _controller: &Arc<dyn crate::metadata_source::MetadataSource>,
        _client_id: &str,
        req: krabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
    ) -> Result<(), String> {
        assert!(!req.directories.is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub(super) fn supervisor_fixture(
    image: MetadataImage,
) -> (
    ReplicatorSupervisor,
    Arc<PartitionRegistry>,
    Arc<CountingAssignDirsReporter>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    let reporter = Arc::new(CountingAssignDirsReporter::default());
    let mut supervisor = ReplicatorSupervisor::new(ReplicatorSupervisorConfig {
        client_dispatch_queue_capacity:
            krabka_client_core::ConnectionDispatchQueueCapacity::default(),
        client_frame_max: krabka_client_core::ClientFrameMax::default(),
        node_id: NodeId(2),
        broker_id: 2,
        controller: Arc::new(static_source(image)),
        partitions: partitions.clone(),
        log_dirs: vec![dir.path().to_path_buf()],
        log_config: LogConfig::default(),
        client_id: "supervisor-test".into(),
        shutdown: CancellationToken::new(),
        txn_coordinator: None,
        share_coordinator: None,
        inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
        inter_broker_listener_protocol: krabka_security::ListenerProtocol::Plaintext,
        inter_broker_server_name: "localhost".into(),
        inter_broker_listener_name: "INTERNAL".into(),
        replication: ReplicationRuntimeConfig::default(),
        throttle_state: Arc::new(ThrottleState::new()),
        log_dir_status: crate::log_dir_status::LogDirRegistry::default(),
        producer_state: Arc::new(crate::producer_state::ProducerState::new()),
        producer_id_expiration: hours(24),
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
