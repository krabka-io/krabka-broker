//! Fixtures shared by the bootstrap unit tests: an in-process controller that
//! has already elected a leader, the two `GroupCoordinator` flavours the tests
//! drive, and the classic record builder they replay.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::assert;
use krabka_raft::ControllerHandle;

use crate::{
    coordinator::{GroupCoordinator, persistence::GroupMetadataValue},
    partition_registry::PartitionRegistry,
};

/// Start a controller, wait until it reports a leader, and return the
/// handle.
pub(super) async fn controller_with_leader(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
    let cfg = krabka_raft::ControllerConfig {
        election_timeout: krabka_units::millis(200),
        heartbeat_interval: Some(krabka_units::millis(50)),
        client_id: "test".into(),
        ..krabka_raft::ControllerConfig::for_tests(krabka_raft::NodeId(1), log_dir)
    };
    let handle = Arc::new(krabka_raft::Controller::start(cfg).await.unwrap());
    let mut rx = handle.watch_leader();
    let deadline = Instant::now() + Duration::from_secs(5);
    while rx.borrow().is_none() {
        assert!(Instant::now() < deadline, "no leader elected in 5s");
        let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
    }
    handle
}

pub(super) fn test_coordinator(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    partitions: &Arc<PartitionRegistry>,
) -> Arc<GroupCoordinator> {
    let offsets_log: Arc<dyn crate::coordinator::unified::offsets_log::OffsetsLog> = Arc::new(
        crate::coordinator::unified::offsets_log::ProductionOffsetsLog::new(
            partitions.clone(),
            controller.clone(),
        ),
    );
    Arc::new(GroupCoordinator::new(
        crate::coordinator::unified::config::NextGenConfig::default(),
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        Arc::new(crate::coordinator::unified::ImageMetadataProvider {
            controller: controller.clone(),
        }),
        offsets_log,
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ))
}

/// Build a bare `GroupCoordinator` with no metadata wiring and no
/// persister wiring.
///
/// It has the same shape as the coordinator in the share and streams
/// replay tests. A test can drive the `apply_record`, `apply_tombstone`,
/// and `finalize` replay path directly with it.
pub(super) fn bare_coordinator() -> Arc<GroupCoordinator> {
    use crate::coordinator::unified::{
        offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
    };

    #[derive(Debug)]
    struct EmptyMeta;
    impl crate::coordinator::unified::actor::MetadataProvider for EmptyMeta {
        fn snapshot(&self) -> ReconcileInput {
            ReconcileInput::default()
        }
    }

    Arc::new(GroupCoordinator::new(
        crate::coordinator::unified::config::NextGenConfig::default(),
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        Arc::new(EmptyMeta),
        Arc::new(InMemoryOffsetsLog::default()),
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ))
}

/// Encode a classic k2 `GroupMetadata` key-value record pair for group
/// `g` with a single member `m1`.
pub(super) fn classic_group_record(
    group_id: &str,
    member_id: &str,
) -> (bytes::Bytes, bytes::Bytes) {
    use crate::coordinator::persistence::MemberMetadata;
    let key = GroupMetadataValue::encode_key(group_id);
    let value = GroupMetadataValue {
        protocol_type: "consumer".into(),
        generation: 3,
        protocol_name: Some("range".into()),
        leader: Some(member_id.into()),
        current_state_timestamp_ms: 0,
        members: vec![MemberMetadata {
            member_id: member_id.into(),
            group_instance_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            rebalance_timeout_ms: 60_000,
            session_timeout_ms: 30_000,
            subscription: bytes::Bytes::new(),
            assignment: bytes::Bytes::from_static(b"asn"),
        }],
    }
    .encode_value();
    (key, value)
}
