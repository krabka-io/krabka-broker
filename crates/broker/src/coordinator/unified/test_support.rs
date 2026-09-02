//! Shared unit-test fixtures for the coordinator modules: coordinator
//! builders, a metadata provider and a metadata source with fixed contents, a
//! share persister, and the record values the replay tests feed in.
//!
//! Several sibling modules assert against the same fixtures, so they live in
//! one module rather than being rebuilt per test file.

use std::sync::Arc;

use super::{
    actor::MetadataProvider,
    config::NextGenConfig,
    group_coordinator::GroupCoordinator,
    persistence_next_gen, reconciler,
    share::{self, config::ShareGroupConfig},
    streams::{self, config::StreamsGroupConfig},
};

/// Yield-poll until `cond` holds.
///
/// A bounded hang-guard makes a real stall fail the test in a
/// deterministic way, and the loop does not spin forever.
pub(crate) async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held: {what}");
}

pub(crate) fn make_coord() -> Arc<GroupCoordinator> {
    make_coord_with_log().0
}

pub(super) fn make_coord_with_log() -> (
    Arc<GroupCoordinator>,
    Arc<crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog>,
) {
    use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;
    let metadata: Arc<dyn MetadataProvider> = Arc::new(ImageMetadatalessProvider);
    let offsets_log = Arc::new(InMemoryOffsetsLog::default());
    let coord = Arc::new(GroupCoordinator::new(
        NextGenConfig::default(),
        ShareGroupConfig::default(),
        metadata,
        offsets_log.clone(),
        StreamsGroupConfig::default(),
    ));
    (coord, offsets_log)
}

#[derive(Debug)]
pub(super) struct ImageMetadatalessProvider;
impl MetadataProvider for ImageMetadatalessProvider {
    fn snapshot(&self) -> reconciler::ReconcileInput {
        reconciler::ReconcileInput::default()
    }
}

/// A metadata source over `image`, with node 1 reported as the controller
/// leader.
pub(super) fn fixed_source(
    image: krabka_metadata::MetadataImage,
) -> Arc<dyn crate::metadata_source::MetadataSource> {
    Arc::new(
        crate::test_support::FakeMetadataSource::builder()
            .image(image)
            .leader(Some(krabka_raft::NodeId(1)))
            .build(),
    )
}

pub(super) fn make_share_persister(
    source: Arc<dyn crate::metadata_source::MetadataSource>,
) -> Arc<crate::share_coordinator::persister_client::SharePersister> {
    let share_coordinator = Arc::new(
        crate::share_coordinator::coordinator::ShareCoordinator::new(
            krabka_metadata::NodeId(1),
            Arc::new(crate::partition_registry::PartitionRegistry::new()),
            crate::share_coordinator::config::ShareCoordinatorConfig::default(),
        ),
    );
    Arc::new(
        crate::share_coordinator::persister_client::SharePersister::new(
            krabka_metadata::NodeId(1),
            share_coordinator,
            source,
            Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
            krabka_security::ListenerProtocol::Plaintext,
            "PLAINTEXT".into(),
        ),
    )
}

pub(super) fn proto_uuid(byte: u8) -> krabka_protocol::primitives::uuid::Uuid {
    krabka_protocol::primitives::uuid::Uuid([byte; 16])
}

pub(super) fn real_uuid(byte: u8) -> uuid::Uuid {
    uuid::Uuid::from_bytes([byte; 16])
}

pub(super) fn next_member(client_id: &str) -> persistence_next_gen::MemberMetadataValue {
    persistence_next_gen::MemberMetadataValue {
        instance_id: Some(format!("{client_id}-instance")),
        rack_id: Some("rack-a".into()),
        client_id: client_id.into(),
        client_host: "host".into(),
        subscribed_topic_names: vec!["topic-a".into()],
        subscribed_topic_regex: Some("topic-.*".into()),
        server_assignor: Some("range".into()),
        rebalance_timeout_ms: 45_000,
        classic: None,
    }
}

pub(super) fn next_current(epoch: i32) -> persistence_next_gen::CurrentMemberAssignmentValue {
    persistence_next_gen::CurrentMemberAssignmentValue {
        member_epoch: epoch,
        previous_member_epoch: epoch - 1,
        state: persistence_next_gen::MemberAssignmentState::Stable,
        assigned_partitions: vec![persistence_next_gen::AssignedTopicPartitions {
            topic_id: proto_uuid(1),
            partitions: vec![0, 1],
        }],
        partitions_pending_revocation: vec![],
    }
}

pub(super) fn share_member(client_id: &str) -> share::persistence::ShareGroupMemberMetadataValue {
    share::persistence::ShareGroupMemberMetadataValue {
        rack_id: Some("rack-b".into()),
        client_id: client_id.into(),
        client_host: "host".into(),
        subscribed_topic_names: vec!["share-topic".into()],
    }
}

pub(super) fn streams_member(
    client_id: &str,
) -> streams::persistence::StreamsGroupMemberMetadataValue {
    streams::persistence::StreamsGroupMemberMetadataValue {
        instance_id: Some(format!("{client_id}-instance")),
        rack_id: Some("rack-c".into()),
        client_id: client_id.into(),
        client_host: "host".into(),
        process_id: "process".into(),
        user_endpoint: Some(streams::persistence::StreamsEndpoint {
            host: "localhost".into(),
            port: 8080,
        }),
        client_tags: vec![("app".into(), "streams".into())],
        rebalance_timeout_ms: 30_000,
        topology_epoch: 4,
    }
}
