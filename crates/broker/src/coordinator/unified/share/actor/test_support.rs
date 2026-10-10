//! Shared unit-test fixtures for the share-group actor modules: a static
//! metadata provider, a coordinator wired to an in-memory offsets log, and a
//! heartbeat round-trip helper.

use std::sync::Arc;

use krabka_protocol::{
    owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    primitives::uuid::Uuid,
};
use tokio::sync::oneshot;

use super::{ShareGroupActorHandle, ShareGroupActorMessage};
use crate::coordinator::unified::{
    GroupCoordinator, actor::MetadataProvider, config::NextGenConfig,
    offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
    share::config::ShareGroupConfig,
};

#[derive(Debug)]
struct StaticMetadata {
    input: ReconcileInput,
}
impl MetadataProvider for StaticMetadata {
    fn snapshot(&self) -> ReconcileInput {
        self.input.clone()
    }
}

/// Metadata snapshot with a single topic of `parts` partitions.
pub(super) fn metadata_with_topic(name: &str, parts: i32) -> (Arc<dyn MetadataProvider>, Uuid) {
    let id = Uuid([7; 16]);
    let input = ReconcileInput {
        topic_id_by_name: [(name.to_string(), id)].into(),
        partitions_per_topic: [(id, parts)].into(),
        ..Default::default()
    };
    (Arc::new(StaticMetadata { input }), id)
}

pub(super) fn make_coordinator(
    metadata: Arc<dyn MetadataProvider>,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    make_coordinator_with_config(
        metadata,
        NextGenConfig::assigning_at_once(),
        ShareGroupConfig::assigning_at_once(),
    )
}

pub(super) fn make_coordinator_with_config(
    metadata: Arc<dyn MetadataProvider>,
    next_gen: NextGenConfig,
    share: ShareGroupConfig,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = Arc::new(GroupCoordinator::new(
        next_gen,
        share,
        metadata,
        log.clone(),
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ));
    (coord, log)
}

/// Ordinary share heartbeat for the test topic, with every other wire field defaulted.
pub(super) fn subscribed_request(member_id: &str, member_epoch: i32) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        subscribed_topic_names: Some(vec!["t".into()]),
        ..Default::default()
    }
}

pub(super) async fn heartbeat(
    handle: &ShareGroupActorHandle,
    req: ShareGroupHeartbeatRequest,
) -> ShareGroupHeartbeatResponse {
    let (tx, rx) = oneshot::channel();
    handle
        .tx
        .send(ShareGroupActorMessage::Heartbeat {
            request: req,
            client_id: "client-a".into(),
            client_host: "/127.0.0.1".into(),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

/// Seeds the group behind `handle` with `partitions` of `topic` already
/// initialized, as bootstrap replay of a `ShareGroupStatePartitionMetadata`
/// record would.
#[derive(krabka_macros::FieldDefaults)]
pub(super) struct InitializedTopicSetup<'a> {
    pub topic_id: Uuid,
    #[default("t")]
    pub topic_name: &'a str,
    #[default(vec![0])]
    pub partitions: Vec<i32>,
}

pub(super) async fn seed_initialized(
    handle: &ShareGroupActorHandle,
    setup: InitializedTopicSetup<'_>,
) {
    use crate::coordinator::unified::{
        ShareGroupSeed,
        share::persistence::{ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo},
    };
    let InitializedTopicSetup {
        topic_id,
        topic_name,
        partitions,
    } = setup;
    handle
        .tx
        .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
            state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                initialized: vec![TopicPartitionsInfo {
                    topic_id: uuid::Uuid::from_bytes(topic_id.0),
                    topic_name: topic_name.to_owned(),
                    partitions,
                }],
                ..ShareGroupStatePartitionMetadataValue::default()
            },
            ..ShareGroupSeed::new_group()
        }))
        .await
        .unwrap();
}
