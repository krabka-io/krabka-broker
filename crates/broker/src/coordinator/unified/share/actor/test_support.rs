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
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = Arc::new(GroupCoordinator::new(
        NextGenConfig::default(),
        ShareGroupConfig::default(),
        metadata,
        log.clone(),
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ));
    (coord, log)
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
