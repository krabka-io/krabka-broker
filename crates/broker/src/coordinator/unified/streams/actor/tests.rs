//! Unit tests that drive the streams-group actor through its handle: the
//! heartbeat epoch sequence with no connected `MetadataSource`, and the
//! resolution of a persisted per-group config override.

use assert2::{assert, check};

use super::*;
use crate::coordinator::unified::{
    GroupCoordinator, actor::MetadataProvider, config::NextGenConfig,
    offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
    share::config::ShareGroupConfig, streams::config::KEY_NUM_STANDBY_REPLICAS,
};

#[test]
fn persisted_group_config_overrides_actor_defaults() {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1GroupConfig(
        krabka_metadata::GroupConfigRecord {
            group_id: "streams-app".into(),
            configs: std::collections::BTreeMap::from([(
                KEY_NUM_STANDBY_REPLICAS.into(),
                "1".into(),
            )]),
        },
    ));
    let config =
        resolve_group_config_from_image(&StreamsGroupConfig::default(), &image, "streams-app");
    assert!(config.num_standby_replicas == 1);

    let unaffected =
        resolve_group_config_from_image(&StreamsGroupConfig::default(), &image, "other-app");
    assert!(unaffected == StreamsGroupConfig::default());
}

#[derive(Debug)]
struct EmptyMetadata;
impl MetadataProvider for EmptyMetadata {
    fn snapshot(&self) -> ReconcileInput {
        ReconcileInput::default()
    }
}

/// Builds a coordinator with no connected `MetadataSource`, so reconcile
/// falls through to `NotReady`, and with a fake offsets log.
fn make_coordinator() -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let metadata: Arc<dyn MetadataProvider> = Arc::new(EmptyMetadata);
    let coord = Arc::new(GroupCoordinator::new(
        NextGenConfig::default(),
        ShareGroupConfig::default(),
        metadata,
        log.clone(),
        StreamsGroupConfig::default(),
    ));
    (coord, log)
}

async fn heartbeat(
    handle: &StreamsGroupActorHandle,
    req: StreamsGroupHeartbeatRequest,
) -> StreamsGroupHeartbeatResponse {
    let (tx, rx) = oneshot::channel();
    handle
        .tx
        .send(StreamsGroupActorMessage::Heartbeat {
            request: Box::new(req),
            client_id: "client".into(),
            client_host: "/127.0.0.1".into(),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_mints_id_advances_epoch_not_ready() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let resp = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: 0,
            ..Default::default()
        },
    )
    .await;
    check!(resp.error_code == codes::NONE);
    check!(!resp.member_id.is_empty(), "server mints a member id");
    // No metadata source / no topology → NotReady, empty assignment, but the
    // member still advances to the (bumped) group epoch.
    check!(resp.member_epoch == 1);
    check!(resp.active_tasks == Some(vec![]));
    check!(resp.standby_tasks == Some(vec![]));
    check!(resp.warmup_tasks == Some(vec![]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_heartbeat_at_right_epoch_accepted() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            ..Default::default()
        },
    )
    .await;
    assert!(join.error_code == codes::NONE);
    let epoch = join.member_epoch;
    let resp = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: epoch,
            ..Default::default()
        },
    )
    .await;
    assert!(resp.error_code == codes::NONE);
    assert!(resp.member_epoch == epoch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_limit_rejects_only_new_members() {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let metadata: Arc<dyn MetadataProvider> = Arc::new(EmptyMetadata);
    let coord = Arc::new(GroupCoordinator::new(
        NextGenConfig::default(),
        ShareGroupConfig::default(),
        metadata,
        log,
        StreamsGroupConfig {
            max_size: 1,
            ..StreamsGroupConfig::default()
        },
    ));
    let handle = coord.get_or_create_streams("g");
    let request = |member_id: &str, member_epoch| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        ..Default::default()
    };

    let joined = heartbeat(&handle, request("m1", 0)).await;
    check!(joined.error_code == codes::NONE);

    let rejected = heartbeat(&handle, request("m2", 0)).await;
    check!(rejected.error_code == codes::GROUP_MAX_SIZE_REACHED);

    let existing = heartbeat(&handle, request("m1", joined.member_epoch)).await;
    check!(existing.error_code == codes::NONE);
    check!(existing.member_epoch == joined.member_epoch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_epoch_is_rejected() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            ..Default::default()
        },
    )
    .await;
    assert!(join.member_epoch == 1);
    // member_epoch below the server's view → STALE_MEMBER_EPOCH (the member
    // is known at epoch 1, so re-sending epoch 0 is treated as a stale
    // existing member, not a first-join).
    let resp = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: -2,
            ..Default::default()
        },
    )
    .await;
    // -2 < 1 → stale. (member_epoch 0 from a *known* member is the
    // first-join guard's `!contains_key` miss, so we use a clearly-stale
    // value here.)
    assert!(resp.error_code == codes::STALE_MEMBER_EPOCH);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn known_member_epoch_zero_is_stale_not_first_join() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            ..Default::default()
        },
    )
    .await;
    assert!(join.member_epoch == 1);

    let resp = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            ..Default::default()
        },
    )
    .await;

    assert!(resp.error_code == codes::STALE_MEMBER_EPOCH);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_epoch_is_rejected() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            ..Default::default()
        },
    )
    .await;
    assert!(join.member_epoch == 1);
    let resp = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 99,
            ..Default::default()
        },
    )
    .await;
    assert!(resp.error_code == codes::FENCED_MEMBER_EPOCH);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_removes_member() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: 0,
            ..Default::default()
        },
    )
    .await;
    let mid = join.member_id.clone();
    let pre_leave = log.batches().await.len();

    let resp = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: mid,
            member_epoch: -1,
            ..Default::default()
        },
    )
    .await;
    assert!(resp.error_code == codes::NONE);
    assert!(resp.member_epoch == -1);
    let batches = log.batches().await;
    assert!(batches.len() == pre_leave + 1);
    let leave_batch = &batches[batches.len() - 1];
    assert!(
        leave_batch.records.iter().any(|r| r.value.is_none()),
        "leave batch must contain at least one tombstone"
    );
}
