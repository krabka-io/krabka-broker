//! Unit tests that drive the streams-group actor through its handle: the
//! heartbeat epoch sequence with no connected `MetadataSource`, and the
//! resolution of a persisted per-group config override.

use std::sync::atomic::Ordering;

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
            configs: maplit::btreemap! {KEY_NUM_STANDBY_REPLICAS.into() => "1".into()},
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

/// The member epoch rule of Kafka's `throwIfStreamsGroupMemberEpochIsInvalid`.
/// Member `m1` is at epoch 3 with previous epoch 2: it joins at epoch 1, `m2`
/// joins (group epoch 2), `m1` heartbeats at 1, `m3` joins (group epoch 3),
/// and `m1` heartbeats at 2. With no metadata source every assignment is
/// empty. Each row sends one heartbeat on a fresh group. An accepted row must
/// answer exactly what a heartbeat at the member epoch answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_epoch_rule_matches_kafka() {
    use krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds;

    // A request that reports owned tasks reports all three lists, as the
    // Streams client does; the standby and warmup lists are empty.
    let request = |member_id: &str, member_epoch, active_tasks: Option<Vec<TaskIds>>| {
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            member_epoch,
            standby_tasks: active_tasks.as_ref().map(|_| vec![]),
            warmup_tasks: active_tasks.as_ref().map(|_| vec![]),
            active_tasks,
            ..Default::default()
        }
    };
    let unassigned = Some(vec![TaskIds {
        subtopology_id: "s".into(),
        partitions: vec![0],
        ..Default::default()
    }]);
    // (request epoch, owned active tasks, expected error code)
    let rows = [
        (0, None, codes::NONE),
        (2, Some(vec![]), codes::NONE),
        (2, unassigned, codes::FENCED_MEMBER_EPOCH),
        (2, None, codes::FENCED_MEMBER_EPOCH),
        (1, Some(vec![]), codes::FENCED_MEMBER_EPOCH),
        (3, None, codes::NONE),
        (4, Some(vec![]), codes::FENCED_MEMBER_EPOCH),
    ];

    for (index, (member_epoch, active_tasks, error_code)) in rows.into_iter().enumerate() {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        check!(
            heartbeat(&handle, request("m1", 0, None))
                .await
                .member_epoch
                == 1
        );
        check!(
            heartbeat(&handle, request("m2", 0, None))
                .await
                .member_epoch
                == 2
        );
        check!(
            heartbeat(&handle, request("m1", 1, None))
                .await
                .member_epoch
                == 2
        );
        check!(
            heartbeat(&handle, request("m3", 0, None))
                .await
                .member_epoch
                == 3
        );
        check!(
            heartbeat(&handle, request("m1", 2, None))
                .await
                .member_epoch
                == 3
        );

        let resp = heartbeat(&handle, request("m1", member_epoch, active_tasks)).await;

        let expected = if error_code == codes::NONE {
            heartbeat(&handle, request("m1", 3, None)).await
        } else {
            super::response::error_resp(error_code, &StreamsGroupConfig::default())
        };
        check!(resp == expected, "row {index}");
    }
}

/// Kafka's `streamsGroupLeave` answers `UNKNOWN_MEMBER_ID` to a member that
/// the group does not have, and writes no record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_of_an_unknown_member_is_refused_and_writes_nothing() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let joined = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            ..Default::default()
        },
    )
    .await;
    check!(joined.error_code == codes::NONE);
    let before = log.batches().await;

    let resp = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m9".into(),
            member_epoch: -1,
            ..Default::default()
        },
    )
    .await;

    check!(
        resp == super::response::error_resp(
            codes::UNKNOWN_MEMBER_ID,
            &StreamsGroupConfig::default()
        )
    );
    check!(log.batches().await == before);
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
async fn persistence_failure_returns_loading_and_writes_no_partial_batch() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    log.fail_next.store(true, Ordering::SeqCst);

    let response = heartbeat(
        &handle,
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            ..Default::default()
        },
    )
    .await;

    check!(response.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
    assert!(log.batches().await.is_empty());
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

/// A heartbeat applies the member fields that it carries, a rejoin at epoch 0
/// included, and keeps the fields that it leaves out
/// (`StreamsGroupMember.Builder.maybeUpdate*`). Each row sends one heartbeat
/// and compares the persisted `(process_id, rack_id, rebalance_timeout_ms,
/// user endpoint)` of the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_applies_member_metadata() {
    use krabka_protocol::owned::common::streams_group_heartbeat_request::endpoint::Endpoint;

    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let request =
        |member_epoch, process: Option<&str>, rack: Option<&str>, timeout, port: Option<u16>| {
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch,
                process_id: process.map(str::to_owned),
                rack_id: rack.map(str::to_owned),
                rebalance_timeout_ms: timeout,
                user_endpoint: port.map(|port| Endpoint {
                    host: "h".into(),
                    port,
                    ..Default::default()
                }),
                ..Default::default()
            }
        };
    let joined = heartbeat(&handle, request(0, Some("p1"), Some("r1"), 1_000, Some(1))).await;
    check!(joined.error_code == codes::NONE);
    let epoch = joined.member_epoch;
    // (request, expected (process id, rack id, rebalance timeout, endpoint port))
    let rows = [
        (
            request(0, Some("p2"), Some("r2"), 2_000, None),
            ("p2", Some("r2"), 2_000, None),
        ),
        (
            request(-2, None, None, -1, Some(7)),
            ("p2", Some("r2"), 2_000, Some(7)),
        ),
        (
            request(-2, Some("p3"), None, 3_000, None),
            ("p3", Some("r2"), 3_000, Some(7)),
        ),
    ];
    for (index, (mut req, (process, rack, timeout, port))) in rows.into_iter().enumerate() {
        if req.member_epoch == -2 {
            req.member_epoch = coord
                .cached_streams_seed("g")
                .and_then(|seed| seed.current_per_member.get("m1").map(|c| c.member_epoch))
                .unwrap_or(epoch);
        }
        let resp = heartbeat(&handle, req).await;
        check!(resp.error_code == codes::NONE, "row {index}");
        let member = coord.cached_streams_seed("g").expect("seed cached").members["m1"].clone();
        check!(
            (
                member.process_id.as_str(),
                member.rack_id.as_deref(),
                member.rebalance_timeout_ms,
                member.user_endpoint.map(|endpoint| endpoint.port),
            ) == (process, rack, timeout, port),
            "row {index}"
        );
    }
}
