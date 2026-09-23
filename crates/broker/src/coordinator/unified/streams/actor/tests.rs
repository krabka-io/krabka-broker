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

/// The member ids that the group holds, sorted.
async fn describe_member_ids(handle: &StreamsGroupActorHandle) -> Vec<String> {
    let (tx, rx) = oneshot::channel();
    handle
        .tx
        .send(StreamsGroupActorMessage::Describe { reply: tx })
        .await
        .unwrap();
    let mut ids: Vec<String> = rx
        .await
        .unwrap()
        .members
        .into_iter()
        .map(|member| member.member_id)
        .collect();
    ids.sort();
    ids
}

async fn heartbeat_result(
    handle: &StreamsGroupActorHandle,
    req: StreamsGroupHeartbeatRequest,
) -> StreamsHeartbeatResult {
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

async fn heartbeat(
    handle: &StreamsGroupActorHandle,
    req: StreamsGroupHeartbeatRequest,
) -> StreamsGroupHeartbeatResponse {
    heartbeat_result(handle, req).await.response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_advances_epoch_not_ready() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
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
    check!(resp.error_code == codes::NONE);
    check!(resp.member_id == "m1");
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
            // A rejoin at epoch 0 gets the task lists; a heartbeat with an
            // unchanged assignment does not.
            let rejoin = (member_epoch == 0).then(Vec::new);
            StreamsGroupHeartbeatResponse {
                active_tasks: rejoin.clone(),
                standby_tasks: rejoin.clone(),
                warmup_tasks: rejoin,
                ..heartbeat(&handle, request("m1", 3, None)).await
            }
        } else {
            let relation = if member_epoch > 3 {
                "greater"
            } else {
                "smaller"
            };
            super::response::error_resp(
                error_code,
                Some(format!(
                    "The streams group member has a {relation} member epoch ({member_epoch}) than \
                     the one known by the group coordinator (3). The member must abandon all its \
                     partitions and rejoin."
                )),
            )
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
            Some("Member m9 is not a member of group g.".into())
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

/// Builds the metadata image of one broker, `broker_rack` its rack, that
/// holds each `(name, id byte, partitions)` topic.
fn image_of(
    broker_rack: Option<&str>,
    topics: &[(&str, u8, i32)],
) -> krabka_metadata::MetadataImage {
    use krabka_metadata::{
        BrokerRegistrationRecord, LeaderEpoch, MetadataRecord, PartitionRecord, TopicRecord,
    };

    let broker = krabka_audit::NodeId(1);
    let mut records = vec![MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            node_id: broker,
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::nil(),
            host: "127.0.0.1".into(),
            port: 9092,
            rack: broker_rack.map(str::to_owned),
            endpoints: vec![],
            log_dirs: vec![],
            features: std::collections::BTreeMap::new(),
        },
    )];
    for &(name, id, partitions) in topics {
        records.push(MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: uuid::Uuid::from_bytes([id; 16]),
            partitions,
            replication_factor: 1,
        }));
        for partition in 0..partitions {
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                topic: name.into(),
                partition,
                leader: broker,
                replicas: vec![broker],
                isr: vec![broker],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }
    }
    krabka_metadata::MetadataImage::from_records(uuid::Uuid::nil(), &records)
}

/// A topology with one subtopology `0` that reads `in` and, when `stateful`,
/// keeps the changelog topic `store-changelog`.
fn one_subtopology(
    stateful: bool,
) -> krabka_protocol::owned::streams_group_heartbeat_request::Topology {
    use krabka_protocol::owned::{
        common::streams_group_heartbeat_request::topic_info::TopicInfo,
        streams_group_heartbeat_request::{Subtopology, Topology},
    };

    Topology {
        epoch: 1,
        subtopologies: vec![Subtopology {
            subtopology_id: "0".into(),
            source_topics: vec!["in".into()],
            state_changelog_topics: if stateful {
                vec![TopicInfo {
                    name: "store-changelog".into(),
                    ..Default::default()
                }]
            } else {
                vec![]
            },
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Kafka refreshes the topic metadata of a streams group on the next
/// heartbeat after a change (`onMetadataUpdate`, `hasMetadataExpired`,
/// `computeMetadataHash`) and bumps the group epoch when the hash or the
/// member metadata changed (`hasStreamsMemberMetadataChanged`). Each row joins
/// one member, applies one change, sends one heartbeat at the member epoch, and
/// compares the whole response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_heartbeat_after_a_topic_or_member_change_recomputes_the_assignment() {
    use krabka_protocol::owned::common::streams_group_heartbeat_response::{
        status::Status, task_ids::TaskIds,
    };

    use crate::{
        coordinator::unified::streams::topology::status, test_support::FakeMetadataSource,
    };

    enum Change {
        Image(Box<krabka_metadata::MetadataImage>),
        Rack(&'static str),
        Process(&'static str),
    }
    struct Row {
        name: &'static str,
        initial: krabka_metadata::MetadataImage,
        stateful: bool,
        change: Change,
        /// The owned active tasks of the heartbeat, when it reports them.
        owned_active: Option<Vec<i32>>,
        epoch: i32,
        /// The active tasks, when the response sends the task lists.
        active: Option<Vec<i32>>,
        status: Option<Vec<Status>>,
    }
    let rows = [
        Row {
            name: "the missing source topic is created",
            initial: image_of(None, &[]),
            stateful: false,
            change: Change::Image(Box::new(image_of(None, &[("in", 1, 2)]))),
            owned_active: None,
            epoch: 2,
            active: Some(vec![0, 1]),
            status: Some(vec![]),
        },
        Row {
            name: "partitions are added to the source topic",
            initial: image_of(None, &[("in", 1, 1)]),
            stateful: false,
            change: Change::Image(Box::new(image_of(None, &[("in", 1, 2)]))),
            owned_active: Some(vec![0]),
            epoch: 2,
            active: Some(vec![0, 1]),
            status: Some(vec![]),
        },
        Row {
            name: "the source topic is deleted",
            initial: image_of(None, &[("in", 1, 2)]),
            stateful: false,
            change: Change::Image(Box::new(image_of(None, &[]))),
            owned_active: Some(vec![]),
            epoch: 2,
            active: Some(vec![]),
            status: Some(vec![Status {
                status_code: status::MISSING_SOURCE_TOPICS,
                status_detail: "Source topics in are missing.".into(),
                ..Default::default()
            }]),
        },
        Row {
            name: "the member sends a new rack id",
            initial: image_of(None, &[("in", 1, 1)]),
            stateful: false,
            change: Change::Rack("rack-b"),
            owned_active: Some(vec![0]),
            epoch: 2,
            active: None,
            status: Some(vec![]),
        },
        Row {
            name: "the member sends a new process id",
            initial: image_of(None, &[("in", 1, 1)]),
            stateful: false,
            change: Change::Process("process-b"),
            owned_active: Some(vec![0]),
            epoch: 2,
            active: None,
            status: Some(vec![]),
        },
        Row {
            name: "the internal topic is created",
            initial: image_of(None, &[("in", 1, 1)]),
            stateful: true,
            change: Change::Image(Box::new(image_of(
                None,
                &[("in", 1, 1), ("store-changelog", 2, 1)],
            ))),
            owned_active: Some(vec![]),
            epoch: 2,
            active: Some(vec![0]),
            status: Some(vec![]),
        },
    ];

    for row in rows {
        let source = Arc::new(FakeMetadataSource::builder().image(row.initial).build());
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source.clone());
        let handle = coord.get_or_create_streams("g");
        let request = |member_epoch, rack: &str, process: &str| StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch,
            rack_id: Some(rack.into()),
            process_id: Some(process.into()),
            rebalance_timeout_ms: 1_000,
            ..Default::default()
        };
        let joined = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                topology: Some(one_subtopology(row.stateful)),
                ..request(0, "rack-a", "process-a")
            },
        )
        .await;
        check!(joined.member_epoch == 1, "{}", row.name);

        let (rack, process) = match row.change {
            Change::Image(image) => {
                source.set_image(*image);
                ("rack-a", "process-a")
            }
            Change::Rack(rack) => (rack, "process-a"),
            Change::Process(process) => ("rack-a", process),
        };
        let owned = row.owned_active.map(|partitions| {
            vec![
                krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds {
                    subtopology_id: "0".into(),
                    partitions,
                    ..Default::default()
                },
            ]
        });
        let resp = heartbeat(
            &handle,
            StreamsGroupHeartbeatRequest {
                standby_tasks: owned.as_ref().map(|_| vec![]),
                warmup_tasks: owned.as_ref().map(|_| vec![]),
                active_tasks: owned,
                ..request(joined.member_epoch, rack, process)
            },
        )
        .await;

        let tasks = |partitions: Vec<i32>| {
            if partitions.is_empty() {
                vec![]
            } else {
                vec![TaskIds {
                    subtopology_id: "0".into(),
                    partitions,
                    ..Default::default()
                }]
            }
        };
        let expected = StreamsGroupHeartbeatResponse {
            member_id: "m1".into(),
            status: row.status,
            standby_tasks: row.active.as_ref().map(|_| vec![]),
            warmup_tasks: row.active.as_ref().map(|_| vec![]),
            active_tasks: row.active.map(tasks),
            ..super::response::base_resp(codes::NONE, row.epoch, &StreamsGroupConfig::default())
        };
        check!(resp == expected, "{}", row.name);
    }
}

/// A reconcile that installs a new target changes the assignment of every
/// member, so the record batch of the heartbeat that ran it carries the
/// target assignment of every member, as Kafka's `TargetAssignmentBuilder`
/// writes one record for each member whose target changed. Without them a
/// replay pairs the new assignment epoch with old member targets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_target_persists_the_target_of_every_member() {
    use crate::{
        coordinator::unified::{persistence::Key, streams::persistence::StreamsGroupKey},
        test_support::FakeMetadataSource,
    };

    let source = Arc::new(
        FakeMetadataSource::builder()
            .image(image_of(None, &[("in", 1, 2)]))
            .build(),
    );
    let (coord, log) = make_coordinator();
    coord.set_metadata_source(source.clone());
    let handle = coord.get_or_create_streams("g");
    let request = |member_id: &str, member_epoch| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        rebalance_timeout_ms: 1_000,
        topology: (member_epoch == 0).then(|| one_subtopology(false)),
        ..Default::default()
    };
    let m1 = heartbeat(&handle, request("m1", 0)).await;
    let m2 = heartbeat(&handle, request("m2", 0)).await;
    check!((m1.member_epoch, m2.member_epoch) == (1, 2));

    source.set_image(image_of(None, &[("in", 1, 4)]));
    let resp = heartbeat(&handle, request("m1", 1)).await;
    check!(resp.member_epoch == 3);

    let batches = log.batches().await;
    let last = batches.last().expect("the heartbeat wrote a batch");
    let mut targets: Vec<String> = last
        .records
        .iter()
        .filter_map(|record| {
            let key = record.key.as_deref()?;
            match crate::coordinator::unified::persistence::parse_key(key) {
                Ok(Key::Streams(StreamsGroupKey::TargetAssignmentMember { member_id, .. })) => {
                    Some(member_id)
                }
                _ => None,
            }
        })
        .collect();
    targets.sort();
    check!(targets == vec!["m1".to_string(), "m2".to_string()]);
}

/// A seeded group whose internal topics are missing asks for them again on
/// the next heartbeat, as Kafka configures the topology of a loaded group on
/// its first heartbeat and returns the internal topics to create. The seed
/// carries the metadata hash, so the hash alone does not trigger it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seeded_group_asks_for_its_missing_internal_topics_again() {
    use krabka_protocol::owned::common::streams_group_heartbeat_response::status::Status;

    use crate::{
        coordinator::unified::streams::topology::{InternalTopicSpec, status},
        test_support::FakeMetadataSource,
    };

    let image = || image_of(None, &[("in", 1, 1)]);
    let (before, _log) = make_coordinator();
    before.set_metadata_source(Arc::new(
        FakeMetadataSource::builder().image(image()).build(),
    ));
    let joined = heartbeat(
        &before.get_or_create_streams("g"),
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            topology: Some(one_subtopology(true)),
            ..Default::default()
        },
    )
    .await;
    check!(joined.member_epoch == 1);
    let seed = before
        .cached_streams_seed("g")
        .expect("the join cached a seed");

    let (after, _log) = make_coordinator();
    after.set_metadata_source(Arc::new(
        FakeMetadataSource::builder().image(image()).build(),
    ));
    after.update_streams_cache("g", seed);
    let result = heartbeat_result(
        &after.get_or_create_streams("g"),
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 1,
            ..Default::default()
        },
    )
    .await;

    check!(
        result.creatable_topics
            == vec![InternalTopicSpec {
                name: "store-changelog".into(),
                partitions: 1,
                replication_factor: 0,
                configs: std::collections::BTreeMap::new(),
            }]
    );
    let expected = StreamsGroupHeartbeatResponse {
        member_id: "m1".into(),
        status: Some(vec![Status {
            status_code: status::MISSING_INTERNAL_TOPICS,
            status_detail: "Internal topics are missing: store-changelog".into(),
            ..Default::default()
        }]),
        ..super::response::base_resp(codes::NONE, 1, &StreamsGroupConfig::default())
    };
    check!(result.response == expected);
}

/// Kafka sizes the internal topics with `InternalTopicManager`: a repartition
/// topic takes the partition count of its writer, and a copartition group
/// coerces it to the partition count of the external topics. Each row joins
/// one member with a two-subtopology topology and compares the internal topics
/// that the heartbeat asks `CreateTopics` for, and the whole response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_join_sizes_the_internal_topics_as_kafka_does() {
    use krabka_protocol::owned::{
        common::{
            streams_group_heartbeat_request::topic_info::TopicInfo,
            streams_group_heartbeat_response::status::Status,
        },
        streams_group_heartbeat_request::{CopartitionGroup, Subtopology, Topology},
    };

    use crate::{
        coordinator::unified::streams::topology::{InternalTopicSpec, status},
        test_support::FakeMetadataSource,
    };

    struct Row {
        name: &'static str,
        /// The partition counts of `orders` and `customers`.
        partitions: (i32, i32),
        copartitioned: bool,
        /// The expected partition counts of `rp` and `store-changelog`.
        rp: i32,
        changelog: i32,
    }
    let rows = [
        Row {
            name: "copartition coerces rp to the customers topic",
            partitions: (6, 3),
            copartitioned: true,
            rp: 3,
            changelog: 3,
        },
        Row {
            name: "rp takes the partition count of its writer",
            partitions: (4, 8),
            copartitioned: false,
            rp: 4,
            changelog: 8,
        },
    ];

    for row in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(
                    None,
                    &[
                        ("orders", 1, row.partitions.0),
                        ("customers", 2, row.partitions.1),
                    ],
                ))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        let topology = Topology {
            epoch: 1,
            subtopologies: vec![
                Subtopology {
                    subtopology_id: "0".into(),
                    source_topics: vec!["orders".into()],
                    repartition_sink_topics: vec!["rp".into()],
                    ..Default::default()
                },
                Subtopology {
                    subtopology_id: "1".into(),
                    source_topics: vec!["customers".into()],
                    repartition_source_topics: vec![TopicInfo {
                        name: "rp".into(),
                        ..Default::default()
                    }],
                    state_changelog_topics: vec![TopicInfo {
                        name: "store-changelog".into(),
                        ..Default::default()
                    }],
                    copartition_groups: if row.copartitioned {
                        vec![CopartitionGroup {
                            source_topics: vec![0],
                            repartition_source_topics: vec![0],
                            ..Default::default()
                        }]
                    } else {
                        vec![]
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let result = heartbeat_result(
            &handle,
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                rebalance_timeout_ms: 1_000,
                topology: Some(topology),
                ..Default::default()
            },
        )
        .await;

        let spec = |name: &str, partitions| InternalTopicSpec {
            name: name.into(),
            partitions,
            replication_factor: 0,
            configs: std::collections::BTreeMap::new(),
        };
        check!(
            result.creatable_topics
                == vec![spec("rp", row.rp), spec("store-changelog", row.changelog)],
            "{}",
            row.name
        );
        let expected = StreamsGroupHeartbeatResponse {
            member_id: "m1".into(),
            status: Some(vec![Status {
                status_code: status::MISSING_INTERNAL_TOPICS,
                status_detail: "Internal topics are missing: rp, store-changelog".into(),
                ..Default::default()
            }]),
            active_tasks: Some(vec![]),
            standby_tasks: Some(vec![]),
            warmup_tasks: Some(vec![]),
            ..super::response::base_resp(codes::NONE, 1, &StreamsGroupConfig::default())
        };
        check!(result.response == expected, "{}", row.name);
    }
}

/// Kafka builds the `StreamsGroupHeartbeat` status list on every heartbeat:
/// `STALE_TOPOLOGY` for a member behind the group topology, the topology
/// configuration status, and `SHUTDOWN_APPLICATION` while a shutdown request
/// stands. The list is empty, not null, when nothing holds, and the shutdown
/// request ends when the group becomes empty. Each row runs its heartbeats on
/// a fresh group with a one-partition source topic and compares the whole
/// response of the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_status_list_follows_kafka() {
    use std::collections::HashMap;

    use krabka_protocol::owned::{
        common::streams_group_heartbeat_response::{status::Status, task_ids::TaskIds},
        streams_group_heartbeat_request::{Subtopology, Topology},
    };

    use crate::{
        coordinator::unified::streams::topology::status, test_support::FakeMetadataSource,
    };

    #[derive(Clone, Copy)]
    enum Beat {
        /// A join with this topology epoch.
        Join(&'static str, i32),
        /// A heartbeat at the member epoch, with `shutdown_application`.
        Heartbeat(&'static str, bool),
        /// A leave, with `shutdown_application`.
        Leave(&'static str, bool),
    }
    struct Row {
        name: &'static str,
        sources: &'static [&'static str],
        beats: Vec<Beat>,
        member: &'static str,
        epoch: i32,
        /// The active tasks, when the response sends the task lists.
        active: Option<Vec<i32>>,
        status: Vec<(i8, &'static str)>,
    }
    let rows = [
        Row {
            name: "a ready group sends an empty list",
            sources: &["in"],
            beats: vec![Beat::Join("m1", 1)],
            member: "m1",
            epoch: 1,
            active: Some(vec![0]),
            status: vec![],
        },
        Row {
            name: "two missing source topics give one entry",
            sources: &["b", "a"],
            beats: vec![Beat::Join("m1", 1)],
            member: "m1",
            epoch: 1,
            active: Some(vec![]),
            status: vec![(
                status::MISSING_SOURCE_TOPICS,
                "Source topics a, b are missing.",
            )],
        },
        Row {
            name: "a heartbeat requests the shutdown",
            sources: &["in"],
            beats: vec![Beat::Join("m1", 1), Beat::Heartbeat("m1", true)],
            member: "m1",
            epoch: 1,
            active: None,
            status: vec![(
                status::SHUTDOWN_APPLICATION,
                "Streams group member m1 encountered a fatal error and requested a shutdown for \
                 the entire application.",
            )],
        },
        Row {
            name: "a leave requests the shutdown",
            sources: &["in"],
            beats: vec![
                Beat::Join("m1", 1),
                Beat::Join("m2", 1),
                Beat::Leave("m2", true),
                Beat::Heartbeat("m1", false),
            ],
            member: "m1",
            epoch: 3,
            active: None,
            status: vec![(
                status::SHUTDOWN_APPLICATION,
                "Streams group member m2 encountered a fatal error and requested a shutdown for \
                 the entire application.",
            )],
        },
        Row {
            name: "the shutdown request ends when the group becomes empty",
            sources: &["in"],
            beats: vec![
                Beat::Join("m1", 1),
                Beat::Heartbeat("m1", true),
                Beat::Leave("m1", false),
                Beat::Join("m2", 1),
            ],
            member: "m2",
            epoch: 3,
            active: Some(vec![0]),
            status: vec![],
        },
        Row {
            name: "a member behind the group topology gets STALE_TOPOLOGY",
            sources: &["in"],
            beats: vec![Beat::Join("m1", 2), Beat::Join("m2", 1)],
            member: "m2",
            epoch: 2,
            active: Some(vec![]),
            status: vec![(
                status::STALE_TOPOLOGY,
                "The member's topology epoch 1 is behind the group's topology epoch 2.",
            )],
        },
        Row {
            name: "a member at the group topology does not get STALE_TOPOLOGY",
            sources: &["in"],
            beats: vec![
                Beat::Join("m1", 2),
                Beat::Join("m2", 1),
                Beat::Heartbeat("m1", false),
            ],
            member: "m1",
            epoch: 2,
            active: None,
            status: vec![],
        },
    ];

    for row in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 1)]))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        let mut epochs: HashMap<&str, i32> = HashMap::new();
        let mut last = None;
        for beat in &row.beats {
            let (member, member_epoch, shutdown, topology_epoch) = match *beat {
                Beat::Join(member, topology_epoch) => (member, 0, false, Some(topology_epoch)),
                Beat::Heartbeat(member, shutdown) => (member, epochs[member], shutdown, None),
                Beat::Leave(member, shutdown) => (member, -1, shutdown, None),
            };
            let resp = heartbeat(
                &handle,
                StreamsGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: member.into(),
                    member_epoch,
                    rebalance_timeout_ms: 1_000,
                    shutdown_application: shutdown,
                    topology: topology_epoch.map(|epoch| Topology {
                        epoch,
                        subtopologies: vec![Subtopology {
                            subtopology_id: "0".into(),
                            source_topics: row.sources.iter().map(|s| (*s).to_string()).collect(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await;
            check!(resp.error_code == codes::NONE, "{}", row.name);
            epochs.insert(member, resp.member_epoch);
            last = Some(resp);
        }

        let expected = StreamsGroupHeartbeatResponse {
            member_id: row.member.into(),
            status: Some(
                row.status
                    .iter()
                    .map(|(status_code, detail)| Status {
                        status_code: *status_code,
                        status_detail: (*detail).into(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            standby_tasks: row.active.as_ref().map(|_| vec![]),
            warmup_tasks: row.active.as_ref().map(|_| vec![]),
            active_tasks: row.active.map(|partitions| {
                if partitions.is_empty() {
                    vec![]
                } else {
                    vec![TaskIds {
                        subtopology_id: "0".into(),
                        partitions,
                        ..Default::default()
                    }]
                }
            }),
            ..super::response::base_resp(codes::NONE, row.epoch, &StreamsGroupConfig::default())
        };
        check!(last == Some(expected), "{}", row.name);
    }
}

/// The parts of Kafka's `StreamsGroupHeartbeat` response beyond the member
/// epoch and the status: the task lists only on a join or a change, the
/// endpoint information for Interactive Queries, the leave response and the
/// error response. Each row runs its heartbeats on a fresh group with a
/// source topic of the given partition count and compares the whole response
/// of the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_response_carries_what_kafka_sends() {
    use std::collections::HashMap;

    use krabka_protocol::owned::{
        common::{
            streams_group_heartbeat_request::endpoint::Endpoint as RequestEndpoint,
            streams_group_heartbeat_response::{
                endpoint::Endpoint, task_ids::TaskIds, topic_partition::TopicPartition,
            },
        },
        streams_group_heartbeat_response::EndpointToPartitions,
    };

    use crate::test_support::FakeMetadataSource;

    #[derive(Clone, Copy)]
    enum Beat {
        /// A join with this user endpoint port.
        Join(&'static str, Option<u16>),
        /// A heartbeat at the member epoch, or at this epoch.
        Heartbeat(&'static str, Option<i32>),
        Leave(&'static str),
    }
    let endpoint = |port: u16, active: &[i32]| EndpointToPartitions {
        user_endpoint: Endpoint {
            host: "localhost".into(),
            port,
            ..Default::default()
        },
        active_partitions: if active.is_empty() {
            vec![]
        } else {
            vec![TopicPartition {
                topic: "in".into(),
                partitions: active.to_vec(),
                ..Default::default()
            }]
        },
        standby_partitions: vec![],
        ..Default::default()
    };
    let config = StreamsGroupConfig::default();
    let accepted =
        |member_id: &str, member_epoch, tasks: Option<Vec<i32>>| StreamsGroupHeartbeatResponse {
            member_id: member_id.into(),
            status: Some(vec![]),
            active_tasks: tasks.as_ref().map(|partitions| {
                if partitions.is_empty() {
                    vec![]
                } else {
                    vec![TaskIds {
                        subtopology_id: "0".into(),
                        partitions: partitions.clone(),
                        ..Default::default()
                    }]
                }
            }),
            standby_tasks: tasks.as_ref().map(|_| vec![]),
            warmup_tasks: tasks.as_ref().map(|_| vec![]),
            ..super::response::base_resp(codes::NONE, member_epoch, &config)
        };
    let rows = [
        (
            "the joining member of a new group gets its endpoint information",
            1,
            1,
            vec![Beat::Join("m1", Some(1))],
            StreamsGroupHeartbeatResponse {
                partitions_by_user_endpoint: Some(vec![endpoint(1, &[0])]),
                ..accepted("m1", 1, Some(vec![0]))
            },
        ),
        (
            "two members with endpoints",
            10,
            1,
            vec![Beat::Join("m1", Some(1)), Beat::Join("m2", Some(2))],
            StreamsGroupHeartbeatResponse {
                endpoint_information_epoch: 1,
                partitions_by_user_endpoint: Some(vec![endpoint(1, &[0]), endpoint(2, &[])]),
                ..accepted("m2", 2, Some(vec![]))
            },
        ),
        (
            "a member that must revoke a task gets its tasks at its epoch",
            10,
            2,
            vec![
                Beat::Join("m1", None),
                Beat::Join("m2", None),
                Beat::Heartbeat("m1", None),
            ],
            accepted("m1", 1, Some(vec![0])),
        ),
        (
            "a member with an endpoint that must revoke a task",
            10,
            2,
            vec![
                Beat::Join("m1", Some(1)),
                Beat::Join("m2", None),
                Beat::Heartbeat("m1", None),
            ],
            StreamsGroupHeartbeatResponse {
                endpoint_information_epoch: 1,
                partitions_by_user_endpoint: Some(vec![endpoint(1, &[0])]),
                ..accepted("m1", 1, Some(vec![0]))
            },
        ),
        (
            "a heartbeat with an unchanged assignment",
            10,
            1,
            vec![Beat::Join("m1", None), Beat::Heartbeat("m1", None)],
            accepted("m1", 1, None),
        ),
        (
            "a leave",
            10,
            1,
            vec![Beat::Join("m1", None), Beat::Leave("m1")],
            StreamsGroupHeartbeatResponse {
                member_id: "m1".into(),
                member_epoch: -1,
                status: Some(vec![]),
                ..Default::default()
            },
        ),
        (
            "a full group",
            1,
            1,
            vec![Beat::Join("m1", None), Beat::Join("m2", None)],
            super::response::error_resp(
                codes::GROUP_MAX_SIZE_REACHED,
                Some("The streams group has reached its maximum capacity of 1 members.".into()),
            ),
        ),
        (
            "an unknown member",
            10,
            1,
            vec![Beat::Join("m1", None), Beat::Heartbeat("m9", Some(3))],
            super::response::error_resp(
                codes::UNKNOWN_MEMBER_ID,
                Some("Member m9 is not a member of group g.".into()),
            ),
        ),
    ];

    for (name, max_size, partitions, beats, expected) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, partitions)]))
                .build(),
        );
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            ShareGroupConfig::default(),
            Arc::new(EmptyMetadata),
            Arc::new(InMemoryOffsetsLog::default()),
            StreamsGroupConfig {
                max_size,
                ..StreamsGroupConfig::default()
            },
        ));
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        let mut epochs: HashMap<&str, i32> = HashMap::new();
        let mut last = None;
        for beat in beats {
            let (member, member_epoch, port) = match beat {
                Beat::Join(member, port) => (member, 0, port),
                Beat::Heartbeat(member, epoch) => {
                    (member, epoch.unwrap_or_else(|| epochs[member]), None)
                }
                Beat::Leave(member) => (member, -1, None),
            };
            let resp = heartbeat(
                &handle,
                StreamsGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: member.into(),
                    member_epoch,
                    rebalance_timeout_ms: 1_000,
                    user_endpoint: port.map(|port| RequestEndpoint {
                        host: "localhost".into(),
                        port,
                        ..Default::default()
                    }),
                    topology: (member_epoch == 0).then(|| one_subtopology(false)),
                    ..Default::default()
                },
            )
            .await;
            epochs.insert(member, resp.member_epoch);
            last = Some(resp);
        }
        check!(last == Some(expected), "{name}");
    }
}

/// KIP-1071 reconciliation through the actor, as Kafka's
/// `CurrentAssignmentBuilder` and `scheduleStreamsGroupRebalanceTimeout` run
/// it. `m1` owns both tasks of a two-partition topic and `m2` joins, so the
/// new target moves task 1 to `m2`. `m1` keeps its member epoch until it stops
/// reporting task 1, and it is fenced when it does not revoke within its
/// rebalance timeout. Each row compares the whole responses of a last
/// heartbeat of `m1` and then of `m2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_keeps_its_epoch_until_it_revokes_and_is_fenced_after_its_timeout() {
    use krabka_protocol::owned::common::{
        streams_group_heartbeat_request::task_ids::TaskIds as OwnedTaskIds,
        streams_group_heartbeat_response::task_ids::TaskIds,
    };

    use crate::test_support::FakeMetadataSource;

    let config = StreamsGroupConfig::default();
    let accepted =
        |member_id: &str, member_epoch, tasks: Option<Vec<i32>>| StreamsGroupHeartbeatResponse {
            member_id: member_id.into(),
            status: Some(vec![]),
            active_tasks: tasks.as_ref().map(|partitions| {
                if partitions.is_empty() {
                    vec![]
                } else {
                    vec![TaskIds {
                        subtopology_id: "0".into(),
                        partitions: partitions.clone(),
                        ..Default::default()
                    }]
                }
            }),
            standby_tasks: tasks.as_ref().map(|_| vec![]),
            warmup_tasks: tasks.as_ref().map(|_| vec![]),
            ..super::response::base_resp(codes::NONE, member_epoch, &config)
        };
    // (name, rebalance timeout of m1, m1 revokes task 1, expected last m1 and
    // m2 responses)
    let rows = [
        (
            "revokes before the timeout",
            600_000,
            true,
            accepted("m1", 2, None),
            accepted("m2", 2, Some(vec![1])),
        ),
        (
            "does not revoke, the timeout is not reached",
            600_000,
            false,
            accepted("m1", 1, None),
            accepted("m2", 2, None),
        ),
        (
            "does not revoke within the timeout",
            50,
            false,
            super::response::error_resp(
                codes::UNKNOWN_MEMBER_ID,
                Some("Member m1 is not a member of group g.".into()),
            ),
            accepted("m2", 3, Some(vec![0, 1])),
        ),
    ];

    for (name, rebalance_timeout_ms, revokes, expected_m1, expected_m2) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 2)]))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        let request =
            |member_id: &str, member_epoch, owned: Option<&[i32]>| StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch,
                rebalance_timeout_ms,
                topology: (member_epoch == 0).then(|| one_subtopology(false)),
                active_tasks: owned.map(|partitions| {
                    vec![OwnedTaskIds {
                        subtopology_id: "0".into(),
                        partitions: partitions.to_vec(),
                        ..Default::default()
                    }]
                }),
                standby_tasks: owned.map(|_| vec![]),
                warmup_tasks: owned.map(|_| vec![]),
                ..Default::default()
            };

        let m1 = heartbeat(&handle, request("m1", 0, Some(&[]))).await;
        check!(m1 == accepted("m1", 1, Some(vec![0, 1])), "{name}");
        let m2 = heartbeat(&handle, request("m2", 0, Some(&[]))).await;
        check!(m2 == accepted("m2", 2, Some(vec![])), "{name}");
        let m1 = heartbeat(&handle, request("m1", 1, Some(&[0, 1]))).await;
        check!(m1 == accepted("m1", 1, Some(vec![0])), "{name}");
        if revokes {
            let m1 = heartbeat(&handle, request("m1", 1, Some(&[0]))).await;
            check!(m1 == accepted("m1", 2, None), "{name}");
        }
        // A row whose member never revokes within its rebalance timeout waits
        // for the fence, which the actor runs at the deadline.
        if rebalance_timeout_ms < 1_000 {
            for _ in 0..100 {
                if !describe_member_ids(&handle)
                    .await
                    .contains(&"m1".to_string())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        let owned_by_m1: &[i32] = if revokes { &[0] } else { &[0, 1] };
        let m1_epoch = if revokes { 2 } else { 1 };
        let last_m1 = heartbeat(&handle, request("m1", m1_epoch, Some(owned_by_m1))).await;
        let last_m2 = heartbeat(&handle, request("m2", 2, Some(&[]))).await;
        check!((last_m1, last_m2) == (expected_m1, expected_m2), "{name}");
    }
}

/// Kafka refuses, inside the coordinator, a join whose topology differs from
/// the group topology (`maybeUpdateTopology`) and a heartbeat that owns a task
/// the ready topology does not have (`throwIfRequestContainsInvalidTasks`).
/// Each row compares the whole response and the members of the group
/// afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topology_update_or_an_invalid_owned_task_is_refused() {
    use krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds;

    use crate::test_support::FakeMetadataSource;

    let join = |member_id: &str, epoch, source: &str| {
        let mut topology = one_subtopology(false);
        topology.epoch = epoch;
        topology.subtopologies[0].source_topics = vec![source.into()];
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            active_tasks: Some(vec![]),
            standby_tasks: Some(vec![]),
            warmup_tasks: Some(vec![]),
            topology: Some(topology),
            ..Default::default()
        }
    };
    let owning = |subtopology: &str, partition| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m1".into(),
        member_epoch: 1,
        active_tasks: Some(vec![TaskIds {
            subtopology_id: subtopology.into(),
            partitions: vec![partition],
            ..Default::default()
        }]),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        ..Default::default()
    };
    let rows = [
        (
            "a join with another topology at the same epoch",
            join("m2", 1, "other"),
            "Topology updates are not supported yet.",
        ),
        (
            "a join with the same subtopologies at a higher epoch",
            join("m2", 2, "in"),
            "Topology updates are not supported yet.",
        ),
        (
            "an owned task of an unknown subtopology",
            owning("9", 0),
            "Subtopology 9 does not exist in the topology.",
        ),
        (
            "an owned task out of range",
            owning("0", 5),
            "Task 5 for subtopology 0 is invalid. Number of tasks for this subtopology: 1",
        ),
    ];

    for (name, request, message) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 1), ("other", 2, 1)]))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        check!(
            heartbeat(&handle, join("m1", 1, "in")).await.error_code == codes::NONE,
            "{name}"
        );

        let resp = heartbeat(&handle, request).await;

        check!(
            resp == super::response::error_resp(codes::INVALID_REQUEST, Some(message.into())),
            "{name}"
        );
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(StreamsGroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        let members: Vec<String> = rx
            .await
            .unwrap()
            .members
            .into_iter()
            .map(|m| m.member_id)
            .collect();
        check!(members == vec!["m1".to_string()], "{name}");
    }
}

/// KIP-1071 static membership, as Kafka's
/// `getOrMaybeCreateStaticStreamsGroupMember` and
/// `streamsGroupStaticMemberGroupLeave` run it. Member `m1` holds instance id
/// `i1` in a group with a one-partition topic. Each row sends its heartbeats
/// and compares the whole last response and the members afterwards, as
/// `(member id, member epoch, active tasks)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_static_member_follows_kafka_static_membership() {
    use crate::test_support::FakeMetadataSource;

    let config = StreamsGroupConfig {
        max_size: 2,
        ..StreamsGroupConfig::default()
    };
    let join = |member_id: &str, instance_id: &str| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch: 0,
        instance_id: Some(instance_id.into()),
        rebalance_timeout_ms: 1_000,
        active_tasks: Some(vec![]),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        topology: Some(one_subtopology(false)),
        ..Default::default()
    };
    let beat = |member_id: &str, member_epoch, instance_id: &str| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        instance_id: Some(instance_id.into()),
        ..Default::default()
    };
    let assigned = |member_id: &str, member_epoch| StreamsGroupHeartbeatResponse {
        member_id: member_id.into(),
        status: Some(vec![]),
        active_tasks: Some(vec![
            krabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds {
                subtopology_id: "0".into(),
                partitions: vec![0],
                ..Default::default()
            },
        ]),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        ..super::response::base_resp(codes::NONE, member_epoch, &config)
    };
    let rows = [
        (
            "a static member leaves for a while and keeps its tasks",
            vec![beat("m1", -2, "i1")],
            StreamsGroupHeartbeatResponse {
                member_id: "m1".into(),
                member_epoch: -2,
                status: Some(vec![]),
                ..Default::default()
            },
            vec![("m1".to_string(), -2, vec![0])],
        ),
        (
            "a new member replaces the released static member",
            vec![beat("m1", -2, "i1"), join("m2", "i1")],
            assigned("m2", 1),
            vec![("m2".to_string(), 1, vec![0])],
        ),
        (
            "a join with an instance id that a member still holds",
            vec![join("m2", "i1")],
            super::response::error_resp(
                codes::UNRELEASED_INSTANCE_ID,
                Some(
                    "Static member m2 with instance id i1 cannot join the group because the \
                     instance id is owned by m1 member."
                        .into(),
                ),
            ),
            vec![("m1".to_string(), 1, vec![0])],
        ),
        (
            "a heartbeat with the instance id of another member",
            vec![beat("m2", 5, "i1")],
            super::response::error_resp(
                codes::FENCED_INSTANCE_ID,
                Some("Static member m2 with instance id i1 was fenced by member m1.".into()),
            ),
            vec![("m1".to_string(), 1, vec![0])],
        ),
        (
            "a heartbeat with an unknown instance id",
            vec![beat("m1", 1, "i9")],
            super::response::error_resp(
                codes::UNKNOWN_MEMBER_ID,
                Some("Instance id i9 is unknown.".into()),
            ),
            vec![("m1".to_string(), 1, vec![0])],
        ),
        (
            "a released static member does not count against a full group",
            vec![join("m3", "i3"), beat("m1", -2, "i1"), join("m2", "i1")],
            assigned("m2", 2),
            vec![
                ("m2".to_string(), 2, vec![0]),
                ("m3".to_string(), 2, vec![]),
            ],
        ),
        (
            "a replacement takes over a member id that another member holds",
            vec![join("m2", "i2"), beat("m1", -2, "i1"), join("m2", "i1")],
            assigned("m2", 3),
            vec![("m2".to_string(), 3, vec![0])],
        ),
        (
            "a static member leaves for good",
            vec![beat("m1", -1, "i1")],
            StreamsGroupHeartbeatResponse {
                member_id: "m1".into(),
                member_epoch: -1,
                status: Some(vec![]),
                ..Default::default()
            },
            vec![],
        ),
    ];

    for (name, beats, expected, members) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 1)]))
                .build(),
        );
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            ShareGroupConfig::default(),
            Arc::new(EmptyMetadata),
            Arc::new(InMemoryOffsetsLog::default()),
            config.clone(),
        ));
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        check!(
            heartbeat(&handle, join("m1", "i1")).await == assigned("m1", 1),
            "{name}"
        );

        let mut last = None;
        for request in beats {
            last = Some(heartbeat(&handle, request).await);
        }

        check!(last == Some(expected), "{name}");
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(StreamsGroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        let mut described: Vec<(String, i32, Vec<i32>)> = rx
            .await
            .unwrap()
            .members
            .into_iter()
            .map(|member| {
                let active = member.active.get("0").cloned().unwrap_or_default();
                (member.member_id, member.member_epoch, active)
            })
            .collect();
        described.sort();
        check!(described == members, "{name}");
    }
}
