//! Unit tests that drive the streams-group actor through its handle: the
//! heartbeat epoch sequence with no connected `MetadataSource`, and the
//! resolution of a persisted per-group config override.

use {
    super::{
        test_support::{
            coordinator_with_log, describe, heartbeat_result_at, make_coordinator, member_request,
            response_tasks, undelayed,
        },
        *,
    },
    crate::{
        api_catalog::UnstableApiVersions,
        coordinator::unified::{
            GroupCoordinator,
            offsets_log::fake::InMemoryOffsetsLog,
            persistence::parse_key,
            streams::{
                config::KEY_NUM_STANDBY_REPLICAS,
                persistence::{
                    DescriptionEpochs, LastAssignmentConfig, PendingStreamsRecords,
                    StoredSubtopology, StoredTopicInfo, StreamsGroupMetadataValue,
                },
                state::{StreamsGroupState, StreamsMemberState},
            },
            test_support::{assert_single_member_limit_with_log, heartbeat_write_failures},
        },
        test_support::{
            FakeMetadataSource, broker_registration, source_to_repartition, streams_join_request,
        },
    },
    assert2::{assert, check},
    krabka_protocol::{
        owned::{
            common::{
                streams_group_heartbeat_request::task_ids::TaskIds as RequestTaskIds,
                streams_group_heartbeat_response::{
                    status::Status, task_ids::TaskIds as ResponseTaskIds,
                },
            },
            streams_group_heartbeat_request::{MAX_VERSION, Topology},
        },
        records::RecordBatch,
    },
    std::{collections::BTreeMap, sync::Arc, time::Duration},
    tokio::{task::yield_now, time::sleep},
};

fn partition_indices(partitions: Option<Vec<i32>>) -> Option<Vec<krabka_ids::PartitionIndex>> {
    partitions.map(|partitions| {
        partitions
            .into_iter()
            .map(krabka_ids::PartitionIndex)
            .collect()
    })
}

krabka_macros::single_replica_partition_fixture!(partition_record);

fn epoch_five_member() -> StreamsMemberState {
    let mut member = StreamsMemberState::joining("m1", "client", "/127.0.0.1");
    member.member_epoch = 5;
    member
}

/// A metadata source whose image holds only the topic `in`.
fn input_topic_source(partitions: i32) -> Arc<FakeMetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image_of(None, &[ImageTopic {
                name: "in",
                id: 1,
                partitions,
            }]))
            .build(),
    )
}

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

/// The member ids that the group holds, sorted.
async fn describe_member_ids(handle: &StreamsGroupActorHandle) -> Vec<String> {
    let mut ids: Vec<String> = describe(handle)
        .await
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
    heartbeat_result_at(handle, req, MAX_VERSION).await
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
    let resp = heartbeat(&handle, member_request("m1", 0)).await;
    check!(resp.error_code == codes::NONE);
    check!(resp.member_id == "m1");
    // No metadata source / no topology → NotReady, empty assignment, but the
    // member still advances to the group epoch, which the join bumps past
    // the initial epoch 1.
    check!(resp.member_epoch == 2);
    check!(resp.active_tasks == Some(vec![]));
    check!(resp.standby_tasks == Some(vec![]));
    check!(resp.warmup_tasks == Some(vec![]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_heartbeat_at_right_epoch_accepted() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(&handle, member_request("m1", 0)).await;
    assert!(join.error_code == codes::NONE);
    let epoch = join.member_epoch;
    let batches = log.batches().await.len();
    let resp = heartbeat(&handle, member_request("m1", epoch)).await;
    assert!(resp.error_code == codes::NONE);
    assert!(resp.member_epoch == epoch);
    // A heartbeat that changes nothing writes nothing, as Kafka's
    // `streamsGroupHeartbeat` adds no record for an unchanged member.
    assert!(log.batches().await.len() == batches);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_limit_rejects_only_new_members() {
    assert_single_member_limit_with_log(
        |log| {
            coordinator_with_log(
                StreamsGroupConfig {
                    max_size: 1,
                    ..undelayed()
                },
                log,
            )
            .get_or_create_streams("g")
        },
        member_request,
        heartbeat,
    )
    .await;
}

/// Kafka 4.3.1's `throwIfStreamsGroupIsFull(group)` runs on every join and
/// counts a member that is already in the group, so a rejoin at epoch 0 to a
/// full group is refused. Trunk exempts the known member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_known_member_that_rejoins_a_full_group_is_refused_by_4_3_1_only() {
    use crate::api_catalog::UnstableApiVersions::{Disabled, Enabled};

    for (unstable, expected) in [
        (Disabled, codes::GROUP_MAX_SIZE_REACHED),
        (Enabled, codes::NONE),
    ] {
        let coord = coordinator_with_log(
            StreamsGroupConfig {
                max_size: 1,
                unstable_api_versions: unstable,
                ..undelayed()
            },
            Arc::new(InMemoryOffsetsLog::default()),
        );
        let handle = coord.get_or_create_streams("g");
        let join = member_request("m1", 0);

        let joined = heartbeat(&handle, join.clone()).await;
        let rejoined = heartbeat(&handle, join).await;

        check!(joined.error_code == codes::NONE, "{unstable:?}");
        check!(rejoined.error_code == expected, "{unstable:?}");
    }
}

/// Kafka's `streamsGroupLeave` answers `UNKNOWN_MEMBER_ID` to a member that
/// the group does not have, and writes no record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_of_an_unknown_member_is_refused_and_writes_nothing() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let joined = heartbeat(&handle, member_request("m1", 0)).await;
    check!(joined.error_code == codes::NONE);
    let before = log.batches().await;

    let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m9".into(),
        member_epoch: -1,
        ..Default::default()
    })
    .await;

    check!(
        resp == super::response::error_resp(
            codes::UNKNOWN_MEMBER_ID,
            Some("Member m9 is not a member of group g.".into())
        )
    );
    check!(log.batches().await == before);
}

/// A heartbeat that the coordinator refuses on a group that holds nothing
/// does not close the actor while another message is queued behind it: both
/// get their answer, and the group that the second one joins stays.
#[tokio::test]
async fn a_refused_heartbeat_with_a_queued_join_answers_both() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    // On this single-threaded runtime the actor runs only once the test
    // awaits, so both heartbeats are in the mailbox before it handles one.
    let (refused_tx, refused_rx) = oneshot::channel();
    let (joined_tx, joined_rx) = oneshot::channel();
    for (request, reply) in [
        (
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m9".into(),
                member_epoch: -1,
                ..Default::default()
            },
            refused_tx,
        ),
        (member_request("m1", 0), joined_tx),
    ] {
        handle
            .tx
            .try_send(StreamsGroupActorMessage::Heartbeat {
                request: Box::new(request),
                version: MAX_VERSION,
                client_id: "client".into(),
                client_host: "/127.0.0.1".into(),
                reply,
            })
            .expect("a mailbox with room for two heartbeats");
    }

    let refused = refused_rx
        .await
        .expect("an answer to the refused heartbeat");
    let joined = joined_rx.await.expect("an answer to the queued join");

    check!(
        refused.response
            == super::response::error_resp(
                codes::UNKNOWN_MEMBER_ID,
                Some("Member m9 is not a member of group g.".into())
            )
    );
    check!(joined.response.error_code == codes::NONE);
    check!(joined.response.member_epoch == 2);
    // The actor still reads its mailbox.
    let steady = heartbeat(&handle, member_request("m1", 2)).await;
    check!(steady.error_code == codes::NONE);
    check!(coord.streams_groups.contains_key("g"));
}

/// What the mailbox of a group that refused a leave answered, and what the
/// group left behind.
#[derive(Debug, PartialEq)]
struct RefusedDrain {
    refused: Result<StreamsGroupHeartbeatResponse, oneshot::error::RecvError>,
    failed: Result<StreamsGroupHeartbeatResponse, oneshot::error::RecvError>,
    queued: Result<StreamsGroupHeartbeatResponse, oneshot::error::RecvError>,
    described_members: Result<Vec<String>, oneshot::error::RecvError>,
    batches: usize,
    registered: bool,
}

/// A heartbeat message for `request`, and the receiver of its answer.
fn heartbeat_message(
    request: StreamsGroupHeartbeatRequest,
) -> (
    StreamsGroupActorMessage,
    oneshot::Receiver<StreamsHeartbeatResult>,
) {
    let (reply, answer) = oneshot::channel();
    let message = StreamsGroupActorMessage::Heartbeat {
        request: Box::new(request),
        version: MAX_VERSION,
        client_id: "client".into(),
        client_host: "/127.0.0.1".into(),
        reply,
    };
    (message, answer)
}

/// Kafka's `CoordinatorRuntime` reverts the shard to its last committed
/// snapshot when a write fails, so nothing after the failed write sees what it
/// changed.
///
/// Here a leave of an unknown member is refused on an empty group with nothing
/// queued behind it, so the actor closes its mailbox and drains what raced in:
/// a join whose write fails, then a join and a describe. Each was sent with a
/// permit reserved before the close, as `Sender::send` reserves one before it
/// enqueues. The join and the describe after the failed write are not answered
/// from it, nothing is written on top of it, and the group, which nothing
/// persisted, is forgotten.
#[tokio::test]
async fn a_failed_write_in_the_refused_heartbeat_drain_ends_on_the_persisted_state() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    log.fail_next_append(None);

    let (leave, refused) = heartbeat_message(member_request("m9", -1));
    handle.tx.try_send(leave).unwrap();
    let permits = [
        handle.tx.reserve().await.unwrap(),
        handle.tx.reserve().await.unwrap(),
        handle.tx.reserve().await.unwrap(),
    ];
    while !handle.tx.is_closed() {
        yield_now().await;
    }
    let (failing_join, failed) = heartbeat_message(member_request("m1", 0));
    let (join, queued) = heartbeat_message(member_request("m2", 0));
    let (reply, described) = oneshot::channel();
    let messages = [failing_join, join, StreamsGroupActorMessage::Describe {
        reply,
    }];
    for (permit, message) in permits.into_iter().zip(messages) {
        permit.send(message);
    }

    let drain = RefusedDrain {
        refused: refused.await.map(|result| result.response),
        failed: failed.await.map(|result| result.response),
        queued: queued.await.map(|result| result.response),
        described_members: described.await.map(|view| {
            view.members
                .into_iter()
                .map(|member| member.member_id)
                .collect()
        }),
        batches: log.batches().await.len(),
        registered: coord.find_streams("g").is_some(),
    };

    let (_, closed) = oneshot::channel::<()>();
    let recv_error = closed.await.unwrap_err();
    assert!(
        drain
            == RefusedDrain {
                refused: Ok(super::response::error_resp(
                    codes::UNKNOWN_MEMBER_ID,
                    Some("Member m9 is not a member of group g.".into())
                )),
                failed: Ok(super::response::error_resp(
                    codes::COORDINATOR_LOAD_IN_PROGRESS,
                    None
                )),
                queued: Err(recv_error.clone()),
                described_members: Err(recv_error),
                batches: 0,
                registered: false,
            }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_epoch_is_rejected() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(&handle, member_request("m1", 0)).await;
    assert!(join.member_epoch == 2);
    let resp = heartbeat(&handle, member_request("m1", 99)).await;
    assert!(resp.error_code == codes::FENCED_MEMBER_EPOCH);
}

/// A heartbeat whose write fails answers the code of the failure, and the
/// failed write leaves no partial batch. A write that is not committed answers
/// what Kafka's `CoordinatorOperationExceptionHelper` answers for it, so that
/// a member whose join the coordinator never committed looks the coordinator
/// up again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_write_answers_its_code_and_writes_no_partial_batch() {
    for case in heartbeat_write_failures() {
        let fixture = case.coordinator(make_coordinator);
        let handle = fixture.coordinator.get_or_create_streams("g");

        let response = heartbeat(&handle, member_request("m1", 0)).await;

        fixture
            .check_refused(&response, |error_code| {
                super::response::error_resp(error_code, None)
            })
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_removes_member() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(&handle, StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: String::new(),
        member_epoch: 0,
        ..Default::default()
    })
    .await;
    let mid = join.member_id.clone();
    let pre_leave = log.batches().await.len();

    let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: mid,
        member_epoch: -1,
        ..Default::default()
    })
    .await;
    assert!(resp.error_code == codes::NONE);
    assert!(resp.member_epoch == -1);
    let batches = log.batches().await;
    assert!(batches.len() == pre_leave + 1);
    // Kafka's `streamsGroupFenceMember`: the member's current assignment,
    // target and member tombstones, and the group epoch bumped with the
    // group's metadata as it was, in one batch, and no target.
    let seed = coord.cached_streams_seed("g").expect("the group's records");
    let expected = PendingStreamsRecords {
        member_metadata: vec![(join.member_id.clone(), None)],
        target_per_member: vec![(join.member_id.clone(), None)],
        current_per_member: vec![(join.member_id.clone(), None)],
        group_metadata: Some(StreamsGroupMetadataValue {
            epoch: join.member_epoch + 1,
            metadata_hash: seed.metadata_hash,
            validated_topology_epoch: seed.validated_topology_epoch,
            last_assignment_configs: Some(
                seed.last_assignment_configs
                    .iter()
                    .map(|(key, value)| LastAssignmentConfig {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            ),
            description: DescriptionEpochs::default(),
        }),
        ..Default::default()
    }
    .into_batch("g", 0)
    .unwrap();
    let key_values = |batch: &RecordBatch| -> Vec<_> {
        batch
            .records
            .iter()
            .map(|record| (record.key.clone(), record.value.clone()))
            .collect()
    };
    assert!(key_values(&batches[batches.len() - 1]) == key_values(&expected));
    assert!(
        (seed.group_epoch, seed.assignment_epoch) == (join.member_epoch + 1, join.member_epoch)
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

/// A topic of a test metadata image: its name, the byte that fills its topic
/// id, and its partition count.
#[derive(Clone, Copy)]
struct ImageTopic<'a> {
    name: &'a str,
    id: u8,
    partitions: i32,
}

/// A member as the group describes it: its id, epoch and active partitions of
/// subtopology `0`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DescribedMember {
    member_id: String,
    member_epoch: i32,
    active: Vec<i32>,
}

/// A status entry of a heartbeat response: its code and detail.
#[derive(Debug, PartialEq, Eq)]
struct StatusEntry {
    code: i8,
    detail: String,
}

/// Builds the metadata image of one broker, `broker_rack` its rack, that
/// holds each of `topics`.
fn image_of(
    broker_rack: Option<&str>,
    topics: &[ImageTopic<'_>],
) -> krabka_metadata::MetadataImage {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord, TopicRecord};

    let broker = krabka_audit::NodeId(1);
    let mut records = vec![MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            rack: broker_rack.map(str::to_owned),
            ..broker_registration(broker)
        },
    )];
    for topic in topics {
        let ImageTopic {
            name,
            id,
            partitions,
        } = *topic;
        records.push(MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: uuid::Uuid::from_bytes([id; 16]),
            partitions,
            replication_factor: 1,
        }));
        for partition in 0..partitions {
            records.push(MetadataRecord::V1Partition(partition_record(
                name,
                krabka_ids::PartitionIndex(partition),
                broker,
            )));
        }
    }
    krabka_metadata::MetadataImage::from_records(uuid::Uuid::nil(), &records)
}

#[derive(Clone, Copy, Default)]
enum TopologyStorage {
    #[default]
    Stateless,
    StoreChangelog,
}

#[derive(Clone, Copy, Default)]
struct SingleSubtopologySetup {
    storage: TopologyStorage,
}

/// A topology with one subtopology `0` that reads `in` and, with a state store,
/// keeps the changelog topic `store-changelog`.
#[bon::builder]
fn one_subtopology(#[builder(default)] storage: TopologyStorage) -> Topology {
    use krabka_protocol::owned::{
        common::streams_group_heartbeat_request::topic_info::TopicInfo,
        streams_group_heartbeat_request::{Subtopology, Topology},
    };

    let setup = SingleSubtopologySetup { storage };
    Topology {
        epoch: 1,
        subtopologies: vec![Subtopology {
            subtopology_id: "0".into(),
            source_topics: vec!["in".into()],
            state_changelog_topics: if matches!(setup.storage, TopologyStorage::StoreChangelog) {
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

enum HeartbeatChange {
    Image(Box<krabka_metadata::MetadataImage>),
    Rack(&'static str),
    Process(&'static str),
}

struct HeartbeatRow {
    name: &'static str,
    initial: krabka_metadata::MetadataImage,
    storage: TopologyStorage,
    change: HeartbeatChange,
    /// The owned active tasks of the heartbeat, when it reports them.
    owned_active: Option<Vec<i32>>,
    epoch: i32,
    /// The active tasks, when the response sends the task lists.
    active: Option<Vec<i32>>,
    status: Option<Vec<Status>>,
}

/// An image that holds only the topic `in`.
fn in_image(partitions: i32) -> krabka_metadata::MetadataImage {
    image_of(None, &[ImageTopic {
        name: "in",
        id: 1,
        partitions,
    }])
}

/// Rows for [`a_heartbeat_after_a_topic_or_member_change_recomputes_the_assignment`].
fn heartbeat_change_rows() -> Vec<HeartbeatRow> {
    use {
        crate::coordinator::unified::streams::topology::status,
        krabka_protocol::owned::common::streams_group_heartbeat_response::status::Status,
    };

    vec![
        HeartbeatRow {
            name: "the missing source topic is created",
            initial: image_of(None, &[]),
            storage: TopologyStorage::Stateless,
            change: HeartbeatChange::Image(Box::new(in_image(2))),
            owned_active: None,
            epoch: 3,
            active: Some(vec![0, 1]),
            status: Some(vec![]),
        },
        HeartbeatRow {
            name: "partitions are added to the source topic",
            initial: in_image(1),
            storage: TopologyStorage::Stateless,
            change: HeartbeatChange::Image(Box::new(in_image(2))),
            owned_active: Some(vec![0]),
            epoch: 3,
            active: Some(vec![0, 1]),
            status: Some(vec![]),
        },
        HeartbeatRow {
            name: "the source topic is deleted",
            initial: in_image(2),
            storage: TopologyStorage::Stateless,
            change: HeartbeatChange::Image(Box::new(image_of(None, &[]))),
            owned_active: Some(vec![]),
            epoch: 3,
            active: Some(vec![]),
            status: Some(vec![Status {
                status_code: status::MISSING_SOURCE_TOPICS,
                status_detail: "Source topics in are missing.".into(),
                ..Default::default()
            }]),
        },
        HeartbeatRow {
            name: "the member sends a new rack id",
            initial: in_image(1),
            storage: TopologyStorage::Stateless,
            change: HeartbeatChange::Rack("rack-b"),
            owned_active: Some(vec![0]),
            epoch: 3,
            active: None,
            status: Some(vec![]),
        },
        HeartbeatRow {
            name: "the member sends a new process id",
            initial: in_image(1),
            storage: TopologyStorage::Stateless,
            change: HeartbeatChange::Process("process-b"),
            owned_active: Some(vec![0]),
            epoch: 3,
            active: None,
            status: Some(vec![]),
        },
        HeartbeatRow {
            name: "the internal topic is created",
            initial: in_image(1),
            storage: TopologyStorage::StoreChangelog,
            change: HeartbeatChange::Image(Box::new(image_of(None, &[
                ImageTopic {
                    name: "in",
                    id: 1,
                    partitions: 1,
                },
                ImageTopic {
                    name: "store-changelog",
                    id: 2,
                    partitions: 1,
                },
            ]))),
            owned_active: Some(vec![]),
            epoch: 3,
            active: Some(vec![0]),
            status: Some(vec![]),
        },
    ]
}

/// Kafka refreshes the topic metadata of a streams group on the next
/// heartbeat after a change (`onMetadataUpdate`, `hasMetadataExpired`,
/// `computeMetadataHash`) and bumps the group epoch when the hash or the
/// member metadata changed (`hasStreamsMemberMetadataChanged`). Each row joins
/// one member, applies one change, sends one heartbeat at the member epoch, and
/// compares the whole response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_heartbeat_after_a_topic_or_member_change_recomputes_the_assignment() {
    let rows = heartbeat_change_rows();

    for row in rows {
        let source = Arc::new(FakeMetadataSource::builder().image(row.initial).build());
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(Arc::<FakeMetadataSource>::clone(&source));
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
        let joined = heartbeat(&handle, StreamsGroupHeartbeatRequest {
            topology: Some(one_subtopology().storage(row.storage).call()),
            ..request(0, "rack-a", "process-a")
        })
        .await;
        check!(joined.member_epoch == 2, "{}", row.name);

        let (rack, process) = match row.change {
            HeartbeatChange::Image(image) => {
                source.set_image(*image);
                ("rack-a", "process-a")
            }
            HeartbeatChange::Rack(rack) => (rack, "process-a"),
            HeartbeatChange::Process(process) => ("rack-a", process),
        };
        let owned = row.owned_active.map(|partitions| {
            vec![RequestTaskIds {
                subtopology_id: "0".into(),
                partitions,
                ..Default::default()
            }]
        });
        let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
            standby_tasks: owned.as_ref().map(|_| vec![]),
            warmup_tasks: owned.as_ref().map(|_| vec![]),
            active_tasks: owned,
            ..request(joined.member_epoch, rack, process)
        })
        .await;

        let expected = super::test_support::expected_active_response()
            .epoch(row.epoch)
            .maybe_status(row.status)
            .maybe_active(partition_indices(row.active))
            .call();
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
    use crate::coordinator::unified::{persistence::Key, streams::persistence::StreamsGroupKey};

    let source = input_topic_source(2);
    let (coord, log) = make_coordinator();
    coord.set_metadata_source(Arc::<FakeMetadataSource>::clone(&source));
    let handle = coord.get_or_create_streams("g");
    let request = |member_id: &str, member_epoch| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        rebalance_timeout_ms: 1_000,
        topology: (member_epoch == 0).then(|| one_subtopology().call()),
        ..Default::default()
    };
    let m1 = heartbeat(&handle, request("m1", 0)).await;
    let m2 = heartbeat(&handle, request("m2", 0)).await;
    check!((m1.member_epoch, m2.member_epoch) == (2, 3));

    source.set_image(in_image(4));
    let resp = heartbeat(&handle, request("m1", 2)).await;
    check!(resp.member_epoch == 4);

    let batches = log.batches().await;
    let last = batches.last().expect("the heartbeat wrote a batch");
    let mut targets: Vec<String> = last
        .records
        .iter()
        .filter_map(|record| {
            let key = record.key.as_deref()?;
            match parse_key(key) {
                Ok(Key::Streams(StreamsGroupKey::TargetAssignmentMember { member_id, .. })) => {
                    Some(member_id)
                }
                _ => None,
            }
        })
        .collect();
    targets.sort();
    check!(targets == vec!["m1".to_owned(), "m2".to_owned()]);
}

/// A seeded group whose internal topics are missing asks for them again on
/// the next heartbeat, as Kafka configures the topology of a loaded group on
/// its first heartbeat and returns the internal topics to create. The seed
/// carries the metadata hash, so the hash alone does not trigger it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seeded_group_asks_for_its_missing_internal_topics_again() {
    use {
        crate::coordinator::unified::streams::topology::{InternalTopicSpec, status},
        krabka_protocol::owned::common::streams_group_heartbeat_response::status::Status,
    };

    let image = || in_image(1);
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
            topology: Some(
                one_subtopology()
                    .storage(TopologyStorage::StoreChangelog)
                    .call(),
            ),
            ..Default::default()
        },
    )
    .await;
    check!(joined.member_epoch == 2);
    let seed = before
        .cached_streams_seed("g")
        .expect("the join cached a seed");

    let (after, _log) = make_coordinator();
    after.set_metadata_source(Arc::new(
        FakeMetadataSource::builder().image(image()).build(),
    ));
    after.update_streams_cache("g", seed);
    let result = heartbeat_result(&after.get_or_create_streams("g"), member_request("m1", 2)).await;

    check!(
        result.creatable_topics
            == vec![InternalTopicSpec {
                name: "store-changelog".into(),
                partitions: 1,
                replication_factor: 0,
                configs: BTreeMap::new(),
            }]
    );
    let expected = StreamsGroupHeartbeatResponse {
        member_id: "m1".into(),
        status: Some(vec![Status {
            status_code: status::MISSING_INTERNAL_TOPICS,
            status_detail: "Internal topics are missing: store-changelog".into(),
            ..Default::default()
        }]),
        ..super::response::base_resp(codes::NONE, 2, &undelayed())
    };
    check!(result.response == expected);
}

/// The `__consumer_offsets` replay spawns the actor of a loaded group before
/// the broker connects the coordinator's metadata source. The actor still
/// reconciles against the metadata image once the source is there, as Kafka's
/// coordinator gives every loaded group the image: a member that joins after a
/// restart gets the task that the loaded member revokes, and the loaded member
/// keeps the other one.
///
/// Each row is one heartbeat of the exchange after the restart and the whole
/// response it gets. A group that never sees the source assigns nothing, so
/// the loaded member revokes both of its tasks instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_loaded_before_the_metadata_source_connects_assigns_its_tasks() {
    use krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds as OwnedTaskIds;

    struct Row {
        member_id: &'static str,
        member_epoch: i32,
        /// The active tasks that the heartbeat reports as owned.
        owned: Vec<i32>,
        epoch: i32,
        /// The active tasks, when the response sends the task lists.
        active: Option<Vec<i32>>,
    }

    let source = || input_topic_source(2);
    let request = |member_id: &str, member_epoch, owned: Vec<i32>| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        process_id: Some(format!("process-{member_id}")),
        rebalance_timeout_ms: 60_000,
        topology: (member_epoch == 0).then(|| one_subtopology().call()),
        active_tasks: Some(if owned.is_empty() {
            vec![]
        } else {
            vec![OwnedTaskIds {
                subtopology_id: "0".into(),
                partitions: owned,
                ..Default::default()
            }]
        }),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        ..Default::default()
    };
    let expected = |member_id: &str, epoch, active: Option<Vec<i32>>| {
        super::test_support::expected_active_response()
            .member_id(member_id)
            .epoch(epoch)
            .maybe_status(Some(vec![]))
            .maybe_active(active.map(|partitions| {
                partitions
                    .into_iter()
                    .map(krabka_ids::PartitionIndex)
                    .collect()
            }))
            .call()
    };

    let (before, _log) = make_coordinator();
    before.set_metadata_source(source());
    let joined = heartbeat(&before.get_or_create_streams("g"), request("m1", 0, vec![])).await;
    check!(joined == expected("m1", 2, Some(vec![0, 1])));
    let seed = before
        .cached_streams_seed("g")
        .expect("the join cached a seed");

    // The order of `Broker::start`: the replay seeds the group and spawns its
    // actor, and only then does the broker connect the metadata source.
    let (after, _log) = make_coordinator();
    after.streams_seeds.insert("g".into(), seed);
    after.finalize_bootstrap();
    after.set_metadata_source(source());
    let handle = after
        .find_streams("g")
        .expect("the replay spawned the actor");

    let rows = [
        Row {
            member_id: "m2",
            member_epoch: 0,
            owned: vec![],
            epoch: 3,
            active: Some(vec![]),
        },
        Row {
            member_id: "m1",
            member_epoch: 2,
            owned: vec![0, 1],
            epoch: 2,
            active: Some(vec![0]),
        },
        Row {
            member_id: "m1",
            member_epoch: 2,
            owned: vec![0],
            epoch: 3,
            active: None,
        },
        Row {
            member_id: "m2",
            member_epoch: 3,
            owned: vec![],
            epoch: 3,
            active: Some(vec![1]),
        },
    ];
    for (step, row) in rows.into_iter().enumerate() {
        let resp = heartbeat(&handle, request(row.member_id, row.member_epoch, row.owned)).await;
        check!(
            resp == expected(row.member_id, row.epoch, row.active),
            "step {step}"
        );
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
    use krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds as OwnedTaskIds;

    let accepted = accepted_tasks;
    // (name, rebalance timeout of m1, m1 revokes task 1, expected last m1 and
    // m2 responses)
    let rows = [
        (
            "revokes before the timeout",
            600_000,
            true,
            accepted("m1", 3, None),
            accepted("m2", 3, Some(&[1])),
        ),
        (
            "does not revoke, the timeout is not reached",
            600_000,
            false,
            accepted("m1", 2, None),
            accepted("m2", 3, None),
        ),
        (
            "does not revoke within the timeout",
            50,
            false,
            super::response::error_resp(
                codes::UNKNOWN_MEMBER_ID,
                Some("Member m1 is not a member of group g.".into()),
            ),
            accepted("m2", 4, Some(&[0, 1])),
        ),
    ];

    for (name, rebalance_timeout_ms, revokes, expected_m1, expected_m2) in rows {
        let source = input_topic_source(2);
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        let request =
            |member_id: &str, member_epoch, owned: Option<&[i32]>| StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch,
                rebalance_timeout_ms,
                topology: (member_epoch == 0).then(|| one_subtopology().call()),
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
        check!(m1 == accepted("m1", 2, Some(&[0, 1])), "{name}");
        let m2 = heartbeat(&handle, request("m2", 0, Some(&[]))).await;
        check!(m2 == accepted("m2", 3, Some(&[])), "{name}");
        let m1 = heartbeat(&handle, request("m1", 2, Some(&[0, 1]))).await;
        check!(m1 == accepted("m1", 2, Some(&[0])), "{name}");
        if revokes {
            let m1 = heartbeat(&handle, request("m1", 2, Some(&[0]))).await;
            check!(m1 == accepted("m1", 3, None), "{name}");
        }
        // A row whose member never revokes within its rebalance timeout waits
        // for the fence, which the actor runs at the deadline.
        if rebalance_timeout_ms < 1_000 {
            for _ in 0..100 {
                if !describe_member_ids(&handle)
                    .await
                    .contains(&"m1".to_owned())
                {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        }

        let owned_by_m1: &[i32] = if revokes { &[0] } else { &[0, 1] };
        let m1_epoch = if revokes { 3 } else { 2 };
        let last_m1 = heartbeat(&handle, request("m1", m1_epoch, Some(owned_by_m1))).await;
        let last_m2 = heartbeat(&handle, request("m2", 3, Some(&[]))).await;
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

    let join = |member_id: &str, epoch, source: &str| {
        let mut topology = one_subtopology().call();
        topology.epoch = epoch;
        topology.subtopologies[0].source_topics = vec![source.into()];
        streams_join_request()
            .topology(topology)
            .member_id(member_id)
            .call()
    };
    let owning = |subtopology: &str, partition| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m1".into(),
        member_epoch: 2,
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
                .image(image_of(None, &[
                    ImageTopic {
                        name: "in",
                        id: 1,
                        partitions: 1,
                    },
                    ImageTopic {
                        name: "other",
                        id: 2,
                        partitions: 1,
                    },
                ]))
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
        let members: Vec<String> = describe(&handle)
            .await
            .members
            .into_iter()
            .map(|m| m.member_id)
            .collect();
        check!(members == vec!["m1".to_owned()], "{name}");
    }
}

/// A stored subtopology `id` that reads the source topic `source` and the
/// repartition topics `repartition`.
fn stored_subtopology(id: &str, source: &str, repartition: &[&str]) -> StoredSubtopology {
    StoredSubtopology {
        subtopology_id: id.into(),
        source_topics: vec![source.into()],
        source_topic_regex: vec![],
        repartition_sink_topics: vec![],
        state_changelog_topics: vec![],
        repartition_source_topics: repartition
            .iter()
            .map(|name| StoredTopicInfo {
                name: (*name).into(),
                partitions: 4,
                replication_factor: -1,
                topic_configs: vec![],
            })
            .collect(),
        copartition_groups: vec![],
    }
}

/// KIP-1251: Kafka 4.3.1's `StreamsGroup.createAssignmentEpochValidator`.
/// Member `m1` is at epoch 5. It holds active tasks 0 and 1 of subtopology
/// `0`, assigned at epochs 3 and 5, task 2 pending revocation from epoch 4,
/// and task 3 with no recorded epoch. Subtopology `0` reads `in` and the
/// repartition topic `rep`; subtopology `1` reads `other`, where `m1` holds
/// nothing. Each row commits at epoch 4.
#[test]
fn older_epoch_commit_checks_each_tasks_assignment_epoch() {
    /// A partition that the commit writes.
    struct CommittedPartition {
        topic: &'static str,
        partition: i32,
    }

    /// A row: its label, the partitions the commit writes, and the answer.
    struct CommitRow {
        row: &'static str,
        committed: &'static [CommittedPartition],
        expected: Result<(), i16>,
    }

    let topology = StreamsGroupTopologyValue {
        epoch: 1,
        subtopologies: vec![
            stored_subtopology("0", "in", &["rep"]),
            stored_subtopology("1", "other", &[]),
        ],
    };
    let mut group = StreamsGroupState::new("g");
    let mut member = epoch_five_member();
    member.active = maplit::btreemap! {"0".to_owned() => vec![0, 1, 3]};
    member.active_pending_revocation = maplit::btreemap! {"0".to_owned() => vec![2]};
    member.active_epochs = maplit::btreemap! {
        ("0".to_owned(), 0) => 3,
        ("0".to_owned(), 1) => 5,
        ("0".to_owned(), 2) => 4,
    };
    group.members.insert("m1".into(), member);

    // (row, committed (topic, partition)s, expected)
    let rows: [CommitRow; 10] = [
        CommitRow {
            row: "no partitions",
            committed: &[],
            expected: Ok(()),
        },
        CommitRow {
            row: "assigned before the epoch",
            committed: &[CommittedPartition {
                topic: "in",
                partition: 0,
            }],
            expected: Ok(()),
        },
        CommitRow {
            row: "assigned after the epoch",
            committed: &[CommittedPartition {
                topic: "in",
                partition: 1,
            }],
            expected: Err(codes::STALE_MEMBER_EPOCH),
        },
        CommitRow {
            row: "pending revocation at the epoch",
            committed: &[CommittedPartition {
                topic: "in",
                partition: 2,
            }],
            expected: Ok(()),
        },
        CommitRow {
            row: "no recorded epoch reads the member epoch",
            committed: &[CommittedPartition {
                topic: "in",
                partition: 3,
            }],
            expected: Err(codes::STALE_MEMBER_EPOCH),
        },
        CommitRow {
            row: "a repartition source topic",
            committed: &[CommittedPartition {
                topic: "rep",
                partition: 0,
            }],
            expected: Ok(()),
        },
        CommitRow {
            row: "one refused partition refuses the commit",
            committed: &[
                CommittedPartition {
                    topic: "in",
                    partition: 0,
                },
                CommittedPartition {
                    topic: "in",
                    partition: 1,
                },
            ],
            expected: Err(codes::STALE_MEMBER_EPOCH),
        },
        CommitRow {
            row: "a task the member does not hold",
            committed: &[CommittedPartition {
                topic: "in",
                partition: 4,
            }],
            expected: Err(codes::STALE_MEMBER_EPOCH),
        },
        CommitRow {
            row: "another subtopology's task",
            committed: &[CommittedPartition {
                topic: "other",
                partition: 0,
            }],
            expected: Err(codes::STALE_MEMBER_EPOCH),
        },
        CommitRow {
            row: "a topic outside the topology",
            committed: &[CommittedPartition {
                topic: "unknown",
                partition: 0,
            }],
            expected: Err(codes::STALE_MEMBER_EPOCH),
        },
    ];
    for CommitRow {
        row,
        committed,
        expected,
    } in rows
    {
        let partitions: Vec<_> = committed
            .iter()
            .map(|committed| (committed.topic.to_owned(), committed.partition))
            .collect();
        for fence in [
            CommitFence::Offset { api_version: 9 },
            CommitFence::Transactional,
        ] {
            check!(
                validate_offset_commit(
                    &group,
                    Some(&topology),
                    CommitIdentity {
                        member_id: "m1",
                        group_instance_id: None,
                        member_epoch: 4,
                        fence,
                    },
                    &partitions,
                ) == expected,
                "{row} {fence:?}"
            );
        }
    }
}

/// #972: Kafka trunk's `MISSING_CLIENT_TAGS` (`streamsGroupHeartbeat`, after
/// the shutdown status). A heartbeat at version 1 or above from a member that
/// has not sent every tag key of `streams.rack.aware.assignment.tags` carries
/// the status, naming the missing keys in the configured order; version 0
/// never does, and neither does a leave. A heartbeat that sends no client tags
/// keeps the member's earlier ones. Each row runs its heartbeats on a fresh
/// ready group and compares the whole status list of the last response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_missing_a_rack_aware_tag_gets_missing_client_tags_at_version_1() {
    use {
        crate::coordinator::unified::streams::topology::status,
        krabka_protocol::owned::common::{
            streams_group_heartbeat_request::key_value::KeyValue,
            streams_group_heartbeat_response::status::Status,
        },
    };

    /// One heartbeat of member `m1`: its request version, whether it joins,
    /// heartbeats at its epoch or leaves, and the client tags it sends.
    #[derive(Clone, Copy)]
    enum Beat {
        Join(i16, Option<&'static [&'static str]>),
        Heartbeat(i16, Option<&'static [&'static str]>),
        Leave(i16),
    }
    struct Row {
        name: &'static str,
        tags: &'static [&'static str],
        beats: &'static [Beat],
        status: Option<&'static str>,
    }
    let rows = [
        Row {
            name: "version 0 never gets the status",
            tags: &["zone"],
            beats: &[Beat::Join(0, None)],
            status: None,
        },
        Row {
            name: "version 1 names the missing tag",
            tags: &["zone"],
            beats: &[Beat::Join(1, None)],
            status: Some("[zone]"),
        },
        Row {
            name: "the missing tags follow the configured order",
            tags: &["zone", "cell", "rack"],
            beats: &[Beat::Join(1, Some(&["cell"]))],
            status: Some("[zone, rack]"),
        },
        Row {
            name: "a member that sends every tag gets no status",
            tags: &["zone", "rack"],
            beats: &[Beat::Join(1, Some(&["rack", "zone", "extra"]))],
            status: None,
        },
        Row {
            name: "no configured tag gives no status",
            tags: &[],
            beats: &[Beat::Join(1, None)],
            status: None,
        },
        Row {
            name: "a heartbeat without tags keeps the member's tags",
            tags: &["zone"],
            beats: &[Beat::Join(1, Some(&["zone"])), Beat::Heartbeat(1, None)],
            status: None,
        },
        Row {
            name: "a heartbeat without tags keeps the member missing them",
            tags: &["zone"],
            beats: &[Beat::Join(0, None), Beat::Heartbeat(1, None)],
            status: Some("[zone]"),
        },
        Row {
            name: "a heartbeat that sends the tag clears the status",
            tags: &["zone"],
            beats: &[Beat::Join(1, None), Beat::Heartbeat(1, Some(&["zone"]))],
            status: None,
        },
        Row {
            name: "a leave carries no status",
            tags: &["zone"],
            beats: &[Beat::Join(1, None), Beat::Leave(1)],
            status: None,
        },
    ];

    for row in rows {
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = coordinator_with_log(
            StreamsGroupConfig {
                rack_aware_assignment_tags: row.tags.iter().map(|tag| (*tag).to_owned()).collect(),
                ..undelayed()
            },
            log,
        );
        coord.set_metadata_source(input_topic_source(1));
        let handle = coord.get_or_create_streams("g");
        let mut epoch = 0;
        let mut last = None;
        for beat in row.beats {
            let (version, member_epoch, tags, topology) = match *beat {
                Beat::Join(version, tags) => (version, 0, tags, Some(one_subtopology().call())),
                Beat::Heartbeat(version, tags) => (version, epoch, tags, None),
                Beat::Leave(version) => (version, -1, None, None),
            };
            let resp = heartbeat_result_at(
                &handle,
                StreamsGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                    member_epoch,
                    rebalance_timeout_ms: 1_000,
                    client_tags: tags.map(|keys| {
                        keys.iter()
                            .map(|key| KeyValue {
                                key: (*key).into(),
                                value: "v".into(),
                                ..Default::default()
                            })
                            .collect()
                    }),
                    topology,
                    ..Default::default()
                },
                version,
            )
            .await
            .response;
            check!(resp.error_code == codes::NONE, "{}: {resp:?}", row.name);
            epoch = resp.member_epoch;
            last = Some(resp);
        }
        let expected: Vec<Status> = row
            .status
            .map(|missing| Status {
                status_code: status::MISSING_CLIENT_TAGS,
                status_detail: format!(
                    "Missing required client tags for rack-aware standby assignment: {missing}. \
                     Configure them via 'client.tag.<tagKey>' in your Streams config."
                ),
                ..Default::default()
            })
            .into_iter()
            .collect();
        check!(
            last.and_then(|resp| resp.status) == Some(expected),
            "{}",
            row.name
        );
    }
}

/// #972: an accepted heartbeat carries the group's `acceptable.recovery.lag`
/// in version 1's `int64` field, as Kafka trunk sets it, and 0 in version 0's
/// `int32` field, which Kafka never sets. On the wire version 0 carries only
/// the 0 and version 1 only the lag, so a decoder reads the other field's
/// default.
#[test]
fn heartbeat_response_carries_the_recovery_lag_at_version_1_only() {
    use krabka_protocol::{Decode as _, Encode as _};

    let config = StreamsGroupConfig {
        acceptable_recovery_lag: 10_000,
        ..undelayed()
    };
    let response = response::base_resp(codes::NONE, 3, &config);
    let expected = StreamsGroupHeartbeatResponse {
        error_code: codes::NONE,
        member_epoch: 3,
        heartbeat_interval_ms: 5_000,
        acceptable_recovery_lag_legacy: 0,
        acceptable_recovery_lag: 10_000,
        // Kafka 4.3.1 never sets the field.
        task_offset_interval_ms: 0,
        ..Default::default()
    };
    assert!(response == expected);
    for (version, legacy, lag) in [(0, 0, -1), (1, 0, 10_000)] {
        let mut bytes = bytes::BytesMut::new();
        response.encode(&mut bytes, version).expect("encode");
        let decoded =
            StreamsGroupHeartbeatResponse::decode(&mut &bytes[..], version).expect("decode");
        assert!(
            (
                decoded.acceptable_recovery_lag_legacy,
                decoded.acceptable_recovery_lag
            ) == (legacy, lag),
            "version {version}"
        );
    }
}

/// Kafka 4.3.1 never sets `TaskOffsetIntervalMs`, so a heartbeat response
/// carries 0 on the wire. Kafka trunk sets it from
/// `streams.task.offset.interval.ms`, whose default is one minute, and the
/// group config answers the same only while `unstable.api.versions.enable` is
/// on.
#[test]
fn the_task_offset_interval_of_a_heartbeat_response_is_trunks_alone() {
    use crate::api_catalog::UnstableApiVersions::{Disabled, Enabled};

    for (unstable, expected) in [(Disabled, 0), (Enabled, 60_000)] {
        let config = StreamsGroupConfig {
            unstable_api_versions: unstable,
            ..undelayed()
        };

        let response = response::base_resp(codes::NONE, 3, &config);

        check!(response.task_offset_interval_ms == expected, "{unstable:?}");
    }
}

/// `partitionsByUserEndpoint` follows the `EndpointToPartitionsManager` of the
/// release. Kafka 4.3.1 lists the standby tasks alone, cuts a task's
/// partitions to the partition count of a topic that has fewer partitions than
/// the task has (and does nothing else), and keeps an entry that is left
/// empty. Kafka trunk lists standby and warmup tasks together, leaves out the
/// task partitions a topic does not have, and drops an entry with no partition.
#[test]
fn endpoint_partitions_follow_the_endpoint_to_partitions_manager_of_each_release() {
    use {
        crate::{
            api_catalog::UnstableApiVersions::{Disabled, Enabled},
            coordinator::unified::streams::{
                state::{StreamsGroupState, StreamsMemberState},
                topology::ConfiguredSubtopology,
            },
        },
        krabka_protocol::owned::{
            common::streams_group_heartbeat_response::{
                endpoint::Endpoint, topic_partition::TopicPartition,
            },
            streams_group_heartbeat_response::EndpointToPartitions,
        },
        std::collections::{BTreeMap, BTreeSet},
    };

    // The topic `in` has two partitions.
    let image = in_image(2);
    let subtopologies = BTreeMap::from([("0".to_owned(), ConfiguredSubtopology {
        number_of_tasks: 4,
        source_topics: BTreeSet::from(["in".to_owned()]),
        repartition_source_topics: BTreeMap::new(),
        repartition_sink_topics: BTreeSet::new(),
        state_changelog_topics: BTreeMap::new(),
    })]);
    let entry = |active: &[i32], standby: &[i32]| {
        let partitions = |partitions: &[i32]| {
            vec![TopicPartition {
                topic: "in".into(),
                partitions: partitions.to_vec(),
                ..Default::default()
            }]
        };
        EndpointToPartitions {
            user_endpoint: Endpoint {
                host: "localhost".into(),
                port: 1,
                ..Default::default()
            },
            active_partitions: if active.is_empty() {
                vec![]
            } else {
                partitions(active)
            },
            standby_partitions: if standby.is_empty() {
                vec![]
            } else {
                partitions(standby)
            },
            ..Default::default()
        }
    };
    // (name, active, standby and warmup tasks of subtopology 0, 4.3.1, trunk)
    let rows = [
        (
            "standby tasks over the partition count are cut",
            vec![0, 1],
            (vec![1, 2, 3], vec![0]),
            entry(&[0, 1], &[1, 2]),
            entry(&[0, 1], &[0, 1]),
        ),
        (
            "a warmup task is a standby task for trunk only",
            vec![0],
            (vec![], vec![1]),
            entry(&[0], &[]),
            entry(&[0], &[1]),
        ),
        (
            "a partition the topic lacks is kept below the count and filtered by trunk",
            vec![5],
            (vec![], vec![]),
            entry(&[5], &[]),
            entry(&[], &[]),
        ),
    ];
    for (name, active, (standby, warmup), released, trunk) in rows {
        let mut member = StreamsMemberState::joining("m1", "c", "h");
        member.user_endpoint = Some(("localhost".into(), 1));
        let tasks = |partitions: Vec<i32>| {
            if partitions.is_empty() {
                BTreeMap::new()
            } else {
                BTreeMap::from([("0".to_owned(), partitions)])
            }
        };
        member.active = tasks(active);
        member.standby = tasks(standby);
        member.warmup = tasks(warmup);
        let mut state = StreamsGroupState::new("g");
        state.members.insert("m1".into(), member);

        for (unstable, expected) in [(Disabled, released), (Enabled, trunk)] {
            let listed = response::endpoint_to_partitions(
                &state,
                "m1",
                Some(&subtopologies),
                Some(&image),
                unstable,
            );

            check!(listed == vec![expected], "{name}: {unstable:?}");
        }
    }
}

/// Kafka's initial rebalance delay and assignment interval
/// (`maybeUpdateStreamsTargetAssignment`, `computeDelayedTargetAssignment`).
/// Each row configures the delays, then runs timed heartbeats on a paused
/// clock and compares each answer's error code, member epoch, active tasks and
/// status list. With no metadata source every target is empty, so a member's
/// epoch shows whether the assignment of its group epoch was computed. A
/// member that joins while the initial delay holds the assignment back
/// reconciles to Kafka 4.3's initial target assignment epoch 1 with an empty
/// assignment, so its next heartbeat is not a join.
#[tokio::test(start_paused = true)]
async fn assignment_waits_for_the_initial_delay_and_the_interval() {
    use {
        crate::coordinator::unified::streams::{
            actor::reconciliation::{ASSIGNMENT_INTERVAL_DETAIL, INITIAL_DELAY_DETAIL},
            topology::status::ASSIGNMENT_DELAYED,
        },
        std::time::Duration,
    };

    /// A heartbeat: the member id, the member epoch sent, the clock advance
    /// before it, the expected member epoch and the expected
    /// `ASSIGNMENT_DELAYED` detail.
    struct DelayStep {
        member_id: &'static str,
        member_epoch: i32,
        advance_ms: u64,
        want_epoch: i32,
        want_delay: Option<&'static str>,
    }

    /// A delay and interval, and the heartbeats that run under them.
    struct DelayRow {
        name: &'static str,
        delay_ms: u64,
        interval_ms: u64,
        steps: Vec<DelayStep>,
    }

    let initial_assignment = || DelayStep {
        member_id: "m1",
        member_epoch: 0,
        advance_ms: 0,
        want_epoch: 2,
        want_delay: None,
    };
    let rows: [DelayRow; 3] = [
        // Three members join within one second: one assignment, after
        // the delay, for the group epoch of the third join.
        DelayRow {
            name: "initial delay coalesces the joins",
            delay_ms: 3_000,
            interval_ms: 0,
            steps: vec![
                DelayStep {
                    member_id: "m1",
                    member_epoch: 0,
                    advance_ms: 0,
                    want_epoch: 1,
                    want_delay: Some(INITIAL_DELAY_DETAIL),
                },
                DelayStep {
                    member_id: "m2",
                    member_epoch: 0,
                    advance_ms: 500,
                    want_epoch: 1,
                    want_delay: Some(INITIAL_DELAY_DETAIL),
                },
                DelayStep {
                    member_id: "m3",
                    member_epoch: 0,
                    advance_ms: 500,
                    want_epoch: 1,
                    want_delay: Some(INITIAL_DELAY_DETAIL),
                },
                DelayStep {
                    member_id: "m1",
                    member_epoch: 1,
                    advance_ms: 2_100,
                    want_epoch: 4,
                    want_delay: None,
                },
                DelayStep {
                    member_id: "m2",
                    member_epoch: 1,
                    advance_ms: 0,
                    want_epoch: 4,
                    want_delay: None,
                },
            ],
        },
        // A change 200 ms after an assignment waits for the interval.
        DelayRow {
            name: "assignment interval defers a change",
            delay_ms: 0,
            interval_ms: 1_000,
            steps: vec![
                initial_assignment(),
                DelayStep {
                    member_id: "m2",
                    member_epoch: 0,
                    advance_ms: 200,
                    want_epoch: 2,
                    want_delay: Some(ASSIGNMENT_INTERVAL_DETAIL),
                },
                DelayStep {
                    member_id: "m2",
                    member_epoch: 2,
                    advance_ms: 900,
                    want_epoch: 3,
                    want_delay: None,
                },
            ],
        },
        DelayRow {
            name: "no delay assigns at once",
            delay_ms: 0,
            interval_ms: 0,
            steps: vec![initial_assignment(), DelayStep {
                member_id: "m2",
                member_epoch: 0,
                advance_ms: 200,
                want_epoch: 3,
                want_delay: None,
            }],
        },
    ];

    for DelayRow {
        name,
        delay_ms,
        interval_ms,
        steps,
    } in rows
    {
        let coord = coordinator_with_log(
            StreamsGroupConfig {
                initial_rebalance_delay: Duration::from_millis(delay_ms),
                assignment_interval: Duration::from_millis(interval_ms),
                ..StreamsGroupConfig::default()
            },
            Arc::new(InMemoryOffsetsLog::default()),
        );
        let handle = coord.get_or_create_streams("g");
        for DelayStep {
            member_id,
            member_epoch,
            advance_ms,
            want_epoch,
            want_delay,
        } in steps
        {
            sleep(Duration::from_millis(advance_ms)).await;
            let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch,
                rebalance_timeout_ms: 1_000,
                ..Default::default()
            })
            .await;
            let delayed: Vec<StatusEntry> = resp
                .status
                .unwrap_or_default()
                .into_iter()
                .filter(|status| status.status_code == ASSIGNMENT_DELAYED)
                .map(|status| StatusEntry {
                    code: status.status_code,
                    detail: status.status_detail,
                })
                .collect();
            let want: Vec<StatusEntry> = want_delay
                .map(|detail| StatusEntry {
                    code: ASSIGNMENT_DELAYED,
                    detail: detail.to_owned(),
                })
                .into_iter()
                .collect();
            // Only a join gets the task lists, since every target is empty.
            let want_active = (member_epoch == 0).then(Vec::new);
            check!(
                (
                    resp.error_code,
                    resp.member_epoch,
                    resp.active_tasks,
                    delayed
                ) == (codes::NONE, want_epoch, want_active, want),
                "{name}: {member_id} after {advance_ms} ms"
            );
        }
    }
}

fn accepted_tasks(
    member_id: &str,
    member_epoch: i32,
    tasks: Option<&[i32]>,
) -> StreamsGroupHeartbeatResponse {
    let config = undelayed();
    StreamsGroupHeartbeatResponse {
        member_id: member_id.into(),
        status: Some(vec![]),
        active_tasks: tasks.map(|partitions| response_tasks(partitions.to_vec())),
        standby_tasks: tasks.map(|_| vec![]),
        warmup_tasks: tasks.map(|_| vec![]),
        ..super::response::base_resp(codes::NONE, member_epoch, &config)
    }
}

/// KIP-1263: a streams group replays the `AssignmentTimestamp` of its target
/// assignment metadata record, and Kafka's `canComputeNextTargetAssignment`
/// runs the assignment interval from it. A stored time inside the interval
/// holds the next assignment back, and an unknown one (0) or an elapsed
/// interval lets it run, which writes the time it finished.
#[tokio::test(start_paused = true)]
async fn the_replayed_assignment_timestamp_holds_the_interval() {
    use {
        crate::coordinator::unified::{
            StreamsGroupSeed,
            streams::{
                actor::reconciliation::ASSIGNMENT_INTERVAL_DETAIL,
                topology::status::ASSIGNMENT_DELAYED,
            },
            wall_clock_ms,
        },
        std::time::Duration,
    };

    // (case, milliseconds before now of the stored timestamp, or `None` for
    // 0, the expected (member epoch, ASSIGNMENT_DELAYED detail, whether the
    // group wrote a new timestamp))
    let rows = [
        ("no stored time", None, (3, None, true)),
        (
            "an assignment 200 ms ago",
            Some(200),
            (2, Some(ASSIGNMENT_INTERVAL_DETAIL.to_owned()), false),
        ),
        // Leave a full interval of margin: the wall/monotonic clock samples
        // can round to different milliseconds even while Tokio is paused.
        // The pure assignment-interval helper tests cover the exact boundary.
        ("an assignment 2 s ago", Some(2_000), (3, None, true)),
    ];
    for (case, ago, expected) in rows {
        let coord = coordinator_with_log(
            StreamsGroupConfig {
                initial_rebalance_delay: Duration::ZERO,
                assignment_interval: Duration::from_secs(1),
                ..StreamsGroupConfig::default()
            },
            Arc::new(InMemoryOffsetsLog::default()),
        );
        let handle = coord.get_or_create_streams("g");
        let stored = ago.map_or(0, |ago| wall_clock_ms() - ago);
        handle
            .tx
            .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                group_epoch: 2,
                assignment_epoch: 2,
                assignment_timestamp_ms: stored,
                ..StreamsGroupSeed::default()
            }))
            .await
            .unwrap();
        let before = wall_clock_ms();
        let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            ..Default::default()
        })
        .await;
        let after = wall_clock_ms();
        let delayed = resp
            .status
            .unwrap_or_default()
            .into_iter()
            .find(|status| status.status_code == ASSIGNMENT_DELAYED)
            .map(|status| status.status_detail);
        let written = coord
            .cached_streams_seed("g")
            .unwrap()
            .assignment_timestamp_ms;
        // The paused clock reads the same millisecond throughout, give or
        // take the rounding of the real clocks underneath it.
        let fresh = (before - 1..=after + 1).contains(&written);
        check!((resp.member_epoch, delayed, fresh) == expected, "{case}");
        check!(fresh || written == stored, "{case}");
    }
}

/// Kafka's `streamsGroupHeartbeat` bumps the epoch of a group whose members
/// and topology did not change when the topology epoch that it validates, or
/// its assignment configuration, differs from what the group's last
/// `StreamsGroupMetadataValue` recorded, and the bump records the new
/// values. With no metadata source no topology is ready, so the group
/// validates -1, and the default configuration is `num.standby.replicas=0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_validation_or_assignment_config_bumps_the_epoch() {
    use crate::coordinator::unified::{
        StreamsGroupSeed,
        streams::persistence::{
            StreamsGroupCurrentMemberAssignmentValue, StreamsGroupMemberMetadataValue,
            StreamsMemberWireState,
        },
    };

    let configs = |standby: &str| {
        maplit::btreemap! {"num.standby.replicas".to_owned() => standby.to_owned()}
    };
    // The record that the bump writes: the epoch after the loaded 2, the
    // validated topology epoch, and the configuration.
    let bump = Some((3, -1, configs("0")));
    // (case, the stored validated topology epoch, the stored configuration,
    // what a steady heartbeat writes)
    let rows = [
        ("as recorded", -1, configs("0"), None),
        ("another configuration", -1, configs("1"), bump.clone()),
        (
            "a null configuration list",
            -1,
            BTreeMap::new(),
            bump.clone(),
        ),
        (
            "a topology validated before the load",
            0,
            configs("0"),
            bump,
        ),
    ];
    for (case, validated, last_configs, expected) in rows {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        handle
            .tx
            .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                group_epoch: 2,
                validated_topology_epoch: validated,
                last_assignment_configs: last_configs,
                assignment_epoch: 2,
                members: [("m1".to_owned(), StreamsGroupMemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "client".into(),
                    client_host: "/127.0.0.1".into(),
                    process_id: "p1".into(),
                    user_endpoint: None,
                    client_tags: vec![],
                    rebalance_timeout_ms: 60_000,
                    topology_epoch: 0,
                })]
                .into(),
                current_per_member: [("m1".to_owned(), StreamsGroupCurrentMemberAssignmentValue {
                    member_epoch: 2,
                    previous_member_epoch: 1,
                    state: StreamsMemberWireState::Stable,
                    ..StreamsGroupCurrentMemberAssignmentValue::default()
                })]
                .into(),
                ..StreamsGroupSeed::default()
            }))
            .await
            .unwrap();
        heartbeat(&handle, StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 2,
            ..Default::default()
        })
        .await;
        let written = coord.cached_streams_seed("g").map(|seed| {
            (
                seed.group_epoch,
                seed.validated_topology_epoch,
                seed.last_assignment_configs,
            )
        });
        check!(written == expected, "{case}");
    }
}

/// The bump of a group whose topology the metadata holds in a valid
/// configuration records the topology epoch as `ValidatedTopologyEpoch`,
/// and every bump records `LastAssignmentConfigs`, as Kafka's
/// `newStreamsGroupMetadataRecord` writes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bump_records_the_validated_topology_epoch() {
    // (case, the topics in the image, the recorded validated topology epoch)
    let rows = [
        (
            "the source topic exists",
            vec![ImageTopic {
                name: "in",
                id: 1,
                partitions: 2,
            }],
            1,
        ),
        ("the source topic is missing", vec![], -1),
    ];
    for (case, topics, validated) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &topics))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        heartbeat(&handle, StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            topology: Some(one_subtopology().call()),
            ..Default::default()
        })
        .await;
        let seed = coord.cached_streams_seed("g").unwrap();
        // The member got its active tasks at its epoch, and its current
        // assignment record lists that epoch for each of them.
        let current = &seed.current_per_member["m1"];
        let at_member_epoch: BTreeMap<String, Vec<i32>> = current
            .active
            .iter()
            .map(|(subtopology, partitions)| {
                (subtopology.clone(), vec![
                    current.member_epoch;
                    partitions.len()
                ])
            })
            .collect();
        check!(current.active_epochs == at_member_epoch, "{case}");
        check!(
            (seed.validated_topology_epoch, seed.last_assignment_configs)
                == (
                    validated,
                    maplit::btreemap! {"num.standby.replicas".to_owned() => "0".to_owned()}
                ),
            "{case}"
        );
    }
}

#[derive(serde::Deserialize)]
struct StreamsFenceInput {
    live: bool,
    member_id: Option<String>,
    instance_id: Option<String>,
    epoch: i32,
    offset_commit_version: Option<i16>,
}
#[derive(Default)]
struct StreamsOffsetFence {
    state: Option<StreamsGroupState>,
    input: Option<StreamsFenceInput>,
    result: Option<Result<(), i16>>,
}
#[rstest::fixture]
fn streams_offset_fence() -> StreamsOffsetFence {
    // Every membership decision starts with fresh group state.
    StreamsOffsetFence::default()
}
#[rstest_bdd_macros::given("streams offset identity {input} with live member m1 at epoch 5")]
fn streams_fence_identity(streams_offset_fence: &mut StreamsOffsetFence, input: String) {
    let input: StreamsFenceInput = serde_json::from_str(&input).expect("offset identity");
    let mut state = StreamsGroupState::new("g");
    if input.live {
        state.members.insert("m1".into(), epoch_five_member());
    }
    streams_offset_fence.state = Some(state);
    streams_offset_fence.input = Some(input);
}
#[rstest_bdd_macros::when("the streams group validates committing in partition 0")]
fn streams_commit_fence(streams_offset_fence: &mut StreamsOffsetFence) {
    let input = streams_offset_fence
        .input
        .as_ref()
        .expect("commit identity");
    let fence = input
        .offset_commit_version
        .map_or(CommitFence::Transactional, |api_version| {
            CommitFence::Offset { api_version }
        });
    streams_offset_fence.result = Some(validate_offset_commit(
        streams_offset_fence.state.as_ref().expect("streams group"),
        None,
        CommitIdentity {
            member_id: input.member_id.as_deref().unwrap_or(""),
            group_instance_id: input.instance_id.as_deref(),
            member_epoch: input.epoch,
            fence,
        },
        &[("in".to_owned(), 0)],
    ));
}
#[rstest_bdd_macros::when("the streams group validates fetching offsets")]
fn streams_fetch_fence(streams_offset_fence: &mut StreamsOffsetFence) {
    let input = streams_offset_fence.input.as_ref().expect("fetch identity");
    streams_offset_fence.result = Some(validate_offset_fetch(
        streams_offset_fence.state.as_ref().expect("streams group"),
        input.member_id.as_deref(),
        input.epoch,
    ));
}
#[rstest_bdd_macros::then("the streams offset validation returns Kafka code {code}")]
fn streams_offset_validation(streams_offset_fence: &StreamsOffsetFence, code: i16) {
    let expected = if code == 0 { Ok(()) } else { Err(code) };
    check!(
        streams_offset_fence
            .result
            .as_ref()
            .expect("validation executed")
            == &expected
    );
}
rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/coordinator/unified/streams/actor/tests/offset_validation.feature",
    fixtures = [streams_offset_fence: StreamsOffsetFence],
);
type SubtopologyInput = (String, String, Vec<String>, Vec<String>);

#[derive(serde::Deserialize)]
struct StreamsTopologyInput {
    live: bool,
    topology: Option<Vec<SubtopologyInput>>,
}
#[derive(Default)]
struct StreamsSubscriptions {
    state: Option<StreamsGroupState>,
    topology: Option<StreamsGroupTopologyValue>,
    result: Option<SubscribedTopics>,
}
#[rstest::fixture]
fn streams_subscriptions() -> StreamsSubscriptions {
    // Each subscription projection owns its complete topology.
    StreamsSubscriptions::default()
}
#[rstest_bdd_macros::given("streams group membership and source/repartition topology {input}")]
fn streams_subscription_topology(streams_subscriptions: &mut StreamsSubscriptions, input: String) {
    let input: StreamsTopologyInput = serde_json::from_str(&input).expect("subscription topology");
    let mut state = StreamsGroupState::new("g");
    if input.live {
        state.members.insert("m1".into(), epoch_five_member());
        state.phase = StreamsGroupStatePhase::Stable;
    }
    streams_subscriptions.state = Some(state);
    streams_subscriptions.topology = input.topology.map(|rows| StreamsGroupTopologyValue {
        epoch: 1,
        subtopologies: rows
            .into_iter()
            .map(|(id, source, repartition, sinks)| {
                let refs = repartition.iter().map(String::as_str).collect::<Vec<_>>();
                StoredSubtopology {
                    repartition_sink_topics: sinks,
                    ..stored_subtopology(&id, &source, &refs)
                }
            })
            .collect(),
    });
}
#[rstest_bdd_macros::when("OffsetDelete asks which topics the streams group subscribes to")]
fn streams_subscribed_topics(streams_subscriptions: &mut StreamsSubscriptions) {
    streams_subscriptions.result = Some(subscribed_topics(
        streams_subscriptions.state.as_ref().expect("streams group"),
        streams_subscriptions.topology.as_ref(),
    ));
}
#[rstest_bdd_macros::then("the complete subscribed topic set is {topics}")]
fn streams_subscription_set(streams_subscriptions: &StreamsSubscriptions, topics: String) {
    let topics: Vec<String> = serde_json::from_str(&topics).expect("subscribed topics");
    check!(
        streams_subscriptions
            .result
            .as_ref()
            .expect("subscriptions observed")
            == &SubscribedTopics::Named(topics.into_iter().collect())
    );
}
rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/coordinator/unified/streams/actor/tests/subscriptions.feature",
    fixtures = [streams_subscriptions: StreamsSubscriptions],
);

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/streams/actor/tests/epochs.feature",
    name = "member epoch rule matches kafka"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_epoch_rule_matches_kafka(
    streams_actor_behavior: StreamsActorBehavior,
    environment: String,
    requests: String,
    expected: String,
    codes: String,
) {
    let _ = (
        streams_actor_behavior,
        environment,
        requests,
        expected,
        codes,
    );
}

#[rstest_bdd_macros::then("the acknowledged streams setup member epochs are {epochs}")]
fn streams_setup_epochs(streams_actor_behavior: &StreamsActorBehavior, epochs: String) {
    let epochs: Vec<i32> = serde_json::from_str(&epochs).expect("setup epochs");
    check!(
        streams_actor_behavior
            .responses
            .iter()
            .take(epochs.len())
            .map(|r| r.member_epoch)
            .collect::<Vec<_>>()
            == epochs
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/streams/actor/tests/internal_topics.feature",
    name = "a join sizes the internal topics as kafka does"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_join_sizes_the_internal_topics_as_kafka_does(
    streams_actor_behavior: StreamsActorBehavior,
    environment: String,
    copartitioned: String,
    topics: String,
    expected: String,
) {
    let _ = (
        streams_actor_behavior,
        environment,
        copartitioned,
        topics,
        expected,
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/streams/actor/tests/statuses.feature",
    name = "the heartbeat status list follows kafka"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_status_list_follows_kafka(
    streams_actor_behavior: StreamsActorBehavior,
    environment: String,
    requests: String,
    expected: String,
    codes: String,
) {
    let _ = (
        streams_actor_behavior,
        environment,
        requests,
        expected,
        codes,
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/streams/actor/tests/responses.feature",
    name = "the heartbeat response carries what kafka sends"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_response_carries_what_kafka_sends(
    streams_actor_behavior: StreamsActorBehavior,
    environment: String,
    requests: String,
    expected: String,
) {
    let _ = (streams_actor_behavior, environment, requests, expected);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/streams/actor/tests/static_members.feature",
    name = "a static member follows kafka static membership"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_static_member_follows_kafka_static_membership(
    streams_actor_behavior: StreamsActorBehavior,
    requests: String,
    expected: String,
    members: String,
) {
    let _ = (streams_actor_behavior, requests, expected, members);
}

use {
    crate::coordinator::unified::streams::topology::InternalTopicSpec,
    krabka_protocol::owned::{
        common::{
            streams_group_heartbeat_request::{
                endpoint::Endpoint as HeartbeatRequestEndpoint, topic_info::TopicInfo,
            },
            streams_group_heartbeat_response::{
                endpoint::Endpoint as HeartbeatEndpoint,
                topic_partition::TopicPartition as HeartbeatTopicPartition,
            },
        },
        streams_group_heartbeat_request::{CopartitionGroup, Subtopology},
        streams_group_heartbeat_response::EndpointToPartitions,
    },
};

#[derive(serde::Deserialize)]
struct ActorEnvironment {
    topics: Option<Vec<(String, u8, i32)>>,
    max_size: usize,
    unstable: bool,
}
type HeartbeatTasks = Vec<(String, Vec<i32>)>;
type InteractiveQueryEndpoints = Vec<(u16, Vec<i32>)>;

#[derive(serde::Deserialize)]
struct ActorHeartbeat {
    timeout_ms: i32,
    member: String,
    epoch: Option<i32>,
    topology: Option<(i32, Vec<String>)>,
    instance: Option<String>,
    port: Option<u16>,
    shutdown: bool,
    owned: Option<HeartbeatTasks>,
}
#[derive(Default)]
struct StreamsActorBehavior {
    coordinator: Option<Arc<GroupCoordinator>>,
    handle: Option<Arc<StreamsGroupActorHandle>>,
    responses: Vec<StreamsGroupHeartbeatResponse>,
    creatable_topics: Vec<InternalTopicSpec>,
    members: Vec<DescribedMember>,
}
#[rstest::fixture]
fn streams_actor_behavior() -> StreamsActorBehavior {
    // A new native coordinator and actor own each scenario's membership and log.
    StreamsActorBehavior::default()
}
#[rstest_bdd_macros::given("a fresh streams actor with environment {environment}")]
fn fresh_streams_actor(streams_actor_behavior: &mut StreamsActorBehavior, environment: String) {
    let environment: ActorEnvironment =
        serde_json::from_str(&environment).expect("actor environment");
    let config = StreamsGroupConfig {
        max_size: environment.max_size,
        unstable_api_versions: if environment.unstable {
            UnstableApiVersions::Enabled
        } else {
            UnstableApiVersions::Disabled
        },
        ..undelayed()
    };
    let coordinator = coordinator_with_log(config, Arc::new(InMemoryOffsetsLog::default()));
    if let Some(topics) = environment.topics {
        let image_topics = topics
            .iter()
            .map(|(name, id, partitions)| ImageTopic {
                name,
                id: *id,
                partitions: *partitions,
            })
            .collect::<Vec<_>>();
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &image_topics))
                .build(),
        );
        coordinator.set_metadata_source(source);
    }
    streams_actor_behavior.handle = Some(coordinator.get_or_create_streams("g"));
    streams_actor_behavior.coordinator = Some(coordinator);
}

#[rstest_bdd_macros::when("the actor receives these heartbeat requests {requests}")]
async fn streams_actor_heartbeats(
    streams_actor_behavior: &mut StreamsActorBehavior,
    requests: String,
) {
    let requests: Vec<ActorHeartbeat> =
        serde_json::from_str(&requests).expect("heartbeat requests");
    let handle = streams_actor_behavior
        .handle
        .as_ref()
        .expect("streams actor");
    for request in requests {
        let epoch = request.epoch.unwrap_or_else(|| {
            streams_actor_behavior
                .responses
                .iter()
                .rev()
                .find(|response| response.member_id == request.member)
                .expect("member's last acknowledged epoch")
                .member_epoch
        });
        let owned = request.owned.map(|tasks| {
            tasks
                .into_iter()
                .map(|(subtopology_id, partitions)| RequestTaskIds {
                    subtopology_id,
                    partitions,
                    ..Default::default()
                })
                .collect::<Vec<_>>()
        });
        let response = heartbeat(handle, StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: request.member,
            member_epoch: epoch,
            instance_id: request.instance,
            shutdown_application: request.shutdown,
            user_endpoint: request.port.map(|port| HeartbeatRequestEndpoint {
                host: "localhost".into(),
                port,
                ..Default::default()
            }),
            rebalance_timeout_ms: request.timeout_ms,
            topology: request.topology.map(|(epoch, source_topics)| Topology {
                epoch,
                subtopologies: vec![Subtopology {
                    subtopology_id: "0".into(),
                    source_topics,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            standby_tasks: owned.as_ref().map(|_| vec![]),
            warmup_tasks: owned.as_ref().map(|_| vec![]),
            active_tasks: owned,
            ..Default::default()
        })
        .await;
        streams_actor_behavior.responses.push(response);
    }
    streams_actor_behavior.members = describe(handle)
        .await
        .members
        .into_iter()
        .map(|member| DescribedMember {
            active: member.active.get("0").cloned().unwrap_or_default(),
            member_id: member.member_id,
            member_epoch: member.member_epoch,
        })
        .collect();
    streams_actor_behavior.members.sort();
}

#[derive(serde::Deserialize)]
struct ExpectedActorResponse {
    code: i16,
    message: Option<String>,
    member: String,
    epoch: i32,
    heartbeat_ms: i32,
    task_ms: i32,
    lag: i64,
    active: Option<HeartbeatTasks>,
    standby: Option<HeartbeatTasks>,
    warmup: Option<HeartbeatTasks>,
    status: Vec<(i8, String)>,
    endpoint_epoch: i32,
    endpoints: Option<InteractiveQueryEndpoints>,
}
impl ExpectedActorResponse {
    fn native(self) -> StreamsGroupHeartbeatResponse {
        let tasks = |rows: Option<HeartbeatTasks>| {
            rows.map(|rows| {
                rows.into_iter()
                    .map(|(subtopology_id, partitions)| ResponseTaskIds {
                        subtopology_id,
                        partitions,
                        ..Default::default()
                    })
                    .collect()
            })
        };
        StreamsGroupHeartbeatResponse {
            error_code: self.code,
            error_message: self.message,
            member_id: self.member,
            member_epoch: self.epoch,
            heartbeat_interval_ms: self.heartbeat_ms,
            task_offset_interval_ms: self.task_ms,
            acceptable_recovery_lag: self.lag,
            active_tasks: tasks(self.active),
            standby_tasks: tasks(self.standby),
            warmup_tasks: tasks(self.warmup),
            status: Some(
                self.status
                    .into_iter()
                    .map(|(status_code, status_detail)| Status {
                        status_code,
                        status_detail,
                        ..Default::default()
                    })
                    .collect(),
            ),
            endpoint_information_epoch: self.endpoint_epoch,
            partitions_by_user_endpoint: self.endpoints.map(|rows| {
                rows.into_iter()
                    .map(|(port, active)| EndpointToPartitions {
                        user_endpoint: HeartbeatEndpoint {
                            host: "localhost".into(),
                            port,
                            ..Default::default()
                        },
                        active_partitions: if active.is_empty() {
                            vec![]
                        } else {
                            vec![HeartbeatTopicPartition {
                                topic: "in".into(),
                                partitions: active,
                                ..Default::default()
                            }]
                        },
                        standby_partitions: vec![],
                        ..Default::default()
                    })
                    .collect()
            }),
            ..Default::default()
        }
    }
}
#[rstest_bdd_macros::then("the complete last streams heartbeat response is {expected}")]
fn streams_last_response(streams_actor_behavior: &StreamsActorBehavior, expected: String) {
    let expected: ExpectedActorResponse =
        serde_json::from_str(&expected).expect("complete expected response");
    check!(
        streams_actor_behavior
            .responses
            .last()
            .expect("heartbeat executed")
            == &expected.native()
    );
}
#[rstest_bdd_macros::then("every streams heartbeat returns these Kafka codes {codes}")]
fn streams_response_codes(streams_actor_behavior: &StreamsActorBehavior, codes: String) {
    let codes: Vec<i16> = serde_json::from_str(&codes).expect("expected heartbeat codes");
    check!(
        streams_actor_behavior
            .responses
            .iter()
            .map(|response| response.error_code)
            .collect::<Vec<_>>()
            == codes
    );
}
#[rstest_bdd_macros::then("the acknowledged streams member epochs are {epochs}")]
fn streams_response_epochs(streams_actor_behavior: &StreamsActorBehavior, epochs: String) {
    let epochs: Vec<i32> = serde_json::from_str(&epochs).expect("expected acknowledged epochs");
    check!(
        streams_actor_behavior
            .responses
            .iter()
            .map(|response| response.member_epoch)
            .collect::<Vec<_>>()
            == epochs
    );
}
#[rstest_bdd_macros::then("the complete initial streams heartbeat response is {expected}")]
fn streams_initial_response(streams_actor_behavior: &StreamsActorBehavior, expected: String) {
    let expected: ExpectedActorResponse =
        serde_json::from_str(&expected).expect("initial expected response");
    check!(
        streams_actor_behavior
            .responses
            .first()
            .expect("initial heartbeat executed")
            == &expected.native()
    );
}
#[rstest_bdd_macros::then(
    "the described member identities, epochs and active partitions are {members}"
)]
fn streams_described_members(streams_actor_behavior: &StreamsActorBehavior, members: String) {
    let members: Vec<(String, i32, Vec<i32>)> =
        serde_json::from_str(&members).expect("complete member projection");
    let expected = members
        .into_iter()
        .map(|(member_id, member_epoch, active)| DescribedMember {
            member_id,
            member_epoch,
            active,
        })
        .collect::<Vec<_>>();
    check!(streams_actor_behavior.members == expected);
}

#[rstest_bdd_macros::when(
    "a member joins a two-subtopology orders to rp to customers topology with copartition {copartitioned}"
)]
async fn join_internal_topology(
    streams_actor_behavior: &mut StreamsActorBehavior,
    copartitioned: bool,
) {
    let topology = Topology {
        epoch: 1,
        subtopologies: vec![source_to_repartition(), Subtopology {
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
            copartition_groups: if copartitioned {
                vec![CopartitionGroup {
                    source_topics: vec![0],
                    repartition_source_topics: vec![0],
                    ..Default::default()
                }]
            } else {
                vec![]
            },
            ..Default::default()
        }],
        ..Default::default()
    };
    let result = heartbeat_result(
        streams_actor_behavior
            .handle
            .as_ref()
            .expect("streams actor"),
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
    streams_actor_behavior.responses.push(result.response);
    streams_actor_behavior.creatable_topics = result.creatable_topics;
}
#[rstest_bdd_macros::then(
    "the requested internal topics have names and partition counts {topics} with replication zero and no configs"
)]
fn streams_internal_topic_specs(streams_actor_behavior: &StreamsActorBehavior, topics: String) {
    let topics: Vec<(String, i32)> =
        serde_json::from_str(&topics).expect("expected internal topics");
    let expected = topics
        .into_iter()
        .map(|(name, partitions)| InternalTopicSpec {
            name,
            partitions,
            replication_factor: 0,
            configs: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    check!(streams_actor_behavior.creatable_topics == expected);
}

#[test]
fn task_assignor_registry_unavailable_persisted_name_preserves_other_overrides() {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1GroupConfig(
        krabka_metadata::GroupConfigRecord {
            group_id: "streams-app".into(),
            configs: maplit::btreemap! {
                KEY_NUM_STANDBY_REPLICAS.into() => "1".into(),
                "streams.assignor.name".into() => "unavailable-extension".into(),
            },
        },
    ));
    let config =
        resolve_group_config_from_image(&StreamsGroupConfig::default(), &image, "streams-app");
    assert!(config.num_standby_replicas == 1);
    assert!(config.selected_task_assignor_name() == "sticky");
}
