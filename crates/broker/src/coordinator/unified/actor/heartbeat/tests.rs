//! Unit tests for the KIP-848 heartbeat path.

use std::{collections::HashMap, sync::Arc};

use assert2::{assert, check};
use krabka_protocol::primitives::uuid::Uuid;

use super::*;
use crate::coordinator::unified::{
    GroupCoordinator,
    actor::{
        GroupActorMessage,
        member_state::build_member,
        test_support::{
            StaticMetadata, empty_metadata, make_coordinator, make_coordinator_with_topic, rpc,
            seed_classic_member, subscription_blob,
        },
    },
    offsets_log::fake::InMemoryOffsetsLog,
    reconciler::ReconcileInput,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_emits_one_batch() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    let resp = rx.await.unwrap();
    assert!(resp.error_code == 0);
    let batches = log.batches().await;
    assert!(
        batches.len() == 1,
        "first join should write exactly one batch"
    );
    // Minimum: k3 (group metadata) + k5 (member metadata) + k8 (current).
    assert!(batches[0].records.len() >= 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_adopts_client_member_id() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "client-uuid-1".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    let resp = rx.await.unwrap();
    // The join must succeed, echo the client-supplied member id, and
    // advance the epoch off 0. The client-id first-join takes the same
    // flush path as the empty-id case and persists exactly one batch.
    check!(resp.error_code == 0);
    check!(resp.member_id.as_deref() == Some("client-uuid-1"));
    check!(resp.member_epoch >= 1);
    check!(log.batches().await.len() == 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_limit_rejects_only_new_members() {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = Arc::new(GroupCoordinator::new(
        NextGenConfig {
            max_size: 1,
            ..NextGenConfig::default()
        },
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        empty_metadata(),
        log,
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ));
    let handle = coord.get_or_create_consumer("g");

    let joined = rpc::consumer_heartbeat(&handle, "m1", 0, Some("t")).await;
    check!(joined.error_code == codes::NONE);

    let rejected = rpc::consumer_heartbeat(&handle, "m2", 0, Some("t")).await;
    check!(rejected.error_code == codes::GROUP_MAX_SIZE_REACHED);

    let existing = rpc::consumer_heartbeat(&handle, "m1", joined.member_epoch, Some("t")).await;
    check!(existing.error_code == codes::NONE);
    check!(existing.member_epoch == joined.member_epoch);
}

const IDENTITY_TOPIC: Uuid = Uuid([9; 16]);

/// One row of [`heartbeat_identity_rules_follow_kafka`].
struct IdentityRow {
    name: &'static str,
    /// Send a -2 leave for `s1` before the row's request.
    release_s1_first: bool,
    member_id: &'static str,
    instance_id: Option<&'static str>,
    member_epoch: i32,
    owned: Option<Vec<i32>>,
    expected: ConsumerGroupHeartbeatResponse,
    /// `(member id, member epoch)` of every member after, sorted.
    members_after: Vec<(&'static str, i32)>,
    /// The member that owns instance `i1` after.
    i1_after: Option<&'static str>,
}

impl IdentityRow {
    fn new(
        name: &'static str,
        (member_id, instance_id, member_epoch): (&'static str, Option<&'static str>, i32),
        owned: Option<Vec<i32>>,
        expected: ConsumerGroupHeartbeatResponse,
    ) -> Self {
        Self {
            name,
            release_s1_first: false,
            member_id,
            instance_id,
            member_epoch,
            owned,
            expected,
            members_after: vec![("m1", 5), ("s1", 5)],
            i1_after: Some("s1"),
        }
    }
}

fn heartbeat_interval_ms() -> i32 {
    i32::try_from(NextGenConfig::default().heartbeat_interval.as_millis()).unwrap()
}

fn identity_ok(
    member_id: &str,
    member_epoch: i32,
    partitions: Option<Vec<i32>>,
) -> ConsumerGroupHeartbeatResponse {
    use krabka_protocol::owned::common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions;

    ConsumerGroupHeartbeatResponse {
        member_id: Some(member_id.into()),
        member_epoch,
        heartbeat_interval_ms: heartbeat_interval_ms(),
        assignment: partitions.map(|partitions| RespAssignment {
            topic_partitions: vec![TopicPartitions {
                topic_id: IDENTITY_TOPIC,
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn identity_error(error_code: i16, message: &str) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code,
        error_message: Some(message.into()),
        heartbeat_interval_ms: heartbeat_interval_ms(),
        ..Default::default()
    }
}

fn fenced_epoch(direction: &str, received: i32) -> ConsumerGroupHeartbeatResponse {
    identity_error(
        codes::FENCED_MEMBER_EPOCH,
        &format!(
            "The consumer group member has a {direction} member epoch ({received}) than the one \
             known by the group coordinator (5). The member must abandon all its partitions and \
             rejoin."
        ),
    )
}

fn identity_rows() -> Vec<IdentityRow> {
    let fenced_instance = || {
        identity_error(
            codes::FENCED_INSTANCE_ID,
            "Static member m9 with instance id i1 was fenced by member s1.",
        )
    };
    vec![
        IdentityRow {
            members_after: vec![("m1", 5), ("s1", -2)],
            ..IdentityRow::new(
                "static member leaves with epoch -2",
                ("s1", Some("i1"), -2),
                None,
                identity_ok("s1", -2, None),
            )
        },
        IdentityRow {
            release_s1_first: true,
            members_after: vec![("m1", 5), ("s2", 5)],
            i1_after: Some("s2"),
            ..IdentityRow::new(
                "new member takes a released instance id",
                ("s2", Some("i1"), 0),
                Some(vec![]),
                identity_ok("s2", 5, Some(vec![2, 3])),
            )
        },
        IdentityRow::new(
            "new member with an unreleased instance id",
            ("s2", Some("i1"), 0),
            Some(vec![]),
            identity_error(
                codes::UNRELEASED_INSTANCE_ID,
                "Static member s2 with instance id i1 cannot join the group because the instance \
                 id is owned by s1 member.",
            ),
        ),
        IdentityRow::new(
            "known member rejoins with epoch 0",
            ("m1", None, 0),
            Some(vec![]),
            identity_ok("m1", 5, Some(vec![0, 1])),
        ),
        IdentityRow::new(
            "previous epoch with a subset of the assignment",
            ("m1", None, 4),
            Some(vec![0]),
            identity_ok("m1", 5, Some(vec![0, 1])),
        ),
        IdentityRow::new(
            "previous epoch with a superset of the assignment",
            ("m1", None, 4),
            Some(vec![0, 1, 2]),
            fenced_epoch("smaller", 4),
        ),
        IdentityRow::new(
            "epoch older than the previous epoch",
            ("m1", None, 3),
            Some(vec![0]),
            fenced_epoch("smaller", 3),
        ),
        IdentityRow::new(
            "greater epoch",
            ("m1", None, 6),
            Some(vec![0, 1]),
            fenced_epoch("greater", 6),
        ),
        IdentityRow::new(
            "leave of an unknown member",
            ("ghost", None, -1),
            None,
            identity_error(
                codes::UNKNOWN_MEMBER_ID,
                "Member ghost is not a member of group g.",
            ),
        ),
        IdentityRow {
            members_after: vec![("s1", 5)],
            ..IdentityRow::new(
                "dynamic member leaves with epoch -1",
                ("m1", None, -1),
                None,
                identity_ok("m1", -1, None),
            )
        },
        IdentityRow::new(
            "static heartbeat from another member id",
            ("m9", Some("i1"), 5),
            Some(vec![2, 3]),
            fenced_instance(),
        ),
        IdentityRow::new(
            "static leave from another member id",
            ("m9", Some("i1"), -2),
            None,
            fenced_instance(),
        ),
        IdentityRow::new(
            "static heartbeat with an unknown instance id",
            ("s1", Some("i9"), 5),
            Some(vec![2, 3]),
            identity_error(codes::UNKNOWN_MEMBER_ID, "Instance id i9 is unknown."),
        ),
    ]
}

fn identity_request(
    member_id: &str,
    instance_id: Option<&str>,
    member_epoch: i32,
    owned: Option<Vec<i32>>,
) -> ConsumerGroupHeartbeatRequest {
    use krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions;

    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        instance_id: instance_id.map(str::to_string),
        member_epoch,
        subscribed_topic_names: Some(vec!["t".into()]),
        rebalance_timeout_ms: 60_000,
        topic_partitions: owned.map(|partitions| {
            vec![TopicPartitions {
                topic_id: IDENTITY_TOPIC,
                partitions,
                ..Default::default()
            }]
        }),
        ..Default::default()
    }
}

/// A group with a dynamic member `m1` on partitions 0 and 1 and a static
/// member `s1` (instance `i1`) on partitions 2 and 3, both at epoch 5 with
/// previous epoch 4, and a settled target.
fn identity_group() -> GroupState {
    let mut state = GroupState::new("g");
    state.group_epoch = 5;
    for (member_id, instance_id, partitions) in
        [("m1", None, vec![0, 1]), ("s1", Some("i1"), vec![2, 3])]
    {
        let mut member = build_member(
            member_id,
            &identity_request(member_id, instance_id, 0, None),
            crate::coordinator::unified::ClientIdentity { id: "c", host: "h" },
            Instant::now(),
        );
        member.member_epoch = 5;
        member.previous_member_epoch = 4;
        member.assigned_partitions = HashMap::from([(IDENTITY_TOPIC, partitions.clone())]);
        state.add_or_update_member(member);
        state.target.per_member.insert(
            member_id.to_string(),
            HashMap::from([(IDENTITY_TOPIC, partitions)]),
        );
    }
    state.target.epoch = 5;
    state.dirty = false;
    state
}

/// Kafka's KIP-848 identity rules for `ConsumerGroupHeartbeat`
/// (`GroupMetadataManager.consumerGroupHeartbeat`, `consumerGroupLeave`,
/// `throwIfConsumerGroupMemberEpochIsInvalid`, the instance checks and
/// `getOrMaybeSubscribeStaticConsumerGroupMember`). Each row starts from
/// [`identity_group`] and compares the whole response and the members after.
#[test]
fn heartbeat_identity_rules_follow_kafka() {
    let config = NextGenConfig::default();
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: HashMap::from([("t".to_string(), IDENTITY_TOPIC)]),
            partitions_per_topic: HashMap::from([(IDENTITY_TOPIC, 4)]),
            ..Default::default()
        },
    };
    let client = crate::coordinator::unified::ClientIdentity { id: "c", host: "h" };

    for row in identity_rows() {
        let mut state = identity_group();
        if row.release_s1_first {
            let released = step_heartbeat(
                &mut state,
                &config,
                &metadata,
                &identity_request("s1", Some("i1"), -2, None),
                client,
                Instant::now(),
            );
            check!(released.response.error_code == codes::NONE, "{}", row.name);
        }

        let step = step_heartbeat(
            &mut state,
            &config,
            &metadata,
            &identity_request(row.member_id, row.instance_id, row.member_epoch, row.owned),
            client,
            Instant::now(),
        );

        let mut members: Vec<(&str, i32)> = state
            .members
            .values()
            .map(|member| (member.member_id.as_str(), member.member_epoch))
            .collect();
        members.sort_unstable();
        check!(step.response == row.expected, "{}", row.name);
        check!(members == row.members_after, "{}", row.name);
        check!(
            state.current_member_for_instance("i1") == row.i1_after,
            "{}",
            row.name
        );
    }
}

/// The member ids a record list writes, each with `true` for a value and
/// `false` for a tombstone, sorted.
fn written<T>(records: &[(String, Option<T>)]) -> Vec<(String, bool)> {
    let mut out: Vec<(String, bool)> = records
        .iter()
        .map(|(id, value)| (id.clone(), value.is_some()))
        .collect();
    out.sort_unstable();
    out
}

/// A static member that leaves with -2 writes only its current assignment at
/// epoch -2. A new member with the same instance id then writes its own
/// records and the released member's tombstones in one batch, as Kafka's
/// `replaceMember` does.
#[test]
fn static_replacement_writes_new_records_and_tombstones_the_released_member() {
    let config = NextGenConfig::default();
    let metadata = empty_metadata();
    let client = crate::coordinator::unified::ClientIdentity { id: "c", host: "h" };
    let join = |member_id: &str, member_epoch: i32| {
        identity_request(member_id, Some("i1"), member_epoch, Some(vec![]))
    };
    let mut state = GroupState::new("g");
    let joined = step_heartbeat(
        &mut state,
        &config,
        &*metadata,
        &join("s1", 0),
        client,
        Instant::now(),
    );
    check!(joined.response.error_code == codes::NONE);

    let left = step_heartbeat(
        &mut state,
        &config,
        &*metadata,
        &join("s1", -2),
        client,
        Instant::now(),
    );
    let current: Vec<(&str, Option<i32>)> = left
        .pending
        .current_per_member
        .iter()
        .map(|(id, value)| (id.as_str(), value.as_ref().map(|value| value.member_epoch)))
        .collect();
    check!(current == vec![("s1", Some(-2))]);
    check!(left.pending.member_metadata.is_empty());
    check!(left.pending.group_metadata.is_none());

    let replaced = step_heartbeat(
        &mut state,
        &config,
        &*metadata,
        &ConsumerGroupHeartbeatRequest {
            rack_id: Some("rack-b".into()),
            rebalance_timeout_ms: 12_345,
            ..join("s2", 0)
        },
        client,
        Instant::now(),
    );
    check!(replaced.response.error_code == codes::NONE);
    check!(replaced.response.member_epoch == joined.response.member_epoch);
    let expected = vec![("s1".to_string(), false), ("s2".to_string(), true)];
    check!(written(&replaced.pending.member_metadata) == expected);
    check!(written(&replaced.pending.target_per_member) == expected);
    check!(written(&replaced.pending.current_per_member) == expected);
    // The restarted process's join fields replace the released member's.
    let metadata_of_s2 = replaced
        .pending
        .member_metadata
        .iter()
        .find_map(|(id, value)| (id == "s2").then_some(value.as_ref()).flatten())
        .expect("s2 member metadata");
    check!(metadata_of_s2.rack_id.as_deref() == Some("rack-b"));
    check!(metadata_of_s2.rebalance_timeout_ms == 12_345);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_heartbeat_emits_no_batch() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    let resp1 = rx.await.unwrap();
    let mid = resp1.member_id.clone().unwrap();
    let batches_after_join = log.batches().await.len();

    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: mid,
                member_epoch: resp1.member_epoch,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    let _ = rx.await.unwrap();
    let batches_after_steady = log.batches().await.len();
    assert!(
        batches_after_steady == batches_after_join,
        "steady-state heartbeat should not write"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_emits_tombstone_batch() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    let mid = rx.await.unwrap().member_id.unwrap();
    let pre_leave = log.batches().await.len();

    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: mid,
                member_epoch: -1,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    let _ = rx.await.unwrap();
    let batches = log.batches().await;
    assert!(batches.len() == pre_leave + 1);
    let leave_batch = &batches[batches.len() - 1];
    assert!(
        leave_batch.records.iter().any(|r| r.value.is_none()),
        "leave batch must contain at least one tombstone"
    );
}

#[test]
fn leave_reconciles_and_persists_survivor_assignments() {
    let config = NextGenConfig::default();
    let topic_id = Uuid([8; 16]);
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".into(), topic_id)].into(),
            partitions_per_topic: [(topic_id, 2)].into(),
            ..Default::default()
        },
    };
    let mut state = GroupState::new("g");
    for member_id in ["m1", "m2"] {
        state.add_or_update_member(build_member(
            member_id,
            &ConsumerGroupHeartbeatRequest {
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            crate::coordinator::unified::ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
        ));
    }
    run_reconcile(&mut state, &config, &metadata);
    let epoch_before = state.group_epoch;

    let step = step_heartbeat(
        &mut state,
        &config,
        &metadata,
        &ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m2".into(),
            member_epoch: -1,
            ..Default::default()
        },
        crate::coordinator::unified::ClientIdentity {
            id: "client",
            host: "host",
        },
        Instant::now(),
    );

    check!(state.group_epoch == epoch_before + 1);
    check!(state.target.per_member["m1"][&topic_id] == vec![0, 1]);
    check!(
        step.pending
            .target_per_member
            .iter()
            .any(|(member_id, value)| member_id == "m1" && value.is_some())
    );
    check!(
        step.pending
            .current_per_member
            .iter()
            .any(|(member_id, value)| member_id == "m1" && value.is_some())
    );
    assert!(
        step.pending
            .member_metadata
            .iter()
            .any(|(member_id, value)| member_id == "m2" && value.is_none())
    );
}

/// KIP-848 upgrade trigger: a `ConsumerGroupHeartbeat` for a *classic*
/// group under the default `bidirectional` policy converts that group in
/// place to a next-gen consumer group that hosts the classic member. The
/// conversion atomically tombstones the classic k2 `GroupMetadata` and
/// writes the full next-gen record set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_heartbeat_upgrades_a_classic_group() {
    use crate::coordinator::unified::{
        classic_state::{ClassicGroup as ClassicState, Member},
        group::{CoordinatorGroup, GroupKind},
    };

    let (coord, log) = make_coordinator_with_topic("t", 2);

    // Seed a classic group with one classic consumer member subscribed to
    // "t". Seeding (vs a JoinGroup round-trip) keeps the test deterministic
    // and timing-free; `classic_is_convertible` only inspects protocol_type
    // and each member's protocol_metadata, both set here.
    let mut cs = ClassicState::new("g");
    cs.protocol_type = Some("consumer".into());
    cs.generation_id = 1;
    cs.add_member(Member::new(
        "m-classic",
        "client",
        "127.0.0.1",
        std::time::Duration::from_secs(30),
        std::time::Duration::from_mins(1),
        vec![("range".into(), subscription_blob(&["t"]))],
    ));
    let group = Box::new(CoordinatorGroup::seeded(
        "g",
        GroupKind::Classic(cs),
        HashMap::new(),
    ));
    coord.seed_classic("g", group);
    let handle = coord.find("g").expect("seeded classic actor");

    // A native consumer-protocol heartbeat for the same group → upgrade.
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    let resp = rx.await.unwrap();
    assert!(resp.error_code == codes::NONE);

    // Describe now reports 2 members: the hosted classic member and the new
    // native consumer member.
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Describe { reply: tx })
        .await
        .unwrap();
    let describe = rx.await.unwrap();
    // The hosted classic member must survive the upgrade, the new native
    // consumer member must be present, and the upgrade batch tombstoned
    // the classic k2 GroupMetadata record.
    check!(describe.members.len() == 2);
    check!(describe.members.iter().any(|m| m.is_classic));
    check!(describe.members.iter().any(|m| !m.is_classic));
    check!(log.has_classic_group_metadata_tombstone("g").await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_upgrade_append_keeps_the_atomic_batch_unpublished() {
    let (coord, log) = make_coordinator_with_topic("t", 1);
    let handle = seed_classic_member(&coord, "m-classic", "t", None);
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "native".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();

    let response = rx.await.unwrap();
    check!(response.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
    assert!(log.batches().await.is_empty());
}

#[test]
fn step_heartbeat_first_join_targets_all_partitions() {
    use crate::coordinator::unified::consumer_state::GroupState;
    let topic_id = Uuid([7; 16]);
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".to_string(), topic_id)].into(),
            partitions_per_topic: [(topic_id, 2)].into(),
            ..Default::default()
        },
    };
    let config = NextGenConfig::default();
    let mut group = GroupState::new("g");
    let req = ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m1".into(),
        member_epoch: 0,
        subscribed_topic_names: Some(vec!["t".into()]),
        rebalance_timeout_ms: 60_000,
        ..Default::default()
    };
    let step = step_heartbeat(
        &mut group,
        &config,
        &metadata,
        &req,
        crate::coordinator::unified::ClientIdentity {
            id: "client-a",
            host: "",
        },
        Instant::now(),
    );
    // First join succeeds, advances to group epoch 1, targets all
    // partitions of "t", and must persist records.
    check!(step.response.error_code == 0);
    check!(step.response.member_epoch == 1);
    check!(group.target.per_member["m1"][&topic_id].clone() == vec![0, 1]);
    check!(!step.pending.is_empty());
}
