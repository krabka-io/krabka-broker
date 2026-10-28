//! Unit tests for the KIP-848 heartbeat path.

use {
    super::*,
    crate::coordinator::unified::{
        ClientIdentity,
        actor::{
            member_state::build_member,
            test_support::{
                StaticMetadata, classic_member_for_topic, consumer_native_join_request,
                coordinator_with_log, empty_metadata, make_coordinator,
                make_coordinator_with_topic, make_coordinator_with_topic_policy,
                rpc::{self, consumer_request},
                seed_classic_member,
            },
        },
        persistence_next_gen::GroupMetadataValue,
        reconciler::{ReconcileInput, metadata_hash},
        test_support::{
            MemberEpoch, assert_next_tombstone_batch, assert_single_member_limit_with_log,
            consumer_join_request, heartbeat_write_failures,
        },
    },
    assert2::{assert, check},
    krabka_ids::PartitionIndex,
    krabka_protocol::{owned::consumer_group_heartbeat_request, primitives::uuid::Uuid},
    std::{collections::HashMap, sync::atomic::Ordering},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_emits_one_batch() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let resp = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(resp.error_code == 0);
    let batches = log.batches().await;
    assert!(
        batches.len() == 1,
        "first join should write exactly one batch"
    );
    // Minimum: k3 (group metadata) + k5 (member metadata) + k8 (current).
    assert!(batches[0].records.len() >= 3);
}

/// A heartbeat whose write fails answers the code of that failure, as
/// Kafka's `CoordinatorOperationExceptionHelper.handleOperationException`
/// does: `NOT_COORDINATOR` after a lost leadership, so the member looks the
/// coordinator up again, and `COORDINATOR_NOT_AVAILABLE` after a write that
/// did not commit in time. The failed write publishes no batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_write_answers_its_code_and_writes_no_partial_batch() {
    for case in heartbeat_write_failures() {
        let fixture = case.coordinator(make_coordinator);
        let handle = fixture.coordinator.get_or_create_consumer("g");

        let response = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;

        fixture
            .check_refused(&response, |error_code| ConsumerGroupHeartbeatResponse {
                error_code,
                ..Default::default()
            })
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_adopts_client_member_id() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let resp = rpc::consumer_request(
        &handle,
        consumer_native_join_request()
            .member_id("client-uuid-1")
            .call(),
    )
    .await;
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
    assert_single_member_limit_with_log(
        |log| {
            coordinator_with_log(
                NextGenConfig {
                    max_size: 1,
                    ..NextGenConfig::assigning_at_once()
                },
                empty_metadata(),
                log,
            )
            .get_or_create_consumer("g")
        },
        |member_id, member_epoch| (member_id.to_owned(), member_epoch),
        async |handle, (member_id, member_epoch)| {
            rpc::consumer_heartbeat(handle, &member_id, member_epoch, Some("t")).await
        },
    )
    .await;
}

/// A consumer actor exists from its first heartbeat, but the group only from
/// its first join. Kafka's `getOrMaybeCreateConsumerGroup` and
/// `consumerGroupLeave` answer `GROUP_ID_NOT_FOUND` to any other epoch while
/// the group is missing, and `UNKNOWN_MEMBER_ID` once it exists and lacks the
/// member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_actor_holds_no_consumer_group_before_the_first_join() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");

    let mut answers = Vec::new();
    for (member_id, epoch) in [("m1", 3), ("m1", -1), ("m1", 0), ("m2", 3), ("m1", -1)] {
        let answer = rpc::consumer_heartbeat(&handle, member_id, epoch, Some("t")).await;
        answers.push((answer.error_code, answer.error_message));
    }

    check!(
        answers
            == vec![
                (
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Consumer group g not found.".to_owned())
                ),
                (
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group g not found.".to_owned())
                ),
                (codes::NONE, None),
                // The group exists now.
                (
                    codes::UNKNOWN_MEMBER_ID,
                    Some("Member m2 is not a member of group g.".to_owned())
                ),
                (codes::NONE, None),
            ]
    );
}

const IDENTITY_TOPIC: Uuid = Uuid([9; 16]);

/// The one topic `t`, with two partitions, of the handoff test.
fn handoff_metadata() -> StaticMetadata {
    StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".into(), IDENTITY_TOPIC)].into(),
            partitions_per_topic: [(IDENTITY_TOPIC, 2)].into(),
            ..Default::default()
        },
    }
}

fn handoff_heartbeat(
    state: &mut GroupState,
    request: ConsumerGroupHeartbeatRequest,
) -> HeartbeatStep {
    step_heartbeat(
        state,
        &NextGenConfig::assigning_at_once(),
        &handoff_metadata(),
        &ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            ..request
        },
        ClientIdentity {
            id: "client",
            host: "host",
        },
        Instant::now(),
        &RegexResolution::none(),
    )
}

/// The heartbeat with which a consumer joins `handoff_metadata`'s topic.
fn handoff_join(state: &mut GroupState, member_id: &str) -> HeartbeatStep {
    handoff_heartbeat(
        state,
        consumer_join_request()
            .group_id("")
            .member_id(member_id)
            .call(),
    )
}

/// What the Java client sends in steady state
/// (`ConsumerHeartbeatRequestManager.HeartbeatState.buildRequestData`): no
/// subscription and no rebalance timeout, and `TopicPartitions` only when its
/// assignment changed, so `owned` is `None` while it did not.
fn handoff_keepalive(
    state: &mut GroupState,
    member_id: &str,
    member_epoch: i32,
    owned: Option<Vec<i32>>,
) -> HeartbeatStep {
    handoff_heartbeat(state, ConsumerGroupHeartbeatRequest {
        member_id: member_id.into(),
        member_epoch,
        rebalance_timeout_ms: -1,
        topic_partitions: owned.map(|partitions| {
            vec![consumer_group_heartbeat_request::TopicPartitions {
                topic_id: IDENTITY_TOPIC,
                partitions,
                ..Default::default()
            }]
        }),
        ..Default::default()
    })
}

/// The partitions of `t` that a response assigns, or `None` when it carries no
/// assignment.
fn assigned_partitions(response: &ConsumerGroupHeartbeatResponse) -> Option<Vec<i32>> {
    response.assignment.as_ref().map(|assignment| {
        assignment
            .topic_partitions
            .iter()
            .flat_map(|topic| topic.partitions.iter().copied())
            .collect()
    })
}

/// Kafka reconciles a member only inside its own heartbeat, and reads a
/// heartbeat without `TopicPartitions` as "the owned set is unchanged"
/// (`CurrentAssignmentBuilder.ownsRevokedPartitions(null)`). So with two
/// members, the joiner is granted a partition only after the incumbent, told to
/// revoke it in its own heartbeat, reports an owned set without it. Stock
/// clients send that steady-state heartbeat with a null owned set, which used
/// to wipe the pending revocation and hand the partition to both members.
#[test]
fn a_joiner_gets_a_partition_only_after_the_incumbent_reports_it_revoked() {
    use crate::coordinator::unified::persistence_next_gen::MemberAssignmentState::{
        Stable, UnreleasedPartitions, UnrevokedPartitions,
    };

    let mut state = GroupState::new("g");
    let incumbent = handoff_join(&mut state, "a");
    check!(
        incumbent.response
            == identity_ok()
                .member_id("a")
                .epoch(2)
                .maybe_partitions(Some(vec![PartitionIndex(0), PartitionIndex(1)]))
                .call()
    );

    // `b` joins: the group moves to epoch 3 and `a` keeps both partitions
    // until its own heartbeat.
    let joiner = handoff_join(&mut state, "b");
    check!(joiner.response.member_epoch == 3);
    check!(assigned_partitions(&joiner.response) == Some(vec![]));
    check!(state.members["b"].assignment_state == UnreleasedPartitions);
    check!(state.members["a"].assigned_partitions == [(IDENTITY_TOPIC, vec![0, 1])].into());

    // The incumbent's heartbeat carries no owned set. It is told its smaller
    // assignment, and it stays at epoch 2 with the other partition pending
    // revocation.
    let told = handoff_keepalive(&mut state, "a", 2, None);
    let kept = assigned_partitions(&told.response).expect("a is told its assignment shrank");
    check!(kept.len() == 1);
    check!(
        told.response
            == identity_ok()
                .member_id("a")
                .epoch(2)
                .maybe_partitions(Some(kept.iter().copied().map(PartitionIndex).collect()))
                .call()
    );
    let revoked = 1 - kept[0];
    check!(state.members["a"].assignment_state == UnrevokedPartitions);
    check!(
        state.members["a"].partitions_pending_revocation
            == [(IDENTITY_TOPIC, vec![revoked])].into()
    );

    // Neither `a`'s next null heartbeat nor `b`'s moves anything: `a` still
    // owns the partition, so `b` does not get it.
    let again = handoff_keepalive(&mut state, "a", 2, None);
    check!(again.response == identity_ok().member_id("a").epoch(2).call());
    let waiting = handoff_keepalive(&mut state, "b", 3, None);
    check!(waiting.response == identity_ok().member_id("b").epoch(3).call());
    check!(state.members["b"].assigned_partitions.is_empty());
    check!(state.members["a"].assignment_state == UnrevokedPartitions);

    // `a` reports what it owns now, without the revoked partition. It moves to
    // the target epoch, and `b` is granted the partition at its next heartbeat.
    let acknowledged = handoff_keepalive(&mut state, "a", 2, Some(kept));
    check!(acknowledged.response == identity_ok().member_id("a").epoch(3).call());
    check!(state.members["a"].assignment_state == Stable);
    let granted = handoff_keepalive(&mut state, "b", 3, None);
    check!(
        granted.response
            == identity_ok()
                .member_id("b")
                .epoch(3)
                .maybe_partitions(Some(vec![PartitionIndex(revoked)]))
                .call()
    );
    check!(state.members["b"].assignment_state == Stable);
}

fn heartbeat_interval_ms() -> i32 {
    i32::try_from(
        NextGenConfig::assigning_at_once()
            .heartbeat_interval
            .as_millis(),
    )
    .unwrap()
}

#[derive(krabka_macros::FieldDefaults)]
struct IdentityExpectationSetup<'a> {
    #[default("m1")]
    member_id: &'a str,
    #[default(MemberEpoch(5))]
    epoch: MemberEpoch,
    partitions: Option<Vec<PartitionIndex>>,
}

#[bon::builder]
fn identity_ok(
    #[builder(default = "m1")] member_id: &'_ str,
    #[builder(into, default = MemberEpoch (5))] epoch: MemberEpoch,
    partitions: Option<Vec<PartitionIndex>>,
) -> ConsumerGroupHeartbeatResponse {
    use krabka_protocol::owned::common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions;

    let setup = IdentityExpectationSetup {
        member_id,
        epoch,
        partitions,
    };
    let IdentityExpectationSetup {
        member_id,
        epoch,
        partitions,
    } = setup;
    ConsumerGroupHeartbeatResponse {
        member_id: Some(member_id.into()),
        member_epoch: epoch.0,
        // Kafka's leave responses carry only the member id and epoch.
        heartbeat_interval_ms: if epoch.0 < 0 {
            0
        } else {
            heartbeat_interval_ms()
        },
        assignment: partitions.map(|partitions| RespAssignment {
            topic_partitions: vec![TopicPartitions {
                topic_id: IDENTITY_TOPIC,
                partitions: partitions.into_iter().map(|index| index.0).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[derive(krabka_macros::FieldDefaults)]
struct IdentityRequestSetup<'a> {
    #[default("m1")]
    member_id: &'a str,
    instance_id: Option<&'a str>,
    #[default(MemberEpoch(5))]
    member_epoch: MemberEpoch,
    owned: Option<Vec<PartitionIndex>>,
}

#[bon::builder]
fn identity_request(
    #[builder(default = "m1")] member_id: &'_ str,
    instance_id: Option<&'_ str>,
    #[builder(into, default = MemberEpoch (5))] member_epoch: MemberEpoch,
    owned: Option<Vec<PartitionIndex>>,
) -> ConsumerGroupHeartbeatRequest {
    use krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions;

    let setup = IdentityRequestSetup {
        member_id,
        instance_id,
        member_epoch,
        owned,
    };
    let IdentityRequestSetup {
        member_id,
        instance_id,
        member_epoch,
        owned,
    } = setup;
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        instance_id: instance_id.map(str::to_owned),
        member_epoch: member_epoch.0,
        subscribed_topic_names: Some(vec!["t".into()]),
        rebalance_timeout_ms: 60_000,
        topic_partitions: owned.map(|partitions| {
            vec![TopicPartitions {
                topic_id: IDENTITY_TOPIC,
                partitions: partitions.into_iter().map(|index| index.0).collect(),
                ..Default::default()
            }]
        }),
        ..Default::default()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MemberRecordKind {
    Tombstone,
    Value,
}

/// A member identity and the kind of record written for it.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct WrittenMember {
    member_id: String,
    kind: MemberRecordKind,
}

fn written_member(member_id: &str, kind: MemberRecordKind) -> WrittenMember {
    WrittenMember {
        member_id: member_id.to_owned(),
        kind,
    }
}

/// The member identities and record kinds, sorted.
fn written<T>(records: &[(String, Option<T>)]) -> Vec<WrittenMember> {
    let mut out: Vec<_> = records
        .iter()
        .map(|(id, value)| WrittenMember {
            member_id: id.clone(),
            kind: if value.is_some() {
                MemberRecordKind::Value
            } else {
                MemberRecordKind::Tombstone
            },
        })
        .collect();
    out.sort_unstable();
    out
}

/// A member id and the epoch of its current assignment record, `None` for a
/// tombstone.
#[derive(Debug, PartialEq)]
struct CurrentEpoch<'a> {
    member_id: &'a str,
    epoch: Option<i32>,
}

/// A static member that leaves with -2 writes only its current assignment at
/// epoch -2. A new member with the same instance id then writes its own
/// records and the released member's tombstones in one batch, as Kafka's
/// `replaceMember` does.
#[test]
fn static_replacement_writes_new_records_and_tombstones_the_released_member() {
    let config = NextGenConfig::assigning_at_once();
    let metadata = empty_metadata();
    let client = ClientIdentity { id: "c", host: "h" };
    let join = |member_id: &str, member_epoch: i32| {
        identity_request()
            .member_id(member_id)
            .maybe_instance_id(Some("i1"))
            .member_epoch(member_epoch)
            .maybe_owned(Some(vec![]))
            .call()
    };
    let mut state = GroupState::new("g");
    let joined = step_heartbeat(
        &mut state,
        &config,
        &*metadata,
        &join("s1", 0),
        client,
        Instant::now(),
        &RegexResolution::none(),
    );
    check!(joined.response.error_code == codes::NONE);

    let left = step_heartbeat(
        &mut state,
        &config,
        &*metadata,
        &join("s1", -2),
        client,
        Instant::now(),
        &RegexResolution::none(),
    );
    let current: Vec<CurrentEpoch<'_>> = left
        .pending
        .current_per_member
        .iter()
        .map(|(id, value)| CurrentEpoch {
            member_id: id.as_str(),
            epoch: value.as_ref().map(|value| value.member_epoch),
        })
        .collect();
    check!(
        current
            == vec![CurrentEpoch {
                member_id: "s1",
                epoch: Some(-2),
            }]
    );
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
        &RegexResolution::none(),
    );
    check!(replaced.response.error_code == codes::NONE);
    check!(replaced.response.member_epoch == joined.response.member_epoch);
    // Kafka's `replaceMember` records come first: the released member's
    // tombstones and the copy under the new id.
    let expected = vec![
        written_member("s1", MemberRecordKind::Tombstone),
        written_member("s2", MemberRecordKind::Value),
    ];
    check!(written(&replaced.pending.member_metadata) == expected);
    check!(written(&replaced.pending.target_per_member) == expected);
    check!(written(&replaced.pending.current_per_member) == expected);
    // Then the heartbeat's own: the restarted process's join fields replace
    // the released member's, and the copy reconciles from epoch 0.
    let own = replaced
        .pending
        .then
        .as_deref()
        .expect("the heartbeat's records");
    check!(written(&own.member_metadata) == vec![written_member("s2", MemberRecordKind::Value)]);
    check!(written(&own.current_per_member) == vec![written_member("s2", MemberRecordKind::Value)]);
    let metadata_of_s2 = own
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
    let resp1 = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    let mid = resp1.member_id.clone().unwrap();
    let batches_after_join = log.batches().await.len();

    let _ = rpc::consumer_heartbeat(&handle, &mid, resp1.member_epoch, Some("t")).await;
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
    let response = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    let mid = response.member_id.unwrap();
    let pre_leave = log.batches().await.len();

    let _ = rpc::consumer_request(&handle, ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: mid,
        member_epoch: -1,
        ..Default::default()
    })
    .await;
    assert_next_tombstone_batch(&log, pre_leave).await;
}

/// Kafka's `consumerGroupFenceMembers`: a leave writes the leaver's
/// tombstones and the group epoch, and computes no target. The next
/// heartbeat of a survivor computes the target, writes the targets that
/// changed and the target metadata, and reconciles the survivor.
#[test]
fn a_leave_bumps_the_epoch_and_the_next_heartbeat_assigns() {
    let config = NextGenConfig::assigning_at_once();
    let topic_id = Uuid([8; 16]);
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".into(), topic_id)].into(),
            partitions_per_topic: [(topic_id, 2)].into(),
            ..Default::default()
        },
    };
    let client = ClientIdentity {
        id: "client",
        host: "host",
    };
    let join = |member_id: &str| consumer_join_request().member_id(member_id).call();
    let mut state = GroupState::new("g");
    let step = |state: &mut GroupState, req: &ConsumerGroupHeartbeatRequest| {
        step_heartbeat(
            state,
            &config,
            &metadata,
            req,
            client,
            Instant::now(),
            &RegexResolution::none(),
        )
    };
    step(&mut state, &join("m1"));
    step(&mut state, &join("m2"));
    let (epoch, target_epoch) = (state.group_epoch, state.target.epoch);
    check!((epoch, target_epoch) == (3, 3));

    let leave = step(&mut state, &ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m2".into(),
        member_epoch: -1,
        ..Default::default()
    });
    check!(
        leave.pending
            == PendingRecords {
                member_metadata: vec![("m2".into(), None)],
                target_per_member: vec![("m2".into(), None)],
                current_per_member: vec![("m2".into(), None)],
                group_metadata: Some(GroupMetadataValue {
                    epoch: 4,
                    metadata_hash: state.metadata_hash(),
                }),
                ..PendingRecords::default()
            }
    );
    check!((state.group_epoch, state.target.epoch) == (4, 3));

    let m1_epoch = state.members["m1"].member_epoch;
    let next = step(&mut state, &ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m1".into(),
        member_epoch: m1_epoch,
        rebalance_timeout_ms: -1,
        ..Default::default()
    });
    check!(state.target.epoch == 4);
    check!(state.target.per_member["m1"][&topic_id] == vec![0, 1]);
    check!(next.pending.group_metadata == None);
    check!(
        next.pending
            .target_metadata
            .map(|value| value.assignment_epoch)
            == Some(4)
    );
    check!(
        next.pending
            .target_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect::<Vec<_>>()
            == vec!["m1"]
    );
}

/// KIP-848 upgrade trigger: a `ConsumerGroupHeartbeat` for a *classic*
/// group under the default `bidirectional` policy converts that group in
/// place to a next-gen consumer group that hosts the classic member. The
/// conversion atomically tombstones the classic k2 `GroupMetadata` and
/// writes the full next-gen record set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_heartbeat_upgrades_a_classic_group() {
    let (coord, log) = make_coordinator_with_topic("t", 2);

    // Seed a classic group with one classic consumer member subscribed to
    // "t". Seeding (vs a JoinGroup round-trip) keeps the test deterministic
    // and timing-free; `classic_is_convertible` only inspects protocol_type
    // and each member's protocol_metadata, both set here.
    let handle = seed_classic_member().coord(&coord).call();

    // A native consumer-protocol heartbeat for the same group → upgrade.
    let resp = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(resp.error_code == codes::NONE);

    // Describe now reports 2 members: the hosted classic member and the new
    // native consumer member.
    let describe = rpc::describe(&handle).await;
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
    let handle = seed_classic_member().coord(&coord).call();
    log.fail_next.store(true, Ordering::SeqCst);

    let response = consumer_request(
        &handle,
        consumer_native_join_request().member_id("native").call(),
    )
    .await;
    check!(response.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
    assert!(log.batches().await.is_empty());
}

#[test]
fn step_heartbeat_first_join_targets_all_partitions() {
    use crate::coordinator::unified::consumer_state::GroupState;
    let topic_id = Uuid([7; 16]);
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".to_owned(), topic_id)].into(),
            partitions_per_topic: [(topic_id, 2)].into(),
            ..Default::default()
        },
    };
    let config = NextGenConfig::assigning_at_once();
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
        ClientIdentity {
            id: "client-a",
            host: "",
        },
        Instant::now(),
        &RegexResolution::none(),
    );
    // First join succeeds, advances the new group from epoch 1 to 2, targets
    // all partitions of "t", and must persist records.
    check!(step.response.error_code == 0);
    check!(step.response.member_epoch == 2);
    check!(group.target.per_member["m1"][&topic_id].clone() == vec![0, 1]);
    // Kafka's first `ConsumerGroupMetadataValue` and target assignment
    // metadata of a new group carry epoch 2.
    check!(
        (
            step.pending.group_metadata.map(|value| value.epoch),
            step.pending
                .target_metadata
                .map(|value| value.assignment_epoch)
        ) == (Some(2), Some(2))
    );
}

use {
    crate::coordinator::unified::{
        GroupCoordinator, actor::GroupActorHandle, offsets_log::fake::InMemoryOffsetsLog,
    },
    krabka_protocol::owned::common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions as IdentityAssignmentPartition,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    serde::Deserialize,
    std::sync::Arc,
};
#[derive(Default)]
struct ConsumerIdentityBehavior {
    state: Option<GroupState>,
    metadata: Option<StaticMetadata>,
    release_code: Option<i16>,
    response: Option<ConsumerGroupHeartbeatResponse>,
}
#[rstest::fixture]
fn consumer_identity() -> ConsumerIdentityBehavior {
    // Start this scenario with fresh inputs and observations.
    ConsumerIdentityBehavior::default()
}
#[derive(DataTableRow)]
struct ExistingConsumerIdentity {
    member: String,
    instance: String,
    epoch: i32,
    previous_epoch: i32,
    partitions: String,
}
#[given(
    "consumer group g has group and target epochs 5 with four partitions of topic t UUID 09090909-0909-0909-0909-090909090909 and these settled members"
)]
fn existing_consumer_identities(
    consumer_identity: &mut ConsumerIdentityBehavior,
    #[datatable] rows: Rows<ExistingConsumerIdentity>,
) {
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: HashMap::from([("t".to_owned(), IDENTITY_TOPIC)]),
            partitions_per_topic: HashMap::from([(IDENTITY_TOPIC, 4)]),
            ..Default::default()
        },
    };
    let mut state = GroupState::new("g");
    state.group_epoch = 5;
    state.target.epoch = 5;
    for row in rows {
        let instance: Option<String> =
            serde_json::from_str(&row.instance).expect("literal static instance");
        let partitions: Vec<i32> =
            serde_json::from_str(&row.partitions).expect("literal settled assignment");
        let mut member = build_member(
            &row.member,
            &identity_request()
                .member_id(&row.member)
                .maybe_instance_id(instance.as_deref())
                .member_epoch(0)
                .call(),
            ClientIdentity { id: "c", host: "h" },
            Instant::now(),
        );
        member.member_epoch = row.epoch;
        member.previous_member_epoch = row.previous_epoch;
        member.assigned_partitions = HashMap::from([(IDENTITY_TOPIC, partitions.clone())]);
        state
            .target
            .per_member
            .insert(row.member, HashMap::from([(IDENTITY_TOPIC, partitions)]));
        state.add_or_update_member(member);
    }
    state.record_metadata_hash(metadata_hash(&state, &metadata.input));
    consumer_identity.state = Some(state);
    consumer_identity.metadata = Some(metadata);
}
#[given("static instance i1 has already released member s1 with epoch -2")]
fn released_consumer_identity(consumer_identity: &mut ConsumerIdentityBehavior) {
    let released = step_heartbeat(
        consumer_identity.state.as_mut().expect("group prepared"),
        &NextGenConfig::assigning_at_once(),
        consumer_identity.metadata.as_ref().expect("topic metadata"),
        &identity_request()
            .member_id("s1")
            .maybe_instance_id(Some("i1"))
            .member_epoch(-2)
            .call(),
        ClientIdentity { id: "c", host: "h" },
        Instant::now(),
        &RegexResolution::none(),
    );
    consumer_identity.release_code = Some(released.response.error_code);
}
#[derive(DataTableRow)]
struct ConsumerIdentityRequest {
    member: String,
    instance: String,
    epoch: i32,
    owned: String,
}
#[when(
    "that consumer heartbeats subscribed to t with rebalance timeout 60000 ms and these identity fields"
)]
fn request_consumer_identity(
    consumer_identity: &mut ConsumerIdentityBehavior,
    #[datatable] rows: Rows<ConsumerIdentityRequest>,
) {
    for row in rows {
        let instance: Option<String> =
            serde_json::from_str(&row.instance).expect("literal request instance");
        let owned: Option<Vec<i32>> =
            serde_json::from_str(&row.owned).expect("literal owned partitions");
        let request = identity_request()
            .member_id(&row.member)
            .maybe_instance_id(instance.as_deref())
            .member_epoch(row.epoch)
            .maybe_owned(
                owned.map(|partitions| partitions.into_iter().map(PartitionIndex).collect()),
            )
            .call();
        let step = step_heartbeat(
            consumer_identity.state.as_mut().expect("group prepared"),
            &NextGenConfig::assigning_at_once(),
            consumer_identity.metadata.as_ref().expect("topic metadata"),
            &request,
            ClientIdentity { id: "c", host: "h" },
            Instant::now(),
            &RegexResolution::none(),
        );
        consumer_identity.response = Some(step.response);
    }
}
#[derive(Deserialize)]
struct ExpectedIdentityResponse {
    error_code: i16,
    error_message: Option<String>,
    member_id: Option<String>,
    member_epoch: i32,
    heartbeat_interval_ms: i32,
    assignment: Option<Vec<i32>>,
}
#[derive(Deserialize)]
struct ExpectedConsumerIdentity {
    response: ExpectedIdentityResponse,
    members: Vec<(String, i32)>,
    instance_i1: Option<String>,
}
#[then("the complete consumer response and remaining member epochs and instance owner are")]
fn consumer_identity_result(consumer_identity: &ConsumerIdentityBehavior, docstring: String) {
    let expected: ExpectedConsumerIdentity =
        serde_json::from_str(&docstring).expect("literal independent identity result");
    let response = expected.response;
    let expected_response = ConsumerGroupHeartbeatResponse {
        error_code: response.error_code,
        error_message: response.error_message,
        member_id: response.member_id,
        member_epoch: response.member_epoch,
        heartbeat_interval_ms: response.heartbeat_interval_ms,
        assignment: response.assignment.map(|partitions| RespAssignment {
            topic_partitions: vec![IdentityAssignmentPartition {
                topic_id: IDENTITY_TOPIC,
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    check!(consumer_identity.response.as_ref() == Some(&expected_response));
    let state = consumer_identity.state.as_ref().expect("heartbeat applied");
    let mut members = state
        .members
        .values()
        .map(|member| (member.member_id.clone(), member.member_epoch))
        .collect::<Vec<_>>();
    members.sort_unstable();
    check!(members == expected.members);
    check!(state.current_member_for_instance("i1") == expected.instance_i1.as_deref());
    if let Some(code) = consumer_identity.release_code {
        check!(code == codes::NONE);
    }
}
mod identity_scenarios {
    use super::*;
    scenarios!("tests/features/migrated/src/coordinator/unified/actor/heartbeat/tests.feature",tags="@consumer-identity",fixtures=[consumer_identity:ConsumerIdentityBehavior]);
}
#[derive(Default)]
struct ClassicHeartbeatMigration {
    coordinator: Option<Arc<GroupCoordinator>>,
    log: Option<Arc<InMemoryOffsetsLog>>,
    handle: Option<Arc<GroupActorHandle>>,
    response: Option<ConsumerGroupHeartbeatResponse>,
    tombstone: Option<bool>,
}
#[rstest::fixture]
fn classic_heartbeat_migration() -> ClassicHeartbeatMigration {
    // Start this scenario with fresh inputs and observations.

    ClassicHeartbeatMigration::default()
}
#[given(
    "classic group g at generation 1 has {members} members subscribed to one-partition topic t with protocol {protocol} and migration policy {policy}"
)]
fn classic_group_before_consumer_heartbeat(
    classic_heartbeat_migration: &mut ClassicHeartbeatMigration,
    members: usize,
    protocol: String,
    policy: String,
) {
    use crate::coordinator::unified::{
        classic_state::ClassicGroup,
        config::ConsumerGroupMigrationPolicy as Policy,
        group::{CoordinatorGroup, GroupKind},
    };
    let policy = match policy.as_str() {
        "disabled" => Policy::Disabled,
        "upgrade" => Policy::Upgrade,
        "bidirectional" => Policy::Bidirectional,
        _ => panic!("unknown migration policy"),
    };
    let (coordinator, log) = make_coordinator_with_topic_policy("t", 1, policy);
    let mut classic = ClassicGroup::new("g");
    classic.protocol_type = if protocol == "absent" {
        None
    } else {
        Some(protocol)
    };
    for index in 0..members {
        classic.add_member(classic_member_for_topic(format!("classic-{index}"), "t"));
    }
    classic.generation_id = 1;
    coordinator.seed_classic(
        "g",
        Box::new(CoordinatorGroup::seeded(
            "g",
            GroupKind::Classic(classic),
            HashMap::new(),
        )),
    );
    classic_heartbeat_migration.handle = Some(coordinator.find("g").expect("seeded classic actor"));
    classic_heartbeat_migration.coordinator = Some(coordinator);
    classic_heartbeat_migration.log = Some(log);
}
#[when(
    "native consumer native heartbeats at epoch {epoch} subscribed to t with rebalance timeout 60000 ms and no owned partitions"
)]
async fn consumer_heartbeat_to_classic_group(
    classic_heartbeat_migration: &mut ClassicHeartbeatMigration,
    epoch: i32,
) {
    classic_heartbeat_migration.response = Some(
        consumer_request(
            classic_heartbeat_migration
                .handle
                .as_ref()
                .expect("classic group prepared"),
            ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "native".into(),
                member_epoch: epoch,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60000,
                topic_partitions: Some(vec![]),
                ..Default::default()
            },
        )
        .await,
    );
    classic_heartbeat_migration.tombstone = Some(
        classic_heartbeat_migration
            .log
            .as_ref()
            .expect("offsets log")
            .has_classic_group_metadata_tombstone("g")
            .await,
    );
}
#[derive(Deserialize)]
struct ExpectedClassicMigration {
    error_code: i16,
    error_message: Option<String>,
    classic_tombstone: bool,
}
#[then("the consumer admission error and classic metadata tombstone are")]
fn classic_heartbeat_migration_result(
    classic_heartbeat_migration: &ClassicHeartbeatMigration,
    docstring: String,
) {
    let expected: ExpectedClassicMigration =
        serde_json::from_str(&docstring).expect("literal migration outcome");
    let response = classic_heartbeat_migration
        .response
        .as_ref()
        .expect("heartbeat answered");
    check!(response.error_code == expected.error_code);
    check!(response.error_message == expected.error_message);
    check!(classic_heartbeat_migration.tombstone == Some(expected.classic_tombstone));
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/actor/heartbeat/tests.feature",
    name = "An offset-only classic group upgrades even with migration disabled"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_heartbeat_replaces_or_upgrades_a_classic_group_as_kafka_does(
    classic_heartbeat_migration: ClassicHeartbeatMigration,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/actor/heartbeat/tests.feature",
    name = "An empty connect group is replaced by a consumer group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_classic_heartbeat_migration_1(
    classic_heartbeat_migration: ClassicHeartbeatMigration,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/actor/heartbeat/tests.feature",
    name = "A live classic group refuses a consumer join when online upgrade is disabled"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_classic_heartbeat_migration_2(
    classic_heartbeat_migration: ClassicHeartbeatMigration,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/actor/heartbeat/tests.feature",
    name = "A live classic group upgrades when online upgrade is allowed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_classic_heartbeat_migration_3(
    classic_heartbeat_migration: ClassicHeartbeatMigration,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/coordinator/unified/actor/heartbeat/tests.feature",
    name = "A steady heartbeat cannot create a consumer group over an offset-only group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_classic_heartbeat_migration_4(
    classic_heartbeat_migration: ClassicHeartbeatMigration,
) {
}
