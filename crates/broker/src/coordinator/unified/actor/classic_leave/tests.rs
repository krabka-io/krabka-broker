//! Unit tests for the classic `LeaveGroup` and `DeleteGroups` paths.

use std::time::Instant;

use assert2::check;
use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

use super::*;
use crate::coordinator::unified::{
    actor::{
        GroupActorMessage,
        member_state::build_member,
        test_support::{
            await_until, completing_classic_group, last_classic_metadata, make_coordinator,
            make_coordinator_with_topic_policy, rpc, seed_classic_member,
        },
    },
    classic_state::GroupState as ClassicGroupState,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_last_member_persists_empty_generation() {
    use krabka_protocol::owned::leave_group_request::MemberIdentity;

    let (coord, log) = make_coordinator();
    let mut group = completing_classic_group(&["m1"]);
    group.as_classic_mut().unwrap().state = ClassicGroupState::Stable;
    let prior_generation = group.as_classic().unwrap().generation_id;
    coord.seed_classic("g", Box::new(group));
    let handle = coord.find("g").unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicLeave {
            req: LeaveGroupRequest {
                group_id: "g".into(),
                members: vec![MemberIdentity {
                    member_id: "m1".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            version: 3,
            reply: tx,
        })
        .await
        .unwrap();

    let result = rx.await.unwrap();
    check!(result.error_code == codes::NONE);
    check!(result.members[0].error_code == codes::NONE);
    let view = rpc::classic_inspect(&handle).await;
    check!(view.state == ClassicGroupState::Empty);
    check!(view.generation_id == prior_generation + 1);
    let persisted = last_classic_metadata(&log).await;
    check!(persisted.generation == prior_generation + 1);
    check!(persisted.members.is_empty());
    check!(persisted.leader.is_none());
    check!(persisted.protocol_name.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_with_members_remaining_does_not_commit_empty_generation() {
    use krabka_protocol::owned::leave_group_request::MemberIdentity;

    let (coord, log) = make_coordinator();
    let mut group = completing_classic_group(&["m1", "m2"]);
    group.as_classic_mut().unwrap().state = ClassicGroupState::Stable;
    let prior_generation = group.as_classic().unwrap().generation_id;
    coord.seed_classic("g", Box::new(group));
    let handle = coord.find("g").unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicLeave {
            req: LeaveGroupRequest {
                group_id: "g".into(),
                members: vec![MemberIdentity {
                    member_id: "m1".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            version: 3,
            reply: tx,
        })
        .await
        .unwrap();

    let result = rx.await.unwrap();
    check!(result.error_code == codes::NONE);
    check!(result.members[0].error_code == codes::NONE);
    let view = rpc::classic_inspect(&handle).await;
    check!(view.generation_id == prior_generation);
    check!(view.members.len() == 1);
    check!(view.members[0].member_id == "m2");
    check!(log.batches().await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_legacy_append_failure_rolls_back_and_reports_error() {
    let (coord, log) = make_coordinator();
    let mut group = completing_classic_group(&["m1"]);
    group.as_classic_mut().unwrap().state = ClassicGroupState::Stable;
    coord.seed_classic("g", Box::new(group));
    let handle = coord.find("g").unwrap();
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicLeave {
            req: LeaveGroupRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                ..Default::default()
            },
            version: 2,
            reply: tx,
        })
        .await
        .unwrap();

    let result = rx.await.unwrap();
    check!(result.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
    check!(result.members.is_empty());
    let view = rpc::classic_inspect(&handle).await;
    check!(view.state == ClassicGroupState::Stable);
    check!(view.members.len() == 1);
    check!(view.members[0].member_id == "m1");
    check!(log.batches().await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_delete_append_failure_keeps_group_registered() {
    let (coord, log) = make_coordinator();
    let _ = coord.get_or_create_classic("g");
    coord.mark_classic("g");
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);

    check!(coord.delete_group("g").await == Err(crate::coordinator::DeleteGroupError::Internal));
    check!(coord.describe_group("g").await.is_some());
    check!(coord.group_type("g") == Some(crate::coordinator::unified::GroupType::Classic));
    check!(!log.has_classic_group_metadata_tombstone("g").await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_removes_a_hosted_member_from_an_upgraded_group() {
    use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;

    let (coord, _log) =
        make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);
    let handle = seed_classic_member(&coord, "m-classic", "t", None);
    let joined = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    check!(joined.error_code == codes::NONE);
    let native = joined.member_id.expect("native member id");

    let response = rpc::classic_leave(&handle, "m-classic").await;
    check!(response.len() == 1);
    check!(response[0].error_code == codes::NONE);

    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Describe { reply: tx })
        .await
        .unwrap();
    let view = rx.await.unwrap();
    check!(view.members.len() == 1);
    check!(view.members[0].member_id == native);
    check!(!view.members[0].is_classic);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_consumer_log_failure_stops_the_actor() {
    use krabka_protocol::owned::leave_group_request::MemberIdentity;

    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let joined = rpc::consumer_heartbeat(&handle, "", 0, None).await;
    check!(joined.error_code == codes::NONE);
    let native = joined.member_id.expect("native member id");
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicLeave {
            req: LeaveGroupRequest {
                group_id: "g".into(),
                members: vec![MemberIdentity {
                    member_id: native,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version: 3,
            reply: tx,
        })
        .await
        .unwrap();
    let result = rx.await.unwrap();
    check!(result.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
    check!(result.members.is_empty());

    await_until("consumer-kind log failure stops the actor", || {
        handle.tx.is_closed()
    })
    .await;
    check!(handle.tx.is_closed());
}

#[test]
fn consumer_classic_leave_resolves_batch_and_static_identities() {
    use krabka_protocol::owned::leave_group_request::MemberIdentity;

    let mut group = GroupState::new("g");
    let dynamic = ConsumerGroupHeartbeatRequest {
        member_id: "m1".into(),
        ..Default::default()
    };
    group.add_or_update_member(build_member(
        "m1",
        &dynamic,
        crate::coordinator::unified::ClientIdentity { id: "c", host: "h" },
        Instant::now(),
        &std::collections::HashSet::new(),
    ));
    let static_request = ConsumerGroupHeartbeatRequest {
        member_id: "m-static".into(),
        instance_id: Some("instance-a".into()),
        ..Default::default()
    };
    group.add_or_update_member(build_member(
        "m-static",
        &static_request,
        crate::coordinator::unified::ClientIdentity { id: "c", host: "h" },
        Instant::now(),
        &std::collections::HashSet::new(),
    ));
    let request = LeaveGroupRequest {
        members: vec![
            MemberIdentity {
                member_id: "missing".into(),
                ..Default::default()
            },
            MemberIdentity {
                member_id: String::new(),
                group_instance_id: Some("instance-a".into()),
                ..Default::default()
            },
            MemberIdentity {
                member_id: "stale".into(),
                group_instance_id: Some("instance-a".into()),
                ..Default::default()
            },
            MemberIdentity {
                member_id: "m1".into(),
                ..Default::default()
            },
            MemberIdentity {
                member_id: "m1".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let (responses, removed) = resolve_consumer_classic_leave(&group, &request, 3);
    check!(
        responses
            .iter()
            .map(|response| response.error_code)
            .collect::<Vec<_>>()
            == vec![
                codes::UNKNOWN_MEMBER_ID,
                codes::NONE,
                codes::FENCED_INSTANCE_ID,
                codes::NONE,
                codes::NONE,
            ]
    );
    check!(
        responses
            .iter()
            .map(|response| (
                response.member_id.as_str(),
                response.group_instance_id.as_deref(),
            ))
            .collect::<Vec<_>>()
            == vec![
                ("missing", None),
                ("", Some("instance-a")),
                ("stale", Some("instance-a")),
                ("m1", None),
                ("m1", None),
            ]
    );
    check!(removed == vec!["m-static".to_string(), "m1".to_string()]);
    check!(
        group.members.len() == 2,
        "resolution must not mutate the group"
    );

    let legacy = LeaveGroupRequest {
        member_id: "m1".into(),
        ..Default::default()
    };
    let (responses, removed) = resolve_consumer_classic_leave(&group, &legacy, 2);
    check!(responses.len() == 1);
    check!(responses[0].error_code == codes::NONE);
    check!(removed == vec!["m1".to_string()]);
}

/// A `TxnOffsetCommit` reserves its keys before its durable append and marks
/// them after it. A `DeleteGroups` that runs between the two must still
/// tombstone those keys: the transactional record is already in the log, and
/// its commit marker would otherwise bring the deleted group back. A released
/// reservation (a failed append) is not tombstoned, and a reservation sent
/// after the delete finds the actor stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_tombstones_the_keys_of_an_in_flight_transactional_commit() {
    use crate::coordinator::unified::{
        actor::TxnOffsetReservation,
        persistence::{Key, parse_key},
    };

    struct Row {
        name: &'static str,
        reserve: &'static [i32],
        release: &'static [i32],
        tombstoned: &'static [i32],
    }

    async fn reservation(
        handle: &crate::coordinator::unified::actor::GroupActorHandle,
        keys: Vec<(String, i32)>,
        reserve: bool,
    ) -> bool {
        let (reply, ack) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::TxnOffsetReservation(
                TxnOffsetReservation {
                    producer_id: 7,
                    keys,
                    reserve,
                    reply,
                },
            ))
            .await
            .is_ok()
            && ack.await.is_ok()
    }

    let keys = |partitions: &[i32]| -> Vec<(String, i32)> {
        partitions
            .iter()
            .map(|partition| ("orders".to_string(), *partition))
            .collect()
    };
    let rows = [
        Row {
            name: "reserved, not marked",
            reserve: &[1],
            release: &[],
            tombstoned: &[1],
        },
        Row {
            name: "reserved, then released",
            reserve: &[1],
            release: &[1],
            tombstoned: &[],
        },
        Row {
            name: "two reserved, one released",
            reserve: &[1, 2],
            release: &[2],
            tombstoned: &[1],
        },
    ];
    for Row {
        name,
        reserve,
        release,
        tombstoned: expected,
    } in rows
    {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_classic("g");
        coord.mark_classic("g");
        check!(reservation(&handle, keys(reserve), true).await, "{name}");
        if !release.is_empty() {
            check!(reservation(&handle, keys(release), false).await, "{name}");
        }

        check!(coord.delete_group("g").await == Ok(()), "{name}");

        let tombstoned: Vec<i32> = log
            .batches()
            .await
            .iter()
            .flat_map(|batch| &batch.records)
            .filter_map(|record| match parse_key(record.key.as_ref().unwrap()) {
                Ok(Key::OffsetCommit { partition, .. }) => Some(partition),
                _ => None,
            })
            .collect();
        check!(tombstoned == expected, "{name}");
        await_until("deleted group actor stops", || handle.tx.is_closed()).await;
        check!(!reservation(&handle, keys(&[3]), true).await, "{name}");
    }
}
