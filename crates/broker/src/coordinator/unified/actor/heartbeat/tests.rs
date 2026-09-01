//! Unit tests for the KIP-848 heartbeat path.

use std::{collections::HashMap, sync::Arc};

use assert2::{assert, check};
use krabka_protocol::primitives::uuid::Uuid;

use super::*;
use crate::coordinator::unified::{
    GroupCoordinator,
    actor::{
        GroupActorMessage,
        test_support::{
            StaticMetadata, empty_metadata, make_coordinator, make_coordinator_with_topic, rpc,
            subscription_blob,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn known_member_id_epoch_zero_is_stale() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    // First join with a client id, epoch 0 → succeeds, epoch advances.
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "client-uuid-2".into(),
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
    assert!(rx.await.unwrap().error_code == 0);

    // Same id re-sending epoch 0 is now a known member at a higher epoch →
    // stale, not a re-join.
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "client-uuid-2".into(),
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
    assert!(rx.await.unwrap().error_code == codes::STALE_MEMBER_EPOCH);
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
