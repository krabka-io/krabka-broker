//! Shared unit-test fixtures for the group-actor modules: a static metadata
//! provider, coordinator builders, classic-group seeders, and the offsets-log
//! readers that several actor submodules assert against.

use std::{collections::HashMap, sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest, primitives::uuid::Uuid,
};

pub(super) mod rpc;

use super::{GroupActorHandle, GroupActorMessage, MetadataProvider};
use crate::{
    codes,
    coordinator::unified::{
        GroupCoordinator,
        config::NextGenConfig,
        group::{CoordinatorGroup, GroupKind},
        offsets_log::fake::InMemoryOffsetsLog,
        reconciler::ReconcileInput,
    },
};

/// Yield-polls until `cond` holds. A bounded hang-guard makes a real stall
/// fail the test deterministically instead of spinning forever.
pub(super) async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held: {what}");
}

#[derive(Debug)]
pub(super) struct StaticMetadata {
    pub(super) input: ReconcileInput,
}
impl MetadataProvider for StaticMetadata {
    fn snapshot(&self) -> ReconcileInput {
        self.input.clone()
    }
}

pub(super) fn empty_metadata() -> Arc<dyn MetadataProvider> {
    Arc::new(StaticMetadata {
        input: ReconcileInput::default(),
    })
}

pub(super) fn make_coordinator() -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = Arc::new(GroupCoordinator::new(
        NextGenConfig::default(),
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        empty_metadata(),
        log.clone(),
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ));
    (coord, log)
}

pub(super) fn completing_classic_group(member_ids: &[&str]) -> CoordinatorGroup {
    use super::super::classic_state::{ClassicGroup as ClassicState, Member};

    let mut state = ClassicState::new("g");
    state.protocol_type = Some("consumer".into());
    for member_id in member_ids {
        state.add_member(Member::new(
            *member_id,
            "client",
            "host",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), Bytes::from_static(b"subscription"))],
        ));
    }
    state.resolve_selected_protocol_metadata("range");
    state.complete_rebalance("range");
    CoordinatorGroup {
        empty_since_ms: None,
        group_id: "g".into(),
        kind: GroupKind::Classic(state),
        committed_offsets: HashMap::new(),
    }
}

pub(super) async fn last_classic_metadata(
    log: &InMemoryOffsetsLog,
) -> crate::coordinator::unified::persistence::GroupMetadataValue {
    use crate::coordinator::unified::persistence::{GroupMetadataValue, Key, parse_key};

    for batch in log.batches().await.iter().rev() {
        for record in batch.records.iter().rev() {
            if record.key.as_ref().is_some_and(|key| {
                matches!(
                    parse_key(key),
                    Ok(Key::GroupMetadata { group_id: ref id }) if id == "g"
                )
            }) {
                return GroupMetadataValue::decode_value(
                    record.value.as_deref().expect("classic metadata value"),
                )
                .expect("valid classic metadata");
            }
        }
    }
    panic!("classic metadata record not found")
}

/// A coordinator whose metadata image holds one topic `t` with `partitions`
/// partitions, so the reconciler can resolve a `t` subscription to real
/// topic-id/partitions and compute a target assignment.
pub(super) fn make_coordinator_with_topic(
    topic: &str,
    partitions: i32,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    make_coordinator_with_topic_policy(
        topic,
        partitions,
        crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::default(),
    )
}

/// As [`make_coordinator_with_topic`], but with an explicit migration
/// policy. Hosted-classic tests pin `Upgrade` so that the native member's
/// leave in `seed_and_upgrade` does NOT trigger a downgrade back to
/// classic, which would strand them on the wrong RPC path. The tests
/// exercise the downgrade trigger itself with `Bidirectional` and
/// `Downgrade`.
pub(super) fn make_coordinator_with_topic_policy(
    topic: &str,
    partitions: i32,
    policy: crate::coordinator::unified::config::ConsumerGroupMigrationPolicy,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    let topic_id = Uuid([7; 16]);
    let input = ReconcileInput {
        topic_id_by_name: [(topic.to_string(), topic_id)].into(),
        partitions_per_topic: [(topic_id, partitions)].into(),
        ..Default::default()
    };
    let metadata: Arc<dyn MetadataProvider> = Arc::new(StaticMetadata { input });
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = Arc::new(GroupCoordinator::new(
        NextGenConfig {
            migration_policy: policy,
            ..NextGenConfig::default()
        },
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        metadata,
        log.clone(),
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ));
    (coord, log)
}

// ── KIP-848: serving hosted classic members off the reconciler ─────

/// A real classic consumer client's `JoinGroup` protocol metadata: a
/// `ConsumerProtocolSubscription` with the leading version-negotiation
/// prefix.
pub(super) fn subscription_blob(topics: &[&str]) -> Bytes {
    use bytes::{BufMut, BytesMut};
    use krabka_protocol::{
        Encode, owned::consumer_protocol_subscription::ConsumerProtocolSubscription,
    };
    let sub = ConsumerProtocolSubscription {
        topics: topics.iter().map(|s| (*s).to_string()).collect(),
        ..Default::default()
    };
    let mut out = BytesMut::new();
    out.put_i16(0);
    sub.encode(&mut out, 0).unwrap();
    out.freeze()
}

/// Decode a `SyncGroup` assignment blob (version prefix + body) back into a
/// `ConsumerProtocolAssignment`.
pub(super) fn decode_assignment(
    blob: &Bytes,
) -> krabka_protocol::owned::consumer_protocol_assignment::ConsumerProtocolAssignment {
    use bytes::Buf;
    use krabka_protocol::{
        Decode, owned::consumer_protocol_assignment::ConsumerProtocolAssignment,
    };
    let mut cur = &blob[..];
    let version = cur.get_i16();
    ConsumerProtocolAssignment::decode(&mut cur, version).expect("assignment decodes")
}

/// Seeds a classic consumer group with member `m-classic` subscribed to
/// `topic`, then upgrades it in place with a native consumer heartbeat.
/// After this returns, the group is consumer-kind and `m-classic` has a
/// target.
pub(super) async fn seed_and_upgrade(
    coord: &Arc<GroupCoordinator>,
    topic: &str,
) -> Arc<GroupActorHandle> {
    use super::super::{
        classic_state::{ClassicGroup as ClassicState, Member},
        group::{CoordinatorGroup, GroupKind},
    };

    let mut cs = ClassicState::new("g");
    cs.protocol_type = Some("consumer".into());
    cs.generation_id = 1;
    cs.add_member(Member::new(
        "m-classic",
        "client",
        "127.0.0.1",
        std::time::Duration::from_secs(30),
        std::time::Duration::from_mins(1),
        vec![("range".into(), subscription_blob(&[topic]))],
    ));
    let group = Box::new(CoordinatorGroup {
        empty_since_ms: None,
        group_id: "g".into(),
        kind: GroupKind::Classic(cs),
        committed_offsets: HashMap::new(),
    });
    coord.seed_classic("g", group);
    let handle = coord.find("g").expect("seeded classic actor");

    // Native consumer heartbeat triggers the in-place upgrade and the
    // reconcile that gives m-classic a target.
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec![topic.into()]),
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

    // The native heartbeat minted a transient consumer member to drive the
    // upgrade. Have it leave so the group hosts only the classic member(s)
    // under test — otherwise it would claim a share of the partitions.
    let native_id = resp.member_id.expect("native member id");
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: native_id,
                member_epoch: -1,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    assert!(rx.await.unwrap().error_code == codes::NONE);
    handle
}

/// `true` if and only if some appended record WRITES a classic k2
/// `GroupMetadata` for `group_id` with a non-null value.
pub(super) async fn log_has_classic_group_metadata_write(
    log: &InMemoryOffsetsLog,
    group_id: &str,
) -> bool {
    use crate::coordinator::unified::persistence::{Key, parse_key};
    log.batches().await.iter().any(|batch| {
        batch.records.iter().any(|rec| {
            rec.value.is_some()
                && rec.key.as_ref().is_some_and(|k| {
                    matches!(
                        parse_key(k),
                        Ok(Key::GroupMetadata { group_id: ref gid }) if gid == group_id
                    )
                })
        })
    })
}

/// Seeds a classic consumer group "g" with a single classic member
/// `member_id` subscribed to `topic`, and with an optional KIP-345 static
/// `group_instance_id`. It mirrors the inline seeding that the upgrade and
/// downgrade tests use, but it takes parameters, so a static-identity test
/// can attach an instance id. The fixed `m-classic` in `seed_and_upgrade`
/// cannot do that.
pub(super) fn seed_classic_member(
    coord: &Arc<GroupCoordinator>,
    member_id: &str,
    topic: &str,
    instance_id: Option<&str>,
) -> Arc<GroupActorHandle> {
    use super::super::{
        classic_state::{ClassicGroup as ClassicState, Member},
        group::{CoordinatorGroup, GroupKind},
    };

    let mut cs = ClassicState::new("g");
    cs.protocol_type = Some("consumer".into());
    cs.generation_id = 1;
    cs.add_member(
        Member::new(
            member_id,
            "client",
            "127.0.0.1",
            std::time::Duration::from_secs(30),
            std::time::Duration::from_mins(1),
            vec![("range".into(), subscription_blob(&[topic]))],
        )
        .with_instance_id(instance_id.map(str::to_string)),
    );
    let group = Box::new(CoordinatorGroup {
        empty_since_ms: None,
        group_id: "g".into(),
        kind: GroupKind::Classic(cs),
        committed_offsets: HashMap::new(),
    });
    coord.seed_classic("g", group);
    coord.find("g").expect("seeded classic actor")
}
