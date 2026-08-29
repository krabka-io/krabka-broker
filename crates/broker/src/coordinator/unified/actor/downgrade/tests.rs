//! Unit tests for the KIP-848 downgrade trigger and the classic state it
//! restores.

use std::collections::HashMap;

use assert2::{assert, check};
use krabka_log::Offset;
use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

use crate::{
    codes,
    coordinator::unified::{
        actor::{
            GroupActorMessage, GroupKindTag,
            test_support::{
                decode_assignment, log_has_classic_group_metadata_write,
                make_coordinator_with_topic, make_coordinator_with_topic_policy, rpc,
                seed_classic_member, subscription_blob,
            },
        },
        classic_state::OffsetEntry,
    },
};

/// KIP-848 DOWNGRADE trigger: a consumer group that hosts a classic member
/// must flip back to classic in place when the LAST native consumer member
/// leaves, under the default `Bidirectional` policy. The flip tombstones
/// the next-gen k3 `GroupMetadata`, writes a classic k2, and re-expresses
/// the hosted classic member as a classic member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_consumer_member_leaving_downgrades_to_classic() {
    use crate::coordinator::unified::{
        classic_state::{ClassicGroup as ClassicState, Member},
        group::{CoordinatorGroup, GroupKind},
    };

    // Default policy is Bidirectional → downgrade is allowed.
    let (coord, log) = make_coordinator_with_topic("t", 2);

    // Seed a classic group with one classic member subscribed to "t".
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
    let group = Box::new(CoordinatorGroup {
        group_id: "g".into(),
        kind: GroupKind::Classic(cs),
        committed_offsets: HashMap::new(),
    });
    coord.seed_classic("g", group);
    let handle = coord.find("g").expect("seeded classic actor");

    // A native consumer heartbeat upgrades the group in place; it now hosts
    // the classic member AND the native consumer member.
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
    let native_id = resp.member_id.expect("native member id");

    // The native consumer member leaves (member_epoch == -1). It was the
    // only native member, so the group downgrades back to classic.
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

    // The group is now classic again. `describe_group` only returns
    // classic groups; it must surface "g" with the hosted classic member
    // re-expressed as a classic member.
    let snap = coord
        .describe_group("g")
        .await
        .expect("group downgraded to classic");
    // Exactly the hosted classic member remains (the departed native
    // member is gone), and the downgrade batch tombstoned the next-gen k3
    // GroupMetadata record AND the group-level k6 TargetAssignmentMetadata
    // record (which would otherwise survive log compaction and resurrect
    // the group as next-gen), and wrote a classic k2 GroupMetadata
    // (non-tombstone) for "g".
    check!(snap.members.len() == 1);
    check!(snap.members.iter().any(|m| m.member_id == "m-classic"));
    check!(log.has_next_gen_group_metadata_tombstone("g").await);
    check!(log.has_next_gen_target_metadata_tombstone("g").await);
    check!(log_has_classic_group_metadata_write(&log, "g").await);
}

/// Scenario 1: a full upgrade and downgrade round trip under
/// `Bidirectional`. A classic member "m1" joins. A native consumer "c1"
/// heartbeats, which upgrades the group. Then c1 leaves, which downgrades
/// it. The group must end CLASSIC with "m1" still present and still
/// assigned, with its partitions kept across both flips.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upgrade_then_downgrade_round_trip() {
    use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
    let (coord, _log) =
        make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);
    let handle = seed_classic_member(&coord, "m1", "t", None);

    // A native consumer "c1" heartbeats → in-place UPGRADE; the group is now
    // consumer-kind and hosts both m1 (classic facade) and c1.
    let up = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(up.error_code == codes::NONE);
    let c1 = up.member_id.expect("native member id");
    let describe = {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    };
    assert!(
        describe.members.len() == 2,
        "upgraded group hosts both m1 and c1"
    );

    // c1 leaves (member_epoch -1). It was the only native member → DOWNGRADE
    // back to classic.
    let leave = rpc::consumer_heartbeat(&handle, &c1, -1, None).await;
    assert!(leave.error_code == codes::NONE);

    // The group is classic again, with m1 restored and still assigned.
    let snap = coord
        .describe_group("g")
        .await
        .expect("group downgraded back to classic");
    assert!(
        snap.members.iter().any(|m| m.member_id == "m1"),
        "m1 must survive the upgrade→downgrade round trip"
    );
    let m1 = snap
        .members
        .iter()
        .find(|m| m.member_id == "m1")
        .expect("m1 present");
    // Decode the assignment blob and verify m1's partitions are preserved.
    // `!is_empty()` is insufficient — the blob always has a 2-byte version
    // prefix even if the partition list were empty.
    //
    // After upgrade, the range assignor splits {0,1} between m1 and c1;
    // m1 is assigned partition [1] (range assignor gives the higher range
    // to the lexicographically-later member when both subscribe to "t").
    // On downgrade, c1 (the only native member) has departed, so the
    // downgrade RE-RECONCILES over the surviving members BEFORE converting
    // to classic. m1 is now the sole member subscribed to "t", so the range
    // assignor gives it BOTH partitions — no partition is orphaned. Its
    // seed assignment in the restored classic group is therefore [0, 1].
    let assignment_bytes = bytes::Bytes::from(m1.assignment.clone());
    let decoded = decode_assignment(&assignment_bytes);
    let tp = decoded
        .assigned_partitions
        .iter()
        .find(|tp| tp.topic == "t")
        .expect("decoded assignment must contain topic t");
    let mut parts = tp.partitions.clone();
    parts.sort_unstable();
    assert!(
        parts == vec![0, 1],
        "m1 (sole surviving member) must own BOTH partitions after the downgrade re-reconcile; got {parts:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_of_last_native_member_triggers_downgrade() {
    use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;

    let (coord, _log) =
        make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);
    let handle = seed_classic_member(&coord, "m-classic", "t", None);
    let joined = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    check!(joined.error_code == codes::NONE);
    let native = joined.member_id.expect("native member id");

    let response = rpc::classic_leave(&handle, &native).await;
    check!(response.len() == 1);
    check!(response[0].error_code == codes::NONE);

    let view = rpc::classic_inspect(&handle).await;
    check!(view.members.len() == 1);
    check!(view.members[0].member_id.as_str() == "m-classic");
}

/// Scenario 2: KIP-345 static identity must survive both flips. A classic
/// member with `group.instance.id = "inst-a"` joins, then the group
/// upgrades, then it downgrades. The restored classic member must still
/// carry `group_instance_id == Some("inst-a")`. The test reads this from
/// the classic inspect view, because `MemberSnapshot` does not carry the
/// instance id but `ClassicMemberView` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_member_identity_survives_both_flips() {
    use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
    let (coord, _log) =
        make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);
    let handle = seed_classic_member(&coord, "m1", "t", Some("inst-a"));

    // Upgrade via a native consumer heartbeat, then downgrade by having that
    // native member leave.
    let up = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(up.error_code == codes::NONE);
    let native = up.member_id.expect("native member id");
    let leave = rpc::consumer_heartbeat(&handle, &native, -1, None).await;
    assert!(leave.error_code == codes::NONE);

    // The group is classic again; the restored member must still carry the
    // static identity (convert_classic_to_consumer maps instance_id and
    // convert_consumer_to_classic restores it).
    let view = rpc::classic_inspect(&handle).await;
    let m1 = view
        .members
        .iter()
        .find(|m| m.member_id == "m1")
        .expect("m1 restored as a classic member");
    assert!(
        m1.group_instance_id.as_deref() == Some("inst-a"),
        "the static identity must survive both flips"
    );
}

/// Scenario 3: under the `Disabled` policy a classic group stays classic.
/// The broker REJECTS a native consumer heartbeat for that group instead
/// of upgrading it. This reproduces the hard classic and next-gen
/// separation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_disabled_keeps_group_classic() {
    use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
    let (coord, _log) =
        make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Disabled);
    let handle = seed_classic_member(&coord, "m1", "t", None);

    // A native consumer heartbeat must be rejected (no upgrade is allowed).
    let resp = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(
        resp.error_code != codes::NONE,
        "Disabled policy must reject the upgrade heartbeat"
    );
    assert!(
        resp.error_code == codes::GROUP_ID_NOT_FOUND,
        "an un-upgradable classic group surfaces as GROUP_ID_NOT_FOUND"
    );

    // The group is untouched: still classic, still hosting m1.
    let view = rpc::classic_inspect(&handle).await;
    assert!(
        view.members.iter().any(|m| m.member_id == "m1"),
        "the group must remain classic with m1 intact"
    );
    assert!(handle.kind == GroupKindTag::Classic);
}

/// Scenario 4: committed offsets live on the kind-agnostic `Group`
/// container and must survive both flips unchanged. The test commits an
/// offset for ("t", 0) on a classic group, upgrades, asserts the offset is
/// still readable, downgrades, and asserts it is STILL there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_offsets_survive_a_flip() {
    use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
    let (coord, _log) =
        make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);
    let handle = seed_classic_member(&coord, "m1", "t", None);

    // Record a committed offset for ("t", 0) via the kind-agnostic path.
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::UpdateCommitted {
            entries: vec![(
                ("t".to_string(), 0),
                OffsetEntry {
                    offset: Offset(99),
                    leader_epoch: 3,
                    metadata: String::new(),
                    commit_timestamp_ms: 0,
                },
            )],
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap();

    // Upgrade → the offset must still be readable.
    let up = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(up.error_code == codes::NONE);
    let native = up.member_id.expect("native member id");
    let after_upgrade = rpc::fetch_committed(&handle).await;
    assert!(
        after_upgrade.get(&("t".to_string(), 0)).map(|e| e.offset) == Some(Offset(99)),
        "committed offset must survive the upgrade"
    );

    // Downgrade → the offset must STILL be there.
    let leave = rpc::consumer_heartbeat(&handle, &native, -1, None).await;
    assert!(leave.error_code == codes::NONE);
    let after_downgrade = rpc::fetch_committed(&handle).await;
    assert!(
        after_downgrade.get(&("t".to_string(), 0)).map(|e| e.offset) == Some(Offset(99)),
        "committed offset must survive the downgrade too"
    );
}

/// Regression (user-requested): a group that downgraded in place becomes
/// deletable. The test spawns a CONSUMER group, whose first RPC is a
/// `ConsumerGroupHeartbeat`, hosts a classic member, then downgrades when
/// the native consumer leaves.
///
/// The handle's spawn-time `kind` is a stale `Consumer`, but `delete_group`
/// dispatches on `ClassicDelete`'s live-kind reply. The downgraded group
/// answers as classic, so a non-empty group reports `NonEmpty` and NOT
/// `NotFound`. That proves delete sees it as classic. Before the refactor,
/// the stale `handle.kind == Consumer` gate short-circuited to
/// `NotFound`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downgraded_group_is_deletable_once_empty() {
    use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
    let (coord, _log) =
        make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);

    // SPAWN consumer-kind; host a classic member; downgrade.
    let handle = coord.get_or_create_consumer("g");
    assert!(handle.kind == GroupKindTag::Consumer);
    let up = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(up.error_code == codes::NONE);
    let native = up.member_id.expect("native member id");
    let join = rpc::classic_join(&handle, "m-classic", "t").await;
    assert!(join.error_code == codes::NONE);
    let leave = rpc::consumer_heartbeat(&handle, &native, -1, None).await;
    assert!(leave.error_code == codes::NONE);

    // Barrier: only a classic-kind group answers `ClassicInspect`, so this
    // round-trip guarantees the downgrade completed. The lone hosted classic
    // member keeps it non-empty.
    let view = rpc::classic_inspect(&handle).await;
    check!(view.members.iter().any(|m| m.member_id == "m-classic"));

    // The spawn-time kind is the stale `Consumer`; delete must not consult
    // it. A non-empty downgraded (live-classic) group reports `NonEmpty`,
    // NOT `NotFound` — proving delete sees it as classic.
    check!(handle.kind == GroupKindTag::Consumer);
    check!(
        coord.delete_group("g").await == Err(crate::coordinator::DeleteGroupError::NonEmpty),
        "a downgraded non-empty group must report NonEmpty (seen as classic), \
         not the stale-handle.kind NotFound"
    );

    // Drain the last hosted classic member so the group is empty, then it
    // must be deletable.
    let resp = rpc::classic_leave(&handle, "m-classic").await;
    assert!(!resp.is_empty());
    let view = rpc::classic_inspect(&handle).await;
    assert!(
        view.members.is_empty(),
        "the group must be empty after the classic member leaves"
    );
    assert!(
        coord.delete_group("g").await == Ok(()),
        "an empty downgraded group must be deletable"
    );
}
