//! Tests for the group kind a replay settles on when a group moved between
//! the classic and the KIP-848 next-gen protocols.
//!
//! An upgrade writes next-gen records over a classic group, and a downgrade
//! tombstones them and writes a fresh classic record. Log compaction then
//! leaves only the last value per key, so the tests pin both the compacted
//! residue that must replay as classic and the stray next-gen write that
//! resurrects the group as a consumer.

use assert2::assert;
use krabka_protocol::records::RecordBatch;

use super::{
    replay::{Replayed, apply_record, apply_tombstone, finalize},
    test_support::{bare_coordinator, classic_group_record},
};
use crate::coordinator::persistence::{self, GroupMetadataValue};

/// PROBLEM A, the downgrade trap: a group that started classic, then was
/// UPGRADED to next-gen, then was DOWNGRADED back to classic must replay
/// as a CLASSIC group and not as an empty next-gen group.
///
/// The downgrade drops the k3 `GroupMetadata` with a tombstone. Replay
/// must remove the next-gen seed completely, so that the fresh k2 record
/// that comes later rebuilds the classic group. Log order wins.
#[tokio::test]
async fn downgraded_group_replays_as_classic() {
    use crate::coordinator::unified::{
        GroupType, persistence_next_gen as ng, persistence_next_gen,
    };

    let coord = bare_coordinator();

    // Helper to encode a next-gen (group/member) record key.
    let ng_group_key = |gid: &str| {
        ng::encode_key(&ng::NextGenKey::GroupMetadata {
            group_id: gid.into(),
        })
    };
    let ng_member_key = |gid: &str, mid: &str| {
        ng::encode_key(&ng::NextGenKey::MemberMetadata {
            group_id: gid.into(),
            member_id: mid.into(),
        })
    };

    // Record stream in log order.
    let (k2_key, k2_val) = classic_group_record("g", "m1");
    let (k2_key2, k2_val2) = classic_group_record("g", "m1");
    let stream: Vec<(bytes::Bytes, Option<bytes::Bytes>)> = vec![
        // 1. initial classic group
        (k2_key, Some(k2_val)),
        // 2. upgrade drops k2 (tombstone)
        (GroupMetadataValue::encode_key("g"), None),
        // 3. upgrade: next-gen group metadata
        (
            ng_group_key("g"),
            Some(persistence_next_gen::GroupMetadataValue { epoch: 1 }.encode()),
        ),
        // 4. upgrade: next-gen member metadata
        (
            ng_member_key("g", "m1"),
            Some(
                persistence_next_gen::MemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "c1".into(),
                    client_host: "/127.0.0.1".into(),
                    subscribed_topic_names: vec!["t".into()],
                    subscribed_topic_regex: None,
                    server_assignor: None,
                    rebalance_timeout_ms: 60_000,
                    classic: None,
                }
                .encode(),
            ),
        ),
        // 5. downgrade drops k3 (next-gen group tombstone)
        (ng_group_key("g"), None),
        // 6. downgrade drops k5 (next-gen member tombstone)
        (ng_member_key("g", "m1"), None),
        // 7. downgrade writes a fresh k2 classic group
        (k2_key2, Some(k2_val2)),
    ];

    let batch = RecordBatch::default();
    let mut acc = Replayed::default();
    for (k, v) in stream {
        let key = persistence::parse_key(&k).unwrap();
        match v {
            Some(value) => apply_record(&coord, &mut acc, key, &value, &batch).unwrap(),
            None => apply_tombstone(&coord, &mut acc, key),
        }
    }
    finalize(&coord, acc);

    // The group must NOT be next-gen, and the classic describe path must
    // surface it with member "m1".
    assert!(coord.group_type("g") != Some(GroupType::NextGen));
    let snap = coord
        .describe_group("g")
        .await
        .expect("classic group present");
    assert!(snap.members.iter().any(|m| m.member_id == "m1"));
    // And there is no next-gen consumer actor for "g".
    assert!(
        coord
            .find("g")
            .is_some_and(|h| h.kind == crate::coordinator::unified::actor::GroupKindTag::Classic)
    );
}

/// PROBLEM A under LOG COMPACTION, the resurrection trap.
///
/// Take a downgraded group whose batch tombstoned the k3 `GroupMetadata`
/// but NOT the group-level k6 `TargetAssignmentMetadata`. After compaction
/// collects the tombstoned k3, a k6 write survives. `__consumer_offsets`
/// is compacted by default, so replay then sees the post-compaction
/// residue: the surviving k6 write and the fresh classic k2, with NO k3
/// and NO k3 tombstone.
///
/// `replay_target_assignment_metadata` calls
/// `seeds.entry(..).or_default()`, so that lone k6 re-creates a next-gen
/// seed. `finalize` then classifies the group as next-gen and drops the
/// classic k2. The group comes back as an empty next-gen consumer.
///
/// The fix tombstones k6 in the downgrade batch, so compaction keeps the
/// k6 TOMBSTONE, the last value per key, and not a stale write. This test
/// pins the corrected post-compaction shape and asserts that the group
/// replays CLASSIC.
#[tokio::test]
async fn compacted_downgrade_residue_replays_as_classic() {
    use crate::coordinator::unified::{GroupType, persistence_next_gen as ng};

    let coord = bare_coordinator();

    // Post-compaction record stream. Compaction keeps only the LAST value
    // per key, and the k3 + its tombstone both GC away (both gone), leaving:
    let (k2_key, k2_val) = classic_group_record("g", "m1");
    let stream: Vec<(bytes::Bytes, Option<bytes::Bytes>)> = vec![
        // The k6 TOMBSTONE the fix emits in the downgrade batch survives
        // compaction as the last value for the group-level k6 key. Replaying
        // a tombstone must NOT create a next-gen seed.
        (
            ng::encode_key(&ng::NextGenKey::TargetAssignmentMetadata {
                group_id: "g".into(),
            }),
            None,
        ),
        // The fresh classic k2 written by the downgrade.
        (k2_key, Some(k2_val)),
    ];

    let batch = RecordBatch::default();
    let mut acc = Replayed::default();
    for (k, v) in stream {
        let key = persistence::parse_key(&k).unwrap();
        match v {
            Some(value) => apply_record(&coord, &mut acc, key, &value, &batch).unwrap(),
            None => apply_tombstone(&coord, &mut acc, key),
        }
    }
    finalize(&coord, acc);

    // The group must replay CLASSIC, not resurrect as next-gen.
    assert!(coord.group_type("g") != Some(GroupType::NextGen));
    let snap = coord
        .describe_group("g")
        .await
        .expect("classic group present");
    assert!(snap.members.iter().any(|m| m.member_id == "m1"));
    assert!(
        coord
            .find("g")
            .is_some_and(|h| h.kind == crate::coordinator::unified::actor::GroupKindTag::Classic)
    );
}

/// Counterpoint to `compacted_downgrade_residue_replays_as_classic`.
///
/// WITHOUT the k6 tombstone, a surviving k6 WRITE re-creates a next-gen
/// seed, and the group wrongly comes back as next-gen. That is the bug
/// this work fixes. The test pins the hazard, so it catches a regression
/// that drops the k6 tombstone.
#[tokio::test]
async fn surviving_k6_write_resurrects_as_next_gen_without_fix() {
    use crate::coordinator::unified::persistence_next_gen as ng;

    let coord = bare_coordinator();
    let (k2_key, k2_val) = classic_group_record("g", "m1");
    let stream: Vec<(bytes::Bytes, Option<bytes::Bytes>)> = vec![
        // A surviving k6 WRITE (what compaction would retain if the
        // downgrade had NOT tombstoned k6).
        (
            ng::encode_key(&ng::NextGenKey::TargetAssignmentMetadata {
                group_id: "g".into(),
            }),
            Some(
                ng::TargetAssignmentMetadataValue {
                    assignment_epoch: 1,
                }
                .encode(),
            ),
        ),
        (k2_key, Some(k2_val)),
    ];

    let batch = RecordBatch::default();
    let mut acc = Replayed::default();
    for (k, v) in stream {
        let key = persistence::parse_key(&k).unwrap();
        match v {
            Some(value) => apply_record(&coord, &mut acc, key, &value, &batch).unwrap(),
            None => apply_tombstone(&coord, &mut acc, key),
        }
    }

    // The lone k6 write re-created a next-gen seed via the
    // `seeds.entry(..).or_default()` in `replay_target_assignment_metadata`
    // — the exact hazard the k6 tombstone prevents. `finalize` derives its
    // next-gen id set from `coordinator.seeds`, so this stray seed is what
    // makes it suppress the classic k2 reconstruction.
    assert!(coord.seeds.contains_key("g"));

    finalize(&coord, acc);

    // Resurrection: `finalize` spawned a CONSUMER (next-gen) actor for "g"
    // off that stray seed instead of the classic actor the k2 should have
    // produced. (Asserting the spawned actor's kind, set synchronously at
    // spawn, avoids the async `group_types` mark the actor records only as
    // it processes its seed.)
    assert!(
        coord
            .find("g")
            .is_some_and(|h| h.kind == crate::coordinator::unified::actor::GroupKindTag::Consumer)
    );
}

/// An upgrade-only replay, with k3 live and no tombstone after it, must
/// still give a CONSUMER, that is next-gen, group.
///
/// The test guards the PROBLEM A fix against an over-eager seed removal.
#[tokio::test]
async fn upgraded_group_without_tombstone_replays_as_consumer() {
    use crate::coordinator::unified::{
        GroupType, persistence_next_gen as ng, persistence_next_gen,
    };

    let coord = bare_coordinator();
    let stream: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
        (
            ng::encode_key(&ng::NextGenKey::GroupMetadata {
                group_id: "g".into(),
            }),
            persistence_next_gen::GroupMetadataValue { epoch: 1 }.encode(),
        ),
        (
            ng::encode_key(&ng::NextGenKey::MemberMetadata {
                group_id: "g".into(),
                member_id: "m1".into(),
            }),
            persistence_next_gen::MemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                subscribed_topic_names: vec!["t".into()],
                subscribed_topic_regex: None,
                server_assignor: None,
                rebalance_timeout_ms: 60_000,
                classic: None,
            }
            .encode(),
        ),
    ];
    let batch = RecordBatch::default();
    let mut acc = Replayed::default();
    for (k, v) in stream {
        let key = persistence::parse_key(&k).unwrap();
        apply_record(&coord, &mut acc, key, &v, &batch).unwrap();
    }
    finalize(&coord, acc);

    assert!(coord.group_type("g") != Some(GroupType::Classic));
    let handle = coord.find("g").expect("consumer actor present");
    assert!(handle.kind == crate::coordinator::unified::actor::GroupKindTag::Consumer);
}

/// PROBLEM B, the facade is not restored: a k5 `MemberMetadataValue` that
/// carries a `classic` block must rebuild the in-memory member's
/// `ClassicMemberFacade` on replay.
///
/// The replayed consumer group's member "m1" must report
/// `is_classic == true` in the next-gen `Describe` view.
#[tokio::test]
async fn member_with_classic_block_replays_facade() {
    use tokio::sync::oneshot;

    use crate::coordinator::unified::{
        actor::{GroupActorMessage, GroupKindTag},
        persistence_next_gen as ng, persistence_next_gen,
    };

    let coord = bare_coordinator();
    let stream: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
        (
            ng::encode_key(&ng::NextGenKey::GroupMetadata {
                group_id: "g".into(),
            }),
            persistence_next_gen::GroupMetadataValue { epoch: 2 }.encode(),
        ),
        (
            ng::encode_key(&ng::NextGenKey::MemberMetadata {
                group_id: "g".into(),
                member_id: "m1".into(),
            }),
            persistence_next_gen::MemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                subscribed_topic_names: vec!["t".into()],
                subscribed_topic_regex: None,
                server_assignor: None,
                rebalance_timeout_ms: 60_000,
                classic: Some(persistence_next_gen::ClassicMemberMetadata {
                    session_timeout_ms: 30_000,
                    supported_protocols: vec![("range".into(), bytes::Bytes::from_static(b"meta"))],
                    last_synced_assignment: bytes::Bytes::from_static(b"asn"),
                }),
            }
            .encode(),
        ),
    ];
    let batch = RecordBatch::default();
    let mut acc = Replayed::default();
    for (k, v) in stream {
        let key = persistence::parse_key(&k).unwrap();
        apply_record(&coord, &mut acc, key, &v, &batch).unwrap();
    }
    finalize(&coord, acc);

    let handle = coord.find("g").expect("consumer actor present");
    assert!(handle.kind == GroupKindTag::Consumer);
    let (tx, rx) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Describe { reply: tx })
        .await
        .unwrap();
    let view = rx.await.unwrap();
    let m1 = view
        .members
        .iter()
        .find(|m| m.member_id == "m1")
        .expect("member m1 present");
    assert!(m1.is_classic, "facade reconstructed from k5 classic block");
}
