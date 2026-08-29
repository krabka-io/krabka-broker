//! Tests for the reconstruction of the KIP-932 share-group and KIP-1071
//! streams-group seeds from their persisted records.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_protocol::records::RecordBatch;

use super::replay::{Replayed, apply_record, apply_tombstone};
use crate::coordinator::persistence;

/// A replay of a share-group's records must rebuild the cached seed, so
/// that a freshly-spawned actor restores the same membership after a
/// restart.
///
/// The records are the group metadata, the member metadata, the target
/// assignment, and the current assignment.
#[tokio::test]
async fn share_group_records_replay_into_seed() {
    use krabka_protocol::primitives::uuid::Uuid;

    use crate::coordinator::unified::{
        GroupCoordinator, offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
        share::persistence as sp,
    };

    #[derive(Debug)]
    struct EmptyMeta;
    impl crate::coordinator::unified::actor::MetadataProvider for EmptyMeta {
        fn snapshot(&self) -> ReconcileInput {
            ReconcileInput::default()
        }
    }

    let coord = Arc::new(GroupCoordinator::new(
        crate::coordinator::unified::config::NextGenConfig::default(),
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        Arc::new(EmptyMeta),
        Arc::new(InMemoryOffsetsLog::default()),
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ));

    let tid = Uuid([9; 16]);
    // Drive the same path bootstrap takes: parse_key on the encoded key,
    // then apply_record on the value bytes.
    let recs: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
        (
            sp::encode_share_key(&sp::ShareGroupKey::GroupMetadata {
                group_id: "sg".into(),
            }),
            sp::ShareGroupMetadataValue { epoch: 4 }.encode(),
        ),
        (
            sp::encode_share_key(&sp::ShareGroupKey::MemberMetadata {
                group_id: "sg".into(),
                member_id: "m1".into(),
            }),
            sp::ShareGroupMemberMetadataValue {
                rack_id: None,
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                subscribed_topic_names: vec!["t".into()],
            }
            .encode(),
        ),
        (
            sp::encode_share_key(&sp::ShareGroupKey::CurrentMemberAssignment {
                group_id: "sg".into(),
                member_id: "m1".into(),
            }),
            sp::ShareGroupCurrentMemberAssignmentValue {
                member_epoch: 4,
                assigned_partitions: vec![(tid, vec![0, 1])],
            }
            .encode(),
        ),
    ];
    let batch = RecordBatch::default();
    let mut acc = Replayed::default();
    for (k, v) in recs {
        let key = persistence::parse_key(&k).unwrap();
        apply_record(&coord, &mut acc, key, &v, &batch).unwrap();
    }

    // Type locked + seed reconstructed.
    assert!(coord.group_type("sg") == Some(crate::coordinator::unified::GroupType::Share));
    let seed = coord.cached_share_seed("sg").expect("seed cached");
    check!(seed.group_epoch == 4);
    check!(seed.members.contains_key("m1"));
    check!(seed.current_per_member["m1"].member_epoch == 4);

    // A member tombstone scrubs the member from the seed.
    let tomb_key =
        persistence::parse_key(&sp::encode_share_key(&sp::ShareGroupKey::MemberMetadata {
            group_id: "sg".into(),
            member_id: "m1".into(),
        }))
        .unwrap();
    apply_tombstone(&coord, tomb_key);
    let seed = coord.cached_share_seed("sg").expect("seed still present");
    assert!(!seed.members.contains_key("m1"), "tombstone removed member");
}

/// A replay of a streams-group's records must lock the group type to
/// Streams and rebuild the cached seed.
///
/// The records are the group metadata, the member metadata, and the
/// current assignment. A member tombstone removes that member from the
/// seed.
#[tokio::test]
async fn streams_group_records_replay_into_seed() {
    use std::collections::BTreeMap;

    use crate::coordinator::unified::{
        GroupCoordinator, offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
        streams::persistence as sp,
    };

    #[derive(Debug)]
    struct EmptyMeta;
    impl crate::coordinator::unified::actor::MetadataProvider for EmptyMeta {
        fn snapshot(&self) -> ReconcileInput {
            ReconcileInput::default()
        }
    }

    let coord = Arc::new(GroupCoordinator::new(
        crate::coordinator::unified::config::NextGenConfig::default(),
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        Arc::new(EmptyMeta),
        Arc::new(InMemoryOffsetsLog::default()),
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ));

    // Drive the same path bootstrap takes: parse_key on the encoded key,
    // then apply_record on the value bytes.
    let recs: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
        (
            sp::encode_streams_key(&sp::StreamsGroupKey::GroupMetadata {
                group_id: "stg".into(),
            }),
            sp::StreamsGroupMetadataValue { epoch: 7 }.encode(),
        ),
        (
            sp::encode_streams_key(&sp::StreamsGroupKey::MemberMetadata {
                group_id: "stg".into(),
                member_id: "m1".into(),
            }),
            sp::StreamsGroupMemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                process_id: "p1".into(),
                user_endpoint: None,
                client_tags: vec![],
                rebalance_timeout_ms: 60_000,
                topology_epoch: 2,
            }
            .encode(),
        ),
        (
            sp::encode_streams_key(&sp::StreamsGroupKey::CurrentMemberAssignment {
                group_id: "stg".into(),
                member_id: "m1".into(),
            }),
            sp::StreamsGroupCurrentMemberAssignmentValue {
                member_epoch: 7,
                previous_member_epoch: 6,
                state: 0,
                active: BTreeMap::from([("0".to_string(), vec![0, 1])]),
                standby: BTreeMap::new(),
                warmup: BTreeMap::new(),
                active_pending_revocation: BTreeMap::new(),
            }
            .encode(),
        ),
    ];
    let batch = RecordBatch::default();
    let mut acc = Replayed::default();
    for (k, v) in recs {
        let key = persistence::parse_key(&k).unwrap();
        apply_record(&coord, &mut acc, key, &v, &batch).unwrap();
    }

    // Type locked to Streams + seed reconstructed.
    assert!(coord.group_type("stg") == Some(crate::coordinator::unified::GroupType::Streams));
    let seed = coord.cached_streams_seed("stg").expect("seed cached");
    check!(seed.group_epoch == 7);
    check!(seed.members.contains_key("m1"));
    check!(seed.current_per_member["m1"].member_epoch == 7);

    // A member tombstone scrubs the member from the seed.
    let tomb_key = persistence::parse_key(&sp::encode_streams_key(
        &sp::StreamsGroupKey::MemberMetadata {
            group_id: "stg".into(),
            member_id: "m1".into(),
        },
    ))
    .unwrap();
    apply_tombstone(&coord, tomb_key);
    let seed = coord
        .cached_streams_seed("stg")
        .expect("seed still present");
    assert!(!seed.members.contains_key("m1"), "tombstone removed member");
}
