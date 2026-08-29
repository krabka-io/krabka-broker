//! Tests for the rebuild of a classic group's members and state from a
//! persisted `GroupMetadata` value.

use assert2::{assert, check};

use super::apply::apply_group_metadata;
use crate::coordinator::{
    persistence::GroupMetadataValue,
    unified::classic_state::{ClassicGroup as ClassicState, GroupState as ClassicGroupState},
};

#[test]
fn apply_group_metadata_rebuilds_members_and_state() {
    use bytes::Bytes;

    use crate::coordinator::persistence::MemberMetadata;

    let mut g = ClassicState::new("g");
    let v = GroupMetadataValue {
        protocol_type: "consumer".into(),
        generation: 5,
        protocol_name: Some("range".into()),
        leader: Some("m1".into()),
        current_state_timestamp_ms: 0,
        members: vec![MemberMetadata {
            member_id: "m1".into(),
            group_instance_id: Some("inst".into()),
            client_id: "c".into(),
            client_host: "h".into(),
            rebalance_timeout_ms: 60_000,
            session_timeout_ms: 30_000,
            subscription: Bytes::new(),
            assignment: Bytes::from_static(b"asn"),
        }],
    };
    apply_group_metadata(&mut g, v, 0);
    check!(g.generation_id == 5);
    check!(g.protocol_type.as_deref() == Some("consumer"));
    check!(g.leader_id.as_deref() == Some("m1"));
    check!(g.state == ClassicGroupState::Stable);
    check!(g.members.contains_key("m1"));
    check!(g.members["m1"].assignment.as_deref() == Some(b"asn" as &[u8]));
    check!(g.current_member_id_for_instance("inst") == Some("m1"));

    // No members → Empty state.
    let mut empty = ClassicState::new("g2");
    apply_group_metadata(
        &mut empty,
        GroupMetadataValue {
            protocol_type: "consumer".into(),
            generation: 0,
            protocol_name: None,
            leader: None,
            current_state_timestamp_ms: 0,
            members: vec![],
        },
        0,
    );
    assert!(empty.state == ClassicGroupState::Empty);
}
