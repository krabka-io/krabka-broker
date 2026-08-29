//! Client-side drivers for the classic (`JoinGroup`/`SyncGroup`) side of the
//! upgrade scenarios.
//!
//! A classic group is what both scenarios start from, and forming one takes the
//! `MEMBER_ID_REQUIRED` two-step plus a leader `SyncGroup`, so that sequence is
//! kept here apart from the streams-side heartbeat drivers.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use krabka_client_core::Client;
use krabka_protocol::owned::{
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
};

use crate::upgrade_harness::{ERR_MEMBER_ID_REQUIRED, ERR_NONE};

pub fn join_request(group_id: &str, member_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group_id.to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 30_000,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".to_string(),
            metadata: Bytes::from_static(b""),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Drive the `JoinGroup` two-step (`MEMBER_ID_REQUIRED` + re-join) then `SyncGroup`.
/// Returns `(member_id, generation_id)`. The caller is the sole member so it
/// is also the leader and supplies a trivial self-assignment in `SyncGroup`.
pub async fn classic_join_sync(client: &Client, group_id: &str) -> (String, i32) {
    // Round 1: empty member_id → broker mints one and returns MEMBER_ID_REQUIRED.
    let r1 = tokio::time::timeout(
        Duration::from_secs(5),
        client.send(join_request(group_id, "")),
    )
    .await
    .expect("JoinGroup1 timeout")
    .expect("JoinGroup1");
    assert!(
        r1.error_code == ERR_MEMBER_ID_REQUIRED,
        "expected MEMBER_ID_REQUIRED, got {r1:?}"
    );
    let member_id = r1.member_id.clone();
    assert!(!member_id.is_empty());

    // Round 2: rejoin with assigned member_id — broker blocks for the
    // initial-rebalance-delay then returns as sole leader.
    let r2 = tokio::time::timeout(
        Duration::from_secs(10),
        client.send(join_request(group_id, &member_id)),
    )
    .await
    .expect("JoinGroup2 timeout")
    .expect("JoinGroup2");
    assert!(
        r2.error_code == ERR_NONE,
        "second JoinGroup must succeed, got {r2:?}"
    );
    let generation_id = r2.generation_id;

    // SyncGroup: sole leader supplies its own assignment.
    let r3 = client
        .send(SyncGroupRequest {
            group_id: group_id.to_string(),
            generation_id,
            member_id: member_id.clone(),
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: member_id.clone(),
                assignment: Bytes::from_static(b""),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("SyncGroup");
    assert!(
        r3.error_code == ERR_NONE,
        "SyncGroup must succeed, got {r3:?}"
    );

    (member_id, generation_id)
}
