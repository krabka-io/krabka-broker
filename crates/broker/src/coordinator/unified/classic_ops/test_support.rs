//! Fixture builders shared by the unit tests of the `classic_ops` submodules.
//!
//! The builders drive a [`ClassicState`] through the real `handle_join` and
//! `try_complete` transitions, so a test in any submodule starts from a group
//! that the production code itself produced.

use std::time::Duration;

use bytes::Bytes;
use krabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};

use super::join::{JoinAction, try_complete};
use crate::coordinator::unified::classic_state::ClassicGroup as ClassicState;

pub(super) fn handle_join(
    state: &mut ClassicState,
    req: &JoinGroupRequest,
    client_host: &str,
) -> JoinAction {
    let mut req = req.clone();
    super::join::handle_join(
        state,
        &mut req,
        "client-a",
        client_host,
        true,
        Duration::from_secs(3),
    )
}

pub(super) fn join_req(member_id: &str, instance: Option<&str>) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: "g".into(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        member_id: member_id.into(),
        group_instance_id: instance.map(String::from),
        protocol_type: "consumer".into(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: Bytes::from_static(b"meta"),
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(super) fn stable_two_member_group() -> ClassicState {
    let mut g = ClassicState::new("g");
    let _ = handle_join(&mut g, &join_req("m1", None), "h");
    let _ = handle_join(&mut g, &join_req("m2", None), "h");
    try_complete(&mut g).unwrap(); // leader = m1 (min id), CompletingRebalance
    g
}
