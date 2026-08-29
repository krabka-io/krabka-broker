//! The classic `LeaveGroup` transition.
//!
//! `handle_leave` resolves each requested identity through the KIP-345 static
//! instance index or the member index, removes the members it resolved, and
//! reopens a rebalance when a survivor remains in a `Stable` group. It returns
//! the per-member responses in request order.

use std::time::Instant;

use krabka_protocol::owned::{
    leave_group_request::LeaveGroupRequest, leave_group_response::MemberResponse,
};

use crate::{
    codes,
    coordinator::unified::classic_state::{ClassicGroup as ClassicState, GroupState},
};

struct MemberIdentityIn {
    member_id: String,
    group_instance_id: Option<String>,
}

/// Port of `handlers/leave_group.rs`. It removes the resolved members. If the
/// group was `Stable` and members survive, it reopens a rebalance and sets the
/// deadline. It returns the per-member responses.
pub(crate) fn handle_leave(
    state: &mut ClassicState,
    req: &LeaveGroupRequest,
    version: i16,
) -> Vec<MemberResponse> {
    let inputs: Vec<MemberIdentityIn> = if version >= 3 {
        req.members
            .iter()
            .map(|m| MemberIdentityIn {
                member_id: m.member_id.clone(),
                group_instance_id: m.group_instance_id.clone(),
            })
            .collect()
    } else {
        vec![MemberIdentityIn {
            member_id: req.member_id.clone(),
            group_instance_id: None,
        }]
    };

    let mut member_responses: Vec<MemberResponse> = Vec::with_capacity(inputs.len());
    let mut any_removed = false;
    for ident in &inputs {
        let (resolved_id, code): (Option<String>, i16) =
            match (ident.group_instance_id.as_deref(), ident.member_id.as_str()) {
                (Some(iid), "") => match state.current_member_id_for_instance(iid) {
                    Some(pinned) => (Some(pinned.to_string()), codes::NONE),
                    None => (None, codes::UNKNOWN_MEMBER_ID),
                },
                (Some(iid), mid) => match state.current_member_id_for_instance(iid) {
                    Some(pinned) if pinned == mid => (Some(pinned.to_string()), codes::NONE),
                    Some(_) => (None, codes::FENCED_INSTANCE_ID),
                    None => (None, codes::UNKNOWN_MEMBER_ID),
                },
                (None, mid) => {
                    if state.members.contains_key(mid) {
                        (Some(mid.to_string()), codes::NONE)
                    } else {
                        (None, codes::UNKNOWN_MEMBER_ID)
                    }
                }
            };
        if let Some(id) = resolved_id {
            state.remove_member(&id);
            any_removed = true;
        }
        member_responses.push(MemberResponse {
            member_id: ident.member_id.clone(),
            group_instance_id: ident.group_instance_id.clone(),
            error_code: code,
            ..Default::default()
        });
    }
    if any_removed && !state.members.is_empty() && matches!(state.state, GroupState::Stable) {
        state.state = GroupState::PreparingRebalance;
        // A member left a live group: this is a membership-change rebalance,
        // not a start-from-empty herd, so the survivors eager-complete.
        state.rebalance_from_empty = false;
        state.rebalance_deadline = Some(
            Instant::now()
                + state
                    .members
                    .values()
                    .map(|m| m.rebalance_timeout)
                    .max()
                    .expect("nonempty group has a rebalance timeout"),
        );
    }
    member_responses
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::owned::leave_group_request::MemberIdentity;

    use super::*;
    use crate::coordinator::unified::classic_ops::test_support::{
        handle_join, join_req, stable_two_member_group,
    };

    #[test]
    fn leave_v2_single_member_removed() {
        let mut g = stable_two_member_group();
        g.state = GroupState::Stable;
        let req = LeaveGroupRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            ..Default::default()
        };
        let out = handle_leave(&mut g, &req, 2);
        assert!(out.len() == 1);
        check!(out[0].error_code == codes::NONE);
        check!(!g.members.contains_key("m1"));
        // Surviving member + was Stable → reopened a rebalance.
        check!(g.state == GroupState::PreparingRebalance);
    }

    #[test]
    fn leave_v3_list_with_instance_resolution_and_unknown() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", Some("inst-a")), "h");
        let req = LeaveGroupRequest {
            group_id: "g".into(),
            members: vec![
                MemberIdentity {
                    member_id: String::new(),
                    group_instance_id: Some("inst-a".into()),
                    ..Default::default()
                },
                MemberIdentity {
                    member_id: "ghost".into(),
                    group_instance_id: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let out = handle_leave(&mut g, &req, 3);
        assert!(out.len() == 2);
        check!(out[0].error_code == codes::NONE); // resolved via instance index
        check!(out[1].error_code == codes::UNKNOWN_MEMBER_ID);
        check!(!g.members.contains_key("m1"));
    }
}
