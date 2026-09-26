//! The classic `SyncGroup` transition.
//!
//! `handle_sync` fences the request, installs the leader's assignments over
//! every current member, and returns a [`SyncAction`] that tells the actor
//! whether to reply, to park the follower until the leader arrives, or to drain
//! the parked followers. `read_sync_result` reads one member's installed
//! assignment back out.

use bytes::Bytes;
use krabka_protocol::owned::sync_group_request::SyncGroupRequest;

use crate::{
    codes,
    coordinator::unified::{
        actor::SyncResult,
        classic_state::{ClassicGroup as ClassicState, GroupState},
    },
};

/// What the actor should do with a `ClassicSync`.
pub(crate) enum SyncAction {
    /// Reply right away, after a validation error or for a follower while the
    /// group is `Stable`.
    Immediate(SyncResult),
    /// Park the follower until the leader's `SyncGroup` installs assignments.
    Park,
    /// The leader installed assignments. Reply with this result to the leader
    /// and drain the parked followers.
    LeaderInstalled(SyncResult),
}

/// Port of `handlers/sync_group.rs`. It operates on `ClassicState`.
pub(crate) fn handle_sync(state: &mut ClassicState, req: &SyncGroupRequest) -> SyncAction {
    let protocol_type = state.protocol_type.clone();
    let protocol_name = state.protocol_name.clone();

    // KIP-345 fence, as Kafka's `ClassicGroup.validateMember`.
    if let Err(code) = state.validate_member(&req.member_id, req.group_instance_id.as_deref()) {
        return SyncAction::Immediate(sync_err(code, protocol_type, protocol_name));
    }
    if state.generation_id != req.generation_id {
        return SyncAction::Immediate(sync_err(
            codes::ILLEGAL_GENERATION,
            protocol_type,
            protocol_name,
        ));
    }

    // Kafka's `classicGroupSyncToClassicGroup`: only the leader's
    // `SyncGroup` in `CompletingRebalance` installs assignments. In
    // `Stable` every member, the leader included, reads its current one; a
    // KIP-814 leader that skipped the assignment sends none.
    let is_leader = state.leader_id.as_deref() == Some(&req.member_id);
    if is_leader && state.state == GroupState::CompletingRebalance {
        let supplied: std::collections::HashMap<&str, &Bytes> = req
            .assignments
            .iter()
            .map(|a| (a.member_id.as_str(), &a.assignment))
            .collect();
        // Kafka installs an assignment for every current member. A leader may
        // omit a member from the request; that member gets an empty assignment
        // instead of retaining bytes from the previous generation.
        let assignments = state
            .members
            .keys()
            .map(|member_id| {
                (
                    member_id.clone(),
                    supplied
                        .get(member_id.as_str())
                        .map_or_else(Bytes::new, |assignment| (*assignment).clone()),
                )
            })
            .collect();
        state.install_assignments(assignments);
        SyncAction::LeaderInstalled(read_sync_result(
            state,
            &req.member_id,
            protocol_type,
            protocol_name,
        ))
    } else if matches!(
        state.state,
        GroupState::Stable | GroupState::PreparingRebalance
    ) {
        // `PreparingRebalance` answers `REBALANCE_IN_PROGRESS`.
        SyncAction::Immediate(read_sync_result(
            state,
            &req.member_id,
            protocol_type,
            protocol_name,
        ))
    } else {
        SyncAction::Park
    }
}

/// Read back one member's installed assignment. Mirrors `sync_group.rs` step 3.
/// It returns `REBALANCE_IN_PROGRESS` if the group is not `Stable`.
pub(crate) fn read_sync_result(
    state: &ClassicState,
    member_id: &str,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
) -> SyncResult {
    if !matches!(state.state, GroupState::Stable) {
        return sync_err(codes::REBALANCE_IN_PROGRESS, protocol_type, protocol_name);
    }
    let assignment = state
        .members
        .get(member_id)
        .and_then(|m| m.assignment.clone())
        .unwrap_or_default();
    SyncResult {
        error_code: codes::NONE,
        assignment,
        protocol_type,
        protocol_name,
    }
}

fn sync_err(code: i16, protocol_type: Option<String>, protocol_name: Option<String>) -> SyncResult {
    SyncResult {
        error_code: code,
        assignment: Bytes::new(),
        protocol_type,
        protocol_name,
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::owned::sync_group_request::SyncGroupRequestAssignment;

    use super::*;
    use crate::coordinator::unified::classic_ops::test_support::stable_two_member_group;

    fn sync_req(member_id: &str, generation: i32) -> SyncGroupRequest {
        SyncGroupRequest {
            group_id: "g".into(),
            generation_id: generation,
            member_id: member_id.into(),
            ..Default::default()
        }
    }

    #[test]
    fn sync_unknown_member_and_wrong_generation() {
        let mut g = stable_two_member_group();
        let cur_gen = g.generation_id;
        match handle_sync(&mut g, &sync_req("ghost", cur_gen)) {
            SyncAction::Immediate(r) => assert!(r.error_code == codes::UNKNOWN_MEMBER_ID),
            _ => panic!("expected UNKNOWN_MEMBER_ID"),
        }
        match handle_sync(&mut g, &sync_req("m1", cur_gen + 9)) {
            SyncAction::Immediate(r) => assert!(r.error_code == codes::ILLEGAL_GENERATION),
            _ => panic!("expected ILLEGAL_GENERATION"),
        }
        // KIP-345, as Kafka's `ClassicGroup.validateMember`: an instance id no
        // member holds (a static member whose session expired) is unknown, and
        // an instance id another member holds is fenced.
        g.static_members.insert("i2".into(), "m2".into());
        for (member, instance, want) in [
            ("m1", "i-expired", codes::UNKNOWN_MEMBER_ID),
            ("m1", "i2", codes::FENCED_INSTANCE_ID),
        ] {
            let req = SyncGroupRequest {
                group_instance_id: Some(instance.into()),
                ..sync_req(member, cur_gen)
            };
            match handle_sync(&mut g, &req) {
                SyncAction::Immediate(r) => assert!(r.error_code == want, "{member} {instance}"),
                _ => panic!("expected an immediate error for {member} {instance}"),
            }
        }
    }

    #[test]
    fn sync_leader_installs_follower_parks_then_reads() {
        let mut g = stable_two_member_group();
        let cur_gen = g.generation_id;
        let leader = g.leader_id.clone().unwrap();
        let follower = if leader == "m1" { "m2" } else { "m1" };
        // Follower before the leader syncs → Park (not yet Stable).
        assert!(matches!(
            handle_sync(&mut g, &sync_req(follower, cur_gen)),
            SyncAction::Park
        ));
        // Leader installs assignments.
        let mut req = sync_req(&leader, cur_gen);
        req.assignments = vec![
            SyncGroupRequestAssignment {
                member_id: leader.clone(),
                assignment: Bytes::from_static(b"L"),
                ..Default::default()
            },
            SyncGroupRequestAssignment {
                member_id: follower.into(),
                assignment: Bytes::from_static(b"F"),
                ..Default::default()
            },
        ];
        match handle_sync(&mut g, &req) {
            SyncAction::LeaderInstalled(r) => {
                assert!(r.error_code == codes::NONE);
                assert!(r.assignment == Bytes::from_static(b"L"));
            }
            _ => panic!("expected LeaderInstalled"),
        }
        assert!(g.state == GroupState::Stable);
        // Now the follower (re-sync) reads its assignment immediately.
        match handle_sync(&mut g, &sync_req(follower, cur_gen)) {
            SyncAction::Immediate(r) => assert!(r.assignment == Bytes::from_static(b"F")),
            _ => panic!("expected Immediate follower assignment"),
        }
    }

    #[test]
    fn sync_leader_clears_omitted_member_assignment() {
        let mut g = stable_two_member_group();
        let generation = g.generation_id;
        let leader = g.leader_id.clone().unwrap();
        let omitted = if leader == "m1" { "m2" } else { "m1" };
        g.members.get_mut(omitted).unwrap().assignment = Some(Bytes::from_static(b"stale"));

        let mut req = sync_req(&leader, generation);
        req.assignments = vec![SyncGroupRequestAssignment {
            member_id: leader,
            assignment: Bytes::from_static(b"leader"),
            ..Default::default()
        }];
        assert!(matches!(
            handle_sync(&mut g, &req),
            SyncAction::LeaderInstalled(_)
        ));

        check!(g.members[omitted].assignment.as_deref() == Some(&b""[..]));
    }

    #[test]
    fn read_sync_result_rebalance_in_progress_when_not_stable() {
        let mut g = stable_two_member_group(); // CompletingRebalance, not Stable
        let r = read_sync_result(&g, "m1", None, None);
        assert!(r.error_code == codes::REBALANCE_IN_PROGRESS);
        // Drive to Stable, then it returns NONE.
        let leader = g.leader_id.clone().unwrap();
        let cur_gen = g.generation_id;
        let mut req = sync_req(&leader, cur_gen);
        req.assignments = vec![SyncGroupRequestAssignment {
            member_id: leader.clone(),
            assignment: Bytes::new(),
            ..Default::default()
        }];
        let _ = handle_sync(&mut g, &req);
        let r = read_sync_result(&g, &leader, None, None);
        assert!(r.error_code == codes::NONE);
    }

    /// KIP-814: a leader that rejoined a `Stable` group with
    /// `skip_assignment` sends `SyncGroup` with no assignments, and reads
    /// its current one back instead of clearing every member's.
    #[test]
    fn leader_sync_in_stable_reads_current_assignment() {
        let mut g = stable_two_member_group();
        g.install_assignments(std::collections::HashMap::from([
            ("m1".to_string(), Bytes::from_static(b"L")),
            ("m2".to_string(), Bytes::from_static(b"F")),
        ]));
        let generation = g.generation_id;
        match handle_sync(&mut g, &sync_req("m1", generation)) {
            SyncAction::Immediate(r) => {
                check!(r.assignment == Bytes::from_static(b"L"));
            }
            _ => panic!("expected the leader's current assignment"),
        }
        check!(g.members["m2"].assignment.as_deref() == Some(&b"F"[..]));
    }
}
