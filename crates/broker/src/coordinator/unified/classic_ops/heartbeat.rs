//! The classic `Heartbeat` transition.
//!
//! `handle_heartbeat` fences the request against the static-instance index, the
//! member index, the generation, and the group state, and refreshes the
//! member's `last_heartbeat` when all four checks pass.

use std::time::Instant;

use krabka_protocol::owned::heartbeat_request::HeartbeatRequest;

use crate::{
    codes,
    coordinator::unified::classic_state::{ClassicGroup as ClassicState, GroupState},
};

/// Port of `handlers/heartbeat.rs`. It returns the error code, and it refreshes
/// `last_heartbeat` on success.
pub(crate) fn handle_heartbeat(state: &mut ClassicState, req: &HeartbeatRequest) -> i16 {
    if let Err(code) = state.validate_member(&req.member_id, req.group_instance_id.as_deref()) {
        code
    } else if state.generation_id != req.generation_id {
        codes::ILLEGAL_GENERATION
    } else if !matches!(state.state, GroupState::Stable) {
        codes::REBALANCE_IN_PROGRESS
    } else {
        state
            .members
            .get_mut(&req.member_id)
            .expect("contains_key checked above")
            .last_heartbeat = Instant::now();
        codes::NONE
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::classic_ops::test_support::stable_two_member_group;

    #[test]
    fn heartbeat_codes_cover_all_branches() {
        let mut g = stable_two_member_group();
        let hb = |member: &str, gen_id: i32| HeartbeatRequest {
            group_id: "g".into(),
            generation_id: gen_id,
            member_id: member.into(),
            ..Default::default()
        };
        let cur_gen = g.generation_id;
        // Not Stable yet → REBALANCE_IN_PROGRESS.
        assert!(handle_heartbeat(&mut g, &hb("m1", cur_gen)) == codes::REBALANCE_IN_PROGRESS);
        g.state = GroupState::Stable;
        for (member, gen_id, want) in [
            ("ghost", cur_gen, codes::UNKNOWN_MEMBER_ID),
            ("m1", cur_gen + 9, codes::ILLEGAL_GENERATION),
            ("m1", cur_gen, codes::NONE),
        ] {
            assert!(handle_heartbeat(&mut g, &hb(member, gen_id)) == want);
        }
        // KIP-345, as Kafka's `ClassicGroup.validateMember`: an instance id no
        // member holds (a static member whose session expired) is unknown, and
        // an instance id another member holds is fenced.
        g.static_members.insert("i2".into(), "m2".into());
        for (member, instance, want) in [
            ("m1", "i-expired", codes::UNKNOWN_MEMBER_ID),
            ("m1", "i2", codes::FENCED_INSTANCE_ID),
            ("m2", "i2", codes::NONE),
        ] {
            let req = HeartbeatRequest {
                group_instance_id: Some(instance.into()),
                ..hb(member, cur_gen)
            };
            assert!(
                handle_heartbeat(&mut g, &req) == want,
                "{member} {instance}"
            );
        }
    }
}
