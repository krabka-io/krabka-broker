//! Classic-membership validation for an `OffsetCommit`.
//!
//! `validate_commit` decides whether a commit may proceed against the group's
//! current membership and generation. It lets a simple consumer through
//! untouched, and it returns the Kafka error code to reject with when the
//! member, the static instance, or the generation does not match.

use crate::{codes, coordinator::unified::classic_state::ClassicGroup as ClassicState};

/// Port of `offset_commit::validate`. It returns `Some(code)` to reject.
pub(crate) fn validate_commit(
    state: &ClassicState,
    member_id: &str,
    group_instance_id: Option<&str>,
    generation_id: i32,
) -> Option<i16> {
    if member_id.is_empty() && group_instance_id.is_none() {
        return None; // simple consumer
    }
    if let Some(iid) = group_instance_id {
        match state.current_member_id_for_instance(iid) {
            None => return Some(codes::UNKNOWN_MEMBER_ID),
            Some(pinned) => {
                if !member_id.is_empty() && pinned != member_id {
                    return Some(codes::FENCED_INSTANCE_ID);
                }
            }
        }
    } else if !state.members.contains_key(member_id) {
        return Some(codes::UNKNOWN_MEMBER_ID);
    }
    if state.generation_id != generation_id {
        return Some(codes::ILLEGAL_GENERATION);
    }
    None
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::{
        classic_ops::test_support::stable_two_member_group, classic_state::GroupState,
    };

    #[test]
    fn validate_commit_branches() {
        let mut g = stable_two_member_group();
        g.state = GroupState::Stable;
        for (member, instance, gen_id, want) in [
            // Simple consumer (no member, no instance) → allowed.
            ("", None, -1, None),
            // Unknown member → UNKNOWN_MEMBER_ID.
            (
                "ghost",
                None,
                g.generation_id,
                Some(codes::UNKNOWN_MEMBER_ID),
            ),
            // Wrong generation → ILLEGAL_GENERATION.
            (
                "m1",
                None,
                g.generation_id + 9,
                Some(codes::ILLEGAL_GENERATION),
            ),
            // Correct → allowed.
            ("m1", None, g.generation_id, None),
            // Instance set but unknown → UNKNOWN_MEMBER_ID.
            (
                "",
                Some("nope"),
                g.generation_id,
                Some(codes::UNKNOWN_MEMBER_ID),
            ),
        ] {
            assert!(validate_commit(&g, member, instance, gen_id) == want);
        }
    }
}
