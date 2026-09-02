//! Pure admission policy shared by coordinator replay adapters.
//!
//! Key decoding selects the record kind. Value decoding must report the same
//! kind before a write can mutate state. Child records never create a group,
//! and per-member assignments never create a member. These rules keep a late
//! tombstone dominant until a later group or member record explicitly rebuilds
//! the parent object.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReplayRecordKind {
    GroupMetadata,
    MemberMetadata,
    Topology,
    PartitionMetadata,
    TargetAssignmentMetadata,
    TargetAssignmentMember,
    CurrentMemberAssignment,
    StatePartitionMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReplayMutation {
    Apply,
    Ignore,
    RemoveField,
    RemoveGroup,
    Reject,
}

/// Decide whether one decoded log record may mutate the replay projection.
///
/// `value_kind = None` represents a tombstone. A present value must have the
/// same statically selected decoder kind as its key. `group_exists` and
/// `member_exists` are read from the projection being updated.
#[must_use]
pub(crate) fn replay_mutation(
    key_kind: ReplayRecordKind,
    value_kind: Option<ReplayRecordKind>,
    group_exists: bool,
    member_exists: bool,
) -> ReplayMutation {
    let Some(value_kind) = value_kind else {
        return if key_kind == ReplayRecordKind::GroupMetadata {
            ReplayMutation::RemoveGroup
        } else if group_exists {
            ReplayMutation::RemoveField
        } else {
            ReplayMutation::Ignore
        };
    };

    if value_kind != key_kind {
        return ReplayMutation::Reject;
    }
    match key_kind {
        ReplayRecordKind::GroupMetadata => ReplayMutation::Apply,
        ReplayRecordKind::TargetAssignmentMember | ReplayRecordKind::CurrentMemberAssignment => {
            if group_exists && member_exists {
                ReplayMutation::Apply
            } else {
                ReplayMutation::Ignore
            }
        }
        ReplayRecordKind::MemberMetadata
        | ReplayRecordKind::Topology
        | ReplayRecordKind::PartitionMetadata
        | ReplayRecordKind::TargetAssignmentMetadata
        | ReplayRecordKind::StatePartitionMetadata => {
            if group_exists {
                ReplayMutation::Apply
            } else {
                ReplayMutation::Ignore
            }
        }
    }
}

#[must_use]
pub(crate) fn replay_write_is_admissible(
    kind: ReplayRecordKind,
    group_exists: bool,
    member_exists: bool,
) -> bool {
    replay_mutation(kind, Some(kind), group_exists, member_exists) == ReplayMutation::Apply
}

/// Epoch-bearing records accept nonnegative monotonic values. Equality admits
/// an exact retry without changing the result; `i32::MAX` needs no arithmetic.
#[must_use]
pub(crate) const fn replay_epoch_is_admissible(current: i32, incoming: i32) -> bool {
    incoming >= 0 && incoming >= current
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::{ReplayMutation, ReplayRecordKind, replay_epoch_is_admissible, replay_mutation};

    #[test]
    fn binding_parentage_and_epoch_boundaries_fail_closed() {
        use ReplayMutation::{Apply, Ignore, Reject, RemoveField, RemoveGroup};
        use ReplayRecordKind::{CurrentMemberAssignment, GroupMetadata, MemberMetadata, Topology};

        check!(replay_mutation(GroupMetadata, None, true, true) == RemoveGroup);
        check!(replay_mutation(MemberMetadata, None, true, true) == RemoveField);
        check!(replay_mutation(MemberMetadata, Some(MemberMetadata), false, false) == Ignore);
        check!(
            replay_mutation(
                CurrentMemberAssignment,
                Some(CurrentMemberAssignment),
                true,
                false,
            ) == Ignore
        );
        check!(replay_mutation(Topology, Some(Topology), true, false) == Apply);
        assert!(replay_mutation(Topology, Some(MemberMetadata), true, true) == Reject);

        check!(!replay_epoch_is_admissible(0, -1));
        check!(!replay_epoch_is_admissible(4, 3));
        check!(replay_epoch_is_admissible(4, 4));
        assert!(replay_epoch_is_admissible(i32::MAX, i32::MAX));
    }
}
