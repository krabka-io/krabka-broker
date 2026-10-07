use super::*;
use crate::reconfiguration::{
    CurrentVoterSet, ReconfigurationLeadership, TargetMembership, TargetVoter, VoterChangeKind,
    VoterChangeRequest, VoterReconfigurationPlan,
};

mod oracle;
use oracle::{check_overlap, current, leading, target};
mod commit_waiter;
mod control_support;
mod sets;

fn generated_request(
    count: usize,
    bits: u16,
    operation: u8,
    membership: u8,
    version: u16,
    requested: u16,
) -> (
    ReconfigurationLeadership,
    CurrentVoterSet,
    VoterChangeRequest,
    TargetVoter,
) {
    let flag = |i: u32| bits & (1u16 << i) != 0;
    (
        ReconfigurationLeadership {
            is_leader: flag(0),
            no_pending_change: flag(1),
            epoch_committed: flag(2),
        },
        CurrentVoterSet {
            voter_count: count,
            kraft_version: version,
            latest_controls_committed: flag(3),
            all_voters_support_requested: flag(4),
        },
        VoterChangeRequest {
            kind: match operation {
                0 => VoterChangeKind::Add,
                1 => VoterChangeKind::Remove,
                2 => VoterChangeKind::Update,
                _ => VoterChangeKind::FinalizeKraftVersion,
            },
            requested_kraft_version: requested,
        },
        TargetVoter {
            membership: match membership {
                0 => TargetMembership::Absent,
                1 => TargetMembership::PresentUnknownDirectory,
                2 => TargetMembership::PresentSameDirectory,
                _ => TargetMembership::PresentOtherDirectory,
            },
            version_compatible: flag(5),
            caught_up: flag(6),
        },
    )
}
