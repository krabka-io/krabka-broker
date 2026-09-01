//! `KRaft` voter-reconfiguration admission and result shape.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum VoterChangeKind {
    Add,
    Remove,
    Update,
    FinalizeKraftVersion,
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct VoterReconfigurationPlan {
    pub next_voter_count: usize,
    pub next_kraft_version: u16,
    pub write_voters: bool,
    pub write_kraft_version: bool,
    pub preflight_only: bool,
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum VoterReconfigurationDecision {
    NotLeader,
    InProgress,
    EpochUncommitted,
    EmptyCurrentVoterSet,
    UnsupportedKraftVersion,
    DuplicateVoter,
    IncompatibleVoter,
    VoterNotCaughtUp,
    VoterNotFound,
    DirectoryMismatch,
    LastVoter,
    InvalidVersionTransition,
    Admit(VoterReconfigurationPlan),
}

/// Admit one fresh KIP-853 operation and construct its exact result shape.
#[ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        plan.next_voter_count@ > 0 && plan.next_kraft_version@ <= 1,
    _ => true,
})]
#[ensures(match (kind, result) {
    (VoterChangeKind::Add, VoterReconfigurationDecision::Admit(plan)) =>
        !target_present && target_version_compatible && target_caught_up
            && current_kraft_version@ == 1
            && plan.next_voter_count@ == current_voter_count@ + 1
            && plan.next_kraft_version@ == current_kraft_version@
            && plan.write_voters && !plan.write_kraft_version && !plan.preflight_only,
    (VoterChangeKind::Remove, VoterReconfigurationDecision::Admit(plan)) =>
        target_present && directory_matches && current_voter_count@ > 1
            && current_kraft_version@ == 1
            && plan.next_voter_count@ + 1 == current_voter_count@
            && plan.next_kraft_version@ == current_kraft_version@
            && plan.write_voters && !plan.write_kraft_version && !plan.preflight_only,
    (VoterChangeKind::Update, VoterReconfigurationDecision::Admit(plan)) =>
        target_present && directory_matches && target_version_compatible
            && plan.next_voter_count@ == current_voter_count@
            && plan.next_kraft_version@ == current_kraft_version@
            && (if current_kraft_version@ == 0 {
                !plan.write_voters && !plan.write_kraft_version && plan.preflight_only
            } else {
                plan.write_voters && !plan.write_kraft_version && !plan.preflight_only
            }),
    (VoterChangeKind::FinalizeKraftVersion, VoterReconfigurationDecision::Admit(plan)) =>
        current_kraft_version@ == 0 && requested_kraft_version@ == 1
            && all_voters_support_v1
            && plan.next_voter_count@ == current_voter_count@
            && plan.next_kraft_version@ == 1
            && plan.write_voters && plan.write_kraft_version && !plan.preflight_only,
    _ => true,
})]
#[must_use]
#[allow(
    clippy::fn_params_excessive_bools,
    clippy::too_many_arguments,
    reason = "each argument is one proved admission fact"
)]
pub fn voter_reconfiguration_decision(
    is_leader: bool,
    single_flight_clear: bool,
    epoch_committed: bool,
    kind: VoterChangeKind,
    current_voter_count: usize,
    current_kraft_version: u16,
    requested_kraft_version: u16,
    target_present: bool,
    directory_matches: bool,
    target_version_compatible: bool,
    target_caught_up: bool,
    all_voters_support_v1: bool,
) -> VoterReconfigurationDecision {
    if !is_leader {
        return VoterReconfigurationDecision::NotLeader;
    }
    if !single_flight_clear {
        return VoterReconfigurationDecision::InProgress;
    }
    if !epoch_committed {
        return VoterReconfigurationDecision::EpochUncommitted;
    }
    if current_voter_count == 0 {
        return VoterReconfigurationDecision::EmptyCurrentVoterSet;
    }
    if current_kraft_version > 1 {
        return VoterReconfigurationDecision::InvalidVersionTransition;
    }

    match kind {
        VoterChangeKind::Add => {
            if current_kraft_version != 1 {
                return VoterReconfigurationDecision::UnsupportedKraftVersion;
            }
            if target_present {
                return VoterReconfigurationDecision::DuplicateVoter;
            }
            if !target_version_compatible {
                return VoterReconfigurationDecision::IncompatibleVoter;
            }
            if !target_caught_up {
                return VoterReconfigurationDecision::VoterNotCaughtUp;
            }
            let Some(next_voter_count) = current_voter_count.checked_add(1) else {
                return VoterReconfigurationDecision::InvalidVersionTransition;
            };
            VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
                next_voter_count,
                next_kraft_version: current_kraft_version,
                write_voters: true,
                write_kraft_version: false,
                preflight_only: false,
            })
        }
        VoterChangeKind::Remove => {
            if current_kraft_version != 1 {
                return VoterReconfigurationDecision::UnsupportedKraftVersion;
            }
            if !target_present {
                return VoterReconfigurationDecision::VoterNotFound;
            }
            if !directory_matches {
                return VoterReconfigurationDecision::DirectoryMismatch;
            }
            if current_voter_count == 1 {
                return VoterReconfigurationDecision::LastVoter;
            }
            VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
                next_voter_count: current_voter_count - 1,
                next_kraft_version: current_kraft_version,
                write_voters: true,
                write_kraft_version: false,
                preflight_only: false,
            })
        }
        VoterChangeKind::Update => {
            if !target_present {
                return VoterReconfigurationDecision::VoterNotFound;
            }
            if !directory_matches {
                return VoterReconfigurationDecision::DirectoryMismatch;
            }
            if !target_version_compatible {
                return VoterReconfigurationDecision::IncompatibleVoter;
            }
            let preflight_only = current_kraft_version == 0;
            VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
                next_voter_count: current_voter_count,
                next_kraft_version: current_kraft_version,
                write_voters: !preflight_only,
                write_kraft_version: false,
                preflight_only,
            })
        }
        VoterChangeKind::FinalizeKraftVersion => {
            if current_kraft_version != 0 || requested_kraft_version != 1 {
                return VoterReconfigurationDecision::InvalidVersionTransition;
            }
            if !all_voters_support_v1 {
                return VoterReconfigurationDecision::IncompatibleVoter;
            }
            VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
                next_voter_count: current_voter_count,
                next_kraft_version: 1,
                write_voters: true,
                write_kraft_version: true,
                preflight_only: false,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn decide(kind: VoterChangeKind) -> VoterReconfigurationDecision {
        voter_reconfiguration_decision(
            true, true, true, kind, 3, 1, 1, false, true, true, true, true,
        )
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one table enumerates every distinct rejection outcome"
    )]
    fn every_rejection_reason_is_ordered_and_specific() {
        let cases = [
            (
                voter_reconfiguration_decision(
                    false,
                    true,
                    true,
                    VoterChangeKind::Add,
                    3,
                    1,
                    1,
                    false,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::NotLeader,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    false,
                    true,
                    VoterChangeKind::Add,
                    3,
                    1,
                    1,
                    false,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::InProgress,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    false,
                    VoterChangeKind::Add,
                    3,
                    1,
                    1,
                    false,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::EpochUncommitted,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Add,
                    0,
                    1,
                    1,
                    false,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::EmptyCurrentVoterSet,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Add,
                    3,
                    0,
                    1,
                    false,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::UnsupportedKraftVersion,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Add,
                    3,
                    1,
                    1,
                    true,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::DuplicateVoter,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Add,
                    3,
                    1,
                    1,
                    false,
                    true,
                    false,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::IncompatibleVoter,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Add,
                    3,
                    1,
                    1,
                    false,
                    true,
                    true,
                    false,
                    true,
                ),
                VoterReconfigurationDecision::VoterNotCaughtUp,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Remove,
                    3,
                    1,
                    1,
                    false,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::VoterNotFound,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Remove,
                    3,
                    1,
                    1,
                    true,
                    false,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::DirectoryMismatch,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Remove,
                    1,
                    1,
                    1,
                    true,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::LastVoter,
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::FinalizeKraftVersion,
                    3,
                    1,
                    0,
                    false,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::InvalidVersionTransition,
            ),
        ];
        for (got, expected) in cases {
            assert!(got == expected);
        }
    }

    #[test]
    fn legal_transitions_have_exact_result_shapes() {
        let cases = [
            (
                decide(VoterChangeKind::Add),
                VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
                    next_voter_count: 4,
                    next_kraft_version: 1,
                    write_voters: true,
                    write_kraft_version: false,
                    preflight_only: false,
                }),
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Remove,
                    3,
                    1,
                    1,
                    true,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
                    next_voter_count: 2,
                    next_kraft_version: 1,
                    write_voters: true,
                    write_kraft_version: false,
                    preflight_only: false,
                }),
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Update,
                    3,
                    1,
                    1,
                    true,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
                    next_voter_count: 3,
                    next_kraft_version: 1,
                    write_voters: true,
                    write_kraft_version: false,
                    preflight_only: false,
                }),
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::Update,
                    3,
                    0,
                    0,
                    true,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
                    next_voter_count: 3,
                    next_kraft_version: 0,
                    write_voters: false,
                    write_kraft_version: false,
                    preflight_only: true,
                }),
            ),
            (
                voter_reconfiguration_decision(
                    true,
                    true,
                    true,
                    VoterChangeKind::FinalizeKraftVersion,
                    3,
                    0,
                    1,
                    false,
                    true,
                    true,
                    true,
                    true,
                ),
                VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
                    next_voter_count: 3,
                    next_kraft_version: 1,
                    write_voters: true,
                    write_kraft_version: true,
                    preflight_only: false,
                }),
            ),
        ];
        for (got, expected) in cases {
            assert!(got == expected);
        }
    }
}
