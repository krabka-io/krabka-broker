use assert2::assert;

use super::*;

#[test]
fn leadership_and_single_flight_gate_every_operation_first() {
    use TargetMembership::{Absent, PresentSameDirectory};
    let follower = ReconfigurationLeadership {
        is_leader: false,
        ..LEADING
    };
    let pending = ReconfigurationLeadership {
        no_pending_change: false,
        ..LEADING
    };
    let fresh_epoch = ReconfigurationLeadership {
        epoch_committed: false,
        ..LEADING
    };
    let uncommitted = CurrentVoterSet {
        latest_controls_committed: false,
        ..voters(3, 1)
    };
    for (kind, membership) in [
        (VoterChangeKind::Add, Absent),
        (VoterChangeKind::Remove, PresentSameDirectory),
        (VoterChangeKind::Update, PresentSameDirectory),
        (VoterChangeKind::FinalizeKraftVersion, Absent),
    ] {
        let cases = [
            // `validateLeaderOnlyRequest` wins over every other failure.
            (follower, uncommitted, D::NotLeader),
            // `isOperationPending` precedes the leader-HWM check.
            (
                ReconfigurationLeadership {
                    epoch_committed: false,
                    ..pending
                },
                voters(3, 1),
                D::InProgress,
            ),
            (fresh_epoch, uncommitted, D::EpochUncommitted),
            // A VotersRecord from the previous leader has not committed.
            (LEADING, uncommitted, D::InProgress),
            (LEADING, voters(0, 1), D::EmptyCurrentVoterSet),
            (LEADING, voters(3, 2), D::InvalidVersionTransition),
        ];
        for (leadership, current, expected) in cases {
            let got = voter_reconfiguration_decision(
                leadership,
                current,
                request(kind),
                target(membership),
            );
            assert!(got == expected, "{kind:?}");
        }
    }
}

#[test]
fn add_voter_follows_add_voter_handler_order() {
    use TargetMembership::{
        Absent, PresentOtherDirectory, PresentSameDirectory, PresentUnknownDirectory,
    };
    let add = request(VoterChangeKind::Add);
    let incompatible = TargetVoter {
        version_compatible: false,
        caught_up: false,
        ..target(Absent)
    };
    let lagging = TargetVoter {
        caught_up: false,
        ..target(Absent)
    };
    let cases = [
        // kraft.version 0 answers UNSUPPORTED_VERSION before the duplicate check.
        (
            "static quorum",
            voters(3, 0),
            target(PresentSameDirectory),
            D::UnsupportedKraftVersion,
        ),
        (
            "same key",
            voters(3, 1),
            target(PresentSameDirectory),
            D::DuplicateVoter,
        ),
        // DUPLICATE_VOTER compares voter ids only.
        (
            "same id",
            voters(3, 1),
            target(PresentOtherDirectory),
            D::DuplicateVoter,
        ),
        (
            "legacy id",
            voters(3, 1),
            target(PresentUnknownDirectory),
            D::DuplicateVoter,
        ),
        (
            "old range",
            voters(3, 1),
            incompatible,
            D::IncompatibleVoter,
        ),
        ("lagging", voters(3, 1), lagging, D::VoterNotCaughtUp),
        (
            "full",
            voters(usize::MAX, 1),
            target(Absent),
            D::InvalidVersionTransition,
        ),
        (
            "admitted",
            voters(3, 1),
            target(Absent),
            plan(4, 1, true, false),
        ),
    ];
    for (case, current, voter, expected) in cases {
        assert!(
            voter_reconfiguration_decision(LEADING, current, add, voter) == expected,
            "{case}"
        );
    }
}

#[test]
fn remove_voter_answers_voter_not_found_unless_voter_set_removes() {
    use TargetMembership::{
        Absent, PresentOtherDirectory, PresentSameDirectory, PresentUnknownDirectory,
    };
    let remove = request(VoterChangeKind::Remove);
    let cases = [
        (
            "static quorum",
            voters(3, 0),
            PresentSameDirectory,
            D::UnsupportedKraftVersion,
        ),
        // `VoterSet.removeVoter` is empty for each of these, and
        // `RemoveVoterHandler` answers VOTER_NOT_FOUND.
        ("unknown id", voters(3, 1), Absent, D::VoterNotFound),
        (
            "other directory",
            voters(3, 1),
            PresentOtherDirectory,
            D::VoterNotFound,
        ),
        (
            "legacy directory",
            voters(3, 1),
            PresentUnknownDirectory,
            D::VoterNotFound,
        ),
        (
            "last voter",
            voters(1, 1),
            PresentSameDirectory,
            D::VoterNotFound,
        ),
        (
            "last voter under another key",
            voters(1, 1),
            PresentOtherDirectory,
            D::VoterNotFound,
        ),
        (
            "admitted",
            voters(3, 1),
            PresentSameDirectory,
            plan(2, 1, true, false),
        ),
    ];
    for (case, current, membership, expected) in cases {
        assert!(
            voter_reconfiguration_decision(LEADING, current, remove, target(membership))
                == expected,
            "{case}"
        );
    }
}
