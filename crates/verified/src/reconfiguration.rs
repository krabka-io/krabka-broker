//! `KRaft` voter-reconfiguration admission and result shape.
//!
//! The decision follows the order in which Kafka's `KafkaRaftClient`,
//! `AddVoterHandler`, `RemoveVoterHandler`, `UpdateVoterHandler`, and
//! `LeaderState.maybeAppendUpgradedKRaftVersion` reject a request, and it
//! admits a change only while KIP-853's one-change-at-a-time rule holds: the
//! node leads, its epoch is committed, no earlier change is pending, and no
//! control record is uncommitted.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::*;

/// The KIP-853 control operation being validated.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum VoterChangeKind {
    Add,
    Remove,
    Update,
    FinalizeKraftVersion,
}

/// The receiving node's leadership state for one reconfiguration.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ReconfigurationLeadership {
    /// The node is the current `KRaft` leader.
    pub is_leader: bool,
    /// No earlier voter change is waiting for its record to commit.
    pub no_pending_change: bool,
    /// The leader has committed its own epoch, so Kafka's leader HWM exists.
    pub epoch_committed: bool,
}

/// The committed voter set the change applies to.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct CurrentVoterSet {
    pub voter_count: usize,
    /// The committed `kraft.version`.
    pub kraft_version: u16,
    /// The latest voter set and `kraft.version` in the log equal the
    /// committed ones: no control record is uncommitted.
    pub latest_controls_committed: bool,
    /// Every current voter's supported range covers the requested
    /// `kraft.version`. Only a finalization reads it.
    pub all_voters_support_requested: bool,
}

/// One requested control operation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct VoterChangeRequest {
    pub kind: VoterChangeKind,
    /// The `kraft.version` a finalization asks for.
    pub requested_kraft_version: u16,
}

/// How the request's voter key relates to the current voter set.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum TargetMembership {
    /// No current voter has the request's voter id.
    Absent,
    /// The voter id is current, and its stored directory id is unknown.
    PresentUnknownDirectory,
    /// The voter id is current, with the request's directory id.
    PresentSameDirectory,
    /// The voter id is current, with a different known directory id.
    PresentOtherDirectory,
}

/// Facts about the voter an add, remove, or update names. A finalization
/// names no voter and does not read them.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct TargetVoter {
    pub membership: TargetMembership,
    /// The voter's supported range covers the committed `kraft.version`.
    pub version_compatible: bool,
    /// The voter has fetched up to the leader's log end.
    pub caught_up: bool,
}

/// The exact control-record shape of an admitted change.
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
    InvalidVersionTransition,
    Admit(VoterReconfigurationPlan),
}

/// KIP-853's one-change-at-a-time rule: only a leader whose epoch is
/// committed, with no pending change and no uncommitted control record, may
/// start a voter change.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn may_reconfigure(leadership: ReconfigurationLeadership, voters: CurrentVoterSet) -> bool {
    pearlite! {
        leadership.is_leader
            && leadership.no_pending_change
            && leadership.epoch_committed
            && voters.latest_controls_committed
    }
}

/// Whether a decision admits the change.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn is_admit(decision: VoterReconfigurationDecision) -> bool {
    pearlite! {
        match decision {
            VoterReconfigurationDecision::Admit(_) => true,
            _ => false,
        }
    }
}

/// Whether Kafka's `VoterSet.removeVoter` returns a new voter set: the
/// stored `ReplicaKey` equals the requested one, and at least one voter
/// remains. `KafkaRaftClient` rejects a request without a directory id, so an
/// unknown stored directory never matches. `RemoveVoterHandler` answers every
/// other case, the last voter included, with `VOTER_NOT_FOUND`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn voter_set_removes(membership: TargetMembership, voters: CurrentVoterSet) -> bool {
    pearlite! {
        membership == TargetMembership::PresentSameDirectory && voters.voter_count@ > 1
    }
}

/// Whether the request's voter key names a current voter for an update:
/// Kafka's `VoterNode.isVoter`, which accepts an unknown stored directory and
/// an equal one but rejects a different known one.
///
/// Kafka applies this rule at `kraft.version` 1. At version 0 its
/// `updateVoterIgnoringDirectoryId` matches by voter id alone, but only to
/// stage upgrade data; Krabka's version-0 preflight applies the updated set to
/// the live quorum, so a different known directory, which would take that
/// voter's identity away, is refused at every version.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn update_key_matches(membership: TargetMembership) -> bool {
    pearlite! {
        membership == TargetMembership::PresentUnknownDirectory
            || membership == TargetMembership::PresentSameDirectory
    }
}

/// The first rejection for a request, in Kafka's precedence order, or `None`
/// when the request is admitted.
///
/// Leadership comes first (`validateLeaderOnlyRequest`), then the pending
/// change and the leader HWM, then an uncommitted control record. Kafka reads
/// the latest `kraft.version`, so an uncommitted control record there always
/// means version 1 and a `REQUEST_TIMED_OUT`; that is why the check precedes
/// the version checks here, which read the committed version. (Kafka's
/// `UpdateVoterHandler` checks the voter's range before it reads the voter
/// set, so for an update with both faults Kafka reports the range.) A
/// finalization is also held to this single-flight rule, which Kafka's
/// `upgradeKRaftVersion` does not check. An empty voter set and a version
/// above 1 cannot occur in Kafka and fail closed. The per-operation checks
/// then follow each handler's order.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn voter_reconfiguration_rejection(
    leadership: ReconfigurationLeadership,
    voters: CurrentVoterSet,
    request: VoterChangeRequest,
    target: TargetVoter,
) -> Option<VoterReconfigurationDecision> {
    pearlite! {
        if !leadership.is_leader {
            Some(VoterReconfigurationDecision::NotLeader)
        } else if !leadership.no_pending_change {
            Some(VoterReconfigurationDecision::InProgress)
        } else if !leadership.epoch_committed {
            Some(VoterReconfigurationDecision::EpochUncommitted)
        } else if !voters.latest_controls_committed {
            Some(VoterReconfigurationDecision::InProgress)
        } else if voters.voter_count@ == 0 {
            Some(VoterReconfigurationDecision::EmptyCurrentVoterSet)
        } else if voters.kraft_version@ > 1 {
            Some(VoterReconfigurationDecision::InvalidVersionTransition)
        } else {
            match request.kind {
                VoterChangeKind::Add => {
                    if voters.kraft_version@ != 1 {
                        Some(VoterReconfigurationDecision::UnsupportedKraftVersion)
                    } else if target.membership != TargetMembership::Absent {
                        Some(VoterReconfigurationDecision::DuplicateVoter)
                    } else if !target.version_compatible {
                        Some(VoterReconfigurationDecision::IncompatibleVoter)
                    } else if !target.caught_up {
                        Some(VoterReconfigurationDecision::VoterNotCaughtUp)
                    } else if voters.voter_count@ == usize::MAX@ {
                        Some(VoterReconfigurationDecision::InvalidVersionTransition)
                    } else {
                        None
                    }
                }
                VoterChangeKind::Remove => {
                    if voters.kraft_version@ != 1 {
                        Some(VoterReconfigurationDecision::UnsupportedKraftVersion)
                    } else if !voter_set_removes(target.membership, voters) {
                        Some(VoterReconfigurationDecision::VoterNotFound)
                    } else {
                        None
                    }
                }
                VoterChangeKind::Update => {
                    if !target.version_compatible {
                        Some(VoterReconfigurationDecision::IncompatibleVoter)
                    } else if target.membership == TargetMembership::Absent {
                        Some(VoterReconfigurationDecision::VoterNotFound)
                    } else if !update_key_matches(target.membership) {
                        Some(VoterReconfigurationDecision::DirectoryMismatch)
                    } else {
                        None
                    }
                }
                VoterChangeKind::FinalizeKraftVersion => {
                    if voters.kraft_version@ != 0 || request.requested_kraft_version@ != 1 {
                        Some(VoterReconfigurationDecision::InvalidVersionTransition)
                    } else if !voters.all_voters_support_requested {
                        Some(VoterReconfigurationDecision::IncompatibleVoter)
                    } else {
                        None
                    }
                }
            }
        }
    }
}

/// The exact result shape of an admitted change: add and remove write one
/// `VotersRecord` for one more or one fewer voter; an update rewrites the
/// set at version 1 and is in-memory preflight data at version 0; a
/// finalization writes the version-1 record with the unchanged voter set.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn admitted_plan(
    voters: CurrentVoterSet,
    kind: VoterChangeKind,
    plan: VoterReconfigurationPlan,
) -> bool {
    pearlite! {
        match kind {
            VoterChangeKind::Add => plan.next_voter_count@ == voters.voter_count@ + 1
                && plan.next_kraft_version == voters.kraft_version
                && plan.write_voters && !plan.write_kraft_version && !plan.preflight_only,
            VoterChangeKind::Remove => plan.next_voter_count@ + 1 == voters.voter_count@
                && plan.next_kraft_version == voters.kraft_version
                && plan.write_voters && !plan.write_kraft_version && !plan.preflight_only,
            VoterChangeKind::Update => plan.next_voter_count == voters.voter_count
                && plan.next_kraft_version == voters.kraft_version
                && plan.write_voters == (voters.kraft_version@ != 0)
                && !plan.write_kraft_version
                && plan.preflight_only == (voters.kraft_version@ == 0),
            VoterChangeKind::FinalizeKraftVersion => plan.next_voter_count == voters.voter_count
                && plan.next_kraft_version@ == 1
                && plan.write_voters && plan.write_kraft_version && !plan.preflight_only,
        }
    }
}

/// Admit one fresh KIP-853 operation and construct its exact result shape.
///
/// The result is exactly the first rejection of
/// `voter_reconfiguration_rejection`, which follows the precedence of Kafka's
/// handlers, and otherwise an admission with the plan `admitted_plan`
/// describes. Every admitted change satisfies `may_reconfigure`.
#[cfg_attr(creusot, ensures(match voter_reconfiguration_rejection(leadership, voters, request, target) {
    Some(rejection) => result == rejection,
    None => match result {
        VoterReconfigurationDecision::Admit(plan) => admitted_plan(voters, request.kind, plan),
        _ => false,
    },
}))]
#[cfg_attr(creusot, ensures(is_admit(result) ==> may_reconfigure(leadership, voters)))]
#[cfg_attr(creusot, ensures(forall<plan: VoterReconfigurationPlan>
    result == VoterReconfigurationDecision::Admit(plan)
        ==> plan.next_voter_count@ > 0 && plan.next_kraft_version@ <= 1))]
#[must_use]
pub fn voter_reconfiguration_decision(
    leadership: ReconfigurationLeadership,
    voters: CurrentVoterSet,
    request: VoterChangeRequest,
    target: TargetVoter,
) -> VoterReconfigurationDecision {
    if !leadership.is_leader {
        return VoterReconfigurationDecision::NotLeader;
    }
    if !leadership.no_pending_change {
        return VoterReconfigurationDecision::InProgress;
    }
    if !leadership.epoch_committed {
        return VoterReconfigurationDecision::EpochUncommitted;
    }
    if !voters.latest_controls_committed {
        return VoterReconfigurationDecision::InProgress;
    }
    if voters.voter_count == 0 {
        return VoterReconfigurationDecision::EmptyCurrentVoterSet;
    }
    if voters.kraft_version > 1 {
        return VoterReconfigurationDecision::InvalidVersionTransition;
    }
    match request.kind {
        VoterChangeKind::Add => add_decision(voters, target),
        VoterChangeKind::Remove => remove_decision(voters, target),
        VoterChangeKind::Update => update_decision(voters, target),
        VoterChangeKind::FinalizeKraftVersion => finalize_decision(voters, request),
    }
}

/// `AddVoterHandler`: version, duplicate id, `ApiVersions` range, catch-up.
#[cfg_attr(creusot, requires(voters.voter_count@ > 0 && voters.kraft_version@ <= 1))]
#[cfg_attr(creusot, ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        voters.kraft_version@ == 1
            && target.membership == TargetMembership::Absent
            && target.version_compatible
            && target.caught_up
            && voters.voter_count@ < usize::MAX@
            && admitted_plan(voters, VoterChangeKind::Add, plan),
    VoterReconfigurationDecision::UnsupportedKraftVersion => voters.kraft_version@ != 1,
    VoterReconfigurationDecision::DuplicateVoter => voters.kraft_version@ == 1
        && target.membership != TargetMembership::Absent,
    VoterReconfigurationDecision::IncompatibleVoter => voters.kraft_version@ == 1
        && target.membership == TargetMembership::Absent
        && !target.version_compatible,
    VoterReconfigurationDecision::VoterNotCaughtUp => voters.kraft_version@ == 1
        && target.membership == TargetMembership::Absent
        && target.version_compatible
        && !target.caught_up,
    VoterReconfigurationDecision::InvalidVersionTransition => voters.kraft_version@ == 1
        && target.membership == TargetMembership::Absent
        && target.version_compatible
        && target.caught_up
        && voters.voter_count@ == usize::MAX@,
    _ => false,
}))]
fn add_decision(voters: CurrentVoterSet, target: TargetVoter) -> VoterReconfigurationDecision {
    if voters.kraft_version != 1 {
        return VoterReconfigurationDecision::UnsupportedKraftVersion;
    }
    if !matches!(target.membership, TargetMembership::Absent) {
        return VoterReconfigurationDecision::DuplicateVoter;
    }
    if !target.version_compatible {
        return VoterReconfigurationDecision::IncompatibleVoter;
    }
    if !target.caught_up {
        return VoterReconfigurationDecision::VoterNotCaughtUp;
    }
    // A voter set of `usize::MAX` members cannot grow; fail closed.
    let Some(next_voter_count) = voters.voter_count.checked_add(1) else {
        return VoterReconfigurationDecision::InvalidVersionTransition;
    };
    VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
        next_voter_count,
        next_kraft_version: voters.kraft_version,
        write_voters: true,
        write_kraft_version: false,
        preflight_only: false,
    })
}

/// `RemoveVoterHandler`: version, then whether `VoterSet.removeVoter`
/// returns a new set; an absent id, a different stored key, and the last
/// voter are all its `VOTER_NOT_FOUND`.
#[cfg_attr(creusot, requires(voters.voter_count@ > 0 && voters.kraft_version@ <= 1))]
#[cfg_attr(creusot, ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        voters.kraft_version@ == 1
            && voter_set_removes(target.membership, voters)
            && admitted_plan(voters, VoterChangeKind::Remove, plan),
    VoterReconfigurationDecision::UnsupportedKraftVersion => voters.kraft_version@ != 1,
    VoterReconfigurationDecision::VoterNotFound => voters.kraft_version@ == 1
        && !voter_set_removes(target.membership, voters),
    _ => false,
}))]
fn remove_decision(voters: CurrentVoterSet, target: TargetVoter) -> VoterReconfigurationDecision {
    if voters.kraft_version != 1 {
        return VoterReconfigurationDecision::UnsupportedKraftVersion;
    }
    if !matches!(target.membership, TargetMembership::PresentSameDirectory)
        || voters.voter_count == 1
    {
        return VoterReconfigurationDecision::VoterNotFound;
    }
    VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
        next_voter_count: voters.voter_count - 1,
        next_kraft_version: voters.kraft_version,
        write_voters: true,
        write_kraft_version: false,
        preflight_only: false,
    })
}

/// `UpdateVoterHandler`: `ApiVersions` range, then the voter lookup.
#[cfg_attr(creusot, requires(voters.voter_count@ > 0 && voters.kraft_version@ <= 1))]
#[cfg_attr(creusot, ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        target.version_compatible
            && update_key_matches(target.membership)
            && admitted_plan(voters, VoterChangeKind::Update, plan),
    VoterReconfigurationDecision::IncompatibleVoter => !target.version_compatible,
    VoterReconfigurationDecision::VoterNotFound => target.version_compatible
        && target.membership == TargetMembership::Absent,
    VoterReconfigurationDecision::DirectoryMismatch => target.version_compatible
        && target.membership != TargetMembership::Absent
        && !update_key_matches(target.membership),
    _ => false,
}))]
fn update_decision(voters: CurrentVoterSet, target: TargetVoter) -> VoterReconfigurationDecision {
    if !target.version_compatible {
        return VoterReconfigurationDecision::IncompatibleVoter;
    }
    match target.membership {
        TargetMembership::Absent => return VoterReconfigurationDecision::VoterNotFound,
        TargetMembership::PresentOtherDirectory => {
            return VoterReconfigurationDecision::DirectoryMismatch;
        }
        TargetMembership::PresentUnknownDirectory | TargetMembership::PresentSameDirectory => {}
    }
    let preflight_only = voters.kraft_version == 0;
    VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
        next_voter_count: voters.voter_count,
        next_kraft_version: voters.kraft_version,
        write_voters: !preflight_only,
        write_kraft_version: false,
        preflight_only,
    })
}

/// `maybeAppendUpgradedKRaftVersion`: only the 0 to 1 upgrade, and only when
/// every voter supports version 1.
#[cfg_attr(creusot, requires(voters.voter_count@ > 0))]
#[cfg_attr(creusot, ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        voters.kraft_version@ == 0
            && request.requested_kraft_version@ == 1
            && voters.all_voters_support_requested
            && admitted_plan(voters, VoterChangeKind::FinalizeKraftVersion, plan),
    VoterReconfigurationDecision::InvalidVersionTransition => voters.kraft_version@ != 0
        || request.requested_kraft_version@ != 1,
    VoterReconfigurationDecision::IncompatibleVoter => voters.kraft_version@ == 0
        && request.requested_kraft_version@ == 1
        && !voters.all_voters_support_requested,
    _ => false,
}))]
fn finalize_decision(
    voters: CurrentVoterSet,
    request: VoterChangeRequest,
) -> VoterReconfigurationDecision {
    if voters.kraft_version != 0 || request.requested_kraft_version != 1 {
        return VoterReconfigurationDecision::InvalidVersionTransition;
    }
    if !voters.all_voters_support_requested {
        return VoterReconfigurationDecision::IncompatibleVoter;
    }
    VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
        next_voter_count: voters.voter_count,
        next_kraft_version: 1,
        write_voters: true,
        write_kraft_version: true,
        preflight_only: false,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        CurrentVoterSet, ReconfigurationLeadership, TargetMembership, TargetVoter, VoterChangeKind,
        VoterChangeRequest, VoterReconfigurationDecision as D, VoterReconfigurationPlan,
        voter_reconfiguration_decision,
    };

    const LEADING: ReconfigurationLeadership = ReconfigurationLeadership {
        is_leader: true,
        no_pending_change: true,
        epoch_committed: true,
    };

    const fn voters(voter_count: usize, kraft_version: u16) -> CurrentVoterSet {
        CurrentVoterSet {
            voter_count,
            kraft_version,
            latest_controls_committed: true,
            all_voters_support_requested: true,
        }
    }

    const fn request(kind: VoterChangeKind) -> VoterChangeRequest {
        VoterChangeRequest {
            kind,
            requested_kraft_version: 1,
        }
    }

    const fn target(membership: TargetMembership) -> TargetVoter {
        TargetVoter {
            membership,
            version_compatible: true,
            caught_up: true,
        }
    }

    const fn plan(
        next_voter_count: usize,
        next_kraft_version: u16,
        write_voters: bool,
        write_kraft_version: bool,
    ) -> D {
        D::Admit(VoterReconfigurationPlan {
            next_voter_count,
            next_kraft_version,
            write_voters,
            write_kraft_version,
            preflight_only: !write_voters,
        })
    }

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

    #[test]
    fn update_voter_matches_the_stored_key_as_voter_node_is_voter_does() {
        use TargetMembership::{
            Absent, PresentOtherDirectory, PresentSameDirectory, PresentUnknownDirectory,
        };
        let update = request(VoterChangeKind::Update);
        let incompatible = TargetVoter {
            version_compatible: false,
            ..target(Absent)
        };
        let cases = [
            // The ApiVersions range check precedes the voter lookup.
            (
                "old range",
                voters(3, 1),
                incompatible,
                D::IncompatibleVoter,
            ),
            ("unknown id", voters(3, 1), target(Absent), D::VoterNotFound),
            (
                "other directory",
                voters(3, 1),
                target(PresentOtherDirectory),
                D::DirectoryMismatch,
            ),
            (
                "legacy directory",
                voters(3, 1),
                target(PresentUnknownDirectory),
                plan(3, 1, true, false),
            ),
            (
                "same directory",
                voters(3, 1),
                target(PresentSameDirectory),
                plan(3, 1, true, false),
            ),
            (
                "preflight unknown id",
                voters(3, 0),
                target(Absent),
                D::VoterNotFound,
            ),
            // The preflight changes the live quorum, so it does not take
            // Kafka's id-only version-0 match.
            (
                "preflight other",
                voters(3, 0),
                target(PresentOtherDirectory),
                D::DirectoryMismatch,
            ),
            (
                "preflight same",
                voters(3, 0),
                target(PresentSameDirectory),
                plan(3, 0, false, false),
            ),
            (
                "preflight legacy",
                voters(3, 0),
                target(PresentUnknownDirectory),
                plan(3, 0, false, false),
            ),
        ];
        for (case, current, voter, expected) in cases {
            assert!(
                voter_reconfiguration_decision(LEADING, current, update, voter) == expected,
                "{case}"
            );
        }
    }

    #[test]
    fn kraft_version_finalization_is_only_the_zero_to_one_upgrade() {
        let lacking = CurrentVoterSet {
            all_voters_support_requested: false,
            ..voters(3, 0)
        };
        let finalize = |requested_kraft_version| VoterChangeRequest {
            kind: VoterChangeKind::FinalizeKraftVersion,
            requested_kraft_version,
        };
        let cases = [
            (
                "already finalized",
                voters(3, 1),
                finalize(1),
                D::InvalidVersionTransition,
            ),
            (
                "no-op level",
                voters(3, 0),
                finalize(0),
                D::InvalidVersionTransition,
            ),
            (
                "unknown level",
                voters(3, 0),
                finalize(2),
                D::InvalidVersionTransition,
            ),
            (
                "unsupported voter",
                lacking,
                finalize(1),
                D::IncompatibleVoter,
            ),
            (
                "admitted",
                voters(3, 0),
                finalize(1),
                plan(3, 1, true, true),
            ),
        ];
        for (case, current, change, expected) in cases {
            let got = voter_reconfiguration_decision(
                LEADING,
                current,
                change,
                target(TargetMembership::Absent),
            );
            assert!(got == expected, "{case}");
        }
    }
}
