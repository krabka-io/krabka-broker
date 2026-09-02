//! KIP-595 Vote wire and membership admission decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Whether the signed Vote fields can be converted without aliasing a Kafka
/// sentinel to a real node or epoch.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum VoteWireDecision {
    Reject,
    Accept,
}

/// Whether unsigned consensus fields fit Kafka's signed Vote wire fields.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum VoteEncodeDecision {
    Reject,
    Accept,
}

/// Reject values that would otherwise be clamped and alias another identity
/// or epoch when encoded as Kafka `int32` fields.
#[ensures((result == VoteEncodeDecision::Reject) == (
    voter_id@ > i32::MAX@
        || candidate_id@ > i32::MAX@
        || candidate_epoch@ > i32::MAX@
        || last_epoch@ > i32::MAX@
))]
#[ensures((result == VoteEncodeDecision::Accept) == (
    voter_id@ <= i32::MAX@
        && candidate_id@ <= i32::MAX@
        && candidate_epoch@ <= i32::MAX@
        && last_epoch@ <= i32::MAX@
))]
#[must_use]
pub fn vote_encode_decision(
    voter_id: u64,
    candidate_id: u64,
    candidate_epoch: u32,
    last_epoch: u32,
) -> VoteEncodeDecision {
    if voter_id > 2_147_483_647
        || candidate_id > 2_147_483_647
        || candidate_epoch > 2_147_483_647
        || last_epoch > 2_147_483_647
    {
        VoteEncodeDecision::Reject
    } else {
        VoteEncodeDecision::Accept
    }
}

/// Validate the signed identity and epoch fields before conversion to the
/// unsigned consensus types. Zero is a legitimate broker id and epoch.
#[ensures((result == VoteWireDecision::Reject) == (
    voter_id@ < 0 || candidate_id@ < 0 || candidate_epoch@ < 0 || last_epoch@ < 0
))]
#[ensures((result == VoteWireDecision::Accept) == (
    voter_id@ >= 0 && candidate_id@ >= 0 && candidate_epoch@ >= 0 && last_epoch@ >= 0
))]
#[must_use]
pub fn vote_wire_decision(
    voter_id: i32,
    candidate_id: i32,
    candidate_epoch: i32,
    last_epoch: i32,
) -> VoteWireDecision {
    if voter_id < 0 || candidate_id < 0 || candidate_epoch < 0 || last_epoch < 0 {
        VoteWireDecision::Reject
    } else {
        VoteWireDecision::Accept
    }
}

/// Admission shared by binding votes and pre-votes before epoch/log checks.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum VoteAdmissionDecision {
    /// The Vote targets another voter and must be ignored without a reply.
    IgnoreWrongTarget,
    /// The target is local, but the local node or candidate lacks membership.
    Deny,
    /// The exact target and both membership requirements hold.
    Consider,
}

/// Classify exact recipient and membership admission.
#[ensures((result == VoteAdmissionDecision::IgnoreWrongTarget)
    == (voter_id@ != local_id@ || !target_directory_matches))]
#[ensures((result == VoteAdmissionDecision::Deny) == (
    voter_id@ == local_id@ && target_directory_matches
        && (!cluster_matches || !local_is_voter || !candidate_is_voter)
))]
#[ensures((result == VoteAdmissionDecision::Consider) == (
    voter_id@ == local_id@ && target_directory_matches
        && cluster_matches && local_is_voter && candidate_is_voter
))]
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies four independent host boundary facts"
)]
#[must_use]
pub fn vote_admission_decision(
    voter_id: u64,
    local_id: u64,
    target_directory_matches: bool,
    cluster_matches: bool,
    local_is_voter: bool,
    candidate_is_voter: bool,
) -> VoteAdmissionDecision {
    if voter_id != local_id || !target_directory_matches {
        VoteAdmissionDecision::IgnoreWrongTarget
    } else if !cluster_matches || !local_is_voter || !candidate_is_voter {
        VoteAdmissionDecision::Deny
    } else {
        VoteAdmissionDecision::Consider
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn wire_fields_reject_negatives_without_rejecting_zero() {
        use VoteWireDecision::{Accept, Reject};

        for (voter, candidate, epoch, last_epoch, expected) in [
            (0, 0, 0, 0, Accept),
            (-1, 0, 0, 0, Reject),
            (0, -1, 0, 0, Reject),
            (0, 0, -1, 0, Reject),
            (0, 0, 0, -1, Reject),
        ] {
            check!(vote_wire_decision(voter, candidate, epoch, last_epoch) == expected);
        }
    }

    #[test]
    fn encode_fields_reject_values_above_signed_wire_maximum() {
        use VoteEncodeDecision::{Accept, Reject};

        let max_id = 2_147_483_647_u64;
        let max_epoch = 2_147_483_647_u32;
        for (voter, candidate, epoch, last_epoch, expected) in [
            (max_id, max_id, max_epoch, max_epoch, Accept),
            (max_id + 1, 0, 0, 0, Reject),
            (0, max_id + 1, 0, 0, Reject),
            (0, 0, max_epoch + 1, 0, Reject),
            (0, 0, 0, max_epoch + 1, Reject),
        ] {
            check!(vote_encode_decision(voter, candidate, epoch, last_epoch) == expected);
        }
    }

    #[test]
    fn admission_requires_the_exact_target_and_both_memberships() {
        use VoteAdmissionDecision::{Consider, Deny, IgnoreWrongTarget};

        for (target, local, target_dir, cluster, local_voter, candidate_voter, expected) in [
            (1, 2, true, true, true, true, IgnoreWrongTarget),
            (0, 1, true, true, true, true, IgnoreWrongTarget),
            (1, 1, false, true, true, true, IgnoreWrongTarget),
            (0, 0, true, true, true, true, Consider),
            (1, 1, true, false, true, true, Deny),
            (1, 1, true, true, false, true, Deny),
            (1, 1, true, true, true, false, Deny),
            (1, 1, true, true, true, true, Consider),
        ] {
            check!(
                vote_admission_decision(
                    target,
                    local,
                    target_dir,
                    cluster,
                    local_voter,
                    candidate_voter,
                ) == expected
            );
        }
    }
}
