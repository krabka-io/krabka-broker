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

mod leadership_and_single_flight_gate_every_operation_first;

mod update_voter_matches_the_stored_key_as_voter_node_is_voter_does;
