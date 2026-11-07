//! Classification and logging of the leader's `AddRaftVoter` reply.
//!
//! The join loop never terminates on a reply — only on seeing itself in the
//! committed voter set — so this mapping from error code to `JoinOutcome` is
//! purely diagnostic. It lives apart from the loop so the code-to-outcome table
//! can be unit tested without a running broker.

use {crate::codes, krabka_protocol::owned::add_raft_voter_response::AddRaftVoterResponse};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinOutcome {
    Accepted,
    NotLeader,
    TimedOut,
    Refused,
    Unexpected(i16),
}

/// What the leader's `AddRaftVoter` reply says about the join.
pub(super) fn classify_join_response(resp: &AddRaftVoterResponse) -> JoinOutcome {
    match resp.error_code {
        codes::NONE => JoinOutcome::Accepted,
        codes::NOT_LEADER_OR_FOLLOWER => JoinOutcome::NotLeader,
        codes::REQUEST_TIMED_OUT => JoinOutcome::TimedOut,
        codes::INVALID_REQUEST => JoinOutcome::Refused,
        other => JoinOutcome::Unexpected(other),
    }
}

/// Log the leader's `AddRaftVoter` reply at the appropriate level. None of the
/// outcomes terminate the loop — the `voters().contains` check at the top of
/// `run` is the sole exit — so this is purely diagnostic.
pub(super) fn log_join_outcome(
    self_id: krabka_raft::NodeId,
    target: &str,
    resp: &AddRaftVoterResponse,
) {
    match classify_join_response(resp) {
        JoinOutcome::Accepted => {
            // The committed V1Voters record may not be visible in our local
            // image yet (we're still catching up); the next loop iteration's
            // `voters().contains` check confirms before exiting.
            tracing::info!(
                node_id = self_id.0,
                leader = %target,
                "auto-join accepted by leader"
            );
        }
        JoinOutcome::NotLeader => {
            // Not the leader. The error message may name the current leader,
            // but it isn't a routable address. The next attempt asks the
            // leader the local quorum state names then, else the next
            // bootstrap server in the rotation.
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join target is not the leader; retrying the known leader, else the next bootstrap server"
            );
        }
        JoinOutcome::TimedOut => {
            // Kafka's `AddVoterHandler` answers this for a pending voter change
            // and for a candidate that is not caught up yet. Keep replicating
            // and retry shortly.
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join: reconfiguration pending or not yet caught up; retrying"
            );
        }
        JoinOutcome::Refused => {
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join: request refused; retrying"
            );
        }
        JoinOutcome::Unexpected(other) => {
            tracing::warn!(
                node_id = self_id.0,
                server = %target,
                error_code = other,
                msg = ?resp.error_message,
                "auto-join: unexpected error_code; retrying"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_join_response_maps_every_response_code() {
        let cases = [
            (codes::NONE, JoinOutcome::Accepted),
            (codes::NOT_LEADER_OR_FOLLOWER, JoinOutcome::NotLeader),
            (codes::REQUEST_TIMED_OUT, JoinOutcome::TimedOut),
            (codes::INVALID_REQUEST, JoinOutcome::Refused),
            (1234, JoinOutcome::Unexpected(1234)),
        ];
        for (error_code, expected) in cases {
            let response = AddRaftVoterResponse {
                error_code,
                ..Default::default()
            };
            assert2::check!(classify_join_response(&response) == expected);
        }
    }
}
