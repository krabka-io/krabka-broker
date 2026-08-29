//! Classification and logging of the leader's `AddRaftVoter` reply.
//!
//! The join loop never terminates on a reply — only on seeing itself in the
//! committed voter set — so this mapping from error code to `JoinOutcome` is
//! purely diagnostic. It lives apart from the loop so the code-to-outcome table
//! can be unit tested without a running broker.

use krabka_protocol::owned::add_raft_voter_response::AddRaftVoterResponse;

use crate::codes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinOutcome {
    Accepted,
    NotLeader,
    TimedOut,
    NotCaughtUp,
    Unexpected(i16),
}

/// Log the leader's `AddRaftVoter` reply at the appropriate level. None of the
/// outcomes terminate the loop — the `voters().contains` check at the top of
/// `run` is the sole exit — so this is purely diagnostic.
pub(super) fn log_join_outcome(
    self_id: krabka_raft::NodeId,
    target: &str,
    resp: &AddRaftVoterResponse,
) -> JoinOutcome {
    match resp.error_code {
        codes::NONE => {
            tracing::info!(
                node_id = self_id.0,
                leader = %target,
                "auto-join accepted by leader"
            );
            // The committed V1Voters record may not be visible in our local
            // image yet (we're still catching up); the next loop iteration's
            // `voters().contains` check confirms before exiting.
            JoinOutcome::Accepted
        }
        codes::NOT_LEADER_OR_FOLLOWER => {
            // Not the leader. The error message may name the current leader,
            // but it isn't a routable address — fall back to rotating across
            // the configured bootstrap servers.
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join target is not the leader; trying next bootstrap server"
            );
            JoinOutcome::NotLeader
        }
        codes::REQUEST_TIMED_OUT => {
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                "auto-join: reconfiguration in progress on leader; retrying"
            );
            JoinOutcome::TimedOut
        }
        codes::INVALID_REQUEST => {
            // Observer not yet caught up within the lag bound. Keep replicating
            // (openraft is doing that in the background) and retry shortly.
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join: not yet caught up; retrying"
            );
            JoinOutcome::NotCaughtUp
        }
        other => {
            tracing::warn!(
                node_id = self_id.0,
                server = %target,
                error_code = other,
                msg = ?resp.error_message,
                "auto-join: unexpected error_code; retrying"
            );
            JoinOutcome::Unexpected(other)
        }
    }
}

#[cfg(test)]
mod tests {
    use krabka_raft::NodeId;

    use super::*;

    #[test]
    fn log_join_outcome_classifies_response_codes() {
        let target = "127.0.0.1:9092";
        let response = |error_code| AddRaftVoterResponse {
            error_code,
            ..Default::default()
        };

        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(codes::NONE)),
            JoinOutcome::Accepted
        );
        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(codes::NOT_LEADER_OR_FOLLOWER)),
            JoinOutcome::NotLeader
        );
        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(codes::REQUEST_TIMED_OUT)),
            JoinOutcome::TimedOut
        );
        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(codes::INVALID_REQUEST)),
            JoinOutcome::NotCaughtUp
        );
        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(1234)),
            JoinOutcome::Unexpected(1234)
        );
    }
}
