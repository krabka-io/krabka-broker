//! Answering an inbound `Vote`: the recipient side of both the KIP-996
//! pre-vote and the binding vote.
//!
//! The grant rules differ between the two rounds, and only the binding vote
//! is persisted, so the whole decision is kept in one place next to the
//! log-recency comparison it depends on.

use krabka_verified::{VoteAdmissionDecision, vote_admission_decision};

use super::{QuorumStateMachine, VoteRequest};
use crate::{
    action::{Action, TimerKind},
    event::LogEnd,
    role::Role,
    types::{LogView, ReplicaKey, SimInstant},
};

#[cfg(test)]
mod tests;

impl QuorumStateMachine {
    /// `true` if `candidate_log` is at least as up-to-date as ours.
    ///
    /// KIP-595: the higher last epoch wins. On a tie, the higher or equal
    /// offset wins.
    fn log_is_up_to_date(log: &dyn LogView, cand: LogEnd) -> bool {
        krabka_verified::log_is_up_to_date(
            log.last_epoch(),
            log.end_offset(),
            cand.last_epoch,
            cand.last_offset,
        )
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, from = request.from.0, ?request.cluster_id, voter_id = request.voter_id.0, voter_directory_id = %request.voter_directory_id, candidate = request.candidate.0, candidate_directory_id = %request.candidate_directory_id, candidate_epoch = request.candidate_epoch, pre_vote = request.pre_vote)
    )]
    pub(super) fn handle_vote_request(
        &mut self,
        request: VoteRequest,
        log: &dyn LogView,
        now: SimInstant,
    ) -> Vec<Action> {
        let VoteRequest {
            from,
            cluster_id,
            voter_id,
            voter_directory_id,
            candidate_epoch,
            candidate,
            candidate_directory_id,
            candidate_log_end: cand_log,
            pre_vote,
        } = request;
        let mut actions = Vec::new();
        let candidate_key = ReplicaKey {
            id: candidate,
            directory_id: candidate_directory_id,
        };
        // The wire adapter rejects signed sentinels before conversion. The pure
        // classifier then requires the exact target and both membership checks;
        // the candidate can be in either side of one adjacent KIP-853 transition.
        match vote_admission_decision(
            voter_id.0,
            self.me.0,
            self.local_voter_directory_matches(voter_directory_id),
            cluster_id.is_none_or(|id| id == self.state.cluster_id),
            self.is_voter(),
            self.current_or_adjacent_voter_key(candidate_key),
        ) {
            VoteAdmissionDecision::IgnoreWrongTarget => {
                tracing::warn!(
                    addressed_to = voter_id.0,
                    me = self.me.0,
                    "ignoring Vote addressed to a different voter"
                );
                return Vec::new();
            }
            VoteAdmissionDecision::Deny => {
                actions.push(Action::ReplyVote {
                    to: from,
                    epoch: self.state.leader_epoch,
                    granted: false,
                });
                return actions;
            }
            VoteAdmissionDecision::Consider => {}
        }
        // Fenced: candidate is behind our epoch.
        if candidate_epoch < self.state.leader_epoch {
            actions.push(Action::ReplyVote {
                to: from,
                epoch: self.state.leader_epoch,
                granted: false,
            });
            return actions;
        }
        // A standard vote at a higher epoch first advances us to that epoch
        // (Unattached), clearing any prior vote. Pre-vote never changes epoch.
        if !pre_vote && candidate_epoch > self.state.leader_epoch {
            self.transition_to_unattached(candidate_epoch, now, &mut actions);
        }
        let up_to_date = Self::log_is_up_to_date(log, cand_log);
        let granted = if pre_vote {
            // Non-binding: grant if log is up to date and we don't already
            // follow a leader in this (or a higher) epoch.
            up_to_date && self.state.leader_id.is_none()
        } else {
            let not_voted_other = match self.state.voted_key {
                None => true,
                Some(key) => self.same_voter(key, candidate_key),
            };
            up_to_date && not_voted_other && self.state.leader_id.is_none()
        };
        if granted && !pre_vote {
            // Binding: persist the vote, become Voted.
            self.state.voted_key = Some(candidate_key);
            let deadline = self.election_deadline(now);
            self.role = Role::Voted {
                election_deadline: deadline,
            };
            actions.push(Action::PersistQuorumState);
            actions.push(Action::TransitionedTo(self.role.name()));
            // Arm the election timer: if the candidate we voted for dies, this
            // node must time out and start its own election (else deadlock).
            actions.push(Action::ResetTimer {
                kind: TimerKind::Election,
                deadline,
            });
        }
        actions.push(Action::ReplyVote {
            to: from,
            epoch: self.state.leader_epoch,
            granted,
        });
        actions
    }
}
