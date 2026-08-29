//! Starting an election and winning one: the KIP-996 pre-vote round, the
//! vote round that follows it, and the promotions between them.
//!
//! Every path that ends with this replica holding the leadership lives
//! here, from the timer that starts the pre-vote to the `LeaderChange`
//! append that announces the new epoch.

use std::collections::{BTreeMap, BTreeSet};

use super::QuorumStateMachine;
use crate::{
    action::{Action, TimerKind},
    role::{ReplicaProgress, Role},
    types::{Epoch, LogView, NodeId, ReplicaKey, SimInstant},
};

#[cfg(test)]
mod tests;

impl QuorumStateMachine {
    /// The election timer fired.
    ///
    /// A voter starts a KIP-996 pre-vote round and becomes `Prospective`. An
    /// observer never elects.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, is_voter = self.is_voter())
    )]
    pub(super) fn handle_election_timeout(
        &mut self,
        log: &dyn LogView,
        now: SimInstant,
    ) -> Vec<Action> {
        if !self.is_voter() {
            return Vec::new();
        }
        self.start_election(log, now)
    }

    /// Shared election-start path for `ElectionTimeout` and for a resigning
    /// leader's `EndQuorumEpoch`.
    ///
    /// This method makes the replica `Prospective` and broadcasts a non-binding
    /// pre-vote at the *current* epoch. The epoch is not bumped until the
    /// pre-vote succeeds.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch)
    )]
    pub(super) fn start_election(&mut self, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        // Starting a pre-vote round means we have given up on the current leader
        // (our fetch timed out, or the leader resigned). Drop the leader belief:
        // KIP-996 only grants a pre-vote when the voter is no longer following a
        // live leader, and the grant check keys off `leader_id.is_none()`. If we
        // kept `leader_id = Some(old)` here, a `Prospective` voter would refuse to
        // grant pre-votes to an equally-stranded peer, and re-election after the
        // leader is lost would deadlock (no voter can ever clear its stale leader
        // belief without a new leader, which can never be elected). The epoch is
        // unchanged — this is not a step-up to a new epoch, just abandoning the
        // dead leader for the current one.
        self.state.leader_id = None;
        let mut granted = BTreeSet::new();
        granted.insert(self.me);
        let deadline = self.election_deadline(now);
        self.role = Role::Prospective {
            granted,
            election_deadline: deadline,
        };
        let mut actions = vec![
            Action::TransitionedTo(self.role.name()),
            Action::SendVoteRequest {
                epoch: self.state.leader_epoch,
                pre_vote: true,
            },
            Action::ResetTimer {
                kind: TimerKind::Election,
                deadline,
            },
        ];
        // A lone voter wins its own pre-vote immediately.
        if self.tally_prevote_reached_majority() {
            actions.extend(self.promote_to_candidate(log, now));
        }
        actions
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, from = from.0, resp_epoch = epoch, vote_granted, role = self.role.name())
    )]
    pub(super) fn handle_vote_response(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        epoch: Epoch,
        vote_granted: bool,
        now: SimInstant,
    ) -> Vec<Action> {
        // A higher-epoch rejection fences us: step down to that epoch.
        if !vote_granted && epoch > self.state.leader_epoch {
            let mut actions = Vec::new();
            self.transition_to_unattached(epoch, now, &mut actions);
            return actions;
        }
        if !vote_granted {
            return Vec::new();
        }
        // Match the grant to our round by our OWN role + epoch — exactly as
        // Kafka does (its `VoteResponse` carries no pre-vote flag). `Prospective`
        // ⇒ this is a pre-vote grant; `Candidate` ⇒ a real-vote grant. The epoch
        // guard drops a stale grant from a superseded round (e.g. a late pre-vote
        // grant at epoch E arriving after we bumped to E+1 and became Candidate).
        match &mut self.role {
            Role::Prospective { granted, .. } if epoch == self.state.leader_epoch => {
                granted.insert(from);
                if self.tally_prevote_reached_majority() {
                    self.promote_to_candidate(log, now)
                } else {
                    Vec::new()
                }
            }
            Role::Candidate { granted, .. } if epoch == self.state.leader_epoch => {
                granted.insert(from);
                if self.tally_candidate_reached_majority() {
                    self.promote_to_leader(log)
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }

    /// Whether the current `Prospective` grant set has reached a quorum.
    fn tally_prevote_reached_majority(&self) -> bool {
        match &self.role {
            Role::Prospective { granted, .. } => granted.len() >= self.state.majority(),
            _ => false,
        }
    }

    /// Whether the current `Candidate` grant set has reached a quorum.
    fn tally_candidate_reached_majority(&self) -> bool {
        match &self.role {
            Role::Candidate { granted, .. } => granted.len() >= self.state.majority(),
            _ => false,
        }
    }

    /// Pre-vote succeeded: bump the epoch, self-vote, and broadcast a real vote.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch)
    )]
    fn promote_to_candidate(&mut self, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        self.state.leader_epoch = self.state.leader_epoch.saturating_add(1);
        self.state.leader_id = None;
        self.state.voted_key = Some(ReplicaKey {
            id: self.me,
            directory_id: uuid::Uuid::nil(),
        });
        let mut granted = BTreeSet::new();
        granted.insert(self.me);
        let deadline = self.election_deadline(now);
        self.role = Role::Candidate {
            granted,
            election_deadline: deadline,
        };
        let mut actions = vec![
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
            Action::SendVoteRequest {
                epoch: self.state.leader_epoch,
                pre_vote: false,
            },
            Action::ResetTimer {
                kind: TimerKind::Election,
                deadline,
            },
        ];
        // A lone voter wins its own election immediately.
        if self.tally_candidate_reached_majority() {
            actions.extend(self.promote_to_leader_inner(log));
        }
        actions
    }

    /// Real vote succeeded: become leader for the current epoch.
    fn promote_to_leader(&mut self, log: &dyn LogView) -> Vec<Action> {
        self.promote_to_leader_inner(log)
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch)
    )]
    fn promote_to_leader_inner(&mut self, log: &dyn LogView) -> Vec<Action> {
        let epoch = self.state.leader_epoch;
        self.state.leader_id = Some(self.me);
        let mut replicas = BTreeMap::new();
        for id in self.state.voters.ids() {
            if id != self.me {
                replicas.insert(id, ReplicaProgress::default());
            }
        }
        // The leader's `LeaderChange` / first current-epoch record sits at the
        // current log end. The HWM may only advance past this offset (Fig.8).
        let epoch_start_offset = log.end_offset();
        self.role = Role::Leader {
            replicas,
            high_watermark: 0,
            epoch_start_offset,
        };
        vec![
            Action::AppendLeaderChange { epoch },
            Action::SendBeginQuorumEpoch { epoch },
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
        ]
    }
}
