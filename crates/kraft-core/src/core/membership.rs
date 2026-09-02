//! Voter-set updates and the KIP-853 adjacent-voter view.
//!
//! A replica applies a `VotersRecord` as soon as it reads one, before the
//! record commits, so it must keep both sides of the one in-flight
//! transition addressable. This module holds that bookkeeping and the role
//! changes a membership change forces on the local replica.

use krabka_voters::VoterSet;

use super::QuorumStateMachine;
use crate::{
    action::{Action, TimerKind},
    role::Role,
    types::{NodeId, ReplicaKey, SimInstant},
};

impl QuorumStateMachine {
    /// Apply the latest voter set read from the Raft log or a snapshot.
    ///
    /// KIP-853 requires replicas to use an uncommitted `VotersRecord`
    /// immediately. The durable engine owns record history and invokes this
    /// method again with the preceding set if the log is truncated.
    pub fn apply_voter_set(&mut self, voters: VoterSet, now: SimInstant) -> Vec<Action> {
        let was_voter = self.is_voter();
        let leader_id = self.state.leader_id;
        if self.state.voters != voters {
            self.adjacent_voters = Some(self.state.voters.clone());
        }
        self.state.voters = voters;

        if let Role::Leader {
            replicas,
            fetched_voters,
            ..
        } = &mut self.role
        {
            replicas.retain(|id, _| self.state.voters.contains(*id) && *id != self.me);
            for id in self.state.voters.ids() {
                if id != self.me {
                    replicas.entry(id).or_default();
                }
            }
            // A removed voter's fetch cannot count toward the new
            // configuration's check-quorum majority, so drop it from the
            // in-flight window rather than let it inflate the next tally.
            fetched_voters.retain(|id| self.state.voters.contains(*id) && *id != self.me);
            // Start a fresh window against the new configuration. A sole voter
            // arms nothing at promotion, so without this the leader of a
            // cluster that has just grown past one voter would never run
            // check-quorum for the rest of its epoch. Re-arming here also gives
            // an added voter a full window to make its first Fetch, rather than
            // deposing the leader the instant the record applies.
            if self.runs_check_quorum() {
                return vec![Action::ResetTimer {
                    kind: TimerKind::CheckQuorum,
                    deadline: self.check_quorum_deadline(now),
                }];
            }
            return Vec::new();
        }

        match (was_voter, self.is_voter()) {
            (true, false) => {
                let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
                self.role = Role::Observer {
                    leader_id,
                    fetch_deadline,
                };
                let mut actions = vec![Action::TransitionedTo(self.role.name())];
                if let Some(leader_id) = leader_id {
                    actions.push(Action::SendFetch { leader_id });
                    actions.push(Action::ResetTimer {
                        kind: TimerKind::Fetch,
                        deadline: fetch_deadline,
                    });
                }
                actions
            }
            (false, true) => {
                if let Some(leader_id) = leader_id {
                    let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
                    self.role = Role::Follower {
                        leader_id,
                        fetch_deadline,
                    };
                    vec![
                        Action::TransitionedTo(self.role.name()),
                        Action::SendFetch { leader_id },
                        Action::ResetTimer {
                            kind: TimerKind::Fetch,
                            deadline: fetch_deadline,
                        },
                    ]
                } else {
                    let election_deadline = self.election_deadline(now);
                    self.role = Role::Unattached { election_deadline };
                    vec![
                        Action::TransitionedTo(self.role.name()),
                        Action::ResetTimer {
                            kind: TimerKind::Election,
                            deadline: election_deadline,
                        },
                    ]
                }
            }
            _ => Vec::new(),
        }
    }

    /// Apply a committed `KRaftVersionRecord`.
    pub fn set_kraft_version(&mut self, version: u16) {
        self.state.kraft_version = version;
    }

    /// Forget the preceding voter view once the latest voter record commits.
    pub fn commit_voter_set(&mut self) {
        self.adjacent_voters = None;
    }

    fn voter_key_matches(&self, voters: &VoterSet, key: ReplicaKey) -> bool {
        voters.get(key.id).is_some_and(|voter| {
            self.state.kraft_version == 0 || voter.directory_id == key.directory_id
        })
    }

    pub(super) fn local_voter_directory_matches(&self, directory_id: uuid::Uuid) -> bool {
        self.state
            .voters
            .get(self.me)
            .is_none_or(|voter| self.state.kraft_version == 0 || voter.directory_id == directory_id)
    }

    pub(super) fn current_or_adjacent_voter(&self, id: NodeId) -> bool {
        self.state.voters.contains(id)
            || self
                .adjacent_voters
                .as_ref()
                .is_some_and(|voters| voters.contains(id))
    }

    pub(super) fn current_or_adjacent_voter_key(&self, key: ReplicaKey) -> bool {
        self.voter_key_matches(&self.state.voters, key)
            || self
                .adjacent_voters
                .as_ref()
                .is_some_and(|voters| self.voter_key_matches(voters, key))
    }

    pub(super) fn same_voter(&self, left: ReplicaKey, right: ReplicaKey) -> bool {
        left.id == right.id
            && (self.state.kraft_version == 0 || left.directory_id == right.directory_id)
    }

    /// Complete removal of the local leader after the reduced voter set has
    /// committed. Fetch serving continues until the engine invokes this edge.
    pub fn finish_local_leader_removal(&mut self, now: SimInstant) -> Vec<Action> {
        if self.is_voter() || !self.role.is_leader() {
            return Vec::new();
        }
        let epoch = self.state.leader_epoch;
        self.state.leader_id = None;
        let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
        self.role = Role::Observer {
            leader_id: None,
            fetch_deadline,
        };
        vec![
            Action::SendEndQuorumEpoch { epoch },
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
        ]
    }
}
