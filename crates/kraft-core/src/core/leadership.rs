//! Following somebody else: the `BeginQuorumEpoch` and `EndQuorumEpoch`
//! announcements a leader sends about its own epoch.
//!
//! Both events decide whether this replica attaches to the announced leader
//! or detaches from it, and both apply the same staleness and membership
//! guards, so they share a module.

use super::QuorumStateMachine;
use crate::{
    action::{Action, TimerKind},
    role::Role,
    types::{Epoch, LogView, NodeId, SimInstant},
};

impl QuorumStateMachine {
    /// A leader announced its epoch.
    ///
    /// If the epoch is at least our current epoch, we follow that leader and
    /// become a `Follower` or an attached `Observer`. This method ignores a
    /// stale announcement at a lower epoch.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, leader_id = leader_id.0, leader_epoch)
    )]
    pub(super) fn handle_begin_quorum_epoch(
        &mut self,
        leader_id: NodeId,
        leader_epoch: Epoch,
        now: SimInstant,
    ) -> Vec<Action> {
        // A never-initialized joiner has no membership view yet and discovers
        // its first leader through configured bootstrap endpoints. Once a view
        // exists, accept only the current or explicitly adjacent KIP-853 set.
        let membership_known = !self.state.voters.is_empty()
            || self
                .adjacent_voters
                .as_ref()
                .is_some_and(|voters| !voters.is_empty());
        if membership_known && !self.current_or_adjacent_voter(leader_id) {
            return Vec::new();
        }
        // Accept a strictly-higher epoch, or an equal epoch only if we do not
        // already know a leader for it (one leader per epoch). Otherwise ignore.
        let accept = leader_epoch > self.state.leader_epoch
            || (leader_epoch == self.state.leader_epoch && self.state.leader_id.is_none());
        if !accept {
            return Vec::new();
        }
        self.state.leader_epoch = leader_epoch;
        self.state.leader_id = Some(leader_id);
        self.state.voted_key = None;
        let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
        self.role = if self.is_voter() {
            Role::Follower {
                leader_id,
                fetch_deadline,
            }
        } else {
            Role::Observer {
                leader_id: Some(leader_id),
                fetch_deadline,
            }
        };
        vec![
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
            Action::SendFetch { leader_id },
            Action::ResetTimer {
                kind: TimerKind::Fetch,
                deadline: fetch_deadline,
            },
        ]
    }

    /// A resigning leader asked us to start an election.
    ///
    /// If the request is not stale, a voter starts a pre-vote round immediately
    /// and does not wait for the election timer. An observer detaches and
    /// continues to observe.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, leader_epoch)
    )]
    pub(super) fn handle_end_quorum_epoch(
        &mut self,
        log: &dyn LogView,
        _leader_id: NodeId,
        leader_epoch: Epoch,
        now: SimInstant,
    ) -> Vec<Action> {
        if leader_epoch < self.state.leader_epoch {
            return Vec::new();
        }
        if self.is_voter() {
            self.start_election(log, now)
        } else {
            let mut actions = Vec::new();
            self.transition_to_unattached(self.state.leader_epoch, now, &mut actions);
            actions
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::{
        core::test_support::{FakeLog, machine, voters},
        event::Event,
    };

    #[test]
    fn begin_quorum_epoch_makes_us_follower() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(
            (
                matches!(
                    m.role(),
                    Role::Follower {
                        leader_id: NodeId(2),
                        ..
                    }
                ),
                m.quorum_state().leader_epoch,
                m.quorum_state().leader_id,
                actions.iter().any(|a| matches!(
                    a,
                    Action::SendFetch {
                        leader_id: NodeId(2)
                    }
                )),
                actions
                    .iter()
                    .any(|a| matches!(a, Action::PersistQuorumState)),
            ) == (true, 4, Some(NodeId(2)), true, true)
        );
    }

    #[test]
    fn end_quorum_epoch_triggers_immediate_election() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        // follow leader 2 @ epoch 4 first
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        let actions = m.on_event(
            Event::ReceiveEndQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(11),
        );
        // immediately start pre-vote (Prospective), not wait for timeout
        assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
        assert2::assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::SendVoteRequest { pre_vote: true, .. }))
        );
    }

    #[test]
    fn stale_begin_quorum_epoch_ignored() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        m.force_epoch(7);
        let log = FakeLog {
            end: 5,
            last_epoch: 7,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        assert2::assert!((actions.is_empty(), m.quorum_state().leader_id) == (true, None));
    }

    #[test]
    fn begin_quorum_epoch_from_adjacent_voter_view_is_accepted() {
        // KIP-853: a newly elected leader may be absent from our temporarily
        // stale local voter view. Adopt the higher epoch and fetch its log.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let _ = m.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(99)]), SimInstant(0));
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(99), // not a voter
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(
            (
                m.quorum_state().leader_id,
                m.quorum_state().leader_epoch,
                matches!(m.role(), Role::Follower { .. }),
                actions.iter().any(|action| matches!(
                    action,
                    Action::SendFetch {
                        leader_id: NodeId(99)
                    }
                )),
            ) == (Some(NodeId(99)), 4, true, true)
        );
    }

    #[test]
    fn begin_quorum_epoch_from_voter_leader_still_accepted() {
        // C-2 must not break the legitimate path: a voter leader is adopted.
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2), // a real voter
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        assert2::assert!(matches!(
            m.role(),
            Role::Follower {
                leader_id: NodeId(2),
                ..
            }
        ));
        check!(m.quorum_state().leader_id == Some(NodeId(2)));
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendFetch {
                leader_id: NodeId(2)
            }
        )));
    }
}
