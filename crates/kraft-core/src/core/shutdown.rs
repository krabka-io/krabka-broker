//! Graceful shutdown: the leader hands its epoch over before it stops.
//!
//! Kafka's `KafkaRaftClient.shutdown` sets a `GracefulShutdown`, and the poll
//! loop then does two things with it. `pollLeader` resigns at once, through
//! `transitionToResigned(nonLeaderVotersByDescendingFetchOffset)`, which sends
//! `EndQuorumEpoch` to every voter, so the most caught-up one elects without
//! waiting out its fetch timeout. `maybeCompleteShutdown` ends the shutdown as
//! soon as nothing is left to hand over: the replica is an observer, the only
//! voter, or knows a leader that is not itself.

use {
    super::QuorumStateMachine,
    crate::{action::Action, types::SimInstant},
};

impl QuorumStateMachine {
    /// Resign the leadership this replica holds because it is shutting down.
    ///
    /// A leader becomes `Resigned` and sends `EndQuorumEpoch`, naming the other
    /// voters by descending fetch offset as its preferred successors. This is
    /// `pollLeader` with a `GracefulShutdown` set. Any other role holds no
    /// leadership to hand over, and nothing changes.
    pub fn resign_for_shutdown(&mut self, now: SimInstant) -> Vec<Action> {
        if self.role.is_leader() && !self.graceful_shutdown_complete() {
            self.transition_to_resigned(now)
        } else {
            Vec::new()
        }
    }

    /// Whether a shutting-down replica has nothing left to hand over.
    ///
    /// This is Kafka's `KafkaRaftClient.maybeCompleteShutdown`: an observer, the
    /// only voter, and a replica that knows a leader other than itself stop at
    /// once. A leader, a resigned leader, and a voter between leaders keep
    /// running until a remote leader is known, which is what lets the quorum
    /// elect a successor without them.
    #[must_use]
    pub fn graceful_shutdown_complete(&self) -> bool {
        !self.is_voter()
            || self.state.voters.len() == 1
            || self.known_leader().is_some_and(|leader| leader != self.me)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            action::TimerKind,
            core::{
                election_jitter_ms,
                test_support::{FakeLog, machine, three_voter_machine, win_election},
            },
            event::Event,
            role::Role,
            types::NodeId,
        },
        assert2::check,
    };

    /// Voter 1 leading the standard three-voter quorum at epoch 1, after
    /// voter 2 fetched to `fetched_by_2` and voter 3 to `fetched_by_3`.
    fn leader_after_fetches(
        log: &FakeLog,
        fetched_by_2: i64,
        fetched_by_3: i64,
    ) -> QuorumStateMachine {
        let mut m = three_voter_machine();
        win_election(&mut m, log, &[NodeId(2), NodeId(3)], SimInstant(2000));
        for (from, fetch_offset) in [(NodeId(2), fetched_by_2), (NodeId(3), fetched_by_3)] {
            m.on_event(
                Event::ReceiveFetch {
                    from,
                    fetch_epoch: 1,
                    fetch_offset,
                },
                log,
                SimInstant(2100),
            );
        }
        m
    }

    /// The leader resigns with its followers ranked by how far they fetched,
    /// the most caught-up first, and with the id breaking a tie. That order is
    /// what makes the first successor elect at once.
    #[yare::parameterized(
        third_voter_ahead = { 2, 5, vec![NodeId(3), NodeId(2)] },
        second_voter_ahead = { 5, 2, vec![NodeId(2), NodeId(3)] },
        tied = { 4, 4, vec![NodeId(2), NodeId(3)] },
    )]
    fn a_leader_resigns_to_its_most_caught_up_followers(
        fetched_by_2: i64,
        fetched_by_3: i64,
        preferred_successors: Vec<NodeId>,
    ) {
        let log = FakeLog::new(5, 1);
        let mut m = leader_after_fetches(&log, fetched_by_2, fetched_by_3);

        let actions = m.resign_for_shutdown(SimInstant(3000));

        check!(
            actions
                == vec![
                    Action::SendEndQuorumEpoch {
                        epoch: 1,
                        preferred_successors,
                    },
                    Action::PersistQuorumState,
                    Action::TransitionedTo("Resigned"),
                    Action::ResetTimer {
                        kind: TimerKind::Election,
                        deadline: SimInstant(3000 + 1000 + election_jitter_ms(NodeId(1), 1, 1000)),
                    },
                ]
        );
        check!(
            (
                m.role().clone(),
                m.quorum_state().leader_id,
                m.quorum_state().leader_epoch
            ) == (Role::Resigned, None, 1)
        );
        check!(!m.graceful_shutdown_complete());
    }

    /// Only a leader has an epoch to hand over. Resigning anything else, or
    /// resigning twice, changes nothing.
    #[test]
    fn only_a_leader_resigns_for_shutdown() {
        let log = FakeLog::new(5, 1);
        let mut resigned = leader_after_fetches(&log, 5, 5);
        resigned.resign_for_shutdown(SimInstant(3000));
        let mut sole = machine(NodeId(1), &[NodeId(1)]);
        win_election(&mut sole, &log, &[], SimInstant(2000));
        let cases = [
            ("unattached voter", three_voter_machine()),
            (
                "observer",
                machine(NodeId(4), &[NodeId(1), NodeId(2), NodeId(3)]),
            ),
            ("resigned leader", resigned),
            ("sole voter leading", sole),
        ];
        for (label, mut m) in cases {
            let before = m.clone();
            let actions = m.resign_for_shutdown(SimInstant(4000));
            check!((label, actions, m == before) == (label, Vec::new(), true));
        }
    }

    /// Kafka's `maybeCompleteShutdown`: a replica stops at once when it is an
    /// observer, the only voter, or knows a remote leader, and keeps running
    /// while the quorum may still need it to elect a successor.
    #[test]
    fn a_shutdown_completes_once_nothing_is_left_to_hand_over() {
        let log = FakeLog::new(5, 1);
        let mut follower = three_voter_machine();
        follower.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        let mut prospective = follower.clone();
        prospective.on_event(Event::FetchTimeout, &log, SimInstant(20));
        let mut resigned = leader_after_fetches(&log, 5, 5);
        resigned.resign_for_shutdown(SimInstant(3000));
        let mut successor_known = resigned.clone();
        successor_known.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(3),
                leader_epoch: 2,
            },
            &log,
            SimInstant(3100),
        );
        let cases = [
            (
                "observer",
                machine(NodeId(4), &[NodeId(1), NodeId(2), NodeId(3)]),
                true,
            ),
            ("only voter", machine(NodeId(1), &[NodeId(1)]), true),
            ("follower", follower, true),
            ("prospective keeping its leader", prospective, true),
            ("successor known", successor_known, true),
            ("unattached voter", three_voter_machine(), false),
            ("leader", leader_after_fetches(&log, 5, 5), false),
            ("resigned leader", resigned, false),
        ];
        for (label, m, complete) in cases {
            check!((label, m.graceful_shutdown_complete()) == (label, complete));
        }
    }
}
