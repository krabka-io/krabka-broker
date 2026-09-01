use assert2::check;

use super::*;
use crate::{
    core::test_support::{FakeLog, TEST_ELECTION_TIMEOUT, machine, voters},
    event::Event,
    types::{NodeId, QuorumState},
};

fn dynamic_machine() -> (QuorumStateMachine, uuid::Uuid, uuid::Uuid, uuid::Uuid) {
    let cluster_id = uuid::Uuid::from_u128(1);
    let voter_directory_id = uuid::Uuid::from_u128(11);
    let candidate_directory_id = uuid::Uuid::from_u128(22);
    let voters = krabka_voters::VoterSet::from_voters([
        krabka_voters::Voter {
            id: NodeId(1),
            directory_id: voter_directory_id,
            endpoints: vec![],
            kraft_version: krabka_voters::KRaftVersionRange::default(),
        },
        krabka_voters::Voter {
            id: NodeId(2),
            directory_id: candidate_directory_id,
            endpoints: vec![],
            kraft_version: krabka_voters::KRaftVersionRange::default(),
        },
    ]);
    let mut state = QuorumState::bootstrap(cluster_id, voters);
    state.kraft_version = 1;
    (
        QuorumStateMachine::new(NodeId(1), state, TEST_ELECTION_TIMEOUT),
        cluster_id,
        voter_directory_id,
        candidate_directory_id,
    )
}

/// A replica that is not a voter denies every vote request, whoever the
/// candidate is.
///
/// Being a voter and the candidate being one are separate requirements,
/// and an observer satisfies neither -- joining them so that both must
/// fail before denying would let an observer cast a vote.
#[test]
fn an_observer_denies_a_vote_request() {
    // Not in its own voter set: an observer.
    let mut m = machine(NodeId(9), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(9),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    check!(
        actions
            .iter()
            .any(|a| matches!(a, Action::ReplyVote { granted: false, .. }))
    );
    check!(
        m.quorum_state().voted_key.is_none(),
        "an observer must not record a vote"
    );
}

/// A candidate at our own epoch is not fenced.
///
/// Fencing is for a candidate *behind* us. Treating "equal" as behind
/// would deny every first-round vote, because a candidate that bumps to
/// epoch E asks replicas still at E.
#[test]
fn a_candidate_at_our_own_epoch_is_not_fenced() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 0,
    };
    // Both sides at epoch 0, the bootstrap epoch.
    check!(m.quorum_state().leader_epoch == 0);
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 0,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 0,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    check!(
        actions
            .iter()
            .any(|a| matches!(a, Action::ReplyVote { granted: true, .. }))
    );
}

/// A standard vote request from a higher epoch moves us to that epoch
/// before the grant is decided. Pre-vote never does.
#[test]
fn a_standard_vote_from_a_higher_epoch_advances_our_epoch() {
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    for (pre_vote, want_epoch) in [(false, 7), (true, 0)] {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                cluster_id: None,
                voter_id: NodeId(1),
                voter_directory_id: uuid::Uuid::nil(),
                candidate_epoch: 7,
                candidate: NodeId(2),
                candidate_directory_id: uuid::Uuid::nil(),
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote,
            },
            &log,
            SimInstant(0),
        );
        check!(
            m.quorum_state().leader_epoch == want_epoch,
            "pre_vote={pre_vote}: epoch {}",
            m.quorum_state().leader_epoch
        );
    }
}

#[test]
fn grants_standard_vote_when_log_up_to_date_and_not_voted() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::ReplyVote {
            to: NodeId(2),
            granted: true,
            ..
        }
    )));
    assert2::assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(NodeId(2))); // binding
}

#[test]
fn denies_standard_vote_when_candidate_log_behind() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 10,
        last_epoch: 2,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 2,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 3,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::ReplyVote { granted: false, .. }))
    );
}

#[test]
fn pre_vote_grant_is_non_binding() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: true,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!((m.quorum_state().voted_key, m.quorum_state().leader_epoch) == (None, 0));
}

#[test]
fn denies_standard_vote_when_already_voted_for_other() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    // vote for 2 first
    m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    // now 3 asks in the same epoch
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(3),
            cluster_id: None,
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(3),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::ReplyVote {
            to: NodeId(3),
            granted: false,
            ..
        }
    )));
}

#[test]
fn fenced_when_candidate_epoch_below_current() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    m.force_epoch(5); // test helper
    let log = FakeLog {
        end: 5,
        last_epoch: 5,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 3,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 5,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::ReplyVote { granted: false, .. }))
    );
}

#[test]
fn vote_from_adjacent_voter_view_is_granted_when_up_to_date() {
    // KIP-853 permits an up-to-date candidate from an adjacent voter view;
    // only the local latest set determines whether this replica may vote.
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let _ = m.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(99)]), SimInstant(0));
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(99),
            cluster_id: None,
            voter_id: NodeId(1), // addressed to us
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(99), // not a voter
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(
        m.quorum_state()
            .voted_key
            .is_some_and(|key| key.id == NodeId(99))
    );
    assert2::assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::ReplyVote { granted: true, .. }))
    );
}

#[test]
fn vote_addressed_to_other_voter_rejected() {
    // C-2: a Vote addressed (voter_id) to a different node than us is
    // ignored, even if the candidate is a legitimate voter.
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(3), // addressed to node 3, not us (node 1)
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!((actions.is_empty(), m.quorum_state().voted_key) == (true, None));
}

#[test]
fn vote_from_voter_addressed_to_us_still_granted() {
    // C-2 must not break the legitimate path: a voter candidate addressing
    // us is still granted.
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(1), // addressed to us
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::ReplyVote {
            to: NodeId(2),
            granted: true,
            ..
        }
    )));
    assert2::assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(NodeId(2)));
}

#[test]
fn zero_target_is_not_a_wildcard_for_a_nonzero_voter() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(0),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!((actions.is_empty(), m.quorum_state().voted_key) == (true, None));
}

#[test]
fn zero_target_is_valid_for_voter_zero() {
    let mut m = machine(NodeId(0), &[NodeId(0), NodeId(2)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: None,
            voter_id: NodeId(0),
            voter_directory_id: uuid::Uuid::nil(),
            candidate_epoch: 1,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::ReplyVote { granted: true, .. }))
    );
    assert2::assert!(m.quorum_state().voted_key.map(|key| key.id) == Some(NodeId(2)));
}

#[test]
fn stale_target_directory_is_ignored_before_epoch_mutation() {
    let (mut m, cluster_id, _voter_directory_id, candidate_directory_id) = dynamic_machine();
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: Some(cluster_id),
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::from_u128(99),
            candidate_epoch: 7,
            candidate: NodeId(2),
            candidate_directory_id,
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(actions.is_empty());
    assert2::assert!((m.quorum_state().leader_epoch, m.quorum_state().voted_key) == (0, None));
}

#[test]
fn stale_candidate_directory_is_denied_before_epoch_mutation() {
    let (mut m, cluster_id, voter_directory_id, _candidate_directory_id) = dynamic_machine();
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: Some(cluster_id),
            voter_id: NodeId(1),
            voter_directory_id,
            candidate_epoch: 7,
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::from_u128(99),
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::ReplyVote { granted: false, .. }))
    );
    assert2::assert!((m.quorum_state().leader_epoch, m.quorum_state().voted_key) == (0, None));
}

#[test]
fn foreign_cluster_is_denied_before_epoch_mutation() {
    let (mut m, _cluster_id, voter_directory_id, candidate_directory_id) = dynamic_machine();
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(
        Event::ReceiveVoteRequest {
            from: NodeId(2),
            cluster_id: Some(uuid::Uuid::from_u128(99)),
            voter_id: NodeId(1),
            voter_directory_id,
            candidate_epoch: 7,
            candidate: NodeId(2),
            candidate_directory_id,
            candidate_log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::ReplyVote { granted: false, .. }))
    );
    assert2::assert!((m.quorum_state().leader_epoch, m.quorum_state().voted_key) == (0, None));
}
