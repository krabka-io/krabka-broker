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

/// A vote or a pre-vote from a higher epoch moves us to that epoch before the
/// grant is decided, as `KafkaRaftClient.handleVoteRequest` does for both.
#[test]
fn a_vote_or_pre_vote_from_a_higher_epoch_advances_our_epoch() {
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    for (pre_vote, want_epoch) in [(false, 7), (true, 7)] {
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
    // The epoch is adopted, but the pre-vote is not a vote: none is recorded.
    assert2::assert!((m.quorum_state().voted_key, m.quorum_state().leader_epoch) == (None, 1));
}

/// One inbound Vote or pre-vote of `epoch` from `candidate`, whose log ends at
/// `(last_epoch, last_offset)`.
fn vote_event(
    candidate: NodeId,
    epoch: u32,
    pre_vote: bool,
    last_epoch: u32,
    last_offset: i64,
) -> Event {
    Event::ReceiveVoteRequest {
        from: candidate,
        cluster_id: None,
        voter_id: NodeId(1),
        voter_directory_id: uuid::Uuid::nil(),
        candidate_epoch: epoch,
        candidate,
        candidate_directory_id: uuid::Uuid::nil(),
        candidate_log_end: LogEnd {
            last_epoch,
            last_offset,
        },
        pre_vote,
    }
}

/// Voter 1's log ends at offset 5 in epoch 1: node 2's log is up to date at
/// `(1, 5)` and behind at `(1, 4)`.
const OUR_LOG: FakeLog = FakeLog {
    end: 5,
    last_epoch: 1,
};

/// What `m` answers a Vote or pre-vote of `epoch` from node 2, as (granted,
/// epoch in the reply).
fn vote_answer(
    m: &mut QuorumStateMachine,
    epoch: u32,
    pre_vote: bool,
    up_to_date: bool,
) -> (bool, u32) {
    let last_offset = if up_to_date { 5 } else { 4 };
    let actions = m.on_event(
        vote_event(NodeId(2), epoch, pre_vote, 1, last_offset),
        &OUR_LOG,
        SimInstant(0),
    );
    actions
        .iter()
        .find_map(|action| match action {
            Action::ReplyVote { epoch, granted, .. } => Some((*granted, *epoch)),
            _ => None,
        })
        .expect("a vote request is answered")
}

/// A test state machine, built fresh for each row of a table.
type Build = Box<dyn Fn() -> QuorumStateMachine>;

/// A machine for voter 1 in a three-voter quorum that follows node 3 at epoch
/// 4, and has fetched from it when `fetched`.
fn follower_of_three(fetched: bool) -> QuorumStateMachine {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    m.on_event(
        Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(3),
            leader_epoch: 4,
        },
        &OUR_LOG,
        SimInstant(10),
    );
    if fetched {
        m.on_event(
            Event::ReceiveFetchResponse {
                leader_id: NodeId(3),
                leader_epoch: 4,
                diverging: None,
            },
            &OUR_LOG,
            SimInstant(20),
        );
    }
    assert2::assert!(matches!(m.role(), Role::Follower { .. }));
    m
}

/// A machine for voter 1 in a three-voter quorum that leads epoch 1.
fn leader_of_three() -> QuorumStateMachine {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    m.on_event(Event::ElectionTimeout, &OUR_LOG, SimInstant(0));
    for epoch in [0, 1] {
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch,
                vote_granted: true,
            },
            &OUR_LOG,
            SimInstant(1),
        );
    }
    assert2::assert!(m.role().is_leader());
    m
}

/// Each state's `canGrantVote` for a request at the state's own epoch: a
/// follower grants a pre-vote only until it has fetched from its leader, a
/// leader grants nothing, a candidate or a resigned leader grants a pre-vote on
/// log recency alone, and nothing but an unattached, prospective or voted
/// replica grants a binding vote.
#[test]
fn each_state_grants_pre_votes_and_votes_by_its_own_rule() {
    let unattached = || machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let candidate = || {
        let mut m = leader_of_three();
        m.on_event(Event::CheckQuorumTimeout, &OUR_LOG, SimInstant(2));
        // A resigned leader's election timer starts the next pre-vote round.
        m.on_event(Event::ElectionTimeout, &OUR_LOG, SimInstant(3));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(3),
                epoch: m.quorum_state().leader_epoch,
                vote_granted: true,
            },
            &OUR_LOG,
            SimInstant(4),
        );
        assert2::assert!(matches!(m.role(), Role::Candidate { .. }), "{:?}", m.role());
        m
    };
    let resigned = || {
        let mut m = leader_of_three();
        m.on_event(Event::CheckQuorumTimeout, &OUR_LOG, SimInstant(2));
        assert2::assert!(matches!(m.role(), Role::Resigned));
        m
    };
    // (state, machine, its epoch, pre-vote granted when up to date, vote granted
    // when up to date)
    let cases: Vec<(&str, Build, u32, bool, bool)> = vec![
        ("leader", Box::new(leader_of_three), 1, false, false),
        (
            "follower not yet fetched",
            Box::new(|| follower_of_three(false)),
            4,
            true,
            false,
        ),
        (
            "follower that has fetched",
            Box::new(|| follower_of_three(true)),
            4,
            false,
            false,
        ),
        ("candidate", Box::new(candidate), 2, true, false),
        ("resigned leader", Box::new(resigned), 1, true, false),
        ("unattached", Box::new(unattached), 0, true, true),
    ];
    for (label, build, epoch, pre_vote_granted, vote_granted) in cases {
        let mut m = build();
        check!(
            vote_answer(&mut m, epoch, true, true) == (pre_vote_granted, epoch),
            "{label}: pre-vote"
        );
        let mut m = build();
        check!(
            vote_answer(&mut m, epoch, false, true) == (vote_granted, epoch),
            "{label}: vote"
        );
        let mut m = build();
        check!(
            vote_answer(&mut m, epoch, true, false) == (false, epoch),
            "{label}: pre-vote with a log that is behind"
        );
    }
}

/// A stale leader or follower that sees a pre-vote from a higher epoch steps
/// down to that epoch and answers from there, with the new epoch, so the
/// election proceeds without waiting for a check-quorum or fetch timeout.
#[test]
fn a_pre_vote_from_a_higher_epoch_steps_down_a_stale_leader_or_follower() {
    let cases: Vec<(&str, Build)> = vec![
        ("leader", Box::new(leader_of_three)),
        (
            "follower that has fetched",
            Box::new(|| follower_of_three(true)),
        ),
    ];
    for (label, build) in cases {
        let mut m = build();
        let epoch = m.quorum_state().leader_epoch + 3;
        check!(
            vote_answer(&mut m, epoch, true, true) == (true, epoch),
            "{label}: grants at the new epoch"
        );
        check!(
            (
                matches!(m.role(), Role::Unattached { .. }),
                m.quorum_state().leader_epoch,
                m.quorum_state().leader_id,
                m.quorum_state().voted_key,
            ) == (true, epoch, None, None),
            "{label}: is unattached at the new epoch"
        );
    }
}

/// Only the candidate a replica already voted for gets its vote again, however
/// the candidate's log compares, as `unattachedOrProspectiveCanGrantVote` says.
#[test]
fn a_vote_is_granted_again_only_to_the_candidate_it_went_to() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    check!(vote_answer(&mut m, 1, false, true) == (true, 1));
    check!(
        vote_answer(&mut m, 1, false, false) == (true, 1),
        "same candidate"
    );
    let actions = m.on_event(
        vote_event(NodeId(3), 1, false, 1, 5),
        &OUR_LOG,
        SimInstant(0),
    );
    check!(
        actions
            .iter()
            .any(|action| matches!(action, Action::ReplyVote { granted: false, .. })),
        "another candidate"
    );
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

#[test]
fn prevote_rejected_when_candidate_log_is_not_up_to_date() {
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
                last_offset: 5,
            },
            pre_vote: true,
        },
        &log,
        SimInstant(0),
    );
    assert2::assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::ReplyVote { granted: false, .. }))
    );
}
