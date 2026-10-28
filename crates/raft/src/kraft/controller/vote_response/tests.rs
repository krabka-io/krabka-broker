use {
    super::*,
    crate::kraft::{
        controller::{
            quorum_requests::NOT_LEADER_OR_FOLLOWER,
            test_support::{build_engine_only, record_peer_sends, recv_peer_send},
        },
        role::Role,
        transport::api_key,
    },
};

/// Node 1 asks at epoch 5.
const ASKER: VoteAsker = VoteAsker {
    me: NodeId(1),
    epoch: 5,
    known_leader: None,
};

use rstest_bdd_macros::{given, scenarios, then, when};
#[derive(Default)]
struct VoteInterpretation {
    known_leader: Option<NodeId>,
    step: Option<VoteResponseStep>,
}
#[rstest::fixture]
fn interpretation() -> VoteInterpretation {
    // Start this scenario with fresh inputs and observations.

    VoteInterpretation::default()
}
fn response_leader(value: &str) -> Option<NodeId> {
    if value == "none" {
        None
    } else {
        Some(NodeId(value.parse().expect("leader node identifier")))
    }
}
#[given("node 1 has requested votes at epoch 5 with known leader {leader}")]
fn voting_node(interpretation: &mut VoteInterpretation, leader: String) {
    interpretation.known_leader = response_leader(&leader);
}
#[when(
    "a Vote response names leader {leader} at epoch {epoch} with error {error} and grant {granted}"
)]
fn interpret_vote(
    interpretation: &mut VoteInterpretation,
    leader: String,
    epoch: Epoch,
    error: i16,
    granted: bool,
) {
    interpretation.step = Some(vote_response_step(
        VoteAsker {
            known_leader: interpretation.known_leader,
            ..ASKER
        },
        VoteAnswer {
            error_code: error,
            granted,
            leader_id: response_leader(&leader),
            epoch,
        },
    ));
}
#[then("the voter chooses {action} at epoch {epoch} with leader {leader} and grant {granted}")]
fn vote_transition(
    interpretation: &VoteInterpretation,
    action: String,
    epoch: Epoch,
    leader: String,
    granted: bool,
) {
    let expected = match action.as_str() {
        "ignore" => VoteResponseStep::Ignore,
        "follow" => VoteResponseStep::Follow {
            leader_id: response_leader(&leader).expect("leader to follow"),
            epoch,
        },
        "unattached" => VoteResponseStep::Unattached { epoch },
        "count" => VoteResponseStep::Count { epoch, granted },
        _ => panic!("unknown vote transition"),
    };
    assert2::assert!(interpretation.step.as_ref() == Some(&expected));
}
scenarios!("tests/features/migrated/src/kraft/controller/vote_response/tests.feature", fixtures = [interpretation: VoteInterpretation]);

/// A Vote answer encoded as voter 2 sends it.
fn answer(granted: bool, leader_id: Option<NodeId>, epoch: Epoch) -> bytes::Bytes {
    wire::PeerResponse::Vote {
        error_code: 0,
        granted,
        leader: wire::QuorumLeader {
            leader_id,
            epoch,
            endpoint: None,
        },
    }
    .encode()
}

/// A prospective that a voter refuses while naming the leader of its epoch
/// follows that leader and fetches from it at once, as Kafka's
/// `maybeHandleCommonResponse` calls `transitionToFollower`. One that a newer
/// epoch with a leader fences follows that leader in that epoch. One that a
/// newer epoch without a leader fences becomes Unattached in it and keeps the
/// election deadline it drew.
#[yare::parameterized(
    leader_of_this_epoch = { Some(NodeId(3)), 0, Some(NodeId(3)), "Follower" },
    leader_of_a_newer_epoch = { Some(NodeId(3)), 4, Some(NodeId(3)), "Follower" },
    newer_epoch_without_a_leader = { None, 4, None, "Unattached" },
)]
#[test_macro(tokio::test)]
async fn a_nominee_acts_on_the_leader_a_vote_answer_names(
    leader_id: Option<NodeId>,
    epoch: Epoch,
    fetches_from: Option<NodeId>,
    role: &'static str,
) {
    let (mut engine, _dir) = build_engine_only().call();
    engine.on_event(Event::ElectionTimeout);
    let deadline = match engine.core.role() {
        Role::Prospective {
            election_deadline, ..
        } => *election_deadline,
        other => panic!("expected a prospective, found {other:?}"),
    };
    let mut sends = record_peer_sends(&mut engine, bytes::Bytes::new());

    engine.on_vote_response(NodeId(2), &answer(false, leader_id, epoch));

    let state = engine.core.quorum_state();
    assert2::assert!((state.leader_id, state.leader_epoch) == (leader_id, epoch));
    assert2::assert!(engine.core.role().name() == role);
    if let Some(peer) = fetches_from {
        let sent = recv_peer_send(&mut sends).await;
        assert2::assert!((sent.peer, sent.api_key) == (peer, api_key::FETCH));
    } else {
        assert2::assert!(
            *engine.core.role()
                == Role::Unattached {
                    election_deadline: deadline,
                }
        );
    }
}

/// A plain grant in the nominee's epoch is counted, so the pre-vote of a
/// three-voter quorum wins with it.
#[tokio::test]
async fn a_grant_in_the_nominee_epoch_is_counted() {
    let (mut engine, _dir) = build_engine_only().call();
    engine.on_event(Event::ElectionTimeout);

    engine.on_vote_response(NodeId(2), &answer(true, None, 0));

    assert2::assert!(matches!(engine.core.role(), Role::Candidate { .. }));
    assert2::assert!(engine.core.quorum_state().leader_epoch == 1);
}
