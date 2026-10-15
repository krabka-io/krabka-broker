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

/// Kafka's `FENCED_LEADER_EPOCH`.
const FENCED_LEADER_EPOCH: i16 = 74;
/// Kafka's `INVALID_VOTER_KEY`.
const INVALID_VOTER_KEY: i16 = 125;

/// Node 1 asks at epoch 5.
const ASKER: VoteAsker = VoteAsker {
    me: NodeId(1),
    epoch: 5,
    known_leader: None,
};

/// Each answer is read as Kafka's `maybeHandleCommonResponse` and then
/// `handleVoteResponse` read it.
#[yare::parameterized(
    older_epoch = { ASKER, 0, false, Some(NodeId(2)), 4, VoteResponseStep::Ignore },
    unknown_leader_epoch = { ASKER, UNKNOWN_LEADER_EPOCH, false, None, 5, VoteResponseStep::Ignore },
    newer_epoch_with_a_leader = {
        ASKER, 0, false, Some(NodeId(3)), 6, VoteResponseStep::Follow { leader_id: NodeId(3), epoch: 6 }
    },
    newer_epoch_fenced_with_a_leader = {
        ASKER, FENCED_LEADER_EPOCH, false, Some(NodeId(3)), 6,
        VoteResponseStep::Follow { leader_id: NodeId(3), epoch: 6 }
    },
    newer_epoch_without_a_leader = {
        ASKER, 0, false, None, 6, VoteResponseStep::Unattached { epoch: 6 }
    },
    newer_epoch_naming_the_asker = { ASKER, 0, false, Some(NodeId(1)), 6, VoteResponseStep::Ignore },
    same_epoch_leader_while_none_is_known = {
        ASKER, 0, false, Some(NodeId(3)), 5, VoteResponseStep::Follow { leader_id: NodeId(3), epoch: 5 }
    },
    same_epoch_leader_with_not_leader = {
        ASKER, NOT_LEADER_OR_FOLLOWER, false, Some(NodeId(3)), 5,
        VoteResponseStep::Follow { leader_id: NodeId(3), epoch: 5 }
    },
    same_epoch_leader_while_one_is_known = {
        VoteAsker { known_leader: Some(NodeId(2)), ..ASKER }, 0, false, Some(NodeId(3)), 5,
        VoteResponseStep::Count { epoch: 5, granted: false }
    },
    same_epoch_grant = { ASKER, 0, true, None, 5, VoteResponseStep::Count { epoch: 5, granted: true } },
    same_epoch_refusal = { ASKER, 0, false, None, 5, VoteResponseStep::Count { epoch: 5, granted: false } },
    same_epoch_error = { ASKER, INVALID_VOTER_KEY, false, None, 5, VoteResponseStep::Ignore },
)]
fn a_vote_answer_is_read_as_kafka_reads_it(
    asker: VoteAsker,
    error_code: i16,
    granted: bool,
    leader_id: Option<NodeId>,
    epoch: Epoch,
    expected: VoteResponseStep,
) {
    let answer = VoteAnswer {
        error_code,
        granted,
        leader_id,
        epoch,
    };
    assert2::assert!(vote_response_step(asker, answer) == expected);
}

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
