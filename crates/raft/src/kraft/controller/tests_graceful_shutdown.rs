//! Tests for how a leading engine stops: a graceful shutdown resigns and waits
//! for a successor, as Kafka's `KafkaRaftClient.shutdown` does, and a halt
//! hands nothing over.

use {
    super::*,
    crate::kraft::controller::{
        graceful_shutdown::graceful_shutdown_window,
        test_support::{
            CapturedPeerSend, await_leader, elect_leader_with_helper, spawn_recording_controller,
        },
    },
    assert2::check,
    krabka_units::prelude::secs,
    std::time::{Duration as StdDuration, Instant as StdInstant},
};

/// How a test stops the leader.
#[derive(Clone, Copy, Debug)]
enum Stop {
    /// A graceful shutdown, after which voter 2 announces itself the leader
    /// of the next epoch.
    GracefulWithSuccessor,
    /// A graceful shutdown that no successor answers.
    GracefulAlone,
    /// A halt.
    Halt,
}

/// Every `EndQuorumEpoch` the controller has sent so far, decoded, by peer.
fn end_quorum_epochs(
    sends: &mut tokio::sync::mpsc::UnboundedReceiver<CapturedPeerSend>,
) -> Vec<(NodeId, Option<wire::PeerRequest>)> {
    let mut ends = Vec::new();
    while let Ok(send) = sends.try_recv() {
        if send.api_key == api_key::END_QUORUM_EPOCH {
            ends.push((send.peer, wire::decode_end(&send.body)));
        }
    }
    ends.sort_unstable_by_key(|(peer, _)| *peer);
    ends
}

/// A graceful shutdown of a leader resigns: it sends `EndQuorumEpoch` to every
/// other voter, naming them as preferred successors, and keeps the engine
/// running until a successor announces itself, or until the graceful-shutdown
/// window runs out. A halt sends nothing and stops at once.
#[yare::parameterized(
    graceful_with_successor = { Stop::GracefulWithSuccessor, true, false },
    graceful_alone = { Stop::GracefulAlone, true, true },
    halt = { Stop::Halt, false, false },
)]
#[test_macro(tokio::test)]
async fn a_stopping_leader(stop: Stop, resigns: bool, waits_out_the_window: bool) {
    let timeouts = QuorumTimeouts {
        election: secs(5),
        fetch: secs(5),
    };
    let window = graceful_shutdown_window(timeouts.fetch);
    let (ctrl, _dir, mut sends) =
        spawn_recording_controller(timeouts, wire::PeerResponse::Ack { epoch: 1 }.encode());
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;
    let _ = end_quorum_epochs(&mut sends);

    let started = StdInstant::now();
    let stopping = ctrl.clone();
    let stopped = tokio::spawn(async move {
        match stop {
            Stop::Halt => stopping.halt().await,
            Stop::GracefulWithSuccessor | Stop::GracefulAlone => stopping.shutdown().await,
        }
    });
    if matches!(stop, Stop::GracefulWithSuccessor) {
        await_leader(&ctrl, None).await;
        ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(2),
            leader_epoch: 2,
        })
        .await
        .expect("the resigned engine is still running");
    }
    tokio::time::timeout(window + StdDuration::from_secs(10), stopped)
        .await
        .expect("the engine stops")
        .expect("the stop task");
    let took = started.elapsed();

    let resignation = |peer: NodeId| {
        (
            peer,
            Some(wire::PeerRequest::EndQuorumEpoch {
                cluster_id: Some(uuid::Uuid::nil()),
                leader_id: NodeId(1),
                leader_epoch: 1,
                preferred_candidates: [NodeId(2), NodeId(3)]
                    .map(|id| wire::PreferredCandidate {
                        id,
                        directory_id: uuid::Uuid::nil(),
                    })
                    .to_vec(),
            }),
        )
    };
    let expected = if resigns {
        vec![resignation(NodeId(2)), resignation(NodeId(3))]
    } else {
        Vec::new()
    };
    check!(end_quorum_epochs(&mut sends) == expected);
    check!(
        (took >= window) == waits_out_the_window,
        "took {took:?} against {window:?}"
    );
}

/// A follower has no leadership to hand over, so its graceful shutdown stops
/// at once, as Kafka's `pollFollowerAsVoter` does.
#[tokio::test]
async fn a_following_engine_stops_at_once() {
    let timeouts = QuorumTimeouts {
        election: secs(5),
        fetch: secs(5),
    };
    let (ctrl, _dir, mut sends) =
        spawn_recording_controller(timeouts, wire::PeerResponse::Ack { epoch: 1 }.encode());
    ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 1,
    })
    .await
    .expect("the engine is running");
    await_leader(&ctrl, Some(NodeId(2))).await;

    let started = StdInstant::now();
    ctrl.shutdown().await;

    check!(end_quorum_epochs(&mut sends) == Vec::new());
    check!(started.elapsed() < graceful_shutdown_window(timeouts.fetch));
}
