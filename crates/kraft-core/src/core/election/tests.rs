use assert2::check;

use super::*;
use crate::{
    core::test_support::{FakeLog, machine},
    event::{Event, LogEnd},
};

/// Only a rejection from a *higher* epoch fences us.
///
/// Each piece of `!granted && epoch > ours` matters: a grant must never
/// fence, a rejection at our own epoch must not either -- that is the
/// ordinary "you lost the vote" reply -- and only a rejection carrying a
/// newer epoch means the cluster has moved past us. Stepping down clears
/// the vote we are holding, so whether the vote survives is the tell.
#[test]
fn only_a_rejection_from_a_higher_epoch_steps_us_down() {
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    // (what it is, granted, epoch offered, do we keep the vote we hold?)
    let cases = [
        ("a rejection at our own epoch", false, 3, true),
        ("a grant from a higher epoch", true, 9, true),
        ("a rejection from a higher epoch", false, 9, false),
    ];
    for (what, vote_granted, epoch, keeps_vote) in cases {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        // Cast a binding vote at epoch 3, so a step-down has something to
        // clear and "nothing happened" is distinguishable.
        m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                voter_id: NodeId(1),
                candidate_epoch: 3,
                candidate: NodeId(2),
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
            m.quorum_state().voted_key.is_some(),
            "{what}: setup should vote"
        );

        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch,
                vote_granted,
            },
            &log,
            SimInstant(0),
        );
        let kept = m.quorum_state().voted_key.is_some();
        check!(kept == keeps_vote, "{what}: vote kept = {kept}");
    }
}

#[test]
fn election_timeout_starts_prevote_prospective() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
    assert2::assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::SendVoteRequest { pre_vote: true, .. }))
    );
    check!(m.quorum_state().leader_epoch == 0); // pre-vote: epoch not bumped yet
}

#[test]
fn prevote_majority_promotes_to_candidate_and_bumps_epoch() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000)); // Prospective
    // 1 (self) + grant from 2 = majority of 3
    let actions = m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: 0,
            vote_granted: true,
        },
        &log,
        SimInstant(2001),
    );
    check!(
        (
            matches!(m.role(), Role::Candidate { .. }),
            m.quorum_state().leader_epoch,
            m.quorum_state().voted_key.map(|k| k.id),
        ) == (true, 1, Some(NodeId(1)))
    );
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::SendVoteRequest {
            pre_vote: false,
            epoch: 1
        }
    )));
}

#[test]
fn real_majority_promotes_to_leader_and_appends_leader_change() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: 0,
            vote_granted: true,
        },
        &log,
        SimInstant(2001),
    );
    let actions = m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: 1,
            vote_granted: true,
        },
        &log,
        SimInstant(2002),
    );
    check!(
        (
            m.role().is_leader(),
            m.quorum_state().leader_id,
            actions
                .iter()
                .any(|a| matches!(a, Action::AppendLeaderChange { epoch: 1 })),
            actions
                .iter()
                .any(|a| matches!(a, Action::SendBeginQuorumEpoch { epoch: 1 })),
        ) == (true, Some(NodeId(1)), true, true)
    );
}

#[test]
fn observer_never_starts_election() {
    let mut m = machine(NodeId(99), &[NodeId(1), NodeId(2), NodeId(3)]); // 99 is not a voter
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    assert2::assert!(matches!(m.role(), Role::Observer { .. }));
    assert2::assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::SendVoteRequest { .. }))
    );
}

#[test]
fn prospective_counts_grant_with_no_wire_prevote_signal() {
    // A JVM voter's `VoteResponse` carries no pre-vote flag. The candidate
    // must still count the grant as a PRE-VOTE because it is Prospective —
    // this is the KIP-996 interop fix (was dropped by the old echo-tag path).
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000)); // → Prospective, epoch 0
    assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
    let actions = m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: 0,
            vote_granted: true,
        },
        &log,
        SimInstant(2001),
    );
    // Pre-vote majority (self + 2) → promote to Candidate and bump the epoch.
    assert2::assert!(matches!(m.role(), Role::Candidate { .. }));
    check!(m.quorum_state().leader_epoch == 1);
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::SendVoteRequest {
            pre_vote: false,
            epoch: 1
        }
    )));
}

#[test]
fn stale_prevote_grant_ignored_after_promotion() {
    // A late pre-vote grant at the old epoch must not be miscounted toward
    // the real election once we have promoted to Candidate at epoch+1.
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 5,
        last_epoch: 1,
    };
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: 0,
            vote_granted: true,
        },
        &log,
        SimInstant(2001),
    ); // → Candidate @ epoch 1
    assert2::assert!(matches!(m.role(), Role::Candidate { .. }));
    // A duplicate/late pre-vote grant still tagged epoch 0 arrives.
    let actions = m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(3),
            epoch: 0,
            vote_granted: true,
        },
        &log,
        SimInstant(2002),
    );
    // Epoch guard (0 != 1) drops it: we stay Candidate, do NOT become leader.
    check!(
        (
            matches!(m.role(), Role::Candidate { .. }),
            m.role().is_leader(),
            actions.is_empty()
        ) == (true, false, true)
    );
    // The ignored stale grant must not have entered the real-vote tally:
    // after promotion the Candidate's grant set holds only our self-vote.
    if let Role::Candidate { granted, .. } = m.role() {
        assert2::assert!((granted.len(), granted.contains(&NodeId(3))) == (1, false));
    } else {
        panic!("expected Candidate");
    }
}
