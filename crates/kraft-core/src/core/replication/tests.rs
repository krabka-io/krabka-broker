use super::*;
use crate::{
    core::test_support::{CellLog, FakeLog, machine},
    event::Event,
};

#[test]
fn leader_advances_hwm_at_majority_fetch_offset() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    // The log end is 0 at promotion so the leader's `epoch_start_offset` is
    // 0; the leader-completeness gate then permits advancing the HWM to any
    // majority offset > 0. After promotion the log grows to end 10, which is
    // what followers replicate against.
    let log = CellLog {
        end: std::cell::Cell::new(0),
        last_epoch: 0,
    };
    // drive to leader (epoch_start_offset captured as end_offset() == 0)
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
    m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: 1,
            vote_granted: true,
        },
        &log,
        SimInstant(2002),
    );
    assert2::assert!(matches!(
        m.role(),
        Role::Leader {
            epoch_start_offset: 0,
            ..
        }
    ));
    // Leader's log now ends at 10. follower 2 fetches at 8, follower 3 at 4.
    log.end.set(10);
    let a2 = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 8,
        },
        &log,
        SimInstant(2100),
    );
    // majority of {self=10, 2=8} = 8, and 8 > epoch_start_offset 0 → advances
    assert2::assert!(
        a2.iter()
            .any(|a| matches!(a, Action::AdvanceHighWatermark(8)))
    );
    let _ = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(3),
            fetch_epoch: 1,
            fetch_offset: 4,
        },
        &log,
        SimInstant(2101),
    );
    // sorted match offsets {10,8,4}; majority (2nd highest) = 8 → no regress
    if let Role::Leader { high_watermark, .. } = m.role() {
        assert2::assert!(*high_watermark == 8);
    } else {
        panic!()
    }
}

#[test]
fn leader_hwm_does_not_regress_on_reordered_stale_fetch() {
    // A follower's recorded `fetch_offset` can fall (a reordered/stale fetch,
    // or a legitimate post-truncation re-fetch), dropping the `majority()`-th
    // match offset below the current HWM. `recompute_high_watermark` must
    // clamp to the existing HWM (never regress) rather than return a lower
    // value — historically a lower return tripped a debug-only assertion.
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = CellLog {
        end: std::cell::Cell::new(0),
        last_epoch: 0,
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
    m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: 1,
            vote_granted: true,
        },
        &log,
        SimInstant(2002),
    );
    // Leader log ends at 10; follower 2 fetches at 8 → HWM advances to 8.
    log.end.set(10);
    m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 8,
        },
        &log,
        SimInstant(2100),
    );
    if let Role::Leader { high_watermark, .. } = m.role() {
        assert2::assert!(*high_watermark == 8);
    } else {
        panic!("expected leader")
    }
    // Now follower 2 sends a STALE lower fetch (offset 2 < its prior 8).
    // match offsets {self=10, 2=2, 3=0} → majority (2nd highest) = 2, which is
    // below the current HWM 8. The HWM must hold at 8, and no spurious
    // AdvanceHighWatermark may be emitted. (Pre-clamp this panicked.)
    let a = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 2,
        },
        &log,
        SimInstant(2101),
    );
    assert2::assert!(
        !a.iter()
            .any(|x| matches!(x, Action::AdvanceHighWatermark(_)))
    );
    if let Role::Leader { high_watermark, .. } = m.role() {
        assert2::assert!(*high_watermark == 8);
    } else {
        panic!("expected leader")
    }
}

#[test]
fn leader_holds_hwm_for_prior_epoch_entries_until_current_epoch_committed() {
    // Leader-completeness (Raft Fig.8): a leader promoted at log end 10
    // (epoch_start_offset = 10) must NOT advance the HWM to a majority
    // offset that only covers prior-epoch entries (8 < 10).
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 10,
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
    m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: 1,
            vote_granted: true,
        },
        &log,
        SimInstant(2002),
    );
    assert2::assert!(matches!(
        m.role(),
        Role::Leader {
            epoch_start_offset: 10,
            ..
        }
    ));
    // follower 2 fetches at 8: majority of {10, 8} = 8, but 8 <= 10 → hold.
    let a2 = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 8,
        },
        &log,
        SimInstant(2100),
    );
    assert2::assert!(
        !a2.iter()
            .any(|a| matches!(a, Action::AdvanceHighWatermark(_)))
    );
    if let Role::Leader { high_watermark, .. } = m.role() {
        assert2::assert!(*high_watermark == 0);
    } else {
        panic!()
    }
}

#[test]
fn leader_detects_divergence_and_returns_truncate() {
    // log has last_epoch 2 ending at 10; epoch-1 ended at 5.
    struct L;
    impl LogView for L {
        fn end_offset(&self) -> i64 {
            10
        }
        fn last_epoch(&self) -> Epoch {
            2
        }
        fn end_offset_for_epoch(&self, e: Epoch) -> Option<i64> {
            match e {
                0 => Some(0),
                1 => Some(5),
                2 => Some(10),
                _ => None,
            }
        }
    }
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = L;
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
    m.on_event(
        Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: 1,
            vote_granted: true,
        },
        &log,
        SimInstant(2002),
    );
    // follower claims it fetched epoch 1 at offset 8, but epoch 1 ended at 5 → diverged.
    let actions = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 8,
        },
        &log,
        SimInstant(2100),
    );
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::TruncateTo(LogOffsetMetadata {
            offset: 5,
            epoch: 1
        })
    )));
}

#[test]
fn follower_truncates_on_diverging_fetch_response() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 10,
        last_epoch: 2,
    };
    m.on_event(
        Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(2),
            leader_epoch: 3,
        },
        &log,
        SimInstant(10),
    );
    let actions = m.on_event(
        Event::ReceiveFetchResponse {
            leader_id: NodeId(2),
            leader_epoch: 3,
            diverging: Some(LogOffsetMetadata {
                offset: 5,
                epoch: 1,
            }),
        },
        &log,
        SimInstant(11),
    );
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::TruncateTo(LogOffsetMetadata {
            offset: 5,
            epoch: 1
        })
    )));
}

/// Drives `m` from `Unattached` to `Role::Leader`, returning every action the
/// pre-vote round, the vote round and the promotion emitted.
///
/// `peers` is the rest of the voter set. Grants past the majority are ignored
/// by the core's own epoch/role guards, so passing all of them is safe.
fn win_election(
    m: &mut QuorumStateMachine,
    log: &dyn LogView,
    peers: &[NodeId],
    now: SimInstant,
) -> Vec<Action> {
    let mut actions = m.on_event(Event::ElectionTimeout, log, now);
    // Pre-vote grants at the pre-bump epoch, then real-vote grants at the epoch
    // the successful pre-vote bumped us to.
    for epoch in [0, 1] {
        for &from in peers {
            actions.extend(m.on_event(
                Event::ReceiveVoteResponse {
                    from,
                    epoch,
                    vote_granted: true,
                },
                log,
                now,
            ));
        }
    }
    actions
}

fn armed_check_quorum(actions: &[Action]) -> Option<SimInstant> {
    actions.iter().find_map(|a| match a {
        Action::ResetTimer {
            kind: TimerKind::CheckQuorum,
            deadline,
        } => Some(*deadline),
        _ => None,
    })
}

/// A new leader arms the check-quorum window at 1.5x the fetch timeout, which
/// is Kafka's `CHECK_QUORUM_TIMEOUT_FACTOR` over the same configured extent the
/// follower fetch deadline uses. A lone voter has nobody to hear from, so it
/// arms nothing at all -- Kafka's `timeUntilCheckQuorumExpires` reports
/// `Long.MAX_VALUE` for a single-voter quorum.
#[test]
fn promotion_arms_the_check_quorum_window_unless_the_leader_is_alone() {
    // `TEST_ELECTION_TIMEOUT` is one second, so the window is 1500ms.
    for (name, voter_ids, want) in [
        (
            "three voters",
            &[NodeId(1), NodeId(2), NodeId(3)][..],
            Some(SimInstant(3500)),
        ),
        (
            "two voters",
            &[NodeId(1), NodeId(2)][..],
            Some(SimInstant(3500)),
        ),
        ("sole voter", &[NodeId(1)][..], None),
    ] {
        let mut m = machine(NodeId(1), voter_ids);
        let log = FakeLog {
            end: 0,
            last_epoch: 0,
        };
        let peers: Vec<NodeId> = voter_ids
            .iter()
            .copied()
            .filter(|&id| id != NodeId(1))
            .collect();
        let actions = win_election(&mut m, &log, &peers, SimInstant(2000));
        assert2::assert!(m.role().is_leader(), "case {name}");
        assert2::check!(armed_check_quorum(&actions) == want, "case {name}");
    }
}

/// A fetch re-arms the window only once the leader has heard from the majority
/// it needs *besides itself*: one follower of three voters, two of five. Every
/// fetch below that count leaves the window running, so a leader that only ever
/// hears from a minority still reaches its deadline and resigns.
#[test]
fn only_a_majority_of_followers_re_arms_the_check_quorum_window() {
    for (name, voter_ids, fetchers, want) in [
        (
            "three voters, one follower is the majority",
            &[NodeId(1), NodeId(2), NodeId(3)][..],
            &[NodeId(2)][..],
            vec![Some(SimInstant(3600))],
        ),
        (
            "five voters, one follower is short",
            &[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)][..],
            &[NodeId(2)][..],
            vec![None],
        ),
        (
            "five voters, two followers reach it",
            &[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)][..],
            &[NodeId(2), NodeId(3)][..],
            vec![None, Some(SimInstant(3600))],
        ),
        (
            "a repeat fetch from one follower is not two voters",
            &[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)][..],
            &[NodeId(2), NodeId(2)][..],
            vec![None, None],
        ),
        (
            "a non-voter observer never counts",
            &[NodeId(1), NodeId(2), NodeId(3)][..],
            &[NodeId(9)][..],
            vec![None],
        ),
    ] {
        let mut m = machine(NodeId(1), voter_ids);
        let log = FakeLog {
            end: 0,
            last_epoch: 0,
        };
        let peers: Vec<NodeId> = voter_ids
            .iter()
            .copied()
            .filter(|&id| id != NodeId(1))
            .collect();
        win_election(&mut m, &log, &peers, SimInstant(2000));
        let got: Vec<Option<SimInstant>> = fetchers
            .iter()
            .map(|&from| {
                armed_check_quorum(&m.on_event(
                    Event::ReceiveFetch {
                        from,
                        fetch_epoch: 1,
                        fetch_offset: 0,
                    },
                    &log,
                    SimInstant(2100),
                ))
            })
            .collect();
        assert2::check!(got == want, "case {name}");
    }
}

/// A `FetchSnapshot` is contact from that voter even though it moves no
/// replication progress, so Kafka scores it for check-quorum exactly as it
/// scores a Fetch. Without this a leader whose only reachable follower is
/// mid-snapshot resigns under a perfectly healthy quorum.
#[test]
fn a_snapshot_fetch_is_check_quorum_contact() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 0,
        last_epoch: 0,
    };
    win_election(&mut m, &log, &[NodeId(2), NodeId(3)], SimInstant(2000));
    let actions = m.on_event(
        Event::ReceiveFetchSnapshot { from: NodeId(2) },
        &log,
        SimInstant(2100),
    );
    assert2::assert!(
        actions
            == vec![Action::ResetTimer {
                kind: TimerKind::CheckQuorum,
                deadline: SimInstant(3600),
            }]
    );
}

/// A fetch that the leader answers with a truncation hint is still proof the
/// follower is talking to us, so it re-arms the window as well. Resigning the
/// quorum over a truncation round would cost a whole election for nothing.
#[test]
fn a_diverging_fetch_still_counts_as_contact() {
    struct EpochOneLog;
    impl LogView for EpochOneLog {
        fn end_offset(&self) -> i64 {
            5
        }
        fn last_epoch(&self) -> Epoch {
            1
        }
        fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
            (epoch <= 1).then_some(5)
        }
    }
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = EpochOneLog;
    win_election(&mut m, &log, &[NodeId(2), NodeId(3)], SimInstant(2000));
    // Follower 2 claims epoch 1 out to offset 9; our epoch 1 ends at 5.
    let actions = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 9,
        },
        &log,
        SimInstant(2100),
    );
    assert2::assert!(
        actions
            == vec![
                Action::ResetTimer {
                    kind: TimerKind::CheckQuorum,
                    deadline: SimInstant(3600),
                },
                Action::TruncateTo(LogOffsetMetadata {
                    offset: 5,
                    epoch: 1,
                }),
            ]
    );
}

/// The window expiring is the leader losing the quorum: it drops the
/// leadership, tells the voters to elect, persists the change, and arms the
/// election timer that carries it into its next pre-vote round.
///
/// Clearing `leader_id` is the point of the whole mechanism. An isolated old
/// leader that keeps naming itself answers `DescribeQuorum`, Metadata and
/// `BrokerHeartbeat` as the controller leader for an epoch the rest of the
/// cluster has already replaced.
#[test]
fn check_quorum_expiry_resigns_the_leader() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 0,
        last_epoch: 0,
    };
    win_election(&mut m, &log, &[NodeId(2), NodeId(3)], SimInstant(2000));
    assert2::assert!(m.quorum_state().leader_id == Some(NodeId(1)));

    let actions = m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(3500));
    assert2::assert!(
        actions
            == vec![
                Action::SendEndQuorumEpoch { epoch: 1 },
                Action::PersistQuorumState,
                Action::TransitionedTo("Resigned"),
                Action::ResetTimer {
                    kind: TimerKind::Election,
                    deadline: SimInstant(
                        3500 + 1000 + crate::core::election_jitter_ms(NodeId(1), 1, 1000)
                    ),
                },
            ]
    );
    assert2::assert!(
        (
            m.role().clone(),
            m.quorum_state().leader_id,
            m.quorum_state().leader_epoch
        ) == (Role::Resigned, None, 1)
    );
}

/// A resigned replica is not stuck: its election timer starts the ordinary
/// KIP-996 pre-vote round, which is how the surviving side of a healed
/// partition can hand it the leadership back.
#[test]
fn a_resigned_replica_elects_on_its_election_timer() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 0,
        last_epoch: 0,
    };
    win_election(&mut m, &log, &[NodeId(2), NodeId(3)], SimInstant(2000));
    m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(3500));
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(5000));
    assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::SendVoteRequest {
            epoch: 1,
            pre_vote: true
        }
    )));
}

/// A sole voter has no quorum to lose, so the expiry is inert for it. Nothing
/// arms the timer there, but a stray tick must not depose the only replica that
/// can serve the cluster.
#[test]
fn a_sole_voter_never_resigns_on_check_quorum() {
    let mut m = machine(NodeId(1), &[NodeId(1)]);
    let log = FakeLog {
        end: 0,
        last_epoch: 0,
    };
    win_election(&mut m, &log, &[], SimInstant(2000));
    let actions = m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(9000));
    assert2::assert!((actions, m.role().is_leader()) == (Vec::new(), true));
}

/// Only a leader owns a check-quorum window. A follower that somehow sees the
/// tick keeps following its leader rather than resigning a leadership it does
/// not hold.
#[test]
fn a_follower_ignores_a_check_quorum_tick() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog {
        end: 0,
        last_epoch: 0,
    };
    m.on_event(
        Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(2),
            leader_epoch: 4,
        },
        &log,
        SimInstant(10),
    );
    let actions = m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(9000));
    assert2::assert!((actions, m.quorum_state().leader_id) == (Vec::new(), Some(NodeId(2))));
}

/// A leader that was alone arms nothing at promotion, so the voter record that
/// grows the cluster past one voter is what must start its first window.
/// Without that, a leader promoted as the sole voter would never run
/// check-quorum again for the rest of its epoch.
#[test]
fn growing_past_one_voter_starts_the_leader_a_window() {
    let mut m = machine(NodeId(1), &[NodeId(1)]);
    let log = FakeLog {
        end: 0,
        last_epoch: 0,
    };
    let promotion = win_election(&mut m, &log, &[], SimInstant(2000));
    assert2::assert!(armed_check_quorum(&promotion) == None);

    let actions = m.apply_voter_set(
        crate::core::test_support::voters(&[NodeId(1), NodeId(2)]),
        SimInstant(4000),
    );
    assert2::assert!(
        actions
            == vec![Action::ResetTimer {
                kind: TimerKind::CheckQuorum,
                deadline: SimInstant(5500),
            }]
    );
}
