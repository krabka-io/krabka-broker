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
