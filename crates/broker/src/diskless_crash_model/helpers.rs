use super::*;

pub(super) fn fsync_quorum(s: &mut CrashState) {
    let mut synced = 0;
    for node in 0..WAL_NODES {
        if !s.wal_lost[node] && synced < WAL_MAJORITY {
            s.wal_nodes[node] = s.log_end;
            synced += 1;
        }
    }
    s.wal_acked = quorum_frontier(s);
    s.advertised_hwm = s.wal_acked;
}

fn quorum_frontier(s: &CrashState) -> i64 {
    let live: Vec<i64> = s
        .wal_nodes
        .iter()
        .copied()
        .zip(s.wal_lost.iter().copied())
        .filter_map(|(offset, lost)| (!lost).then_some(offset))
        .collect();
    if live.len() < WAL_MAJORITY {
        return s.trimmed;
    }
    let mut live = live;
    live.sort_unstable();
    let leader_end = live.pop().unwrap_or(0);
    let followers = live;
    krabka_verified::consensus::majority_watermark(leader_end, &followers, WAL_MAJORITY, 0)
}

/// The controller's reservation for one appender's one-record batch: the
/// pending chain folded from the image frontier with
/// `wal_reservation_frontier`, then `reserve_offsets` from its end. Either
/// kernel refusing is a failure of the run, not a pruned step.
pub(super) fn reserve_via_controller(s: &CrashState) -> (i64, i64) {
    let pending_frontier = s
        .reservations
        .iter()
        .try_fold(0, |frontier, &(base, end)| {
            krabka_verified::offset_allocator::wal_reservation_frontier(frontier, base, end - base)
        })
        .expect("the controller's pending reservation chain stays exact");
    krabka_verified::offset_allocator::reserve_offsets(pending_frontier, 1)
        .expect("a bounded reservation is representable")
}

pub(super) fn surviving_wal_frontier(s: &CrashState) -> i64 {
    s.wal_nodes
        .iter()
        .copied()
        .zip(s.wal_lost.iter().copied())
        .filter_map(|(offset, lost)| (!lost).then_some(offset))
        .max()
        .unwrap_or(s.index_frontier)
        .max(s.index_frontier)
}
