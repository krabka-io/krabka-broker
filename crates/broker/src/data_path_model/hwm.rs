//! The seam onto the real high-watermark core: it rebuilds a `ReplicaState`
//! from the model state and asks the production code where the watermark
//! belongs.
//!
//! Both release paths live here, because they answer the same question from
//! different durability evidence. The replicated path takes the minimum
//! in-sync-replica log end offset, and the diskless path takes the fsynced
//! write-ahead-log prefix. `consistent_leo` sits with them: it is the reason
//! the replicated answer is sound, since a follower that has not reconciled a
//! divergence must not count toward the watermark.

use std::time::Instant;

use krabka_log::Offset;

use super::{
    bounds::{NB_U8, has, node},
    state::DpState,
};
use crate::replica_state::ReplicaState;

/// The follower's effective LEO *as seen by the leader*. It is the length of
/// the longest epoch-consistent common prefix with the leader's log. A real
/// follower truncates any divergence with `OffsetForLeaderEpoch` BEFORE it
/// advances its reported fetch offset, so the leader never sees divergent
/// follower data and never advances the HWM over it. A raw `len()` here would
/// let the HWM commit data that a divergent follower has not reconciled. That
/// is the bug this composition surfaced.
fn consistent_leo(follower_log: &[u8], leader_log: &[u8]) -> i64 {
    follower_log
        .iter()
        .zip(leader_log.iter())
        .take_while(|(f, l)| f == l)
        .count()
        .try_into()
        .expect("bounded model offset fits in i64")
}

/// Drive the REAL HWM core. It reconstructs a `ReplicaState` from the model's
/// ISR and the consistent per-follower LEOs, then returns the recomputed HWM.
/// That HWM is the minimum ISR LEO, clamped to the leader LEO.
pub(super) fn real_hwm(s: &DpState, base: Instant) -> i64 {
    let leader = s.leader;
    let leader_leo = s.leader_leo();
    let leader_log = &s.log[usize::from(leader)];
    let isr_nodes: Vec<krabka_audit::NodeId> = (0..NB_U8)
        .filter(|&b| has(s.isr, b))
        .map(|b| krabka_audit::NodeId(node(b)))
        .collect();
    let replica_nodes: Vec<krabka_audit::NodeId> =
        (0..NB_U8).map(|b| krabka_audit::NodeId(node(b))).collect();
    let mut rs = ReplicaState::new();
    rs.install_isr(
        &isr_nodes,
        &replica_nodes,
        krabka_audit::NodeId(node(leader)),
        base,
    );
    for b in 0..NB_U8 {
        if b != leader && has(s.isr, b) {
            let leo = consistent_leo(&s.log[usize::from(b)], leader_log);
            // Wrap this model's `i64` LEOs into `Offset` for the real HWM core.
            rs.update_follower_leo(
                krabka_audit::NodeId(node(b)),
                Offset(leo),
                Offset(leader_leo),
                base,
            );
        }
    }
    // Unwrap the recomputed `Offset` HWM back into this model's `i64` world.
    rs.recompute_hw_for_leader_append(Offset(leader_leo)).0
}

/// Drive the REAL diskless WAL durable-HW core. Slice 1 uses local fsync only,
/// so the model constrains the ISR to the leader broker and releases exactly
/// the durable WAL prefix.
pub(super) fn real_wal_hwm(leader: u8, durable_leo: i64, base: Instant) -> i64 {
    let leader = krabka_audit::NodeId(node(leader));
    let mut rs = ReplicaState::new();
    rs.install_isr(&[leader], &[leader], leader, base);
    rs.recompute_hw_for_wal_durable(Offset(durable_leo)).0
}
