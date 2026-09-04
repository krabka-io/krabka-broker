//! Durability settlement: the step that decides which pending client appends
//! have crossed their configured commit point and records their linearizability
//! returns. It is separate because it is the only place the model turns
//! replication progress into an observable acknowledgement.

use krabka_raft::kraft::types::Epoch;
use stateright::semantics::ConsistencyTester;

use super::{
    spec::LogRet,
    state::{CommitPoint, ModelState, node_high_watermark},
};

pub(super) fn wal_quorum_frontier(state: &ModelState) -> i64 {
    let mut frontiers: Vec<i64> = state.wal_frontiers.values().copied().collect();
    frontiers.sort_unstable_by(|left, right| right.cmp(left));
    let majority = frontiers.len() / 2 + 1;
    frontiers.get(majority - 1).copied().unwrap_or(0)
}

/// Extends [`ModelState::committed_epochs`] over every raft offset the cluster
/// has newly committed, stamping each with the epoch it was written in.
///
/// The offsets are read off the node holding the highest high watermark,
/// because that node is by construction one that both holds the entries and has
/// seen them commit. The record is append-only: once an offset is committed its
/// epoch is fixed forever, which is exactly what a later leader must reproduce.
fn record_committed_epochs(state: &mut ModelState) {
    let Some(node) = state.nodes.values().max_by_key(|n| node_high_watermark(n)) else {
        return;
    };
    let high_watermark = node_high_watermark(node);
    let known = i64::try_from(state.committed_epochs.len()).expect("committed prefix fits in i64");
    let fresh: Vec<Epoch> = (known..high_watermark)
        .map_while(|offset| node.log.epoch_at(offset))
        .collect();
    state.committed_epochs.extend(fresh);
}

/// Records `on_return` for the durable pending prefix. `KRaft` appends use the
/// replicated high watermark; diskless appends use the independent WAL-quorum
/// frontier. Looking only at the maximum HWM would acknowledge bytes held by a
/// single node and make minority WAL loss disappear from this model.
pub(super) fn settle_committed(state: &mut ModelState) {
    record_committed_epochs(state);
    let max_hwm = state
        .nodes
        .values()
        .map(node_high_watermark)
        .max()
        .unwrap_or(0);
    let wal_frontier = wal_quorum_frontier(state);
    let mut ready = Vec::new();
    for (&offset, (_, _, commit_point)) in &state.pending {
        let durable = match commit_point {
            CommitPoint::KRaftHighWatermark => offset < max_hwm,
            CommitPoint::WalQuorumDurable => offset < wal_frontier,
        };
        if !durable {
            break;
        }
        ready.push(offset);
    }
    for off in ready {
        let (client, value, _) = state.pending.remove(&off).expect("pending entry exists");
        state.committed.push(value);
        let _ = state
            .linz
            .on_return(client, LogRet::Committed(state.committed.clone()))
            .expect("matching invoke recorded");
    }
}
