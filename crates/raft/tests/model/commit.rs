//! Durability settlement: the step that decides which pending client appends
//! have crossed their configured commit point and records their linearizability
//! returns. It is separate because it is the only place the model turns
//! replication progress into an observable acknowledgement.

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

/// Records `on_return` for the durable pending prefix. `KRaft` appends use the
/// replicated high watermark; diskless appends use the independent WAL-quorum
/// frontier. Looking only at the maximum HWM would acknowledge bytes held by a
/// single node and make minority WAL loss disappear from this model.
pub(super) fn settle_committed(state: &mut ModelState) {
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
