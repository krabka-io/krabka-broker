//! The bounded configuration of the KIP-966 winner-selection model, the
//! gathered responses its search enumerates, and the projection onto the real
//! `ReplicaLogInfo`.
//!
//! The model state has to be hashable for the search, and `ReplicaLogInfo` is
//! not, so the reported log state is mirrored here and converted on the way
//! into the production selectors.

use std::collections::BTreeMap;

use krabka_raft::NodeId;

use crate::unclean_recovery::ReplicaLogInfo;

/// One replica's reported log state. This is a hashable mirror of
/// `ReplicaLogInfo`, which isn't `Hash`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct ReplicaLog {
    pub(super) last_written_leader_epoch: i32,
    pub(super) log_end_offset: i64,
    pub(super) current_leader_epoch: i32,
}

/// Bounded config for the KIP-966 winner-selection model.
pub(super) struct RecoveryModel {
    pub(super) replicas: Vec<NodeId>,
    pub(super) max_epoch: i32,
    pub(super) max_leo: i64,
    pub(super) known_leader_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct RecoveryState {
    pub(super) responses: BTreeMap<NodeId, ReplicaLog>,
    pub(super) known_leader_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum RecoveryAction {
    AddResponse {
        node: NodeId,
        last_written_epoch: i32,
        leo: i64,
        current_epoch: i32,
    },
}

impl RecoveryModel {
    pub(super) fn offset_recovery() -> Self {
        Self {
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            max_epoch: 2,
            max_leo: 2,
            known_leader_epoch: 1,
        }
    }
}

/// Project the gathered responses into the real wire-decoupled type.
pub(super) fn infos_of(s: &RecoveryState) -> Vec<ReplicaLogInfo> {
    s.responses
        .iter()
        .map(|(id, l)| ReplicaLogInfo {
            broker_id: *id,
            last_written_leader_epoch: l.last_written_leader_epoch,
            log_end_offset: l.log_end_offset,
            current_leader_epoch: l.current_leader_epoch,
        })
        .collect()
}
