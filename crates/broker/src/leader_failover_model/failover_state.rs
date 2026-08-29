//! The bounded configuration of the failover-scan model, the state its search
//! enumerates, the actions that move between two states, and the projection
//! onto the `PartitionRecord` that the real `failover_one` reads.
//!
//! Order is significant twice over: `isr[0]` is what a clean election picks,
//! and the replica order is what the KIP-841 out-of-ISR pick walks, so both
//! stay `Vec` rather than a set.

use std::collections::{BTreeSet, HashSet};

use krabka_metadata::PartitionRecord;
use krabka_raft::NodeId;

use crate::config_keys::RecoveryStrategy;

/// Bounded config for the failover-scan model.
pub(super) struct FailoverModel {
    pub(super) replicas: Vec<NodeId>, // replicas[0] is the fixed initial leader
    /// Data-bearing witnesses among `replicas`. A witness stays in the ISR and
    /// counts toward min-ISR, and it never leads. `replicas[0]` must not be a
    /// witness, because it is the initial leader.
    pub(super) witnesses: HashSet<NodeId>,
    pub(super) strategy: RecoveryStrategy,
    pub(super) unclean_enabled: bool,
    pub(super) max_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct FailoverState {
    pub(super) leader: NodeId,
    pub(super) isr: Vec<NodeId>, // order significant (clean election picks isr.first())
    pub(super) replicas: Vec<NodeId>, // fixed; order significant (KIP-841 picks replicas order)
    pub(super) leader_epoch: i32,
    pub(super) alive: BTreeSet<NodeId>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum FailoverAction {
    Die(NodeId),
    Revive(NodeId),
    Failover(NodeId),
}

impl FailoverModel {
    /// `witness_ids` names the replicas that carry the witness role.
    pub(super) fn config(
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
        witness_ids: &[u64],
    ) -> Self {
        Self {
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            witnesses: witness_ids
                .iter()
                .copied()
                .map(krabka_audit::NodeId)
                .collect(),
            strategy,
            unclean_enabled,
            max_epoch: 6,
        }
    }
}

/// Build a minimal `PartitionRecord` from the model state to drive the real
/// `failover_one`. This function fills the fields `failover_one` ignores with
/// dummy values.
pub(super) fn pr_of(s: &FailoverState) -> PartitionRecord {
    PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: s.leader,
        replicas: s.replicas.clone(),
        isr: s.isr.clone(),
        leader_epoch: krabka_metadata::LeaderEpoch(s.leader_epoch),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }
}
