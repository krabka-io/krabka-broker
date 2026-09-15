//! Which `__transaction_state` partitions this broker leads.
//!
//! Many tasks refresh the leadership from the metadata image that each one
//! read: the reconcile loop, every transaction handler, the completion task
//! and the expiry sweeps. A task can apply its image after another task has
//! applied a newer one. Kafka orders coordinator elections and resignations by
//! the coordinator epoch, which is the partition leader epoch
//! (`TransactionStateManager.loadTransactionsForTxnTopicPartition` and
//! `removeTransactionsForTxnTopicPartition`). This module does the same: an
//! image changes the leadership of a partition only when its leader epoch is
//! higher than the epoch that the coordinator already holds.

use std::collections::HashMap;

use krabka_ids::PartitionIndex;
use krabka_metadata::{LeaderEpoch, MetadataImage, NodeId};

use crate::txn::bootstrap;

/// The leadership of one `__transaction_state` partition, from the newest
/// leader epoch that the coordinator has seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StatePartitionLeadership {
    /// The partition leader epoch of the image that set this value.
    pub(super) leader_epoch: LeaderEpoch,
    /// Whether this broker is the leader at `leader_epoch`.
    pub(super) leads: bool,
}

/// Map from `__transaction_state` partition to its newest known leadership.
pub(super) type StatePartitionLeaders = HashMap<PartitionIndex, StatePartitionLeadership>;

/// Applies the `__transaction_state` leadership of `image` to `known`.
///
/// A partition changes only when `image` gives it a leader epoch higher than
/// the one in `known`. A partition that `image` does not hold keeps its value,
/// because a stale image can come from before the topic was created.
pub(super) fn apply_image(
    known: &mut StatePartitionLeaders,
    node_id: NodeId,
    image: &MetadataImage,
) {
    for partition in image.partitions_of(bootstrap::TOPIC) {
        let index = PartitionIndex(partition.partition);
        let newer = known
            .get(&index)
            .is_none_or(|current| partition.leader_epoch > current.leader_epoch);
        if newer {
            known.insert(
                index,
                StatePartitionLeadership {
                    leader_epoch: partition.leader_epoch,
                    leads: partition.leader == node_id,
                },
            );
        }
    }
}

/// Returns `true` when `known` says that this broker leads `partition`.
pub(super) fn leads(known: &StatePartitionLeaders, partition: PartitionIndex) -> bool {
    known
        .get(&partition)
        .is_some_and(|leadership| leadership.leads)
}
