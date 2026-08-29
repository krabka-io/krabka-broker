//! Per-broker `ShareCoordinator`, the KIP-932 persister.
//!
//! The coordinator owns the in-memory delivery state for each
//! `(group, topicId, partition)` of every `__share_group_state` partition that
//! this broker hosts as leader. It writes each state change as a
//! `ShareSnapshot` or `ShareUpdate` record in the matching
//! `__share_group_state` partition. On `Broker::start` it replays those
//! partitions to recover the state.
//!
//! This coordinator mirrors [`crate::txn::coordinator::TxnCoordinator`].
//!
//! Leadership: the wire handlers check [`ShareCoordinator::is_leader`] before
//! they dispatch. The state-machine methods here are `initialize`, `write`,
//! `read`, and `delete`. They assume that this broker leads the related
//! `__share_group_state` partition. `persist_record` still guards against a
//! missing local partition log and returns
//! [`BrokerError::Share`](crate::error::BrokerError::Share) in that case.
//!
//! This file holds the coordinator's identity: the shared types, the struct,
//! and the leadership and partitioning accessors. The state machine lives in
//! `state_machine`, the durable append and the log prune in `persist`, and the
//! log replay in `recovery`.

use std::{collections::HashSet, sync::Arc};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use tokio::sync::{Mutex, RwLock};

mod persist;
mod recovery;
mod state_machine;

#[cfg(test)]
mod test_support;

use crate::{
    partition_registry::PartitionRegistry,
    share_coordinator::{
        bootstrap, config::ShareCoordinatorConfig, partitioner::partition_for_share_key,
        state::SharePartitionState,
    },
};

/// In-memory map key: `(group_id, topic_id, partition)`.
type ShareStateKey3 = (String, uuid::Uuid, i32);

/// KIP-932 share-group state epoch.
///
/// The coordinator bumps this epoch on initialization and on
/// re-initialization, for example `AlterShareGroupOffsets`. It fences a write
/// or initialize that carries an older epoch with `FENCED_STATE_EPOCH`.
pub(crate) type StateEpoch = i32;

/// Leader epoch of the share-partition leader that issued a state write.
///
/// The coordinator fences a stale value with `FENCED_LEADER_EPOCH`.
pub(crate) type LeaderEpoch = i32;

/// Per-partition Kafka wire error code for a failed state-machine operation.
///
/// See [`crate::codes`].
pub(crate) type ShareErrorCode = i16;

/// Summary tuple that [`ShareCoordinator::read_summary`] returns.
///
/// The fields are `state_epoch`, `leader_epoch`, `start_offset`, and
/// `delivery_complete_count`.
pub(crate) type ShareStateSummary = (StateEpoch, LeaderEpoch, Offset, i32);

/// `start_offset` sentinel for "no persisted share state".
///
/// This value tells the share-partition leader to initialize delivery from
/// scratch (KIP-932).
pub(crate) const UNINITIALIZED_START_OFFSET: i64 = -1;

/// Per-broker share-state coordinator.
///
/// `Broker::start` constructs the coordinator and shares it with the
/// share-state wire handlers through an `Arc`.
pub(crate) struct ShareCoordinator {
    pub(crate) node_id: krabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    /// Live in-memory state: `(group, topicId, partition)` → locked state.
    state: DashMap<ShareStateKey3, Arc<Mutex<SharePartitionState>>>,
    /// Set of `__share_group_state` partition indices this broker leads.
    leader_partitions: RwLock<HashSet<PartitionIndex>>,
    config: ShareCoordinatorConfig,
}

impl ShareCoordinator {
    pub(crate) fn new(
        node_id: krabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        config: ShareCoordinatorConfig,
    ) -> Self {
        Self {
            node_id,
            partitions,
            state: DashMap::new(),
            leader_partitions: RwLock::new(HashSet::new()),
            config,
        }
    }

    /// Recomputes which `__share_group_state` partitions this broker leads.
    ///
    /// The coordinator reads the current `MetadataImage`. `recover` calls this
    /// method, and the broker calls it again on every metadata change.
    pub(crate) async fn refresh_leader_partitions(&self, image: &MetadataImage) {
        let mut set = HashSet::new();
        for p in image.partitions_of(bootstrap::TOPIC) {
            if p.leader == self.node_id {
                set.insert(PartitionIndex(p.partition));
            }
        }
        *self.leader_partitions.write().await = set;
    }

    /// Returns `true` if this broker leads `__share_group_state`-`state_partition`.
    pub(crate) async fn is_leader(&self, state_partition: PartitionIndex) -> bool {
        self.leader_partitions
            .read()
            .await
            .contains(&state_partition)
    }

    #[cfg(test)]
    pub(crate) async fn lead_all_partitions_for_test(&self) {
        let mut set = HashSet::new();
        for p in 0..self.config.state_topic_num_partitions {
            set.insert(PartitionIndex(p));
        }
        *self.leader_partitions.write().await = set;
    }

    #[must_use]
    pub(crate) fn state_topic_num_partitions(&self) -> i32 {
        self.config.state_topic_num_partitions
    }

    pub(crate) fn state_topic_replication_factor(&self) -> i16 {
        self.config.state_topic_replication_factor
    }

    /// Returns the `__share_group_state` partition index responsible for the
    /// share key `(group, topic_id, partition)`.
    #[must_use]
    pub(crate) fn state_partition_for(
        &self,
        group: &str,
        topic_id: &uuid::Uuid,
        partition: i32,
    ) -> PartitionIndex {
        PartitionIndex(partition_for_share_key(
            group,
            topic_id,
            partition,
            self.config.state_topic_num_partitions,
        ))
    }
}
