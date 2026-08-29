//! Share-partition leader manager (KIP-932).
//!
//! The manager owns one [`AcquisitionState`] machine per
//! `(group, topic_id, partition)` that this broker leads. It loads each machine
//! lazily from the durable `SharePersister` and persists it again each time it
//! goes dirty. The `ShareFetch` and `ShareAcknowledge` handlers drive the
//! per-cell state under its `tokio::sync::Mutex`. A background sweeper expires
//! the acquisition locks.
//!
//! Locking discipline: the `DashMap` guard is NEVER held across an `.await`.
//! Callers clone the cell `Arc` out of the map first, then lock and await.
//!
//! This file holds the type, its constructor, and its `Debug` view. The methods
//! sit in one module per concern: `sessions` runs the share-session lifecycle
//! and the release path a disconnect takes, `metadata_lookup` answers the
//! topic, leader, and epoch questions against the metadata image, `cells` owns
//! the lazily loaded per-partition machines, `persistence` writes a dirty
//! machine back to the persister, and `sweeper` runs the background
//! acquisition-lock timeout.

use std::sync::Arc;

use dashmap::DashMap;
use krabka_metadata::NodeId;
use tokio::sync::Mutex;

use crate::{
    coordinator::unified::share::config::ShareGroupConfig,
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
    share_coordinator::persister_client::SharePersister,
    share_partition::{session::ShareSessionCache, state::AcquisitionState},
};

mod cells;
mod metadata_lookup;
mod persistence;
mod sessions;
mod sweeper;

#[cfg(test)]
mod test_support;

/// Live acquisition-state machines keyed by `(group, topic_id, partition)`.
type LeaderKey = (String, uuid::Uuid, i32);

/// Per-broker owner of the share-partition acquisition state machines.
///
/// The manager owns one machine for each `(group, topic, partition)` triple
/// that this broker leads.
pub(crate) struct SharePartitionLeaderManager {
    node_id: NodeId,
    partitions: Arc<PartitionRegistry>,
    controller: Arc<dyn MetadataSource>,
    persister: Arc<SharePersister>,
    config: Arc<ShareGroupConfig>,
    sessions: ShareSessionCache,
    leaders: DashMap<LeaderKey, Arc<Mutex<AcquisitionState>>>,
}

impl std::fmt::Debug for SharePartitionLeaderManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharePartitionLeaderManager")
            .field("node_id", &self.node_id)
            .field("live_partitions", &self.leaders.len())
            .finish_non_exhaustive()
    }
}

impl SharePartitionLeaderManager {
    pub(crate) fn new(
        node_id: NodeId,
        partitions: Arc<PartitionRegistry>,
        controller: Arc<dyn MetadataSource>,
        persister: Arc<SharePersister>,
        config: Arc<ShareGroupConfig>,
        session_max: usize,
    ) -> Self {
        Self {
            node_id,
            partitions,
            controller,
            persister,
            config,
            sessions: ShareSessionCache::new(session_max),
            leaders: DashMap::new(),
        }
    }
}
