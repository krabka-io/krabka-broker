//! Per-broker `ShareCoordinator`, the KIP-932 persister.
//!
//! The coordinator owns the in-memory delivery state for each
//! `(group, topicId, partition)` of every `__share_group_state` partition that
//! this broker hosts as leader. It writes each state change as a
//! `ShareSnapshot` or `ShareUpdate` record in the matching
//! `__share_group_state` partition. It replays a partition when it becomes the
//! leader of it.
//!
//! This coordinator mirrors [`crate::txn::coordinator::TxnCoordinator`].
//!
//! Leadership follows Kafka's `CoordinatorRuntime`. When this broker becomes
//! the leader of a `__share_group_state` partition (a new entry in the
//! metadata image, or a new leader epoch), the coordinator drops the keys of
//! that partition and replays its log in a background task. Until the replay
//! ends, every state-machine method answers `COORDINATOR_LOAD_IN_PROGRESS` for
//! the keys of that partition. When the broker stops leading the partition,
//! the coordinator drops the keys and answers `NOT_COORDINATOR`.
//!
//! Each state-machine method holds a read guard on the leadership map from
//! its status check to its last in-memory change. A load or an unload takes
//! the write guard, so it never interleaves with an operation that is still
//! running on the same partition.
//!
//! This file holds the coordinator's identity: the shared types, the struct,
//! and the leadership and partitioning accessors. The state machine lives in
//! `state_machine`, the durable append and the log prune in `persist`, and the
//! log replay in `recovery`.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};

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

/// Load status of one `__share_group_state` partition that this broker leads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoadStatus {
    /// The metadata image names this broker as leader, but the partition log
    /// is not open locally yet. The next refresh after the log opens starts
    /// the load.
    Pending,
    /// A replay of the partition log runs.
    Loading,
    /// The replay ended. The partition serves requests.
    Active,
    /// The replay failed. The partition answers `NOT_COORDINATOR`, and the
    /// next refresh loads it again.
    Failed,
}

/// One led `__share_group_state` partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LedPartition {
    /// The partition leader epoch that the metadata image gave for this term.
    leader_epoch: i32,
    /// Identifies the load of this term. A load that ends after a newer term
    /// started does not install its state.
    generation: u64,
    status: LoadStatus,
}

/// Map from led `__share_group_state` partition to its load status.
pub(super) type LeaderPartitions = HashMap<PartitionIndex, LedPartition>;

/// The load tasks that one [`ShareCoordinator::refresh_leader_partitions`]
/// call started.
///
/// A caller that does not wait drops this value. The tasks run on.
#[derive(Debug, Default)]
pub(crate) struct ScheduledLoads(Vec<tokio::task::JoinHandle<()>>);

impl ScheduledLoads {
    /// Waits until every load task of this refresh has ended.
    pub(crate) async fn finished(self) {
        for handle in self.0 {
            if let Err(error) = handle.await {
                tracing::warn!(%error, "__share_group_state load task failed");
            }
        }
    }
}

/// Per-broker share-state coordinator.
///
/// `Broker::start` constructs the coordinator and shares it with the
/// share-state wire handlers through an `Arc`.
pub(crate) struct ShareCoordinator {
    pub(crate) node_id: krabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    /// Live in-memory state: `(group, topicId, partition)` → locked state.
    state: DashMap<ShareStateKey3, Arc<Mutex<SharePartitionState>>>,
    /// The `__share_group_state` partitions this broker leads, with the load
    /// status of each.
    leader_partitions: RwLock<LeaderPartitions>,
    /// Source of [`LedPartition::generation`].
    next_generation: AtomicU64,
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
            leader_partitions: RwLock::new(HashMap::new()),
            next_generation: AtomicU64::new(0),
            config,
        }
    }

    /// Applies the leadership of `image` to the led partitions.
    ///
    /// For each partition that this broker now leads with a new leader epoch,
    /// the method drops the in-memory keys of the partition and starts a
    /// replay of its log, as Kafka's `ShareCoordinatorService.onElection`
    /// does. For each partition that this broker no longer leads, the method
    /// drops the keys, as `onResignation` does. The broker calls this method
    /// on every metadata change.
    pub(crate) async fn refresh_leader_partitions(
        self: &Arc<Self>,
        image: &MetadataImage,
    ) -> ScheduledLoads {
        let desired: HashMap<PartitionIndex, i32> = image
            .partitions_of(bootstrap::TOPIC)
            .filter(|p| p.leader == self.node_id)
            .map(|p| (PartitionIndex(p.partition), p.leader_epoch.0))
            .collect();
        // Most refreshes change nothing. Check that under the read guard, so
        // a refresh does not wait for the operations that are running.
        if !self.leadership_changes(&desired).await {
            return ScheduledLoads::default();
        }
        let mut to_load = Vec::new();
        let mut to_drop = Vec::new();
        {
            let mut led = self.leader_partitions.write().await;
            led.retain(|partition, _| {
                let keep = desired.contains_key(partition);
                if !keep {
                    to_drop.push(*partition);
                }
                keep
            });
            for (partition, leader_epoch) in desired {
                let local = self.partitions.contains(bootstrap::TOPIC, partition);
                let entry = led.get(&partition).copied();
                let new_term = entry.is_none_or(|e| e.leader_epoch != leader_epoch);
                let reload = entry
                    .is_some_and(|e| matches!(e.status, LoadStatus::Pending | LoadStatus::Failed))
                    && local;
                if !new_term && !reload {
                    continue;
                }
                let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                let status = if local {
                    LoadStatus::Loading
                } else {
                    LoadStatus::Pending
                };
                led.insert(
                    partition,
                    LedPartition {
                        leader_epoch,
                        generation,
                        status,
                    },
                );
                to_drop.push(partition);
                if local {
                    to_load.push((partition, generation));
                }
            }
            // Drop the old keys while the write guard still holds, so no
            // operation sees a half-dropped partition.
            for partition in &to_drop {
                self.drop_partition_state(*partition);
            }
        }
        ScheduledLoads(
            to_load
                .into_iter()
                .map(|(partition, generation)| {
                    let coordinator = Arc::clone(self);
                    tokio::spawn(async move {
                        coordinator.load_partition(partition, generation).await;
                    })
                })
                .collect(),
        )
    }

    /// Whether `desired` (led partition to leader epoch) differs from the led
    /// partitions, or a pending or failed partition can load now.
    async fn leadership_changes(&self, desired: &HashMap<PartitionIndex, i32>) -> bool {
        let led = self.leader_partitions.read().await;
        led.len() != desired.len()
            || desired.iter().any(|(partition, leader_epoch)| {
                led.get(partition).is_none_or(|entry| {
                    entry.leader_epoch != *leader_epoch
                        || (matches!(entry.status, LoadStatus::Pending | LoadStatus::Failed)
                            && self.partitions.contains(bootstrap::TOPIC, *partition))
                })
            })
    }

    /// Removes every in-memory key that maps to `state_partition`.
    fn drop_partition_state(&self, state_partition: PartitionIndex) {
        self.state.retain(|(group, topic_id, partition), _| {
            self.state_partition_for(group, topic_id, *partition) != state_partition
        });
    }

    /// The load status of `state_partition`, or `None` when this broker does
    /// not lead it.
    pub(crate) async fn load_status(&self, state_partition: PartitionIndex) -> Option<LoadStatus> {
        self.leader_partitions
            .read()
            .await
            .get(&state_partition)
            .map(|led| led.status)
    }

    /// Returns a read guard on the leadership map when `state_partition` is
    /// active.
    ///
    /// # Errors
    ///
    /// Returns `COORDINATOR_LOAD_IN_PROGRESS` while the partition loads, and
    /// `NOT_COORDINATOR` when this broker does not lead it or its load failed. These are the
    /// codes of Kafka's `CoordinatorRuntime.withActiveContextOrThrow`.
    pub(super) async fn active(
        &self,
        state_partition: PartitionIndex,
    ) -> Result<RwLockReadGuard<'_, LeaderPartitions>, ShareErrorCode> {
        let led = self.leader_partitions.read().await;
        match led.get(&state_partition).map(|entry| entry.status) {
            Some(LoadStatus::Active) => Ok(led),
            Some(LoadStatus::Pending | LoadStatus::Loading) => {
                Err(crate::codes::COORDINATOR_LOAD_IN_PROGRESS)
            }
            Some(LoadStatus::Failed) | None => Err(crate::codes::NOT_COORDINATOR),
        }
    }

    /// Returns `true` if this broker leads `__share_group_state`-`state_partition`.
    ///
    /// The answer is `true` also while the partition loads. The persister
    /// client uses it to route a call to the local coordinator, which then
    /// answers `COORDINATOR_LOAD_IN_PROGRESS` until the load ends.
    pub(crate) async fn is_leader(&self, state_partition: PartitionIndex) -> bool {
        self.load_status(state_partition).await.is_some()
    }

    #[cfg(test)]
    pub(crate) async fn lead_all_partitions_for_test(&self) {
        let mut led = self.leader_partitions.write().await;
        led.clear();
        for p in 0..self.config.state_topic_num_partitions {
            led.insert(
                PartitionIndex(p),
                LedPartition {
                    leader_epoch: 0,
                    generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
                    status: LoadStatus::Active,
                },
            );
        }
    }

    /// Test-only: starts a new term on every state partition and replays each
    /// log, as a load after an election does.
    #[cfg(test)]
    pub(crate) async fn reload_all_partitions_for_test(&self) {
        let mut terms = Vec::new();
        {
            let mut led = self.leader_partitions.write().await;
            for p in 0..self.config.state_topic_num_partitions {
                let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                led.insert(
                    PartitionIndex(p),
                    LedPartition {
                        leader_epoch: 0,
                        generation,
                        status: LoadStatus::Loading,
                    },
                );
                self.drop_partition_state(PartitionIndex(p));
                terms.push((PartitionIndex(p), generation));
            }
        }
        for (partition, generation) in terms {
            self.load_partition(partition, generation).await;
        }
    }

    #[must_use]
    pub(crate) fn state_topic_num_partitions(&self) -> i32 {
        self.config.state_topic_num_partitions
    }

    pub(crate) fn state_topic_replication_factor(&self) -> i16 {
        self.config.state_topic_replication_factor
    }

    /// The topic configs `__share_group_state` is created with.
    pub(crate) fn state_topic_configs(&self) -> std::collections::BTreeMap<String, String> {
        super::bootstrap::topic_configs(&self.config)
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
