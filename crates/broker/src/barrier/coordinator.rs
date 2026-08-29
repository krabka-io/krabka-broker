//! Per-broker `BarrierCoordinator`.
//!
//! The coordinator owns the barrier groups whose `__barrier_state` partition
//! this broker leads. It keeps one entry per group behind a mutex, and it
//! holds that mutex for a whole injection. A scheduled tick, a triggered
//! injection, and a group edit serialise against each other for that reason.
//! The mutex is the only concurrency device here, and a caller that cannot
//! take it gets [`BarrierError::InjectionInProgress`].
//!
//! This coordinator mirrors [`crate::txn::coordinator::TxnCoordinator`] and
//! the share coordinator.
//!
//! # The injection protocol
//!
//! 1. Refuse when this broker does not coordinate the group, and refuse when
//!    the group entry is busy.
//! 2. Allocate the next epoch, and write the injection-start record before the
//!    first marker append. That record is what makes an epoch impossible to
//!    reuse across a coordinator crash.
//! 3. Freeze the target set in that record. A topic-set edit and a
//!    partition-count change both apply from the next epoch.
//! 4. Fan the markers out over the frozen set, grouped by current leader.
//! 5. Retry the partitions that carry no marker, up to the deadline.
//! 6. Write the cut record. A deadline that runs out gives a partial cut that
//!    names the partitions it missed. The epoch is consumed either way.
//! 7. Tombstone the epoch that leaves the retention window. The coordinator
//!    never trims the log, because the group definitions share the prefix.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;
use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::{Time, convert::TimeExt as _};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use crate::{
    barrier::{
        STATE_TOPIC,
        config::BarrierConfig,
        error::BarrierError,
        injection::{MarkerFanout, RemoteMarkerWriter, freeze_targets},
        marker::BarrierMarker,
        metrics::{BarrierMetrics, InjectionReport},
        partitioner::partition_for_group,
        persistence::{
            CutStatus, CutValue, GroupValue, InjectionStartValue, RecordKey, RecordKind,
            decode_cut, decode_group, decode_injection_start, decode_key, encode_cut, encode_group,
            encode_injection_start, encode_key,
        },
        state::{
            GroupEntry, GroupSpec, PendingInjection, StateRecord, TargetPartition, apply_record,
            build_cut, expand_targets, expired_cut_epochs, is_due, next_epoch, schedule_next,
        },
    },
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
    time_util::now_ms,
};

/// How long `create_group` waits for the state topic's partition to take a
/// leader before it gives up. Creation and leader assignment are two separate
/// metadata rounds, and the caller cannot act until the second one lands.
const STATE_TOPIC_READY_TIMEOUT: Time = krabka_units::secs(10);

/// How often that wait re-reads the metadata image.
const STATE_TOPIC_READY_POLL: Time = krabka_units::millis(20);

/// What one published cut says about its group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetainedCut {
    pub(crate) epoch: i64,
    pub(crate) cut: CutValue,
}

/// What one finished injection returns to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InjectionOutcome {
    pub(crate) epoch: i64,
    pub(crate) cut: CutValue,
}

/// What `DescribeBarrierGroups` reports for one group.
///
/// The type is [`PartialEq`] but not [`Eq`], because [`GroupValue`] carries a
/// [`Time`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupDescription {
    pub(crate) group: String,
    pub(crate) definition: GroupValue,
    /// The epochs of the cuts the group retains, in ascending order.
    pub(crate) cut_epochs: Vec<i64>,
    /// The epoch of an injection that published no cut yet.
    pub(crate) pending_epoch: Option<i64>,
}

/// Check a group definition that a caller supplied.
///
/// # Errors
/// Returns [`BarrierError::InvalidDefinition`] when the topic list is empty,
/// when it holds an empty name or a duplicate name, when `retained_cuts` is
/// below one, or when the interval is not positive.
/// The fan-out deadline a request gets.
///
/// `ceiling` is the operator's configured bound. A request that names none
/// takes it, and one that asks for longer is held to it, so a caller cannot
/// hold a group's lock past what the operator allows.
fn clamp_timeout(requested: Option<Time>, ceiling: Time) -> Time {
    requested.map_or(ceiling, |asked| asked.min(ceiling))
}

pub(crate) fn validate_spec(spec: &GroupSpec) -> Result<(), BarrierError> {
    if spec.topics.is_empty() {
        return Err(BarrierError::InvalidDefinition(
            "a barrier group needs at least one topic".to_owned(),
        ));
    }
    if spec.topics.iter().any(String::is_empty) {
        return Err(BarrierError::InvalidDefinition(
            "a barrier group topic name is empty".to_owned(),
        ));
    }
    let unique: HashSet<&String> = spec.topics.iter().collect();
    if unique.len() != spec.topics.len() {
        return Err(BarrierError::InvalidDefinition(
            "a barrier group names one topic twice".to_owned(),
        ));
    }
    if spec.retained_cuts < 1 {
        return Err(BarrierError::InvalidDefinition(format!(
            "retained_cuts is {}, and it must be one or more",
            spec.retained_cuts
        )));
    }
    if spec.interval.is_some_and(|i| i.millis_i64() <= 0) {
        return Err(BarrierError::InvalidDefinition(
            "the injection interval must be one millisecond or more".to_owned(),
        ));
    }
    Ok(())
}

/// Per-broker barrier coordinator.
///
/// `Broker::start` builds it and shares it with the barrier wire handlers and
/// the scheduler through an `Arc`.
pub(crate) struct BarrierCoordinator {
    pub(crate) node_id: NodeId,
    partitions: Arc<PartitionRegistry>,
    controller: Arc<dyn MetadataSource>,
    config: BarrierConfig,
    metrics: Arc<dyn BarrierMetrics>,
    remote: Option<Arc<dyn RemoteMarkerWriter>>,
    /// Live groups: name to locked entry.
    groups: DashMap<String, Arc<Mutex<GroupEntry>>>,
    /// The `__barrier_state` partition indices this broker leads.
    leader_partitions: RwLock<HashSet<PartitionIndex>>,
}

impl BarrierCoordinator {
    pub(crate) fn new(
        node_id: NodeId,
        partitions: Arc<PartitionRegistry>,
        controller: Arc<dyn MetadataSource>,
        config: BarrierConfig,
        metrics: Arc<dyn BarrierMetrics>,
    ) -> Self {
        Self {
            node_id,
            partitions,
            controller,
            config,
            metrics,
            remote: None,
            groups: DashMap::new(),
            leader_partitions: RwLock::new(HashSet::new()),
        }
    }

    /// Bind the leg of the fan-out that leaves this broker.
    ///
    /// A coordinator with no transport marks only the partitions it leads, and
    /// every remote partition lands in the `missing` list of the cut.
    pub(crate) fn configure_marker_transport(&mut self, remote: Arc<dyn RemoteMarkerWriter>) {
        self.remote = Some(remote);
    }

    #[must_use]
    pub(crate) fn state_topic_num_partitions(&self) -> i32 {
        self.config.state_topic_num_partitions
    }

    #[must_use]
    pub(crate) fn state_topic_replication_factor(&self) -> i16 {
        self.config.state_topic_replication_factor
    }

    /// How often the scheduler should call [`Self::run_due_injections`].
    #[must_use]
    pub(crate) fn scheduler_tick(&self) -> Time {
        self.config.scheduler_tick
    }

    /// The `__barrier_state` partition that carries `group`.
    #[must_use]
    pub(crate) fn state_partition_for(&self, group: &str) -> PartitionIndex {
        PartitionIndex(partition_for_group(
            group,
            self.config.state_topic_num_partitions,
        ))
    }

    /// Recompute which `__barrier_state` partitions this broker leads.
    ///
    /// The replicator supervisor calls this on every metadata change, beside
    /// the same call for the transaction and share coordinators.
    pub(crate) async fn refresh_leader_partitions(&self, image: &MetadataImage) {
        let mut set = HashSet::new();
        for p in image.partitions_of(STATE_TOPIC) {
            if p.leader == self.node_id {
                set.insert(PartitionIndex(p.partition));
            }
        }
        *self.leader_partitions.write().await = set;
    }

    /// Whether this broker coordinates `group` now.
    pub(crate) async fn is_coordinator_for(&self, group: &str) -> bool {
        let partition = self.state_partition_for(group);
        self.leader_partitions.read().await.contains(&partition)
    }

    /// The coordinator epoch of `group`, which is the leader epoch of its
    /// state partition.
    ///
    /// It fences a coordinator that lost and regained the partition, the same
    /// way the transaction coordinator fences a marker dispatch.
    #[must_use]
    pub(crate) fn coordinator_epoch(&self, group: &str, image: &MetadataImage) -> Option<i32> {
        image
            .partition(STATE_TOPIC, self.state_partition_for(group).get())
            .map(|p| p.leader_epoch.get())
    }

    /// Create a group.
    ///
    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker owns the
    /// group, [`BarrierError::GroupExists`] when the name is live,
    /// [`BarrierError::InvalidDefinition`] when the definition is not usable,
    /// and [`BarrierError::Persist`] when the append fails.
    /// Create `__barrier_state` if no broker has yet, and wait until the
    /// partition `group` hashes to has a leader.
    ///
    /// The call is idempotent and returns at once when the topic is already
    /// there. The wait is what makes lazy creation safe: leadership is
    /// assigned after the topic record lands, and `is_coordinator_for` reads
    /// the leader set, so a caller that raced the assignment would be told it
    /// is not the coordinator for a group it just asked to create.
    async fn ensure_state_topic(&self, group: &str) -> Result<(), BarrierError> {
        crate::barrier::bootstrap::ensure_topic(
            &self.controller,
            self.state_topic_num_partitions(),
            self.state_topic_replication_factor(),
        )
        .await?;

        let partition = self.state_partition_for(group);
        let deadline = Instant::now() + STATE_TOPIC_READY_TIMEOUT.to_std();
        loop {
            let image = self.controller.current_image();
            let led = image
                .partition(STATE_TOPIC, partition.get())
                .is_some_and(|p| p.leader == self.node_id);
            // Metadata leadership lands one round before the log is open here,
            // and the coordinator writes through the partition, so both have
            // to be true before the caller can be told it is the coordinator.
            let open = self.partitions.get(STATE_TOPIC, partition).is_some();
            if led && open {
                self.refresh_leader_partitions(&image).await;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(BarrierError::NotCoordinator {
                    group: group.to_owned(),
                });
            }
            tokio::time::sleep(STATE_TOPIC_READY_POLL.to_std()).await;
        }
    }

    pub(crate) async fn create_group(
        &self,
        group: &str,
        spec: GroupSpec,
    ) -> Result<GroupValue, BarrierError> {
        validate_spec(&spec)?;
        // The first group is what brings __barrier_state into being. The topic
        // has to exist before require_coordinator can decide anything, because
        // that decision is the leadership of the partition the group hashes
        // to. A broker that loses the race finds the topic already there.
        self.ensure_state_topic(group).await?;
        self.require_coordinator(group).await?;
        let handle = self.entry_handle(group);
        let mut entry = handle.lock().await;
        if entry.is_defined() {
            return Err(BarrierError::GroupExists {
                group: group.to_owned(),
            });
        }

        let definition = GroupValue {
            topics: spec.topics,
            interval: spec.interval,
            retained_cuts: spec.retained_cuts,
            last_epoch: entry.last_epoch(),
        };
        self.append_records(
            group,
            vec![(
                RecordKey::group(group),
                Some(encode_group(&definition).into()),
            )],
        )
        .await?;

        entry.definition = definition.clone();
        schedule_next(&mut entry, now_ms());
        drop(entry);
        self.report_group_count();
        Ok(definition)
    }

    /// Replace the definition of a live group.
    ///
    /// The new topic set and the new partition counts apply from the next
    /// epoch, because an injection freezes its target set before it appends
    /// any marker.
    ///
    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker owns the
    /// group, [`BarrierError::UnknownGroup`] when no group of that name is
    /// live, [`BarrierError::InvalidDefinition`] when the definition is not
    /// usable, and [`BarrierError::Persist`] when the append fails.
    pub(crate) async fn update_group(
        &self,
        group: &str,
        spec: GroupSpec,
    ) -> Result<GroupValue, BarrierError> {
        validate_spec(&spec)?;
        self.require_coordinator(group).await?;
        let handle = self.live_entry(group)?;
        let mut entry = handle.lock().await;
        if !entry.is_defined() {
            return Err(BarrierError::UnknownGroup {
                group: group.to_owned(),
            });
        }

        let definition = GroupValue {
            topics: spec.topics,
            interval: spec.interval,
            retained_cuts: spec.retained_cuts,
            last_epoch: entry.last_epoch(),
        };
        self.append_records(
            group,
            vec![(
                RecordKey::group(group),
                Some(encode_group(&definition).into()),
            )],
        )
        .await?;

        entry.definition = definition.clone();
        schedule_next(&mut entry, now_ms());
        Ok(definition)
    }

    /// Delete a group and every record it owns.
    ///
    /// The coordinator tombstones the group record, the cut record of every
    /// retained epoch, and the injection-start record of an injection that
    /// published no cut. No record of the group then survives compaction.
    ///
    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker owns the
    /// group, [`BarrierError::UnknownGroup`] when no group of that name is
    /// live, and [`BarrierError::Persist`] when the append fails.
    pub(crate) async fn delete_group(&self, group: &str) -> Result<(), BarrierError> {
        self.require_coordinator(group).await?;
        let handle = self.live_entry(group)?;
        let mut entry = handle.lock().await;
        if !entry.is_defined() {
            return Err(BarrierError::UnknownGroup {
                group: group.to_owned(),
            });
        }

        let mut records = vec![(RecordKey::group(group), None)];
        for epoch in entry.cuts.keys().copied() {
            records.push((RecordKey::cut(group, epoch), None));
        }
        if let Some(pending) = &entry.pending {
            records.push((RecordKey::injection_start(group, pending.epoch), None));
        }
        self.append_records(group, records).await?;

        entry.cuts.clear();
        entry.pending = None;
        drop(entry);
        self.groups.remove(group);
        self.report_group_count();
        Ok(())
    }

    /// Describe the named groups, or every group this broker holds when
    /// `names` is empty.
    pub(crate) async fn describe_groups(&self, names: &[String]) -> Vec<GroupDescription> {
        let selected: Vec<(String, Arc<Mutex<GroupEntry>>)> = if names.is_empty() {
            self.groups
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect()
        } else {
            names
                .iter()
                .filter_map(|name| {
                    self.groups
                        .get(name)
                        .map(|e| (name.clone(), e.value().clone()))
                })
                .collect()
        };

        let mut out = Vec::with_capacity(selected.len());
        for (group, handle) in selected {
            let entry = handle.lock().await;
            if !entry.is_defined() {
                continue;
            }
            out.push(GroupDescription {
                group,
                definition: entry.definition.clone(),
                cut_epochs: entry.cuts.keys().copied().collect(),
                pending_epoch: entry.pending.as_ref().map(|p| p.epoch),
            });
        }
        out.sort_by(|a, b| a.group.cmp(&b.group));
        out
    }

    /// The cuts that `group` retains, newest last.
    ///
    /// # Errors
    /// Returns [`BarrierError::UnknownGroup`] when no group of that name is
    /// live on this broker.
    pub(crate) async fn list_cuts(&self, group: &str) -> Result<Vec<RetainedCut>, BarrierError> {
        let handle = self.live_entry(group)?;
        let entry = handle.lock().await;
        if !entry.is_defined() {
            return Err(BarrierError::UnknownGroup {
                group: group.to_owned(),
            });
        }
        Ok(entry
            .cuts
            .iter()
            .map(|(epoch, cut)| RetainedCut {
                epoch: *epoch,
                cut: cut.clone(),
            })
            .collect())
    }

    /// Run one injection for `group`.
    ///
    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker owns the
    /// group, [`BarrierError::UnknownGroup`] when no group of that name is
    /// live, [`BarrierError::InjectionInProgress`] when the group entry is
    /// busy, [`BarrierError::CoordinatorEpochChanged`] when this broker lost
    /// the state partition during the fan-out, and [`BarrierError::Persist`]
    /// when an append fails.
    /// `timeout` bounds how long the fan-out retries the partitions that carry
    /// no marker yet. `None` uses the configured default, and a value above it
    /// is clamped to it, so a caller cannot hold the group's lock for longer
    /// than the operator allows.
    ///
    /// The bound shortens the fan-out deadline rather than dropping the
    /// injection. Abandoning it would leave the epoch's injection-start record
    /// with no cut record, which is the state a crashed coordinator leaves
    /// behind, so a caller's impatience must not manufacture one.
    pub(crate) async fn trigger_injection(
        &self,
        group: &str,
        timeout: Option<Time>,
    ) -> Result<InjectionOutcome, BarrierError> {
        self.require_coordinator(group).await?;
        let handle = self.live_entry(group)?;
        let mut entry = handle
            .try_lock()
            .map_err(|_| BarrierError::InjectionInProgress {
                group: group.to_owned(),
            })?;
        if !entry.is_defined() {
            return Err(BarrierError::UnknownGroup {
                group: group.to_owned(),
            });
        }
        self.inject(group, &mut entry, self.effective_timeout(timeout))
            .await
    }

    /// The fan-out deadline for one injection.
    fn effective_timeout(&self, requested: Option<Time>) -> Time {
        clamp_timeout(requested, self.config.injection_timeout)
    }

    /// Inject into every group whose interval elapsed at `now_ms`.
    ///
    /// The scheduler calls this. A group that another caller holds keeps its
    /// due time, so the next tick picks it up again.
    pub(crate) async fn run_due_injections(&self, now_ms: i64) -> Vec<String> {
        let candidates: Vec<String> = self
            .groups
            .iter()
            .filter_map(|e| {
                let entry = e.value().try_lock().ok()?;
                (entry.is_defined() && is_due(&entry, now_ms)).then(|| e.key().clone())
            })
            .collect();

        let mut injected = Vec::new();
        for group in candidates {
            match self.trigger_injection(&group, None).await {
                Ok(outcome) => {
                    info!(group, epoch = outcome.epoch, "scheduled barrier injection");
                    injected.push(group);
                }
                Err(error) => {
                    warn!(group, %error, "scheduled barrier injection failed");
                }
            }
        }
        injected
    }

    /// Replay every locally-led `__barrier_state` partition, and finalise an
    /// injection that a crash left open.
    ///
    /// # Errors
    /// Returns [`BarrierError::Persist`] when the append of a recovery cut
    /// fails. A per-partition read error skips that partition, as if it holds
    /// nothing to replay.
    pub(crate) async fn recover(&self, image: &MetadataImage) -> Result<(), BarrierError> {
        self.refresh_leader_partitions(image).await;
        let replayed = self.replay_led_partitions().await;

        self.groups.clear();
        let now = now_ms();
        for (group, mut entry) in replayed {
            if !entry.is_defined() {
                warn!(
                    group,
                    "no group record defines this barrier state; dropping it"
                );
                continue;
            }
            schedule_next(&mut entry, now);
            self.groups.insert(group, Arc::new(Mutex::new(entry)));
        }

        self.finalize_open_injections(image).await?;
        self.report_group_count();
        info!(
            groups = self.groups.len(),
            "BarrierCoordinator recovery complete"
        );
        Ok(())
    }

    /// Publish a partial cut for every injection that started and published no
    /// cut.
    ///
    /// A coordinator that crashed between the injection-start record and the
    /// cut record left markers that nothing can withdraw. The partial cut is
    /// what accounts for them, and it consumes the epoch for good. The
    /// coordinator did not observe the offsets, so the cut names every frozen
    /// target as missing.
    async fn finalize_open_injections(&self, image: &MetadataImage) -> Result<(), BarrierError> {
        let open: Vec<(String, Arc<Mutex<GroupEntry>>)> = self
            .groups
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        for (group, handle) in open {
            let mut entry = handle.lock().await;
            let Some(pending) = entry.pending.clone() else {
                continue;
            };
            let Some(current) = self.coordinator_epoch(&group, image) else {
                continue;
            };
            if current < pending.start.coordinator_epoch {
                warn!(
                    group,
                    epoch = pending.epoch,
                    coordinator_epoch = current,
                    frozen_at = pending.start.coordinator_epoch,
                    "a newer coordinator owns this injection; leaving it open"
                );
                continue;
            }

            let completed_at = now_ms();
            let cut = build_cut(
                pending.start.triggered_at,
                completed_at,
                &pending.start.targets,
                &BTreeMap::new(),
            );
            warn!(
                group,
                epoch = pending.epoch,
                missing = cut.missing.len(),
                "finalising an interrupted barrier injection as partial"
            );
            self.publish_cut(&group, &mut entry, pending.epoch, cut)
                .await?;
        }
        Ok(())
    }

    /// Run the injection protocol under the group's mutex.
    async fn inject(
        &self,
        group: &str,
        entry: &mut GroupEntry,
        timeout: Time,
    ) -> Result<InjectionOutcome, BarrierError> {
        let image = self.controller.current_image();
        let coordinator_epoch =
            self.coordinator_epoch(group, &image)
                .ok_or_else(|| BarrierError::NotCoordinator {
                    group: group.to_owned(),
                })?;

        let epoch = next_epoch(entry.last_epoch());
        let triggered_at = now_ms();
        let targets = freeze_targets(&entry.definition.topics, &image);
        let start = InjectionStartValue {
            coordinator_epoch,
            triggered_at,
            targets,
        };

        // The injection-start record lands before the first marker, so a crash
        // here cannot let another coordinator reuse this epoch.
        self.append_records(
            group,
            vec![(
                RecordKey::injection_start(group, epoch),
                Some(encode_injection_start(&start).into()),
            )],
        )
        .await?;
        entry.pending = Some(PendingInjection {
            epoch,
            start: start.clone(),
        });
        self.metrics.injection_started(group, epoch);

        let marker = BarrierMarker {
            group: group.to_owned(),
            epoch,
            triggered_at,
        };
        let placed = self
            .fan_out(&marker, expand_targets(&start.targets), timeout)
            .await;

        // A coordinator that lost the state partition during the fan-out must
        // not write the cut. The new coordinator finalises the epoch from the
        // injection-start record.
        let current = self
            .coordinator_epoch(group, &self.controller.current_image())
            .ok_or_else(|| BarrierError::NotCoordinator {
                group: group.to_owned(),
            })?;
        if current != coordinator_epoch {
            return Err(BarrierError::CoordinatorEpochChanged {
                group: group.to_owned(),
                expected: coordinator_epoch,
                current,
            });
        }

        let completed_at = now_ms();
        let cut = build_cut(triggered_at, completed_at, &start.targets, &placed);
        let report = InjectionReport {
            epoch,
            status: cut.status,
            marked: placed.len(),
            missing: cut.missing.len(),
            elapsed: Time::from_millis(completed_at.saturating_sub(triggered_at)),
        };
        self.publish_cut(group, entry, epoch, cut.clone()).await?;
        self.metrics.injection_completed(group, report);
        if cut.status == CutStatus::Partial {
            warn!(
                group,
                epoch,
                missing = cut.missing.len(),
                "published a partial barrier cut"
            );
        }
        Ok(InjectionOutcome { epoch, cut })
    }

    /// Write the markers of one epoch and collect their offsets.
    async fn fan_out(
        &self,
        marker: &BarrierMarker,
        targets: Vec<TargetPartition>,
        timeout: Time,
    ) -> BTreeMap<TargetPartition, Offset> {
        MarkerFanout {
            node_id: self.node_id,
            partitions: &self.partitions,
            controller: &self.controller,
            remote: self.remote.as_ref(),
            metrics: self.metrics.as_ref(),
            config: &self.config,
        }
        .run(marker, targets, timeout)
        .await
    }

    /// Publish one cut, and retire the epoch that leaves the retention window.
    ///
    /// The cut record, the rewritten group record, and the tombstones go into
    /// one append, so a reader never sees a cut without the group record that
    /// counts it. The coordinator tombstones the expired epoch instead of a
    /// log trim, because the group definitions live in the same prefix and a
    /// trim would delete them.
    async fn publish_cut(
        &self,
        group: &str,
        entry: &mut GroupEntry,
        epoch: i64,
        cut: CutValue,
    ) -> Result<(), BarrierError> {
        let definition = GroupValue {
            last_epoch: epoch,
            ..entry.definition.clone()
        };
        let held: Vec<i64> = entry.cuts.keys().copied().collect();
        let expired = expired_cut_epochs(epoch, entry.definition.retained_cuts, &held);
        let mut records = vec![
            (RecordKey::cut(group, epoch), Some(encode_cut(&cut).into())),
            (
                RecordKey::group(group),
                Some(encode_group(&definition).into()),
            ),
            // The cut supersedes the injection-start record of its own epoch.
            (RecordKey::injection_start(group, epoch), None),
        ];
        for epoch in &expired {
            records.push((RecordKey::cut(group, *epoch), None));
        }
        self.append_records(group, records).await?;

        entry.definition = definition;
        entry.cuts.insert(epoch, cut);
        entry.pending = None;
        for epoch in &expired {
            entry.cuts.remove(epoch);
        }
        schedule_next(entry, now_ms());
        Ok(())
    }

    /// Append one batch of `__barrier_state` records for `group`.
    ///
    /// Every record of a group lands on one partition, so the group's epochs
    /// hold a total order. A `None` value writes a tombstone.
    ///
    /// # Errors
    /// Returns [`BarrierError::StateNotLocal`] when the state partition is not
    /// open here, and [`BarrierError::Persist`] when the append fails.
    async fn append_records(
        &self,
        group: &str,
        records: Vec<(RecordKey, Option<Bytes>)>,
    ) -> Result<Offset, BarrierError> {
        let index = self.state_partition_for(group);
        let partition = self
            .partitions
            .get(STATE_TOPIC, index)
            .ok_or(BarrierError::StateNotLocal { partition: index })?;

        let last = i32::try_from(records.len())
            .map_err(|_| {
                BarrierError::InvalidDefinition("too many records for one append".to_owned())
            })?
            .saturating_sub(1);
        let mut batch = RecordBatch {
            last_offset_delta: last,
            records: Vec::with_capacity(records.len()),
            ..RecordBatch::default()
        };
        for (delta, (key, value)) in records.into_iter().enumerate() {
            batch.records.push(Record {
                offset_delta: i32::try_from(delta).expect("the record count fits in an i32"),
                key: Some(encode_key(&key).into()),
                value,
                ..Record::default()
            });
        }
        Ok(partition.produce_batch(batch).await?)
    }

    /// Replay every `__barrier_state` partition this broker leads.
    async fn replay_led_partitions(&self) -> BTreeMap<String, GroupEntry> {
        let led: Vec<PartitionIndex> = self
            .leader_partitions
            .read()
            .await
            .iter()
            .copied()
            .collect();

        let mut state = BTreeMap::new();
        for index in led {
            let Some(partition) = self.partitions.get(STATE_TOPIC, index) else {
                continue;
            };
            let mut offset = partition.log_start_offset();
            loop {
                let read = match partition.read_log(offset, self.config.recovery_read_max) {
                    Ok(read) => read,
                    Err(error) => {
                        warn!(
                            partition = index.get(),
                            %error,
                            "read error during __barrier_state recovery; skipping partition"
                        );
                        break;
                    }
                };
                if read.batches.is_empty() {
                    break;
                }
                for batch in &read.batches {
                    for record in &batch.records {
                        if let Some(decoded) = decode_state_record(index, record) {
                            apply_record(&mut state, decoded);
                        }
                    }
                    offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
                }
            }
        }
        state
    }

    /// The entry of `group`, or a fresh one.
    fn entry_handle(&self, group: &str) -> Arc<Mutex<GroupEntry>> {
        self.groups
            .entry(group.to_owned())
            .or_default()
            .value()
            .clone()
    }

    /// The entry of a group that this broker holds.
    ///
    /// # Errors
    /// Returns [`BarrierError::UnknownGroup`] when the name is not live here.
    fn live_entry(&self, group: &str) -> Result<Arc<Mutex<GroupEntry>>, BarrierError> {
        self.groups
            .get(group)
            .map(|e| e.value().clone())
            .ok_or_else(|| BarrierError::UnknownGroup {
                group: group.to_owned(),
            })
    }

    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker leads the
    /// state partition of `group`.
    async fn require_coordinator(&self, group: &str) -> Result<(), BarrierError> {
        if self.is_coordinator_for(group).await {
            Ok(())
        } else {
            Err(BarrierError::NotCoordinator {
                group: group.to_owned(),
            })
        }
    }

    fn report_group_count(&self) {
        self.metrics.groups_coordinated(self.groups.len());
    }
}

/// Decode one replayed `__barrier_state` record.
///
/// The function returns `None` for a record with no key, and for a record that
/// carries a key or a value this broker cannot decode.
fn decode_state_record(partition: PartitionIndex, record: &Record) -> Option<StateRecord> {
    let key_bytes = record.key.as_ref()?;
    let key = match decode_key(key_bytes) {
        Ok(key) => key,
        Err(error) => {
            warn!(
                partition = partition.get(),
                %error,
                "invalid __barrier_state key; skipping record"
            );
            return None;
        }
    };
    let value = record.value.as_deref();

    match key.kind {
        RecordKind::Group => Some(StateRecord::Group {
            group: key.group,
            value: match value {
                None => None,
                Some(bytes) => Some(keep_decoded(partition, decode_group(bytes))?),
            },
        }),
        RecordKind::InjectionStart => Some(StateRecord::InjectionStart {
            group: key.group,
            epoch: key.epoch,
            value: match value {
                None => None,
                Some(bytes) => Some(keep_decoded(partition, decode_injection_start(bytes))?),
            },
        }),
        RecordKind::Cut => Some(StateRecord::Cut {
            group: key.group,
            epoch: key.epoch,
            value: match value {
                None => None,
                Some(bytes) => Some(keep_decoded(partition, decode_cut(bytes))?),
            },
        }),
    }
}

/// Keep a decoded value, and drop one that does not decode.
///
/// A record whose value is present but malformed is not a tombstone, so the
/// caller skips it rather than deleting what the key names.
fn keep_decoded<T>(
    partition: PartitionIndex,
    decoded: Result<T, krabka_protocol::ProtocolError>,
) -> Option<T> {
    match decoded {
        Ok(value) => Some(value),
        Err(error) => {
            warn!(
                partition = partition.get(),
                %error,
                "invalid __barrier_state value; skipping record"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    /// The configured timeout is a ceiling, not a default a caller can raise.
    /// A request that asks for longer would otherwise hold the group's lock
    /// past what the operator allows.
    #[test]
    fn a_requested_timeout_is_clamped_to_the_configured_ceiling() {
        let ceiling = krabka_units::secs(30);
        let cases = [
            ("no opinion takes the ceiling", None, ceiling),
            (
                "under the ceiling is honoured",
                Some(krabka_units::secs(5)),
                krabka_units::secs(5),
            ),
            (
                "over the ceiling is clamped",
                Some(krabka_units::secs(600)),
                ceiling,
            ),
            ("exactly the ceiling", Some(ceiling), ceiling),
        ];
        for (case, requested, expected) in cases {
            assert!(clamp_timeout(requested, ceiling) == expected, "{case}");
        }
    }

    use assert2::{assert, check};
    use krabka_metadata::MetadataRecord;
    use krabka_units::millis;
    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::barrier::{
        marker::parse_barrier_marker,
        metrics::NoBarrierMetrics,
        persistence::{MissingPartition, PartitionOffset, TopicOffsets},
        test_support::{StaticSource, open_partition, topic_records},
    };

    const GROUP: &str = "orders-cut";

    fn config() -> BarrierConfig {
        BarrierConfig {
            state_topic_num_partitions: 4,
            injection_timeout: millis(30),
            retry_backoff: millis(1),
            retry_backoff_max: millis(2),
            ..BarrierConfig::default()
        }
    }

    fn spec(topics: &[&str], interval: Option<Time>, retained_cuts: i32) -> GroupSpec {
        GroupSpec {
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            interval,
            retained_cuts,
        }
    }

    fn cluster_records() -> Vec<MetadataRecord> {
        [
            topic_records(STATE_TOPIC, 4, NodeId(1)),
            topic_records("orders", 2, NodeId(1)),
            topic_records("payments", 1, NodeId(1)),
        ]
        .concat()
    }

    // A broker that leads every state partition and every data partition.
    struct Fixture {
        _dir: TempDir,
        registry: Arc<PartitionRegistry>,
        source: Arc<StaticSource>,
        config: BarrierConfig,
    }

    impl Fixture {
        // Every partition of the cluster is open here, and this broker leads
        // all of them.
        fn new() -> Self {
            Self::with_data_partitions(&[("orders", 2), ("payments", 1)])
        }

        // Open the state partitions, and only the named data partitions.
        fn with_data_partitions(data: &[(&str, i32)]) -> Self {
            let dir = tempdir().expect("tempdir");
            let registry = Arc::new(PartitionRegistry::new());
            for p in 0..4 {
                open_partition(&registry, dir.path(), STATE_TOPIC, p);
            }
            for (topic, count) in data {
                for p in 0..*count {
                    open_partition(&registry, dir.path(), topic, p);
                }
            }
            Self {
                _dir: dir,
                registry,
                source: Arc::new(StaticSource::new(&cluster_records())),
                config: config(),
            }
        }

        async fn coordinator(&self) -> BarrierCoordinator {
            let controller: Arc<dyn MetadataSource> = Arc::clone(&self.source) as _;
            let coordinator = BarrierCoordinator::new(
                NodeId(1),
                Arc::clone(&self.registry),
                controller,
                self.config.clone(),
                Arc::new(NoBarrierMetrics),
            );
            coordinator
                .refresh_leader_partitions(&self.source.current_image())
                .await;
            coordinator
        }

        // A coordinator that replayed the state partitions from the log.
        async fn recovered(&self) -> BarrierCoordinator {
            let coordinator = self.coordinator().await;
            coordinator
                .recover(&self.source.current_image())
                .await
                .expect("recovery succeeds");
            coordinator
        }
    }

    fn marker_at(
        registry: &PartitionRegistry,
        topic: &str,
        partition: i32,
        offset: Offset,
    ) -> Option<BarrierMarker> {
        let part = registry.get(topic, PartitionIndex(partition))?;
        let read = part
            .read_log(offset, krabka_units::mebibytes(1))
            .expect("read the log back");
        let batch = read.batches.first()?;
        parse_barrier_marker(&batch.records[0]).ok()
    }

    #[test]
    fn a_definition_must_name_at_least_one_usable_topic() {
        let cases: &[(&str, GroupSpec, bool)] = &[
            ("good", spec(&["orders"], Some(millis(1_000)), 4), true),
            ("no interval", spec(&["orders"], None, 1), true),
            ("no topic", spec(&[], None, 4), false),
            ("empty topic name", spec(&["orders", ""], None, 4), false),
            (
                "duplicate topic",
                spec(&["orders", "orders"], None, 4),
                false,
            ),
            ("no retention", spec(&["orders"], None, 0), false),
            ("negative retention", spec(&["orders"], None, -1), false),
            (
                "zero interval",
                spec(&["orders"], Some(Time::ZERO), 4),
                false,
            ),
        ];
        for (case, spec, ok) in cases {
            check!(validate_spec(spec).is_ok() == *ok, "{case}");
        }
    }

    #[tokio::test]
    async fn a_created_group_reads_back_through_describe() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        let definition = coordinator
            .create_group(GROUP, spec(&["orders", "payments"], None, 4))
            .await
            .expect("the group is created");

        let expected = GroupValue {
            topics: vec!["orders".to_owned(), "payments".to_owned()],
            interval: None,
            retained_cuts: 4,
            last_epoch: 0,
        };
        assert!(definition == expected);
        assert!(
            coordinator.describe_groups(&[]).await
                == vec![GroupDescription {
                    group: GROUP.to_owned(),
                    definition: expected,
                    cut_epochs: Vec::new(),
                    pending_epoch: None,
                }]
        );
    }

    #[tokio::test]
    async fn a_second_create_of_the_same_name_is_refused() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 4))
            .await
            .expect("the group is created");
        let again = coordinator
            .create_group(GROUP, spec(&["payments"], None, 4))
            .await;
        assert!(let Err(BarrierError::GroupExists { .. }) = again);
    }

    #[tokio::test]
    async fn a_group_that_this_broker_does_not_coordinate_is_refused() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        // Node 2 takes every state partition.
        fixture.source.set_records(
            &[
                topic_records(STATE_TOPIC, 4, NodeId(2)),
                topic_records("orders", 2, NodeId(1)),
            ]
            .concat(),
        );
        coordinator
            .refresh_leader_partitions(&fixture.source.current_image())
            .await;

        let created = coordinator
            .create_group(GROUP, spec(&["orders"], None, 4))
            .await;
        assert!(let Err(BarrierError::NotCoordinator { .. }) = created);
    }

    #[tokio::test]
    async fn an_injection_marks_every_partition_and_publishes_a_complete_cut() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders", "payments"], None, 4))
            .await
            .expect("the group is created");

        let outcome = coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        assert!(outcome.epoch == 1);
        assert!(outcome.cut.status == CutStatus::Complete);
        assert!(outcome.cut.missing.is_empty());
        assert!(
            outcome.cut.topics
                == vec![
                    TopicOffsets {
                        topic: "orders".to_owned(),
                        partitions: vec![
                            PartitionOffset {
                                partition: PartitionIndex(0),
                                offset: Offset(0),
                            },
                            PartitionOffset {
                                partition: PartitionIndex(1),
                                offset: Offset(0),
                            },
                        ],
                    },
                    TopicOffsets {
                        topic: "payments".to_owned(),
                        partitions: vec![PartitionOffset {
                            partition: PartitionIndex(0),
                            offset: Offset(0),
                        }],
                    },
                ]
        );

        // The record at every named offset is this epoch's marker.
        for topic in &outcome.cut.topics {
            for entry in &topic.partitions {
                let marker = marker_at(
                    &fixture.registry,
                    &topic.topic,
                    entry.partition.get(),
                    entry.offset,
                );
                check!(marker.map(|m| (m.group, m.epoch)) == Some((GROUP.to_owned(), 1)));
            }
        }
    }

    #[tokio::test]
    async fn every_injection_takes_the_next_epoch() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 4))
            .await
            .expect("the group is created");

        let mut epochs = Vec::new();
        for _ in 0..3 {
            epochs.push(
                coordinator
                    .trigger_injection(GROUP, None)
                    .await
                    .expect("the injection runs")
                    .epoch,
            );
        }
        assert!(epochs == vec![1, 2, 3]);
        assert!(
            coordinator.describe_groups(&[]).await[0]
                .definition
                .last_epoch
                == 3
        );
    }

    #[tokio::test]
    async fn a_partition_that_carries_no_marker_makes_the_cut_partial() {
        // Only partition 0 of `orders` is open here, so partition 1 stays
        // unmarked until the deadline runs out.
        let fixture = Fixture::with_data_partitions(&[("orders", 1)]);
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 4))
            .await
            .expect("the group is created");

        let outcome = coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        assert!(outcome.epoch == 1);
        assert!(outcome.cut.status == CutStatus::Partial);
        assert!(
            outcome.cut.missing
                == vec![MissingPartition {
                    topic: "orders".to_owned(),
                    partition: PartitionIndex(1),
                }]
        );

        // The epoch is consumed. The next injection takes epoch 2.
        let next = coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        assert!(next.epoch == 2);
    }

    #[tokio::test]
    async fn a_topic_set_edit_applies_from_the_next_epoch() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 4))
            .await
            .expect("the group is created");
        let first = coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        assert!(first.cut.topics.len() == 1);

        coordinator
            .update_group(GROUP, spec(&["orders", "payments"], None, 4))
            .await
            .expect("the group is updated");
        let second = coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        let topics: Vec<&str> = second.cut.topics.iter().map(|t| t.topic.as_str()).collect();
        assert!(topics == vec!["orders", "payments"]);
        assert!(second.epoch == 2);
    }

    #[tokio::test]
    async fn the_group_keeps_only_its_retained_cuts() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 2))
            .await
            .expect("the group is created");
        for _ in 0..4 {
            coordinator
                .trigger_injection(GROUP, None)
                .await
                .expect("the injection runs");
        }

        let epochs: Vec<i64> = coordinator
            .list_cuts(GROUP)
            .await
            .expect("the group is live")
            .iter()
            .map(|c| c.epoch)
            .collect();
        assert!(epochs == vec![3, 4]);

        // The tombstones are durable, so a replay agrees.
        let replayed = fixture.recovered().await;
        let after: Vec<i64> = replayed
            .list_cuts(GROUP)
            .await
            .expect("the group is live")
            .iter()
            .map(|c| c.epoch)
            .collect();
        assert!(after == vec![3, 4]);
    }

    #[tokio::test]
    async fn a_smaller_retention_drops_every_cut_below_the_new_window() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 8))
            .await
            .expect("the group is created");
        for _ in 0..4 {
            coordinator
                .trigger_injection(GROUP, None)
                .await
                .expect("the injection runs");
        }
        coordinator
            .update_group(GROUP, spec(&["orders"], None, 1))
            .await
            .expect("the group is updated");
        coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");

        let epochs: Vec<i64> = coordinator
            .list_cuts(GROUP)
            .await
            .expect("the group is live")
            .iter()
            .map(|c| c.epoch)
            .collect();
        assert!(epochs == vec![5]);

        let replayed = fixture.recovered().await;
        let after: Vec<i64> = replayed
            .list_cuts(GROUP)
            .await
            .expect("the group is live")
            .iter()
            .map(|c| c.epoch)
            .collect();
        assert!(after == vec![5]);
    }

    #[tokio::test]
    async fn recovery_rebuilds_the_group_and_its_cuts() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders", "payments"], None, 8))
            .await
            .expect("the group is created");
        let first = coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        let second = coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");

        let replayed = fixture.recovered().await;
        assert!(
            replayed.describe_groups(&[]).await
                == vec![GroupDescription {
                    group: GROUP.to_owned(),
                    definition: GroupValue {
                        topics: vec!["orders".to_owned(), "payments".to_owned()],
                        interval: None,
                        retained_cuts: 8,
                        last_epoch: 2,
                    },
                    cut_epochs: vec![1, 2],
                    pending_epoch: None,
                }]
        );
        assert!(
            replayed.list_cuts(GROUP).await.expect("the group is live")
                == vec![
                    RetainedCut {
                        epoch: 1,
                        cut: first.cut,
                    },
                    RetainedCut {
                        epoch: 2,
                        cut: second.cut,
                    },
                ]
        );

        // The recovered coordinator allocates the next epoch, never a used one.
        let third = replayed
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        assert!(third.epoch == 3);
    }

    #[tokio::test]
    async fn recovery_finalises_an_interrupted_injection_as_partial() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 4))
            .await
            .expect("the group is created");

        // A coordinator that crashed after the injection-start record leaves
        // exactly this behind.
        let start = InjectionStartValue {
            coordinator_epoch: 3,
            triggered_at: 1_000,
            targets: vec![crate::barrier::persistence::TopicTarget {
                topic: "orders".to_owned(),
                partition_count: 2,
            }],
        };
        coordinator
            .append_records(
                GROUP,
                vec![(
                    RecordKey::injection_start(GROUP, 1),
                    Some(encode_injection_start(&start).into()),
                )],
            )
            .await
            .expect("the injection-start record lands");

        let replayed = fixture.recovered().await;
        let cuts = replayed.list_cuts(GROUP).await.expect("the group is live");
        assert!(cuts.len() == 1);
        assert!(cuts[0].epoch == 1);
        assert!(cuts[0].cut.status == CutStatus::Partial);
        assert!(cuts[0].cut.triggered_at == 1_000);
        assert!(
            cuts[0].cut.missing
                == vec![
                    MissingPartition {
                        topic: "orders".to_owned(),
                        partition: PartitionIndex(0),
                    },
                    MissingPartition {
                        topic: "orders".to_owned(),
                        partition: PartitionIndex(1),
                    },
                ]
        );
        assert!(
            replayed.describe_groups(&[]).await[0]
                .pending_epoch
                .is_none()
        );

        // The epoch is consumed and it is never reused.
        let next = replayed
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        assert!(next.epoch == 2);
    }

    #[tokio::test]
    async fn a_deleted_group_leaves_no_state_behind() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 4))
            .await
            .expect("the group is created");
        coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");

        coordinator
            .delete_group(GROUP)
            .await
            .expect("the group is deleted");
        assert!(coordinator.describe_groups(&[]).await.is_empty());
        assert!(let Err(BarrierError::UnknownGroup { .. }) = coordinator.list_cuts(GROUP).await);

        let replayed = fixture.recovered().await;
        assert!(replayed.describe_groups(&[]).await.is_empty());
    }

    #[tokio::test]
    async fn deleting_a_group_that_is_not_there_is_refused() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        assert!(let Err(BarrierError::UnknownGroup { .. }) = coordinator.delete_group(GROUP).await);
        assert!(
            let Err(BarrierError::UnknownGroup { .. }) =
                coordinator.trigger_injection(GROUP, None).await
        );
    }

    #[tokio::test]
    async fn the_scheduler_injects_only_a_group_whose_interval_elapsed() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], Some(millis(1_000)), 4))
            .await
            .expect("the group is created");
        coordinator
            .create_group("on-demand", spec(&["payments"], None, 4))
            .await
            .expect("the group is created");

        assert!(coordinator.run_due_injections(0).await.is_empty());

        let due = coordinator.run_due_injections(now_ms() + 2_000).await;
        assert!(due == vec![GROUP.to_owned()]);
        assert!(
            coordinator
                .list_cuts("on-demand")
                .await
                .expect("the group is live")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn an_injection_holds_the_group_against_a_second_caller() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 4))
            .await
            .expect("the group is created");

        let handle = coordinator
            .live_entry(GROUP)
            .expect("the group entry is there");
        let held = handle.lock().await;
        let refused = coordinator.trigger_injection(GROUP, None).await;
        assert!(let Err(BarrierError::InjectionInProgress { .. }) = refused);
        drop(held);

        assert!(coordinator.trigger_injection(GROUP, None).await.is_ok());
    }

    #[tokio::test]
    async fn the_state_partition_of_a_group_carries_every_record_of_that_group() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(&["orders"], None, 4))
            .await
            .expect("the group is created");
        coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");

        let index = coordinator.state_partition_for(GROUP);
        let partition = fixture
            .registry
            .get(STATE_TOPIC, index)
            .expect("the state partition is open");
        let read = partition
            .read_log(Offset(0), krabka_units::mebibytes(1))
            .expect("read the log back");
        let mut kinds = Vec::new();
        for batch in &read.batches {
            for record in &batch.records {
                let key = decode_key(record.key.as_ref().expect("every record has a key"))
                    .expect("the key decodes");
                check!(key.group == GROUP);
                kinds.push(key.kind);
            }
        }
        // Group, injection start, cut, group again, then the retirement of the
        // injection-start record.
        assert!(
            kinds
                == vec![
                    RecordKind::Group,
                    RecordKind::InjectionStart,
                    RecordKind::Cut,
                    RecordKind::Group,
                    RecordKind::InjectionStart,
                ]
        );
    }
}
