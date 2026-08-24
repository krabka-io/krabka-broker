//! `RemoteLogManager`: the KIP-405 tiered-storage copy path.
//!
//! Every `interval`, the manager walks the partition registry. For each
//! partition where this broker is the leader and the topic has
//! `remote.storage.enable=true`, it copies the partition's sealed log
//! segments that are not yet in the remote tier to a
//! [`RemoteStorageManager`]. It records each copy in a
//! [`RemoteLogMetadataManager`] (`CopySegmentStarted` →
//! `CopySegmentFinished`).
//!
//! This is the copy path. Their own modules implement local-retention deletion
//! of copied segments and the remote read path on `Fetch`. The
//! remote-storage SPIs are blocking, so each copy and each delete
//! runs on the `tokio` blocking pool.

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use crabka_log::{LogConfig, Offset, SegmentExport};
use crabka_metadata::NodeId;
use crabka_remote_storage::{
    ChainStamp, EpochId, LogSegmentData, RemoteLogMetadataManager, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState,
    RemotePartitionDeleteMetadata, RemotePartitionDeleteState, RemoteStorageManager,
    TopicIdPartition, WormChainRecord, next_chain_stamp,
};
use crabka_units::{
    ByteSize, Time, bytes,
    convert::{ByteSizeExt as _, TimeExt as _},
    secs,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{partition::Partition, partition_registry::PartitionRegistry};

/// Default cadence of the tiered-storage sweep (copy and retention passes).
const DEFAULT_TIERING_INTERVAL: Time = secs(30);

/// The floor of every size-budget walk in this module.
const NO_BYTES: ByteSize = bytes(0);

/// Whether the remote tier this partition writes to is a write-once archive.
///
/// KIP-405 remote retention deletes segments the tier still holds. A WORM
/// archive cannot honour that and must not be asked to: the eviction set is
/// empty, so the pass never reaches an RSM delete the backend would refuse
/// and the bucket policy would reject anyway.
///
/// A two-variant enum rather than a `bool`, because the retention helpers
/// already take several positional arguments and a bare flag among them is
/// exactly the transposition the style guide's newtype rule targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveMode {
    /// An ordinary tiered-storage backend. Retention may delete what it wrote.
    Mutable,
    /// A write-once archive. Every object written stays written, and every
    /// segment copy is sealed into a chained, verifiable manifest.
    WriteOnce,
}

impl ArchiveMode {
    /// The mode a broker's WORM setting implies: a
    /// [`WormConfig`](crabka_remote_storage::WormConfig) makes the tier
    /// write-once, and its absence leaves it mutable.
    pub(crate) const fn from_worm(worm: Option<&crabka_remote_storage::WormConfig>) -> Self {
        match worm {
            Some(_) => Self::WriteOnce,
            None => Self::Mutable,
        }
    }
}

/// Where a copy's manifest joins its partition's WORM chain, or that the tier
/// keeps no chain at all.
///
/// A copy into a write-once archive **must** carry a chain stamp: an unstamped
/// copy uploads every object and only then fails with
/// [`WormError::MissingChainStamp`](crabka_remote_storage::WormError::MissingChainStamp),
/// leaving orphans that nothing can ever collect. Pairing the mode and the
/// stamp in one value, instead of passing an [`ArchiveMode`] beside an
/// `Option<ChainStamp>`, makes that combination unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainPosition {
    /// Mutable tier: the copy stamps nothing.
    Unchained,
    /// Write-once archive: the next manifest joins the chain here.
    At(ChainStamp),
}

impl ChainPosition {
    /// The position a partition's next copy takes, given every segment the
    /// metadata manager currently holds for it.
    ///
    /// `listed` is the copy pass's own RLMM listing, reused rather than
    /// re-fetched: seeding the chain must not cost a second list per segment.
    /// The fresh epoch id only survives when no receipt in `listed` does, so
    /// [`next_chain_stamp`] takes it as an argument and stays pure.
    fn seed(archive: ArchiveMode, listed: &[RemoteLogSegmentMetadata]) -> Self {
        match archive {
            ArchiveMode::Mutable => Self::Unchained,
            ArchiveMode::WriteOnce => Self::At(next_chain_stamp(listed, EpochId(Uuid::new_v4()))),
        }
    }

    /// The archive mode this position belongs to: a stamp exists exactly when
    /// the tier is write-once.
    const fn archive(self) -> ArchiveMode {
        match self {
            Self::Unchained => ArchiveMode::Mutable,
            Self::At(_) => ArchiveMode::WriteOnce,
        }
    }
}

/// What one [`copy_one`] attempt left behind.
#[derive(Debug)]
enum CopyOutcome {
    /// The segment reached `CopySegmentFinished`.
    Copied {
        /// Where the next copy in this tick joins the chain. Carried out of
        /// the copy so consecutive segments chain without re-listing the RLMM.
        next: ChainPosition,
    },
    /// The attempt did not reach `CopySegmentFinished`. The caller keeps its
    /// chain position and the next tick retries the segment.
    Failed,
}

/// Tunables for [`run`].
#[derive(Debug, Clone)]
pub(crate) struct RemoteLogManagerConfig {
    pub interval: Time,
}

impl Default for RemoteLogManagerConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_TIERING_INTERVAL,
        }
    }
}

pub(crate) struct RemoteLogManagerContext {
    pub partitions: Arc<PartitionRegistry>,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    /// Whether `rsm` is a write-once archive. It gates every delete this
    /// module would otherwise issue, and turns on manifest chaining.
    pub archive: ArchiveMode,
    pub rsm: Arc<dyn RemoteStorageManager>,
    pub rlmm: Arc<dyn RemoteLogMetadataManager>,
    pub node_id: NodeId,
    pub broker_id: i32,
}

/// Spawned task entry point. Ticks every `cfg.interval` until `shutdown`.
// task dependencies; bundling would obscure them
pub(crate) async fn run(
    context: RemoteLogManagerContext,
    cfg: RemoteLogManagerConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.interval.to_std());
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                debug!("remote-log-manager task shutting down");
                return;
            }
        }
        tick_all(
            &context.partitions,
            &*context.controller,
            context.archive,
            &context.rsm,
            &context.rlmm,
            context.node_id,
            context.broker_id,
        )
        .await;
    }
}

async fn tick_all(
    partitions: &PartitionRegistry,
    controller: &dyn crate::metadata_source::MetadataSource,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    node_id: NodeId,
    broker_id: i32,
) {
    // Snapshot first to avoid holding any registry guard across an await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    let image = controller.current_image();
    for partition in snapshot {
        if partition.current_leader.load(Ordering::Relaxed) != node_id {
            continue;
        }
        // Read config + sealed-segment list under the log lock, then drop it.
        let (log_config, exports) = {
            let log = partition.log.lock().expect("log mutex poisoned");
            let cfg = log.config_snapshot();
            if !cfg.remote_storage_enable {
                continue;
            }
            (cfg, log.tierable_segments())
        };
        if exports.is_empty() {
            continue;
        }
        let Some(topic_id) = image.topic(&partition.topic).map(|t| t.topic_id) else {
            // Topic vanished from the metadata image between snapshots; skip.
            continue;
        };
        // Atomic stores the raw epoch; wrap for the remote-storage metadata seam.
        let leader_epoch =
            crabka_ids::LeaderEpoch(partition.current_leader_epoch.load(Ordering::Acquire));
        let tp = TopicIdPartition::new(topic_id, partition.topic.clone(), partition.index.get());
        copy_eligible(
            &tp,
            broker_id,
            leader_epoch,
            exports.clone(),
            archive,
            rsm,
            rlmm,
        )
        .await;
        // Local retention is deliberately not gated on `archive`: evicting a
        // local segment that the archive already holds is the whole point of
        // tiering, and it deletes nothing from the remote tier.
        local_retention_pass(&tp, &partition, &exports, &log_config, rlmm, now_ms());
        remote_retention_pass(&tp, broker_id, &log_config, archive, rsm, rlmm, now_ms()).await;
    }
}

/// Copy every sealed segment in `exports` that the metadata store does not
/// already know about. Returns the number of segments newly copied to
/// `CopySegmentFinished`. This is a separate function from [`tick_all`] so
/// that tests can drive it directly against a real `Log` and a reference
/// RSM/RLMM.
pub(crate) async fn copy_eligible(
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: crabka_ids::LeaderEpoch,
    exports: Vec<SegmentExport>,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> usize {
    let listed = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments");
            return 0;
        }
    };

    // Only a *finished* copy claims a base offset.
    //
    // This skip set used to key on every state. A segment left in
    // `CopySegmentStarted` by a failed copy therefore claimed its offset
    // forever and was never retried. On a mutable tier `rollback` erased that
    // metadata, so the bug stayed hidden; a write-once archive keeps it, and
    // tiering for that offset would stop silently and permanently. A `Delete*`
    // segment does not claim its offset either: its bytes are on the way out,
    // so a still-local segment at the same base is copyable again.
    let mut known: HashSet<i64> = HashSet::new();
    for md in &listed {
        match md.state() {
            RemoteLogSegmentState::CopySegmentFinished => {
                known.insert(md.start_offset());
            }
            // This listing is taken once per tick, so anything already in
            // `CopySegmentStarted` was left there by an earlier tick.
            RemoteLogSegmentState::CopySegmentStarted => {
                warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                      segment = %md.remote_log_segment_id().id,
                      "remote-log-manager: segment still in CopySegmentStarted after an \
                       earlier tick; re-copying it under a fresh segment id");
            }
            RemoteLogSegmentState::DeleteSegmentStarted
            | RemoteLogSegmentState::DeleteSegmentFinished => {}
        }
    }

    let mut chain = ChainPosition::seed(archive, &listed);
    let mut copied = 0;
    for ex in exports {
        if known.contains(&ex.base_offset.0) {
            continue;
        }
        // Each success hands back the next chain position, so a run of
        // consecutive segments chains inside one tick with no further listing.
        if let CopyOutcome::Copied { next } =
            copy_one(tp, broker_id, leader_epoch, &ex, chain, rsm, rlmm).await
        {
            copied += 1;
            chain = next;
        }
    }
    copied
}

/// Compute the highest `target` to pass to
/// [`crabka_log::Log::delete_local_segments_through`] given the
/// partition's local sealed-segment exports and the per-topic
/// local-retention settings. Returns `None` when nothing is deletable.
///
/// A segment is eligible if and only if its `base_offset` is in
/// `finished_bases`, that is, `CopySegmentFinished` in the RLMM, AND it meets
/// either time-based eviction (`now_ms - seg.max_timestamp > effective_local`)
/// or size-based eviction (oldest-first until the sealed total fits
/// `effective_local_size`). The walk stops at the first non-finished
/// segment, so the local prefix stays contiguous. This matches Kafka.
///
/// Size-based eviction ignores the active segment. Operators set
/// local.retention.bytes in MB or GB ranges, where the active segment,
/// bounded by `segment.bytes`, is negligible.
pub(crate) fn local_retention_target(
    exports: &[SegmentExport],
    finished_bases: &HashSet<i64>,
    effective_local: Option<Time>,
    effective_local_size: Option<ByteSize>,
    now_ms: i64,
) -> Option<i64> {
    let sealed_total: ByteSize = exports
        .iter()
        .map(|e| e.size)
        .fold(NO_BYTES, |acc, size| acc + size);
    let mut deletable_size_remaining =
        effective_local_size.map_or(NO_BYTES, |budget| (sealed_total - budget).max(NO_BYTES));

    let mut delete_through_last: Option<i64> = None;
    for ex in exports {
        if !finished_bases.contains(&ex.base_offset.0) {
            break;
        }
        let age = Time::from_millis(now_ms.saturating_sub(ex.max_timestamp));
        let by_time = matches!(effective_local, Some(retention) if age > retention);
        let by_size = deletable_size_remaining > NO_BYTES;
        if !(by_time || by_size) {
            break;
        }
        delete_through_last = Some(ex.last_offset.0);
        if by_size {
            deletable_size_remaining = (deletable_size_remaining - ex.size).max(NO_BYTES);
        }
    }

    delete_through_last.map(|last| last + 1)
}

/// After the copy pass, drop local sealed segments whose
/// remote copy is `CopySegmentFinished` and that fall outside the
/// per-topic local-retention window. Returns the count of segments
/// that this pass physically removed from disk.
pub(crate) fn local_retention_pass(
    tp: &TopicIdPartition,
    partition: &Partition,
    exports: &[SegmentExport],
    log_config: &LogConfig,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    now_ms: i64,
) -> usize {
    let effective_local = log_config.local_retention.or(log_config.retention);
    let effective_local_size = log_config
        .local_retention_size
        .or(log_config.retention_size);

    let finished_bases: HashSet<i64> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments for local retention");
            return 0;
        }
    };

    let Some(target) = local_retention_target(
        exports,
        &finished_bases,
        effective_local,
        effective_local_size,
        now_ms,
    ) else {
        return 0;
    };

    let result = {
        let mut log = partition.log.lock().expect("log mutex poisoned");
        log.delete_local_segments_through(Offset(target))
    };
    match result {
        Ok(n) => {
            debug!(topic = %tp.topic, partition = tp.partition, target, removed = n,
                   "remote-log-manager: local-retention deletion pass completed");
            n
        }
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, target, error = %e,
                  "remote-log-manager: failed to delete local segments");
            0
        }
    }
}

/// KIP-405: compute the set of finished remote segments whose
/// total-retention window has expired, by time or by size budget, in
/// oldest-first order. Mirrors [`local_retention_target`]'s walk. It **stops at
/// the first non-deletable segment**, so the remaining remote prefix stays
/// contiguous. This matches Kafka.
///
/// A segment is deletable when either:
/// - `now_ms - md.max_timestamp_ms > retention`, or
/// - the running sum of sizes from the oldest forward must exceed
///   `total - retention_size` (greedy size eviction).
///
/// A `None` setting disables that axis. The caller must already have filtered
/// to `CopySegmentFinished` and sorted by `start_offset`.
///
/// [`ArchiveMode::WriteOnce`] evicts nothing, whatever the topic's retention
/// settings say: remote retention is a delete, and a write-once archive has
/// none to give.
pub(crate) fn remote_retention_eviction_set(
    archive: ArchiveMode,
    finished: &[RemoteLogSegmentMetadata],
    retention: Option<Time>,
    retention_size: Option<ByteSize>,
    now_ms: i64,
) -> Vec<RemoteLogSegmentMetadata> {
    if archive == ArchiveMode::WriteOnce {
        return Vec::new();
    }
    let total: ByteSize = finished
        .iter()
        .map(segment_size)
        .fold(NO_BYTES, |acc, size| acc + size);
    let mut size_to_reclaim =
        retention_size.map_or(NO_BYTES, |budget| (total - budget).max(NO_BYTES));
    let mut out = Vec::new();
    for md in finished {
        let age = Time::from_millis(now_ms.saturating_sub(md.max_timestamp_ms()));
        let by_time = matches!(retention, Some(window) if age > window);
        let by_size = size_to_reclaim > NO_BYTES;
        if !(by_time || by_size) {
            break;
        }
        if by_size {
            size_to_reclaim = (size_to_reclaim - segment_size(md)).max(NO_BYTES);
        }
        out.push(md.clone());
    }
    out
}

/// The remote metadata's `segment_size_in_bytes` (a wire `int32`) as a
/// quantity. Negative sizes are impossible but cheap to clamp.
fn segment_size(md: &RemoteLogSegmentMetadata) -> ByteSize {
    ByteSize::from_bytes_i64(i64::from(md.segment_size_in_bytes().max(0)))
}

/// KIP-405: evict remote segments past the topic's total
/// retention window (`retention.ms` and `retention.bytes`). For each
/// deletable segment, it runs the lifecycle
/// `CopySegmentFinished` → `DeleteSegmentStarted` → RSM delete →
/// `DeleteSegmentFinished`. A failure logs at WARN and ends the
/// partition's pass early. Leftover `DeleteSegmentStarted` metadata is
/// invisible to the read path's finished-only filter, and the next tick
/// retries it. Returns the count of segments that reached
/// `DeleteSegmentFinished`, that is, the successfully evicted ones.
///
/// Under [`ArchiveMode::WriteOnce`] the pass returns `0` before it lists
/// anything, so a partition on a write-once archive costs a 30-second tick
/// nothing at all.
pub(crate) async fn remote_retention_pass(
    tp: &TopicIdPartition,
    broker_id: i32,
    log_config: &LogConfig,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    now_ms: i64,
) -> usize {
    if archive == ArchiveMode::WriteOnce {
        return 0;
    }
    let retention = log_config.retention;
    let retention_size = log_config.retention_size;
    if retention.is_none() && retention_size.is_none() {
        return 0;
    }

    let mut finished: Vec<RemoteLogSegmentMetadata> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments for retention");
            return 0;
        }
    };
    finished.sort_by_key(RemoteLogSegmentMetadata::start_offset);

    let evict =
        remote_retention_eviction_set(archive, &finished, retention, retention_size, now_ms);
    let mut deleted = 0;
    for md in evict {
        if delete_one_segment(tp, broker_id, &md, archive, rsm, rlmm).await {
            deleted += 1;
        } else {
            // Stop at the first failure to preserve the contiguous-prefix
            // invariant — the next tick re-tries from the same base.
            break;
        }
    }
    deleted
}

/// KIP-405: cascade the
/// [`DeletePartitionMarked` → `DeletePartitionStarted` →
/// `DeletePartitionFinished`] lifecycle for `tp`, and delete every remote
/// segment along the way. The `DeleteTopics` handler runs this as a detached
/// task, so the response does not wait on remote-tier I/O. A failure logs
/// at WARN. Leftover `DeleteSegmentStarted` segments are harmless in the
/// in-memory RLMM, because a `DeleteTopics`-recreate combination regenerates
/// the topic id and the new partition is a fresh `TopicIdPartition`.
///
/// # A write-once archive keeps every byte
///
/// Under [`ArchiveMode::WriteOnce`] the cascade still walks
/// `DeletePartitionMarked` → `DeletePartitionStarted` →
/// `DeletePartitionFinished`, and still clears the partition's segment
/// metadata, but it removes nothing from the archive. Deleting a Kafka topic
/// is a cluster operation; it is not, and must not become, an instruction to
/// erase a compliance archive. The archived segments and their manifests
/// outlive the topic, and the verifier reads them without any broker.
pub(crate) async fn cascade_remote_partition_delete(
    tp: TopicIdPartition,
    broker_id: i32,
    archive: ArchiveMode,
    rsm: Arc<dyn RemoteStorageManager>,
    rlmm: Arc<dyn RemoteLogMetadataManager>,
) {
    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionMarked,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to mark partition deleted");
        return;
    }
    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionStarted,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to start partition delete");
        return;
    }

    let segments = match rlmm.list_remote_log_segments(&tp) {
        Ok(list) => list,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list segments for partition delete");
            return;
        }
    };
    for md in segments {
        // Skip segments already past `DeleteSegmentStarted` (no-op delete).
        if md.state() == RemoteLogSegmentState::DeleteSegmentFinished {
            continue;
        }
        let _ = delete_one_segment(&tp, broker_id, &md, archive, &rsm, &rlmm).await;
    }

    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionFinished,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to finish partition delete");
    }
}

/// Run one blocking [`RemoteLogMetadataManager`] mutation on the blocking
/// pool. The topic-backed manager's synchronous SPI methods bridge to a
/// Tokio runtime with `block_on`, which panics on a runtime worker thread.
/// `spawn_blocking` gives them a thread that is allowed to block. For the
/// in-memory manager the closure is a cheap no-op there.
/// This mirrors the `spawn_blocking` wrapping that this module already uses
/// for the blocking [`RemoteStorageManager`] SPI.
async fn rlmm_mutate<F>(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    op: F,
) -> Result<(), crabka_remote_storage::RemoteStorageError>
where
    F: FnOnce(
            &dyn RemoteLogMetadataManager,
        ) -> Result<(), crabka_remote_storage::RemoteStorageError>
        + Send
        + 'static,
{
    let rlmm = Arc::clone(rlmm);
    match tokio::task::spawn_blocking(move || op(rlmm.as_ref())).await {
        Ok(res) => res,
        Err(e) => Err(crabka_remote_storage::RemoteStorageError::Backend(format!(
            "RLMM mutation task panicked: {e}"
        ))),
    }
}

async fn put_partition_state(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    tp: &TopicIdPartition,
    state: RemotePartitionDeleteState,
    broker_id: i32,
) -> Result<(), crabka_remote_storage::RemoteStorageError> {
    let md = RemotePartitionDeleteMetadata {
        topic_id_partition: tp.clone(),
        state,
        event_timestamp_ms: now_ms(),
        broker_id,
    };
    rlmm_mutate(rlmm, move |m| m.put_remote_partition_delete_metadata(md)).await
}

/// Drive one `CopySegmentFinished` (or in-flight) segment through the
/// `DeleteSegmentStarted` → RSM delete → `DeleteSegmentFinished` chain.
/// Returns `true` when the lifecycle completes cleanly. Shared by
/// [`remote_retention_pass`] and [`cascade_remote_partition_delete`].
///
/// Under [`ArchiveMode::WriteOnce`] the RSM delete is skipped outright and
/// only the metadata lifecycle advances. Calling it would fail — the backend
/// refuses every delete, and the bucket's object-lock policy refuses it under
/// that — so the skip is what keeps a routine pass from logging an error every
/// tick.
async fn delete_one_segment(
    tp: &TopicIdPartition,
    broker_id: i32,
    md: &RemoteLogSegmentMetadata,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> bool {
    let id = md.remote_log_segment_id().clone();
    // Transition to DeleteSegmentStarted unless the segment is already
    // there (cascade may retry against a partially-cleaned partition).
    if md.state() == RemoteLogSegmentState::CopySegmentFinished {
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id.clone(),
            event_timestamp_ms: now_ms(),
            custom_metadata: None,
            state: RemoteLogSegmentState::DeleteSegmentStarted,
            broker_id,
        };
        if let Err(e) = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await
        {
            warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                  error = %e,
                  "remote-log-manager: failed to record DeleteSegmentStarted");
            return false;
        }
    }

    match archive {
        ArchiveMode::Mutable => {
            // RSM delete (blocking).
            let rsm_del = rsm.clone();
            let md_del = md.clone();
            let delete_result =
                tokio::task::spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del)).await;
            match delete_result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                          error = %e, "remote-log-manager: RSM delete failed");
                    return false;
                }
                Err(e) => {
                    warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                          error = %e, "remote-log-manager: RSM delete task panicked");
                    return false;
                }
            }
        }
        // DEBUG and not WARN on purpose: deleting a topic with ten thousand
        // archived segments would otherwise emit ten thousand warnings for
        // behavior that is working exactly as configured.
        ArchiveMode::WriteOnce => {
            debug!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                   worm_retained = true,
                   "remote-log-manager: retaining remote segment data; the tier is a \
                    write-once archive");
        }
    }

    let upd = RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: id,
        event_timestamp_ms: now_ms(),
        custom_metadata: None,
        state: RemoteLogSegmentState::DeleteSegmentFinished,
        broker_id,
    };
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await {
        warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
              error = %e, "remote-log-manager: failed to record DeleteSegmentFinished");
        return false;
    }
    debug!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
           worm_retained = archive == ArchiveMode::WriteOnce,
           "remote-log-manager: remote segment reached DeleteSegmentFinished");
    true
}

/// Copy one sealed segment through the full `Started` → `Finished`
/// lifecycle. On any failure, this function deletes the partial remote data
/// and drops the metadata (`DeleteSegmentStarted` → `DeleteSegmentFinished`),
/// so the next tick retries the segment; see [`rollback`] for the part of
/// that a write-once archive cannot do.
///
/// `chain` decides whether the copy is stamped for a WORM manifest. When it
/// is, the stamp goes on before the copy, so the `CopySegmentStarted` record
/// already says where the manifest was meant to sit, and a durable metadata
/// manager shows that even if the broker dies mid-copy.
async fn copy_one(
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: crabka_ids::LeaderEpoch,
    ex: &SegmentExport,
    chain: ChainPosition,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> CopyOutcome {
    let id = RemoteLogSegmentId::new(tp.clone(), Uuid::new_v4());
    // Unwrap the log-layer `Offset`s into the remote-storage metadata's `i64`
    // world at the seam; the epoch map keeps its `LeaderEpoch` keys, which
    // `RemoteLogSegmentMetadata` carries verbatim.
    let epochs: BTreeMap<crabka_ids::LeaderEpoch, i64> = if ex.leader_epochs.is_empty() {
        BTreeMap::from([(
            crabka_ids::LeaderEpoch(leader_epoch.0.max(0)),
            ex.base_offset.0,
        )])
    } else {
        ex.leader_epochs
            .iter()
            .map(|&(epoch, off)| (epoch, off.0))
            .collect()
    };
    let size = ex.size.bytes_i32();

    let metadata = match RemoteLogSegmentMetadata::new(
        id.clone(),
        ex.base_offset.0,
        ex.last_offset.0,
        ex.max_timestamp,
        broker_id,
        now_ms(),
        crabka_remote_storage::RemoteLogSegmentDetails::new(
            size,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs.clone(),
        ),
    ) {
        Ok(m) => m,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: skipping segment with invalid metadata");
            return CopyOutcome::Failed;
        }
    };
    // KIP-405 txnIndexEmpty: set true when the log segment has no transaction
    // index file (non-transactional topics or segments written before txn support).
    let metadata = if ex.transaction_index_path.is_none() {
        metadata.with_txn_index_empty(true)
    } else {
        metadata
    };
    // The chain stamp goes on before the copy: a WORM backend refuses to seal
    // an unstamped manifest, and it refuses only *after* uploading every
    // object, which would leave orphans in a bucket that takes nothing back.
    let metadata = match chain {
        ChainPosition::Unchained => metadata,
        ChainPosition::At(stamp) => {
            metadata.with_custom_metadata(WormChainRecord::request(stamp).to_custom_metadata())
        }
    };

    let md_started = metadata.clone();
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.add_remote_log_segment_metadata(md_started)).await
    {
        warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
              error = %e, "remote-log-manager: failed to record CopySegmentStarted");
        return CopyOutcome::Failed;
    }

    let data = LogSegmentData {
        log_segment: ex.log_path.clone(),
        offset_index: ex.offset_index_path.clone(),
        time_index: ex.time_index_path.clone(),
        transaction_index: ex.transaction_index_path.clone(),
        producer_snapshot_index: Some(ex.producer_snapshot_path.clone()),
        leader_epoch_index: leader_epoch_index_bytes(&epochs),
    };

    // The RSM is a blocking SPI — run the copy on the blocking pool.
    let rsm_copy = rsm.clone();
    let md_copy = metadata.clone();
    let copy_result =
        tokio::task::spawn_blocking(move || rsm_copy.copy_log_segment_data(&md_copy, &data)).await;

    // Copy failed (or the blocking task panicked): clean up so the segment
    // is retried next tick.
    let returned = match copy_result {
        Ok(Ok(returned)) => returned,
        Ok(Err(e)) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: segment copy failed");
            rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
            return CopyOutcome::Failed;
        }
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: segment copy task panicked");
            rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
            return CopyOutcome::Failed;
        }
    };

    // A write-once copy is only complete once the backend hands back a receipt
    // carrying the head its manifest produced. Without one the objects landed
    // with no verifiable manifest over them, so the segment must not be marked
    // finished: the read path serves every finished segment, and it would then
    // be serving unattested data. Leaving it in `CopySegmentStarted` is what
    // makes the next tick retry it under a fresh segment id.
    let next = match chain {
        ChainPosition::Unchained => ChainPosition::Unchained,
        ChainPosition::At(_) => {
            let Some(stamp) = returned
                .as_ref()
                .and_then(|custom| WormChainRecord::from_custom_metadata(custom).ok())
                .and_then(|receipt| receipt.next_stamp())
            else {
                error!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                       "remote-log-manager: write-once copy returned no chain receipt; \
                        leaving the segment in CopySegmentStarted rather than serving \
                        unattested data");
                return CopyOutcome::Failed;
            };
            ChainPosition::At(stamp)
        }
    };

    let upd = RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: id,
        event_timestamp_ms: now_ms(),
        // The backend's receipt is the chain position a restart reads back, so
        // it has to be durable alongside the segment, not dropped here.
        custom_metadata: returned,
        state: RemoteLogSegmentState::CopySegmentFinished,
        broker_id,
    };
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await {
        warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
              error = %e, "remote-log-manager: failed to record CopySegmentFinished");
        return CopyOutcome::Failed;
    }
    debug!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
           end = ex.last_offset.0, "remote-log-manager: copied segment to remote tier");
    CopyOutcome::Copied { next }
}

/// Delete partial remote data and drop the metadata after a failed copy.
///
/// # A write-once archive keeps its partial objects
///
/// Under [`ArchiveMode::WriteOnce`] the RSM delete is skipped and only the
/// metadata is dropped. Whatever objects the failed copy managed to write stay
/// in the archive for good, because the backend refuses every delete and the
/// bucket policy refuses it under that. They are inert — the copy never sealed
/// a manifest, so no chain references them — and the retry runs under a fresh
/// segment UUID, so its keys cannot collide with theirs. This residue is
/// exactly what the WORM verifier reports as `orphan_objects`: unreferenced
/// bytes are the standing cost of a tier that can take nothing back.
async fn rollback(
    metadata: &RemoteLogSegmentMetadata,
    broker_id: i32,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) {
    let id = metadata.remote_log_segment_id().clone();
    match archive {
        ArchiveMode::Mutable => {
            let rsm_del = rsm.clone();
            let md_del = metadata.clone();
            let _ =
                tokio::task::spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del)).await;
        }
        ArchiveMode::WriteOnce => {
            debug!(topic = %id.topic_id_partition.topic,
                   partition = id.topic_id_partition.partition,
                   base = metadata.start_offset(), worm_retained = true,
                   "remote-log-manager: leaving a failed copy's objects in the write-once \
                    archive; the verifier reports them as orphans");
        }
    }
    for state in [
        RemoteLogSegmentState::DeleteSegmentStarted,
        RemoteLogSegmentState::DeleteSegmentFinished,
    ] {
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id.clone(),
            event_timestamp_ms: now_ms(),
            custom_metadata: None,
            state,
            broker_id,
        };
        let _ = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await;
    }
}

/// Serialize a segment's leader-epoch map into Kafka's
/// `leader-epoch-checkpoint` text format (the bytes carried as
/// `LogSegmentData.leader_epoch_index`).
fn leader_epoch_index_bytes(epochs: &BTreeMap<crabka_ids::LeaderEpoch, i64>) -> Bytes {
    use std::fmt::Write as _;
    let mut s = String::from("0\n");
    let _ = writeln!(s, "{}", epochs.len());
    for (epoch, start) in epochs {
        // On-disk `leader-epoch-checkpoint` text format: unwrap to the raw
        // `i32` so the serialized bytes stay byte-identical.
        let _ = writeln!(s, "{} {start}", epoch.0);
    }
    Bytes::from(s.into_bytes())
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use assert2::{assert, check};
    use crabka_ids::{LeaderEpoch, PartitionIndex};
    use crabka_log::{Log, LogConfig};
    use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    use crabka_protocol::records::{Record, RecordBatch};
    use crabka_remote_storage::{
        ChainHead, CustomMetadata, IndexType, InmemoryRemoteLogMetadataManager, LocalTieredStorage,
        ManifestSeq, ObjectEntry, RemoteStorageError, Sha256Digest, WormArchiver,
    };
    use crabka_units::{hours, millis};

    use super::*;

    /// An RSM whose copy always fails, but whose delete succeeds. The tests
    /// use it to exercise the failure rollback path.
    struct AlwaysFailRsm;

    impl RemoteStorageManager for AlwaysFailRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::InvalidArgument("boom".into()))
        }
        fn fetch_log_segment(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _index_type: IndexType,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn delete_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    /// An RSM whose copy always succeeds, handing back `receipt` verbatim,
    /// and whose delete always succeeds. It touches no files, so tests can
    /// drive it with synthetic exports.
    struct AcceptingRsm {
        receipt: Option<CustomMetadata>,
    }

    impl RemoteStorageManager for AcceptingRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Ok(self.receipt.clone())
        }
        fn fetch_log_segment(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _index_type: IndexType,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn delete_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    /// Records the metadata every copy hands the backend, then fails the copy.
    /// Tests use it to see what the broker stamped on a segment before the
    /// upload, which is the only moment that stamp is observable.
    #[derive(Default)]
    struct CapturingRsm {
        seen: Mutex<Vec<RemoteLogSegmentMetadata>>,
    }

    impl RemoteStorageManager for CapturingRsm {
        fn copy_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            self.seen
                .lock()
                .expect("captured-metadata mutex poisoned")
                .push(metadata.clone());
            Err(RemoteStorageError::InvalidArgument("captured".into()))
        }
        fn fetch_log_segment(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _index_type: IndexType,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn delete_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    /// A stand-in write-once archive. Every copy seals a real (unsigned) WORM
    /// manifest over the segment's leader-epoch bytes, keeps that manifest in
    /// memory, and returns the chain receipt the backend would.
    ///
    /// Its delete **panics**. A write-once backend refuses every delete, so a
    /// broker that reaches one has already lost: the panic turns that into a
    /// test failure instead of a warning nobody reads.
    struct FakeWormArchive {
        archiver: WormArchiver,
        manifests: Mutex<BTreeMap<Uuid, Vec<u8>>>,
    }

    impl FakeWormArchive {
        fn new() -> Self {
            Self {
                archiver: WormArchiver::new(None),
                manifests: Mutex::new(BTreeMap::new()),
            }
        }

        fn archived_segments(&self) -> usize {
            self.manifests
                .lock()
                .expect("archived-manifest mutex poisoned")
                .len()
        }
    }

    impl RemoteStorageManager for FakeWormArchive {
        fn copy_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            let body = data.leader_epoch_index.clone();
            let entry = ObjectEntry {
                suffix: IndexType::LeaderEpoch.suffix().to_string(),
                key: format!("{}.leader-epoch", metadata.remote_log_segment_id().id),
                size_bytes: u64::try_from(body.len()).expect("test object fits in u64"),
                sha256: Sha256Digest::of(&body),
                e_tag: None,
                version_id: None,
            };
            let sealed = self.archiver.seal(metadata, vec![entry])?;
            self.manifests
                .lock()
                .expect("archived-manifest mutex poisoned")
                .insert(metadata.remote_log_segment_id().id, sealed.bytes.to_vec());
            Ok(Some(sealed.receipt.to_custom_metadata()))
        }
        fn fetch_log_segment(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _index_type: IndexType,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn delete_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            panic!(
                "a write-once archive must never reach an RSM delete (segment {})",
                metadata.remote_log_segment_id().id
            );
        }
    }

    /// An RSM that refuses every delete the way a WORM backend does, and
    /// counts how many times it was asked. Modelled on [`AlwaysFailRsm`],
    /// with the failure moved from the copy to the delete.
    #[derive(Default)]
    struct RefusesDeleteRsm {
        deletes_attempted: std::sync::atomic::AtomicUsize,
    }

    impl RemoteStorageManager for RefusesDeleteRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Ok(None)
        }
        fn fetch_log_segment(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _index_type: IndexType,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn delete_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            self.deletes_attempted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(RemoteStorageError::Worm(
                crabka_remote_storage::WormError::DeleteRefused {
                    key: format!("{}.log", metadata.remote_log_segment_id().id),
                },
            ))
        }
    }

    struct FixedMetadataSource {
        image: Arc<MetadataImage>,
        leader_tx: tokio::sync::watch::Sender<Option<NodeId>>,
    }

    impl FixedMetadataSource {
        fn new(image: MetadataImage) -> Self {
            let (leader_tx, _) = tokio::sync::watch::channel(Some(NodeId(1)));
            Self {
                image: Arc::new(image),
                leader_tx,
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for FixedMetadataSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }

        fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<MetadataImage>> {
            let (_, rx) = tokio::sync::watch::channel(self.image.clone());
            rx
        }

        fn watch_leader(&self) -> tokio::sync::watch::Receiver<Option<NodeId>> {
            self.leader_tx.subscribe()
        }

        fn quorum_state(&self) -> crabka_raft::QuorumState {
            crabka_raft::QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: *self.leader_tx.borrow(),
                voters: Vec::new(),
                voter_nodes: std::collections::BTreeMap::new(),
                per_voter_matched_index: std::collections::BTreeMap::new(),
            }
        }

        async fn submit_change(
            &self,
            _records: Vec<MetadataRecord>,
        ) -> Result<crabka_raft::SubmitChangeResult, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn change_membership(
            &self,
            _new_voters: std::collections::BTreeSet<NodeId>,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn add_learner(
            &self,
            _node_id: NodeId,
            _node: crabka_raft::Node,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        fn controller_bound_addr(&self) -> std::net::SocketAddr {
            std::net::SocketAddr::from(([0, 0, 0, 0], 0))
        }

        fn read_snapshot_range(
            &self,
            _position: i64,
            _max_bytes: i32,
        ) -> crabka_raft::SnapshotRange {
            crabka_raft::SnapshotRange::NoSnapshot
        }

        async fn trigger_snapshot(&self) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn add_voter(
            &self,
            _req: crabka_raft::AddVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn remove_voter(
            &self,
            _req: crabka_raft::RemoveVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn update_voter(
            &self,
            _req: crabka_raft::UpdateVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn cancel(&self) {}
    }

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn image_with_orders_topic() -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::from_u128(9));
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: tp().topic_id,
            partitions: 1,
            replication_factor: 1,
        }));
        image
    }

    fn batch(n: i32) -> RecordBatch {
        let mut b = RecordBatch {
            last_offset_delta: n - 1,
            ..RecordBatch::default()
        };
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(vec![b'x'; 64])),
                ..Default::default()
            });
        }
        b
    }

    /// Build a log rolled into several sealed segments under `dir`.
    fn rolled_log(dir: &std::path::Path) -> Log {
        let mut log = Log::open(
            dir,
            LogConfig {
                segment_size: bytes(256), // tiny so we roll fast
                ..LogConfig::default()
            },
        )
        .unwrap();
        for _ in 0..12 {
            let mut b = batch(2);
            log.append(&mut b).unwrap();
        }
        log
    }

    fn rolled_tiered_partition_with_config(
        log_dir: &std::path::Path,
        config: LogConfig,
    ) -> Arc<Partition> {
        let part_dir = crate::log_dir::partition_dir(log_dir, "orders", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let mut log = Log::open(&part_dir, config).unwrap();
        for _ in 0..12 {
            let mut b = batch(2);
            log.append(&mut b).unwrap();
        }
        let partition = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        partition.current_leader.store(1, Ordering::Relaxed);
        partition.current_leader_epoch.store(0, Ordering::Release);
        partition
    }

    fn rolled_tiered_partition(log_dir: &std::path::Path) -> Arc<Partition> {
        rolled_tiered_partition_with_config(
            log_dir,
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                retention: None,
                retention_size: None,
                ..LogConfig::default()
            },
        )
    }

    async fn wait_for_remote_segments(rlmm: &Arc<dyn RemoteLogMetadataManager>, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
                if listed.len() >= expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remote-log-manager run loop did not copy expected segments");
    }

    #[tokio::test]
    async fn run_ticks_and_copies_eligible_segments() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = Arc::new(PartitionRegistry::new());
        let partition = rolled_tiered_partition(log_dir.path());
        let export_count = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .tierable_segments()
            .len();
        assert!(export_count >= 2, "test needs multiple sealed segments");
        partitions.insert("orders".to_string(), PartitionIndex(0), partition);

        let controller: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(FixedMetadataSource::new(image_with_orders_topic()));
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            RemoteLogManagerContext {
                partitions,
                controller,
                archive: ArchiveMode::Mutable,
                rsm,
                rlmm: rlmm.clone(),
                node_id: NodeId(1),
                broker_id: 1,
            },
            RemoteLogManagerConfig {
                interval: millis(10),
            },
            shutdown.clone(),
        ));

        wait_for_remote_segments(&rlmm, export_count).await;
        shutdown.cancel();
        task.await.expect("remote-log-manager task panicked");

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == export_count);
        assert!(
            listed
                .iter()
                .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
        );
    }

    #[tokio::test]
    async fn tick_all_copies_local_leader_remote_enabled_partition() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition(log_dir.path());
        let export_count = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .tierable_segments()
            .len();
        assert!(export_count >= 2, "test needs multiple sealed segments");
        partitions.insert("orders".to_string(), PartitionIndex(0), partition);

        let controller = FixedMetadataSource::new(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        tick_all(
            &partitions,
            &controller,
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
            NodeId(1),
            1,
        )
        .await;

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == export_count);
        assert!(
            listed
                .iter()
                .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
        );
    }

    #[tokio::test]
    async fn tick_all_skips_partition_led_by_other_node() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition(log_dir.path());
        partition.current_leader.store(2, Ordering::Relaxed);
        partitions.insert("orders".to_string(), PartitionIndex(0), partition);

        let controller = FixedMetadataSource::new(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        tick_all(
            &partitions,
            &controller,
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
            NodeId(1),
            1,
        )
        .await;

        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn tick_all_skips_remote_storage_disabled_partition() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: false,
                retention: None,
                retention_size: None,
                ..LogConfig::default()
            },
        );
        partitions.insert("orders".to_string(), PartitionIndex(0), partition);

        let controller = FixedMetadataSource::new(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        tick_all(
            &partitions,
            &controller,
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
            NodeId(1),
            1,
        )
        .await;

        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn copies_all_sealed_segments_and_records_finished() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == exports.len());

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == exports.len());
        for md in &listed {
            // The data + offset/leader-epoch indexes are fetchable (non-empty)
            // from the remote store.
            check!(md.state() == RemoteLogSegmentState::CopySegmentFinished);
            check!(!rsm.fetch_log_segment(md, 0, None).unwrap().is_empty());
            check!(!rsm.fetch_index(md, IndexType::Offset).unwrap().is_empty());
            check!(
                !rsm.fetch_index(md, IndexType::ProducerSnapshot)
                    .unwrap()
                    .is_empty()
            );
            check!(
                !rsm.fetch_index(md, IndexType::LeaderEpoch)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn re_running_is_idempotent() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let first = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(first == exports.len());
        // Second pass: everything is already known → nothing re-copied.
        let second = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(second == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len());
    }

    #[tokio::test]
    async fn empty_exports_copies_nothing() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            Vec::new(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn copy_failure_rolls_back_and_leaves_no_metadata() {
        let log_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        assert!(!exports.is_empty());

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AlwaysFailRsm);
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == 0, "every copy failed");
        // Rollback (delete + DeleteSegmentStarted -> DeleteSegmentFinished)
        // drops the started metadata, so nothing is left behind and a later
        // run with a healthy store can retry the same segments.
        assert!(
            rlmm.list_remote_log_segments(&tp()).unwrap().is_empty(),
            "failed copies must not leave dangling metadata"
        );
    }

    #[tokio::test]
    async fn fallback_leader_epoch_when_export_has_none() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        // Hand-build an export with no leader epochs but real files on disk.
        let src = tempfile::tempdir().unwrap();
        let write = |name: &str, bytes: &[u8]| {
            let p = src.path().join(name);
            std::fs::write(&p, bytes).unwrap();
            p
        };
        let export = SegmentExport {
            base_offset: Offset(0),
            last_offset: Offset(9),
            max_timestamp: 42,
            size: bytes(10),
            log_path: write("00.log", b"0123456789"),
            offset_index_path: write("00.index", b"i"),
            time_index_path: write("00.timeindex", b"t"),
            transaction_index_path: None,
            producer_snapshot_path: write("10.snapshot", b"snapshot"),
            leader_epochs: Vec::new(),
        };

        let copied = copy_eligible(
            &tp(),
            7,
            LeaderEpoch(3),
            vec![export],
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == 1);
        let md = &rlmm.list_remote_log_segments(&tp()).unwrap()[0];
        // The fallback recorded the partition's current leader epoch (3).
        assert!(md.segment_leader_epochs().get(&LeaderEpoch(3)) == Some(&0));
    }

    fn synth_export(base: i64, last: i64, max_ts: i64, size: u32) -> SegmentExport {
        SegmentExport {
            base_offset: Offset(base),
            last_offset: Offset(last),
            max_timestamp: max_ts,
            size: bytes(size),
            log_path: std::path::PathBuf::new(),
            offset_index_path: std::path::PathBuf::new(),
            time_index_path: std::path::PathBuf::new(),
            transaction_index_path: None,
            producer_snapshot_path: std::path::PathBuf::new(),
            leader_epochs: Vec::new(),
        }
    }

    #[test]
    fn local_retention_target_returns_none_when_no_finished_segments() {
        let exports = vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)];
        let finished: HashSet<i64> = HashSet::new();
        // Big enough time-pressure to delete everything, but nothing is finished.
        assert!(local_retention_target(&exports, &finished, Some(millis(1)), None, 10_000) == None);
    }

    #[test]
    fn local_retention_target_time_based_eviction() {
        let exports = vec![
            synth_export(0, 9, 100, 64),
            synth_export(10, 19, 200, 64),
            synth_export(20, 29, 5_000, 64),
        ];
        let finished: HashSet<i64> = [0, 10, 20].into_iter().collect();
        // now=1000, retention=500ms → segs with max_ts<500 are deletable.
        // Only seg0 (max_ts=100) and seg1 (max_ts=200) qualify; seg2 stops it.
        let target = local_retention_target(&exports, &finished, Some(millis(500)), None, 1_000);
        assert!(target == Some(20));
    }

    #[test]
    fn local_retention_target_size_based_eviction() {
        let exports = vec![
            synth_export(0, 9, 100, 100),
            synth_export(10, 19, 200, 100),
            synth_export(20, 29, 300, 100),
        ];
        let finished: HashSet<i64> = [0, 10, 20].into_iter().collect();
        let cases = [
            // Total = 300; budget = 150 → must evict 150 bytes → oldest two go.
            (Some(bytes(150)), Some(20)),
            // Budget tighter than one segment: still only the oldest, because
            // after evicting 100B the remaining is 100 (>budget? no, 200>150,
            // wait: total=300, budget=150 → need to evict 150; after dropping
            // first 100B we still need 50 more → second segment also drops.
            // Test with budget = 50: need to evict 250 → all three? but the
            // walk stops since segments 0..=2 all become deletable.
            (Some(bytes(50)), Some(30)),
            // Budget larger than total → nothing deletable.
            (Some(bytes(10_000)), None),
        ];
        for (budget, expected) in cases {
            let target = local_retention_target(&exports, &finished, None, budget, 1_000);
            assert!(target == expected, "budget: {budget:?}");
        }
    }

    #[test]
    fn local_retention_target_equal_size_budget_keeps_all_segments() {
        let exports = vec![synth_export(0, 9, 100, 100), synth_export(10, 19, 200, 100)];
        let finished: HashSet<i64> = [0, 10].into_iter().collect();
        let target = local_retention_target(&exports, &finished, None, Some(bytes(200)), 1_000);
        assert!(target == None);
    }

    #[test]
    fn local_retention_target_skips_unfinished_segments_and_stops() {
        let exports = vec![
            synth_export(0, 9, 100, 64),
            synth_export(10, 19, 200, 64),
            synth_export(20, 29, 300, 64),
        ];
        // Segment at base=10 has NOT been copy-finished. Walk stops there.
        let finished: HashSet<i64> = [0, 20].into_iter().collect();
        let target = local_retention_target(&exports, &finished, Some(millis(1)), None, 10_000);
        assert!(
            target == Some(10),
            "only seg0 deletable; walk stops at seg1"
        );
    }

    #[test]
    fn local_retention_target_uses_already_resolved_effective_ms() {
        // The pure helper takes already-resolved effective_* args. This test
        // pins that contract: when caller passes effective_local_ms equal to
        // the topic's `retention` (the fallback), the helper deletes the
        // same set as if `local_retention` had been set directly.
        let exports = vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)];
        let finished: HashSet<i64> = [0, 10].into_iter().collect();
        // Caller resolved effective_local = retention = 250ms; now=1000.
        let target = local_retention_target(&exports, &finished, Some(millis(250)), None, 1_000);
        assert!(target == Some(20));
    }

    /// Test-only drive helper. It mirrors the body of `local_retention_pass`
    /// without the `Partition` wrapper, so the test can exercise the
    /// integration against a real `Log` and no broker fixtures.
    fn local_retention_drive(
        log: &mut Log,
        finished_bases: &HashSet<i64>,
        log_config: &LogConfig,
        now_ms: i64,
    ) -> usize {
        let effective_local = log_config.local_retention.or(log_config.retention);
        let effective_local_size = log_config
            .local_retention_size
            .or(log_config.retention_size);
        let exports = log.tierable_segments();
        let Some(target) = local_retention_target(
            &exports,
            finished_bases,
            effective_local,
            effective_local_size,
            now_ms,
        ) else {
            return 0;
        };
        log.delete_local_segments_through(Offset(target)).unwrap()
    }

    #[tokio::test]
    async fn local_retention_drive_deletes_copied_segments() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                local_retention: Some(millis(1)),
                ..LogConfig::default()
            },
        )
        .unwrap();
        for _ in 0..12 {
            let mut b = batch(2);
            log.append(&mut b).unwrap();
        }
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "test needs multiple sealed segments");
        let log_config = log.config_snapshot();

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == exports.len());

        // Gather finished bases the same way `local_retention_pass` would.
        let finished_bases: HashSet<i64> = rlmm
            .list_remote_log_segments(&tp())
            .unwrap()
            .iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect();
        assert!(finished_bases.len() == exports.len());

        // Drive retention with `now_ms` far in the future so every sealed
        // segment satisfies the 1ms time-based eviction.
        let future = now_ms() + 1_000_000;
        let removed = local_retention_drive(&mut log, &finished_bases, &log_config, future);
        assert!(removed == exports.len());

        // local_log_start_offset advanced; sealed log files are gone.
        let last = exports.last().unwrap().last_offset;
        assert!(log.local_log_start_offset() == last + 1);
        for ex in &exports {
            assert!(
                !ex.log_path.exists(),
                "sealed segment {:?} should be deleted",
                ex.log_path
            );
        }
        // Re-running is a no-op.
        let removed_again = local_retention_drive(&mut log, &finished_bases, &log_config, future);
        assert!(removed_again == 0);
    }

    #[tokio::test]
    async fn local_retention_pass_deletes_finished_segments_and_returns_count() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                local_retention: Some(millis(1)),
                ..LogConfig::default()
            },
        );
        let (exports, log_config) = {
            let log = partition.log.lock().expect("partition log mutex poisoned");
            (log.tierable_segments(), log.config_snapshot())
        };
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == exports.len());

        let removed = local_retention_pass(
            &tp(),
            &partition,
            &exports,
            &log_config,
            &rlmm,
            now_ms() + 1_000_000,
        );

        assert!(removed == exports.len());
        let log = partition.log.lock().expect("partition log mutex poisoned");
        assert!(log.local_log_start_offset() == exports.last().unwrap().last_offset + 1);
        assert!(log.tierable_segments().is_empty());
    }

    // ── remote-retention helper + cascade tests ────────────

    fn synth_remote_md(
        id: u128,
        start: i64,
        end: i64,
        max_ts: i64,
        size: i32,
    ) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            start,
            end,
            max_ts,
            1,
            max_ts,
            crabka_remote_storage::RemoteLogSegmentDetails::new(
                size,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), start)]),
            ),
        )
        .unwrap()
        .with_update(&RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            event_timestamp_ms: max_ts,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap()
    }

    #[test]
    fn remote_retention_eviction_set_returns_empty_when_no_segments() {
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &[],
            Some(millis(1)),
            Some(bytes(1)),
            10_000,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn remote_retention_eviction_set_time_based_picks_oldest_until_first_in_window() {
        let segs = vec![
            synth_remote_md(10, 0, 9, 100, 100),
            synth_remote_md(11, 10, 19, 200, 100),
            synth_remote_md(12, 20, 29, 9_500, 100),
        ];
        // now=10_000, retention=500ms → seg with max_ts < 9_500 is deletable.
        // seg0 (100) + seg1 (200) qualify; seg2 (9_500) stops the walk.
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segs,
            Some(millis(500)),
            None,
            10_000,
        );
        assert!(out.len() == 2);
        check!(out[0].start_offset() == 0);
        check!(out[1].start_offset() == 10);
    }

    #[test]
    fn remote_retention_eviction_set_size_based_evicts_oldest_first() {
        let segs = vec![
            synth_remote_md(10, 0, 9, 100, 100),
            synth_remote_md(11, 10, 19, 200, 100),
            synth_remote_md(12, 20, 29, 300, 100),
        ];
        let cases = [
            // Total=300, budget=150 → reclaim 150 → oldest two go.
            (Some(bytes(150)), 2),
            // Budget tighter than one segment → all three.
            (Some(bytes(50)), 3),
            // Budget larger than total → none.
            (Some(bytes(10_000)), 0),
        ];
        for (budget, expected_len) in cases {
            let out =
                remote_retention_eviction_set(ArchiveMode::Mutable, &segs, None, budget, 1_000);
            assert!(out.len() == expected_len, "budget: {budget:?}");
        }
    }

    #[test]
    fn remote_retention_eviction_set_equal_size_budget_keeps_all_segments() {
        let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segs,
            None,
            Some(bytes(100)),
            1_000,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn remote_retention_eviction_set_time_and_size_take_union_of_either() {
        let segs = vec![
            synth_remote_md(10, 0, 9, 100, 100),
            synth_remote_md(11, 10, 19, 200, 100),
            synth_remote_md(12, 20, 29, 5_000, 100),
        ];
        // Time-window: seg0+seg1 qualify (max_ts<500). Budget very generous
        // so size-based evicts nothing. Result is the time-window prefix.
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segs,
            Some(millis(500)),
            Some(bytes(10_000)),
            1_000,
        );
        assert!(out.len() == 2);
    }

    #[test]
    fn remote_retention_eviction_set_none_settings_disable_axis() {
        let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
        // No time or size → no eviction.
        assert!(
            remote_retention_eviction_set(ArchiveMode::Mutable, &segs, None, None, 10_000)
                .is_empty()
        );
    }

    #[test]
    fn remote_retention_eviction_set_walk_stops_at_first_non_deletable() {
        let segs = vec![
            synth_remote_md(10, 0, 9, 100, 100),     // deletable by time
            synth_remote_md(11, 10, 19, 9_500, 100), // in window → stops walk
            synth_remote_md(12, 20, 29, 200, 100),   // also deletable by time, but
                                                     // walk stopped at seg1 already.
        ];
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segs,
            Some(millis(500)),
            None,
            10_000,
        );
        assert!(out.len() == 1);
        assert!(out[0].start_offset() == 0);
    }

    #[tokio::test]
    async fn remote_retention_pass_evicts_old_segments_through_lifecycle() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == exports.len());
        let pre = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(!pre.is_empty());

        let cfg = LogConfig {
            retention: Some(millis(1)),
            ..LogConfig::default()
        };
        // far-future `now_ms` → every finished segment is past the window.
        let deleted = remote_retention_pass(
            &tp(),
            1,
            &cfg,
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
            now_ms() + 1_000_000,
        )
        .await;
        assert!(deleted == exports.len());

        // DeleteSegmentFinished drops the entries entirely from the cache.
        let post = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(
            post.is_empty(),
            "every segment should be gone, got {} left",
            post.len()
        );
        // RSM data is gone too.
        for md in &pre {
            assert!(rsm.fetch_log_segment(md, 0, None).is_err());
        }
    }

    #[tokio::test]
    async fn remote_retention_pass_noop_when_nothing_qualifies() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;

        let cfg = LogConfig {
            // Long retention; nothing is past the window.
            retention: Some(hours(8_760)),
            retention_size: None,
            ..LogConfig::default()
        };
        // Use a `now_ms` close to the segments' max_timestamp so the test
        // is independent of wall-clock. `rolled_log` builds batches with
        // default base_timestamp=0, so picking now=1 keeps every segment
        // inside the year-long retention window.
        let deleted =
            remote_retention_pass(&tp(), 1, &cfg, ArchiveMode::Mutable, &rsm, &rlmm, 1).await;
        assert!(deleted == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len());
    }

    #[tokio::test]
    async fn remote_retention_pass_no_settings_no_op() {
        // Neither retention.ms nor retention.bytes — early return, no list.
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let cfg = LogConfig {
            retention: None,
            retention_size: None,
            ..LogConfig::default()
        };
        let deleted =
            remote_retention_pass(&tp(), 1, &cfg, ArchiveMode::Mutable, &rsm, &rlmm, now_ms())
                .await;
        assert!(deleted == 0);
    }

    #[tokio::test]
    async fn cascade_remote_partition_delete_drops_every_segment() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm_impl = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let rlmm: Arc<dyn RemoteLogMetadataManager> = rlmm_impl.clone();
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == exports.len());

        cascade_remote_partition_delete(tp(), 1, ArchiveMode::Mutable, rsm.clone(), rlmm.clone())
            .await;

        // All segments are gone from the cache.
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
        // The remote directory for this partition is empty (or absent).
        // Kafka LocalTieredStorage layout:
        // <remote_dir>/<topic>-<partition>-<topic_id_base64>/.
        let part_dir = remote_dir.path().join("orders-0-AAAAAAAAAAAAAAAAAAAAAQ");
        let entries: Vec<_> = std::fs::read_dir(&part_dir).unwrap().collect();
        assert!(entries.is_empty(), "stray remote files: {entries:?}");
        let dump = rlmm_impl.export();
        let partition = dump
            .partitions
            .iter()
            .find(|partition| partition.topic_id_partition == tp())
            .expect("partition delete state should be dumped");
        assert!(
            partition.delete_state == Some(RemotePartitionDeleteState::DeletePartitionFinished)
        );
    }

    #[tokio::test]
    async fn cascade_remote_partition_delete_is_noop_on_empty_partition() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        // No add — partition has no segments. Cascade still walks the
        // three partition-delete states without error.
        cascade_remote_partition_delete(tp(), 1, ArchiveMode::Mutable, rsm, rlmm.clone()).await;
        // No segments after, no panics; that's the test.
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    // ── write-once (WORM) archive tests ────────────────────

    /// Add a `CopySegmentStarted` record and leave it there, the way a copy
    /// that died after the metadata write but before the backend answered
    /// does. Returns the segment's UUID.
    fn stuck_started_segment(
        rlmm: &Arc<dyn RemoteLogMetadataManager>,
        id: u128,
        base: i64,
    ) -> Uuid {
        let segment_id = RemoteLogSegmentId::new(tp(), Uuid::from_u128(id));
        let md = RemoteLogSegmentMetadata::new(
            segment_id.clone(),
            base,
            base + 9,
            100,
            1,
            100,
            crabka_remote_storage::RemoteLogSegmentDetails::new(
                100,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), base)]),
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(md).unwrap();
        segment_id.id
    }

    /// Put `count` `CopySegmentFinished` segments into `rlmm`, ten offsets
    /// apart, without going near an RSM.
    fn seed_finished_segments(rlmm: &Arc<dyn RemoteLogMetadataManager>, count: usize) {
        for i in 0..count {
            let index = u128::try_from(i).expect("test segment count fits in u128");
            let base = i64::try_from(i).expect("test segment count fits in i64") * 10;
            let id = 0x5000 + index;
            stuck_started_segment(rlmm, id, base);
            rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
                event_timestamp_ms: 100,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            })
            .unwrap();
        }
    }

    /// Every WORM receipt the metadata manager holds for `tp()`, oldest
    /// segment first.
    fn chain_records(rlmm: &Arc<dyn RemoteLogMetadataManager>) -> Vec<WormChainRecord> {
        let mut listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        listed.sort_by_key(RemoteLogSegmentMetadata::start_offset);
        listed
            .iter()
            .map(|md| {
                WormChainRecord::from_custom_metadata(
                    md.custom_metadata()
                        .expect("an archived segment carries a chain receipt"),
                )
                .expect("the chain receipt decodes")
            })
            .collect()
    }

    #[test]
    fn remote_retention_eviction_set_is_empty_for_a_write_once_archive() {
        let segs = vec![
            synth_remote_md(10, 0, 9, 100, 100),
            synth_remote_md(11, 10, 19, 200, 100),
            synth_remote_md(12, 20, 29, 300, 100),
        ];
        // The `mutable_len` column keeps the fixture honest: an empty result
        // under `WriteOnce` only means something if the very same inputs do
        // evict on a mutable tier.
        let cases = [
            ("time window past for all", Some(millis(1)), None, 10_000, 3),
            (
                "time window past for a prefix",
                Some(millis(9_750)),
                None,
                10_000,
                2,
            ),
            (
                "size budget below one segment",
                None,
                Some(bytes(50)),
                1_000,
                3,
            ),
            (
                "size budget of half the total",
                None,
                Some(bytes(150)),
                1_000,
                2,
            ),
            (
                "time and size together",
                Some(millis(1)),
                Some(bytes(150)),
                10_000,
                3,
            ),
        ];
        for (name, retention, retention_size, now, mutable_len) in cases {
            check!(
                remote_retention_eviction_set(
                    ArchiveMode::Mutable,
                    &segs,
                    retention,
                    retention_size,
                    now
                )
                .len()
                    == mutable_len,
                "case {name}"
            );
            check!(
                remote_retention_eviction_set(
                    ArchiveMode::WriteOnce,
                    &segs,
                    retention,
                    retention_size,
                    now
                )
                .is_empty(),
                "case {name}"
            );
        }
    }

    #[tokio::test]
    async fn remote_retention_pass_never_reaches_the_rsm_for_a_write_once_archive() {
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        seed_finished_segments(&rlmm, 3);
        // `FakeWormArchive::delete_log_segment_data` panics.
        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
        let cfg = LogConfig {
            retention: Some(millis(1)),
            retention_size: Some(bytes(1)),
            ..LogConfig::default()
        };

        let deleted = remote_retention_pass(
            &tp(),
            1,
            &cfg,
            ArchiveMode::WriteOnce,
            &rsm,
            &rlmm,
            now_ms() + 1_000_000,
        )
        .await;

        check!(deleted == 0);
        // Not even the metadata lifecycle moved: the pass returns before it
        // lists, so a 30-second tick over a WORM partition costs nothing.
        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        check!(listed.len() == 3);
        check!(
            listed
                .iter()
                .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
        );
    }

    #[tokio::test]
    async fn remote_retention_pass_reaches_a_refusing_rsm_only_on_a_mutable_tier() {
        let cases = [
            ("mutable tier asks and is refused", ArchiveMode::Mutable, 1),
            ("write-once archive never asks", ArchiveMode::WriteOnce, 0),
        ];
        for (name, archive, expected_attempts) in cases {
            let rlmm: Arc<dyn RemoteLogMetadataManager> =
                Arc::new(InmemoryRemoteLogMetadataManager::new());
            seed_finished_segments(&rlmm, 3);
            let rsm_impl = Arc::new(RefusesDeleteRsm::default());
            let rsm: Arc<dyn RemoteStorageManager> = rsm_impl.clone();
            let cfg = LogConfig {
                retention: Some(millis(1)),
                ..LogConfig::default()
            };

            let deleted =
                remote_retention_pass(&tp(), 1, &cfg, archive, &rsm, &rlmm, now_ms() + 1_000_000)
                    .await;

            check!(deleted == 0, "case {name}");
            check!(
                rsm_impl
                    .deletes_attempted
                    .load(std::sync::atomic::Ordering::Relaxed)
                    == expected_attempts,
                "case {name}"
            );
        }
    }

    #[tokio::test]
    async fn local_retention_still_evicts_under_a_write_once_archive() {
        let log_dir = tempfile::tempdir().unwrap();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                local_retention: Some(millis(1)),
                ..LogConfig::default()
            },
        );
        let (exports, log_config) = {
            let log = partition.log.lock().expect("partition log mutex poisoned");
            (log.tierable_segments(), log.config_snapshot())
        };
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::WriteOnce,
            &rsm,
            &rlmm,
        )
        .await;
        check!(copied == exports.len());

        // Archiving a segment is exactly what makes its local copy droppable.
        // A write-once remote tier does not change that: local retention
        // deletes local files and never touches the archive.
        let removed = local_retention_pass(
            &tp(),
            &partition,
            &exports,
            &log_config,
            &rlmm,
            now_ms() + 1_000_000,
        );

        check!(removed == exports.len());
        let log = partition.log.lock().expect("partition log mutex poisoned");
        check!(log.local_log_start_offset() == exports.last().unwrap().last_offset + 1);
        check!(log.tierable_segments().is_empty());
    }

    #[tokio::test]
    async fn copy_one_stamps_a_chain_request_on_the_started_metadata() {
        let stamp = ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(0x5eed)),
            seq: ManifestSeq(4),
            prev_head: ChainHead([7; 32]),
        };
        let cases = [
            (
                "mutable tier stamps nothing",
                ChainPosition::Unchained,
                None,
            ),
            (
                "write-once stamps the request form",
                ChainPosition::At(stamp),
                Some(WormChainRecord::request(stamp).to_custom_metadata()),
            ),
        ];
        for (name, chain, expected) in cases {
            let rsm_impl = Arc::new(CapturingRsm::default());
            let rsm: Arc<dyn RemoteStorageManager> = rsm_impl.clone();
            let rlmm: Arc<dyn RemoteLogMetadataManager> =
                Arc::new(InmemoryRemoteLogMetadataManager::new());
            let export = synth_export(0, 9, 100, 64);

            let outcome = copy_one(&tp(), 1, LeaderEpoch(0), &export, chain, &rsm, &rlmm).await;

            check!(matches!(outcome, CopyOutcome::Failed), "case {name}");
            let seen = rsm_impl
                .seen
                .lock()
                .expect("captured-metadata mutex poisoned");
            check!(seen.len() == 1, "case {name}");
            // The stamp is on the metadata the backend sees, which is the
            // same value the RLMM recorded as `CopySegmentStarted`.
            check!(
                seen[0].custom_metadata() == expected.as_ref(),
                "case {name}"
            );
        }
    }

    #[tokio::test]
    async fn copy_eligible_records_the_rsm_receipt_on_copy_segment_finished() {
        let receipt = CustomMetadata(b"backend-receipt-42".to_vec());
        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AcceptingRsm {
            receipt: Some(receipt.clone()),
        });
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            vec![synth_export(0, 9, 100, 64)],
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;

        check!(copied == 1);
        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        check!(listed.len() == 1);
        check!(listed[0].state() == RemoteLogSegmentState::CopySegmentFinished);
        check!(listed[0].custom_metadata() == Some(&receipt));
    }

    #[tokio::test]
    async fn copy_eligible_chains_consecutive_segments() {
        let archive = Arc::new(FakeWormArchive::new());
        let rsm: Arc<dyn RemoteStorageManager> = archive.clone();
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let exports = vec![
            synth_export(0, 9, 100, 64),
            synth_export(10, 19, 200, 64),
            synth_export(20, 29, 300, 64),
        ];

        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports,
            ArchiveMode::WriteOnce,
            &rsm,
            &rlmm,
        )
        .await;

        check!(copied == 3);
        check!(archive.archived_segments() == 3);
        let records = chain_records(&rlmm);
        check!(records.len() == 3);
        check!(
            records.iter().map(|r| r.seq).collect::<Vec<_>>()
                == vec![ManifestSeq(0), ManifestSeq(1), ManifestSeq(2)]
        );
        // One chain run, and each manifest hashes onto the one before it.
        check!(records.iter().all(|r| r.epoch_id == records[0].epoch_id));
        check!(records[0].prev_head == ChainHead::GENESIS);
        check!(records[1].prev_head == records[0].head.unwrap());
        check!(records[2].prev_head == records[1].head.unwrap());
    }

    #[tokio::test]
    async fn copy_eligible_resumes_the_chain_from_the_rlmm_after_a_restart() {
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let first_rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)],
            ArchiveMode::WriteOnce,
            &first_rsm,
            &rlmm,
        )
        .await;
        check!(copied == 2);
        let before = chain_records(&rlmm);

        // A restart: a brand-new backend and a brand-new copy pass, sharing
        // only the metadata manager. The chain continues from the receipts.
        let second_rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            vec![
                synth_export(0, 9, 100, 64),
                synth_export(10, 19, 200, 64),
                synth_export(20, 29, 300, 64),
            ],
            ArchiveMode::WriteOnce,
            &second_rsm,
            &rlmm,
        )
        .await;

        check!(copied == 1, "only the segment the archive lacks is copied");
        let after = chain_records(&rlmm);
        check!(after.len() == 3);
        check!(after[..2] == before[..]);
        check!(after[2].epoch_id == before[0].epoch_id);
        check!(after[2].seq == ManifestSeq(2));
        check!(after[2].prev_head == before[1].head.unwrap());
    }

    #[tokio::test]
    async fn copy_eligible_starts_a_new_epoch_when_the_rlmm_is_empty() {
        let mut genesis = Vec::new();
        for _ in 0..2 {
            let rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
            let rlmm: Arc<dyn RemoteLogMetadataManager> =
                Arc::new(InmemoryRemoteLogMetadataManager::new());
            let copied = copy_eligible(
                &tp(),
                1,
                LeaderEpoch(0),
                vec![synth_export(0, 9, 100, 64)],
                ArchiveMode::WriteOnce,
                &rsm,
                &rlmm,
            )
            .await;
            check!(copied == 1);
            genesis.push(chain_records(&rlmm).remove(0));
        }

        // A metadata manager holding no receipt cannot continue the old
        // chain, so each run says so with a fresh epoch instead of restarting
        // the old one at sequence zero and looking like a rewrite.
        check!(genesis[0].epoch_id != genesis[1].epoch_id);
        for record in &genesis {
            check!((record.seq, record.prev_head) == (ManifestSeq(0), ChainHead::GENESIS));
        }
    }

    #[tokio::test]
    async fn cascade_partition_delete_retains_archive_objects_but_finishes_the_lifecycle() {
        let archive = Arc::new(FakeWormArchive::new());
        let rsm: Arc<dyn RemoteStorageManager> = archive.clone();
        let rlmm_impl = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let rlmm: Arc<dyn RemoteLogMetadataManager> = rlmm_impl.clone();
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)],
            ArchiveMode::WriteOnce,
            &rsm,
            &rlmm,
        )
        .await;
        check!(copied == 2);

        // The RSM panics on delete, so reaching one fails this test.
        cascade_remote_partition_delete(tp(), 1, ArchiveMode::WriteOnce, rsm.clone(), rlmm.clone())
            .await;

        check!(
            rlmm.list_remote_log_segments(&tp()).unwrap().is_empty(),
            "the broker's own metadata is still cleared"
        );
        let dump = rlmm_impl.export();
        let partition = dump
            .partitions
            .iter()
            .find(|partition| partition.topic_id_partition == tp())
            .expect("partition delete state should be dumped");
        check!(partition.delete_state == Some(RemotePartitionDeleteState::DeletePartitionFinished));
        check!(
            archive.archived_segments() == 2,
            "deleting a topic must not erase a compliance archive"
        );
    }

    #[tokio::test]
    async fn copy_eligible_retries_a_segment_stuck_in_copy_segment_started() {
        let cases: [(&str, ArchiveMode, Arc<dyn RemoteStorageManager>); 2] = [
            (
                "mutable tier",
                ArchiveMode::Mutable,
                Arc::new(AcceptingRsm { receipt: None }),
            ),
            (
                "write-once archive",
                ArchiveMode::WriteOnce,
                Arc::new(FakeWormArchive::new()),
            ),
        ];
        for (name, archive, rsm) in cases {
            let rlmm: Arc<dyn RemoteLogMetadataManager> =
                Arc::new(InmemoryRemoteLogMetadataManager::new());
            let abandoned = stuck_started_segment(&rlmm, 0x57c, 0);

            let copied = copy_eligible(
                &tp(),
                1,
                LeaderEpoch(0),
                vec![synth_export(0, 9, 100, 64)],
                archive,
                &rsm,
                &rlmm,
            )
            .await;

            check!(copied == 1, "case {name}");
            let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
            let finished: Vec<&RemoteLogSegmentMetadata> = listed
                .iter()
                .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
                .collect();
            check!(finished.len() == 1, "case {name}");
            check!(finished[0].start_offset() == 0, "case {name}");
            check!(
                finished[0].remote_log_segment_id().id != abandoned,
                "case {name}: the retry runs under a fresh segment id"
            );
        }
    }

    #[test]
    fn now_ms_tracks_current_unix_epoch_millis() {
        let before = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let observed = now_ms();
        let after = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();

        assert!(observed >= before);
        assert!(observed <= after);
    }
}
