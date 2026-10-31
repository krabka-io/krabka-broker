//! Projection of committed diskless WAL index events.

use {
    super::wal_index::{
        WalDeleteFloorKey, WalDeleteFloorRecord, WalFlushRecord, WalIndexCache, WalIndexEntry,
        WalIndexKey, WalObjectReservationKey, object_id_from_key,
    },
    crate::{error::BrokerError, metrics::BrokerMetrics},
    bytes::Bytes,
    futures_util::{StreamExt, future::try_join_all},
    krabka_remote_storage::diskless::index::{
        REPLAY_FENCE_KEY, WAL_OBJECT_RESERVATION_PARTITION, wal_index_key_version,
    },
    krabka_remote_storage_topic::{
        KeyedEvent, MetadataEventLog, MetadataEventRecord, MetadataEventStream, PartitionStart,
    },
    std::{
        collections::HashMap,
        num::NonZeroU32,
        sync::{
            Arc, Mutex as StdMutex, PoisonError, Weak,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
    tokio::{
        sync::{Mutex, OwnedMutexGuard, watch},
        time,
    },
};

#[cfg(test)]
pub mod test_support;

pub const DISKLESS_WAL_INDEX_TOPIC: &str = "__diskless_wal_index";

/// How far the pump has walked the replay it started with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayProgress {
    /// Records delivered for whichever still-unreplayed partition has seen
    /// the fewest. It only rises when the slowest partition moves, so one
    /// partition going silent freezes it however busy the rest are.
    slowest_pending: u64,
    /// Whether every partition has reached its replay target.
    caught_up: bool,
    invalid: bool,
}

#[derive(Clone)]
pub struct DisklessIndexLog {
    log: Arc<dyn MetadataEventLog>,
    cache: Arc<Mutex<WalIndexCache>>,
    progress: watch::Receiver<ReplayProgress>,
    applied: watch::Receiver<u64>,
    consumed_offsets: watch::Receiver<Vec<i64>>,
    valid: Arc<AtomicBool>,
    object_fences: Arc<StdMutex<ObjectFences>>,
}

impl DisklessIndexLog {
    #[cfg(test)]
    pub(crate) async fn start(log: Arc<dyn MetadataEventLog>) -> Result<Self, BrokerError> {
        let cache = Arc::new(Mutex::new(WalIndexCache::default()));
        Self::start_with_cache(log, cache, BrokerMetrics::new()).await
    }

    /// Subscribe the projection to the index topic and replay it from the
    /// start.
    ///
    /// The subscription is established *before* the end offsets are read.
    /// `subscribe` replays each partition's backlog and then forwards live
    /// appends, so a watermark taken afterwards names only offsets the stream
    /// is bound to deliver — including a record another broker appended
    /// between the two calls, which a watermark taken first would have left
    /// out of the target. A stable keyed fence is then appended to every
    /// non-empty partition. The fence offsets are the catch-up target that
    /// [`Self::wait_until_caught_up`] reports against. Kafka compaction can
    /// leave holes, including at the former end offset, but the current fence
    /// remains present and proves the pump walked the whole committed view.
    ///
    /// # Errors
    ///
    /// Returns an error when the index topic's end offsets cannot be read.
    pub(crate) async fn start_with_cache(
        log: Arc<dyn MetadataEventLog>,
        cache: Arc<Mutex<WalIndexCache>>,
        metrics: BrokerMetrics,
    ) -> Result<Self, BrokerError> {
        let starts = (0..log.partition_count())
            .map(|partition| PartitionStart {
                partition,
                start_offset: 0,
            })
            .collect();
        let (stream, _assignment) = log.subscribe(starts);

        let pending = publish_replay_fences(&*log).await?;
        let (progress_tx, progress_rx) = watch::channel(ReplayProgress {
            slowest_pending: 0,
            caught_up: pending.is_empty(),
            invalid: false,
        });
        let (applied_tx, applied_rx) = watch::channel(0u64);
        let partition_count = usize::try_from(log.partition_count()).map_err(|_| {
            BrokerError::Txn(format!(
                "diskless index partition count {} is negative",
                log.partition_count()
            ))
        })?;
        let (consumed_tx, consumed_rx) = watch::channel(vec![-1; partition_count]);
        let valid = Arc::new(AtomicBool::new(true));

        let object_fences = Arc::new(StdMutex::new(ObjectFences::default()));
        let delivered = pending.keys().map(|partition| (*partition, 0)).collect();
        let pump = IndexPump {
            cache: Arc::clone(&cache),
            valid: Arc::clone(&valid),
            object_fences: Arc::clone(&object_fences),
            metrics,
            progress_tx,
            applied_tx,
            consumed_tx,
            pending,
            delivered,
        };
        tokio::spawn(pump.run(stream));
        Ok(Self {
            log,
            cache,
            progress: progress_rx,
            applied: applied_rx,
            consumed_offsets: consumed_rx,
            valid,
            object_fences,
        })
    }

    /// Resolve `true` once the projection has replayed every record the index
    /// topic held when this log started.
    ///
    /// Resolve `false` when the replay makes no progress for `stall_timeout`,
    /// or when the pump stops outright. Neither is recoverable on this
    /// subscription: a [`krabka_remote_storage_topic::KafkaMetadataEventLog`]
    /// partition whose fetch loop died while connecting goes silent forever
    /// without closing the shared stream, so a caller that keeps waiting
    /// never flushes again. Rebuild the log instead.
    pub(crate) async fn wait_until_caught_up(&self, stall_timeout: Duration) -> bool {
        let mut progress = self.progress.clone();
        loop {
            let state = *progress.borrow_and_update();
            if state.invalid {
                return false;
            }
            if state.caught_up {
                return true;
            }
            match time::timeout(stall_timeout, progress.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => return false,
            }
        }
    }

    #[must_use]
    pub(crate) fn cache(&self) -> Arc<Mutex<WalIndexCache>> { Arc::clone(&self.cache) }

    /// Bring the projection up to a fence committed to every index partition
    /// now, so it holds every index record committed before this call.
    ///
    /// `timeout` bounds the whole wait, fence publication included: a stalled
    /// index partition makes the caller skip its destructive work and return
    /// to the flusher loop, never hold it up. Returns `false` when the view
    /// cannot be established in time or the projection is invalid.
    pub(crate) async fn wait_for_fresh_view(&self, timeout: Duration) -> bool {
        time::timeout(timeout, self.fresh_view())
            .await
            .unwrap_or(false)
    }

    async fn fresh_view(&self) -> bool {
        if !self.is_valid() {
            return false;
        }
        let fences = (0..self.log.partition_count()).map(|partition| {
            self.log.publish_keyed(
                partition,
                Bytes::from_static(REPLAY_FENCE_KEY),
                Some(Bytes::new()),
            )
        });
        let targets = match try_join_all(fences).await {
            Ok(targets) => targets,
            Err(error) => {
                tracing::warn!(%error, "diskless index freshness fence failed");
                return false;
            }
        };
        let mut consumed = self.consumed_offsets.clone();
        loop {
            if !self.is_valid() {
                return false;
            }
            let reached = {
                let offsets = consumed.borrow_and_update();
                offsets.len() == targets.len()
                    && offsets
                        .iter()
                        .zip(&targets)
                        .all(|(seen, target)| seen >= target)
            };
            if reached {
                return true;
            }
            if consumed.changed().await.is_err() {
                return false;
            }
        }
    }

    /// Commit an object reservation before its bytes are put. The existing
    /// v1 delete-floor wire shape keeps rolling 1.x readers compatible.
    pub(crate) async fn reserve_object(&self, object_key: &str) -> Result<(), BrokerError> {
        let (object_id, key) = object_reservation_key(object_key)?;
        let floor_key = WalDeleteFloorKey::from_bytes(&key).ok_or_else(|| {
            BrokerError::Txn(format!(
                "diskless object reservation key for {object_key} is not a delete-floor key"
            ))
        })?;
        let value = WalDeleteFloorRecord {
            topic_id: floor_key.topic_id,
            partition: floor_key.partition,
            floor: 0,
        }
        .to_bytes()
        .map_err(BrokerError::Txn)?;
        self.log
            .publish_keyed(
                index_partition(&key, self.log.partition_count())?,
                key,
                Some(value),
            )
            .await
            .map_err(|error| BrokerError::Txn(format!("diskless object reservation: {error}")))?;

        if self
            .wait_for_projection(Duration::from_secs(30), |cache| {
                cache.reserved_objects().contains(&object_id)
            })
            .await
        {
            Ok(())
        } else {
            Err(BrokerError::Txn(
                "diskless object reservation projection timed out".into(),
            ))
        }
    }

    /// Release a reservation only after every range record for the object has
    /// been acknowledged and applied locally.
    pub(crate) async fn release_object_reservation(
        &self,
        object_key: &str,
    ) -> Result<(), BrokerError> {
        let (object_id, key) = object_reservation_key(object_key)?;
        self.log
            .publish_keyed(
                index_partition(&key, self.log.partition_count())?,
                key,
                None,
            )
            .await
            .map_err(|error| {
                BrokerError::Txn(format!("diskless object reservation tombstone: {error}"))
            })?;
        self.cache
            .lock()
            .await
            .release_object_reservation(object_id);
        Ok(())
    }

    /// Fence a projection addition against reclamation of `object_key`.
    ///
    /// Callers acquire this before the cache mutex. Object keys are immutable
    /// UUID names and are never legitimately published again after retirement;
    /// the fence closes the check/delete race with replay already in flight.
    pub(crate) async fn object_fence(&self, object_key: &str) -> OwnedMutexGuard<()> {
        lock_object_fence(&self.object_fences, object_key).await
    }

    pub(crate) fn is_valid(&self) -> bool { self.valid.load(Ordering::Acquire) }

    /// Wait until the live projection becomes invalid or its pump stops.
    pub(crate) async fn wait_until_unusable(&self) {
        let mut progress = self.progress.clone();
        loop {
            if progress.borrow_and_update().invalid || progress.changed().await.is_err() {
                return;
            }
        }
    }

    /// Wait until `record` appears in the committed projection.
    pub(crate) async fn wait_until_applied(
        &self,
        record: &WalFlushRecord,
        timeout: Duration,
    ) -> bool {
        self.wait_for_projection(timeout, |cache| cache.contains_record(record))
            .await
    }

    async fn wait_for_projection(
        &self,
        timeout: Duration,
        mut predicate: impl FnMut(&WalIndexCache) -> bool,
    ) -> bool {
        let mut applied = self.applied.clone();
        time::timeout(timeout, async {
            loop {
                if predicate(&*self.cache.lock().await) {
                    return true;
                }
                if applied.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false)
    }

    /// Publish one keyed index record per range of `record`, and resolve to
    /// the offset of the last range's record.
    ///
    /// Ranges of different data partitions are submitted together, at most
    /// [`INDEX_PUBLISH_GROUP_MAX`] to a group, so a flush that spans many
    /// partitions waits for one acknowledgment round trip per group rather
    /// than one per range. Ranges of one data partition still go one group
    /// after another, in record order: [`publication_waves`] says why.
    ///
    /// # Errors
    ///
    /// Returns an error when a record cannot be encoded or published. Ranges
    /// of earlier groups may then be published already, and for each data
    /// partition they are a prefix of its ranges in `record`.
    pub(crate) async fn publish_flush(&self, record: &WalFlushRecord) -> Result<i64, BrokerError> {
        let partitions = self.log.partition_count();
        let events = record
            .entries
            .iter()
            .map(|entry| {
                let key = WalIndexKey::from(entry).to_bytes();
                let event = WalFlushRecord {
                    object_key: record.object_key.clone(),
                    format_version: record.format_version,
                    entries: vec![entry.clone()],
                }
                .to_bytes()
                .map_err(BrokerError::Txn)?;
                Ok(KeyedEvent {
                    partition: index_partition(&key, partitions)?,
                    key,
                    event: Some(event),
                })
            })
            .collect::<Result<Vec<_>, BrokerError>>()?;
        let mut offsets = vec![-1; events.len()];
        for wave in publication_waves(&record.entries) {
            for group in wave.chunks(INDEX_PUBLISH_GROUP_MAX) {
                let batch = group
                    .iter()
                    .map(|&index| events.get(index).cloned())
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| {
                        BrokerError::Txn("diskless index publication wave is out of range".into())
                    })?;
                let published = self.log.publish_keyed_group(batch).await.map_err(|error| {
                    BrokerError::Txn(format!("diskless index publish: {error}"))
                })?;
                for (&index, offset) in group.iter().zip(published) {
                    if let Some(slot) = offsets.get_mut(index) {
                        *slot = offset;
                    }
                }
            }
        }
        Ok(offsets.last().copied().unwrap_or(-1))
    }

    /// Publish one partition's `DeleteRecords` floor and wait until the
    /// committed projection carries it.
    ///
    /// `DeleteRecords` acknowledges only after this returns, so a client that
    /// was told its records are gone cannot see them again after a restart or
    /// a leadership move.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be published, or when the
    /// projection does not report it within `timeout`.
    pub(crate) async fn publish_delete_floor(
        &self,
        topic_id: uuid::Uuid,
        partition: i32,
        floor: i64,
        timeout: Duration,
    ) -> Result<(), BrokerError> {
        let key = WalDeleteFloorKey {
            topic_id,
            partition,
        };
        let bytes = WalDeleteFloorRecord {
            topic_id,
            partition,
            floor,
        }
        .to_bytes()
        .map_err(BrokerError::Txn)?;
        let key_bytes = key.to_bytes();
        self.log
            .publish_keyed(
                index_partition(&key_bytes, self.log.partition_count())?,
                key_bytes,
                Some(bytes),
            )
            .await
            .map_err(|error| BrokerError::Txn(format!("diskless delete floor publish: {error}")))?;

        let mut applied = self.applied.clone();
        time::timeout(timeout, async {
            loop {
                if self.cache.lock().await.delete_floor(topic_id, partition) >= floor {
                    return true;
                }
                if applied.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false)
        .then_some(())
        .ok_or_else(|| BrokerError::Txn("diskless delete floor projection timed out".into()))
    }

    async fn publish_tombstone(&self, bytes: Bytes, context: &str) -> Result<(), BrokerError> {
        self.log
            .publish_keyed(
                index_partition(&bytes, self.log.partition_count())?,
                bytes,
                None,
            )
            .await
            .map(|_| ())
            .map_err(|error| BrokerError::Txn(format!("{context}: {error}")))
    }

    /// Tombstone one partition's floor, so a deleted topic leaves none behind.
    pub(crate) async fn tombstone_delete_floor(
        &self,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), BrokerError> {
        let key = WalDeleteFloorKey {
            topic_id,
            partition,
        };
        self.publish_tombstone(key.to_bytes(), "diskless delete floor tombstone")
            .await?;
        self.cache.lock().await.clear_delete_floor(key);
        Ok(())
    }

    /// Tombstone every projected range for a deleted topic.
    pub(crate) async fn tombstone_topic(&self, topic_id: uuid::Uuid) -> Result<(), BrokerError> {
        let keys = self.cache.lock().await.keys_for_topic(topic_id);
        self.tombstone_ranges(&keys).await
    }

    /// Tombstone named logical ranges, one keyed record each, and drop them
    /// from the projection.
    ///
    /// A partial failure leaves the ranges it already published tombstoned and
    /// the rest untouched. That is safe to retry: a tombstone for a range the
    /// projection no longer holds is a no-op, and the caller recomputes the
    /// remaining set on its next pass.
    pub(crate) async fn tombstone_ranges(&self, keys: &[WalIndexKey]) -> Result<(), BrokerError> {
        for key in keys {
            self.publish_tombstone(key.to_bytes(), "diskless index tombstone")
                .await?;
            self.cache.lock().await.remove(*key);
        }
        Ok(())
    }
}

/// The task that applies the index topic's records to the projection, in
/// the order the subscription delivers them.
struct IndexPump {
    cache: Arc<Mutex<WalIndexCache>>,
    valid: Arc<AtomicBool>,
    object_fences: Arc<StdMutex<ObjectFences>>,
    metrics: BrokerMetrics,
    progress_tx: watch::Sender<ReplayProgress>,
    applied_tx: watch::Sender<u64>,
    consumed_tx: watch::Sender<Vec<i64>>,
    /// The replay fence offset of each partition still being replayed.
    pending: HashMap<i32, i64>,
    /// Records delivered so far for each partition in `pending`.
    delivered: HashMap<i32, u64>,
}

impl IndexPump {
    async fn run(mut self, mut stream: MetadataEventStream) {
        while let Some(event) = stream.next().await {
            self.apply(event).await;
        }
    }

    async fn apply(&mut self, event: MetadataEventRecord) {
        match classify(&event) {
            IndexEvent::UnknownKeyVersion { key_version } => {
                self.invalidate();
                tracing::error!(
                    partition = event.partition,
                    offset = event.offset,
                    key_version = ?key_version,
                    "diskless WAL index record has an unknown key version; projection is unsafe"
                );
            }
            IndexEvent::ReleaseReservation(key) => {
                self.cache
                    .lock()
                    .await
                    .release_object_reservation(key.object_id());
                self.bump_applied();
            }
            IndexEvent::ClearFloor(key) => {
                self.cache.lock().await.clear_delete_floor(key);
                self.bump_applied();
            }
            IndexEvent::RemoveRange(key) => {
                self.cache.lock().await.remove(key);
                self.bump_applied();
            }
            IndexEvent::TombstoneNoop | IndexEvent::ReplayFence => {}
            IndexEvent::Reserve(key) => {
                self.cache.lock().await.reserve_object(key.object_id());
                self.bump_applied();
            }
            IndexEvent::InvalidReservation | IndexEvent::MismatchedReservation => self.invalidate(),
            IndexEvent::RaiseFloor { key, floor } => {
                self.cache
                    .lock()
                    .await
                    .raise_delete_floor(key.topic_id, key.partition, floor);
                self.bump_applied();
            }
            IndexEvent::ApplyRange { key, record } => {
                // An object-local fence serializes additions with the
                // reclaimer's final check and DELETE, without blocking
                // unrelated cache traffic.
                let _object_fence =
                    lock_object_fence(&self.object_fences, &record.object_key).await;
                self.cache.lock().await.apply_keyed(key, &record);
                self.bump_applied();
            }
            IndexEvent::Unkeyed => {
                self.invalidate();
                tracing::error!(
                    partition = event.partition,
                    offset = event.offset,
                    "unkeyed diskless WAL index record; projection is unsafe"
                );
            }
            IndexEvent::Rejected(error) => {
                self.invalidate();
                tracing::error!(
                    partition = event.partition,
                    offset = event.offset,
                    %error,
                    "diskless WAL index record rejected; projection is unsafe"
                );
            }
        }
        self.record_consumed(&event);
        self.report_progress(&event);
    }

    /// Tell projection waiters the cache changed.
    fn bump_applied(&self) {
        self.applied_tx
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    /// Mark the projection unsafe to read, and count the record that made it
    /// so.
    fn invalidate(&self) {
        self.metrics.diskless_wal_index_decode_failures_total.inc();
        self.valid.store(false, Ordering::Release);
        self.progress_tx
            .send_modify(|progress| progress.invalid = true);
    }

    fn record_consumed(&self, event: &MetadataEventRecord) {
        self.consumed_tx.send_modify(|offsets| {
            // A negative partition addresses no slot, as one past the end does.
            if let Ok(index) = usize::try_from(event.partition)
                && let Some(offset) = offsets.get_mut(index)
            {
                *offset = (*offset).max(event.offset);
            }
        });
    }

    fn report_progress(&mut self, event: &MetadataEventRecord) {
        let Some(target) = self.pending.get(&event.partition).copied() else {
            return;
        };
        *self.delivered.entry(event.partition).or_default() += 1;
        if event.offset >= target {
            self.pending.remove(&event.partition);
            self.delivered.remove(&event.partition);
        }
        let next = ReplayProgress {
            slowest_pending: self.delivered.values().copied().min().unwrap_or(u64::MAX),
            caught_up: self.pending.is_empty(),
            invalid: !self.valid.load(Ordering::Acquire),
        };
        // Notify only on a real change, so a waiter's stall timer is not
        // reset by a busy partition while another one is stuck.
        self.progress_tx.send_if_modified(|progress| {
            let changed = *progress != next;
            *progress = next;
            changed
        });
    }
}

#[derive(Default)]
struct ObjectFences {
    entries: HashMap<String, Weak<Mutex<()>>>,
    acquisitions_until_prune: usize,
}

impl ObjectFences {
    fn get(&mut self, object_key: &str) -> Arc<Mutex<()>> {
        if self.acquisitions_until_prune == 0 {
            self.entries.retain(|_, fence| fence.strong_count() > 0);
            self.entries.shrink_to(64);
            // Amortize registry scans over at least its live size. Inactive
            // object names then stay bounded without scanning on every append.
            self.acquisitions_until_prune = self.entries.len().max(64);
        }
        self.acquisitions_until_prune -= 1;
        if let Some(fence) = self.entries.get(object_key).and_then(Weak::upgrade) {
            return fence;
        }
        let fence = Arc::new(Mutex::new(()));
        self.entries
            .insert(object_key.to_owned(), Arc::downgrade(&fence));
        fence
    }
}

async fn lock_object_fence(
    fences: &StdMutex<ObjectFences>,
    object_key: &str,
) -> OwnedMutexGuard<()> {
    let fence = fences
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(object_key);
    fence.lock_owned().await
}

/// Publish a replay fence on every index partition that holds records, and
/// return the offset of each fence by partition.
async fn publish_replay_fences(
    log: &dyn MetadataEventLog,
) -> Result<HashMap<i32, i64>, BrokerError> {
    let high_water_marks = log
        .high_water_marks()
        .await
        .map_err(|error| BrokerError::Txn(format!("diskless index end offsets: {error}")))?;
    let mut pending = HashMap::new();
    for partition in 0..log.partition_count() {
        let index = usize::try_from(partition).map_err(|_| {
            BrokerError::Txn(format!("diskless index partition {partition} is negative"))
        })?;
        if high_water_marks.get(index).copied().unwrap_or(0) == 0 {
            continue;
        }
        let offset = log
            .publish_keyed(
                partition,
                Bytes::from_static(REPLAY_FENCE_KEY),
                Some(Bytes::new()),
            )
            .await
            .map_err(|error| BrokerError::Txn(format!("diskless index replay fence: {error}")))?;
        pending.insert(partition, offset);
    }
    Ok(pending)
}

/// What one committed index record asks of the projection.
#[derive(Debug)]
enum IndexEvent {
    /// A key that is no index key this build reads, and not the replay
    /// fence: a key version this build does not read. Skipping it would
    /// leave a range or floor out of the projection, so it is as unsafe as
    /// an unkeyed record.
    UnknownKeyVersion { key_version: Option<i16> },
    /// A tombstone of an object reservation.
    ReleaseReservation(WalObjectReservationKey),
    /// A tombstone of a partition's delete floor.
    ClearFloor(WalDeleteFloorKey),
    /// A tombstone of a range.
    RemoveRange(WalIndexKey),
    /// A tombstone of the replay fence, or of no key the projection holds.
    TombstoneNoop,
    /// A replay fence. It marks a position and projects nothing.
    ReplayFence,
    /// A committed object reservation.
    Reserve(WalObjectReservationKey),
    /// A reservation-key record that is not on the reservation partition or
    /// does not carry floor zero.
    InvalidReservation,
    /// A reservation-key record whose value names another key.
    MismatchedReservation,
    /// A partition's delete floor.
    RaiseFloor { key: WalDeleteFloorKey, floor: i64 },
    /// The latest value for a range.
    ApplyRange {
        key: WalIndexKey,
        record: WalFlushRecord,
    },
    /// A flush record with no range key. Every index record is keyed
    /// (`publish_flush`); an unkeyed one has no range it is the latest for,
    /// so the projection can no longer be trusted.
    Unkeyed,
    /// A value that does not decode.
    Rejected(String),
}

/// Decode one index record, each part once, into what it asks of the
/// projection.
fn classify(event: &MetadataEventRecord) -> IndexEvent {
    let key = event.key.as_deref();
    // A floor key and a range key never decode as each other, so the two
    // projections stay independent however the index topic's partitions
    // interleave them. A reservation key decodes as a floor key too.
    let floor_key = key.and_then(WalDeleteFloorKey::from_bytes);
    let reservation_key = key.and_then(WalObjectReservationKey::from_bytes);
    let range_key = key.and_then(WalIndexKey::from_bytes);
    if let Some(key) = key
        && key != REPLAY_FENCE_KEY
        && floor_key.is_none()
        && reservation_key.is_none()
        && range_key.is_none()
    {
        return IndexEvent::UnknownKeyVersion {
            key_version: wal_index_key_version(key),
        };
    }
    if event.tombstone {
        return match (reservation_key, floor_key, range_key) {
            (Some(key), _, _) => IndexEvent::ReleaseReservation(key),
            (None, Some(key), _) => IndexEvent::ClearFloor(key),
            (None, None, Some(key)) => IndexEvent::RemoveRange(key),
            (None, None, None) => IndexEvent::TombstoneNoop,
        };
    }
    if key == Some(REPLAY_FENCE_KEY) {
        return IndexEvent::ReplayFence;
    }
    floor_key.map_or_else(
        || classify_flush_value(&event.payload, range_key),
        |floor_key| classify_floor_value(event, floor_key, reservation_key),
    )
}

/// Classify the value of a delete-floor key, which an object reservation
/// shares.
fn classify_floor_value(
    event: &MetadataEventRecord,
    key: WalDeleteFloorKey,
    reservation_key: Option<WalObjectReservationKey>,
) -> IndexEvent {
    let record = match WalDeleteFloorRecord::from_bytes(&event.payload) {
        Ok(record) => record,
        Err(error) => return IndexEvent::Rejected(error),
    };
    let Some(reservation_key) = reservation_key else {
        return IndexEvent::RaiseFloor {
            key,
            floor: record.floor,
        };
    };
    if key.partition != WAL_OBJECT_RESERVATION_PARTITION || record.floor != 0 {
        return IndexEvent::InvalidReservation;
    }
    // The value must match its compacted key; this namespace is otherwise a
    // normal v1 delete-floor record to old readers.
    if record.topic_id != key.topic_id
        || record.partition != key.partition
        || reservation_key.to_bytes().as_ref() != event.key.as_deref().unwrap_or_default()
    {
        return IndexEvent::MismatchedReservation;
    }
    IndexEvent::Reserve(reservation_key)
}

/// Classify a value that is not a delete floor's: a flush record.
fn classify_flush_value(payload: &[u8], range_key: Option<WalIndexKey>) -> IndexEvent {
    let record = match WalFlushRecord::from_bytes(payload) {
        Ok(record) => record,
        Err(error) => return IndexEvent::Rejected(error),
    };
    range_key.map_or(IndexEvent::Unkeyed, |key| IndexEvent::ApplyRange {
        key,
        record,
    })
}

/// Most index records that [`DisklessIndexLog::publish_flush`] submits before
/// it waits for their acknowledgments.
///
/// It bounds what one flush holds in the producer at a time. An index record
/// is about a hundred bytes, so a full group stays far below the producer's
/// buffer, and a flush of up to this many partitions takes one round trip.
const INDEX_PUBLISH_GROUP_MAX: usize = 1024;

/// Split a flush's ranges into waves, by index into `entries`. Wave `n` holds
/// the `n`th range of every data partition, in record order.
///
/// A wave may be published all at once, but a data partition's next range
/// waits for the wave before it to be acknowledged. The flusher resumes each
/// partition at its [`WalIndexCache::flushed_frontier`], the end of the
/// highest range the projection holds. A later range that became durable while
/// an earlier one of the same partition did not, through a failed publish or a
/// crash, would move that frontier past the gap. The next flush would then
/// skip the gap's records, and the trim behind it would drop them for good.
/// One partition's ranges therefore stay durable as a prefix, as they did when
/// every range was published one at a time.
fn publication_waves(entries: &[WalIndexEntry]) -> Vec<Vec<usize>> {
    let mut next_wave: HashMap<(uuid::Uuid, i32), usize> = HashMap::new();
    let mut waves: Vec<Vec<usize>> = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let wave = next_wave
            .entry((entry.topic_id, entry.partition))
            .or_default();
        match waves.get_mut(*wave) {
            Some(members) => members.push(index),
            None => waves.push(vec![index]),
        }
        *wave += 1;
    }
    waves
}

fn index_partition(key: &[u8], partitions: i32) -> Result<i32, BrokerError> {
    let hash = key.iter().copied().fold(0u32, |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(u32::from(byte))
    });
    let partitions = u32::try_from(partitions)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| BrokerError::Txn("diskless index topic has no partitions".into()))?;
    // The remainder is below `partitions`, which came from an `i32`.
    i32::try_from(hash % partitions)
        .map_err(|_| BrokerError::Txn("diskless index partition exceeds i32".into()))
}

fn object_reservation_key(object_key: &str) -> Result<(uuid::Uuid, bytes::Bytes), BrokerError> {
    let object_id = object_id_from_key(object_key)
        .ok_or_else(|| BrokerError::Txn(format!("invalid diskless object key: {object_key}")))?;
    let key = WalObjectReservationKey::new(object_id).to_bytes();
    Ok((object_id, key))
}

#[cfg(test)]
mod tests {
    use {
        super::{
            test_support::{
                GroupAckLog, HeldPartitionLog, PacedReplayLog, RacingAppendLog, ReplayPace,
            },
            *,
        },
        crate::{
            diskless::wal_index::{WalFlushRecord, WalIndexEntry},
            metrics::BrokerMetrics,
        },
        assert2::assert,
        async_trait::async_trait,
        krabka_remote_storage_topic::{
            AssignmentHandle, InProcessMetadataEventLog, MetadataEventLog, MetadataEventStream,
            MetadataLogError, PartitionStart,
        },
        std::sync::atomic::{AtomicBool, Ordering},
        tokio::{task, time::timeout},
        uuid::Uuid,
    };

    #[tokio::test]
    async fn object_fences_retire_inactive_names_and_keep_live_guards() {
        let fences = StdMutex::new(ObjectFences::default());
        let held = lock_object_fence(&fences, "held").await;
        for index in 0..100 {
            let guard = lock_object_fence(&fences, &format!("object-{index}")).await;
            assert!(fences.lock().unwrap().entries.len() <= 65);
            drop(guard);
        }
        for _ in 0..64 {
            drop(lock_object_fence(&fences, "hot").await);
        }
        assert!(fences.lock().unwrap().entries.len() == 2);
        assert!(fences.lock().unwrap().entries["held"].strong_count() == 1);
        drop(held);
        for _ in 0..64 {
            drop(lock_object_fence(&fences, "hot").await);
        }
        assert!(fences.lock().unwrap().entries.len() == 1);
    }

    #[derive(Clone, Copy)]
    enum PublishOutcome {
        Succeed,
        Fail,
    }

    struct TogglePublishLog {
        inner: Arc<dyn MetadataEventLog>,
        fail: AtomicBool,
    }

    impl TogglePublishLog {
        fn new(inner: Arc<dyn MetadataEventLog>, outcome: PublishOutcome) -> Arc<Self> {
            Arc::new(Self {
                inner,
                fail: AtomicBool::new(matches!(outcome, PublishOutcome::Fail)),
            })
        }

        fn set_outcome(&self, outcome: PublishOutcome) {
            self.fail
                .store(matches!(outcome, PublishOutcome::Fail), Ordering::Relaxed);
        }
    }

    #[async_trait]
    impl MetadataEventLog for TogglePublishLog {
        fn partition_count(&self) -> i32 { self.inner.partition_count() }

        async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
            self.inner.publish(partition, event).await
        }

        async fn publish_keyed(
            &self,
            partition: i32,
            key: Bytes,
            event: Option<Bytes>,
        ) -> Result<i64, MetadataLogError> {
            if self.fail.load(Ordering::Relaxed) {
                Err(MetadataLogError::Closed)
            } else {
                self.inner.publish_keyed(partition, key, event).await
            }
        }

        fn subscribe(
            &self,
            assignment: Vec<PartitionStart>,
        ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
            self.inner.subscribe(assignment)
        }

        async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
            self.inner.high_water_marks().await
        }
    }

    use super::test_support::{flush_record, second_object_flush_record};

    /// An object and the offsets of the range it holds.
    struct SeededRange<'a> {
        object_key: &'a str,
        first_offset: i64,
        last_offset: i64,
    }

    async fn seeded_index(
        event_log: Arc<dyn MetadataEventLog>,
        topic_id: Uuid,
        ranges: &[SeededRange<'_>],
    ) -> DisklessIndexLog {
        let seed = DisklessIndexLog::start(event_log).await.unwrap();
        for &SeededRange {
            object_key: key,
            first_offset: first,
            last_offset: last,
        } in ranges
        {
            seed.publish_flush(
                &flush_record()
                    .topic_id(topic_id)
                    .object_key(key)
                    .first_offset(first)
                    .last_offset(last)
                    .max_timestamp(0)
                    .call(),
            )
            .await
            .unwrap();
        }
        seed
    }

    /// An event log that holds a backlog, the topic it is for, and the index
    /// that seeded it.
    struct Backlog {
        event_log: Arc<InProcessMetadataEventLog>,
        topic_id: Uuid,
        seed: DisklessIndexLog,
    }

    async fn two_range_backlog(partitions: i32) -> Backlog {
        let event_log = InProcessMetadataEventLog::new(partitions);
        let topic_id = Uuid::from_u128(7);
        let seed = seeded_index(
            Arc::<InProcessMetadataEventLog>::clone(&event_log),
            topic_id,
            &[
                SeededRange {
                    object_key: "object-a",
                    first_offset: 0,
                    last_offset: 3,
                },
                SeededRange {
                    object_key: "object-b",
                    first_offset: 4,
                    last_offset: 7,
                },
            ],
        )
        .await;
        Backlog {
            event_log,
            topic_id,
            seed,
        }
    }

    async fn check_replayed_backlog(index: &DisklessIndexLog, topic_id: Uuid, context: &str) {
        assert!(index.wait_until_caught_up(Duration::from_secs(5)).await);
        assert!(
            index.cache().lock().await.flushed_frontier(topic_id, 0) == Some(8),
            "{context}"
        );
    }

    #[tokio::test]
    async fn catch_up_resolves_only_once_the_topic_backlog_is_projected() {
        let Backlog {
            event_log,
            topic_id,
            seed: _seed,
        } = two_range_backlog(2).await;

        // A restart against the now-populated topic.
        let restarted = DisklessIndexLog::start(event_log).await.unwrap();
        check_replayed_backlog(
            &restarted,
            topic_id,
            "catch-up must not resolve before the whole backlog is projected",
        )
        .await;
    }

    #[tokio::test]
    async fn incompatible_index_record_fails_replay_and_increments_metric() {
        let event_log = InProcessMetadataEventLog::new(1);
        let mut old = flush_record().object_key("old-object").call();
        old.format_version = WalFlushRecord::FORMAT_VERSION - 1;
        let bytes =
            <serde_wincode::SerdeCompat<WalFlushRecord> as wincode::Serialize>::serialize(&old)
                .unwrap();
        event_log.publish(0, Bytes::from(bytes)).await.unwrap();

        let metrics = BrokerMetrics::new();
        let failures = metrics.diskless_wal_index_decode_failures_total.clone();
        let index = DisklessIndexLog::start_with_cache(
            event_log,
            Arc::new(Mutex::new(WalIndexCache::default())),
            metrics,
        )
        .await
        .unwrap();

        assert!(!index.wait_until_caught_up(Duration::from_secs(1)).await);
        assert!(!index.is_valid());
        assert!(failures.get() == 1);
    }

    async fn check_invalid_index_log(event_log: Arc<InProcessMetadataEventLog>, case: &str) {
        let metrics = BrokerMetrics::new();
        let failures = metrics.diskless_wal_index_decode_failures_total.clone();
        let cache = Arc::new(Mutex::new(WalIndexCache::default()));
        let index = DisklessIndexLog::start_with_cache(event_log, Arc::clone(&cache), metrics)
            .await
            .unwrap();

        assert!(
            !index.wait_until_caught_up(Duration::from_secs(1)).await,
            "case {case}"
        );
        assert!(!index.is_valid(), "case {case}");
        assert!(failures.get() == 1, "case {case}");
        assert!(
            cache
                .lock()
                .await
                .lookup(Uuid::from_u128(7), 0, 0)
                .is_none(),
            "case {case}"
        );
    }

    #[tokio::test]
    async fn an_index_record_of_an_unknown_key_version_fails_replay_and_increments_metric() {
        let record = flush_record().object_key("future-key").call();
        let mut future_key = WalIndexKey::from(&record.entries[0]).to_bytes().to_vec();
        future_key[..2].copy_from_slice(&7_i16.to_be_bytes());
        for (name, payload) in [
            ("value", Some(record.to_bytes().unwrap())),
            ("tombstone", None),
        ] {
            let event_log = InProcessMetadataEventLog::new(1);
            event_log
                .publish_keyed(0, Bytes::from(future_key.clone()), payload)
                .await
                .unwrap();

            check_invalid_index_log(event_log, name).await;
        }
    }

    #[tokio::test]
    async fn an_unkeyed_index_record_fails_replay_and_increments_metric() {
        let event_log = InProcessMetadataEventLog::new(1);
        let record = flush_record().object_key("unkeyed").call();
        event_log
            .publish(0, record.to_bytes().unwrap())
            .await
            .unwrap();

        check_invalid_index_log(event_log, "unkeyed").await;
    }

    #[tokio::test]
    async fn catch_up_resolves_immediately_for_an_empty_index_topic() {
        let index = DisklessIndexLog::start(InProcessMetadataEventLog::new(2))
            .await
            .unwrap();
        timeout(
            Duration::from_secs(1),
            index.wait_until_caught_up(Duration::from_secs(5)),
        )
        .await
        .expect("an empty topic has no backlog to replay");
    }

    #[tokio::test]
    async fn catch_up_gives_up_when_the_replay_stops_making_progress() {
        let event_log = InProcessMetadataEventLog::new(1);
        let _seed = seeded_index(
            Arc::<InProcessMetadataEventLog>::clone(&event_log),
            Uuid::from_u128(7),
            &[SeededRange {
                object_key: "object-a",
                first_offset: 0,
                last_offset: 3,
            }],
        )
        .await;

        // A silent-but-open stream is what a dead partition fetch loop leaves
        // behind. Waiting on it forever would never flush again.
        let stalled = DisklessIndexLog::start(PacedReplayLog::new(event_log, ReplayPace::Never))
            .await
            .unwrap();
        assert!(
            !stalled
                .wait_until_caught_up(Duration::from_millis(50))
                .await
        );
    }

    #[tokio::test]
    async fn catch_up_keeps_waiting_while_a_slow_replay_still_advances() {
        let Backlog {
            event_log,
            topic_id,
            seed: _seed,
        } = two_range_backlog(1).await;

        // Every record lands well inside the stall window, but the replay as a
        // whole outlasts it: progress, not elapsed time, is what the gate
        // measures.
        let paced = DisklessIndexLog::start(PacedReplayLog::new(
            event_log,
            ReplayPace::OneEvery(Duration::from_millis(40)),
        ))
        .await
        .unwrap();
        assert!(paced.wait_until_caught_up(Duration::from_millis(400)).await);
        assert!(paced.cache().lock().await.flushed_frontier(topic_id, 0) == Some(8));
    }

    #[tokio::test]
    async fn catch_up_covers_a_record_appended_while_the_subscription_opens() {
        let event_log = InProcessMetadataEventLog::new(1);
        let topic_id = Uuid::from_u128(7);
        let _seed = seeded_index(
            Arc::<InProcessMetadataEventLog>::clone(&event_log),
            topic_id,
            &[SeededRange {
                object_key: "object-a",
                first_offset: 0,
                last_offset: 3,
            }],
        )
        .await;

        // The previous leader's in-flight flush lands while this projection is
        // subscribing. Pacing the replay keeps the assertion off the pump's
        // heels, so a gate that stopped one record short stays caught short.
        let racing = second_object_flush_record(topic_id);
        let racing_key = WalIndexKey::from(&racing.entries[0]).to_bytes();
        let restarted = DisklessIndexLog::start(
            RacingAppendLog::build()
                .inner(PacedReplayLog::new(
                    event_log,
                    ReplayPace::OneEvery(Duration::from_millis(40)),
                ))
                .key(racing_key)
                .event(racing.to_bytes().unwrap())
                .call(),
        )
        .await
        .unwrap();

        check_replayed_backlog(
            &restarted,
            topic_id,
            "the racing append must be part of the replay target",
        )
        .await;
    }

    /// The floor has to be a durable record, not a per-broker mirror.
    ///
    /// The range covering offsets 0-7 straddles a floor of 4, so retention
    /// must keep it -- and it is the newest range besides, which retention
    /// never expires. A projection rebuilt over the same index topic therefore
    /// replays that range in full, and only the floor record stops it from
    /// answering for the deleted offsets again.
    #[tokio::test]
    async fn a_delete_floor_survives_a_rebuilt_projection() {
        let event_log = InProcessMetadataEventLog::new(1);
        let topic_id = Uuid::from_u128(7);
        let seed = seeded_index(
            Arc::<InProcessMetadataEventLog>::clone(&event_log),
            topic_id,
            &[SeededRange {
                object_key: "object-a",
                first_offset: 0,
                last_offset: 7,
            }],
        )
        .await;
        seed.publish_delete_floor(topic_id, 0, 4, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(seed.cache().lock().await.delete_floor(topic_id, 0) == 4);

        let restarted =
            DisklessIndexLog::start(Arc::<InProcessMetadataEventLog>::clone(&event_log))
                .await
                .unwrap();
        assert!(restarted.wait_until_caught_up(Duration::from_secs(5)).await);

        let cache = restarted.cache();
        let cache = cache.lock().await;
        assert!(
            cache.delete_floor(topic_id, 0) == 4,
            "a restart must not resurrect the deleted offsets"
        );
        assert!(cache.earliest_covered(topic_id, 0) == Some(4));
        assert!(cache.lookup_fetch_range(topic_id, 0, 3, 4096).is_none());
        assert!(cache.lookup_fetch_range(topic_id, 0, 4, 4096).is_some());
        // The range itself is intact: retention may not expire it, because it
        // still holds every offset from 4 up.
        assert!(cache.flushed_frontier(topic_id, 0) == Some(8));
        drop(cache);

        // A tombstone takes the floor away with its topic.
        restarted.tombstone_delete_floor(topic_id, 0).await.unwrap();
        let after = DisklessIndexLog::start(event_log).await.unwrap();
        assert!(after.wait_until_caught_up(Duration::from_secs(5)).await);
        assert!(after.cache().lock().await.delete_floor(topic_id, 0) == 0);
    }

    /// A floor that only moves forward, so neither a stale client retry nor a
    /// compacted replay that delivers an older record last can widen what a
    /// consumer sees.
    #[tokio::test]
    async fn a_delete_floor_never_moves_backwards() {
        let event_log = InProcessMetadataEventLog::new(1);
        let topic_id = Uuid::from_u128(7);
        let index = DisklessIndexLog::start(event_log).await.unwrap();
        index
            .publish_delete_floor(topic_id, 0, 9, Duration::from_secs(5))
            .await
            .unwrap();

        let stale = index
            .publish_delete_floor(topic_id, 0, 2, Duration::from_millis(50))
            .await;

        // The publish itself is fine; the projection simply keeps the higher
        // floor, which is what the wait reports against.
        assert!(stale.is_ok());
        assert!(index.cache().lock().await.delete_floor(topic_id, 0) == 9);
    }

    #[tokio::test]
    async fn index_log_projects_published_flush_records() {
        let event_log = InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(event_log).await.unwrap();
        let topic_id = Uuid::from_u128(7);
        let record = WalFlushRecord {
            object_key: "object-a".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 0,
                last_offset: 3,
                byte_start: 6,
                byte_len: 10,
                max_timestamp_ms: 0,
            }],
        };

        index.publish_flush(&record).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if index.cache().lock().await.flushed_frontier(topic_id, 0) == Some(4) {
                    break;
                }
                task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(index.cache().lock().await.lookup(topic_id, 0, 2).is_some());
    }

    /// A one-byte range of `partition` in topic `topic`.
    fn range(topic: u128, partition: i32, first_offset: i64, last_offset: i64) -> WalIndexEntry {
        WalIndexEntry {
            topic_id: Uuid::from_u128(topic),
            partition,
            first_offset,
            last_offset,
            byte_start: 0,
            byte_len: 1,
            max_timestamp_ms: 0,
        }
    }

    #[test]
    fn publication_waves_take_one_range_per_partition_in_record_order() {
        struct Case {
            name: &'static str,
            entries: Vec<WalIndexEntry>,
            waves: Vec<Vec<usize>>,
        }
        let cases = [
            Case {
                name: "empty",
                entries: vec![],
                waves: vec![],
            },
            Case {
                name: "one partition stays serial",
                entries: vec![range(7, 0, 0, 1), range(7, 0, 2, 3), range(7, 0, 4, 4)],
                waves: vec![vec![0], vec![1], vec![2]],
            },
            Case {
                name: "distinct partitions share one wave",
                entries: vec![range(7, 0, 0, 1), range(7, 1, 0, 1), range(8, 0, 0, 1)],
                waves: vec![vec![0, 1, 2]],
            },
            Case {
                name: "interleaved partitions",
                entries: mixed_flush().entries,
                waves: vec![vec![0, 1, 3], vec![2, 4], vec![5]],
            },
        ];
        for Case {
            name,
            entries,
            waves,
        } in cases
        {
            assert!(publication_waves(&entries) == waves, "{name}");
        }
    }

    /// One object holding two ranges of partition 0, one of partition 1, and
    /// three of partition 2, interleaved the way a flush may order them.
    fn mixed_flush() -> WalFlushRecord {
        WalFlushRecord {
            object_key: "object-mixed".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![
                range(7, 0, 0, 1),
                range(7, 1, 0, 0),
                range(7, 0, 2, 3),
                range(7, 2, 0, 4),
                range(7, 2, 5, 6),
                range(7, 2, 7, 7),
            ],
        }
    }

    fn keys(record: &WalFlushRecord, indexes: &[usize]) -> Vec<WalIndexKey> {
        indexes
            .iter()
            .map(|&index| WalIndexKey::from(&record.entries[index]))
            .collect()
    }

    #[tokio::test]
    async fn flush_publishes_each_wave_as_one_group() {
        let log = GroupAckLog::build()
            .inner(InProcessMetadataEventLog::new(3))
            .call();
        let index = DisklessIndexLog::start(Arc::<GroupAckLog>::clone(&log))
            .await
            .unwrap();
        let record = mixed_flush();

        index.publish_flush(&record).await.unwrap();

        assert!(
            log.groups()
                == vec![
                    keys(&record, &[0, 1, 3]),
                    keys(&record, &[2, 4]),
                    keys(&record, &[5]),
                ]
        );
        assert!(
            index
                .wait_until_applied(&record, Duration::from_secs(1))
                .await
        );
    }

    /// A range of partition 2 fails in the second wave. The third wave, which
    /// holds the range after it, is never submitted, so the partition's durable
    /// ranges stay a prefix and a restarted projection resumes it at the gap
    /// rather than past it. A retry of the same record fills the gap.
    #[tokio::test]
    async fn a_failed_wave_leaves_each_partition_a_durable_prefix() {
        let inner = InProcessMetadataEventLog::new(3);
        let record = mixed_flush();
        let log = GroupAckLog::build()
            .inner(Arc::<InProcessMetadataEventLog>::clone(&inner))
            .failing(vec![WalIndexKey::from(&record.entries[4])])
            .call();
        let index = DisklessIndexLog::start(Arc::<GroupAckLog>::clone(&log))
            .await
            .unwrap();
        let topic_id = Uuid::from_u128(7);

        let error = index.publish_flush(&record).await.unwrap_err();

        assert!(error.to_string().contains("diskless index publish"));
        assert!(log.groups() == vec![keys(&record, &[0, 1, 3]), keys(&record, &[2, 4])]);
        let restarted = DisklessIndexLog::start(inner).await.unwrap();
        assert!(restarted.wait_until_caught_up(Duration::from_secs(5)).await);
        let frontiers = {
            let cache = restarted.cache();
            let cache = cache.lock().await;
            [0, 1, 2].map(|partition| cache.flushed_frontier(topic_id, partition))
        };
        assert!(frontiers == [Some(4), Some(1), Some(5)]);

        log.heal();
        index.publish_flush(&record).await.unwrap();
        assert!(
            index
                .wait_until_applied(&record, Duration::from_secs(1))
                .await
        );
        assert!(index.cache().lock().await.flushed_frontier(topic_id, 2) == Some(8));
    }

    #[tokio::test]
    async fn startup_surfaces_a_replay_fence_publish_failure() {
        let inner = InProcessMetadataEventLog::new(1);
        inner
            .publish(0, Bytes::from_static(b"record"))
            .await
            .unwrap();

        let Err(error) =
            DisklessIndexLog::start(TogglePublishLog::new(inner, PublishOutcome::Fail)).await
        else {
            panic!("the replay fence publish must fail");
        };
        assert!(error.to_string().contains("diskless index replay fence"));
    }

    /// An index with one projected object, the topic it is for, and the record
    /// that was applied.
    struct ProjectedObject {
        index: DisklessIndexLog,
        topic_id: Uuid,
        record: WalFlushRecord,
    }

    async fn projected_first_object(transport: Arc<dyn MetadataEventLog>) -> ProjectedObject {
        let index = DisklessIndexLog::start(transport).await.unwrap();
        let topic_id = Uuid::from_u128(7);
        let record = flush_record().topic_id(topic_id).call();
        index.publish_flush(&record).await.unwrap();
        assert!(
            index
                .wait_until_applied(&record, Duration::from_secs(1))
                .await
        );
        ProjectedObject {
            index,
            topic_id,
            record,
        }
    }

    #[tokio::test]
    async fn flush_and_tombstone_publish_failures_are_returned() {
        let transport =
            TogglePublishLog::new(InProcessMetadataEventLog::new(1), PublishOutcome::Succeed);
        let ProjectedObject {
            index,
            topic_id,
            record: _record,
        } = projected_first_object(Arc::<TogglePublishLog>::clone(&transport)).await;

        transport.set_outcome(PublishOutcome::Fail);

        let publish_error = index
            .publish_flush(&second_object_flush_record(topic_id))
            .await
            .unwrap_err();
        assert!(publish_error.to_string().contains("diskless index publish"));

        let tombstone_error = index.tombstone_topic(topic_id).await.unwrap_err();
        assert!(
            tombstone_error
                .to_string()
                .contains("diskless index tombstone")
        );
    }

    #[tokio::test]
    async fn consumed_tombstone_removes_the_projected_range() {
        let transport = InProcessMetadataEventLog::new(1);
        let ProjectedObject {
            index,
            topic_id,
            record,
        } = projected_first_object(Arc::<InProcessMetadataEventLog>::clone(&transport)).await;
        let key = WalIndexKey::from(&record.entries[0]);

        let bytes = key.to_bytes();
        transport
            .publish_keyed(
                index_partition(&bytes, transport.partition_count()).unwrap(),
                bytes,
                None,
            )
            .await
            .unwrap();

        timeout(Duration::from_secs(1), async {
            while index.cache().lock().await.lookup(topic_id, 0, 0).is_some() {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    /// A reservation record whose value names another topic than its key
    /// invalidates the projection. A fresh-view wait already in flight must
    /// then give up at once, as it does for every other invalid record, not
    /// sit out its timeout while another index partition lags.
    #[tokio::test(start_paused = true)]
    async fn a_mismatched_reservation_ends_a_fresh_view_wait_at_once() {
        let transport = InProcessMetadataEventLog::new(2);
        let index = DisklessIndexLog::start(HeldPartitionLog::new(
            Arc::<InProcessMetadataEventLog>::clone(&transport),
            1,
        ))
        .await
        .unwrap();
        let waiter = task::spawn({
            let index = index.clone();
            async move { index.wait_for_fresh_view(Duration::from_secs(300)).await }
        });
        let mut consumed = index.consumed_offsets.clone();
        timeout(
            Duration::from_secs(5),
            consumed.wait_for(|offsets| offsets[0] >= 0),
        )
        .await
        .unwrap()
        .unwrap();
        // Let the waiter see partition 0's fence and park again.
        for _ in 0..10 {
            task::yield_now().await;
        }

        let key = WalObjectReservationKey::new(Uuid::from_u128(5)).to_bytes();
        let floor_key = WalDeleteFloorKey::from_bytes(&key).unwrap();
        let mismatched = WalDeleteFloorRecord {
            topic_id: Uuid::from_u128(99),
            partition: floor_key.partition,
            floor: 0,
        }
        .to_bytes()
        .unwrap();
        transport
            .publish_keyed(0, key, Some(mismatched))
            .await
            .unwrap();

        let fresh = timeout(Duration::from_secs(5), waiter)
            .await
            .expect("an invalid projection must end the fresh-view wait")
            .unwrap();
        assert!(!fresh);
        assert!(!index.is_valid());
    }
    /// A removal that another broker publishes bumps the applied generation,
    /// so a projection waiter wakes for it as it does for an addition.
    #[tokio::test(start_paused = true)]
    async fn a_consumed_removal_wakes_a_projection_waiter() {
        type Removed = fn(&WalIndexCache) -> bool;

        let topic_id = Uuid::from_u128(7);
        let range = WalIndexKey::from(&flush_record().topic_id(topic_id).call().entries[0]);
        let floor = WalDeleteFloorKey {
            topic_id,
            partition: 0,
        };
        let cases: [(&str, Bytes, Removed); 2] = [
            ("range", range.to_bytes(), |cache| {
                cache.lookup(Uuid::from_u128(7), 0, 0).is_none()
            }),
            ("floor", floor.to_bytes(), |cache| {
                cache.delete_floor(Uuid::from_u128(7), 0) == 0
            }),
        ];
        for (case, tombstone, removed) in cases {
            let transport = InProcessMetadataEventLog::new(1);
            let ProjectedObject { index, .. } =
                projected_first_object(Arc::<InProcessMetadataEventLog>::clone(&transport)).await;
            index
                .publish_delete_floor(topic_id, 0, 5, Duration::from_secs(1))
                .await
                .unwrap();

            let (woke, published) = tokio::join!(
                index.wait_for_projection(Duration::from_secs(5), removed),
                transport.publish_keyed(0, tombstone, None),
            );
            published.unwrap();
            assert!(woke, "case {case}");
        }
    }
}

/// One event at a time through the pump, each case checked against the whole
/// observable state it leaves behind.
#[cfg(test)]
mod pump_tests {
    use {
        super::*,
        crate::diskless::wal_index::IndexedRange,
        assert2::assert,
        async_trait::async_trait,
        futures_util::stream,
        krabka_remote_storage_topic::{
            AssignmentHandle, InProcessMetadataEventLog, MetadataLogError,
        },
        tokio::{sync::mpsc, time::timeout},
        uuid::Uuid,
    };

    /// The offset of the replay fence the scripted log reports for
    /// partition 0, the only partition with a backlog.
    const REPLAY_TARGET: i64 = 100;
    const TOPIC: Uuid = Uuid::from_u128(7);
    const OTHER_TOPIC: Uuid = Uuid::from_u128(8);
    /// A `UUIDv4`, as diskless object names are.
    const OBJECT: Uuid = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001);

    /// A two-partition log whose subscription delivers exactly what the test
    /// sends. Partition 0 reports a backlog, so its replay is pending until
    /// [`REPLAY_TARGET`]; partition 1 is empty and carries the probe.
    struct ScriptedLog {
        inner: Arc<InProcessMetadataEventLog>,
        events: StdMutex<Option<mpsc::UnboundedReceiver<MetadataEventRecord>>>,
    }

    #[async_trait]
    impl MetadataEventLog for ScriptedLog {
        fn partition_count(&self) -> i32 { 2 }

        async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
            self.inner.publish(partition, event).await
        }

        async fn publish_keyed(
            &self,
            _partition: i32,
            _key: Bytes,
            _event: Option<Bytes>,
        ) -> Result<i64, MetadataLogError> {
            Ok(REPLAY_TARGET)
        }

        fn subscribe(
            &self,
            assignment: Vec<PartitionStart>,
        ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
            let (_, handle) = self.inner.subscribe(assignment);
            let events = self.events.lock().unwrap().take().expect("subscribed once");
            let scripted = stream::unfold(events, |mut events| async move {
                events.recv().await.map(|event| (event, events))
            });
            (Box::pin(scripted), handle)
        }

        async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> { Ok(vec![1, 0]) }
    }

    /// Everything the pump's work can be observed through.
    #[derive(Debug, PartialEq)]
    struct PumpState {
        ranges: Vec<IndexedRange>,
        /// `(topic, partition, floor)` for [`TOPIC`] and the reservation
        /// namespace.
        floors: Vec<(Uuid, i32, i64)>,
        reservations: Vec<Uuid>,
        valid: bool,
        decode_failures: u64,
        applied: u64,
        consumed: Vec<i64>,
        progress: ReplayProgress,
    }

    #[bon::builder]
    fn pump_state(
        #[builder(default)] ranges: Vec<IndexedRange>,
        #[builder(default)] floors: Vec<(Uuid, i32, i64)>,
        #[builder(default)] reservations: Vec<Uuid>,
        #[builder(default)] decode_failures: u64,
        #[builder(default)] applied: u64,
        /// The highest offset consumed on partition 0.
        #[builder(default = -1)]
        consumed: i64,
        /// Partition 0 events delivered before the replay target.
        #[builder(default)]
        delivered: u64,
        #[builder(default)] caught_up: bool,
    ) -> PumpState {
        PumpState {
            ranges,
            floors,
            reservations,
            valid: decode_failures == 0,
            decode_failures,
            applied,
            // The probe always lands on partition 1 at offset 0.
            consumed: vec![consumed, 0],
            progress: ReplayProgress {
                slowest_pending: if caught_up { u64::MAX } else { delivered },
                caught_up,
                invalid: decode_failures != 0,
            },
        }
    }

    fn reservation_topic() -> Uuid {
        WalDeleteFloorKey::from_bytes(&WalObjectReservationKey::new(OBJECT).to_bytes())
            .unwrap()
            .topic_id
    }

    async fn snapshot(index: &DisklessIndexLog, decode_failures: u64) -> PumpState {
        let cache = index.cache();
        let cache = cache.lock().await;
        let mut floors = Vec::new();
        for topic in [TOPIC, reservation_topic()] {
            let mut partitions = cache.delete_floor_partitions(topic);
            partitions.sort_unstable();
            floors.extend(
                partitions
                    .into_iter()
                    .map(|partition| (topic, partition, cache.delete_floor(topic, partition))),
            );
        }
        let mut reservations: Vec<Uuid> = cache.reserved_objects().into_iter().collect();
        reservations.sort_unstable();
        PumpState {
            ranges: cache.timestamp_ranges(TOPIC, 0),
            floors,
            reservations,
            valid: index.is_valid(),
            decode_failures,
            applied: *index.applied.borrow(),
            consumed: index.consumed_offsets.borrow().clone(),
            progress: *index.progress.borrow(),
        }
    }

    /// Feed `events` to a fresh pump, then a no-op probe on partition 1, and
    /// report the state once the probe is consumed: the pump handles events in
    /// order, so every earlier event has been handled by then.
    async fn pump_state_after(events: Vec<MetadataEventRecord>) -> PumpState {
        let (sender, receiver) = mpsc::unbounded_channel();
        let log = Arc::new(ScriptedLog {
            inner: InProcessMetadataEventLog::new(2),
            events: StdMutex::new(Some(receiver)),
        });
        let metrics = BrokerMetrics::new();
        let failures = metrics.diskless_wal_index_decode_failures_total.clone();
        let index = DisklessIndexLog::start_with_cache(
            log,
            Arc::new(Mutex::new(WalIndexCache::default())),
            metrics,
        )
        .await
        .unwrap();
        for event in events {
            sender.send(event).unwrap();
        }
        sender
            .send(MetadataEventRecord {
                partition: 1,
                offset: 0,
                key: Some(Bytes::from_static(REPLAY_FENCE_KEY)),
                payload: Bytes::new(),
                tombstone: false,
            })
            .unwrap();
        let mut consumed = index.consumed_offsets.clone();
        timeout(
            Duration::from_secs(5),
            consumed.wait_for(|offsets| offsets[1] >= 0),
        )
        .await
        .unwrap()
        .unwrap();
        snapshot(&index, failures.get()).await
    }

    /// A partition-0 record at `offset`; `None` payload is a tombstone.
    fn event(offset: i64, key: Option<Bytes>, payload: Option<Bytes>) -> MetadataEventRecord {
        MetadataEventRecord {
            partition: 0,
            offset,
            key,
            tombstone: payload.is_none(),
            payload: payload.unwrap_or_default(),
        }
    }

    fn entry() -> WalIndexEntry {
        WalIndexEntry {
            topic_id: TOPIC,
            partition: 0,
            first_offset: 0,
            last_offset: 3,
            byte_start: 0,
            byte_len: 4,
            max_timestamp_ms: 0,
        }
    }

    fn range_key() -> Bytes { WalIndexKey::from(&entry()).to_bytes() }

    fn range_value() -> Bytes { WalFlushRecord::keyed_entry("object-a", entry()).unwrap().1 }

    fn indexed_range() -> IndexedRange {
        IndexedRange {
            object_key: "object-a".into(),
            entry: entry(),
        }
    }

    fn floor_key() -> Bytes {
        WalDeleteFloorKey {
            topic_id: TOPIC,
            partition: 0,
        }
        .to_bytes()
    }

    fn floor_value(topic_id: Uuid, partition: i32, floor: i64) -> Bytes {
        WalDeleteFloorRecord {
            topic_id,
            partition,
            floor,
        }
        .to_bytes()
        .unwrap()
    }

    fn reservation_key() -> Bytes { WalObjectReservationKey::new(OBJECT).to_bytes() }

    fn reservation_value(topic_id: Uuid, floor: i64) -> Bytes {
        floor_value(topic_id, WAL_OBJECT_RESERVATION_PARTITION, floor)
    }

    fn fence_key() -> Bytes { Bytes::from_static(REPLAY_FENCE_KEY) }

    /// A range key of key version 7, which this build does not read.
    fn unknown_version_key() -> Bytes {
        let mut key = range_key().to_vec();
        key[..2].copy_from_slice(&7_i16.to_be_bytes());
        Bytes::from(key)
    }

    #[tokio::test]
    async fn each_index_event_leaves_the_expected_pump_state() {
        struct Case {
            name: &'static str,
            events: Vec<MetadataEventRecord>,
            expected: PumpState,
        }
        let reservation = reservation_value(reservation_topic(), 0);
        let cases = [
            Case {
                name: "range",
                events: vec![event(0, Some(range_key()), Some(range_value()))],
                expected: pump_state()
                    .ranges(vec![indexed_range()])
                    .applied(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "floor",
                events: vec![event(0, Some(floor_key()), Some(floor_value(TOPIC, 0, 5)))],
                expected: pump_state()
                    .floors(vec![(TOPIC, 0, 5)])
                    .applied(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "reservation",
                events: vec![event(0, Some(reservation_key()), Some(reservation.clone()))],
                expected: pump_state()
                    .reservations(vec![OBJECT])
                    .applied(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "reservation with a nonzero floor",
                events: vec![event(
                    0,
                    Some(reservation_key()),
                    Some(reservation_value(reservation_topic(), 1)),
                )],
                expected: pump_state()
                    .decode_failures(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "reservation whose record names another key",
                events: vec![event(
                    0,
                    Some(reservation_key()),
                    Some(reservation_value(OTHER_TOPIC, 0)),
                )],
                expected: pump_state()
                    .decode_failures(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "range tombstone",
                events: vec![
                    event(0, Some(range_key()), Some(range_value())),
                    event(1, Some(range_key()), None),
                ],
                expected: pump_state().applied(2).consumed(1).delivered(2).call(),
            },
            Case {
                name: "floor tombstone",
                events: vec![
                    event(0, Some(floor_key()), Some(floor_value(TOPIC, 0, 5))),
                    event(1, Some(floor_key()), None),
                ],
                expected: pump_state().applied(2).consumed(1).delivered(2).call(),
            },
            Case {
                name: "reservation tombstone",
                events: vec![
                    event(0, Some(reservation_key()), Some(reservation.clone())),
                    event(1, Some(reservation_key()), None),
                ],
                expected: pump_state().applied(2).consumed(1).delivered(2).call(),
            },
            Case {
                name: "replay fence",
                events: vec![event(0, Some(fence_key()), Some(Bytes::new()))],
                expected: pump_state().consumed(0).delivered(1).call(),
            },
            Case {
                name: "replay fence at the replay target",
                events: vec![event(REPLAY_TARGET, Some(fence_key()), Some(Bytes::new()))],
                expected: pump_state().consumed(REPLAY_TARGET).caught_up(true).call(),
            },
            Case {
                name: "replay fence tombstone",
                events: vec![event(0, Some(fence_key()), None)],
                expected: pump_state().consumed(0).delivered(1).call(),
            },
            Case {
                name: "unkeyed tombstone",
                events: vec![event(0, None, None)],
                expected: pump_state().consumed(0).delivered(1).call(),
            },
            Case {
                name: "unknown key version",
                events: vec![event(0, Some(unknown_version_key()), Some(range_value()))],
                expected: pump_state()
                    .decode_failures(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "unknown key version tombstone",
                events: vec![event(0, Some(unknown_version_key()), None)],
                expected: pump_state()
                    .decode_failures(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "unkeyed record",
                events: vec![event(0, None, Some(range_value()))],
                expected: pump_state()
                    .decode_failures(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "undecodable range payload",
                events: vec![event(0, Some(range_key()), Some(Bytes::from_static(b"xx")))],
                expected: pump_state()
                    .decode_failures(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "undecodable floor payload",
                events: vec![event(0, Some(floor_key()), Some(range_value()))],
                expected: pump_state()
                    .decode_failures(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "undecodable reservation payload",
                events: vec![event(0, Some(reservation_key()), Some(range_value()))],
                expected: pump_state()
                    .decode_failures(1)
                    .consumed(0)
                    .delivered(1)
                    .call(),
            },
            Case {
                name: "invalid projection keeps applying later records",
                events: vec![
                    event(0, None, Some(range_value())),
                    event(1, Some(floor_key()), Some(floor_value(TOPIC, 0, 5))),
                ],
                expected: pump_state()
                    .floors(vec![(TOPIC, 0, 5)])
                    .decode_failures(1)
                    .applied(1)
                    .consumed(1)
                    .delivered(2)
                    .call(),
            },
        ];
        for Case {
            name,
            events,
            expected,
        } in cases
        {
            let actual = pump_state_after(events).await;
            assert!(actual == expected, "case {name}");
        }
    }
}
