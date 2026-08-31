//! Projection of committed diskless WAL index events.

use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::StreamExt;
use krabka_remote_storage_topic::{MetadataEventLog, PartitionStart};
use tokio::sync::{Mutex, watch};

use super::wal_index::{WalFlushRecord, WalIndexCache, WalIndexKey};

#[cfg(test)]
pub(crate) mod test_support;

pub(crate) const DISKLESS_WAL_INDEX_TOPIC: &str = "__diskless_wal_index";

/// How far the pump has walked the replay it started with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayProgress {
    /// Records delivered for whichever still-unreplayed partition has seen
    /// the fewest. It only rises when the slowest partition moves, so one
    /// partition going silent freezes it however busy the rest are.
    slowest_pending: u64,
    /// Whether every partition has reached its replay target.
    caught_up: bool,
}

#[derive(Clone)]
pub(crate) struct DisklessIndexLog {
    log: Arc<dyn MetadataEventLog>,
    cache: Arc<Mutex<WalIndexCache>>,
    progress: watch::Receiver<ReplayProgress>,
    applied: watch::Receiver<u64>,
}

impl DisklessIndexLog {
    #[cfg(test)]
    pub(crate) async fn start(
        log: Arc<dyn MetadataEventLog>,
    ) -> Result<Self, crate::error::BrokerError> {
        let cache = Arc::new(Mutex::new(WalIndexCache::default()));
        Self::start_with_cache(log, cache).await
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
    ) -> Result<Self, crate::error::BrokerError> {
        let starts = (0..log.partition_count())
            .map(|partition| PartitionStart {
                partition,
                start_offset: 0,
            })
            .collect();
        let (mut stream, _assignment) = log.subscribe(starts);

        let high_water_marks = log.high_water_marks().await.map_err(|error| {
            crate::error::BrokerError::Txn(format!("diskless index end offsets: {error}"))
        })?;
        let mut pending = HashMap::new();
        for partition in 0..log.partition_count() {
            let index = usize::try_from(partition).expect("partition non-negative");
            if high_water_marks.get(index).copied().unwrap_or(0) == 0 {
                continue;
            }
            let offset = log
                .publish_keyed(
                    partition,
                    Bytes::from_static(b"__krabka_diskless_replay_fence"),
                    Some(Bytes::new()),
                )
                .await
                .map_err(|error| {
                    crate::error::BrokerError::Txn(format!("diskless index replay fence: {error}"))
                })?;
            pending.insert(partition, offset);
        }
        let (progress_tx, progress_rx) = watch::channel(ReplayProgress {
            slowest_pending: 0,
            caught_up: pending.is_empty(),
        });
        let (applied_tx, applied_rx) = watch::channel(0u64);

        let pump_cache = cache.clone();
        tokio::spawn(async move {
            let mut delivered: HashMap<i32, u64> =
                pending.keys().map(|partition| (*partition, 0)).collect();
            while let Some(event) = stream.next().await {
                if event.tombstone {
                    if let Some(key) = event.key.as_deref().and_then(WalIndexKey::from_bytes) {
                        pump_cache.lock().await.remove(key);
                    }
                } else if let Ok(record) = WalFlushRecord::from_bytes(&event.payload) {
                    let mut cache = pump_cache.lock().await;
                    match event.key.as_deref() {
                        Some(bytes) => {
                            if let Some(key) = WalIndexKey::from_bytes(bytes) {
                                cache.apply_keyed(key, &record);
                            }
                        }
                        None => cache.apply(&record),
                    }
                    drop(cache);
                    applied_tx.send_modify(|generation| {
                        *generation = generation.wrapping_add(1);
                    });
                }
                // A record that fails to decode still advances the replay:
                // the gate tracks position in the topic, not content.
                let Some(target) = pending.get(&event.partition).copied() else {
                    continue;
                };
                *delivered.entry(event.partition).or_default() += 1;
                if event.offset >= target {
                    pending.remove(&event.partition);
                    delivered.remove(&event.partition);
                }
                let next = ReplayProgress {
                    slowest_pending: delivered.values().copied().min().unwrap_or(u64::MAX),
                    caught_up: pending.is_empty(),
                };
                // Notify only on a real change, so a waiter's stall timer is
                // not reset by a busy partition while another one is stuck.
                progress_tx.send_if_modified(|progress| {
                    let changed = *progress != next;
                    *progress = next;
                    changed
                });
            }
        });
        Ok(Self {
            log,
            cache,
            progress: progress_rx,
            applied: applied_rx,
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
            let caught_up = progress.borrow_and_update().caught_up;
            if caught_up {
                self.cache.lock().await.finish_legacy_replay();
                return true;
            }
            match tokio::time::timeout(stall_timeout, progress.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => return false,
            }
        }
    }

    #[must_use]
    pub(crate) fn cache(&self) -> Arc<Mutex<WalIndexCache>> {
        self.cache.clone()
    }

    /// Wait until `record` appears in the committed projection.
    pub(crate) async fn wait_until_applied(
        &self,
        record: &WalFlushRecord,
        timeout: Duration,
    ) -> bool {
        let mut applied = self.applied.clone();
        tokio::time::timeout(timeout, async {
            loop {
                if self.cache.lock().await.contains_record(record) {
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

    pub(crate) async fn publish_flush(
        &self,
        record: &WalFlushRecord,
    ) -> Result<i64, crate::error::BrokerError> {
        let mut last_offset = -1;
        for entry in &record.entries {
            let key = WalIndexKey::from(entry).to_bytes();
            let bytes = WalFlushRecord {
                object_key: record.object_key.clone(),
                format_version: record.format_version,
                entries: vec![entry.clone()],
            }
            .to_bytes()
            .map_err(crate::error::BrokerError::Txn)?;
            last_offset = self
                .log
                .publish_keyed(
                    index_partition(&key, self.log.partition_count()),
                    key,
                    Some(bytes),
                )
                .await
                .map_err(|error| {
                    crate::error::BrokerError::Txn(format!("diskless index publish: {error}"))
                })?;
        }
        Ok(last_offset)
    }

    /// Tombstone every projected range for a deleted topic.
    pub(crate) async fn tombstone_topic(
        &self,
        topic_id: uuid::Uuid,
    ) -> Result<(), crate::error::BrokerError> {
        let keys = self.cache.lock().await.keys_for_topic(topic_id);
        for key in keys {
            let bytes = key.to_bytes();
            self.log
                .publish_keyed(
                    index_partition(&bytes, self.log.partition_count()),
                    bytes,
                    None,
                )
                .await
                .map_err(|error| {
                    crate::error::BrokerError::Txn(format!("diskless index tombstone: {error}"))
                })?;
            self.cache.lock().await.remove(key);
        }
        Ok(())
    }
}

fn index_partition(key: &[u8], partitions: i32) -> i32 {
    let hash = key.iter().copied().fold(0u32, |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(u32::from(byte))
    });
    i32::try_from(hash % u32::try_from(partitions).expect("positive partition count"))
        .expect("index partition fits i32")
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_remote_storage_topic::InProcessMetadataEventLog;
    use tokio::time::timeout;
    use uuid::Uuid;

    use super::{
        test_support::{PacedReplayLog, RacingAppendLog, ReplayPace},
        *,
    };
    use crate::diskless::wal_index::WalIndexEntry;

    fn flush_record(object_key: &str, topic_id: Uuid, first: i64, last: i64) -> WalFlushRecord {
        WalFlushRecord {
            object_key: object_key.into(),
            format_version: 1,
            entries: vec![WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: first,
                last_offset: last,
                byte_start: 0,
                byte_len: 10,
            }],
        }
    }

    #[tokio::test]
    async fn catch_up_resolves_only_once_the_topic_backlog_is_projected() {
        let event_log = InProcessMetadataEventLog::new(2);
        let topic_id = Uuid::from_u128(7);
        let seed = DisklessIndexLog::start(event_log.clone()).await.unwrap();
        for (key, first, last) in [("object-a", 0, 3), ("object-b", 4, 7)] {
            seed.publish_flush(&flush_record(key, topic_id, first, last))
                .await
                .unwrap();
        }

        // A restart against the now-populated topic.
        let restarted = DisklessIndexLog::start(event_log).await.unwrap();
        assert!(restarted.wait_until_caught_up(Duration::from_secs(5)).await);
        assert!(
            restarted.cache().lock().await.flushed_frontier(topic_id, 0) == Some(8),
            "catch-up must not resolve before the whole backlog is projected"
        );
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
        let seed = DisklessIndexLog::start(event_log.clone()).await.unwrap();
        seed.publish_flush(&flush_record("object-a", Uuid::from_u128(7), 0, 3))
            .await
            .unwrap();

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
        let event_log = InProcessMetadataEventLog::new(1);
        let topic_id = Uuid::from_u128(7);
        let seed = DisklessIndexLog::start(event_log.clone()).await.unwrap();
        for (key, first, last) in [("object-a", 0, 3), ("object-b", 4, 7)] {
            seed.publish_flush(&flush_record(key, topic_id, first, last))
                .await
                .unwrap();
        }

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
        let seed = DisklessIndexLog::start(event_log.clone()).await.unwrap();
        seed.publish_flush(&flush_record("object-a", topic_id, 0, 3))
            .await
            .unwrap();

        // The previous leader's in-flight flush lands while this projection is
        // subscribing. Pacing the replay keeps the assertion off the pump's
        // heels, so a gate that stopped one record short stays caught short.
        let racing = flush_record("object-b", topic_id, 4, 7).to_bytes().unwrap();
        let restarted = DisklessIndexLog::start(RacingAppendLog::new(
            PacedReplayLog::new(event_log, ReplayPace::OneEvery(Duration::from_millis(40))),
            0,
            racing,
        ))
        .await
        .unwrap();

        assert!(restarted.wait_until_caught_up(Duration::from_secs(5)).await);
        assert!(
            restarted.cache().lock().await.flushed_frontier(topic_id, 0) == Some(8),
            "the racing append must be part of the replay target"
        );
    }

    #[tokio::test]
    async fn index_log_projects_published_flush_records() {
        let event_log = InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(event_log).await.unwrap();
        let topic_id = Uuid::from_u128(7);
        let record = WalFlushRecord {
            object_key: "object-a".into(),
            format_version: 1,
            entries: vec![WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 0,
                last_offset: 3,
                byte_start: 6,
                byte_len: 10,
            }],
        };

        index.publish_flush(&record).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if index.cache().lock().await.flushed_frontier(topic_id, 0) == Some(4) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(index.cache().lock().await.lookup(topic_id, 0, 2).is_some());
    }

}
