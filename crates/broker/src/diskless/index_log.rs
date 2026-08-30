//! Projection of committed diskless WAL index events.

use std::{collections::HashMap, sync::Arc};

use futures_util::StreamExt;
use krabka_remote_storage_topic::{MetadataEventLog, PartitionStart};
use tokio::sync::{Mutex, watch};

use super::wal_index::{WalFlushRecord, WalIndexCache};

pub(crate) const DISKLESS_WAL_INDEX_TOPIC: &str = "__diskless_wal_index";

#[derive(Clone)]
pub(crate) struct DisklessIndexLog {
    log: Arc<dyn MetadataEventLog>,
    cache: Arc<Mutex<WalIndexCache>>,
    caught_up: watch::Receiver<bool>,
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
    /// The end offsets are read *before* the subscription so the replay is
    /// guaranteed to cover them. They become the catch-up target that
    /// [`Self::wait_until_caught_up`] reports against: until the pump has
    /// walked that far, the projection is a partial view of what object
    /// storage already holds.
    ///
    /// # Errors
    ///
    /// Returns an error when the index topic's end offsets cannot be read.
    pub(crate) async fn start_with_cache(
        log: Arc<dyn MetadataEventLog>,
        cache: Arc<Mutex<WalIndexCache>>,
    ) -> Result<Self, crate::error::BrokerError> {
        let high_water_marks = log.high_water_marks().await.map_err(|error| {
            crate::error::BrokerError::Txn(format!("diskless index end offsets: {error}"))
        })?;
        let mut pending = replay_targets(log.partition_count(), &high_water_marks);
        let (caught_up_tx, caught_up_rx) = watch::channel(pending.is_empty());

        let starts = (0..log.partition_count())
            .map(|partition| PartitionStart {
                partition,
                start_offset: 0,
            })
            .collect();
        let (mut stream, _assignment) = log.subscribe(starts);
        let pump_cache = cache.clone();
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                if let Ok(record) = WalFlushRecord::from_bytes(&event.payload) {
                    pump_cache.lock().await.apply(&record);
                }
                // A record that fails to decode still advances the replay:
                // the gate tracks position in the topic, not content.
                if pending
                    .get(&event.partition)
                    .is_some_and(|target| event.offset >= *target)
                {
                    pending.remove(&event.partition);
                    if pending.is_empty() {
                        let _ = caught_up_tx.send(true);
                    }
                }
            }
        });
        Ok(Self {
            log,
            cache,
            caught_up: caught_up_rx,
        })
    }

    /// Resolve once the projection has replayed every record the index topic
    /// held when this log started. Resolves to `false` when the pump stopped
    /// first, which leaves the projection permanently incomplete.
    pub(crate) async fn wait_until_caught_up(&self) -> bool {
        let mut caught_up = self.caught_up.clone();
        caught_up.wait_for(|done| *done).await.is_ok()
    }

    #[must_use]
    pub(crate) fn cache(&self) -> Arc<Mutex<WalIndexCache>> {
        self.cache.clone()
    }

    pub(crate) async fn publish_flush(
        &self,
        record: &WalFlushRecord,
    ) -> Result<i64, crate::error::BrokerError> {
        let bytes = record.to_bytes().map_err(crate::error::BrokerError::Txn)?;
        self.log
            .publish(
                index_partition(&record.object_key, self.log.partition_count()),
                bytes,
            )
            .await
            .map_err(|error| {
                crate::error::BrokerError::Txn(format!("diskless index publish: {error}"))
            })
    }
}

/// The last offset each non-empty partition must deliver before the replay
/// is complete, keyed by partition. An empty partition has nothing to
/// replay and so contributes no target.
fn replay_targets(partition_count: i32, high_water_marks: &[i64]) -> HashMap<i32, i64> {
    (0..partition_count)
        .filter_map(|partition| {
            let index = usize::try_from(partition).ok()?;
            let high_water_mark = high_water_marks.get(index).copied().unwrap_or(0);
            (high_water_mark > 0).then_some((partition, high_water_mark - 1))
        })
        .collect()
}

fn index_partition(key: &str, partitions: i32) -> i32 {
    let hash = key.bytes().fold(0u32, |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(u32::from(byte))
    });
    i32::try_from(hash % u32::try_from(partitions).expect("positive partition count"))
        .expect("index partition fits i32")
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_remote_storage_topic::InProcessMetadataEventLog;
    use tokio::time::{Duration, timeout};
    use uuid::Uuid;

    use super::*;
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
        assert!(restarted.wait_until_caught_up().await);
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
        timeout(Duration::from_secs(1), index.wait_until_caught_up())
            .await
            .expect("an empty topic has no backlog to replay");
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
