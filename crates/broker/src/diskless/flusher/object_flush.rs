//! One flush of the committed tails into object storage: build a WAL object
//! from the flushable partitions, upload it, publish its `WalFlushRecord` on
//! the diskless index log, wait for the projection to catch up, and trim the
//! local logs behind the durable frontier.

#[cfg(any(test, feature = "test-helpers"))]
use std::collections::HashMap;
use std::{sync::Arc, time::Duration};

use krabka_log::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use super::{FlushConfig, FlushPartition};
use crate::diskless::{
    index_log::DisklessIndexLog,
    wal_index::{WalFlushRecord, WalIndexCache, WalIndexEntry},
    wal_object::WalObjectBuilder,
};

#[cfg(any(test, feature = "test-helpers"))]
static PUT_FAILURES: std::sync::LazyLock<std::sync::Mutex<HashMap<i32, u64>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[cfg(any(test, feature = "test-helpers"))]
#[must_use]
pub(crate) fn put_failure_count(broker_id: i32) -> u64 {
    *PUT_FAILURES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&broker_id)
        .unwrap_or(&0)
}

pub(crate) async fn flush_once(
    object_store: Arc<dyn ObjectStore>,
    broker_id: i32,
    index_log: &DisklessIndexLog,
    cache: Arc<AsyncMutex<WalIndexCache>>,
    partitions: &[FlushPartition],
    config: &FlushConfig,
) -> Result<Option<WalFlushRecord>, crate::error::BrokerError> {
    let mut builder = WalObjectBuilder::new();
    for partition in partitions {
        let remaining = config
            .max_size
            .bytes_usize()
            .saturating_sub(builder.body_len());
        if remaining == 0 {
            break;
        }
        let start = cache
            .lock()
            .await
            .flushed_frontier(partition.topic_id, partition.handle.index.get());
        let raw = {
            let log = partition
                .handle
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let start = start.unwrap_or_else(|| log.log_start_offset().0);
            log.read_raw(
                Offset(start),
                partition.high_watermark,
                ByteSize::from_bytes(u64::try_from(remaining).unwrap_or(u64::MAX)),
            )
            .map_err(crate::error::BrokerError::from)?
        };
        let Some(last_offset) = raw.last_offset else {
            continue;
        };
        builder.append_run(
            partition.topic_id,
            partition.handle.index.get(),
            raw.start_offset.0,
            last_offset.0,
            &raw.bytes,
        );
    }

    if builder.is_empty() {
        return Ok(None);
    }
    let object_key = format!("diskless-wal/{broker_id}/{}.ckwl", Uuid::new_v4());
    let object = builder.finish();
    if let Err(error) = object_store
        .put(
            &Path::from(object_key.clone()),
            PutPayload::from(object.clone()),
        )
        .await
    {
        #[cfg(any(test, feature = "test-helpers"))]
        {
            let mut failures = PUT_FAILURES
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *failures.entry(broker_id).or_default() += 1;
        }
        return Err(crate::error::BrokerError::Txn(format!(
            "diskless wal put: {error}"
        )));
    }

    let entries = batch_index_entries(&object)?;
    let record = WalFlushRecord {
        object_key,
        format_version: 1,
        entries,
    };
    index_log.publish_flush(&record).await?;
    wait_for_committed_projection(cache.clone(), &record, config.index_projection_timeout).await?;

    if let Some(lag) = config.trim_safety_lag {
        for partition in partitions {
            if let Some(frontier) = cache
                .lock()
                .await
                .flushed_frontier(partition.topic_id, partition.handle.index.get())
            {
                let hw_trim_floor = partition.high_watermark.0.saturating_sub(lag.max(0));
                let trim_to = frontier.min(hw_trim_floor);
                let current_start = partition
                    .handle
                    .log
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .log_start_offset()
                    .0;
                if trim_to > current_start {
                    partition.handle.trim_to_offset(Offset(trim_to)).await?;
                }
            }
        }
    }

    Ok(Some(record))
}

fn batch_index_entries(
    object: &bytes::Bytes,
) -> Result<Vec<WalIndexEntry>, crate::error::BrokerError> {
    let mut entries = Vec::new();
    for run in crate::diskless::wal_object::parse_wal_object(object)
        .map_err(|error| crate::error::BrokerError::Txn(error.to_string()))?
    {
        let mut byte_start = usize::try_from(run.byte_start).map_err(|_| {
            crate::error::BrokerError::Txn("diskless WAL byte range overflow".into())
        })?;
        let run_end = byte_start
            .checked_add(usize::try_from(run.byte_len).unwrap_or(usize::MAX))
            .ok_or_else(|| {
                crate::error::BrokerError::Txn("diskless WAL byte range overflow".into())
            })?;
        while byte_start < run_end {
            let mut cursor = &object[byte_start..run_end];
            let batch = RecordBatch::decode(&mut cursor).map_err(|error| {
                crate::error::BrokerError::Txn(format!("diskless WAL batch: {error}"))
            })?;
            let byte_len = run_end - byte_start - cursor.len();
            entries.push(WalIndexEntry {
                topic_id: run.topic_id,
                partition: run.partition,
                first_offset: batch.base_offset,
                last_offset: batch
                    .base_offset
                    .checked_add(i64::from(batch.last_offset_delta))
                    .ok_or_else(|| {
                        crate::error::BrokerError::Txn("diskless WAL batch offset overflow".into())
                    })?,
                byte_start: u64::try_from(byte_start).unwrap_or(u64::MAX),
                byte_len: u32::try_from(byte_len).map_err(|_| {
                    crate::error::BrokerError::Txn("diskless WAL batch exceeds 4 GiB".into())
                })?,
            });
            byte_start = byte_start.checked_add(byte_len).ok_or_else(|| {
                crate::error::BrokerError::Txn("diskless WAL byte range overflow".into())
            })?;
        }
    }
    Ok(entries)
}

async fn wait_for_committed_projection(
    cache: Arc<AsyncMutex<WalIndexCache>>,
    record: &WalFlushRecord,
    timeout: Duration,
) -> Result<(), crate::error::BrokerError> {
    tokio::time::timeout(timeout, async {
        loop {
            {
                let cache = cache.lock().await;
                if cache.contains_record(record) {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| crate::error::BrokerError::Txn("diskless wal index projection timed out".into()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use krabka_metadata::NodeId;
    use krabka_protocol::records::{Record, RecordBatch};
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    use super::*;
    use crate::diskless::flusher::test_support::test_partition;

    #[test]
    fn flush_index_has_one_range_per_batch() {
        let topic_id = Uuid::from_u128(10);
        let batches = [4, 5].map(|base_offset| RecordBatch {
            base_offset,
            records: vec![Record {
                value: Some(Bytes::from_static(b"v")),
                ..Default::default()
            }],
            ..Default::default()
        });
        let mut run = BytesMut::new();
        for batch in &batches {
            batch.encode(&mut run).unwrap();
        }
        let object = crate::diskless::wal_object::WalObjectBuilder::new()
            .finish_with_run(topic_id, 0, 4, 5, &run);

        let entries = batch_index_entries(&object).unwrap();

        assert!(entries.len() == 2);
        assert!(entries[0].first_offset == 4 && entries[0].last_offset == 4);
        assert!(entries[1].first_offset == 5 && entries[1].last_offset == 5);
        assert!(entries[0].byte_start + u64::from(entries[0].byte_len) == entries[1].byte_start);
    }

    #[tokio::test]
    async fn flusher_writes_object_and_publishes_index() {
        let dir = tempdir().unwrap();
        let handle = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let event_log = krabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(event_log).await.unwrap();
        let topic_id = Uuid::from_u128(11);
        let cache = index.cache();
        let record = flush_once(
            store.clone(),
            7,
            &index,
            cache.clone(),
            &[FlushPartition {
                topic_id,
                handle: Arc::clone(&handle),
                high_watermark: Offset(3),
            }],
            &FlushConfig::default(),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(record.entries[0].first_offset == 0);
        assert!(record.entries[0].last_offset == 2);
        assert!(cache.lock().await.flushed_frontier(topic_id, 0) == Some(3));
        assert!(store.head(&Path::from(record.object_key)).await.is_ok());
        assert!(handle.log.lock().unwrap().log_start_offset() == Offset(2));
    }

    #[tokio::test]
    async fn flusher_skips_noop_trim_when_writer_is_stopped() {
        let dir = tempdir().unwrap();
        let handle = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let writer = handle
            .writer_handle
            .lock()
            .unwrap()
            .take()
            .expect("partition writer");
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let index = DisklessIndexLog::start(
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        )
        .await
        .unwrap();
        let cache = index.cache();
        let topic_id = Uuid::from_u128(11);
        let record = flush_once(
            store,
            7,
            &index,
            cache,
            &[FlushPartition {
                topic_id,
                handle: Arc::clone(&handle),
                high_watermark: Offset(3),
            }],
            &FlushConfig {
                trim_safety_lag: Some(3),
                ..FlushConfig::default()
            },
        )
        .await
        .expect("a no-op trim must not depend on the partition writer")
        .expect("the durable prefix is flushed");

        assert!(record.entries[0].last_offset == 2);
        assert!(handle.log.lock().unwrap().log_start_offset() == Offset(0));
    }

    #[tokio::test]
    async fn committed_projection_wait_requires_the_record_to_be_applied() {
        let cache = Arc::new(AsyncMutex::new(WalIndexCache::default()));
        let record = WalFlushRecord {
            object_key: "diskless-wal/test.ckwl".into(),
            format_version: 1,
            entries: vec![WalIndexEntry {
                topic_id: Uuid::from_u128(11),
                partition: 0,
                first_offset: 0,
                last_offset: 2,
                byte_start: 0,
                byte_len: 1,
            }],
        };

        let error =
            wait_for_committed_projection(Arc::clone(&cache), &record, Duration::from_millis(10))
                .await
                .expect_err("an unapplied record must time out");
        assert!(error.to_string().contains("projection timed out"));

        cache.lock().await.apply(&record);
        wait_for_committed_projection(cache, &record, Duration::from_secs(1))
            .await
            .expect("the exact applied record is visible");
    }

    #[tokio::test]
    async fn combined_object_stops_after_size_budget() {
        let dir = tempdir().unwrap();
        let first = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let second = test_partition(dir.path(), "orders", 1, true, NodeId(1));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let index = DisklessIndexLog::start(
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        )
        .await
        .unwrap();
        let cache = index.cache();
        let config = FlushConfig {
            max_size: ByteSize::from_bytes(1),
            trim_safety_lag: None,
            ..FlushConfig::default()
        };

        let record = flush_once(
            store,
            7,
            &index,
            cache,
            &[
                FlushPartition {
                    topic_id: Uuid::from_u128(11),
                    handle: first,
                    high_watermark: Offset(3),
                },
                FlushPartition {
                    topic_id: Uuid::from_u128(11),
                    handle: second,
                    high_watermark: Offset(3),
                },
            ],
            &config,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(record.entries.len() == 1);
        assert!(record.entries[0].partition == 0);
    }
}
