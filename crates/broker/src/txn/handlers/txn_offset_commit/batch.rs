//! The transactional append to `__consumer_offsets` that carries a
//! `TxnOffsetCommit`'s offsets.
//!
//! The rows are the ordinary `OffsetCommitKey` / `OffsetCommitValue` pair, but
//! the batch around them is stamped `is_transactional=true` with the
//! producer's (pid, epoch), so the log's LSO machinery withholds the offsets
//! until a commit or abort marker resolves the transaction.

use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_protocol::{
    owned::txn_offset_commit_request::TxnOffsetCommitRequest,
    records::{Attributes, Record, RecordBatch},
};

use crate::{
    codes,
    coordinator::{bootstrap::OFFSETS_TOPIC, persistence::OffsetCommitValue},
};

/// Append the transactional offset records to `__consumer_offsets`.
/// The offsets partition's `WriteTxnMarkers` handler materializes these records
/// into the owning group actor after the commit marker is durable. This keeps
/// visibility on the group-coordinator broker even when the transaction
/// coordinator is a different broker.
pub(super) async fn append_txn_batch(
    req: &TxnOffsetCommitRequest,
    partitions: &std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    offsets_partition: i32,
    now_ms: i64,
    denied_topics: &std::collections::HashSet<String>,
) -> Result<(), i16> {
    let mut batch = RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        max_timestamp: now_ms,
        producer_id: req.producer_id,
        producer_epoch: req.producer_epoch,
        ..RecordBatch::default()
    };
    let mut delta: i32 = 0;
    for topic in &req.topics {
        if denied_topics.contains(&topic.name) {
            continue;
        }
        for part in &topic.partitions {
            let value = OffsetCommitValue {
                offset: Offset(part.committed_offset),
                leader_epoch: part.committed_leader_epoch,
                metadata: part.committed_metadata.clone().unwrap_or_default(),
                commit_timestamp_ms: now_ms,
            };
            batch.records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(OffsetCommitValue::encode_key(
                    &req.group_id,
                    &topic.name,
                    part.partition_index,
                )),
                value: Some(value.encode_value()),
                ..Default::default()
            });
            delta += 1;
        }
    }

    // If every topic was denied, there's nothing to append; succeed silently.
    if batch.records.is_empty() {
        return Ok(());
    }

    batch.last_offset_delta = (delta - 1).max(0);

    let Some(part_handle) = partitions.get(OFFSETS_TOPIC, PartitionIndex(offsets_partition)) else {
        // __consumer_offsets not hosted here — report NOT_COORDINATOR.
        return Err(codes::NOT_COORDINATOR);
    };
    // `produce_batch` drives the single-writer task and returns the
    // assigned base_offset; we don't need it here.
    part_handle
        .produce_batch(batch)
        .await
        .map(|_| ())
        .map_err(|e| {
            tracing::error!(
                group = %req.group_id,
                tid   = %req.transactional_id,
                error = %e,
                "TxnOffsetCommit: produce_batch failed"
            );
            codes::UNKNOWN_SERVER_ERROR
        })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path, sync::Arc};

    use assert2::{assert, check};
    use krabka_log::{Log, LogConfig};

    use super::*;
    use crate::{
        coordinator::bootstrap::OFFSETS_PARTITION, partition_registry::PartitionRegistry,
        txn::handlers::txn_offset_commit::test_support::request,
    };

    fn open_offsets_partition(registry: &PartitionRegistry, log_dir: &Path) {
        let part_dir = crate::log_dir::partition_dir(log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
        std::fs::create_dir_all(&part_dir).expect("create offsets partition dir");
        let log = Log::open(&part_dir, LogConfig::default()).expect("open offsets log");
        let part = crate::broker::spawn_partition(
            OFFSETS_TOPIC.to_string(),
            PartitionIndex(OFFSETS_PARTITION),
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        registry.insert(
            OFFSETS_TOPIC.to_string(),
            PartitionIndex(OFFSETS_PARTITION),
            part,
        );
    }

    #[tokio::test]
    async fn append_txn_batch_writes_transactional_offset_records() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        open_offsets_partition(&registry, dir.path());
        let req = request();

        append_txn_batch(&req, &registry, OFFSETS_PARTITION, 12_345, &HashSet::new())
            .await
            .expect("append batch");

        let part = registry
            .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
            .expect("offsets partition");
        let log = part.log.lock().expect("lock offsets log");
        let read = log
            .read(krabka_log::Offset(0), krabka_units::mebibytes(1))
            .expect("read offsets log");
        assert!(read.batches.len() == 1);
        let batch = &read.batches[0];
        check!(batch.attributes.is_transactional());
        check!(batch.max_timestamp == 12_345);
        check!(batch.producer_id == 47);
        check!(batch.producer_epoch == 5);
        check!(batch.last_offset_delta == 1);
        let record_rows: Vec<_> = batch
            .records
            .iter()
            .map(|r| {
                (
                    r.offset_delta,
                    r.timestamp_delta,
                    r.key.is_some(),
                    r.value.is_some(),
                )
            })
            .collect();
        assert!(record_rows == vec![(0, 0, true, true), (1, 0, true, true)]);
    }

    #[tokio::test]
    async fn append_txn_batch_skips_denied_topics_without_appending() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        open_offsets_partition(&registry, dir.path());
        let req = request();
        let denied = HashSet::from(["orders".to_string()]);

        append_txn_batch(&req, &registry, OFFSETS_PARTITION, 12_345, &denied)
            .await
            .expect("all denied succeeds");
        let part = registry
            .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
            .expect("offsets partition");
        let log = part.log.lock().expect("lock offsets log");
        let read = log
            .read(krabka_log::Offset(0), krabka_units::mebibytes(1))
            .expect("read offsets log");
        assert!(read.batches.is_empty());
    }

    #[tokio::test]
    async fn append_txn_batch_returns_not_coordinator_when_offsets_partition_missing() {
        let registry = Arc::new(PartitionRegistry::new());
        let err = append_txn_batch(
            &request(),
            &registry,
            OFFSETS_PARTITION,
            12_345,
            &HashSet::new(),
        )
        .await
        .expect_err("missing offsets partition");

        assert!(err == codes::NOT_COORDINATOR);
    }
}
