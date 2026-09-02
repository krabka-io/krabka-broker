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

/// What one `TxnOffsetCommit` durably wrote: the offsets-log position of its
/// records, and the `(topic, partition)` keys they cover.
#[derive(Debug)]
pub(super) struct AppendedTxnOffsets {
    /// Base offset the batch was assigned in `__consumer_offsets`.
    pub(super) written_at: i64,
    pub(super) keys: Vec<(String, i32)>,
}

/// Append the transactional offset records to `__consumer_offsets`, and
/// report where they landed and which `(topic, partition)` keys they cover.
/// `None` means every topic was denied and nothing was appended.
///
/// The offsets partition's `WriteTxnMarkers` handler materializes these records
/// into the owning group actor after the commit marker is durable. This keeps
/// visibility on the group-coordinator broker even when the transaction
/// coordinator is a different broker.
///
/// The returned keys are the ones the caller marks pending on the group actor
/// for KIP-447. They come from the same walk that builds the batch, so a key
/// can never be marked pending without a durable record behind it for the
/// transaction's marker to find again. The base offset travels with them
/// because it is what orders the mark against that marker.
pub(super) async fn append_txn_batch(
    req: &TxnOffsetCommitRequest,
    partitions: &std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    offsets_partition: i32,
    now_ms: i64,
    denied_topics: &std::collections::HashSet<String>,
) -> Result<Option<AppendedTxnOffsets>, i16> {
    let mut batch = RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        max_timestamp: now_ms,
        producer_id: req.producer_id,
        producer_epoch: req.producer_epoch,
        // TxnOffsetCommit records are broker-generated, so the request has no
        // client sequence. Use the stable first sequence so the log retains
        // the producer epoch needed to fence the completion marker.
        base_sequence: 0,
        ..RecordBatch::default()
    };
    let mut delta: i32 = 0;
    let mut keys: Vec<(String, i32)> = Vec::new();
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
                // `TxnOffsetCommit` has no `retention_time_ms` field at any
                // version, so a transactional commit always takes the
                // broker-wide retention.
                expire_timestamp_ms: None,
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
            keys.push((topic.name.clone(), part.partition_index));
            delta += 1;
        }
    }

    // If every topic was denied, there's nothing to append; succeed silently.
    if batch.records.is_empty() {
        return Ok(None);
    }

    batch.last_offset_delta = (delta - 1).max(0);

    let Some(part_handle) = partitions.get(OFFSETS_TOPIC, PartitionIndex(offsets_partition)) else {
        // __consumer_offsets not hosted here — report NOT_COORDINATOR.
        return Err(codes::NOT_COORDINATOR);
    };
    // `produce_batch` drives the single-writer task and returns the assigned
    // base offset, which is the log position the KIP-447 mark is ordered by.
    part_handle
        .produce_batch(batch)
        .await
        .map(|written_at| {
            Some(AppendedTxnOffsets {
                written_at: written_at.get(),
                keys,
            })
        })
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

        let appended =
            append_txn_batch(&req, &registry, OFFSETS_PARTITION, 12_345, &HashSet::new())
                .await
                .expect("append batch")
                .expect("records appended");
        check!(appended.written_at == 0);
        check!(appended.keys == vec![("orders".to_string(), 2), ("orders".to_string(), 3)]);

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
        check!(batch.base_sequence == 0);
        check!(batch.last_offset_delta == 1);
        check!(log.transaction_marker_state(krabka_log::ProducerId(47)) == (5, -1, true));
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
        let denied = maplit::hashset! {"orders".to_string()};

        let appended = append_txn_batch(&req, &registry, OFFSETS_PARTITION, 12_345, &denied)
            .await
            .expect("all denied succeeds");
        check!(appended.is_none());
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
