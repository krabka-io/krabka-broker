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
    error::BrokerError,
    partition::ProduceBatchError,
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
/// `None` means every row was denied or unknown, and nothing was appended.
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
///
/// `producer_check` is the KIP-890 check the log runs under its append lock,
/// with the guard the producer's verification started, and a refusal is
/// answered as Kafka's append throws it. `record_topic_ids` says whether the
/// offset records carry the topic ids the handler resolved, which Kafka 4.3.1
/// does not record.
pub(super) async fn append_txn_batch(
    req: &TxnOffsetCommitRequest,
    partitions: &std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    offsets_partition: i32,
    now_ms: i64,
    (denied_topics, unknown_rows): (
        &std::collections::HashSet<String>,
        &std::collections::HashSet<(String, i32)>,
    ),
    (producer_check, record_topic_ids): (crate::partition::ProducerAppendCheck, bool),
) -> Result<Option<AppendedTxnOffsets>, i16> {
    let mut batch = RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        base_timestamp: now_ms,
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
            if unknown_rows.contains(&(topic.name.clone(), part.partition_index)) {
                continue;
            }
            let value = OffsetCommitValue {
                offset: Offset(part.committed_offset),
                leader_epoch: part.committed_leader_epoch,
                metadata: part.committed_metadata.clone().unwrap_or_default(),
                commit_timestamp_ms: now_ms,
                // `TxnOffsetCommit` has no `retention_time_ms` field at any
                // version, so a transactional commit always takes the
                // broker-wide retention.
                expire_timestamp_ms: None,
                // The id the handler resolved the topic to, as Kafka trunk's
                // `OffsetAndMetadata.fromRequest` keeps it (KIP-1319). 4.3.1
                // keeps the zero id, which is no id.
                topic_id: Some(uuid::Uuid::from_bytes(topic.topic_id.0))
                    .filter(|id| record_topic_ids && !id.is_nil()),
            };
            batch.records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(
                    OffsetCommitValue::encode_key(&req.group_id, &topic.name, part.partition_index)
                        .map_err(|error| {
                            tracing::warn!(
                                group_id = %req.group_id,
                                %error,
                                "transactional offset commit key is not encodable",
                            );
                            codes::UNKNOWN_SERVER_ERROR
                        })?,
                ),
                value: Some(value.encode_value()),
                ..Default::default()
            });
            keys.push((topic.name.clone(), part.partition_index));
            delta += 1;
        }
    }

    // If every row was denied or unknown, there's nothing to append; succeed
    // silently.
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
        .produce_batch_checked(batch, Some(producer_check))
        .await
        .map(|written_at| {
            Some(AppendedTxnOffsets {
                written_at: written_at.get(),
                keys,
            })
        })
        .map_err(|e| {
            let error = match e {
                ProduceBatchError::Rejected(error) => error,
                ProduceBatchError::Indeterminate(error) => BrokerError::Txn(error),
            };
            tracing::error!(
                group = %req.group_id,
                tid   = %req.transactional_id,
                %error,
                "TxnOffsetCommit: produce_batch failed"
            );
            // A refusal of the producer check is the exception Kafka's append
            // throws: INVALID_PRODUCER_EPOCH, INVALID_TXN_STATE. Anything
            // else is unexpected.
            match error {
                BrokerError::TransactionAppend(_) => codes::from_broker_error(&error),
                _ => codes::UNKNOWN_SERVER_ERROR,
            }
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
            OFFSETS_TOPIC.into(),
            PartitionIndex(OFFSETS_PARTITION),
            part,
        );
    }

    /// The check a verification of `req`'s producer on the offsets partition
    /// hands the append, as `verification::verify_producer` starts it.
    async fn verified(
        registry: &PartitionRegistry,
        req: &TxnOffsetCommitRequest,
    ) -> crate::partition::ProducerAppendCheck {
        let part = registry
            .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
            .expect("offsets partition");
        let batch = krabka_log::TransactionalBatch {
            producer_id: krabka_log::ProducerId(req.producer_id),
            producer_epoch: req.producer_epoch,
            base_sequence: 0,
            is_transactional: true,
            is_control: false,
        };
        let guard = part
            .start_transaction_verification(batch, false, (0, i64::MAX))
            .await
            .expect("verification starts");
        crate::partition::ProducerAppendCheck { batch, guard }
    }

    #[tokio::test]
    async fn append_txn_batch_writes_transactional_offset_records() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        open_offsets_partition(&registry, dir.path());
        let req = request();

        let appended = append_txn_batch(
            &req,
            &registry,
            OFFSETS_PARTITION,
            12_345,
            (&HashSet::new(), &HashSet::new()),
            (verified(&registry, &req).await, false),
        )
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
        check!(log.offset_for_timestamp(12_345) == Some((Offset(0), 12_345)));
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

        let appended = append_txn_batch(
            &req,
            &registry,
            OFFSETS_PARTITION,
            12_345,
            (&denied, &HashSet::new()),
            (verified(&registry, &req).await, false),
        )
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
        let req = request();
        let unverified = crate::partition::ProducerAppendCheck {
            batch: krabka_log::TransactionalBatch {
                producer_id: krabka_log::ProducerId(req.producer_id),
                producer_epoch: req.producer_epoch,
                base_sequence: 0,
                is_transactional: true,
                is_control: false,
            },
            guard: krabka_log::VerificationGuard::SENTINEL,
        };
        let err = append_txn_batch(
            &req,
            &registry,
            OFFSETS_PARTITION,
            12_345,
            (&HashSet::new(), &HashSet::new()),
            (unverified, false),
        )
        .await
        .expect_err("missing offsets partition");

        assert!(err == codes::NOT_COORDINATOR);
    }

    #[tokio::test]
    async fn append_txn_batch_skips_unknown_rows_without_appending_them() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        open_offsets_partition(&registry, dir.path());
        let req = request();
        let unknown = maplit::hashset! {("orders".to_string(), 3)};

        let appended = append_txn_batch(
            &req,
            &registry,
            OFFSETS_PARTITION,
            12_345,
            (&HashSet::new(), &unknown),
            (verified(&registry, &req).await, false),
        )
        .await
        .expect("append batch")
        .expect("one row still appended");
        check!(appended.keys == vec![("orders".to_string(), 2)]);

        let part = registry
            .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
            .expect("offsets partition");
        let log = part.log.lock().expect("lock offsets log");
        let read = log
            .read(krabka_log::Offset(0), krabka_units::mebibytes(1))
            .expect("read offsets log");
        assert!(read.batches.len() == 1);
        assert!(read.batches[0].records.len() == 1);
    }

    /// Kafka 4.3.1 records the zero topic id for a transactional commit
    /// (`OffsetAndMetadata.fromRequest(partition, now)`), and trunk records the
    /// id its `KafkaApis` resolved.
    #[tokio::test]
    async fn the_topic_id_is_recorded_only_when_the_caller_asks_for_it() {
        let topic_id = uuid::Uuid::from_u128(0xfeed);
        // (label, record the topic id, the id the offset record carries)
        for (mode, record_topic_ids, recorded) in
            [("4.3.1", false, None), ("trunk", true, Some(topic_id))]
        {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let registry = Arc::new(PartitionRegistry::new());
            open_offsets_partition(&registry, dir.path());
            let mut req = request();
            req.topics[0].topic_id = krabka_protocol::primitives::uuid::Uuid(topic_id.into_bytes());

            append_txn_batch(
                &req,
                &registry,
                OFFSETS_PARTITION,
                12_345,
                (&HashSet::new(), &HashSet::new()),
                (verified(&registry, &req).await, record_topic_ids),
            )
            .await
            .expect("append batch")
            .expect("records appended");

            let part = registry
                .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
                .expect("offsets partition");
            let log = part.log.lock().expect("lock offsets log");
            let read = log
                .read(krabka_log::Offset(0), krabka_units::mebibytes(1))
                .expect("read offsets log");
            let recorded_ids: Vec<Option<uuid::Uuid>> = read.batches[0]
                .records
                .iter()
                .map(|record| {
                    OffsetCommitValue::decode_value(record.value.as_deref().expect("a value"))
                        .expect("decode the offset record")
                        .topic_id
                })
                .collect();
            assert!(recorded_ids == vec![recorded, recorded], "{mode}");
        }
    }

    /// The log runs its producer check under the append lock, so an append the
    /// verification did not admit never lands: a stale epoch is
    /// `INVALID_PRODUCER_EPOCH`, and a producer with no verified transaction is
    /// `INVALID_TXN_STATE`.
    #[tokio::test]
    async fn the_append_refuses_a_producer_the_log_check_does_not_admit() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        open_offsets_partition(&registry, dir.path());
        let req = request();
        append_txn_batch(
            &req,
            &registry,
            OFFSETS_PARTITION,
            12_345,
            (&HashSet::new(), &HashSet::new()),
            (verified(&registry, &req).await, false),
        )
        .await
        .expect("the verified producer appends");

        let mut stale = request();
        stale.producer_epoch = req.producer_epoch - 1;
        let mut other = request();
        other.producer_id += 1;
        // (label, request, expected code)
        let cases = [
            ("a stale epoch", stale, codes::INVALID_PRODUCER_EPOCH),
            ("an unverified producer", other, codes::INVALID_TXN_STATE),
        ];
        for (label, req, expected) in cases {
            let unverified = crate::partition::ProducerAppendCheck {
                batch: krabka_log::TransactionalBatch {
                    producer_id: krabka_log::ProducerId(req.producer_id),
                    producer_epoch: req.producer_epoch,
                    base_sequence: 0,
                    is_transactional: true,
                    is_control: false,
                },
                guard: krabka_log::VerificationGuard::SENTINEL,
            };
            let refused = append_txn_batch(
                &req,
                &registry,
                OFFSETS_PARTITION,
                12_345,
                (&HashSet::new(), &HashSet::new()),
                (unverified, false),
            )
            .await
            .expect_err(label);
            assert!(refused == expected, "{label}");
        }

        let part = registry
            .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
            .expect("offsets partition");
        let log = part.log.lock().expect("lock offsets log");
        let read = log
            .read(krabka_log::Offset(0), krabka_units::mebibytes(1))
            .expect("read offsets log");
        assert!(read.batches.len() == 1, "only the verified append landed");
    }
}
