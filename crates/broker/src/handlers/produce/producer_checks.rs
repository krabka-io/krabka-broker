//! The two producer-identity gates that run between the leadership gate and
//! the append: the KIP-1319 transactional verify, and the idempotent-producer
//! dedup that answers a retry with the offset it already assigned.

use std::time::Duration;

use krabka_protocol::owned::produce_response::PartitionProduceResponse;

use super::{ACKS_ALL, INVALID_OFFSET, durability_frontier, prepare::PreparedBatch};
use crate::codes;

pub(super) async fn validate_transactional_produce(
    batch: &PreparedBatch,
    coordinator: &crate::txn::coordinator::TxnCoordinator,
    image: &krabka_metadata::MetadataImage,
    topic_name: &str,
    partition: i32,
) -> Option<i16> {
    if !batch.attributes.is_transactional() {
        return None;
    }
    if batch.producer_id < 0 {
        return Some(codes::INVALID_PRODUCER_ID_MAPPING);
    }
    let transactional_id = coordinator.tid_for_pid(krabka_log::ProducerId(batch.producer_id));
    let Some(transactional_id) = transactional_id else {
        // Produce carries no transactional ID. This broker can lead the data
        // partition while another broker coordinates the transaction, so a
        // missing local PID mapping is not evidence of an invalid producer.
        return None;
    };
    let topic_partition = crate::txn::state::TopicPartition {
        topic: topic_name.to_string(),
        partition: krabka_ids::PartitionIndex(partition),
    };
    let version = crate::txn::version::resolve_txn_version(image);
    let code = coordinator
        .register_partitions(
            &transactional_id,
            krabka_log::ProducerId(batch.producer_id),
            batch.producer_epoch,
            vec![topic_partition],
            version,
        )
        .await;
    if code == codes::NONE {
        return None;
    }
    Some(code)
}

pub(super) async fn handle_duplicate(
    batch: &PreparedBatch,
    producer_state: &crate::producer_state::ProducerState,
    partition: &crate::partition::Partition,
    topic_name: &str,
    partition_index: i32,
    acks: i16,
    timeout: Duration,
) -> Option<PartitionProduceResponse> {
    if batch.producer_id < 0 {
        return None;
    }
    let decision = producer_state
        .check(
            topic_name,
            krabka_ids::PartitionIndex(partition_index),
            batch.producer_id,
            batch.producer_epoch,
            batch.base_sequence,
            batch.last_offset_delta,
        )
        .await;
    // A recognized retry is an accepted produce, so its row carries the
    // partition's real log start offset just like a fresh append's does. The
    // two refusals below happen before any append and keep the
    // `UNKNOWN_LOG_APPEND_INFO` sentinel. A raw `Produce v8` replayed against
    // `apache/kafka:4.3.1` on a partition whose low watermark `DeleteRecords`
    // had moved off 0 answered the duplicate with that same real value, not
    // with the sentinel.
    let (error_code, base_offset, log_start_offset) = match decision {
        crate::producer_state::Decision::Duplicate { base_offset } => {
            let Some(target) = durability_frontier(base_offset, batch.last_offset_delta) else {
                return Some(PartitionProduceResponse {
                    index: partition_index,
                    error_code: codes::INVALID_RECORD,
                    base_offset: -1,
                    ..Default::default()
                });
            };
            let error_code = if acks == ACKS_ALL {
                let deadline = std::time::Instant::now() + timeout;
                if partition.await_hw_at_least(target, deadline).await.is_ok() {
                    codes::NONE
                } else {
                    codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND
                }
            } else {
                codes::NONE
            };
            (error_code, base_offset, partition.log_start_offset().0)
        }
        crate::producer_state::Decision::OutOfOrder => (
            codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
            INVALID_OFFSET,
            INVALID_OFFSET,
        ),
        crate::producer_state::Decision::Fenced => (
            codes::INVALID_PRODUCER_EPOCH,
            INVALID_OFFSET,
            INVALID_OFFSET,
        ),
        crate::producer_state::Decision::Append => return None,
    };
    Some(PartitionProduceResponse {
        index: partition_index,
        error_code,
        base_offset,
        log_start_offset,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::{assert, check};
    use bytes::Bytes;
    use krabka_compression::RecordDecompressionPolicy;
    use krabka_protocol::records::{Record, RecordBatch};
    use uuid::Uuid;

    use super::{PreparedBatch, validate_transactional_produce};
    use crate::{
        codes,
        handlers::produce::{
            framing::{FramedPartition, PartitionPayload},
            leadership::BrokerProducePolicy,
            pipeline::{PartitionInput, PartitionServices, process_partition},
            test_support::{encode_batch, image_with_topic},
        },
    };

    fn transactional_batch(producer_id: i64, producer_epoch: i16) -> PreparedBatch {
        PreparedBatch {
            attributes: krabka_protocol::records::Attributes::default().with_transactional(true),
            last_offset_delta: 0,
            max_timestamp: 0,
            producer_id,
            producer_epoch,
            base_sequence: 0,
            source: crate::handlers::produce::prepare::PreparedSource::Owned(RecordBatch::default()),
        }
    }

    #[tokio::test]
    async fn transactional_produce_rejects_malformed_producers() {
        let coordinator = crate::txn::coordinator::TxnCoordinator::new(
            krabka_audit::NodeId(1),
            Arc::new(crate::partition_registry::PartitionRegistry::new()),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            1,
            krabka_units::mebibytes(1),
        );
        let image = krabka_metadata::MetadataImage::new(Uuid::nil());

        for producer_id in [-1, i64::MIN] {
            let code = validate_transactional_produce(
                &transactional_batch(producer_id, 0),
                &coordinator,
                &image,
                "orders",
                0,
            )
            .await;
            check!(code == Some(codes::INVALID_PRODUCER_ID_MAPPING));
        }
    }

    #[tokio::test]
    async fn transactional_produce_allows_a_remote_coordinator() {
        let coordinator = crate::txn::coordinator::TxnCoordinator::new(
            krabka_audit::NodeId(1),
            Arc::new(crate::partition_registry::PartitionRegistry::new()),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            1,
            krabka_units::mebibytes(1),
        );
        let image = krabka_metadata::MetadataImage::new(Uuid::nil());

        let code = validate_transactional_produce(
            &transactional_batch(7, 0),
            &coordinator,
            &image,
            "orders",
            0,
        )
        .await;

        check!(code.is_none());
    }

    #[tokio::test]
    async fn transactional_produce_persists_the_exact_partition_on_every_retry() {
        let directory = tempfile::tempdir().expect("tempdir");
        let image = image_with_topic(crate::txn::bootstrap::TOPIC, &[1]);
        let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
        let coordinator = crate::txn::coordinator::TxnCoordinator::new(
            krabka_audit::NodeId(1),
            Arc::clone(&partitions),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            1,
            krabka_units::mebibytes(1),
        );
        let partition_dir =
            crate::log_dir::partition_dir(directory.path(), crate::txn::bootstrap::TOPIC, 0);
        std::fs::create_dir_all(&partition_dir).expect("create transaction-state directory");
        let log = krabka_log::Log::open(&partition_dir, krabka_log::LogConfig::default())
            .expect("open transaction-state log");
        let transaction_partition = crate::broker::spawn_partition(
            crate::txn::bootstrap::TOPIC.to_string(),
            krabka_ids::PartitionIndex(0),
            directory.path().to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        partitions.insert(
            crate::txn::bootstrap::TOPIC.to_string(),
            krabka_ids::PartitionIndex(0),
            Arc::clone(&transaction_partition),
        );
        coordinator.refresh_leader_partitions(&image).await;
        coordinator
            .put(
                crate::txn::state::TxnEntry::new_empty(
                    "tid-a".into(),
                    krabka_log::ProducerId(7),
                    i16::MAX,
                    60_000,
                    0,
                ),
                crate::txn::version::TxnVersion::Classic,
            )
            .await
            .expect("seed transaction");

        for expected_end in [2, 3] {
            let code = validate_transactional_produce(
                &transactional_batch(7, i16::MAX),
                &coordinator,
                &image,
                "orders",
                i32::MAX,
            )
            .await;
            check!(code.is_none());
            check!(transaction_partition.log_end_offset().0 == expected_end);
        }
        let stored = coordinator.get("tid-a").expect("transaction entry");
        let stored = stored.lock().await;
        assert!(
            stored
                .partitions
                .contains(&crate::txn::state::TopicPartition {
                    topic: "orders".into(),
                    partition: krabka_ids::PartitionIndex(i32::MAX),
                })
        );
        assert!(
            !stored
                .partitions
                .contains(&crate::txn::state::TopicPartition {
                    topic: "orders".into(),
                    partition: krabka_ids::PartitionIndex(0),
                })
        );
        drop(stored);

        let stale = validate_transactional_produce(
            &transactional_batch(7, i16::MAX - 1),
            &coordinator,
            &image,
            "payments",
            0,
        )
        .await;
        check!(stale == Some(codes::INVALID_PRODUCER_EPOCH));
        check!(transaction_partition.log_end_offset().0 == 3);
    }

    /// An idempotent retry, `Decision::Duplicate`, under `acks=all` waits
    /// again for the HW to reach the duplicate's *last offset + 1* before it
    /// claims success.
    ///
    /// The duplicate spans offsets 0..=2, so the durability target is 3, which
    /// is `base_offset 0 + last_offset_delta 2 + 1`. When the HW is stuck at
    /// 2, the wait times out and gives `NOT_ENOUGH_REPLICAS_AFTER_APPEND`. The
    /// `+ 1` matters. A mutant that flips it to `- 1` would target offset 1,
    /// which HW 2 already satisfies, and would wrongly return `NONE`.
    #[tokio::test]
    async fn duplicate_acks_all_waits_for_last_offset_plus_one() {
        use krabka_protocol::owned::produce_response::PartitionProduceResponse;

        let dir = tempfile::tempdir().unwrap();
        let image = Arc::new(image_with_topic("orders", &[1]));
        let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
        let txn_coordinator = Arc::new(crate::txn::coordinator::TxnCoordinator::new(
            krabka_audit::NodeId(1),
            Arc::clone(&partitions),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            50,
            krabka_units::mebibytes(1),
        ));
        let producer_state = Arc::new(crate::producer_state::ProducerState::new());
        let log_dir_status = crate::log_dir_status::LogDirRegistry::default();
        let metrics = crate::metrics::BrokerMetrics::new();

        // Materialize the local leader replica for "orders"-0.
        let part_dir = crate::log_dir::partition_dir(dir.path(), "orders", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).unwrap();
        let part = crate::broker::spawn_partition(
            "orders".to_string(),
            krabka_ids::PartitionIndex(0),
            dir.path().to_path_buf(),
            log,
            log_dir_status.clone(),
            Arc::clone(&producer_state),
            false,
        );
        let record = image.partition("orders", 0).expect("partition");
        part.install_replication_target(Some(Uuid::nil()), record.leader.0, record.leader_epoch.0)
            .await;
        part.install_isr(&record.isr, &record.replicas, record.leader)
            .await;
        // Push LEO to 3 so the HW can be clamped to 2 (one below the target).
        {
            let mut batch = RecordBatch {
                last_offset_delta: 2,
                records: (0..3)
                    .map(|i| Record {
                        offset_delta: i,
                        value: Some(Bytes::from_static(b"v")),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            part.log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .append(&mut batch)
                .expect("seed source records");
        }
        assert!(part.log_end_offset() == krabka_log::Offset(3));
        part.set_follower_hw(krabka_log::Offset(2)).await;
        assert!(part.high_watermark().await == krabka_log::Offset(2));
        partitions.insert("orders".to_string(), krabka_ids::PartitionIndex(0), part);

        // Pre-seed the dedup tracker so the incoming batch is a Duplicate whose
        // recorded base_offset is 0 and span is 0..=2.
        let pid: i64 = 7777;
        producer_state
            .commit(
                "orders",
                krabka_ids::PartitionIndex(0),
                (pid, 0),
                (0, 2),
                (0, 0),
            )
            .await;

        // Incoming (retried) batch: same pid/epoch/base_sequence/span.
        let payload = encode_batch(&RecordBatch {
            producer_id: pid,
            producer_epoch: 0,
            base_sequence: 0,
            last_offset_delta: 2,
            records: (0..3)
                .map(|i| Record {
                    offset_delta: i,
                    value: Some(Bytes::from_static(b"v")),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        });

        let resp: PartitionProduceResponse = process_partition(
            PartitionInput {
                schema: None,
                part_data: FramedPartition {
                    index: 0,
                    payload: PartitionPayload::Slice(payload),
                },
                topic_compression: None,
                max_message_bytes: krabka_log::DEFAULT_MAX_MESSAGE_SIZE,
                delivery: None,
                topic_name: "orders".into(),
                freeze: crate::freeze::resolve::FreezeMutationResolution::Admit,
                txn_id_denied: false,
                acks: -1,
                timeout: Duration::from_millis(50),
            },
            PartitionServices {
                schema_validator: None,
                partitions: &partitions,
                txn_coordinator: &txn_coordinator,
                producer_state: &producer_state,
                log_dir_status: &log_dir_status,
                image: &image,
                broker_policy: BrokerProducePolicy {
                    node_id: krabka_audit::NodeId(1),
                    default_min_insync_replicas: 1,
                    is_witness: false,
                },
                record_decompression_policy: RecordDecompressionPolicy::default(),
                metrics: &metrics,
                phases: &crate::metrics::RequestPhases::default(),
            },
        )
        .await
        .expect("process partition");

        check!(resp.base_offset == 0);
        check!(
            resp.error_code == crate::codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            "HW 2 < target 3 must time out; a `-1` mutant would target offset 1 and return NONE"
        );
    }
}
