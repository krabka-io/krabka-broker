//! The two producer-identity gates that run between the leadership gate and
//! the append: the KIP-1319 transactional verify, and the idempotent-producer
//! dedup that answers a retry with the offset it already assigned.

use krabka_log::Offset;
use krabka_protocol::owned::produce_response::PartitionProduceResponse;

use super::{ACKS_ALL, INVALID_OFFSET, durability_frontier, prepare::PreparedBatch};
use crate::codes;

/// What the idempotent-producer dedup gate decided for one batch.
pub(super) enum DedupOutcome {
    /// Not a duplicate: the batch goes on to the append.
    Append,
    /// The gate answered the batch without an append: a recognized retry under
    /// `acks != -1`, an out-of-order sequence, or a fenced epoch.
    Answered(PartitionProduceResponse),
    /// A recognized retry under `acks=-1`. The original append is on the log,
    /// and the row is complete once the high watermark covers it — the same
    /// wait a fresh append of those records would have joined, so the caller
    /// hands it to the request's one overlapped gate rather than waiting here.
    AwaitDurability {
        /// The row so far, carrying the offset the original append was given.
        response: PartitionProduceResponse,
        /// The exclusive frontier the high watermark has to reach.
        target: Offset,
    },
}

/// The request fields the KIP-890 transaction check needs.
#[derive(Debug, Clone, Copy)]
pub(super) struct TransactionRequest<'a> {
    /// The `Produce` request's `transactional_id`.
    pub(super) transactional_id: Option<&'a str>,
    /// The negotiated `Produce` version.
    pub(super) version: i16,
    /// `producer.id.expiration.ms`, which also bounds unused verification
    /// state.
    pub(super) producer_id_expiration_ms: i64,
}

/// The first `Produce` version of transaction version 2. Kafka's
/// `AddPartitionsToTxnManager.produceRequestVersionToTransactionSupportedOperation`
/// gives `ADD_PARTITION` from here, so the leader adds the partition to the
/// transaction instead of only verifying it.
const FIRST_ADD_PARTITION_PRODUCE_VERSION: i16 = 12;

/// The last `Produce` version whose client does not know
/// `TRANSACTION_ABORTABLE` (Kafka's `DEFAULT_ERROR`).
const LAST_DEFAULT_ERROR_PRODUCE_VERSION: i16 = 10;

/// Kafka's `add.partitions.to.txn.retry.backoff.ms` default.
const CONCURRENT_TRANSACTIONS_BACKOFF: std::time::Duration = std::time::Duration::from_millis(20);

/// Kafka's `add.partitions.to.txn.retry.backoff.max.ms` default, which bounds
/// the retries of a transaction version 2 check that answers
/// `CONCURRENT_TRANSACTIONS`.
const CONCURRENT_TRANSACTIONS_RETRY: std::time::Duration = std::time::Duration::from_millis(100);

/// KIP-890 part 1: verify that a batch belongs to a transaction the
/// coordinator knows, and return the check the log runs before the append.
///
/// This is Kafka's `ReplicaManager.handleProduceAppend` up to the append:
///
/// - A transactional batch whose producer has no open transaction at the
///   batch epoch starts a verification on the log. The coordinator of the
///   request's `transactional_id` then verifies the partition (`Produce`
///   below v12) or adds it (v12 and later). A request without a
///   `transactional_id` skips the call, and the append then refuses the batch.
/// - A batch at an epoch below the producer's epoch answers
///   `INVALID_PRODUCER_EPOCH`.
/// - A coordinator error answers the code Kafka's
///   `postVerificationCallback` puts in the produce row.
///
/// Every batch with a producer id gets a check, so the append also refuses a
/// non-transactional batch from a producer with an open transaction.
///
/// # Errors
///
/// Returns the produce row's error code and error message when the batch may
/// not append.
pub(super) async fn verify_transactional_produce(
    batch: &PreparedBatch,
    partition: &crate::partition::Partition,
    coordinator: &std::sync::Arc<crate::txn::coordinator::TxnCoordinator>,
    (image, topic_name): (&krabka_metadata::MetadataImage, &str),
    request: TransactionRequest<'_>,
) -> Result<Option<crate::partition::ProducerAppendCheck>, (i16, Option<String>)> {
    let is_transactional = batch.attributes.is_transactional();
    if batch.producer_id < 0 {
        return if is_transactional {
            Err((codes::INVALID_PRODUCER_ID_MAPPING, None))
        } else {
            Ok(None)
        };
    }
    let transactional_batch = krabka_log::TransactionalBatch {
        producer_id: krabka_log::ProducerId(batch.producer_id),
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        is_transactional,
        is_control: batch.attributes.is_control_batch(),
    };
    let unverified = crate::partition::ProducerAppendCheck {
        batch: transactional_batch,
        guard: krabka_log::VerificationGuard::SENTINEL,
    };
    if !is_transactional {
        return Ok(Some(unverified));
    }
    let supports_epoch_bump = request.version >= FIRST_ADD_PARTITION_PRODUCE_VERSION;
    let guard = partition
        .start_transaction_verification(
            transactional_batch,
            supports_epoch_bump,
            (
                crate::time_util::now_ms(),
                request.producer_id_expiration_ms,
            ),
        )
        .await
        .map_err(|refusal| {
            (
                codes::from_broker_error(&crate::error::BrokerError::TransactionAppend(refusal)),
                None,
            )
        })?;
    if guard == krabka_log::VerificationGuard::SENTINEL {
        return Ok(Some(unverified));
    }
    let Some(transactional_id) = request.transactional_id else {
        // Kafka skips the coordinator call without a transactional id, and
        // the append then refuses the batch with INVALID_TXN_STATE.
        return Ok(Some(unverified));
    };
    let check = crate::txn::coordinator::produce_verification::PartitionCheck {
        transactional_id,
        producer_id: transactional_batch.producer_id,
        producer_epoch: batch.producer_epoch,
        partition: crate::txn::state::TopicPartition {
            topic: topic_name.to_string(),
            partition: partition.index,
        },
        verify_only: !supports_epoch_bump,
    };
    let txnv = crate::txn::version::resolve_txn_version(image);
    let retry_until = std::time::Instant::now() + CONCURRENT_TRANSACTIONS_RETRY;
    let code = loop {
        let code = coordinator
            .add_or_verify_partition(check.clone(), txnv)
            .await;
        if code == codes::CONCURRENT_TRANSACTIONS
            && supports_epoch_bump
            && std::time::Instant::now() < retry_until
        {
            tokio::time::sleep(CONCURRENT_TRANSACTIONS_BACKOFF).await;
            continue;
        }
        break code;
    };
    match produce_verification_code(code, request.version) {
        (codes::NONE, _) => Ok(Some(crate::partition::ProducerAppendCheck {
            batch: transactional_batch,
            guard,
        })),
        refused => Err(refused),
    }
}

/// The produce row's code for a coordinator answer to a transaction check.
///
/// `AddPartitionsToTxnManager` turns `PRODUCER_FENCED` into
/// `INVALID_PRODUCER_EPOCH`, a top-level `CLUSTER_AUTHORIZATION_FAILED` into
/// `INVALID_TXN_STATE`, and `TRANSACTION_ABORTABLE` into `INVALID_TXN_STATE`
/// for a client below `Produce` v11. `ReplicaManager.postVerificationCallback`
/// then turns a coordinator that cannot answer into `NOT_ENOUGH_REPLICAS`, and
/// so is `CONCURRENT_TRANSACTIONS` below v12. Those two translations carry
/// the custom error message that `postVerificationCallback` sets.
pub(super) fn produce_verification_code(code: i16, version: i16) -> (i16, Option<String>) {
    let code = match code {
        codes::PRODUCER_FENCED => codes::INVALID_PRODUCER_EPOCH,
        codes::CLUSTER_AUTHORIZATION_FAILED => codes::INVALID_TXN_STATE,
        codes::TRANSACTION_ABORTABLE if version <= LAST_DEFAULT_ERROR_PRODUCE_VERSION => {
            codes::INVALID_TXN_STATE
        }
        other => other,
    };
    let underlying = match code {
        codes::NETWORK_EXCEPTION => Some("NETWORK_EXCEPTION"),
        codes::COORDINATOR_LOAD_IN_PROGRESS => Some("COORDINATOR_LOAD_IN_PROGRESS"),
        codes::COORDINATOR_NOT_AVAILABLE => Some("COORDINATOR_NOT_AVAILABLE"),
        codes::NOT_COORDINATOR => Some("NOT_COORDINATOR"),
        codes::CONCURRENT_TRANSACTIONS if version < FIRST_ADD_PARTITION_PRODUCE_VERSION => {
            Some("CONCURRENT_TRANSACTIONS")
        }
        _ => None,
    };
    if let Some(underlying) = underlying {
        return (
            codes::NOT_ENOUGH_REPLICAS,
            Some(format!(
                "Unable to verify the partition has been added to the transaction. Underlying error: {underlying}"
            )),
        );
    }
    if code == codes::INVALID_TXN_STATE {
        return (
            code,
            Some("Partition was not added to the transaction".to_owned()),
        );
    }
    (code, None)
}

pub(super) async fn handle_duplicate(
    batch: &PreparedBatch,
    producer_state: &crate::producer_state::ProducerState,
    partition: &crate::partition::Partition,
    topic_name: &str,
    partition_index: i32,
    acks: i16,
) -> DedupOutcome {
    if batch.producer_id < 0 {
        return DedupOutcome::Append;
    }
    let crate::producer_state::Checked {
        decision,
        duplicate,
    } = producer_state
        .check_batch(
            topic_name,
            krabka_ids::PartitionIndex(partition_index),
            (batch.producer_id, batch.producer_epoch),
            (batch.base_sequence, batch.last_offset_delta),
        )
        .await;
    // Kafka's `UnifiedLog.append` answers a duplicate with the retained
    // batch's offsets and puts the batch's timestamp in `logAppendTime`.
    let duplicate_timestamp = duplicate.map_or(super::NO_LOG_APPEND_TIME, |batch| batch.timestamp);
    // A recognized retry is an accepted produce, so its row carries the
    // partition's real log start offset just like a fresh append's does. The
    // two refusals below happen before any append and keep the
    // `UNKNOWN_LOG_APPEND_INFO` sentinel. A raw `Produce v8` replayed against
    // `apache/kafka:4.3.1` on a partition whose low watermark `DeleteRecords`
    // had moved off 0 answered the duplicate with that same real value, not
    // with the sentinel.
    let (error_code, base_offset, log_append_time_ms, log_start_offset) = match decision {
        crate::producer_state::Decision::Duplicate { base_offset } => {
            let Some(target) = durability_frontier(base_offset, batch.last_offset_delta) else {
                return DedupOutcome::Answered(PartitionProduceResponse {
                    index: partition_index,
                    error_code: codes::INVALID_RECORD,
                    base_offset: -1,
                    ..Default::default()
                });
            };
            if acks == ACKS_ALL {
                return DedupOutcome::AwaitDurability {
                    response: PartitionProduceResponse {
                        index: partition_index,
                        base_offset,
                        log_append_time_ms: duplicate_timestamp,
                        log_start_offset: partition.log_start_offset().0,
                        ..Default::default()
                    },
                    target,
                };
            }
            (
                codes::NONE,
                base_offset,
                duplicate_timestamp,
                partition.log_start_offset().0,
            )
        }
        crate::producer_state::Decision::OutOfOrder => (
            codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
            INVALID_OFFSET,
            super::NO_LOG_APPEND_TIME,
            INVALID_OFFSET,
        ),
        crate::producer_state::Decision::Fenced => (
            codes::INVALID_PRODUCER_EPOCH,
            INVALID_OFFSET,
            super::NO_LOG_APPEND_TIME,
            INVALID_OFFSET,
        ),
        crate::producer_state::Decision::Append => return DedupOutcome::Append,
    };
    DedupOutcome::Answered(PartitionProduceResponse {
        index: partition_index,
        error_code,
        base_offset,
        log_append_time_ms,
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

    use super::{PreparedBatch, verify_transactional_produce};
    use crate::{
        codes,
        handlers::produce::{
            framing::{FramedPartition, PartitionPayload},
            leadership::BrokerProducePolicy,
            pipeline::{PartitionInput, PartitionServices, process_partition},
            test_support::{encode_batch, image_with_topic},
            topic_settings::TimestampPolicy,
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
            keyless_records: Vec::new(),
            source: crate::handlers::produce::prepare::PreparedSource::Owned(RecordBatch::default()),
        }
    }

    /// A transactional batch without a producer id is refused before any
    /// transaction check.
    #[tokio::test]
    async fn transactional_produce_rejects_malformed_producers() {
        let directory = tempfile::tempdir().expect("tempdir");
        let coordinator = Arc::new(crate::txn::coordinator::TxnCoordinator::new(
            krabka_audit::NodeId(1),
            Arc::new(crate::partition_registry::PartitionRegistry::new()),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            1,
            krabka_units::mebibytes(1),
        ));
        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        let partition = crate::broker::spawn_partition(
            "orders".to_string(),
            krabka_ids::PartitionIndex(0),
            directory.path().to_path_buf(),
            krabka_log::Log::open(directory.path(), krabka_log::LogConfig::default())
                .expect("open log"),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );

        for producer_id in [-1, i64::MIN] {
            let refused = verify_transactional_produce(
                &transactional_batch(producer_id, 0),
                &partition,
                &coordinator,
                (&image, "orders"),
                super::TransactionRequest {
                    transactional_id: Some("tid"),
                    version: 11,
                    producer_id_expiration_ms: 86_400_000,
                },
            )
            .await;
            check!(refused == Err((codes::INVALID_PRODUCER_ID_MAPPING, None)));
        }
    }

    /// The produce row that Kafka's `AddPartitionsToTxnManager` and
    /// `ReplicaManager.postVerificationCallback` build from a coordinator
    /// answer.
    #[test]
    fn a_coordinator_answer_maps_to_the_kafka_produce_row() {
        let not_verified = |underlying: &str| {
            Some(format!(
                "Unable to verify the partition has been added to the transaction. Underlying error: {underlying}"
            ))
        };
        let not_added = Some("Partition was not added to the transaction".to_owned());
        let cases = [
            (codes::NONE, 11, (codes::NONE, None)),
            (
                codes::PRODUCER_FENCED,
                11,
                (codes::INVALID_PRODUCER_EPOCH, None),
            ),
            (
                codes::INVALID_PRODUCER_EPOCH,
                12,
                (codes::INVALID_PRODUCER_EPOCH, None),
            ),
            (
                codes::TRANSACTION_ABORTABLE,
                10,
                (codes::INVALID_TXN_STATE, not_added.clone()),
            ),
            (
                codes::TRANSACTION_ABORTABLE,
                11,
                (codes::TRANSACTION_ABORTABLE, None),
            ),
            (
                codes::INVALID_TXN_STATE,
                12,
                (codes::INVALID_TXN_STATE, not_added.clone()),
            ),
            (
                codes::CLUSTER_AUTHORIZATION_FAILED,
                11,
                (codes::INVALID_TXN_STATE, not_added),
            ),
            (
                codes::NETWORK_EXCEPTION,
                12,
                (
                    codes::NOT_ENOUGH_REPLICAS,
                    not_verified("NETWORK_EXCEPTION"),
                ),
            ),
            (
                codes::NOT_COORDINATOR,
                11,
                (codes::NOT_ENOUGH_REPLICAS, not_verified("NOT_COORDINATOR")),
            ),
            (
                codes::COORDINATOR_NOT_AVAILABLE,
                11,
                (
                    codes::NOT_ENOUGH_REPLICAS,
                    not_verified("COORDINATOR_NOT_AVAILABLE"),
                ),
            ),
            (
                codes::COORDINATOR_LOAD_IN_PROGRESS,
                11,
                (
                    codes::NOT_ENOUGH_REPLICAS,
                    not_verified("COORDINATOR_LOAD_IN_PROGRESS"),
                ),
            ),
            (
                codes::CONCURRENT_TRANSACTIONS,
                11,
                (
                    codes::NOT_ENOUGH_REPLICAS,
                    not_verified("CONCURRENT_TRANSACTIONS"),
                ),
            ),
            (
                codes::CONCURRENT_TRANSACTIONS,
                12,
                (codes::CONCURRENT_TRANSACTIONS, None),
            ),
            (
                codes::INVALID_PRODUCER_ID_MAPPING,
                12,
                (codes::INVALID_PRODUCER_ID_MAPPING, None),
            ),
        ];
        for (answer, version, want) in cases {
            check!(
                super::produce_verification_code(answer, version) == want,
                "{answer} at v{version}"
            );
        }
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
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(0), part);

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

        let outcome = process_partition(
            PartitionInput {
                schema: None,
                part_data: FramedPartition {
                    index: 0,
                    payload: PartitionPayload::Slice(payload),
                },
                topic_compression: None,
                timestamps: TimestampPolicy::default(),
                compacted_topic: false,
                max_message_bytes: krabka_log::DEFAULT_MAX_MESSAGE_SIZE,
                delivery: None,
                topic_name: "orders".into(),
                freeze: crate::freeze::resolve::FreezeMutationResolution::Admit,
                internal_topic_denied: false,
                transaction: crate::handlers::produce::producer_checks::TransactionRequest {
                    transactional_id: None,
                    version: 9,
                    producer_id_expiration_ms: 86_400_000,
                },
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

        // The duplicate path joins the request's one overlapped `acks=-1`
        // wait rather than blocking inside `process_partition`, so the row is
        // decided here, the way `await_durability` decides it for a request.
        let resp: PartitionProduceResponse = match outcome {
            crate::handlers::produce::pipeline::PartitionOutcome::AwaitingHighWatermark(ack) => {
                ack.finish(std::time::Instant::now() + Duration::from_millis(50))
                    .await
            }
            crate::handlers::produce::pipeline::PartitionOutcome::Done(response) => {
                panic!("an acks=-1 duplicate must wait on the high watermark, got {response:?}")
            }
        };

        check!(resp.base_offset == 0);
        check!(
            resp.error_code == crate::codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            "HW 2 < target 3 must time out; a `-1` mutant would target offset 1 and return NONE"
        );
    }

    /// Kafka's `ProducerStateEntry` retains a producer's last five batches
    /// (`NUM_BATCHES_TO_RETAIN`), and `UnifiedLog.append` answers a retry of
    /// any of them as a duplicate: `NONE`, the original base offset, and the
    /// retained batch timestamp in `logAppendTime`. A retry of a batch that
    /// left the five is out of order.
    #[tokio::test]
    async fn a_retry_of_any_of_the_last_five_batches_is_a_duplicate() {
        use krabka_protocol::owned::produce_response::PartitionProduceResponse;

        const PRODUCER_ID: i64 = 4242;

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
        let part_dir = crate::log_dir::partition_dir(dir.path(), "orders", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let part = crate::broker::spawn_partition(
            "orders".to_string(),
            krabka_ids::PartitionIndex(0),
            dir.path().to_path_buf(),
            krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).unwrap(),
            log_dir_status.clone(),
            Arc::clone(&producer_state),
            false,
        );
        let record = image.partition("orders", 0).expect("partition");
        part.install_replication_target(Some(Uuid::nil()), record.leader.0, record.leader_epoch.0)
            .await;
        part.install_isr(&record.isr, &record.replicas, record.leader)
            .await;
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(0), part);

        // Batch `n` holds two records with sequences `2n` and `2n + 1`, and
        // the max timestamp `1000 + n`.
        let produce = |batch_index: i32| {
            let payload = encode_batch(&RecordBatch {
                producer_id: PRODUCER_ID,
                producer_epoch: 0,
                base_sequence: batch_index * 2,
                last_offset_delta: 1,
                max_timestamp: 1000 + i64::from(batch_index),
                records: (0..2)
                    .map(|offset_delta| Record {
                        offset_delta,
                        timestamp_delta: i64::from(batch_index),
                        value: Some(Bytes::from_static(b"v")),
                        ..Default::default()
                    })
                    .collect(),
                base_timestamp: 1000,
                ..Default::default()
            });
            let partitions = &partitions;
            let txn_coordinator = &txn_coordinator;
            let producer_state = &producer_state;
            let log_dir_status = &log_dir_status;
            let image = &image;
            let metrics = &metrics;
            async move {
                process_partition(
                    PartitionInput {
                        schema: None,
                        part_data: FramedPartition {
                            index: 0,
                            payload: PartitionPayload::Slice(payload),
                        },
                        topic_compression: None,
                        timestamps: TimestampPolicy::default(),
                        compacted_topic: false,
                        max_message_bytes: krabka_log::DEFAULT_MAX_MESSAGE_SIZE,
                        delivery: None,
                        topic_name: "orders".into(),
                        freeze: crate::freeze::resolve::FreezeMutationResolution::Admit,
                        internal_topic_denied: false,
                        transaction: super::TransactionRequest {
                            transactional_id: None,
                            version: 9,
                            producer_id_expiration_ms: 86_400_000,
                        },
                        acks: 1,
                        timeout: Duration::from_secs(5),
                    },
                    PartitionServices {
                        schema_validator: None,
                        partitions,
                        txn_coordinator,
                        producer_state,
                        log_dir_status,
                        image,
                        broker_policy: BrokerProducePolicy {
                            node_id: krabka_audit::NodeId(1),
                            default_min_insync_replicas: 1,
                            is_witness: false,
                        },
                        record_decompression_policy: RecordDecompressionPolicy::default(),
                        metrics,
                        phases: &crate::metrics::RequestPhases::default(),
                    },
                )
                .await
                .expect("process partition")
                .expect_done()
            }
        };

        for batch_index in 0..5 {
            let appended = produce(batch_index).await;
            assert!(
                appended
                    == PartitionProduceResponse {
                        index: 0,
                        base_offset: i64::from(batch_index) * 2,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                        ..Default::default()
                    }
            );
        }

        let duplicate = |batch_index: i32| PartitionProduceResponse {
            index: 0,
            base_offset: i64::from(batch_index) * 2,
            log_append_time_ms: 1000 + i64::from(batch_index),
            log_start_offset: 0,
            ..Default::default()
        };
        for batch_index in 0..5 {
            let replayed = produce(batch_index).await;
            assert!(replayed == duplicate(batch_index), "batch {batch_index}");
        }

        // A sixth batch pushes batch 0 out of the five.
        produce(5).await;
        let out_of_order = PartitionProduceResponse {
            index: 0,
            error_code: crate::codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
            base_offset: -1,
            log_start_offset: -1,
            ..Default::default()
        };
        let replays = [produce(0).await, produce(1).await, produce(5).await];
        assert!(replays == [out_of_order, duplicate(1), duplicate(5)]);
    }
}
