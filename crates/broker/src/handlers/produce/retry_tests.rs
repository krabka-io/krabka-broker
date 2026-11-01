//! A producer's retry of a batch whose first copy is already on the log, but
//! not yet acknowledged, answers with the first copy's offset and appends
//! nothing.
//!
//! Kafka's `UnifiedLog.append` updates the producer state with the append,
//! and `analyzeAndValidateProducerState` finds a retry among the producer's
//! retained batches under the same lock, whether or not the high watermark
//! has reached the first copy. Jepsen's Kafka workload found the gap this
//! closes: with `acks=all`, a follower paused, and the first copy's wait
//! timed out, the client's retry of a transaction's first batch was appended a
//! second and a third time under one leader epoch.

use {
    super::{
        append::PendingAck,
        framing::PartitionPayload,
        pipeline::{PartitionOutcome, PartitionServices, process_partition},
        producer_checks::TransactionRequest,
        test_support::{PipelineFixture, encode_batch, image_with_topic, pipeline_input},
    },
    crate::codes,
    assert2::check,
    bytes::Bytes,
    krabka_ids::{NodeId, PartitionIndex},
    krabka_log::Offset,
    krabka_protocol::{
        owned::produce_response::PartitionProduceResponse,
        records::{Attributes, Record, RecordBatch},
    },
    std::{
        future,
        sync::Arc,
        time::{Duration, Instant},
    },
};

const TOPIC: &str = "t2";
const PRODUCER_ID: i64 = 3027;
const PRODUCER_EPOCH: i16 = 38;
const TIMESTAMP: i64 = 1_000;

/// What happened to the first copy's `acks=all` wait before the retry
/// arrived.
#[derive(Debug, Clone, Copy)]
enum FirstWait {
    /// It is still waiting for the high watermark.
    Held,
    /// Its request ended without an answer, as a closed connection ends it.
    Dropped,
    /// It is still waiting, and the produce path's tracker has lost the
    /// batch, so only the check under the log's lock can find the retry.
    TrackerForgot,
}

/// The batch a case sends and retries.
#[derive(Debug, Clone, Copy)]
enum Retried {
    /// The producer's first batch of a new transaction: sequence 0, which
    /// starts a KIP-890 verification on the partition.
    TransactionStart,
    /// The second batch of that transaction, at sequence 1.
    TransactionContinuation,
    /// An idempotent producer's second batch, at sequence 1.
    LaterSequence,
}

impl Retried {
    fn transactional(self) -> bool { !matches!(self, Self::LaterSequence) }

    /// The sequence of the retried batch. Every sequence below it was
    /// appended and acknowledged first.
    fn sequence(self) -> i32 {
        match self {
            Self::TransactionStart => 0,
            Self::TransactionContinuation | Self::LaterSequence => 1,
        }
    }
}

/// A single-record batch from the producer at `base_sequence`.
fn batch(retried: Retried, base_sequence: i32) -> Bytes {
    encode_batch(&RecordBatch {
        attributes: Attributes::default().with_transactional(retried.transactional()),
        producer_id: PRODUCER_ID,
        producer_epoch: PRODUCER_EPOCH,
        base_sequence,
        base_timestamp: TIMESTAMP,
        max_timestamp: TIMESTAMP,
        records: vec![Record {
            value: Some(Bytes::from_static(b"30")),
            ..Default::default()
        }],
        ..Default::default()
    })
}

/// A transaction version 1 `Produce` on a broker that verifies nothing with
/// the coordinator, so the log admits the transaction's first batch on the
/// guard it starts for it. The verification and its guard are the same as
/// with a coordinator.
fn transaction() -> TransactionRequest<'static> {
    TransactionRequest {
        transactional_id: Some("txn"),
        version: 11,
        producer_id_expiration_ms: 86_400_000,
        verification_enabled: false,
    }
}

async fn produce(
    retried: Retried,
    base_sequence: i32,
    acks: i16,
    services: PartitionServices<'_>,
) -> PartitionOutcome {
    process_partition(
        pipeline_input()
            .topic(TOPIC)
            .payload(PartitionPayload::Slice(batch(retried, base_sequence)))
            .transaction(transaction())
            .acks(acks)
            .call(),
        services,
    )
    .await
    .expect("process partition")
}

/// The high-watermark wait of an `acks=all` produce that appended.
fn pending(outcome: PartitionOutcome) -> PendingAck {
    match outcome {
        PartitionOutcome::AwaitingHighWatermark(ack) => ack,
        PartitionOutcome::Done(row) => panic!("an acks=all append must wait, got {row:?}"),
    }
}

/// The row of an `acks=all` produce, once a short wait for a high watermark
/// that never moves has ended.
async fn timed_out_row(ack: PendingAck) -> PartitionProduceResponse {
    ack.finish(
        Instant::now() + Duration::from_millis(50),
        |_, _| Ok(1),
        future::pending(),
    )
    .await
}

/// A retry that arrives while the first copy waits for `acks=all`, or after
/// that wait was dropped, answers with the first copy's offset, and the
/// partition holds one copy.
///
/// The partition's ISR is this broker and a follower that never fetches, so
/// the high watermark stays at 0 and the first copy's wait cannot end. Both
/// rows time out with the first copy's offset: a duplicate's row carries the
/// retained batch's timestamp in `logAppendTime`, as Kafka's does.
#[tokio::test]
async fn a_retry_during_the_acks_all_wait_answers_the_first_offset() {
    let retried_batches = [
        Retried::TransactionStart,
        Retried::TransactionContinuation,
        Retried::LaterSequence,
    ];
    let first_waits = [
        FirstWait::Held,
        FirstWait::Dropped,
        FirstWait::TrackerForgot,
    ];
    for retried in retried_batches {
        for first_wait in first_waits {
            let label = format!("{retried:?}, first wait {first_wait:?}");
            let dir = tempfile::tempdir().expect("tempdir");
            let image = Arc::new(image_with_topic(TOPIC, &[1, 2]));
            let fixture = PipelineFixture::new(NodeId(1));
            let partition = fixture.register_partition(dir.path(), TOPIC, &image).await;
            let services = fixture.services(&image);

            for sequence in 0..retried.sequence() {
                let row = produce(retried, sequence, 1, services).await.expect_done();
                check!(row.error_code == codes::NONE, "{label}: {row:?}");
            }
            let original = i64::from(retried.sequence());

            let (first_row, retry_row) = {
                let first = pending(produce(retried, retried.sequence(), -1, services).await);
                let first = match first_wait {
                    FirstWait::Held => Some(first),
                    FirstWait::Dropped => {
                        drop(first);
                        None
                    }
                    FirstWait::TrackerForgot => {
                        fixture
                            .producer_state
                            .truncate(TOPIC, PartitionIndex(0), 0)
                            .await;
                        Some(first)
                    }
                };
                let retry_row = timed_out_row(pending(
                    produce(retried, retried.sequence(), -1, services).await,
                ))
                .await;
                check!(
                    partition.log_end_offset() == Offset(original + 1),
                    "{label}: the retry must not append a second copy"
                );
                (
                    match first {
                        Some(first) => Some(timed_out_row(first).await),
                        None => None,
                    },
                    retry_row,
                )
            };

            let duplicate_row = PartitionProduceResponse {
                index: 0,
                error_code: codes::REQUEST_TIMED_OUT,
                base_offset: original,
                log_append_time_ms: TIMESTAMP,
                log_start_offset: 0,
                ..Default::default()
            };
            check!(retry_row == duplicate_row, "{label}");
            let first_copy_row = PartitionProduceResponse {
                log_append_time_ms: -1,
                ..duplicate_row
            };
            check!(first_row.is_none_or(|row| row == first_copy_row), "{label}");
            check!(
                partition.log_end_offset() == Offset(original + 1),
                "{label}"
            );
        }
    }
}
