//! The append itself: the hand-off to the partition's writer actor, the
//! `acks=-1` high-watermark gate, and the idempotent-producer commit that
//! follows a successful append.

use std::{sync::Arc, time::Duration};

use krabka_log::{Offset, VerbatimBatch};
use krabka_protocol::owned::produce_response::PartitionProduceResponse;
use tokio::sync::oneshot;

use super::{
    ACKS_ALL,
    prepare::{PreparedBatch, PreparedSource},
};
use crate::{
    codes,
    error::BrokerError,
    partition::{Partition, ProduceData, ProduceJob, WriterMessage},
};

#[derive(Clone, Copy)]
pub(super) struct AppendContext<'a> {
    pub(super) partition: &'a Arc<crate::partition::Partition>,
    pub(super) producer_state: &'a Arc<crate::producer_state::ProducerState>,
    pub(super) topic_name: &'a str,
    pub(super) partition_index: i32,
    pub(super) acks: i16,
    pub(super) timeout: Duration,
    pub(super) leader_epoch: i32,
}

pub(super) async fn dispatch_prepared(
    prepared: PreparedBatch,
    context: AppendContext<'_>,
) -> Result<PartitionProduceResponse, BrokerError> {
    let mut response = PartitionProduceResponse {
        index: context.partition_index,
        ..Default::default()
    };
    let commit = CommitKey {
        topic: context.topic_name,
        partition: context.partition_index,
        pid: prepared.producer_id,
        epoch: prepared.producer_epoch,
        base_seq: prepared.base_sequence,
        last_offset_delta: prepared.last_offset_delta,
        max_timestamp: prepared.max_timestamp,
    };
    let data = build_produce_data(prepared, context.leader_epoch);
    let (ack_tx, ack_rx) = oneshot::channel();
    let job = WriterMessage::Produce(ProduceJob { data, ack: ack_tx });
    if context.partition.writer_tx.send(job).await.is_err() {
        response.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        return Ok(response);
    }
    match tokio::time::timeout(context.timeout, ack_rx).await {
        Ok(Ok(Ok(base_offset))) => {
            finalize_ack(
                &mut response,
                context.partition,
                context.acks,
                context.timeout,
                base_offset,
                context.producer_state,
                &commit,
            )
            .await;
        }
        Ok(Ok(Err(error))) => response.error_code = codes::from_broker_error(&error),
        Ok(Err(_)) => response.error_code = codes::NOT_LEADER_OR_FOLLOWER,
        Err(_) => response.error_code = codes::REQUEST_TIMED_OUT,
    }
    Ok(response)
}

/// Borrowed bundle of the idempotent-producer dedup identity and the fields
/// that a `producer_state.commit` record needs.
///
/// The struct groups the eight positional `commit` arguments into one value,
/// so the commit call exists in exactly one place, [`finalize_ack`].
struct CommitKey<'a> {
    topic: &'a str,
    partition: i32,
    pid: i64,
    epoch: i16,
    base_seq: i32,
    last_offset_delta: i32,
    max_timestamp: i64,
}

/// Finalize a successful writer append.
///
/// The function applies the `acks=-1` high-watermark durability gate, sets the
/// response `error_code` and `base_offset`, and records the
/// idempotent-producer commit exactly once when `pid >= 0`.
///
/// The behavior per path:
///   * `acks != -1`: NONE, then commit.
///   * `acks == -1`, HW reaches target: NONE, then commit.
///   * `acks == -1`, HW gate times out: `NOT_ENOUGH_REPLICAS_AFTER_APPEND`,
///     then commit. The append is durable on the leader, so the idempotent
///     tracker must advance. A retry is then recognized as a duplicate and
///     not as out-of-order.
///
/// Note that the commit happens on *both* the success and the timeout
/// `acks=-1` sub-paths. The function therefore always commits once it has
/// decided the `error_code` and the `base_offset`.
async fn finalize_ack(
    out: &mut PartitionProduceResponse,
    part: &Arc<Partition>,
    acks: i16,
    timeout: Duration,
    base_offset: Offset,
    producer_state: &Arc<crate::producer_state::ProducerState>,
    key: &CommitKey<'_>,
) {
    let target = base_offset + i64::from(key.last_offset_delta) + 1;
    if acks == ACKS_ALL {
        let deadline = std::time::Instant::now() + timeout;
        out.error_code = match part.await_hw_at_least(target, deadline).await {
            Ok(()) => codes::NONE,
            Err(_timeout) => codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        };
    } else {
        out.error_code = codes::NONE;
    }
    // Unwrap the assigned `Offset` into the wire `base_offset` response field.
    out.base_offset = base_offset.0;
    // Only record the idempotent-producer commit if the appended batch is still
    // on the leader's log. A failover-rejoin divergence truncation can remove
    // the batch while the acks=all HW gate above is waiting (the gate then times
    // out); recording the truncated batch would make a retry dedup against an
    // offset the log no longer holds, and the retry's HW gate would wait forever
    // for a high watermark that can never reach the vanished offset. Skipping
    // the commit lets the retry re-append fresh instead.
    //
    // This is a best-effort check: a truncation racing in between this read and
    // the commit below could still record a stale entry. That is tolerated
    // because the replicator calls `ProducerState::truncate` after *every* log
    // truncation, so any entry stranded by such a race is dropped by the next
    // truncation/failover. Do not "harden" this by removing the check — the
    // check is what avoids recording the common (already-truncated) case.
    if key.pid >= 0 && part.log_end_offset() < target {
        // Evidence for the on-cluster failover verification: the appended batch
        // was truncated before its dedup commit, so we skip the commit (the
        // retry re-appends). Seeing this fire confirms the Bug-D path executed.
        tracing::warn!(
            topic = key.topic,
            partition = key.partition,
            base_offset = base_offset.0,
            target = target.0,
            leo = part.log_end_offset().0,
            "produce: appended batch truncated before dedup commit; skipping commit so retry re-appends"
        );
    }
    if key.pid >= 0 && part.log_end_offset() >= target {
        producer_state
            .commit(
                key.topic,
                krabka_ids::PartitionIndex(key.partition),
                (key.pid, key.epoch),
                (key.base_seq, key.last_offset_delta),
                // Unwrap the assigned `Offset` into the dedup tracker's `i64`.
                (base_offset.0, key.max_timestamp),
            )
            .await;
    }
}

/// Build the writer's [`ProduceData`] from a prepared batch and stamp the
/// leader epoch.
///
/// Verbatim batches carry the producer's exact bytes. Owned batches carry the
/// decoded `RecordBatch`, whose `partition_leader_epoch` the caller has
/// already stamped.
pub(super) fn build_produce_data(prepared: PreparedBatch, leader_epoch: i32) -> ProduceData {
    let is_transactional = prepared.attributes.is_transactional();
    match prepared.source {
        PreparedSource::Verbatim(bytes) => ProduceData::Verbatim(VerbatimBatch {
            last_offset_delta: prepared.last_offset_delta,
            max_timestamp: prepared.max_timestamp,
            // Wrap the atomic-loaded raw epoch into the log seam's `LeaderEpoch`.
            leader_epoch: krabka_log::LeaderEpoch(leader_epoch),
            // Wrap the produce path's decode-side `i64` into the log seam's `ProducerId`.
            producer_id: krabka_log::ProducerId(prepared.producer_id),
            producer_epoch: prepared.producer_epoch,
            base_sequence: prepared.base_sequence,
            is_transactional,
            bytes,
        }),
        PreparedSource::Owned(mut batch) => {
            // The writer stamps the verbatim path's epoch in-place at append;
            // the owned batch carries it as a struct field instead.
            batch.partition_leader_epoch = leader_epoch;
            ProduceData::Owned(batch)
        }
    }
}
