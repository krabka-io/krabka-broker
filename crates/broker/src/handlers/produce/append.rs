//! The append itself: the hand-off to the partition's writer actor, the
//! `acks=-1` high-watermark gate, and the idempotent-producer commit that
//! follows a successful append.
//!
//! The two halves are separate on purpose. Kafka's `ReplicaManager
//! .appendRecords` appends every partition of a request to the local log and
//! only then registers ONE `DelayedProduce` covering all of them, so the
//! replication waits overlap and the request's `timeout.ms` bounds the whole
//! request rather than each partition in turn. [`dispatch_prepared`] is the
//! first half, and it stops at the writer's answer; [`PendingAck`] is what an
//! `acks=all` partition hands back so the handler can drive every partition's
//! wait together.

use std::{sync::Arc, time::Duration};

use krabka_log::{Offset, VerbatimBatch};
use krabka_protocol::owned::produce_response::PartitionProduceResponse;
use tokio::sync::oneshot;

use super::{
    ACKS_ALL, INVALID_OFFSET, NO_LOG_APPEND_TIME, durability_frontier,
    prepare::{PreparedBatch, PreparedSource},
};
use crate::{
    codes,
    error::BrokerError,
    partition::{
        AppendedBatch, Partition, ProduceData, ProduceJob, ReplicationTarget, WriterMessage,
    },
};

/// The partition metadata-transition barrier a produce holds from the dedup
/// gate through to its ack. It is an owned guard, so it travels into a
/// [`PendingAck`] and keeps that promise across the overlapped wait.
pub(super) type ProduceTransition = tokio::sync::OwnedRwLockReadGuard<ReplicationTarget>;

#[derive(Clone, Copy)]
pub(super) struct AppendContext<'a> {
    pub(super) partition: &'a Arc<crate::partition::Partition>,
    pub(super) producer_state: &'a Arc<crate::producer_state::ProducerState>,
    pub(super) partition_index: i32,
    pub(super) acks: i16,
    pub(super) timeout: Duration,
    pub(super) leader_epoch: i32,
    /// The request's phase accumulator. This partition's writer round-trip is
    /// charged to the local phase here; its `acks=-1` high-watermark wait is
    /// charged to the remote phase once, by the handler, around the one wait
    /// that covers every partition of the request.
    pub(super) phases: &'a crate::metrics::RequestPhases,
}

/// What the append half of one partition produced.
pub(super) enum AppendOutcome {
    /// The row is complete. Every pre-append refusal, every `acks != -1`
    /// append, and every writer failure lands here.
    Answered(PartitionProduceResponse),
    /// The batch is on this broker's log and the request asked for `acks=-1`,
    /// so the row is complete only once the high watermark covers it.
    AwaitDurability {
        /// The row so far: the assigned offset and the log-append stamp. The
        /// gate decides its `error_code`, and [`PendingAck::finish`] fills in
        /// the log start offset it reads after the gate.
        response: PartitionProduceResponse,
        /// The exclusive frontier the high watermark has to reach.
        target: Offset,
        /// The idempotent-producer commit this append owes once the gate
        /// resolves, or `None` for a non-idempotent producer.
        commit: Option<AppendCommit>,
    },
}

/// One partition's outstanding `acks=-1` high-watermark wait.
///
/// The handler collects one of these per appended partition and drives them
/// all against a single deadline, which is what Kafka's one `DelayedProduce`
/// per request does. Everything the finish needs is owned, because the wait
/// outlives the per-partition pipeline that created it: the partition handle
/// is an `Arc`, the commit carries the topic name as the `Arc<str>` the metric
/// labels already share, and the metadata-transition barrier is an owned
/// guard.
pub(super) struct PendingAck {
    response: PartitionProduceResponse,
    partition: Arc<Partition>,
    target: Offset,
    commit: Option<AppendCommit>,
    /// The transition barrier this produce took at the leadership gate. A
    /// diskless promotion takes the write side while it hydrates the canonical
    /// log and rebuilds producer state, so holding the read side here is what
    /// keeps that publication boundary from landing between an append and its
    /// ack. It is only ever dropped, never read.
    _transition: ProduceTransition,
}

impl PendingAck {
    /// Build the wait for a partition that appended, or for a recognized
    /// idempotent retry whose original append is not covered by the high
    /// watermark yet.
    pub(super) fn new(
        response: PartitionProduceResponse,
        partition: Arc<Partition>,
        target: Offset,
        commit: Option<AppendCommit>,
        transition: ProduceTransition,
    ) -> Self {
        Self {
            response,
            partition,
            target,
            commit,
            _transition: transition,
        }
    }

    /// Wait for the high watermark to cover this partition's append, then
    /// finish its row.
    ///
    /// `deadline` is the request's, computed once for every partition of it,
    /// so N partitions of one `acks=-1` request wait out one `timeout.ms`
    /// between them rather than N of them in turn.
    ///
    /// The behavior per path, unchanged from when the wait ran inline:
    ///   * HW reaches the target: NONE, then commit;
    ///   * the gate times out: `NOT_ENOUGH_REPLICAS_AFTER_APPEND`, then
    ///     commit. The append is durable on the leader, so the idempotent
    ///     tracker must advance either way. A retry is then recognized as a
    ///     duplicate rather than as out-of-order.
    pub(super) async fn finish(mut self, deadline: std::time::Instant) -> PartitionProduceResponse {
        let gate = self
            .partition
            .await_hw_at_least(self.target, deadline)
            .await;
        self.response.error_code = match gate {
            Ok(()) => codes::NONE,
            Err(_timeout) => codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        };
        // The row's log start offset is read after the gate on both paths, the
        // way it was when the gate ran inline.
        self.response.log_start_offset = stamp_log_start(&self.partition);
        if let Some(commit) = &self.commit {
            commit.record(&self.partition, self.target).await;
        }
        self.response
    }
}

pub(super) async fn dispatch_prepared(
    prepared: PreparedBatch,
    context: AppendContext<'_>,
    shared_topic: &Arc<str>,
) -> Result<AppendOutcome, BrokerError> {
    // No offset is assigned until the writer answers with one. Every failure
    // below — the writer channel gone, the append itself erroring, the ack
    // timing out — leaves the row without an append, which Kafka answers with
    // `UNKNOWN_LOG_APPEND_INFO`, whose `firstOffset` and `logStartOffset` are
    // both -1. `finalize_ack` overwrites both on the one path that appends.
    let mut response = PartitionProduceResponse {
        index: context.partition_index,
        base_offset: INVALID_OFFSET,
        ..Default::default()
    };
    let commit = AppendCommit {
        producer_state: Arc::clone(context.producer_state),
        topic: Arc::clone(shared_topic),
        partition: context.partition_index,
        pid: prepared.producer_id,
        epoch: prepared.producer_epoch,
        base_seq: prepared.base_sequence,
        last_offset_delta: prepared.last_offset_delta,
        max_timestamp: prepared.max_timestamp,
        base_offset: Offset(INVALID_OFFSET),
    };
    let data = build_produce_data(prepared, context.leader_epoch);
    let (ack_tx, ack_rx) = oneshot::channel();
    let job = WriterMessage::Produce(ProduceJob { data, ack: ack_tx });
    // The local phase opens here and closes when the writer answers: the
    // enqueue plus the append is the work this broker's own log does for this
    // partition. A send failure is charged too, so the phase covers every exit.
    let local_started = std::time::Instant::now();
    if context.partition.writer_tx.send(job).await.is_err() {
        context.phases.add_local(local_started.elapsed());
        response.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        return Ok(AppendOutcome::Answered(response));
    }
    let acked = tokio::time::timeout(context.timeout, ack_rx).await;
    context.phases.add_local(local_started.elapsed());
    match acked {
        Ok(Ok(Ok(appended))) => return Ok(finalize_ack(response, context, appended, commit).await),
        Ok(Ok(Err(error))) => response.error_code = codes::from_broker_error(&error),
        Ok(Err(_)) => response.error_code = codes::NOT_LEADER_OR_FOLLOWER,
        Err(_) => response.error_code = codes::REQUEST_TIMED_OUT,
    }
    Ok(AppendOutcome::Answered(response))
}

/// The idempotent-producer dedup identity and the fields a
/// `producer_state.commit` record needs, owned so it can outlive the pipeline
/// that built it and travel into a [`PendingAck`].
///
/// The struct groups the eight positional `commit` arguments into one value,
/// so the commit call exists in exactly one place, [`AppendCommit::record`].
pub(super) struct AppendCommit {
    producer_state: Arc<crate::producer_state::ProducerState>,
    topic: Arc<str>,
    partition: i32,
    pid: i64,
    epoch: i16,
    base_seq: i32,
    last_offset_delta: i32,
    max_timestamp: i64,
    base_offset: Offset,
}

impl AppendCommit {
    /// Record the idempotent-producer commit for an append the log still
    /// holds.
    ///
    /// Only record it if the appended batch is still on the leader's log. A
    /// failover-rejoin divergence truncation can remove the batch while the
    /// acks=all HW gate is waiting (the gate then times out); recording the
    /// truncated batch would make a retry dedup against an offset the log no
    /// longer holds, and the retry's HW gate would wait forever for a high
    /// watermark that can never reach the vanished offset. Skipping the commit
    /// lets the retry re-append fresh instead.
    ///
    /// This is a best-effort check: a truncation racing in between this read
    /// and the commit below could still record a stale entry. That is
    /// tolerated because the replicator calls `ProducerState::truncate` after
    /// *every* log truncation, so any entry stranded by such a race is dropped
    /// by the next truncation/failover. Do not "harden" this by removing the
    /// check — the check is what avoids recording the common
    /// (already-truncated) case.
    async fn record(&self, part: &Partition, target: Offset) {
        if self.pid < 0 {
            return;
        }
        if part.log_end_offset() < target {
            // Evidence for the on-cluster failover verification: the appended
            // batch was truncated before its dedup commit, so we skip the
            // commit (the retry re-appends). Seeing this fire confirms the
            // Bug-D path executed.
            tracing::warn!(
                topic = &*self.topic,
                partition = self.partition,
                base_offset = self.base_offset.0,
                target = target.0,
                leo = part.log_end_offset().0,
                "produce: appended batch truncated before dedup commit; skipping commit so retry re-appends"
            );
            return;
        }
        self.producer_state
            .commit(
                &self.topic,
                krabka_ids::PartitionIndex(self.partition),
                (self.pid, self.epoch),
                (self.base_seq, self.last_offset_delta),
                // Unwrap the assigned `Offset` into the dedup tracker's `i64`.
                (self.base_offset.0, self.max_timestamp),
            )
            .await;
    }
}

/// An appended row carries the partition's real log start offset, not the -1
/// that `UNKNOWN_LOG_APPEND_INFO` supplies to the pre-append refusals.
///
/// Kafka fills `LogAppendInfo.logStartOffset` from `UnifiedLog`'s own pointer
/// at append time and `ReplicaManager` copies it straight into the partition
/// row, so the value moves whenever retention, a `DeleteRecords` or a tiering
/// upload advances the log start. Four single-record batches into
/// `apache/kafka:4.3.1`, then a `kafka-delete-records` to offset 3, then one
/// more raw `Produce v8`, answers `error_code=0 base_offset=4
/// log_append_time_ms=-1 log_start_offset=3`.
///
/// The read is one brief lock on the partition's log, the same one
/// `log_end_offset()` takes.
fn stamp_log_start(part: &Partition) -> i64 {
    part.log_start_offset().0
}

/// Finalize a successful writer append.
///
/// The function sets the response `base_offset` and `log_append_time_ms`, and
/// then either finishes the row outright or hands the caller the
/// high-watermark wait that still stands between the append and its ack.
///
/// The behavior per path:
///   * `acks != -1`: NONE, then commit, all here;
///   * `acks == -1`: [`AppendOutcome::AwaitDurability`], and
///     [`PendingAck::finish`] decides the code and commits after the gate.
///
/// Note that the commit happens on *both* the success and the timeout
/// `acks=-1` sub-paths. Whichever half runs it, it runs once the `error_code`
/// and the `base_offset` are decided.
///
/// The high-watermark gate is the remote phase of the request: the append is
/// already durable on this broker, and everything the gate waits for is a
/// follower taking it. The handler charges that phase once, around the one
/// wait that covers every partition it appended to.
///
/// The function takes the whole [`AppendContext`] rather than the fields it
/// reads out of it, because the caller has one and the fields travel together
/// everywhere on this path.
async fn finalize_ack(
    mut out: PartitionProduceResponse,
    context: AppendContext<'_>,
    appended: AppendedBatch,
    mut commit: AppendCommit,
) -> AppendOutcome {
    let AppendedBatch {
        base_offset,
        log_append_time_ms,
    } = appended;
    let part = context.partition;
    let Some(target) = durability_frontier(base_offset.0, commit.last_offset_delta) else {
        out.error_code = codes::INVALID_RECORD;
        out.base_offset = -1;
        return AppendOutcome::Answered(out);
    };
    commit.base_offset = base_offset;
    // Unwrap the assigned `Offset` into the wire `base_offset` response field.
    out.base_offset = base_offset.0;
    // KIP-32's `logAppendTimeMs`. Kafka fills it from
    // `LogAppendInfo.logAppendTime`, which `UnifiedLog.append` sets only when
    // the topic's `message.timestamp.type` is `LogAppendTime`; every other
    // topic answers the `-1` that `LogAppendInfo` starts at, and that is the
    // `None` arm here. The value is the clock reading the log stamped into the
    // batch, so what the response reports and what the records carry are one
    // number read once.
    out.log_append_time_ms = log_append_time_ms.unwrap_or(NO_LOG_APPEND_TIME);
    if context.acks == ACKS_ALL {
        return AppendOutcome::AwaitDurability {
            response: out,
            target,
            commit: Some(commit),
        };
    }
    out.error_code = codes::NONE;
    out.log_start_offset = stamp_log_start(part);
    commit.record(part, target).await;
    AppendOutcome::Answered(out)
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
