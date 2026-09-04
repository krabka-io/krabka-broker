//! KIP-405 hand-off to the remote tier for an offset the local log no
//! longer holds, and the KFC-1 activation check that keeps a batch the
//! tier returns from going out before it is due.

use krabka_log::{DeliveryPolicy, LeaderEpoch, Offset};
use krabka_protocol::{
    Encode, owned::fetch_response::AbortedTransaction, primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use krabka_units::convert::TimeExt as _;

use super::plan::PendingRead;
use crate::{broker::Broker, codes, partition::Partition};

/// Whether a batch the remote tier returned may go out to a consumer now.
///
/// The local log no longer holds this batch, so the partition's delivery
/// watermark says nothing about it: that watermark is derived from the records
/// the log still has, and it is clamped to at or above the log start. The
/// evidence that survived the copy is the batch's own `max_timestamp`, which is
/// the activation time KFC-1 defines, and this applies the log's own rule to
/// it. A batch is active once its activation time plus the declared clock bound
/// is at or before the broker's clock reading, so delivery is never early.
///
/// A topic that delivers immediately answers `true` without reading the
/// timestamp.
fn remote_batch_is_deliverable(
    policy: DeliveryPolicy,
    uncertainty_ms: i64,
    max_timestamp: i64,
    now_ms: i64,
) -> bool {
    krabka_log::batch_is_deliverable(policy, uncertainty_ms, max_timestamp, now_ms)
}

/// Records how long a cold-tier read held its reader slot, on whichever of
/// `try_remote_read`'s many exits the read leaves by.
///
/// A guard rather than a call at the end because the read returns early on a
/// held-back batch, on an aborted-transaction failure and on every remote
/// error, and a slot the reader held is a slot the pool could not hand out
/// however the read ended.
struct ReadTimer<'metrics> {
    metrics: &'metrics crate::metrics::BrokerMetrics,
    started: std::time::Instant,
}

impl<'metrics> ReadTimer<'metrics> {
    fn started(metrics: &'metrics crate::metrics::BrokerMetrics) -> Self {
        Self {
            metrics,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for ReadTimer<'_> {
    fn drop(&mut self) {
        self.metrics
            .observe_remote_reader_fetch(self.started.elapsed());
    }
}

/// Answers one partition of a `Fetch` that the bounded reader pool refused.
///
/// Kafka's `ReplicaManager.processRemoteFetches` catches the executor's
/// `RejectedExecutionException` and turns it into a `LogReadResult` carrying
/// that exception, which `Errors.forException` has no mapping for, so the
/// partition goes out as `UNKNOWN_SERVER_ERROR`. A client sees a retryable
/// server-side failure on that partition and the rest of the request is
/// unaffected -- which is the whole point of the cap: the local paths keep
/// their resources instead of queueing behind the cold tier.
///
/// Returns zero served bytes, because the partition carries an error and no
/// records.
fn reject_saturated(p: &mut PendingRead) -> usize {
    tracing::warn!(
        topic = %p.topic_name,
        partition = p.partition_index,
        offset = p.fetch_offset,
        "remote-reader: reader pool saturated; refusing the cold-tier read"
    );
    p.out.error_code = codes::UNKNOWN_SERVER_ERROR;
    p.out.records = None;
    if p.read_committed {
        p.out.aborted_transactions = Some(Vec::new());
    }
    0
}

/// KIP-405: try to serve `p`'s requested offset from the remote tier when the
/// local log returned `OFFSET_OUT_OF_RANGE` and the topic has
/// `remote.storage.enable=true`.
///
/// On success the function replaces the partition's error and records, and
/// returns the encoded batch size. On a miss, on an error, or for a
/// non-tiered topic, it leaves `p.out` untouched and returns `None`.
///
/// A consumer read of a scheduled topic is capped here as well. The remote path
/// serves whole batches with no offset limit, and it is the one read path the
/// local delivery watermark cannot bound, so it checks the batch's own
/// activation time instead. See [`remote_batch_is_deliverable`].
///
/// The whole cold read -- the batch and, for a read-committed fetch, the
/// segment's aborted-transaction list -- runs under one permit from the
/// reader's bounded pool, exactly as Kafka runs one `RemoteLogReader` task per
/// remote fetch. A read that arrives with the pool's pending queue already
/// full is refused: see [`reject_saturated`].
pub(super) async fn try_remote_read(
    broker: &Broker,
    p: &mut PendingRead,
    part: &Partition,
) -> Option<usize> {
    let reader = broker.remote_reader.clone()?;
    let (remote_storage_enable, delivery_policy, delivery_uncertainty_ms, log_start) = {
        let log = part.log.lock().expect("log mutex poisoned");
        let config = log.config_snapshot();
        (
            config.remote_storage_enable,
            config.delivery_policy,
            config.delivery_clock_uncertainty.millis_i64_trunc(),
            log.established_log_start(),
        )
    };
    if !remote_storage_enable {
        return None;
    }
    // KIP-405's `RemoteFetchRequestsPerSec`. Counted once this partition is
    // known to be tiered, so a cluster with no tiered topic materialises no
    // series, and before the reader pool can refuse the read, so a refusal
    // still shows up as an attempt.
    broker
        .metrics
        .record_remote_request(crate::metrics::RemoteTierPath::Fetch, &p.topic_name);
    // KIP-405: the remote tier serves `[log_start, local_log_start)` and
    // nothing below. An offset under the global floor was deleted by
    // `DeleteRecords` or by remote retention, so it stays
    // `OFFSET_OUT_OF_RANGE` even while the RLMM still lists the segment that
    // held it. Kafka's `ReplicaManager.handleOffsetOutOfRangeError` builds a
    // `DelayedRemoteFetch` under the same condition.
    //
    // Only a floor this process moved may refuse a read. A floor inferred at
    // `Log::open` from the segments left on disk sits above everything the
    // archive holds on a partition whose local segments were evicted, and
    // refusing against it would hide the whole tier after every restart. See
    // `Log::established_log_start`.
    if matches!(log_start, Some(floor) if p.fetch_offset < floor.0) {
        return None;
    }
    if p.topic_id == WireUuid::ZERO {
        // Without a topic_id we can't build `TopicIdPartition` keyed the
        // same way the RLMM stores entries (Kafka's equality is by id +
        // partition).
        return None;
    }
    let topic_id = uuid::Uuid::from_bytes(p.topic_id.0);
    let tp = krabka_remote_storage::TopicIdPartition::new(
        topic_id,
        p.topic_name.clone(),
        p.partition_index,
    );
    // Atomic stores the raw epoch; wrap into `LeaderEpoch` for the
    // remote-reader / RLMM seam that follows.
    let current_leader_epoch = LeaderEpoch(
        part.current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire),
    );
    // Resolve the leader epoch that *owned* the requested fetch offset from
    // the local leader-epoch checkpoint (Kafka's `epochForOffset`).  The
    // checkpoint is only appended-to / truncated-from-end (never pruned from
    // the start on local eviction), so tiered offsets that are no longer
    // stored locally still resolve to their copy-time epoch.  Fall back to
    // the current leader epoch when the checkpoint has no entries (empty /
    // fresh log) so behavior is at least as good as before.
    let leader_epoch = {
        let log = part.log.lock().expect("log mutex poisoned");
        log.epoch_checkpoint()
            .epoch_for_offset(Offset(p.fetch_offset))
            .unwrap_or(current_leader_epoch)
    };
    let max_bytes = usize::try_from(p.max_bytes.max(0)).unwrap_or(0);

    // KIP-405's `remote.log.reader.threads` / `.max.pending.tasks` cap. The
    // permit is held for the batch read and the `.txnindex` read below, so one
    // cold fetch occupies one reader slot however many objects it touches.
    let Ok(_permit) = reader.pool.acquire().await else {
        return Some(reject_saturated(p));
    };
    // Kafka's `RemoteLogReaderFetchRateAndTimeMs` measures the reader task,
    // which is exactly the span this permit covers.
    let _read_timer = ReadTimer::started(&broker.metrics);

    match reader
        .fetch_batch(&tp, leader_epoch, p.fetch_offset, max_bytes)
        .await
    {
        Ok(Some(batch)) => {
            // A follower is never gated: it replicates a scheduled record, and
            // counts it toward the ISR, before any consumer may see it.
            if !p.is_follower_fetch
                && !remote_batch_is_deliverable(
                    delivery_policy,
                    delivery_uncertainty_ms,
                    batch.max_timestamp,
                    part.delivery.now_ms(),
                )
            {
                // Answer as the local path answers a batch that is not due: an
                // empty partition and no error. `OFFSET_OUT_OF_RANGE` would
                // send the consumer to its reset policy and lose the record it
                // is waiting for, and the batch is due later, not never.
                tracing::debug!(
                    topic = %p.topic_name,
                    partition = p.partition_index,
                    offset = p.fetch_offset,
                    max_timestamp = batch.max_timestamp,
                    "remote-reader: batch is not due yet; holding it back"
                );
                p.out.error_code = codes::NONE;
                if p.read_committed {
                    p.out.aborted_transactions = Some(Vec::new());
                }
                return Some(0);
            }
            let bytes_est = <RecordBatch as Encode>::encoded_len(&batch, 0);
            // `log_start_offset` / HW / LSO stay at whatever `do_read`
            // wrote out (the local view); the remote tier doesn't change
            // those pointers.

            // KIP-405 read-committed: surface the aborted-transaction list
            // from the segment's `.txnindex` so the consumer drops aborted
            // records client-side, mirroring the local `aborted_in_range`
            // call in `do_read` — bounded here to the single batch this read
            // returns (inclusive last offset), since the local path bounds by
            // the returned window over the LSO. `Some(empty)` is the correct
            // read-committed signal (read-uncommitted leaves it `None`).
            if p.read_committed && !p.is_follower_fetch {
                let Some(batch_last_offset) = batch
                    .base_offset
                    .checked_add(i64::from(batch.last_offset_delta))
                else {
                    tracing::warn!(
                        topic = %p.topic_name,
                        partition = p.partition_index,
                        offset = p.fetch_offset,
                        "remote-reader: batch offset overflow; leaving OFFSET_OUT_OF_RANGE"
                    );
                    return None;
                };
                let aborts = match reader
                    .aborted_transactions(&tp, leader_epoch, p.fetch_offset, batch_last_offset)
                    .await
                {
                    Ok(aborts) => aborts,
                    Err(e) => {
                        tracing::warn!(
                            topic = %p.topic_name,
                            partition = p.partition_index,
                            offset = p.fetch_offset,
                            error = %e,
                            "remote-reader: aborted_transactions failed; leaving OFFSET_OUT_OF_RANGE"
                        );
                        return None;
                    }
                };
                p.out.aborted_transactions = Some(
                    aborts
                        .into_iter()
                        .map(|e| AbortedTransaction {
                            producer_id: e.producer_id,
                            first_offset: e.start_offset,
                            ..Default::default()
                        })
                        .collect(),
                );
            }

            p.out.error_code = codes::NONE;
            p.out.records = Some(batch.into());
            // KIP-405's `RemoteFetchBytesPerSec`: what the tier actually
            // served, which is the batch that is about to go out.
            broker.metrics.record_remote_bytes(
                crate::metrics::RemoteTierPath::Fetch,
                &p.topic_name,
                u64::try_from(bytes_est).unwrap_or(0),
            );
            Some(bytes_est)
        }
        Ok(None) => None,
        Err(krabka_remote_storage::RemoteStorageError::NotReady { partition }) => {
            // The metadata partition that would answer this read is assigned
            // to this broker but its consumer has not caught up yet. Leave
            // OFFSET_OUT_OF_RANGE (retryable) — NOT a definitive miss — so the
            // client retries. Expected churn during catch-up, so log at debug.
            tracing::debug!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                metadata_partition = partition,
                "remote-reader: metadata partition not yet caught up; \
                 leaving OFFSET_OUT_OF_RANGE for client retry"
            );
            None
        }
        Err(e) => {
            // KIP-405's `RemoteFetchErrorsPerSec`. `NotReady` above is not one
            // of these: it is the metadata partition still catching up, which
            // the caller already surfaces as a retryable error code and
            // `failed_fetch_requests` already counts.
            broker
                .metrics
                .record_remote_error(crate::metrics::RemoteTierPath::Fetch, &p.topic_name);
            tracing::warn!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                error = %e,
                "remote-reader: fetch_batch failed; leaving OFFSET_OUT_OF_RANGE"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_log::{DeliveryPolicy, Log, LogConfig};
    use krabka_units::prelude::millis;

    /// KIP-405 splits the floors, and only the global one bounds the remote
    /// tier: an offset a `DeleteRecords` deleted is gone from every tier, so
    /// the fetch stays `OFFSET_OUT_OF_RANGE` even while the RLMM still lists
    /// the segment that held it. Kafka's
    /// `ReplicaManager.handleOffsetOutOfRangeError` builds its
    /// `DelayedRemoteFetch` under the same `logStartOffset <= fetchOffset`
    /// condition.
    ///
    /// The two cases run against one tiered partition in one order, so the
    /// second is measured against a tier that has just been shown to answer.
    /// `try_remote_read` reaching `fetch_batch` in the second case would
    /// therefore have served the same batch it served in the first; an
    /// untouched `p.out` is what says it never got there.
    /// The topic every tiered-read case below uses.
    const TIERED_TOPIC: &str = "tiered-delete-records";

    /// KIP-405's `remote.log.reader.max.pending.tasks`: a cold read that
    /// arrives with the reader pool's queue already full is answered with an
    /// error for that partition rather than parked behind the running reads.
    /// Kafka's executor throws `RejectedExecutionException`, which
    /// `Errors.forException` does not map, so the row goes out as
    /// `UNKNOWN_SERVER_ERROR`.
    ///
    /// The read-committed case also gets an empty aborted-transaction list,
    /// because a read-committed consumer reads that field and a `None` there
    /// means "read uncommitted", not "no aborts".
    #[test]
    fn a_refused_cold_read_answers_the_partition_with_an_error_and_no_records() {
        for read_committed in [false, true] {
            let mut pending = super::super::plan::PendingRead {
                topic_name: TIERED_TOPIC.to_string(),
                topic_id: krabka_protocol::primitives::uuid::Uuid::ZERO,
                partition_index: 0,
                current_leader_epoch: 0,
                last_fetched_epoch: -1,
                fetch_offset: 7,
                max_bytes: 1024,
                read_committed,
                is_follower_fetch: false,
                partition: None,
                out: krabka_protocol::owned::fetch_response::PartitionData {
                    error_code: crate::codes::OFFSET_OUT_OF_RANGE,
                    ..Default::default()
                },
                cpu_micros: 0,
            };

            let served = super::reject_saturated(&mut pending);

            check!(served == 0);
            check!(pending.out.error_code == crate::codes::UNKNOWN_SERVER_ERROR);
            check!(pending.out.records.is_none());
            check!(pending.out.aborted_transactions.is_some() == read_committed);
        }
    }

    /// The topic id every tiered-read case below uses.
    fn tiered_topic_id() -> uuid::Uuid {
        uuid::Uuid::from_u128(0x7157)
    }

    /// A partition whose sealed segments are all in the archive and none of
    /// them on local disk any more: exactly the shape the remote read path
    /// exists for.
    ///
    /// `reopen` drops the log after the eviction and opens it again from the
    /// same directory, which is what a broker restart does. Nothing durable
    /// carries the global floor across that yet, so the reopened log infers
    /// one from the segments that survived -- and on this partition that
    /// inference sits above the whole archive.
    async fn tiered_partition(
        broker: &std::sync::Arc<crate::broker::Broker>,
        dir: &std::path::Path,
        reopen: bool,
    ) -> std::sync::Arc<crate::partition::Partition> {
        use krabka_ids::{LeaderEpoch, PartitionIndex};

        use crate::remote_log_manager::{ArchiveMode, copy_eligible, test_support::tier};

        /// A batch wide enough that a 256-byte segment rolls every few
        /// appends, so the log seals segments the copy pass can tier.
        fn tiered_batch() -> krabka_protocol::records::RecordBatch {
            let mut batch = krabka_protocol::records::RecordBatch {
                last_offset_delta: 1,
                ..Default::default()
            };
            for offset_delta in 0..2 {
                batch.records.push(krabka_protocol::records::Record {
                    offset_delta,
                    value: Some(bytes::Bytes::from(vec![b'x'; 64])),
                    ..Default::default()
                });
            }
            batch
        }

        let config = LogConfig {
            segment_size: krabka_units::bytes(256),
            remote_storage_enable: true,
            ..LogConfig::default()
        };
        let part_dir = dir.join(format!("{TIERED_TOPIC}-0"));
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        let mut log = Log::open(&part_dir, config.clone()).expect("open partition log");
        for _ in 0..12 {
            let mut batch = tiered_batch();
            log.append(&mut batch).expect("append");
        }
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "the test needs sealed segments to tier");
        let tiered_through = exports.last().expect("sealed segments").last_offset + 1;

        let tp = krabka_remote_storage::TopicIdPartition::new(tiered_topic_id(), TIERED_TOPIC, 0);
        let reader = broker.remote_reader.clone().expect("remote reader");
        let copied = copy_eligible(
            &tier(ArchiveMode::Mutable, &reader.rsm, &reader.rlmm),
            &tp,
            1,
            LeaderEpoch(0),
            exports,
        )
        .await;
        assert!(copied > 0, "the copy pass should have tiered every segment");

        log.delete_local_segments_through(tiered_through)
            .expect("evict the copied segments");
        if reopen {
            drop(log);
            log = Log::open(&part_dir, config).expect("reopen partition log");
            assert!(
                log.log_start_offset() == tiered_through,
                "the reopened log infers its floor from what survived"
            );
        }
        crate::broker::spawn_partition(
            TIERED_TOPIC.to_string(),
            PartitionIndex(0),
            dir.to_path_buf(),
            log,
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        )
    }

    /// A broker with a local archive and an in-memory RLMM, and the
    /// directories both live in.
    async fn tiered_broker() -> (
        crate::broker::BrokerHandle,
        tempfile::TempDir,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let remote_dir = tempfile::tempdir().expect("remote tempdir");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        config.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
            dir: remote_dir.path().to_path_buf(),
        });
        config.remote_log_metadata = crate::config::RlmmKind::InMemory;
        let handle = crate::broker::Broker::start(config)
            .await
            .expect("start broker");
        (handle, dir, remote_dir)
    }

    /// A `PendingRead` for offset 0 that the local log has already answered
    /// `OFFSET_OUT_OF_RANGE`.
    fn out_of_range_at(
        part: &std::sync::Arc<crate::partition::Partition>,
        fetch_offset: i64,
    ) -> super::PendingRead {
        use krabka_protocol::primitives::uuid::Uuid as WireUuid;

        use crate::codes;

        super::PendingRead {
            topic_name: TIERED_TOPIC.into(),
            topic_id: WireUuid(tiered_topic_id().into_bytes()),
            partition_index: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset,
            max_bytes: 1_048_576,
            read_committed: false,
            is_follower_fetch: false,
            partition: Some(std::sync::Arc::clone(part)),
            out: krabka_protocol::owned::fetch_response::PartitionData {
                error_code: codes::OFFSET_OUT_OF_RANGE,
                ..Default::default()
            },
            cpu_micros: 0,
        }
    }

    #[tokio::test]
    async fn a_fetch_below_the_global_log_start_never_reaches_the_remote_tier() {
        use krabka_log::Offset;

        use crate::codes;

        let (broker_handle, dir, _remote_dir) = tiered_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let part = tiered_partition(&broker, dir.path(), false).await;

        let out_of_range = || out_of_range_at(&part, 0);
        // The floor has not moved, so offset 0 is the remote tier's to serve.
        let mut served = out_of_range();
        let bytes_served = super::try_remote_read(&broker, &mut served, &part).await;
        check!(bytes_served.is_some_and(|n| n > 0), "the tier answers");
        check!(served.out.error_code == codes::NONE);

        // `DeleteRecords` to offset 5. The remote segment that holds offset 0
        // is still listed in the RLMM and still in the archive, and it must
        // stop being reachable all the same.
        part.log
            .lock()
            .expect("log mutex poisoned")
            .set_log_start_offset(Offset(5))
            .expect("move the log start");

        let mut refused = out_of_range();
        let bytes_served = super::try_remote_read(&broker, &mut refused, &part).await;
        check!(bytes_served == None, "no tier answers below the floor");
        check!(refused.out.error_code == codes::OFFSET_OUT_OF_RANGE);
        check!(refused.out.records.is_none());

        // The floor itself still reads: the band above it is the tier's.
        let mut at_floor = out_of_range_at(&part, 5);
        let bytes_served = super::try_remote_read(&broker, &mut at_floor, &part).await;
        check!(bytes_served.is_some_and(|n| n > 0), "the floor is readable");

        broker_handle.shutdown().await;
    }

    /// A restart must not hide the archive.
    ///
    /// `Log::open` reads a global floor off the segments that survived on
    /// disk, and on a tiered partition whose local segments were evicted that
    /// reading sits above every offset the archive holds. Refusing a remote
    /// read against it would answer `OFFSET_OUT_OF_RANGE` for the whole tier
    /// after every restart, so only a floor this process moved refuses one.
    #[tokio::test]
    async fn a_restart_does_not_put_the_archive_out_of_range() {
        use crate::codes;

        let (broker_handle, dir, _remote_dir) = tiered_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let part = tiered_partition(&broker, dir.path(), true).await;

        let mut served = out_of_range_at(&part, 0);
        let bytes_served = super::try_remote_read(&broker, &mut served, &part).await;
        check!(
            bytes_served.is_some_and(|n| n > 0),
            "the tier still answers offset 0 after a restart"
        );
        check!(served.out.error_code == codes::NONE);

        broker_handle.shutdown().await;
    }

    #[test]
    fn a_remote_batch_is_held_back_until_its_activation_time_plus_the_bound() {
        // Immediate delivery reads no timestamp at all.
        assert!(super::remote_batch_is_deliverable(
            DeliveryPolicy::Immediate,
            250,
            10_000,
            0
        ));
        // Scheduled: due at 10_000, and the 250 ms clock bound is added to it.
        assert!(!super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_249
        ));
        assert!(super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_250
        ));
        // The bound is added to the activation time, never subtracted from it,
        // so a clock at the far end of its own uncertainty still never delivers
        // early.
        assert!(!super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_000
        ));
        // A saturating subtraction keeps the far end of the clock range safe.
        assert!(!super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            i64::MIN
        ));
    }

    #[test]
    fn local_and_remote_paths_agree_for_the_same_batch_and_clock() {
        for (activation_ms, now_ms) in [(10_000, 10_249), (10_000, 10_250), (i64::MAX, i64::MAX)] {
            let dir = tempfile::tempdir().expect("tempdir");
            let config = LogConfig {
                delivery_policy: DeliveryPolicy::Scheduled,
                delivery_clock_uncertainty: millis(250),
                ..LogConfig::default()
            };
            let mut log = Log::open(dir.path(), config).expect("open log");
            let mut batch = crate::delivery::test_support::batch_at(activation_ms);
            log.append(&mut batch).expect("append scheduled batch");
            let local_visible =
                log.advance_delivery_watermark(now_ms).watermark == log.log_end_offset();
            let remote_visible = super::remote_batch_is_deliverable(
                DeliveryPolicy::Scheduled,
                250,
                activation_ms,
                now_ms,
            );

            assert!(local_visible == remote_visible);
        }
    }
}
