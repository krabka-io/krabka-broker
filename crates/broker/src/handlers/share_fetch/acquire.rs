//! The acquire passes that KIP-932 runs over the pending partitions: apply the
//! piggybacked acknowledgements, expire stale locks, materialize newly
//! produced records, and lock a batch of `Available` ones for this member.
//!
//! This is the stage that owns the per-partition
//! [`AcquisitionState`](crate::share_partition::state::AcquisitionState) locks,
//! the request-wide record budget, and the retry pass behind the long poll.

use std::{sync::Arc, time::Instant};

use krabka_log::{LogError, Offset};

use super::{
    acknowledge::apply_one_ack,
    long_poll::{arm_waits, long_poll},
    pending::PendingPartition,
    records::{
        AcquireRequest, acquire_read_records, pending_activation_ranges, read_budget,
        unreadable_batch_ranges,
    },
};
use crate::{
    broker::Broker, codes, error::BrokerError,
    share_partition::manager::persistence::fences_the_partition,
};

/// KFC-1: the most not-yet-due records an acquire pass leaves in one share
/// partition's window.
///
/// A deferred run is not in flight, so it does not spend
/// `share_group_max_inflight_records`, and materialization walks past it to
/// reach the due records behind it. That is the point of the whole path, and
/// it needs a second bound of its own: without one, a single far-future batch
/// at the head of the window pulls the rest of the log in behind it, and every
/// later pass re-walks all of it.
///
/// This should be the broker config `share_delivery_max_deferred_records`. It
/// is a constant here because the runtime settings it belongs beside live in
/// `file_config.rs` and in
/// [`ShareGroupConfig`](crate::coordinator::unified::share::config::ShareGroupConfig).
const SHARE_DELIVERY_MAX_DEFERRED_RECORDS: i64 = 1_000;

#[derive(Clone, Copy)]
pub(super) struct AcquireContext<'a> {
    pub(super) broker: &'a Broker,
    pub(super) manager: &'a Arc<crate::share_partition::manager::SharePartitionLeaderManager>,
    pub(super) group: &'a str,
    pub(super) member: &'a str,
    pub(super) max_records: i32,
    pub(super) max_bytes: i32,
    pub(super) renewal: super::acknowledge::Renewal,
    pub(super) config: &'a crate::coordinator::unified::share::config::ShareGroupConfig,
}

pub(super) async fn acquire_records(
    context: &AcquireContext<'_>,
    pending: &mut [PendingPartition],
    max_wait_ms: i32,
) -> Result<(), BrokerError> {
    // Arm the long poll's waiters before the first acquire pass, so a record
    // produced while that pass runs still wakes the park that follows it.
    let waits = if max_wait_ms > 0 {
        arm_waits(context.broker, pending)
    } else {
        Vec::new()
    };
    let acquired = acquire_pass(context, pending, true).await?;
    if acquired == 0 && max_wait_ms > 0 {
        long_poll(waits, max_wait_ms).await;
        acquire_pass(context, pending, false).await?;
    }
    Ok(())
}

/// Pulls freshly produced records into the acquisition window, unless the
/// schedule already holds back [`SHARE_DELIVERY_MAX_DEFERRED_RECORDS`] of them.
///
/// The previous pass's deferral still stands when this runs, which is why the
/// count means something here and would read zero after `promote_deferred`. It
/// is one pass out of date, and that only makes the bound conservative.
fn materialize_within_deferral_bound(
    state: &mut crate::share_partition::state::AcquisitionState,
    upper: Offset,
    max_inflight: i32,
) {
    if state.deferred_records() < SHARE_DELIVERY_MAX_DEFERRED_RECORDS {
        state.materialize(upper, max_inflight);
    }
}

/// Materializes the window and archives the offsets that no share consumer
/// may get, until the window holds an `Available` record or cannot grow.
///
/// Transaction markers occupy log offsets but are broker metadata, not user
/// records. They are archived before acquisition so their encoded coordinator
/// epoch can never appear in a `ShareFetch` response. Under `read_committed`,
/// the data of aborted transactions is archived too, as Kafka's
/// `SharePartition` does: the share consumer has no aborted transaction list
/// to filter them with.
///
/// One materialization adds at most `max_inflight` offsets. When all of them
/// are archived, the window grows again in the same pass, so a large aborted
/// transaction does not turn into a run of empty fetches. The first scan
/// covers the whole window, and each later scan covers only the offsets that
/// the last materialization added.
async fn grow_readable_window(
    state: &mut crate::share_partition::state::AcquisitionState,
    partition: &crate::partition::Partition,
    upper: Offset,
    max_inflight: i32,
    read_committed: bool,
) -> Result<(), BrokerError> {
    let mut scan_from = state.start_offset;
    loop {
        let end_before = state.end_offset;
        materialize_within_deferral_bound(state, upper, max_inflight);
        let scan_start = scan_from.max(state.start_offset);
        for (first, last) in
            unreadable_batch_ranges(partition, scan_start, state.end_offset, read_committed).await?
        {
            state.archive_internal(first, last);
        }
        if state.end_offset == end_before || state.has_available() {
            return Ok(());
        }
        scan_from = state.end_offset;
    }
}

fn remaining_record_budget(max_records: i32, acquired: i64) -> i32 {
    max_records
        .saturating_sub(i32::try_from(acquired).unwrap_or(i32::MAX))
        .max(0)
}

/// The read-only inputs [`grow_and_acquire`] needs, gathered into one value so
/// the function itself takes few enough arguments for `clippy::pedantic`.
struct GrowAndAcquireArgs<'a> {
    part: &'a Arc<crate::partition::Partition>,
    upper: Offset,
    cfg: &'a crate::coordinator::unified::share::config::ShareGroupConfig,
    read_committed: bool,
    member: &'a str,
    max_bytes: i32,
    remaining_records: i32,
    now: Instant,
}

/// Grows the readable window, promotes due deferrals, and acquires records
/// for one partition, in that order.
///
/// This is everything an acquire pass does with a partition's log once its
/// acknowledgements are applied and its exhausted records are archived. It is
/// split out so the caller can catch [`LogError::OffsetTooLow`] around the
/// whole sequence: any of these steps can read at the share-partition start
/// offset, and that offset can sit below the log's start offset.
async fn grow_and_acquire(
    st: &mut crate::share_partition::state::AcquisitionState,
    out: &mut krabka_protocol::owned::share_fetch_response::PartitionData,
    args: GrowAndAcquireArgs<'_>,
) -> Result<i64, BrokerError> {
    let GrowAndAcquireArgs {
        part,
        upper,
        cfg,
        read_committed,
        member,
        max_bytes,
        remaining_records,
        now,
    } = args;
    grow_readable_window(st, part, upper, cfg.max_inflight_records, read_committed).await?;
    // KFC-1: re-derive the deferral from the log and this partition's own
    // clock on every pass, exactly as the control-batch ranges above are.
    // Dropping it first is what keeps a batch that has since come due from
    // staying held back by an older clock reading.
    st.promote_deferred();
    let deferred =
        pending_activation_ranges(part, st.start_offset, st.end_offset, part.delivery.now_ms())
            .await?;
    for (first, last) in deferred {
        st.defer_internal(first, last);
    }
    if remaining_records <= 0 {
        return Ok(0);
    }
    let request = AcquireRequest {
        member,
        max_records: remaining_records,
        max_bytes,
        upper,
        now,
        lock_duration: cfg.record_lock_duration,
        max_attempts: cfg.max_delivery_attempts,
    };
    acquire_read_records(out, part, st, &request).await
}

/// True when `err` is the log answering that a read started below its log
/// start offset: `Log::check_locally_readable`'s [`LogError::OffsetTooLow`].
///
/// This is the retention/`DeleteRecords`/old-earliest-reset race the SPSO
/// cannot see coming: the log start offset moved past it between one acquire
/// pass and the next. It is not a partition-fencing error and not a corrupt
/// read, so the caller recovers from it in place of failing the request.
fn log_start_moved_past_spso(err: &BrokerError) -> bool {
    matches!(err, BrokerError::Log(LogError::OffsetTooLow { .. }))
}

/// Runs one acquire pass over the pending partitions that this broker can
/// lead.
///
/// When `apply_acks` is true, this function applies the piggybacked
/// acknowledgement batches first, and sets `acknowledge_error_code`. A batch
/// offset of type Renew renews its acquisition lock, per KIP-1222.
///
/// Under a `ReadCommitted` isolation level, this function clamps the
/// materialize and read window to the partition's last stable offset, so it
/// never acquires an uncommitted record, and it archives the data batches of
/// aborted transactions in the window. It returns the total number of
/// offsets that it acquired across all partitions in this pass.
///
/// On a KFC-1 scheduled topic it also re-derives which ranges of the window
/// are not due yet and marks them `Deferred`, so acquisition steps over them.
/// The derivation is thrown away and redone on each pass, so nothing outlives
/// the clock reading that produced it.
async fn acquire_pass(
    context: &AcquireContext<'_>,
    pending: &mut [PendingPartition],
    apply_acks: bool,
) -> Result<i64, BrokerError> {
    let &AcquireContext {
        broker,
        manager: mgr,
        group,
        member,
        max_records,
        max_bytes,
        renewal,
        config: cfg,
    } = context;
    let now = Instant::now();
    let read_committed = matches!(
        cfg.isolation_level,
        crate::coordinator::unified::share::config::ShareIsolationLevel::ReadCommitted
    );
    let mut total = 0_i64;

    for p in pending.iter_mut() {
        if !p.leadable {
            continue;
        }
        // Reset any prior pass's data for a clean re-acquire.
        p.out.records = None;
        p.out.acquired_records.clear();

        let has_acks = apply_acks && !p.ack_batches.is_empty();
        // A failed state read fails the partition and caches nothing, as
        // Kafka's `SharePartitionManager.handleInitializationException` does.
        let cell = match mgr.get_or_load(group, p.topic_id, p.partition_index).await {
            Ok(cell) => cell,
            Err(code) => {
                fail_partition(p, has_acks, code);
                continue;
            }
        };
        let mut st = cell.lock().await;

        // Apply piggybacked acknowledgements (first pass only). The type
        // Renew renews the lock of its offsets, and the other types take
        // their normal transition. The change is durable before the
        // acquisition runs, or it is rolled back and the write error becomes
        // the acknowledge error.
        if has_acks {
            let ack_batches = &p.ack_batches;
            let code = mgr
                .apply_durably(group, p.topic_id, p.partition_index, &cell, &mut st, |st| {
                    let mut ack_err = codes::NONE;
                    for (first, last, types) in ack_batches {
                        if let Err(code) =
                            apply_one_ack(st, member, *first, *last, types, now, renewal)
                        {
                            ack_err = code;
                        }
                    }
                    ack_err
                })
                .await;
            p.out.acknowledge_error_code = code;
            if fences_the_partition(code) {
                fail_partition(p, true, code);
                continue;
            }
        }

        if !p.fetchable {
            // Best-effort: a failed write keeps the state dirty for a retry.
            let _ = mgr
                .persist_if_dirty(group, p.topic_id, p.partition_index, Some(&cell), &mut st)
                .await;
            continue;
        }

        // Expire stale locks, materialize freshly produced records, acquire.
        st.expire_locks(now);
        let part = p.topic_name.as_deref().and_then(|name| {
            broker
                .partitions
                .get(name, krabka_ids::PartitionIndex(p.partition_index))
        });
        let Some(part) = part else {
            // Lost the partition between the leadership check and here.
            p.out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
            p.leadable = false;
            // Best-effort: a failed write keeps the state dirty for a retry.
            let _ = mgr
                .persist_if_dirty(group, p.topic_id, p.partition_index, Some(&cell), &mut st)
                .await;
            continue;
        };
        let hwm = part.high_watermark().await;
        // Under read_committed, never surface records past the last stable
        // offset: clamp the materialize/read window to `min(lso, hwm)` so no
        // record from an OPEN transaction can be acquired.
        //
        // KFC-1 puts no delivery watermark here on purpose. That cap is what
        // holds a classic group to offset order, and a share group is exactly
        // the reader that does not need it: the window runs to the high
        // watermark and the deferral marks below hold the waiting records
        // back one range at a time.
        let upper = if read_committed {
            part.last_stable_offset(hwm)
        } else {
            hwm
        };
        // A released or expired record at the delivery limit is archived
        // first, so it cannot hold the window shut.
        st.archive_exhausted(cfg.max_delivery_attempts);
        let remaining_records = remaining_record_budget(max_records, total);
        let outcome = grow_and_acquire(
            &mut st,
            &mut p.out,
            GrowAndAcquireArgs {
                part: &part,
                upper,
                cfg,
                read_committed,
                member,
                max_bytes: read_budget(p.partition_max_bytes, max_bytes),
                remaining_records,
                now,
            },
        )
        .await;
        let acquired_count = match outcome {
            Ok(count) => count,
            Err(err) if log_start_moved_past_spso(&err) => {
                // Kafka's `ShareFetchUtils.processFetchResponse` /
                // `SharePartition.updateCacheAndOffsets`: the log start offset
                // moved past the SPSO. Archive the Available/Deferred records
                // below it, move the SPSO (and the SPEO, if the window had not
                // grown that far), and answer this partition with NONE and no
                // records instead of failing the whole request. An Acquired
                // record below the new start stays locked until it times out.
                st.advance_past_log_start(part.log_start_offset());
                p.out.records = None;
                p.out.acquired_records.clear();
                0
            }
            Err(err) => return Err(err),
        };

        p.out.error_code = codes::NONE;
        // The acquisition itself is not durable state (an acquired record
        // persists as available), so a failed write keeps the state dirty for
        // a retry. A fenced write drops the cell, and the records acquired on
        // it must not reach the client.
        match mgr
            .persist_if_dirty(group, p.topic_id, p.partition_index, Some(&cell), &mut st)
            .await
        {
            Err(code) if fences_the_partition(code) => fail_partition(p, false, code),
            _ => total += acquired_count,
        }
    }
    Ok(total)
}

/// Fails one partition row with a share-partition error, and leaves it out of
/// any later acquire pass.
///
/// The fetch error goes on a row of the share session, and the acknowledge
/// error on a row that carried acknowledgements.
fn fail_partition(p: &mut PendingPartition, has_acks: bool, code: i16) {
    p.out.records = None;
    p.out.acquired_records.clear();
    if p.fetchable {
        p.out.error_code = code;
    }
    if has_acks {
        p.out.acknowledge_error_code = code;
    }
    p.leadable = false;
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn materialization_stops_at_the_deferred_record_bound() {
        let bound = SHARE_DELIVERY_MAX_DEFERRED_RECORDS;
        let inflight = i32::try_from(bound).expect("the bound fits an inflight budget");
        for (deferred, expected_end) in [(bound - 1, bound + 9), (bound, bound)] {
            let mut state = crate::share_partition::state::AcquisitionState::new(Offset(0));
            state.materialize(Offset(deferred), inflight);
            state.defer_internal(Offset(0), Offset(deferred - 1));
            assert!(state.deferred_records() == deferred);

            materialize_within_deferral_bound(&mut state, Offset(deferred + 10), 10);

            assert!(
                state.end_offset == Offset(expected_end),
                "deferred={deferred}"
            );
        }
    }

    /// A transactional batch at offsets 0-2 from producer 1000, its end marker
    /// at offset 3, and a plain record at offset 4.
    fn transaction_then_record(commit: bool) -> Vec<krabka_protocol::records::RecordBatch> {
        use bytes::Bytes;
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        let transactional = Attributes::default().with_transactional(true);
        let value = |v: &'static [u8]| Record {
            value: Some(Bytes::from_static(v)),
            ..Record::default()
        };
        // Control key: version 0, marker type (0 abort, 1 commit). Control
        // value: version 0, coordinator epoch 0.
        let marker_key = [0, 0, 0, u8::from(commit)];
        vec![
            RecordBatch {
                last_offset_delta: 2,
                producer_id: 1000,
                attributes: transactional,
                records: (0..3)
                    .map(|offset_delta| Record {
                        offset_delta,
                        ..value(b"txn")
                    })
                    .collect(),
                ..RecordBatch::default()
            },
            RecordBatch {
                producer_id: 1000,
                attributes: transactional.with_control(true),
                records: vec![Record {
                    key: Some(Bytes::copy_from_slice(&marker_key)),
                    value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            },
            RecordBatch {
                records: vec![value(b"plain")],
                ..RecordBatch::default()
            },
        ]
    }

    /// One pass reaches the first readable record behind a transaction that
    /// fills whole materialization windows with offsets it archives.
    #[tokio::test]
    async fn the_window_grows_past_offsets_that_are_all_archived() {
        use std::sync::Arc;

        use qubit_clock::{FixedWallClock, WallClock};

        use crate::delivery::test_support::{NOW_MS, partition_with_batches, wall_at};

        // (commit, read_committed, max_inflight, acquired)
        let cases = [
            (false, true, 2, vec![(Offset(4), Offset(4))]),
            (false, true, 100, vec![(Offset(4), Offset(4))]),
            (false, false, 2, vec![(Offset(0), Offset(1))]),
            (true, true, 2, vec![(Offset(0), Offset(1))]),
        ];
        let clock: Arc<dyn WallClock> = Arc::new(FixedWallClock::new(wall_at(NOW_MS)));
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (commit, read_committed, max_inflight, acquired) in cases {
            let dir = tempfile::tempdir().expect("a log root");
            let partition = partition_with_batches(
                &dir,
                "txn",
                krabka_log::LogConfig::default(),
                transaction_then_record(commit),
                0,
                &clock,
            );
            let mut state = crate::share_partition::state::AcquisitionState::new(Offset(0));

            grow_readable_window(
                &mut state,
                &partition,
                Offset(5),
                max_inflight,
                read_committed,
            )
            .await
            .expect("grow the window");
            let got: Vec<_> = state
                .acquire(
                    "m",
                    500,
                    Offset(4),
                    std::time::Instant::now(),
                    std::time::Duration::from_secs(30),
                    5,
                )
                .into_iter()
                .map(|range| (range.first, range.last))
                .collect();

            let row = (commit, read_committed, max_inflight);
            actual.push((row, got));
            expected.push((row, acquired));
        }
        assert!(actual == expected);
    }

    #[test]
    fn remaining_record_budget_is_request_wide_and_saturating() {
        check!(remaining_record_budget(500, 300) == 200);
        check!(remaining_record_budget(500, 500) == 0);
        check!(remaining_record_budget(500, i64::MAX) == 0);
    }
}
