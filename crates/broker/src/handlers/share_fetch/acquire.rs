//! The acquire passes that KIP-932 runs over the pending partitions: apply the
//! piggybacked acknowledgements, expire stale locks, materialize newly
//! produced records, and lock a batch of `Available` ones for this member.
//!
//! This is the stage that owns the per-partition
//! [`AcquisitionState`](crate::share_partition::state::AcquisitionState) locks,
//! the request-wide record budget, and the retry pass behind the long poll.

use {
    super::{
        acknowledge::{AckApplication, apply_acknowledgements},
        long_poll::{LongPollOutcome, arm_waits, long_poll},
        pending::PendingPartition,
        records::{
            AcquireMode, AcquireRequest, DisklessSource, MergedBatches, acquire_read_records,
            merge_batches, pending_activation_ranges, read_budget, unreadable_batch_ranges,
        },
        tiered::TieredSource,
    },
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        partition::Partition,
        share_partition::{
            group_settings::GroupShareSettings,
            manager::{SharePartitionLeaderManager, persistence::fences_the_partition},
            source::diskless_logical_start,
            state::AcquisitionState,
        },
    },
    krabka_log::{LogError, Offset},
    krabka_protocol::{owned::share_fetch_response::PartitionData, records::RecordsPayload},
    std::{
        sync::{Arc, PoisonError},
        time::{Duration, Instant},
    },
    tokio::sync::Mutex,
};

/// KFC-1: the most not-yet-due records an acquire pass leaves in one share
/// partition's window.
///
/// A deferred run is not in flight, so it does not spend the record lock
/// limit, `share_group_partition_max_record_locks`, and materialization walks
/// past it to reach the due records behind it. That is the point of the whole path, and
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
    pub(super) manager: &'a Arc<SharePartitionLeaderManager>,
    pub(super) group: &'a str,
    pub(super) member: &'a str,
    pub(super) max_records: i32,
    /// The byte budget of the whole response, split across the partitions.
    pub(super) max_bytes: i32,
    /// The record bytes the response waits for, up to `max_bytes`.
    pub(super) min_bytes: i32,
    /// `ShareAcquireMode` and `BatchSize`.
    pub(super) mode: AcquireMode,
    pub(super) renewal: super::acknowledge::Renewal,
    /// The group's share settings: lock duration, delivery count limit,
    /// record lock limit and isolation level.
    pub(super) settings: GroupShareSettings,
}

/// What the passes of one request have taken so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Spent {
    records: i64,
    bytes: i64,
}

/// Runs acquire passes until the response holds `min_bytes` of records, the
/// record or byte budget is spent, or `max_wait_ms` runs out.
///
/// This is Kafka's `DelayedShareFetch`: the request waits in purgatory until
/// `isMinBytesSatisfied` or the wait ends. Kafka measures the bytes that the
/// log holds past each fetch offset; this measures the bytes it acquired,
/// which is the same for records that no other member takes first. A
/// `min_bytes` of 0 is satisfied at once. Each later pass adds to what the
/// earlier ones acquired.
pub(super) async fn acquire_records(
    context: &AcquireContext<'_>,
    pending: &mut [PendingPartition],
    max_wait_ms: i32,
) {
    let wait = Duration::from_millis(u64::try_from(max_wait_ms).unwrap_or(0));
    let deadline = Instant::now() + wait;
    let min_bytes = i64::from(context.min_bytes.min(context.max_bytes).max(0));
    let mut spent = Spent::default();
    let mut first = true;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        // Arm the long poll's waiters before the pass, so a record produced
        // while the pass runs still wakes the park that follows it.
        let waits = if left.is_zero() {
            Vec::new()
        } else {
            arm_waits(context.broker, pending)
        };
        spent = acquire_pass(context, pending, first, spent).await;
        first = false;
        let done = spent.bytes >= min_bytes
            || spent.records >= i64::from(context.max_records)
            || spent.bytes >= i64::from(context.max_bytes);
        if done || left.is_zero() {
            return;
        }
        let left_ms = i32::try_from(
            deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )
        .unwrap_or(i32::MAX);
        if long_poll(waits, left_ms).await == LongPollOutcome::NoPartitions {
            return;
        }
    }
}

/// Kafka's `PartitionMaxBytesStrategy.uniformPartitionMaxBytes`: the byte
/// budget of the partition at `index` of `partitions`.
///
/// An even share each, with the remainder on one partition. A budget smaller
/// than the partition count gives one byte to as many partitions as it
/// covers and none to the rest. Kafka picks the partitions for the remainder
/// and for the single bytes at random; this picks the first ones, which the
/// round-robin rotation of the partitions spreads over the requests.
fn uniform_share(budget: i64, partitions: usize, index: usize) -> i32 {
    let (Ok(count), Ok(at)) = (i64::try_from(partitions), i64::try_from(index)) else {
        return 0;
    };
    if count == 0 || budget <= 0 {
        return 0;
    }
    let share = if budget >= count {
        budget / count + if at == 0 { budget % count } else { 0 }
    } else {
        i64::from(at < budget)
    };
    i32::try_from(share).unwrap_or(i32::MAX)
}

/// Pulls freshly produced records into the acquisition window, unless the
/// schedule already holds back [`SHARE_DELIVERY_MAX_DEFERRED_RECORDS`] of them.
///
/// The previous pass's deferral still stands when this runs, which is why the
/// count means something here and would read zero after `promote_deferred`. It
/// is one pass out of date, and that only makes the bound conservative.
fn materialize_within_deferral_bound(
    state: &mut AcquisitionState,
    upper: Offset,
    max_record_locks: i32,
) {
    if state.deferred_records() < SHARE_DELIVERY_MAX_DEFERRED_RECORDS {
        state.materialize(upper, max_record_locks);
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
/// One materialization adds at most the room the record lock limit leaves. When all of them
/// are archived, the window grows again in the same pass, so a large aborted
/// transaction does not turn into a run of empty fetches. The first scan
/// covers the whole window, and each later scan covers only the offsets that
/// the last materialization added.
async fn grow_readable_window(
    state: &mut AcquisitionState,
    partition: &Partition,
    upper: Offset,
    max_record_locks: i32,
    read_committed: bool,
) -> Result<(), BrokerError> {
    // The scan floor must never sit below the first offset the local log
    // holds. An `Acquired` batch below a moved log start keeps
    // `state.start_offset` (the SPSO) pinned there until its lock expires
    // (see `AcquisitionState::advance_past_log_start`), but the log itself no
    // longer has that range to read. Without this clamp, every pass would
    // re-scan the now-deleted prefix and hit `LogError::OffsetTooLow` again,
    // even though `Available` offsets exist at or above the log start. On a
    // tiered partition (KIP-405) the offsets below the local log start are in
    // the remote tier, and the acquire step classifies them from the batches
    // it reads there.
    let log_start = partition
        .log
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .local_log_start_offset();
    let mut scan_from = state.start_offset.max(log_start);
    loop {
        let end_before = state.end_offset;
        materialize_within_deferral_bound(state, upper, max_record_locks);
        let scan_start = scan_from.max(state.start_offset).max(log_start);
        for span in
            unreadable_batch_ranges(partition, scan_start, state.end_offset, read_committed).await?
        {
            state.archive_internal(span.first, span.last);
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
    part: &'a Arc<Partition>,
    upper: Offset,
    settings: &'a GroupShareSettings,
    member: &'a str,
    max_bytes: i32,
    min_one_batch: bool,
    remaining_records: i32,
    mode: AcquireMode,
    now: Instant,
    tier: Option<TieredSource<'a>>,
    diskless: Option<DisklessSource<'a>>,
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
    st: &mut AcquisitionState,
    out: &mut PartitionData,
    args: GrowAndAcquireArgs<'_>,
) -> Result<i64, BrokerError> {
    let GrowAndAcquireArgs {
        part,
        upper,
        settings,
        member,
        max_bytes,
        min_one_batch,
        remaining_records,
        mode,
        now,
        tier,
        diskless,
    } = args;
    grow_readable_window(
        st,
        part,
        upper,
        settings.max_record_locks,
        settings.read_committed,
    )
    .await?;
    // KFC-1: re-derive the deferral from the log and this partition's own
    // clock on every pass, exactly as the control-batch ranges above are.
    // Dropping it first is what keeps a batch that has since come due from
    // staying held back by an older clock reading.
    st.promote_deferred();
    let deferred =
        pending_activation_ranges(part, st.start_offset, st.end_offset, part.delivery.now_ms())
            .await?;
    for span in deferred {
        st.defer_internal(span.first, span.last);
    }
    if remaining_records <= 0 {
        return Ok(0);
    }
    let request = AcquireRequest {
        member,
        max_records: remaining_records,
        max_bytes,
        min_one_batch,
        upper,
        now,
        lock_duration: settings.record_lock_duration,
        max_attempts: settings.delivery_count_limit,
        mode,
        tier,
        diskless,
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

/// Applies the acknowledgements piggybacked on `p` (first pass only), all or
/// nothing, and sets its `acknowledge_error_code`.
///
/// The type Renew renews the lock of its offsets, and the other types take
/// their normal transition. The change is durable before the acquisition
/// runs, or it is rolled back and the write error becomes the acknowledge
/// error.
///
/// # Errors
///
/// Returns the acknowledge error code when it fences the partition.
async fn apply_piggybacked_acks(
    context: &AcquireContext<'_>,
    now: Instant,
    p: &mut PendingPartition,
    cell: &Arc<Mutex<AcquisitionState>>,
    st: &mut AcquisitionState,
) -> Result<(), i16> {
    let &AcquireContext {
        manager: mgr,
        group,
        member,
        renewal,
        settings,
        ..
    } = context;
    let application = AckApplication {
        member,
        now,
        renewal,
        max_attempts: settings.delivery_count_limit,
    };
    let batches = p.ack_batches.iter().map(|batch| {
        (
            batch.first_offset,
            batch.last_offset,
            batch.acknowledge_types.as_slice(),
        )
    });
    let code = mgr
        .apply_durably(group, p.topic_id, p.partition_index, cell, st, |st| {
            apply_acknowledgements(st, &application, batches)
        })
        .await;
    p.out.acknowledge_error_code = code;
    if fences_the_partition(code) {
        return Err(code);
    }
    Ok(())
}

/// The remote tier (KIP-405) that `p`'s reads fall back to, when this broker
/// has a remote reader and the row names its topic.
fn tiered_source<'a>(
    broker: &'a Broker,
    p: &PendingPartition,
    read_committed: bool,
) -> Option<TieredSource<'a>> {
    broker
        .remote_reader
        .as_deref()
        .zip(p.topic_name.as_ref())
        .map(|(reader, topic)| TieredSource {
            reader,
            metrics: &broker.metrics,
            tp: krabka_remote_storage::TopicIdPartition::new(
                p.topic_id,
                topic.clone(),
                p.partition_index,
            ),
            read_committed,
        })
}

/// The highest offset an acquire pass may materialize or read on `part`.
///
/// Under read_committed, never surface records past the last stable offset:
/// clamp the materialize/read window to `min(lso, hwm)` so no record from an
/// OPEN transaction can be acquired.
///
/// KFC-1 puts no delivery watermark here on purpose. That cap is what holds a
/// classic group to offset order, and a share group is exactly the reader that
/// does not need it: the window runs to the high watermark and the deferral
/// marks hold the waiting records back one range at a time.
async fn read_upper_bound(part: &Partition, read_committed: bool) -> Offset {
    let hwm = part.high_watermark().await;
    if read_committed {
        part.last_stable_offset(hwm)
    } else {
        hwm
    }
}

/// What one partition may acquire in a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadBudget {
    /// The byte cap of the partition's read.
    max_bytes: i32,
    /// Whether the read may exceed `max_bytes` by the one batch it starts with.
    min_one_batch: bool,
    /// The records the partition may still acquire.
    remaining_records: i32,
}

/// The read budget of a partition whose byte `share` of the pass is known,
/// after the request already spent `total`.
fn partition_read_budget(
    max_records: i32,
    max_bytes: i32,
    total: Spent,
    partition_max_bytes: i32,
    share: i32,
) -> ReadBudget {
    // A partition that the byte budget does not reach acquires nothing this
    // pass.
    let remaining_records = if share > 0 {
        remaining_record_budget(max_records, total.records)
    } else {
        0
    };
    // Kafka's `ReplicaManager.readFromLog`: each read is capped at what the
    // response has left, and only a read made while the response still holds
    // no record may exceed its cap, by the one batch that it starts with.
    // Without that, each partition whose next batch is larger than its share
    // would add one oversized batch.
    let response_left = i64::from(max_bytes).saturating_sub(total.bytes).max(0);
    ReadBudget {
        max_bytes: read_budget(partition_max_bytes, share)
            .min(i32::try_from(response_left).unwrap_or(i32::MAX)),
        min_one_batch: total.bytes == 0,
        remaining_records,
    }
}

/// Runs [`grow_and_acquire`], and recovers once when the log start offset has
/// moved past the SPSO.
///
/// `args` builds the inputs of each attempt. The records of the attempt that
/// counts land in `fresh`; a retry starts it over.
async fn acquire_with_log_start_recovery<'a>(
    broker: &Broker,
    part: &Arc<Partition>,
    topic_id: uuid::Uuid,
    st: &mut AcquisitionState,
    fresh: &mut PartitionData,
    args: impl Fn() -> GrowAndAcquireArgs<'a>,
) -> Result<i64, BrokerError> {
    let outcome = grow_and_acquire(st, fresh, args()).await;
    if !outcome.as_ref().is_err_and(log_start_moved_past_spso) {
        return outcome;
    }
    // Kafka's `ShareFetchUtils.processFetchResponse` /
    // `SharePartition.updateCacheAndOffsets`: the log start offset moved past
    // the SPSO. Archive the Available/Deferred records below it, move the
    // SPSO (and the SPEO, if the window had not grown that far). An Acquired
    // record below the new start stays locked until it times out.
    let floor = match broker.diskless_read.as_deref().filter(|_| part.diskless) {
        Some(reader) => diskless_logical_start(reader, part, topic_id).await,
        None => Ok(part.log_start_offset()),
    }?;
    st.advance_past_log_start(floor);
    *fresh = PartitionData::default();
    // Retry immediately so a repaired floor with readable records does not
    // park for the full long-poll timeout.
    let retried = grow_and_acquire(st, fresh, args()).await;
    if retried.as_ref().is_err_and(log_start_moved_past_spso) {
        // A concurrent floor move is retried on the next pass. The local-only
        // WAL trim frontier cannot establish the logical deletion floor.
        *fresh = PartitionData::default();
        return Ok(0);
    }
    retried
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
    spent: Spent,
) -> Spent {
    let mut pass = PassProgress {
        now: Instant::now(),
        apply_acks,
        fetching: pending.iter().filter(|p| p.leadable && p.fetchable).count(),
        byte_budget: i64::from(context.max_bytes) - spent.bytes,
        fetch_index: 0,
        total: spent,
    };
    for p in pending.iter_mut() {
        acquire_partition(context, &mut pass, p).await;
    }
    pass.total
}

/// What one acquire pass carries from one partition to the next.
struct PassProgress {
    now: Instant,
    /// Whether this pass applies the piggybacked acknowledgements.
    apply_acks: bool,
    /// The fetchable, leadable partitions that the pass splits its bytes over.
    fetching: usize,
    /// The response bytes this pass may still fill.
    byte_budget: i64,
    /// The position of the next fetchable partition in that split.
    fetch_index: usize,
    /// What this pass and the earlier ones have acquired.
    total: Spent,
}

/// Runs one acquire pass over one pending partition, if this broker can lead
/// it.
async fn acquire_partition(
    context: &AcquireContext<'_>,
    pass: &mut PassProgress,
    p: &mut PendingPartition,
) {
    let &AcquireContext {
        broker,
        manager: mgr,
        group,
        member,
        max_records,
        max_bytes,
        mode,
        settings,
        ..
    } = context;
    let read_committed = settings.read_committed;
    if !p.leadable {
        return;
    }

    let mut has_acks = pass.apply_acks && !p.ack_batches.is_empty();
    // Kafka's `SharePartitionManager.acknowledge` runs before the fetch,
    // and answers UNKNOWN_TOPIC_OR_PARTITION for a share partition that no
    // earlier fetch on this broker loaded.
    if has_acks && mgr.cached(group, p.topic_id, p.partition_index).is_none() {
        p.out.acknowledge_error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
        has_acks = false;
    }
    // A failed state read fails the partition and caches nothing, as
    // Kafka's `SharePartitionManager.handleInitializationException` does.
    let cell = match mgr.get_or_load(group, p.topic_id, p.partition_index).await {
        Ok(cell) => cell,
        Err(code) => {
            fail_partition(p, has_acks, code);
            return;
        }
    };
    let mut st = cell.lock().await;
    st.set_dlq_enabled(settings.dlq_enabled);

    if has_acks
        && let Err(code) = apply_piggybacked_acks(context, pass.now, p, &cell, &mut st).await
    {
        fail_partition(p, true, code);
        return;
    }

    if !p.fetchable {
        persist_best_effort(mgr, group, p, &cell, &mut st).await;
        return;
    }

    // Expire stale locks, materialize freshly produced records, acquire.
    st.expire_locks(pass.now, settings.delivery_count_limit);
    let part = p.topic_name.as_deref().and_then(|name| {
        broker
            .partitions
            .get(name, krabka_ids::PartitionIndex(p.partition_index))
    });
    let Some(part) = part else {
        // Lost the partition between the leadership check and here.
        p.out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        p.leadable = false;
        persist_best_effort(mgr, group, p, &cell, &mut st).await;
        return;
    };
    let upper = read_upper_bound(&part, read_committed).await;
    // A released or expired record at the delivery limit is archived
    // first, so it cannot hold the window shut.
    st.archive_exhausted(settings.delivery_count_limit);
    let share = uniform_share(pass.byte_budget, pass.fetching, pass.fetch_index);
    pass.fetch_index += 1;
    let ReadBudget {
        max_bytes: read_max_bytes,
        min_one_batch,
        remaining_records,
    } = partition_read_budget(
        max_records,
        max_bytes,
        pass.total,
        p.partition_max_bytes,
        share,
    );
    let tier = tiered_source(broker, p, read_committed);
    let grow_and_acquire_args = || GrowAndAcquireArgs {
        part: &part,
        upper,
        settings: &settings,
        member,
        max_bytes: read_max_bytes,
        min_one_batch,
        remaining_records,
        mode,
        now: pass.now,
        tier: tier.clone(),
        diskless: broker
            .diskless_read
            .as_deref()
            .map(|reader| DisklessSource {
                reader,
                topic_id: p.topic_id,
            }),
    };
    // This pass's records land in `fresh`, then join what earlier passes
    // put in the row.
    let mut fresh = PartitionData::default();
    let outcome = acquire_with_log_start_recovery(
        broker,
        &part,
        p.topic_id,
        &mut st,
        &mut fresh,
        grow_and_acquire_args,
    )
    .await;
    let acquired_count = match outcome {
        Ok(count) => count,
        Err(err) => {
            // Kafka's `ShareFetchUtils.processFetchResponse` turns a read
            // error of one partition into that partition's error code with
            // no records, and the other partitions of the request still
            // return theirs. A record that this pass locked before the
            // failure stays locked until its lock times out.
            let code = read_failure_code(&err);
            tracing::warn!(
                group,
                topic_id = %p.topic_id,
                partition = p.partition_index,
                error = %err,
                code,
                "share-partition log read failed"
            );
            persist_best_effort(mgr, group, p, &cell, &mut st).await;
            fail_partition(p, false, code);
            return;
        }
    };

    p.out.error_code = codes::NONE;
    // The acquisition itself writes nothing, as in Kafka's
    // `SharePartition.acquire`. The pass may still have dirtied the state,
    // through an expired lock or an archived poison pill, and that write
    // happens here. A failed write keeps the state dirty for a retry. A
    // fenced write drops the cell, and the records acquired on it must not
    // reach the client.
    let persisted = mgr
        .persist_if_dirty(group, p.topic_id, p.partition_index, Some(&cell), &mut st)
        .await;
    drop(st);
    match persisted {
        Err(code) if fences_the_partition(code) => fail_partition(p, false, code),
        _ => match append_records(&mut p.out, fresh) {
            Ok(bytes) => {
                pass.total.records += acquired_count;
                pass.total.bytes += bytes;
            }
            Err(err) => fail_partition(p, false, read_failure_code(&err)),
        },
    }
}

/// Writes `st` back to the share coordinator if it is dirty, and ignores the
/// result.
///
/// Best-effort: a failed write keeps the state dirty for a retry, and a fenced
/// write drops `cell` from the cache without failing the row.
async fn persist_best_effort(
    mgr: &SharePartitionLeaderManager,
    group: &str,
    p: &PendingPartition,
    cell: &Arc<Mutex<AcquisitionState>>,
    st: &mut AcquisitionState,
) {
    let _ = mgr
        .persist_if_dirty(group, p.topic_id, p.partition_index, Some(cell), st)
        .await;
}

/// The error code of a partition whose log read failed, as Kafka's
/// `Errors.forException` maps what `ReplicaManager.readFromLog` catches: an
/// I/O fault is `KAFKA_STORAGE_ERROR`, a batch that does not decode or check
/// is `CORRUPT_MESSAGE`, and anything else is `UNKNOWN_SERVER_ERROR`.
fn read_failure_code(err: &BrokerError) -> i16 {
    match err {
        BrokerError::Log(
            LogError::CrcMismatch { .. }
            | LogError::PartialBatch { .. }
            | LogError::Records(_)
            | LogError::Corrupt(_),
        ) => codes::CORRUPT_MESSAGE,
        BrokerError::Log(LogError::Io(_)) => codes::KAFKA_STORAGE_ERROR,
        other => codes::from_broker_error(other),
    }
}

/// Adds the records and the acquired rows of one pass to a partition row, and
/// returns the record bytes it added.
///
/// A batch that an earlier pass already put in the row is not added again:
/// see [`merge_batches`].
fn append_records(out: &mut PartitionData, fresh: PartitionData) -> Result<i64, BrokerError> {
    out.acquired_records.extend(fresh.acquired_records);
    let Some(RecordsPayload::Raw(added)) = fresh.records else {
        return Ok(0);
    };
    let MergedBatches {
        records: joined,
        added_bytes: added_len,
    } = match out.records.take() {
        Some(RecordsPayload::Raw(before)) if !before.is_empty() => merge_batches(&before, &added)?,
        _ => {
            let added_len = i64::try_from(added.len()).unwrap_or(i64::MAX);
            MergedBatches {
                records: added,
                added_bytes: added_len,
            }
        }
    };
    out.records = Some(RecordsPayload::Raw(joined));
    Ok(added_len)
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
    use {
        super::*,
        crate::{
            handlers::{
                share_fetch::test_fetch_request,
                test_support::{
                    OffsetRun, StartedBroker, acquired_share_records, create_topic,
                    produce_records, share_fetch_partition_rows, share_fetch_wire,
                    share_topic_uuid, start_share_allow_all_no_audit,
                },
            },
            share_partition::state::AcquisitionState,
            test_support::initialize_share_state,
        },
        assert2::{assert, check},
        krabka_protocol::{
            owned::share_fetch_request::AcknowledgementBatch,
            records::{RecordBatch, RecordsError},
        },
        std::{
            io::Error,
            time::{Duration, Instant},
        },
    };

    #[yare::parameterized(
        one_below_bound = {
            SHARE_DELIVERY_MAX_DEFERRED_RECORDS - 1,
            SHARE_DELIVERY_MAX_DEFERRED_RECORDS + 9,
        },
        at_bound = {
            SHARE_DELIVERY_MAX_DEFERRED_RECORDS,
            SHARE_DELIVERY_MAX_DEFERRED_RECORDS,
        },
    )]
    fn materialization_stops_at_the_deferred_record_bound(deferred: i64, expected_end: i64) {
        let bound = SHARE_DELIVERY_MAX_DEFERRED_RECORDS;
        let inflight = i32::try_from(bound).expect("the bound fits an inflight budget");
        let mut state = AcquisitionState::new(Offset(0));
        state.materialize(Offset(deferred), inflight);
        state.defer_internal(Offset(0), Offset(deferred - 1));
        assert2::assert!(state.deferred_records() == deferred);

        materialize_within_deferral_bound(&mut state, Offset(deferred + 10), 10);

        assert2::assert!(
            state.end_offset == Offset(expected_end),
            "deferred={deferred}"
        );
    }

    /// A transactional batch at offsets 0-2 from producer 1000, its end marker
    /// at offset 3, and a plain record at offset 4.
    fn transaction_then_record(commit: bool) -> Vec<RecordBatch> {
        use {
            bytes::Bytes,
            krabka_protocol::records::{Attributes, Record, RecordBatch},
        };

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
        use {
            crate::delivery::test_support::{NOW_MS, partition_with_batches, wall_at},
            qubit_clock::{FixedWallClock, WallClock},
            std::sync::Arc,
        };

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
            let partition = partition_with_batches()
                .dir(&dir)
                .clock(&clock)
                .topic("txn")
                .batches(transaction_then_record(commit))
                .leader(0)
                .call();
            let mut state = AcquisitionState::new(Offset(0));

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
                    Instant::now(),
                    Duration::from_secs(30),
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

    /// Kafka's `uniformPartitionMaxBytes` with the random picks fixed to the
    /// first partitions.
    #[test]
    fn the_byte_budget_splits_evenly_across_the_partitions() {
        let shares = |budget, partitions| {
            (0..partitions)
                .map(|index| uniform_share(budget, partitions, index))
                .collect::<Vec<_>>()
        };
        check!(shares(1 << 20, 1) == vec![1 << 20]);
        check!(shares(10, 3) == vec![4, 3, 3]);
        check!(shares(2, 3) == vec![1, 1, 0]);
        check!(shares(0, 2) == vec![0, 0]);
    }

    #[derive(Default)]
    struct ShareReadFailureScenario {
        error: Option<BrokerError>,
        code: Option<i16>,
    }
    #[rstest::fixture]
    fn share_read_failure() -> ShareReadFailureScenario {
        let error = None;
        ShareReadFailureScenario {
            error,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given("reading share records fails with {failure}")]
    fn failed_share_read(share_read_failure: &mut ShareReadFailureScenario, failure: String) {
        share_read_failure.error = Some(match failure.as_str() {
            "a log disk I/O error" => BrokerError::Log(LogError::Io(Error::other("disk"))),
            "a corrupt transaction index" => {
                BrokerError::Log(LogError::Corrupt("txn index".into()))
            }
            "a record body shorter than one byte" => {
                BrokerError::Log(LogError::Records(RecordsError::BodyTooShort { needed: 1 }))
            }
            "a broker I/O error" => BrokerError::Io(Error::other("disk")),
            _ => panic!("unknown share read failure: {failure}"),
        });
    }
    #[rstest_bdd_macros::when("ShareFetch maps the failed read to a partition response error")]
    fn map_share_read_failure(share_read_failure: &mut ShareReadFailureScenario) {
        share_read_failure.code = Some(read_failure_code(
            share_read_failure.error.as_ref().unwrap(),
        ));
    }
    #[rstest_bdd_macros::then("the ShareFetch partition error is {code}")]
    fn check_share_read_failure(share_read_failure: &ShareReadFailureScenario, code: i16) {
        assert!(share_read_failure.code.unwrap() == code);
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_share_read_errors",fixtures=[share_read_failure:ShareReadFailureScenario]);

    /// What a client and the partition cache show after a renew-ack fetch
    /// whose ack-only row found its state write fenced.
    #[derive(Debug, PartialEq, Eq)]
    struct AckOnlyFencedPersist {
        /// The acknowledge error of the ack-only row.
        acknowledge_error: i16,
        /// The fetch error of the ack-only row.
        fetch_error: i16,
        /// Whether the broker still caches the share partition.
        cached: bool,
        /// The offsets that the next `ShareFetch` acquires.
        next_acquired: Vec<OffsetRun>,
    }

    /// An ack-only row (here, of a renew-ack fetch) writes a state that an
    /// earlier pass left dirty. That write is best-effort: a fenced write
    /// drops the cached cell, but the row keeps the acknowledge error that
    /// the acknowledgement itself produced.
    #[tokio::test]
    async fn a_fenced_write_on_an_ack_only_row_keeps_the_acknowledge_error() {
        let StartedBroker { broker, dir: _dir } = start_share_allow_all_no_audit().await;
        let shared = broker.broker_arc_for_test();
        let group = "ack-only-fenced";
        let topic_id = create_topic()
            .broker(&broker)
            .client_id("share-acquire-test")
            .name(group)
            .call()
            .await;
        let key = share_topic_uuid(topic_id);
        initialize_share_state(&broker, group, key, 0).await;
        let fetch = |epoch: i32, batches: Vec<AcknowledgementBatch>, renew: bool| {
            let request = test_fetch_request()
                .group_id(group.into())
                .member_id("member".into())
                .topic_id(topic_id.0)
                .share_session_epoch(epoch)
                .max_bytes(if renew { 0 } else { 1 << 20 })
                .max_records(if renew { 0 } else { 500 })
                .acknowledgement_batches(batches)
                .is_renew_ack(renew)
                .call();
            let broker = &broker;
            async move {
                let response = share_fetch_wire()
                    .broker(broker)
                    .request(&request)
                    .call()
                    .await;
                share_fetch_partition_rows(response)
                    .into_iter()
                    .next()
                    .unwrap_or_default()
            }
        };
        fetch(0, Vec::new(), false).await;
        produce_records()
            .broker(&broker)
            .topic(group)
            .count(2)
            .call()
            .await;
        let fetched = fetch(1, Vec::new(), false).await;
        assert!(
            acquired_share_records(&fetched) == vec![OffsetRun::new(0, 1)],
            "{fetched:?}"
        );

        // Leave the cached state dirty, then raise the durable state epoch so
        // the next write of that state is fenced.
        let cell = shared
            .share_partition_leaders
            .peek_for_test(group, key, 0)
            .expect("the fetch cached the share partition");
        let state_epoch = {
            let mut st = cell.lock().await;
            st.dirty = true;
            st.state_epoch
        };
        shared
            .share_coordinator
            .initialize(
                &shared.controller.current_image(),
                group,
                key,
                0,
                state_epoch + 1,
                Offset(0),
            )
            .await
            .expect("raise the state epoch");

        // Accepting offsets past the records handed out is refused with
        // `INVALID_REQUEST` and changes no state, so the acknowledgement itself
        // writes nothing and the row's own persist is
        // the write that the coordinator fences.
        let row = fetch(
            2,
            vec![AcknowledgementBatch {
                first_offset: 5,
                last_offset: 6,
                acknowledge_types: vec![1],
                ..Default::default()
            }],
            true,
        )
        .await;
        let cached = shared
            .share_partition_leaders
            .peek_for_test(group, key, 0)
            .is_some();
        let next = fetch(3, Vec::new(), false).await;

        assert!(
            AckOnlyFencedPersist {
                acknowledge_error: row.acknowledge_error_code,
                fetch_error: row.error_code,
                cached,
                next_acquired: acquired_share_records(&next),
            } == AckOnlyFencedPersist {
                acknowledge_error: codes::INVALID_REQUEST,
                fetch_error: codes::NONE,
                cached: false,
                next_acquired: vec![OffsetRun::new(0, 1)],
            }
        );
        broker.shutdown().await;
    }

    /// A request of 500 records and 1000 bytes.
    #[yare::parameterized(
        first_read_takes_its_share = {
            Spent::default(), 0, 400,
            ReadBudget { max_bytes: 400, min_one_batch: true, remaining_records: 500 },
        },
        no_share_acquires_nothing = {
            Spent::default(), 0, 0,
            ReadBudget { max_bytes: 0, min_one_batch: true, remaining_records: 0 },
        },
        partition_cap_is_held_to_the_response_left = {
            Spent { records: 100, bytes: 900 }, 2000, 50,
            ReadBudget { max_bytes: 100, min_one_batch: false, remaining_records: 400 },
        },
        overspent_response_reads_no_bytes = {
            Spent { records: 300, bytes: 1200 }, 0, 10,
            ReadBudget { max_bytes: 0, min_one_batch: false, remaining_records: 200 },
        },
    )]
    fn partition_read_budget_caps_records_and_bytes(
        total: Spent,
        partition_max_bytes: i32,
        share: i32,
        expected: ReadBudget,
    ) {
        assert2::assert!(
            partition_read_budget(500, 1000, total, partition_max_bytes, share) == expected
        );
    }

    #[test]
    fn remaining_record_budget_is_request_wide_and_saturating() {
        check!(remaining_record_budget(500, 300) == 200);
        check!(remaining_record_budget(500, 500) == 0);
        check!(remaining_record_budget(500, i64::MAX) == 0);
    }
}
