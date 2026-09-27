//! The log reads behind a `ShareFetch` response, and the assembly of their
//! bytes into one partition row.
//!
//! An acquire pass hands this module a partition's acquisition state. The
//! module reads the log first, locks only the offsets inside the bytes that
//! the read returned, and gives back those batch bytes plus the
//! `acquired_records` rows that describe them. The same log-scan shape
//! answers the two questions the pass asks before it acquires: which offsets
//! hold control batches or aborted transactional data, and which offsets
//! KFC-1 scheduled delivery has not released yet.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use krabka_log::{Offset, RawRead};
use krabka_protocol::{
    owned::share_fetch_response::{AcquiredRecords, PartitionData},
    records::{HEADER_LEN, RecordBatchHeader, RecordsPayload},
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use zerocopy::FromBytes as _;

use crate::{
    error::BrokerError,
    share_partition::state::{AcquireShape, AcquiredRange, AcquisitionState},
};

/// What one acquire step may take from a share partition.
pub(super) struct AcquireRequest<'a> {
    pub(super) member: &'a str,
    /// The records that the request can still take, across its partitions.
    pub(super) max_records: i32,
    /// The byte budget of the log read.
    pub(super) max_bytes: i32,
    /// Whether the read may exceed `max_bytes` by the one batch it starts
    /// with: Kafka's `minOneMessage`, which only the first partition of a
    /// response that returns records gets.
    pub(super) min_one_batch: bool,
    /// The exclusive end of the readable window: the high watermark, or the
    /// last stable offset under `read_committed`.
    pub(super) upper: Offset,
    pub(super) now: Instant,
    pub(super) lock_duration: Duration,
    pub(super) max_attempts: i16,
    /// How the request shapes what it acquires.
    pub(super) mode: AcquireMode,
}

/// Kafka's `ShareAcquireMode` with the request's `BatchSize`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcquireMode {
    /// `batch_optimized` (0, and the only mode before v2): `MaxRecords` is a
    /// soft limit that rounds up to a log batch end, and new records come
    /// back in rows of about `batch_size` records on log batch boundaries.
    BatchOptimized { batch_size: i32 },
    /// `record_limit` (1): at most `MaxRecords` records, in one row per run.
    RecordLimit,
}

impl AcquireMode {
    /// The mode of a request: its `ShareAcquireMode` and `BatchSize`.
    pub(crate) fn of(share_acquire_mode: i8, batch_size: i32) -> Self {
        if share_acquire_mode == 1 {
            Self::RecordLimit
        } else {
            Self::BatchOptimized { batch_size }
        }
    }
}

/// The byte budget of one partition's log read.
///
/// The per-partition cap is absent at supported protocol versions and decodes
/// to zero, so the read falls back to the request-wide byte budget.
pub(super) fn read_budget(partition_max_bytes: i32, request_max_bytes: i32) -> i32 {
    if partition_max_bytes > 0 {
        partition_max_bytes
    } else {
        request_max_bytes
    }
}

/// Reads the log, then acquires records only inside the bytes that the read
/// returned, and fills `out` with those bytes and the acquired rows.
///
/// Kafka reads first and acquires second. `SharePartition.acquire` bounds the
/// acquisition by the last batch of the fetched records, so every offset in
/// `acquired_records` has its record in `records`. The Java share consumer
/// treats an acquired offset with no record as a gap and acknowledges it as
/// `Gap`, which archives it. An acquisition that ran past the byte budget
/// would therefore lose the records that the read cut off.
///
/// The read starts at the first offset that the state can hand out. The
/// response carries each read batch that holds an acquired offset, and no
/// other batch. It returns the number of offsets that it acquired.
pub(super) async fn acquire_read_records(
    out: &mut PartitionData,
    partition: &Arc<crate::partition::Partition>,
    state: &mut AcquisitionState,
    request: &AcquireRequest<'_>,
) -> Result<i64, BrokerError> {
    let Some(from) = state.first_acquirable_offset(request.max_attempts) else {
        return Ok(0);
    };
    let Some(read) = read_raw(partition, from, request.upper, request.max_bytes).await? else {
        return Ok(0);
    };
    let read_bytes = if request.min_one_batch {
        read.bytes
    } else {
        batches_within(&read.bytes, request.max_bytes)?
    };
    let bounds = batch_bounds(&read_bytes)?;
    let Some(&(_, read_last)) = bounds.last() else {
        return Ok(0);
    };
    let last = Offset(read_last).min(request.upper - 1);
    let ends: Vec<Offset> = bounds.iter().map(|(_, last)| Offset(*last)).collect();
    let acquired = state.acquire_shaped(
        request.member,
        AcquireShape {
            max_records: request.max_records,
            batch_ends: match request.mode {
                AcquireMode::BatchOptimized { .. } => Some(&ends),
                AcquireMode::RecordLimit => None,
            },
        },
        last,
        (request.now, request.lock_duration),
        request.max_attempts,
    );
    if acquired.is_empty() {
        return Ok(0);
    }
    let records = batches_holding(&read_bytes, &acquired)?;
    if !records.is_empty() {
        out.records = Some(RecordsPayload::Raw(records));
    }
    let bases: Vec<i64> = bounds.iter().map(|(base, _)| *base).collect();
    out.acquired_records = acquired
        .iter()
        .flat_map(|range| rows_of(range, request.mode, &bases))
        .collect();
    Ok(acquired
        .iter()
        .map(|range| range.last.0 - range.first.0 + 1)
        .sum())
}

/// The `AcquiredRecords` rows of one acquired run.
///
/// Kafka's `SharePartition.createBatches`: in `batch_optimized` mode a run
/// of new records (delivery count 1) longer than `batch_size` is split into
/// rows on log batch boundaries, a new row starting at the first batch base
/// at least `batch_size` records past the start of the current one. Every
/// other run is one row.
fn rows_of(range: &AcquiredRange, mode: AcquireMode, bases: &[i64]) -> Vec<AcquiredRecords> {
    let row = |first_offset, last_offset| AcquiredRecords {
        first_offset,
        last_offset,
        delivery_count: range.delivery_count,
        ..Default::default()
    };
    let (first, last) = (range.first.0, range.last.0);
    let AcquireMode::BatchOptimized { batch_size } = mode else {
        return vec![row(first, last)];
    };
    if range.delivery_count != 1 || batch_size <= 0 || last - first < i64::from(batch_size) {
        return vec![row(first, last)];
    }
    let mut rows = Vec::new();
    let mut current = first;
    for base in bases
        .iter()
        .copied()
        .filter(|base| *base > first && *base <= last)
    {
        if base - current >= i64::from(batch_size) {
            rows.push(row(current, base - 1));
            current = base;
        }
    }
    rows.push(row(current, last));
    rows
}

/// One v2 batch of a read: where its bytes sit, and the offsets it holds.
struct BatchSpan {
    bytes: std::ops::Range<usize>,
    base: i64,
    last: i64,
}

/// Every v2 batch in `bytes`, in the order that the bytes hold them. It reads
/// only the batch headers.
fn batch_spans(bytes: &Bytes) -> Result<Vec<BatchSpan>, BrokerError> {
    let mut spans = Vec::new();
    let mut at = 0_usize;
    while at < bytes.len() {
        let header = bytes
            .get(at..at + HEADER_LEN)
            .and_then(|raw| RecordBatchHeader::ref_from_bytes(raw).ok())
            .ok_or_else(|| corrupt_read("a truncated record batch header"))?;
        let length = usize::try_from(header.batch_length.get())
            .ok()
            .map(|length| length + LOG_OVERHEAD)
            .filter(|length| *length >= HEADER_LEN && at + length <= bytes.len())
            .ok_or_else(|| corrupt_read("a record batch length outside the read"))?;
        let base = header.base_offset.get();
        spans.push(BatchSpan {
            bytes: at..at + length,
            base,
            last: base + i64::from(header.last_offset_delta.get()),
        });
        at += length;
    }
    Ok(spans)
}

/// The `(base_offset, last_offset)` of every v2 batch in `bytes`, in log
/// order. It reads only the batch headers.
fn batch_bounds(bytes: &Bytes) -> Result<Vec<(i64, i64)>, BrokerError> {
    Ok(batch_spans(bytes)?
        .into_iter()
        .map(|span| (span.base, span.last))
        .collect())
}

/// The byte ranges of `bytes`, joined into one buffer. It copies nothing
/// when one range covers them.
fn gather(bytes: &Bytes, ranges: &[std::ops::Range<usize>]) -> Bytes {
    // Adjacent ranges read as one run.
    let mut runs: Vec<std::ops::Range<usize>> = Vec::new();
    for range in ranges {
        match runs.last_mut() {
            Some(run) if run.end == range.start => run.end = range.end,
            _ => runs.push(range.clone()),
        }
    }
    match runs.as_slice() {
        [] => Bytes::new(),
        [run] => bytes.slice(run.clone()),
        runs => {
            let mut blob = BytesMut::with_capacity(runs.iter().map(ExactSizeIterator::len).sum());
            for run in runs {
                blob.extend_from_slice(&bytes[run.clone()]);
            }
            blob.freeze()
        }
    }
}

/// Returns the batches of `bytes` that hold at least one offset of
/// `acquired`, in log order.
///
/// It walks the v2 batch headers and decodes no record. When every batch
/// qualifies, it returns `bytes` without a copy.
fn batches_holding(bytes: &Bytes, acquired: &[AcquiredRange]) -> Result<Bytes, BrokerError> {
    let kept: Vec<_> = batch_spans(bytes)?
        .into_iter()
        .filter(|span| {
            acquired
                .iter()
                .any(|range| range.first.0 <= span.last && span.base <= range.last.0)
        })
        .map(|span| span.bytes)
        .collect();
    Ok(gather(bytes, &kept))
}

/// The leading whole batches of `bytes` that fit in `max_bytes`.
///
/// This is Kafka's `ReplicaManager.readFromLog` with `minOneMessage` off:
/// only the first partition of a response that returns records may exceed
/// its byte budget, by the one batch that its read starts with. A later
/// partition whose first batch does not fit returns no records.
/// `Log::read_raw` always returns at least one whole batch, so the caller
/// applies the budget here.
fn batches_within(bytes: &Bytes, max_bytes: i32) -> Result<Bytes, BrokerError> {
    let budget = usize::try_from(max_bytes.max(0)).unwrap_or(usize::MAX);
    let kept: Vec<_> = batch_spans(bytes)?
        .into_iter()
        .map(|span| span.bytes)
        .take_while(|range| range.end <= budget)
        .collect();
    Ok(gather(bytes, &kept))
}

/// The bytes of a v2 batch in front of its `batch_length` field: the base
/// offset (8) and the length itself (4).
const LOG_OVERHEAD: usize = 12;

fn corrupt_read(what: &str) -> BrokerError {
    BrokerError::Io(std::io::Error::other(format!(
        "share-fetch read returned {what}"
    )))
}

/// Reads the verbatim on-disk batch bytes for `[fetch_offset, limit_offset)`
/// through `Log::read_raw`, off the reactor thread. It returns `None` when it
/// read nothing.
async fn read_raw(
    part: &crate::partition::Partition,
    fetch_offset: Offset,
    limit_offset: Offset,
    max_bytes: i32,
) -> Result<Option<RawRead>, BrokerError> {
    if limit_offset <= fetch_offset {
        return Ok(None);
    }
    let read_max = ByteSize::from_bytes_i64(i64::from(max_bytes.max(0)));
    let log = part.log.clone();
    let join = tokio::task::spawn_blocking(move || {
        let log = log.lock().expect("log mutex poisoned");
        log.read_raw(fetch_offset, limit_offset, read_max)
    });
    let raw = match join.await {
        Ok(res) => res?,
        Err(join_err) => {
            return Err(BrokerError::Io(std::io::Error::other(format!(
                "share-fetch read task panicked: {join_err}"
            ))));
        }
    };
    Ok((raw.total > 0).then_some(raw))
}

/// The byte budget, in bytes, of one log read while
/// [`unreadable_batch_ranges`] walks a window.
const UNREADABLE_SCAN_CHUNK_BYTES: u64 = 1 << 20;

/// Returns the offset ranges in `[start, end)` that a share consumer must
/// never get: every control batch, and under `read_committed` every data
/// batch of an aborted transaction.
///
/// Share acquisition state is offset-based and therefore materializes log
/// control markers along with data unless the handler explicitly archives
/// them. The decoded log read keeps this classification out of the raw-byte
/// response path.
///
/// A `ShareFetch` response has no aborted-transactions field, so the share
/// consumer cannot drop an aborted batch itself. Kafka's
/// `SharePartition.filterAbortedTransactionalAcquiredRecords` archives those
/// batches for a `read_committed` share group. A batch is aborted when it is
/// transactional and its producer has an aborted transaction whose range, up
/// to the abort marker, holds the batch.
pub(super) async fn unreadable_batch_ranges(
    part: &crate::partition::Partition,
    start: Offset,
    end: Offset,
    read_committed: bool,
) -> Result<Vec<(Offset, Offset)>, BrokerError> {
    if end <= start {
        return Ok(Vec::new());
    }
    let log = part.log.clone();
    let join = tokio::task::spawn_blocking(move || {
        let log = log.lock().expect("log mutex poisoned");
        // Aborted transactions by producer, as `(first offset, abort marker)`.
        let mut aborted: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
        if read_committed {
            for txn in log.aborted_in_range(start, end) {
                aborted
                    .entry(txn.producer_id.get())
                    .or_default()
                    .push((txn.start_offset.0, txn.last_offset.0));
            }
        }
        let is_aborted = |base: i64, last: i64, producer_id: i64| {
            aborted.get(&producer_id).is_some_and(|txns| {
                txns.iter()
                    .any(|&(first, marker)| first <= base && last <= marker)
            })
        };
        // Read the window in bounded chunks, and stop at `end`, so a window
        // far behind the log end does not decode the rest of the log.
        let mut ranges = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let read = log.read(cursor, ByteSize::from_bytes(UNREADABLE_SCAN_CHUNK_BYTES))?;
            let Some(last_batch) = read.batches.last() else {
                break;
            };
            let next = Offset(last_batch.base_offset + i64::from(last_batch.last_offset_delta) + 1);
            for batch in &read.batches {
                let last = batch.base_offset + i64::from(batch.last_offset_delta);
                if batch.base_offset >= end.0 {
                    break;
                }
                let unreadable = batch.attributes.is_control_batch()
                    || (batch.attributes.is_transactional()
                        && is_aborted(batch.base_offset, last, batch.producer_id));
                let first = Offset(batch.base_offset).max(start);
                let last = Offset(last).min(end - 1);
                if unreadable && first <= last {
                    ranges.push((first, last));
                }
            }
            if next <= cursor {
                break;
            }
            cursor = next;
        }
        Ok::<_, krabka_log::LogError>(ranges)
    });
    match join.await {
        Ok(result) => result.map_err(BrokerError::from),
        Err(join_err) => Err(BrokerError::Io(std::io::Error::other(format!(
            "share-fetch control scan panicked: {join_err}"
        )))),
    }
}

/// Returns the offset ranges in `[start, end)` that KFC-1 scheduled delivery
/// has not released yet, as of `now_ms`.
///
/// A share group is the one reader that may take a due record from behind a
/// waiting one, so it needs every gap in the window and not the leading active
/// prefix that caps a classic `Fetch`. The ranges come back batch-aligned and
/// coalesced, which suits an acquisition state that is offset-based and a read
/// path that is batch-granular.
///
/// `now_ms` is the partition's own delivery clock, so an append, the delivery
/// scheduler, and this pass all decide against one timeline. A topic that
/// delivers immediately answers with nothing before it reads a batch header,
/// so the ordinary case costs one call and no I/O.
pub(super) async fn pending_activation_ranges(
    part: &crate::partition::Partition,
    start: Offset,
    end: Offset,
    now_ms: i64,
) -> Result<Vec<(Offset, Offset)>, BrokerError> {
    if end <= start {
        return Ok(Vec::new());
    }
    let log = part.log.clone();
    let join = tokio::task::spawn_blocking(move || {
        let log = log.lock().expect("log mutex poisoned");
        log.pending_activation_ranges(start, end - 1, now_ms)
    });
    join.await.map_err(|join_err| {
        BrokerError::Io(std::io::Error::other(format!(
            "share-fetch activation scan panicked: {join_err}"
        )))
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::DeliveryPolicy;
    use qubit_clock::{FixedWallClock, WallClock};

    use super::*;
    use crate::delivery::test_support::{NOW_MS, scheduled_partition, wall_at};

    #[tokio::test]
    async fn only_the_batches_that_are_not_due_are_reported_as_pending() {
        // Three two-record batches: due, not due, due. The middle one is the
        // case a classic Fetch cannot serve around.
        let activations = [NOW_MS - 60_000, NOW_MS + 60_000, NOW_MS - 60_000];
        let clock: Arc<dyn WallClock> = Arc::new(FixedWallClock::new(wall_at(NOW_MS)));

        let dir = tempfile::tempdir().expect("a log root");
        let scheduled = scheduled_partition(
            &dir,
            "scheduled",
            DeliveryPolicy::Scheduled,
            &activations,
            0,
            &clock,
        );
        let ranges = pending_activation_ranges(&scheduled, Offset(0), Offset(6), NOW_MS)
            .await
            .expect("scan the schedule");
        assert!(ranges == vec![(Offset(2), Offset(3))]);

        // The window bound is exclusive, so a window that stops below the
        // waiting batch reports nothing.
        let clipped = pending_activation_ranges(&scheduled, Offset(0), Offset(2), NOW_MS)
            .await
            .expect("scan the schedule");
        assert!(clipped == Vec::new());

        // An immediate topic answers with nothing whatever its timestamps say.
        let immediate_dir = tempfile::tempdir().expect("a log root");
        let immediate = scheduled_partition(
            &immediate_dir,
            "immediate",
            DeliveryPolicy::Immediate,
            &activations,
            0,
            &clock,
        );
        let none = pending_activation_ranges(&immediate, Offset(0), Offset(6), NOW_MS)
            .await
            .expect("scan the schedule");
        assert!(none == Vec::new());
    }

    /// Kafka's `createBatches` over log batches of five records at 0, 5, 10
    /// and 15.
    #[test]
    fn new_records_split_into_rows_on_batch_boundaries() {
        let bases = [0, 5, 10, 15];
        let range = |first, last, delivery_count| AcquiredRange {
            first: Offset(first),
            last: Offset(last),
            delivery_count,
        };
        let rows = [
            (
                range(0, 19, 1),
                AcquireMode::BatchOptimized { batch_size: 10 },
                vec![(0, 9), (10, 19)],
            ),
            (
                range(0, 19, 1),
                AcquireMode::BatchOptimized { batch_size: 7 },
                vec![(0, 9), (10, 19)],
            ),
            (
                range(0, 19, 1),
                AcquireMode::BatchOptimized { batch_size: 20 },
                vec![(0, 19)],
            ),
            (range(0, 19, 1), AcquireMode::RecordLimit, vec![(0, 19)]),
            // A redelivery keeps its one row.
            (
                range(0, 19, 2),
                AcquireMode::BatchOptimized { batch_size: 5 },
                vec![(0, 19)],
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (range, mode, want) in rows {
            let got: Vec<_> = rows_of(&range, mode, &bases)
                .into_iter()
                .map(|row| (row.first_offset, row.last_offset))
                .collect();
            actual.push((mode, range.delivery_count, got));
            expected.push((mode, range.delivery_count, want));
        }
        assert2::assert!(actual == expected);
    }
}
