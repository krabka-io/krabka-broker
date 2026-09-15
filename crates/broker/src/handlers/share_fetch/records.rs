//! The log reads behind a `ShareFetch` response, and the assembly of their
//! bytes into one partition row.
//!
//! An acquire pass hands this module a partition's acquisition state. The
//! module reads the log first, locks only the offsets inside the bytes that
//! the read returned, and gives back those batch bytes plus the
//! `acquired_records` rows that describe them. The same log-scan shape answers the two questions the pass
//! asks before it acquires: which offsets hold control batches, and which
//! offsets KFC-1 scheduled delivery has not released yet.

use std::{
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
    share_partition::state::{AcquiredRange, AcquisitionState},
};

/// What one acquire step may take from a share partition.
pub(super) struct AcquireRequest<'a> {
    pub(super) member: &'a str,
    /// The records that the request can still take, across its partitions.
    pub(super) max_records: i32,
    /// The byte budget of the log read.
    pub(super) max_bytes: i32,
    /// The exclusive end of the readable window: the high watermark, or the
    /// last stable offset under `read_committed`.
    pub(super) upper: Offset,
    pub(super) now: Instant,
    pub(super) lock_duration: Duration,
    pub(super) max_attempts: i16,
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
    let Some(read_last) = read.last_offset else {
        return Ok(0);
    };
    let last = read_last.min(request.upper - 1);
    let acquired = state.acquire(
        request.member,
        request.max_records,
        last,
        request.now,
        request.lock_duration,
        request.max_attempts,
    );
    if acquired.is_empty() {
        return Ok(0);
    }
    let records = batches_holding(&read.bytes, &acquired)?;
    if !records.is_empty() {
        out.records = Some(RecordsPayload::Raw(records));
    }
    out.acquired_records = acquired
        .iter()
        .map(|range| AcquiredRecords {
            first_offset: range.first.0,
            last_offset: range.last.0,
            delivery_count: range.delivery_count,
            ..Default::default()
        })
        .collect();
    Ok(acquired
        .iter()
        .map(|range| range.last.0 - range.first.0 + 1)
        .sum())
}

/// Returns the batches of `bytes` that hold at least one offset of
/// `acquired`, in log order.
///
/// It walks the v2 batch headers and decodes no record. When every batch
/// qualifies, it returns `bytes` without a copy.
fn batches_holding(bytes: &Bytes, acquired: &[AcquiredRange]) -> Result<Bytes, BrokerError> {
    let mut kept: Vec<std::ops::Range<usize>> = Vec::new();
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
        let last = base + i64::from(header.last_offset_delta.get());
        if acquired
            .iter()
            .any(|range| range.first.0 <= last && base <= range.last.0)
        {
            match kept.last_mut() {
                Some(run) if run.end == at => run.end = at + length,
                _ => kept.push(at..at + length),
            }
        }
        at += length;
    }
    Ok(match kept.as_slice() {
        [] => Bytes::new(),
        [run] => bytes.slice(run.clone()),
        runs => {
            let mut blob = BytesMut::with_capacity(runs.iter().map(ExactSizeIterator::len).sum());
            for run in runs {
                blob.extend_from_slice(&bytes[run.clone()]);
            }
            blob.freeze()
        }
    })
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

/// Returns the control-batch offset ranges in `[start, end)`.
///
/// Share acquisition state is offset-based and therefore materializes log
/// control markers along with data unless the handler explicitly archives
/// them. The decoded log read keeps this classification out of the raw-byte
/// response path.
pub(super) async fn control_batch_ranges(
    part: &crate::partition::Partition,
    start: Offset,
    end: Offset,
) -> Result<Vec<(Offset, Offset)>, BrokerError> {
    if end <= start {
        return Ok(Vec::new());
    }
    let log = part.log.clone();
    let join = tokio::task::spawn_blocking(move || {
        let log = log.lock().expect("log mutex poisoned");
        let read = log.read(start, ByteSize::from_bytes(u64::MAX))?;
        Ok::<_, krabka_log::LogError>(
            read.batches
                .into_iter()
                .filter(|batch| batch.attributes.is_control_batch())
                .filter_map(|batch| {
                    let first = Offset(batch.base_offset).max(start);
                    let last =
                        Offset(batch.base_offset + i64::from(batch.last_offset_delta)).min(end - 1);
                    (first <= last).then_some((first, last))
                })
                .collect(),
        )
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
}
