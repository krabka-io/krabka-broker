//! The log reads behind a `ShareFetch` response, and the assembly of their
//! bytes into one partition row.
//!
//! An acquire pass hands this module the offset ranges it locked, and gets
//! back the verbatim on-disk batch bytes plus the `acquired_records` rows that
//! describe them. The same log-scan shape answers the two questions the pass
//! asks before it acquires: which offsets hold control batches, and which
//! offsets KFC-1 scheduled delivery has not released yet.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use krabka_log::Offset;
use krabka_protocol::{owned::share_fetch_response::AcquiredRecords, records::RecordsPayload};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use super::pending::PendingPartition;
use crate::error::BrokerError;

pub(super) async fn populate_acquired_response(
    pending: &mut PendingPartition,
    partition: &Arc<crate::partition::Partition>,
    acquired: &[crate::share_partition::state::AcquiredRange],
    upper: Offset,
    request_max_bytes: i32,
) -> Result<i64, BrokerError> {
    // The per-partition cap is absent at supported protocol versions and
    // decodes to zero, so fall back to the request-wide byte budget.
    let read_budget = if pending.partition_max_bytes > 0 {
        pending.partition_max_bytes
    } else {
        request_max_bytes
    };
    let mut blob = BytesMut::new();
    for range in acquired {
        let limit = (range.last + 1).min(upper);
        if let Some(bytes) = read_acquired_bytes(partition, range.first, limit, read_budget).await?
        {
            blob.extend_from_slice(&bytes);
        }
    }
    if !blob.is_empty() {
        pending.out.records = Some(RecordsPayload::Raw(blob.freeze()));
    }
    pending.out.acquired_records = acquired
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

/// Reads the verbatim on-disk batch bytes for `[fetch_offset, limit_offset)`
/// through `Log::read_raw`, off the reactor thread. It returns `None` when it
/// read nothing.
async fn read_acquired_bytes(
    part: &crate::partition::Partition,
    fetch_offset: Offset,
    limit_offset: Offset,
    max_bytes: i32,
) -> Result<Option<Bytes>, BrokerError> {
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
    if raw.total > 0 {
        Ok(Some(raw.bytes))
    } else {
        Ok(None)
    }
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
    use qubit_clock::{Clock, DateTime, MockTime};

    use super::*;
    use crate::delivery::test_support::{NOW_MS, scheduled_partition};

    #[tokio::test]
    async fn only_the_batches_that_are_not_due_are_reported_as_pending() {
        // Three two-record batches: due, not due, due. The middle one is the
        // case a classic Fetch cannot serve around.
        let activations = [NOW_MS - 60_000, NOW_MS + 60_000, NOW_MS - 60_000];
        let clock: Arc<dyn Clock> = Arc::new(
            MockTime::at(DateTime::from_timestamp_millis(NOW_MS).expect("a representable instant"))
                .clock(),
        );

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
