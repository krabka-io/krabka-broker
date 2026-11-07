//! One catch-up pass that copies the batches a future log is missing from the
//! partition's current log.
//!
//! The pass is synchronous and holds each log mutex only for a short time, so
//! the replicator task can repeat it between cancellation checks. It meters
//! the copy with the broker-wide KIP-113 log-directory move quota, as Kafka's
//! `LocalLeaderEndPoint.buildFetch` and
//! `ReplicaAlterLogDirsThread.processPartitionData` do. While the measured
//! rate is above the bound, the pass reads nothing. Otherwise it reads a whole
//! `read_chunk`, appends it, and records the bytes it read. The quota is asked
//! first, so a caught-up future log is not promoted while it is exceeded. The
//! pass also re-bases the future log
//! when retention advances the source start.

use {
    crate::{error::BrokerError, partition::Partition, throttle::ReplicationQuota},
    krabka_log::{Log, Offset},
    krabka_protocol::records::RecordBatch,
    krabka_units::ByteSize,
    std::sync::{Arc, Mutex, PoisonError},
};

/// What one catch-up iteration did. `caught_up` is `true` if the future log
/// was caught up at the end of the iteration, that is, if it read nothing AND
/// `future.LEO >= source.LEO`. `throttled` is `true` if the move quota was
/// exceeded, so the iteration read nothing.
pub(super) struct CatchUpProgress {
    pub(super) caught_up: bool,
    pub(super) throttled: bool,
}

/// One catch-up iteration. It reads whatever the future log is missing, up to
/// `read_chunk`, appends it, and records the bytes it read against `throttle`.
pub(super) fn catch_up(
    part: &Arc<Partition>,
    future_log: &Arc<Mutex<Log>>,
    read_chunk: ByteSize,
    throttle: &ReplicationQuota,
) -> Result<CatchUpProgress, BrokerError> {
    // Kafka builds no fetch at all this round while the quota is exceeded
    // (`LocalLeaderEndPoint.buildFetch`), so not even a caught-up future log
    // is seen to be caught up, and its promotion waits for the quota.
    if throttle.quota_exceeded() {
        return Ok(CatchUpProgress {
            caught_up: false,
            throttled: true,
        });
    }
    // Snapshot both source bounds under one lock so a retention update cannot
    // produce an internally inconsistent start/end pair.
    // The move copies local files, so the floor that bounds it is the local
    // one (KIP-405): on a tiered partition the offsets below it are in the
    // remote tier, and `Log::read` refuses them. Reading from the global floor
    // instead would fail every pass and the move would never finish.
    let (current_start, current_leo) = {
        let current = part.log.lock().unwrap_or_else(PoisonError::into_inner);
        (current.local_log_start_offset(), current.log_end_offset())
    };
    // Recover the guard if a panic elsewhere poisoned the mutex rather
    // than killing this (discarded-JoinHandle) replicator task.
    let future_leo = future_log
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .log_end_offset();
    if future_leo >= current_leo && future_leo >= current_start {
        return Ok(CatchUpProgress {
            caught_up: true,
            throttled: false,
        });
    }

    // Retention may advance the source start beyond the future log. Read from
    // the new logical start; the returned first batch can begin below it when
    // the start falls inside a batch, so reset to that physical base and restore
    // the logical start after appending.
    let reset_for_retention = future_leo < current_start;
    let fetch_offset = if reset_for_retention {
        current_start
    } else {
        future_leo
    };
    let read = {
        let log = part.log.lock().unwrap_or_else(PoisonError::into_inner);
        log.read(fetch_offset, read_chunk)?
    };
    if read.batches.is_empty() {
        if reset_for_retention {
            future_log
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .reset_to(current_start)?;
        }
        return Ok(CatchUpProgress {
            caught_up: true,
            throttled: false,
        });
    }

    // `ReplicaAlterLogDirsThread.processPartitionData` records
    // `records.sizeInBytes`, the bytes read, whatever the bound. `Log::read`
    // measures what it returns the same way.
    let read_bytes: usize = read
        .batches
        .iter()
        .map(RecordBatch::encoded_batch_len)
        .sum();
    let mut future = future_log.lock().unwrap_or_else(PoisonError::into_inner);
    if reset_for_retention {
        future.reset_to(read.start_offset)?;
    }
    for mut batch in read.batches {
        let base = batch.base_offset;
        future
            .append_at(&mut batch, Offset(base))
            .map_err(BrokerError::from)?;
    }
    if reset_for_retention {
        future.set_log_start_offset(current_start)?;
    }
    drop(future);
    throttle.record(u64::try_from(read_bytes).unwrap_or(u64::MAX));
    Ok(CatchUpProgress {
        caught_up: false,
        throttled: false,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            future_log::test_support::{
                OpenedFutureLog, append_records, fixture_partition, open_future_log, source_fixture,
            },
            test_support::PartitionRecordsSetup,
            throttle::ReplicationQuota,
        },
        assert2::assert,
        krabka_ids::PartitionIndex,
        krabka_units::{ByteRate, bytes_per_sec, convert::ByteRateExt as _, mebibytes},
        qubit_clock::ManualMonotonicClock,
        std::{path::Path, sync::PoisonError},
        tempfile::tempdir,
    };

    fn first_catch_up(part: &Arc<Partition>, target: &Path) -> FirstCatchUp {
        let OpenedFutureLog { log: future, .. } = open_future_log(target);
        let progress =
            catch_up(part, &future, mebibytes(1), &ReplicationQuota::new()).expect("catch up");
        FirstCatchUp { future, progress }
    }

    /// The future log after a first catch-up, and the progress it reported.
    struct FirstCatchUp {
        future: Arc<Mutex<Log>>,
        progress: CatchUpProgress,
    }

    /// A source partition of two one-record batches, at offsets 0 and 1.
    fn two_batch_fixture() -> (Arc<Partition>, tempfile::TempDir, tempfile::TempDir) {
        let fixture = source_fixture().count(1).call();
        append_records(&fixture.partition, PartitionRecordsSetup {
            count: 1.into(),
            ..Default::default()
        });
        (fixture.partition, fixture.primary, fixture.target)
    }

    /// A move quota on a clock that does not advance, so its measured rate is
    /// the recorded bytes over Kafka's one-window minimum.
    fn frozen_quota() -> ReplicationQuota {
        ReplicationQuota::with_clock(ManualMonotonicClock::new_shared())
    }

    fn future_leo(future: &Arc<Mutex<Log>>) -> Offset {
        future
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .log_end_offset()
    }

    fn progress_flags(progress: &CatchUpProgress) -> (bool, bool) {
        (progress.caught_up, progress.throttled)
    }

    #[tokio::test]
    async fn catch_up_resets_after_source_retention_without_dropping_batch_data() {
        let fixture = source_fixture().call();
        let _primary = fixture.primary;
        let target = fixture.target;
        let part = fixture.partition;
        part.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_log_start_offset(Offset(2))
            .expect("advance source start");
        let FirstCatchUp { future, progress } = first_catch_up(&part, target.path());
        assert!(!progress.caught_up);
        let future = future.lock().unwrap_or_else(PoisonError::into_inner);
        let (start, end) = (future.log_start_offset(), future.log_end_offset());
        let read = future.read(Offset(2), mebibytes(1)).expect("read future");
        drop(future);
        assert!(start == Offset(2));
        assert!(end == Offset(3));
        assert!(read.batches.len() == 1);
        assert!(read.batches[0].base_offset == 0);
    }

    #[tokio::test]
    async fn catch_up_resets_to_empty_source_frontier() {
        let primary = tempdir().unwrap();
        let target = tempdir().unwrap();
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        part.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reset_to(Offset(5))
            .expect("reset source");
        let FirstCatchUp { future, progress } = first_catch_up(&part, target.path());
        assert!(progress.caught_up);
        let (start, end) = {
            let future = future.lock().unwrap_or_else(PoisonError::into_inner);
            (future.log_start_offset(), future.log_end_offset())
        };
        assert!(start == Offset(5));
        assert!(end == Offset(5));
    }

    #[tokio::test]
    async fn catch_up_waits_when_move_throttle_is_exhausted() {
        let fixture = source_fixture().count(1).call();
        let _primary = fixture.primary;
        let target = fixture.target;
        let part = fixture.partition;
        let OpenedFutureLog { log: future, .. } = open_future_log(target.path());
        let throttle = frozen_quota();
        throttle.set_bound(Some(bytes_per_sec(1024)));
        throttle.record(10 * 1024);

        let progress = catch_up(&part, &future, mebibytes(1), &throttle).expect("catch up");

        assert!(
            (
                progress_flags(&progress),
                future_leo(&future),
                throttle.measured_rate()
            ) == ((false, true), Offset(0), bytes_per_sec(10 * 1024))
        );
    }

    #[tokio::test]
    async fn catch_up_within_quota_copies_whole_chunk_and_records_it() {
        let (part, _primary, target) = two_batch_fixture();
        let OpenedFutureLog { log: future, .. } = open_future_log(target.path());
        let throttle = frozen_quota();
        // A bound far below the chunk: a grant-based meter would copy one
        // batch at most, Kafka's quota copies the whole fetch.
        throttle.set_bound(Some(bytes_per_sec(1)));

        let progress = catch_up(&part, &future, mebibytes(1), &throttle).expect("catch up");

        let copied: usize = future
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .read(Offset(0), mebibytes(1))
            .expect("read future")
            .batches
            .iter()
            .map(RecordBatch::encoded_batch_len)
            .sum();
        let copied = i64::try_from(copied).expect("copied bytes");
        assert!(
            (
                progress_flags(&progress),
                future_leo(&future),
                throttle.measured_rate()
            ) == (
                (false, false),
                Offset(2),
                ByteRate::from_bytes_per_sec(copied)
            )
        );
        assert!(throttle.quota_exceeded());
    }

    #[tokio::test]
    async fn catch_up_with_zero_bound_copies_one_chunk_then_waits() {
        let (part, _primary, target) = two_batch_fixture();
        let OpenedFutureLog { log: future, .. } = open_future_log(target.path());
        let throttle = frozen_quota();
        // Kafka accepts a measured rate of 0 under a bound of 0, so the first
        // fetch goes through and only the bytes it records close the quota.
        throttle.set_bound(Some(bytes_per_sec(0)));

        let first = catch_up(&part, &future, mebibytes(1), &throttle).expect("first pass");
        let after_first = future_leo(&future);
        // Give the second pass something to copy, so only the quota can stop it.
        append_records(&part, PartitionRecordsSetup {
            count: 1.into(),
            ..Default::default()
        });
        let second = catch_up(&part, &future, mebibytes(1), &throttle).expect("second pass");

        assert!(
            (
                progress_flags(&first),
                after_first,
                progress_flags(&second),
                future_leo(&future),
            ) == ((false, false), Offset(2), (false, true), Offset(2))
        );
    }

    /// Kafka asks the move quota before it fetches at all, so a future log
    /// that has caught up is not reported caught up, and so not promoted,
    /// while the quota is exceeded. Once the quota reopens it is.
    #[tokio::test]
    async fn a_caught_up_move_waits_for_an_exceeded_quota() {
        let fixture = source_fixture().count(1).call();
        let _primary = fixture.primary;
        let OpenedFutureLog { log: future, .. } = open_future_log(fixture.target.path());
        let throttle = frozen_quota();
        catch_up(&fixture.partition, &future, mebibytes(1), &throttle).expect("copy");
        throttle.set_bound(Some(bytes_per_sec(1)));

        let exceeded =
            catch_up(&fixture.partition, &future, mebibytes(1), &throttle).expect("exceeded pass");
        throttle.set_bound(None);
        let reopened =
            catch_up(&fixture.partition, &future, mebibytes(1), &throttle).expect("reopened pass");

        assert!(
            (progress_flags(&exceeded), progress_flags(&reopened))
                == ((false, true), (true, false))
        );
    }
}
