//! The cuts that keep a KIP-113 future log a prefix of the current log's
//! history.
//!
//! Kafka's swap condition is only `futureLog.logEndOffset ==
//! log.logEndOffset` (`Partition.runCallbackIfFutureReplicaCaughtUp`), so the
//! `ReplicaAlterLogDirsThread` has to keep the future replica consistent with
//! the current one by two other means: `markPartitionsForTruncation`, which a
//! follower calls when it truncates the current log, and a leader-epoch
//! exchange with the current log whenever the fetcher starts
//! (`AbstractFetcherThread.truncateToEpochEndOffsets`). The second one covers a
//! future replica that was offline when the current one truncated and grew back,
//! for example across a broker restart: the mark is only in memory.

use {
    crate::{error::BrokerError, partition::lock_partition_log},
    krabka_ids::{LeaderEpoch, PartitionIndex},
    krabka_log::{LeaderEpochCheckpoint, Log, Offset},
    std::sync::{Mutex, PoisonError},
};

/// Cut the future log to `offset`, or to nothing when `offset` lies below its
/// log start, as the current log did.
pub(super) fn cut_future_log(future_log: &Mutex<Log>, offset: Offset) -> Result<(), BrokerError> {
    let mut future = future_log.lock().unwrap_or_else(PoisonError::into_inner);
    if offset < future.log_start_offset() {
        future.reset_to(offset)?;
    } else {
        future.truncate_to(offset)?;
    }
    drop(future);
    Ok(())
}

/// Kafka's `OffsetTruncationState`: where to truncate, and whether that ends
/// the exchange.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TruncationState {
    offset: Offset,
    completed: bool,
}

/// A current log's answer about an epoch: the epoch it names and where that
/// epoch ends.
#[derive(Clone, Copy)]
struct EpochEnd {
    epoch: LeaderEpoch,
    end: Offset,
}

/// Kafka's `AbstractFetcherThread.getOffsetTruncationState` for the
/// `ReplicaAlterLogDirsThread`, where the current log answers as the leader.
///
/// `current` is the current log's `endOffsetFor` answer for the future log's
/// latest epoch: the largest current epoch at or below it and where that epoch
/// ends, or `(-1, -1)` when the current log cannot place it. `future` is the
/// future log's own epoch history and `future_end` its log end.
///
/// - The current log cannot place the epoch: Kafka truncates to the future
///   replica's high watermark. This broker keeps none for a future log, so it
///   drops the future log's latest epoch and asks again for the one before.
/// - The future log does not know the epoch the current log named (it knows a
///   smaller one): truncate to the end of that smaller epoch and ask again.
/// - Otherwise the lowest of the two ends for the epoch and the future log
///   end, and the exchange is over.
fn offset_truncation_state(
    current: EpochEnd,
    future: &LeaderEpochCheckpoint,
    future_end: Offset,
    latest_epoch_start: Offset,
) -> TruncationState {
    let EpochEnd {
        epoch: current_epoch,
        end: current_end,
    } = current;
    if current_end < Offset(0) {
        return TruncationState {
            offset: latest_epoch_start,
            completed: false,
        };
    }
    let (future_epoch, future_epoch_end) = future.epoch_and_offset_for(current_epoch, future_end);
    if future_epoch_end < Offset(0) {
        TruncationState {
            offset: current_end.min(future_end),
            completed: true,
        }
    } else if future_epoch != current_epoch {
        TruncationState {
            offset: future_epoch_end.min(future_end),
            completed: false,
        }
    } else {
        TruncationState {
            offset: future_epoch_end.min(current_end).min(future_end),
            completed: true,
        }
    }
}

/// Truncates the future log by leader epoch until it and the current log agree
/// on the history they share, as the `ReplicaAlterLogDirsThread` does for a
/// partition it starts to fetch: the future log's latest epoch is put to the
/// current log, which answers where that epoch ends there, and the future log
/// keeps only what both hold.
///
/// A future log with records but no epoch entry cannot be compared, so it is
/// emptied (Kafka truncates it to its high watermark).
///
/// # Errors
/// Returns an error when a cut fails, and
/// [`BrokerError::PartitionWriterDied`] when the current log of
/// `topic`-`partition` is poisoned.
pub(super) fn truncate_to_current_epochs(
    current_log: &Mutex<Log>,
    (topic, partition): (&str, PartitionIndex),
    future_log: &Mutex<Log>,
) -> Result<(), BrokerError> {
    // The lock order the swap uses: the current log, then the future log.
    let current = lock_partition_log(current_log, topic, partition)?;
    let mut future = future_log.lock().unwrap_or_else(PoisonError::into_inner);
    truncate_locked_to_current_epochs(&current, &mut future)
}

/// [`truncate_to_current_epochs`] with both logs already locked.
fn truncate_locked_to_current_epochs(current: &Log, future: &mut Log) -> Result<(), BrokerError> {
    // Every pass drops the future log's latest epoch or ends the exchange.
    for _ in 0..=future.epoch_checkpoint().entries().len() {
        let future_end = future.log_end_offset();
        let Some(latest) = future.epoch_checkpoint().entries().last().copied() else {
            if future_end > future.log_start_offset() {
                let start = future.log_start_offset();
                future.reset_to(start)?;
            }
            return Ok(());
        };
        let (reply_epoch, reply_end) = current
            .epoch_checkpoint()
            .epoch_and_offset_for(latest.epoch, current.log_end_offset());
        let state = offset_truncation_state(
            EpochEnd {
                epoch: reply_epoch,
                end: reply_end,
            },
            future.epoch_checkpoint(),
            future_end,
            latest.start_offset,
        );
        if state.offset < future_end {
            if state.offset < future.log_start_offset() {
                future.reset_to(state.offset)?;
            } else {
                future.truncate_to(state.offset)?;
            }
        }
        if state.completed {
            return Ok(());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{future_log::test_support::step_list, log_dir},
        assert2::assert,
        bytes::Bytes,
        krabka_bdd::{Given, given, then, when},
        krabka_log::LogConfig,
        krabka_protocol::records::{Record, RecordBatch},
        std::{fs, num::ParseIntError, path::Path, str::FromStr},
        tempfile::tempdir,
    };

    /// An epoch and the offset it starts at.
    #[derive(Clone, Copy)]
    struct EpochStart {
        epoch: i32,
        start: i64,
    }

    /// A checkpoint, and the directory that holds its file.
    struct CheckpointInDir {
        _directory_guard: tempfile::TempDir,
        checkpoint: LeaderEpochCheckpoint,
    }

    /// A checkpoint that records `entries`.
    fn checkpoint(entries: &[EpochStart]) -> CheckpointInDir {
        let dir = tempdir().unwrap();
        let mut checkpoint =
            LeaderEpochCheckpoint::open(dir.path().join("leader-epoch-checkpoint")).unwrap();
        for &EpochStart { epoch, start } in entries {
            checkpoint
                .append(LeaderEpoch(epoch), Offset(start))
                .unwrap();
        }
        CheckpointInDir {
            _directory_guard: dir,
            checkpoint,
        }
    }

    #[derive(Default)]
    struct FutureEpochExchange {
        future: Option<CheckpointInDir>,
        current: Option<EpochEnd>,
        future_end: Option<Offset>,
        initial_truncation: Option<Offset>,
        selected: Option<TruncationState>,
    }

    #[rstest::fixture]
    fn future_epoch_exchange() -> FutureEpochExchange {
        // Each migration exchange owns its checkpoint and truncation observation.
        FutureEpochExchange::default()
    }

    #[given(
        "the future log has epoch {first_epoch} starting at {first_start} and epoch {last_epoch} starting at {last_start}"
    )]
    fn supplied_future_epoch_checkpoint(
        future_epoch_exchange: &mut FutureEpochExchange,
        first_epoch: i32,
        first_start: i64,
        last_epoch: i32,
        last_start: i64,
    ) {
        future_epoch_exchange.future = Some(checkpoint(&[
            EpochStart {
                epoch: first_epoch,
                start: first_start,
            },
            EpochStart {
                epoch: last_epoch,
                start: last_start,
            },
        ]));
    }

    #[given("the future log ends at {end} and the initial truncation offset is {initial}")]
    fn supplied_future_epoch_offsets(
        future_epoch_exchange: &mut FutureEpochExchange,
        end: i64,
        initial: i64,
    ) {
        future_epoch_exchange.future_end = Some(Offset(end));
        future_epoch_exchange.initial_truncation = Some(Offset(initial));
    }

    #[given("the current log reports epoch {epoch} ending at {end}")]
    fn supplied_current_epoch_end(
        future_epoch_exchange: &mut FutureEpochExchange,
        epoch: i32,
        end: i64,
    ) {
        future_epoch_exchange.current = Some(EpochEnd {
            epoch: LeaderEpoch(epoch),
            end: Offset(end),
        });
    }

    #[when("the migration chooses its next future-log truncation state")]
    fn choose_future_epoch_truncation(future_epoch_exchange: &mut FutureEpochExchange) {
        future_epoch_exchange.selected = Some(offset_truncation_state(
            future_epoch_exchange
                .current
                .expect("current epoch answer was given"),
            &future_epoch_exchange
                .future
                .as_ref()
                .expect("future checkpoint was given")
                .checkpoint,
            future_epoch_exchange
                .future_end
                .expect("future log end was given"),
            future_epoch_exchange
                .initial_truncation
                .expect("initial truncation offset was given"),
        ));
    }

    #[then(
        "the future-log truncation offset is {offset} and reconciliation completed is {completed}"
    )]
    fn expected_future_epoch_truncation(
        future_epoch_exchange: &FutureEpochExchange,
        offset: i64,
        completed: bool,
    ) {
        assert!(
            future_epoch_exchange.selected
                == Some(TruncationState {
                    offset: Offset(offset),
                    completed
                })
        );
    }

    fn log_with_epochs(dir: &Path, name: &str, batches: &[LeaderEpoch]) -> Mutex<Log> {
        let path = log_dir::partition_dir(dir, name, 0);
        fs::create_dir_all(&path).unwrap();
        let mut log = Log::open(&path, LogConfig::default()).unwrap();
        for &epoch in batches {
            let mut batch = RecordBatch {
                partition_leader_epoch: epoch.0,
                records: vec![Record {
                    value: Some(Bytes::from_static(b"v")),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            };
            log.append(&mut batch).unwrap();
        }
        Mutex::new(log)
    }

    fn leo(log: &Mutex<Log>) -> Offset { log.lock().unwrap().log_end_offset() }

    /// The leader epochs a log's records carry, one record per entry, written
    /// as `1,1,2` or `nothing`.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordEpochs(Vec<LeaderEpoch>);

    impl FromStr for RecordEpochs {
        type Err = ParseIntError;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            step_list(text, |epoch| epoch.parse().map(LeaderEpoch)).map(Self)
        }
    }

    struct ReconcileScenario {
        directory: tempfile::TempDir,
        future: Given<Mutex<Log>>,
        current: Given<Mutex<Log>>,
        current_records: Given<i64>,
    }

    #[rstest::fixture]
    fn reconcile() -> ReconcileScenario {
        const CURRENT: &str = "Given a current log whose records carry leader epochs <current>";
        ReconcileScenario {
            directory: tempdir().unwrap(),
            future: Given::new(
                "future",
                "Given a future log whose records carry leader epochs <future>",
            ),
            current: Given::new("current", CURRENT),
            current_records: Given::new("current_records", CURRENT),
        }
    }

    #[given("a future log whose records carry leader epochs {future}")]
    fn future_log_epochs(reconcile: &mut ReconcileScenario, future: RecordEpochs) {
        let log = log_with_epochs(reconcile.directory.path(), "future", &future.0);
        reconcile.future.set(log);
    }

    #[given("a current log whose records carry leader epochs {current}")]
    fn current_log_epochs(reconcile: &mut ReconcileScenario, current: RecordEpochs) {
        let log = log_with_epochs(reconcile.directory.path(), "current", &current.0);
        reconcile
            .current_records
            .set(i64::try_from(current.0.len()).expect("a record count"));
        reconcile.current.set(log);
    }

    #[when("the future log is reconciled with the current log")]
    fn reconcile_future_log(reconcile: &mut ReconcileScenario) {
        truncate_to_current_epochs(
            reconcile.current.get(),
            ("t", PartitionIndex(0)),
            reconcile.future.get(),
        )
        .unwrap();
    }

    #[then("the future log ends at offset {end} while the current log keeps every record")]
    fn future_log_ends_at(reconcile: &ReconcileScenario, end: i64) {
        assert!(
            (leo(reconcile.future.get()), leo(reconcile.current.get()))
                == (Offset(end), Offset(*reconcile.current_records.get()))
        );
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/future_log/truncation.feature",
        fixtures = [future_epoch_exchange: FutureEpochExchange, reconcile: ReconcileScenario],
    );
}
