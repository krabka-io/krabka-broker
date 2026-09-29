//! The per-move replicator task that drives a future log to its swap.
//!
//! The task repeats a `catch_up` pass until the future log matches the source
//! log's end offset, then asks the partition writer to exchange the two
//! directories with `WriterMessage::SwapFutureLog` and acts on the outcome it
//! acknowledges.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, Offset};
use krabka_units::convert::TimeExt as _;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::{FutureLogState, MovePolicy, catch_up::catch_up};
use crate::{
    error::BrokerError,
    partition::{Partition, SwapOutcome, WriterMessage},
    partition_registry::PartitionRegistry,
};

/// Replicator task body. It copies batches from `part.log` to `future_log`
/// incrementally, then asks the partition writer to swap.
pub(super) struct ReplicatorTask {
    pub(super) part: Arc<Partition>,
    pub(super) future_log: Arc<Mutex<Log>>,
    pub(super) future_path: PathBuf,
    pub(super) target_partition_path: PathBuf,
    pub(super) target_log_dir: PathBuf,
    pub(super) cancel: CancellationToken,
    pub(super) _partitions: Arc<PartitionRegistry>,
    pub(super) future_logs: Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    pub(super) topic: String,
    pub(super) partition: PartitionIndex,
    pub(super) policy: MovePolicy,
}

/// Cut the future log to `offset`, or to nothing when `offset` lies below its
/// log start, as the current log did.
fn cut_future_log(future_log: &Mutex<Log>, offset: Offset) -> Result<(), BrokerError> {
    let mut future = future_log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if offset < future.log_start_offset() {
        future.reset_to(offset)?;
    } else {
        future.truncate_to(offset)?;
    }
    Ok(())
}

pub(super) async fn replicator_loop(task: ReplicatorTask) {
    let ReplicatorTask {
        part,
        future_log,
        future_path,
        target_partition_path,
        target_log_dir,
        cancel,
        _partitions,
        future_logs,
        topic,
        partition,
        policy,
    } = task;
    debug!(
        topic = %topic, partition = partition.get(),
        target = %target_log_dir.display(),
        "future-log replicator started"
    );
    // The lowest offset that the current log was cut to and that the future
    // log has not been cut to yet.
    let mut owed_cut: Option<Offset> = None;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        // Kafka's `markPartitionsForTruncation`: what the current log lost to a
        // truncation, the future log must lose too, or it would install
        // records the new leader never had.
        match part.take_future_truncation().await {
            Ok(taken) => owed_cut = owed_cut.into_iter().chain(taken).min(),
            Err(e) => {
                warn!(
                    topic = %topic, partition = partition.get(),
                    error = %e,
                    "future-log replicator: partition writer is dead; aborting move"
                );
                break;
            }
        }
        if let Some(offset) = owed_cut {
            if let Err(e) = cut_future_log(&future_log, offset) {
                warn!(
                    topic = %topic, partition = partition.get(),
                    error = %e,
                    "future-log replicator could not truncate the future log; retrying"
                );
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(policy.retry_backoff.to_std()) => continue,
                }
            }
            owed_cut = None;
        }
        // Read whatever is missing from the future log up to the source's
        // current LEO, bounded by the broker-wide log-directory copy budget.
        let advance = match catch_up(&part, &future_log, policy.read_chunk, &policy.throttle) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    topic = %topic, partition = partition.get(),
                    error = %e,
                    "future-log replicator catch-up failed; retrying"
                );
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(policy.retry_backoff.to_std()) => continue,
                }
            }
        };

        if advance.throttled {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(policy.retry_backoff.to_std()) => continue,
            }
        }

        if !advance.caught_up {
            // Make forward progress, then immediately re-check. We
            // only wait on `append_notify` once we believe we are
            // caught up.
            continue;
        }

        // We believe we're caught up; ask the writer to swap.
        let (ack_tx, ack_rx) = oneshot::channel();
        let send = part
            .writer_tx
            .send(WriterMessage::SwapFutureLog {
                target_log_dir: target_log_dir.clone(),
                future_log: future_log.clone(),
                future_path: future_path.clone(),
                target_partition_path: target_partition_path.clone(),
                ack: ack_tx,
            })
            .await;
        if send.is_err() {
            warn!(
                topic = %topic, partition = partition.get(),
                "future-log replicator: partition writer is dead; aborting move"
            );
            break;
        }
        match ack_rx.await {
            Ok(Ok(SwapOutcome::Swapped)) => {
                debug!(topic = %topic, partition = partition.get(), "future-log swap complete");
                break;
            }
            Ok(Ok(SwapOutcome::NotCaughtUp)) => {
                // Producers wrote in between catch_up and the writer
                // receiving the message — loop and try again.
            }
            Ok(Err(e)) => {
                warn!(
                    topic = %topic, partition = partition.get(),
                    error = %e,
                    "future-log swap failed; aborting move (partition continues on source dir)"
                );
                break;
            }
            Err(_) => {
                warn!(topic = %topic, partition = partition.get(), "future-log swap ack dropped");
                break;
            }
        }

        // Wait for the next append (or cancellation) before retrying.
        tokio::select! {
            () = cancel.cancelled() => break,
            () = part.append_notify.notified() => {}
        }
    }
    // Whatever the outcome, the future-log entry is no longer useful.
    future_logs.remove(&(topic, partition));
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicU64, time::Duration};

    use assert2::assert;
    use krabka_log::LogConfig;
    use krabka_units::mebibytes;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        future_log::{
            canonicalize_or_self, resume_move,
            test_support::{
                TestStampSource, append_records, append_value_batch, fixture_partition, test_policy,
            },
        },
        log_dir,
    };

    #[tokio::test]
    async fn resume_move_catches_up_and_swaps_future_log() {
        let primary = tempdir().unwrap();
        let target = tempdir().unwrap();
        let stamp_source: Arc<dyn krabka_log::StampSource> =
            Arc::new(TestStampSource(AtomicU64::new(100)));
        let partitions = Arc::new(PartitionRegistry::with_stamp_source(Some(Arc::clone(
            &stamp_source,
        ))));
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        part.log
            .lock()
            .expect("source log")
            .set_stamp_source(stamp_source)
            .expect("source stamp index");
        append_records(&part, 3);
        partitions.insert("t".into(), PartitionIndex(0), part.clone());

        let future_path = log_dir::future_partition_dir(target.path(), "t", 0);
        std::fs::create_dir_all(&future_path).unwrap();

        resume_move(
            &partitions,
            &future_logs,
            target.path(),
            &LogConfig::default(),
            "t",
            PartitionIndex(0),
            test_policy(),
        )
        .expect("resume should spawn a future-log move");

        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                let moved = canonicalize_or_self(&part.log_dir.load_full())
                    == canonicalize_or_self(target.path());
                if moved && future_logs.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("future log should catch up and swap");

        assert!(part.log_end_offset() == 3);
        assert!(
            canonicalize_or_self(&part.log_dir.load_full()) == canonicalize_or_self(target.path())
        );
        assert!(part.stamp_for_offset(Offset(0)) == Some(101));
        append_records(&part, 1);
        assert!(part.stamp_for_offset(Offset(3)) == Some(102));
    }

    #[tokio::test]
    async fn resume_move_continues_after_partial_catch_up() {
        let primary = tempdir().unwrap();
        let target = tempdir().unwrap();
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        for _ in 0..4 {
            append_value_batch(&part, 400 * 1024);
        }
        partitions.insert("t".into(), PartitionIndex(0), part.clone());

        let future_path = log_dir::future_partition_dir(target.path(), "t", 0);
        std::fs::create_dir_all(&future_path).unwrap();

        resume_move(
            &partitions,
            &future_logs,
            target.path(),
            &LogConfig::default(),
            "t",
            PartitionIndex(0),
            test_policy(),
        )
        .expect("resume should spawn a future-log move");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let moved = canonicalize_or_self(&part.log_dir.load_full())
                    == canonicalize_or_self(target.path());
                if moved && future_logs.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("future log should keep copying after a partial catch-up pass");

        assert!(part.log_end_offset() == 4);
    }

    /// A partition of `t-0` holding five one-record batches of ten bytes, and
    /// a future log that is a full copy of it.
    fn partition_with_copied_future_log(
        primary: &std::path::Path,
        target: &std::path::Path,
    ) -> (Arc<Partition>, Arc<Mutex<Log>>, PathBuf) {
        let part = fixture_partition(primary, "t", PartitionIndex(0));
        for _ in 0..5 {
            append_value_batch(&part, 10);
        }
        let future_path = log_dir::future_partition_dir(target, "t", 0);
        std::fs::create_dir_all(&future_path).unwrap();
        let future_log = Arc::new(Mutex::new(
            Log::open(&future_path, LogConfig::default()).unwrap(),
        ));
        let policy = test_policy();
        while !catch_up(&part, &future_log, mebibytes(1), &policy.throttle)
            .unwrap()
            .caught_up
        {}
        assert!(future_log.lock().unwrap().log_end_offset() == Offset(5));
        (part, future_log, future_path)
    }

    /// The value size of each record of `log` from offset 0.
    fn value_sizes(log: &Mutex<Log>) -> Vec<usize> {
        log.lock()
            .unwrap()
            .read(Offset(0), mebibytes(1))
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.records[0].value.as_ref().unwrap().len())
            .collect()
    }

    /// KIP-113 with a leader change: a follower truncates its current log to 3
    /// and takes five records again, two of them from the new leader. The
    /// future log still holds the old records 3 and 4 and ends where the
    /// current log ends, so equal log ends alone would swap the stale records
    /// in.
    #[tokio::test]
    async fn a_truncation_of_the_current_log_cuts_the_future_log_before_the_swap() {
        let primary = tempdir().unwrap();
        let target = tempdir().unwrap();
        let (part, future_log, future_path) =
            partition_with_copied_future_log(primary.path(), target.path());
        part.truncate_to(Offset(3)).await.unwrap();
        for _ in 0..2 {
            append_value_batch(&part, 20);
        }
        assert!(part.log_end_offset() == Offset(5));
        assert!(value_sizes(&part.log) == [10, 10, 10, 20, 20]);

        tokio::time::timeout(
            Duration::from_secs(10),
            replicator_loop(ReplicatorTask {
                part: Arc::clone(&part),
                future_log,
                target_partition_path: log_dir::partition_dir(target.path(), "t", 0),
                future_path,
                target_log_dir: target.path().to_path_buf(),
                cancel: CancellationToken::new(),
                _partitions: Arc::new(PartitionRegistry::new()),
                future_logs: Arc::new(DashMap::new()),
                topic: "t".into(),
                partition: PartitionIndex(0),
                policy: test_policy(),
            }),
        )
        .await
        .expect("the move should finish");

        assert!(
            canonicalize_or_self(&part.log_dir.load_full()) == canonicalize_or_self(target.path())
        );
        assert!(value_sizes(&part.log) == [10, 10, 10, 20, 20]);
    }

    /// The writer reports the lowest offset the log was cut to once, and
    /// refuses a swap until it has.
    #[tokio::test]
    async fn a_swap_waits_for_the_cut_the_writer_has_not_reported() {
        let primary = tempdir().unwrap();
        let target = tempdir().unwrap();
        let (part, future_log, future_path) =
            partition_with_copied_future_log(primary.path(), target.path());
        assert!(part.take_future_truncation().await.unwrap() == None);
        part.truncate_to(Offset(4)).await.unwrap();
        part.truncate_to(Offset(2)).await.unwrap();
        for _ in 0..3 {
            append_value_batch(&part, 20);
        }
        assert!(part.log_end_offset() == Offset(5));
        let (ack, outcome) = oneshot::channel();
        part.writer_tx
            .send(WriterMessage::SwapFutureLog {
                target_log_dir: target.path().to_path_buf(),
                future_log,
                future_path,
                target_partition_path: log_dir::partition_dir(target.path(), "t", 0),
                ack,
            })
            .await
            .unwrap();

        assert!(outcome.await.unwrap().unwrap() == SwapOutcome::NotCaughtUp);
        assert!(part.take_future_truncation().await.unwrap() == Some(Offset(2)));
        assert!(part.take_future_truncation().await.unwrap() == None);
    }
}
