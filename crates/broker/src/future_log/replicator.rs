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
use krabka_log::Log;
use krabka_units::convert::TimeExt as _;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::{FutureLogState, MovePolicy, catch_up::catch_up};
use crate::{
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
    loop {
        if cancel.is_cancelled() {
            break;
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
    use krabka_log::{LogConfig, Offset};
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
        partitions.insert("t".to_string(), PartitionIndex(0), part.clone());

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
        partitions.insert("t".to_string(), PartitionIndex(0), part.clone());

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
}
