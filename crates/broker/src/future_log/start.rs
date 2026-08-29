//! Creation of a KIP-113 future log: the entry points that stage a
//! `<topic>-<partition>-future` directory and start its replicator task.
//!
//! `start_move` serves an `AlterReplicaLogDirs` request and `resume_move`
//! picks up a move that a crash interrupted. Both end in `spawn_move`, which
//! registers the `FutureLogState` and spawns the replicator.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig};
use tokio_util::sync::CancellationToken;

use super::{
    FutureLogState, MoveError, MovePolicy, canonicalize_or_self,
    cleanup::cancel_move,
    replicator::{ReplicatorTask, replicator_loop},
};
use crate::{log_dir, partition::Partition, partition_registry::PartitionRegistry};

/// Start a move of `(topic, partition)` to `target_log_dir`, or confirm an
/// identical move as a no-op. Returns immediately after it spawns the
/// replicator task, so the `AlterReplicaLogDirs` handler can then ack success.
///
/// Idempotency: if a move with the same target is already in flight,
/// returns `Ok(())` without spawning a second task. If its target differs,
/// the old task and future log are removed before the replacement starts.
pub(crate) async fn start_move(
    partitions: &Arc<PartitionRegistry>,
    future_logs: &Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    all_log_dirs: &[PathBuf],
    log_config: &LogConfig,
    topic_partition: (&str, PartitionIndex),
    target_log_dir: &Path,
    policy: MovePolicy,
) -> Result<(), MoveError> {
    let (topic, partition) = topic_partition;
    // (1) Validate the target is a configured log.dir. Path comparison
    //     is canonical-form to side-step trailing-slash / `.` quirks.
    let target_canon = canonicalize_or_self(target_log_dir);
    let target_match = all_log_dirs
        .iter()
        .find(|d| canonicalize_or_self(d) == target_canon)
        .cloned();
    let Some(target_log_dir) = target_match else {
        return Err(MoveError::LogDirNotFound);
    };

    // (2) Partition must be hosted on this broker.
    let key = (topic.to_string(), partition);
    let part = partitions
        .get(topic, partition)
        .ok_or(MoveError::ReplicaNotAvailable)?;

    // (3) Already moving? Keep a same-target request idempotent. Kafka stops
    //     and removes a future replica when a later request changes its
    //     destination, including when the new destination is the current dir.
    if let Some(existing) = future_logs.get(&key).map(|e| e.value().clone()) {
        if canonicalize_or_self(&existing.target_log_dir) == canonicalize_or_self(&target_log_dir) {
            return Ok(());
        }
        cancel_move(future_logs, &key, existing).await?;
    }

    // (4) Already in the target dir? No-op success. This check must follow
    //     cancellation so redirecting a move back to its source stops it.
    let current_log_dir = part.log_dir.load_full();
    if canonicalize_or_self(&current_log_dir) == canonicalize_or_self(&target_log_dir) {
        return Ok(());
    }

    // (5) Open the future log at <target>/<topic>-<partition>-future.
    let future_path = log_dir::future_partition_dir(&target_log_dir, topic, partition.get());
    std::fs::create_dir_all(&future_path)?;
    let future_log = open_future_log(partitions, &future_path, log_config)?;

    spawn_move(MoveTask {
        partitions: partitions.clone(),
        future_logs: future_logs.clone(),
        target_log_dir,
        future_path,
        future_log,
        topic: topic.to_string(),
        partition,
        part,
        policy,
    });
    Ok(())
}

/// Recover an interrupted move discovered on disk at broker startup
/// (a `<topic>-<partition>-future` directory in a configured log.dir
/// whose corresponding partition exists). Re-opens the future log
/// and re-spawns the replicator, picking up at whatever offset the
/// future log already holds.
pub(crate) fn resume_move(
    partitions: &Arc<PartitionRegistry>,
    future_logs: &Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    target_log_dir: &Path,
    log_config: &LogConfig,
    topic: &str,
    partition: PartitionIndex,
    policy: MovePolicy,
) -> Result<(), MoveError> {
    let part = partitions
        .get(topic, partition)
        .ok_or(MoveError::ReplicaNotAvailable)?;
    let future_path = log_dir::future_partition_dir(target_log_dir, topic, partition.get());
    let future_log = open_future_log(partitions, &future_path, log_config)?;
    spawn_move(MoveTask {
        partitions: partitions.clone(),
        future_logs: future_logs.clone(),
        target_log_dir: target_log_dir.to_path_buf(),
        future_path,
        future_log,
        topic: topic.to_string(),
        partition,
        part,
        policy,
    });
    Ok(())
}

fn open_future_log(
    partitions: &PartitionRegistry,
    path: &Path,
    log_config: &LogConfig,
) -> Result<Arc<Mutex<Log>>, MoveError> {
    let mut log = Log::open(path, log_config.clone())?;
    if let Some(stamp_source) = partitions.stamp_source() {
        log.set_stamp_source(stamp_source)?;
    }
    Ok(Arc::new(Mutex::new(log)))
}

/// Shared by [`start_move`] and [`resume_move`]. It builds the
/// `FutureLogState`, inserts it into the registry, and spawns the
/// per-move replicator task.
struct MoveTask {
    partitions: Arc<PartitionRegistry>,
    future_logs: Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    target_log_dir: PathBuf,
    future_path: PathBuf,
    future_log: Arc<Mutex<Log>>,
    topic: String,
    partition: PartitionIndex,
    part: Arc<Partition>,
    policy: MovePolicy,
}

fn spawn_move(task: MoveTask) {
    let cancel = CancellationToken::new();
    let target_partition_path =
        log_dir::partition_dir(&task.target_log_dir, &task.topic, task.partition.get());
    let replicator = tokio::spawn(replicator_loop(ReplicatorTask {
        part: task.part,
        future_log: task.future_log.clone(),
        future_path: task.future_path.clone(),
        target_partition_path,
        target_log_dir: task.target_log_dir.clone(),
        cancel: cancel.clone(),
        _partitions: task.partitions,
        future_logs: task.future_logs.clone(),
        topic: task.topic.clone(),
        partition: task.partition,
        policy: task.policy,
    }));
    let state = Arc::new(FutureLogState {
        target_log_dir: task.target_log_dir,
        future_path: task.future_path,
        future_log: task.future_log,
        cancel,
        task: std::sync::Mutex::new(Some(replicator)),
    });
    task.future_logs.insert((task.topic, task.partition), state);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use tempfile::tempdir;

    use super::*;
    use crate::future_log::test_support::{append_records, fixture_partition, test_policy};

    #[tokio::test]
    async fn move_error_log_dir_not_found_when_target_unknown() {
        // Empty broker — no partitions, no log dirs. `start_move`
        // returns LogDirNotFound before it ever looks at the
        // partition map.
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let log_dirs: Vec<PathBuf> = vec![];
        let bogus = tempdir().unwrap();
        let err = start_move(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            bogus.path(),
            test_policy(),
        )
        .await
        .expect_err("expected LogDirNotFound");
        assert!(matches!(err, MoveError::LogDirNotFound));
    }

    #[tokio::test]
    async fn move_error_replica_not_available_when_partition_missing() {
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let dir = tempdir().unwrap();
        let err = start_move(
            &partitions,
            &future_logs,
            &[dir.path().to_path_buf()],
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            dir.path(),
            test_policy(),
        )
        .await
        .expect_err("expected ReplicaNotAvailable");
        assert!(matches!(err, MoveError::ReplicaNotAvailable));
    }

    #[tokio::test]
    async fn start_move_to_current_dir_is_noop() {
        // Asking to move a partition to the directory it already
        // lives in returns success without touching `future_logs`.
        let primary = tempdir().unwrap();
        let extra = tempdir().unwrap();
        let log_dirs = vec![primary.path().to_path_buf(), extra.path().to_path_buf()];
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        partitions.insert("t".to_string(), PartitionIndex(0), part);

        start_move(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            primary.path(),
            test_policy(),
        )
        .await
        .expect("noop should succeed");
        assert!(
            future_logs.is_empty(),
            "noop must not register a future log"
        );
    }

    #[test]
    fn resume_move_errors_when_partition_missing() {
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let target = tempdir().unwrap();

        let err = resume_move(
            &partitions,
            &future_logs,
            target.path(),
            &LogConfig::default(),
            "missing",
            PartitionIndex(0),
            test_policy(),
        )
        .expect_err("missing partition must reject resume");

        assert!(matches!(err, MoveError::ReplicaNotAvailable));
        assert!(future_logs.is_empty());
    }

    #[tokio::test]
    async fn start_move_idempotent_for_same_target() {
        // Two ARLD calls with the same target while the first move is
        // still in flight collapse to one — second call returns Ok(())
        // and the registry still has one entry.
        let primary = tempdir().unwrap();
        let extra = tempdir().unwrap();
        let log_dirs = vec![primary.path().to_path_buf(), extra.path().to_path_buf()];
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        partitions.insert("t".to_string(), PartitionIndex(0), part);

        // Plant a registry entry as if a prior ARLD already kicked off
        // a move — exercises the "already moving, same target" branch
        // without racing the replicator's swap-and-remove.
        let future_path = log_dir::future_partition_dir(extra.path(), "t", 0);
        std::fs::create_dir_all(&future_path).unwrap();
        let future_log = Arc::new(Mutex::new(
            Log::open(&future_path, LogConfig::default()).unwrap(),
        ));
        future_logs.insert(
            ("t".to_string(), PartitionIndex(0)),
            Arc::new(FutureLogState {
                target_log_dir: extra.path().to_path_buf(),
                future_path: future_path.clone(),
                future_log,
                cancel: CancellationToken::new(),
                task: std::sync::Mutex::new(None),
            }),
        );

        start_move(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            extra.path(),
            test_policy(),
        )
        .await
        .expect("same-target alter must be idempotent");
        assert!(future_logs.len() == 1);
    }

    #[tokio::test]
    async fn start_move_redirects_conflicting_target() {
        // Kafka stops and removes the old future replica before it starts a
        // replacement toward the new destination.
        let primary = tempdir().unwrap();
        let extra = tempdir().unwrap();
        let third = tempdir().unwrap();
        let log_dirs = vec![
            primary.path().to_path_buf(),
            extra.path().to_path_buf(),
            third.path().to_path_buf(),
        ];
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        append_records(&part, 3);
        partitions.insert("t".to_string(), PartitionIndex(0), part.clone());

        // Plant a registry entry pointing at `extra`.
        let future_path = log_dir::future_partition_dir(extra.path(), "t", 0);
        std::fs::create_dir_all(&future_path).unwrap();
        let future_log = Arc::new(Mutex::new(
            Log::open(&future_path, LogConfig::default()).unwrap(),
        ));
        let old_cancel = CancellationToken::new();
        future_logs.insert(
            ("t".to_string(), PartitionIndex(0)),
            Arc::new(FutureLogState {
                target_log_dir: extra.path().to_path_buf(),
                future_path: future_path.clone(),
                future_log,
                cancel: old_cancel.clone(),
                task: std::sync::Mutex::new(None),
            }),
        );

        start_move(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            third.path(),
            test_policy(),
        )
        .await
        .expect("conflicting-target alter must redirect");

        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                let moved = canonicalize_or_self(&part.log_dir.load_full())
                    == canonicalize_or_self(third.path());
                if moved && future_logs.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement move should complete");

        assert!(old_cancel.is_cancelled());
        assert!(!future_path.exists());
        assert!(part.log_end_offset() == 3);
    }
}
