//! The KIP-113 intra-broker log-dir swap that the writer performs in place.
//!
//! The swap is the only writer message that renames directories and reopens
//! two logs under one lock, so its filesystem recovery path is isolated here.

use {
    super::storage::{lock_log, storage_failure_error},
    crate::{
        blocking::spawn_blocking, error::BrokerError, log_dir_status::LogDirRegistry,
        partition::SwapOutcome,
    },
    arc_swap::ArcSwap,
    krabka_log::{Log, LogConfig, StampSource},
    krabka_verified::storage,
    std::{
        fs, mem,
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    },
};

/// The future log and the paths of one [`swap_future_log`].
pub(super) struct SwapRequest {
    /// The log dir that holds the partition once the swap succeeds.
    pub(super) target_log_dir: PathBuf,
    /// The caught-up future log.
    pub(super) future_log: Arc<Mutex<Log>>,
    /// The future log's `*.future` directory.
    pub(super) future_path: PathBuf,
    /// The canonical partition directory under the target log dir.
    pub(super) target_partition_path: PathBuf,
}

/// Why a swap failed, and whether the partition still has a log to serve.
#[derive(Debug)]
pub(super) enum SwapFailure {
    /// The partition serves its pre-swap log again.
    Restored(BrokerError),
    /// No real log could be reopened: the partition holds an empty placeholder
    /// until restart, and its log dir must be taken offline.
    Offline(BrokerError),
}

/// Run [`swap_future_log`] on the blocking pool, off the writer's async loop.
///
/// A swap that leaves the partition without its log, or that panics part way,
/// marks the source log dir offline, as Kafka does for an `IOException` in
/// `LogManager.replaceCurrentWithFutureLog`. A swap that restored the source
/// log only reports its error.
pub(super) async fn run_swap(
    log: &Arc<Mutex<Log>>,
    log_dir: &Arc<ArcSwap<PathBuf>>,
    registry: &LogDirRegistry,
    request: SwapRequest,
) -> Result<SwapOutcome, BrokerError> {
    let source_dir = log_dir.load_full();
    let swap_log = Arc::clone(log);
    let swap_log_dir = Arc::clone(log_dir);
    let result =
        match spawn_blocking(move || swap_future_log(&swap_log, &swap_log_dir, request)).await {
            Ok(result) => result,
            Err(panic) => Err(SwapFailure::Offline(storage_failure_error(
                "log-dir swap panicked",
                panic,
            ))),
        };
    result.map_err(|failure| match failure {
        SwapFailure::Restored(error) => error,
        SwapFailure::Offline(error) => {
            registry.mark_offline(
                &source_dir,
                &format!("KIP-113 log-dir swap left the partition without a log: {error}"),
            );
            error
        }
    })
}

/// A log's directory and stamp source: what reopening it takes.
struct LogSlot {
    path: PathBuf,
    stamp_source: Option<Arc<dyn StampSource>>,
}

impl LogSlot {
    fn open(&self, config: &LogConfig) -> Result<Log, BrokerError> {
        let mut log = Log::open(&self.path, config.clone())?;
        if let Some(stamp_source) = &self.stamp_source {
            log.set_stamp_source(Arc::clone(stamp_source))?;
        }
        Ok(log)
    }
}

/// KIP-113 intra-broker log-dir swap.
///
/// [`run_swap`] calls this function on the blocking pool. The function holds
/// the partition's `log` mutex for the full rename, so no other appender sees
/// a half-swapped state.
///
/// The future log MUST be caught up: its LEO == the current log's LEO. If a
/// producer added a batch between the caller's catch-up check and this writer
/// cycle, the function reports `NotCaughtUp`. The replicator loop then drains
/// the lag and retries.
///
/// Log end equality is all Kafka checks too
/// (`Partition.runCallbackIfFutureReplicaCaughtUp`). That the future log holds
/// what the current log holds is the replicator's job, as it is the
/// `ReplicaAlterLogDirsThread`'s: see the `future_log` module.
///
/// Every failure after the logs are closed reopens both at their pre-swap
/// directories. Only when the source log cannot be reopened does the function
/// report [`SwapFailure::Offline`].
pub(super) fn swap_future_log(
    log: &Arc<Mutex<Log>>,
    log_dir: &Arc<ArcSwap<PathBuf>>,
    request: SwapRequest,
) -> Result<SwapOutcome, SwapFailure> {
    let SwapRequest {
        target_log_dir,
        future_log,
        future_path,
        target_partition_path,
    } = request;
    // Acquire both logs under the writer's serialization and re-check
    // the caught-up invariant. If the future log fell behind between
    // the caller's check and this cycle, refuse the swap and let the
    // replicator catch up.
    let mut log_guard = lock_log(log);
    let config = log_guard.config_snapshot();
    let current_leo = log_guard.log_end_offset();
    let mut future_guard = lock_log(&future_log);
    if !storage::future_log_swap_admission(current_leo.0, future_guard.log_end_offset().0) {
        return Ok(SwapOutcome::NotCaughtUp);
    }
    let source = LogSlot {
        path: log_guard.dir().to_path_buf(),
        stamp_source: log_guard.stamp_source(),
    };
    let staged = LogSlot {
        path: future_path,
        stamp_source: future_guard.stamp_source(),
    };

    // Release segment file descriptors on both Logs before mutating the
    // filesystem. `Log::close` consumes the value, so both move out via
    // `mem::replace` against throwaway Logs anchored to a sacrificial `*.tomb`
    // directory. Both throwaways open before either log closes, so a failure
    // here leaves the partition as it was.
    let tomb_dir = staged.path.with_extension("krabka-swap-tomb");
    let tombs = fs::create_dir_all(&tomb_dir)
        .map_err(BrokerError::from)
        .and_then(|()| {
            Ok((
                Log::open(&tomb_dir, config.clone())?,
                Log::open(&tomb_dir, config.clone())?,
            ))
        });
    let (current_tomb, future_tomb) = match tombs {
        Ok(tombs) => tombs,
        Err(error) => {
            remove_tomb(&tomb_dir);
            return Err(SwapFailure::Restored(error));
        }
    };
    mem::replace(&mut *log_guard, current_tomb).close();
    mem::replace(&mut *future_guard, future_tomb).close();
    let restore = Restore {
        source: &source,
        staged: &staged,
        config: &config,
        tomb_dir: &tomb_dir,
    };

    // Promote the future dir into the canonical `<topic>-<partition>` slot
    // under the target log.dir. The source dir stays until the promoted log
    // opens, so every failure up to then can go back to it.
    if let Err(error) = fs::rename(&staged.path, &target_partition_path) {
        return Err(restore.reopen(&mut log_guard, &mut future_guard, error.into()));
    }
    let target = LogSlot {
        path: target_partition_path,
        stamp_source: staged.stamp_source.clone(),
    };
    let reopened = match target.open(&config) {
        Ok(reopened) => reopened,
        Err(error) => {
            // Move the future dir back, so the source is the partition's only
            // canonical directory again.
            if let Err(back) = fs::rename(&target.path, &staged.path) {
                tracing::error!(
                    error = %error,
                    rename_back = %back,
                    target = %target.path.display(),
                    "swap_future_log: promoted log failed to open AND could not be \
                     moved back; partition is offline until restart"
                );
                return Err(SwapFailure::Offline(error));
            }
            return Err(restore.reopen(&mut log_guard, &mut future_guard, error));
        }
    };
    // Both the directory and the reopened log change under the log's lock.
    log_dir.store(Arc::new(target_log_dir));
    mem::replace(&mut *log_guard, reopened).close();
    drop(future_guard);
    drop(log_guard);

    if let Err(e) = fs::remove_dir_all(&source.path) {
        tracing::warn!(
            source = %source.path.display(),
            error = %e,
            "swap_future_log: failed to remove source partition dir; \
             partition is live at target, source will be cleaned on next restart"
        );
    }
    remove_tomb(&tomb_dir);
    Ok(SwapOutcome::Swapped)
}

/// Where a failed swap puts the partition back.
struct Restore<'a> {
    source: &'a LogSlot,
    staged: &'a LogSlot,
    config: &'a LogConfig,
    tomb_dir: &'a Path,
}

impl Restore<'_> {
    /// Reopen the source log and the future log in place of the throwaways,
    /// and classify `error` by whether the source came back.
    fn reopen(&self, live: &mut Log, future: &mut Log, error: BrokerError) -> SwapFailure {
        let future_restored = match self.staged.open(self.config) {
            Ok(reopened) => {
                mem::replace(future, reopened).close();
                true
            }
            Err(reopen_err) => {
                tracing::warn!(
                    error = %reopen_err,
                    future = %self.staged.path.display(),
                    "swap_future_log: future log failed to reopen after a failed swap"
                );
                false
            }
        };
        match self.source.open(self.config) {
            Ok(reopened) => {
                mem::replace(live, reopened).close();
                // A throwaway still installed keeps its directory.
                if future_restored {
                    remove_tomb(self.tomb_dir);
                }
                SwapFailure::Restored(error)
            }
            Err(reopen_err) => {
                tracing::error!(
                    error = %error,
                    reopen_error = %reopen_err,
                    source = %self.source.path.display(),
                    "swap_future_log: swap failed AND source reopen failed; \
                     partition is offline until restart"
                );
                SwapFailure::Offline(error)
            }
        }
    }
}

/// Delete the throwaway logs' directory, logging a failure.
fn remove_tomb(tomb_dir: &Path) {
    if let Err(e) = fs::remove_dir_all(tomb_dir)
        && tomb_dir.exists()
    {
        tracing::warn!(
            tomb = %tomb_dir.display(),
            error = %e,
            "swap_future_log: failed to remove the swap placeholder dir"
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            log_dir_status::LogDirRegistry,
            partition_writer::test_support::{FixedStamp, open_log_with_records},
        },
        assert2::check,
        krabka_log::name::stampindex_path,
        std::path::Path,
        tempfile::tempdir,
    };

    struct SwapPaths {
        source_dir: PathBuf,
        target_dir: PathBuf,
        source_partition: PathBuf,
        future_path: PathBuf,
        target_partition_path: PathBuf,
    }

    fn swap_paths(dir: &Path) -> SwapPaths {
        let source_dir = dir.join("source");
        let target_dir = dir.join("target");
        SwapPaths {
            source_partition: source_dir.join("t-0"),
            future_path: target_dir.join("t-0.future"),
            target_partition_path: target_dir.join("t-0"),
            source_dir,
            target_dir,
        }
    }

    fn request(
        future_log: &Arc<Mutex<Log>>,
        target_log_dir: PathBuf,
        future_path: &Path,
        target_partition_path: &Path,
    ) -> SwapRequest {
        SwapRequest {
            target_log_dir,
            future_log: Arc::clone(future_log),
            future_path: future_path.to_path_buf(),
            target_partition_path: target_partition_path.to_path_buf(),
        }
    }

    impl SwapPaths {
        /// A request to swap `future_log` into the target slot.
        fn request(&self, future_log: &Arc<Mutex<Log>>) -> SwapRequest {
            request(
                future_log,
                self.target_dir.clone(),
                &self.future_path,
                &self.target_partition_path,
            )
        }

        /// The partition's log-dir handle, pointing at the source dir.
        fn source_log_dir(&self) -> Arc<ArcSwap<PathBuf>> {
            Arc::new(ArcSwap::from_pointee(self.source_dir.clone()))
        }
    }

    #[test]
    fn swap_future_log_accepts_future_at_same_leo() {
        let dir = tempdir().expect("tempdir");
        let SwapPaths {
            source_dir,
            target_dir,
            source_partition,
            future_path,
            target_partition_path,
        } = swap_paths(dir.path());

        let mut source_log = open_log_with_records(&source_partition, 2);
        source_log
            .set_stamp_source(Arc::new(FixedStamp(7)))
            .expect("source stamp index");
        let log = Arc::new(Mutex::new(source_log));
        let mut staged_log = open_log_with_records(&future_path, 2);
        staged_log
            .set_stamp_source(Arc::new(FixedStamp(7)))
            .expect("future stamp index");
        let future_log = Arc::new(Mutex::new(staged_log));
        let log_dir = Arc::new(ArcSwap::from_pointee(source_dir));

        let result = swap_future_log(
            &log,
            &log_dir,
            request(
                &future_log,
                target_dir.clone(),
                &future_path,
                &target_partition_path,
            ),
        )
        .expect("swap");

        // Pull both log observations under one lock acquisition — two
        // `lock()` temporaries in a single assert statement would deadlock.
        let (leo, log_dir_now, has_stamp_source) = {
            let guard = log.lock().unwrap();
            (
                guard.log_end_offset(),
                guard.dir().to_path_buf(),
                guard.stamp_source().is_some(),
            )
        };
        check!(result == SwapOutcome::Swapped);
        check!(leo == 2);
        check!(log_dir_now == target_partition_path);
        check!(has_stamp_source);
        check!(log_dir.load().as_ref().clone() == target_dir);
        check!(!source_partition.exists());
        check!(target_partition_path.exists());
    }

    #[test]
    fn swap_future_log_rejects_mismatched_leo() {
        for (current_records, future_records) in [(2, 1), (1, 2)] {
            let dir = tempdir().expect("tempdir");
            let SwapPaths {
                source_dir,
                target_dir,
                source_partition,
                future_path,
                target_partition_path,
            } = swap_paths(dir.path());

            let log = Arc::new(Mutex::new(open_log_with_records(
                &source_partition,
                current_records,
            )));
            let future_log = Arc::new(Mutex::new(open_log_with_records(
                &future_path,
                future_records,
            )));
            let log_dir = Arc::new(ArcSwap::from_pointee(source_dir.clone()));
            let result = swap_future_log(
                &log,
                &log_dir,
                request(
                    &future_log,
                    target_dir,
                    &future_path,
                    &target_partition_path,
                ),
            )
            .expect("not caught up response");

            let (leo, log_dir_now) = {
                let guard = log.lock().unwrap();
                (guard.log_end_offset(), guard.dir().to_path_buf())
            };
            check!(result == SwapOutcome::NotCaughtUp);
            check!(leo == i64::from(current_records));
            check!(log_dir_now == source_partition);
            check!(log_dir.load().as_ref().clone() == source_dir);
            check!(source_partition.exists());
            check!(future_path.exists());
            check!(!target_partition_path.exists());
        }
    }

    /// Both logs of a refused swap, observed one lock at a time.
    struct Observed {
        current_dir: PathBuf,
        current_leo: i64,
        current_stamped: bool,
        future_dir: PathBuf,
        future_leo: i64,
    }

    fn observe(log: &Mutex<Log>, future_log: &Mutex<Log>) -> Observed {
        let (current_dir, current_leo, current_stamped) = {
            let guard = log.lock().unwrap();
            (
                guard.dir().to_path_buf(),
                guard.log_end_offset().0,
                guard.stamp_source().is_some(),
            )
        };
        let (future_dir, future_leo) = {
            let guard = future_log.lock().unwrap();
            (guard.dir().to_path_buf(), guard.log_end_offset().0)
        };
        Observed {
            current_dir,
            current_leo,
            current_stamped,
            future_dir,
            future_leo,
        }
    }

    /// Run a swap that must fail and restore the source log, and observe both
    /// logs afterwards.
    fn swap_restoring_the_source(
        paths: &SwapPaths,
        log: &Arc<Mutex<Log>>,
        future_log: &Arc<Mutex<Log>>,
    ) -> Observed {
        let log_dir = paths.source_log_dir();
        let result = swap_future_log(log, &log_dir, paths.request(future_log));
        let observed = observe(log, future_log);
        check!(let Err(SwapFailure::Restored(_)) = result);
        check_source_kept(&observed, paths, &log_dir);
        observed
    }

    /// A non-empty directory in the canonical slot, which refuses the rename.
    fn occupy_target_slot(paths: &SwapPaths) {
        fs::create_dir_all(&paths.target_partition_path).expect("occupied slot");
        fs::write(paths.target_partition_path.join("occupant"), b"x").expect("occupant");
    }

    /// The source log is back in place: same dir, both records, still stamped.
    fn check_source_kept(observed: &Observed, paths: &SwapPaths, log_dir: &ArcSwap<PathBuf>) {
        check!(log_dir.load().as_ref().clone() == paths.source_dir);
        check!(observed.current_dir == paths.source_partition);
        check!(observed.current_leo == 2);
        check!(observed.current_stamped);
    }

    /// Two stamped logs of two records each, at the source and future paths.
    fn stamped_logs(paths: &SwapPaths) -> (Arc<Mutex<Log>>, Arc<Mutex<Log>>) {
        let mut source_log = open_log_with_records(&paths.source_partition, 2);
        source_log
            .set_stamp_source(Arc::new(FixedStamp(7)))
            .expect("source stamp index");
        let mut staged_log = open_log_with_records(&paths.future_path, 2);
        staged_log
            .set_stamp_source(Arc::new(FixedStamp(7)))
            .expect("future stamp index");
        (
            Arc::new(Mutex::new(source_log)),
            Arc::new(Mutex::new(staged_log)),
        )
    }

    #[test]
    fn swap_future_log_restores_both_logs_when_rename_fails() {
        let dir = tempdir().expect("tempdir");
        let paths = swap_paths(dir.path());
        let (log, future_log) = stamped_logs(&paths);
        occupy_target_slot(&paths);

        let observed = swap_restoring_the_source(&paths, &log, &future_log);
        check!(observed.future_dir == paths.future_path);
        check!(observed.future_leo == 2);
        check!(
            !paths
                .future_path
                .with_extension("krabka-swap-tomb")
                .exists()
        );
    }

    #[test]
    fn swap_future_log_restores_source_when_target_open_fails() {
        let dir = tempdir().expect("tempdir");
        let paths = swap_paths(dir.path());
        let (log, future_log) = stamped_logs(&paths);
        // The moved directory cannot be reopened with its stamp source: its
        // stamp index is garbage, which the open future log never reads.
        let stamp_index = stampindex_path(&paths.future_path, 0);
        fs::write(&stamp_index, b"garbage").expect("corrupt stamp index");

        swap_restoring_the_source(&paths, &log, &future_log);
        check!(paths.source_partition.exists());
        check!(paths.future_path.exists());
        check!(!paths.target_partition_path.exists());
    }

    #[tokio::test]
    async fn run_swap_takes_the_source_dir_offline_when_no_log_reopens() {
        let dir = tempdir().expect("tempdir");
        let paths = swap_paths(dir.path());
        let (log, future_log) = stamped_logs(&paths);
        // The rename fails, and the source cannot be reopened with its stamp
        // source either.
        occupy_target_slot(&paths);
        fs::write(stampindex_path(&paths.source_partition, 0), b"garbage")
            .expect("corrupt stamp index");
        let log_dir = paths.source_log_dir();
        let registry = LogDirRegistry::probe(&[paths.source_dir.clone(), paths.target_dir.clone()]);

        let result = run_swap(&log, &log_dir, &registry, paths.request(&future_log)).await;

        check!(let Err(_) = result);
        check!(registry.is_offline(&paths.source_dir));
        check!(!registry.is_offline(&paths.target_dir));
        check!(log_dir.load().as_ref().clone() == paths.source_dir);
    }
}
