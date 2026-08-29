//! KIP-113 (`AlterReplicaLogDirs`): intra-broker log-dir reassignment.
//!
//! When the `AlterReplicaLogDirs` handler accepts a move
//! `(topic, partition) → target log.dir`, it asks this module to:
//!
//! 1. Open a fresh `krabka_log::Log` at
//!    `<target_log_dir>/<topic>-<partition>-future/`.
//! 2. Spawn a per-move replicator task that reads batches from the
//!    partition's current `Log` and appends them to the future log with
//!    `Log::append_at`, which keeps the leader-assigned offsets.
//! 3. Once `future_log.LEO == current_log.LEO`, ask the partition
//!    writer to swap atomically with `WriterMessage::SwapFutureLog`.
//!
//! The on-disk `*-future` directory is the only persisted state. A
//! crash mid-move leaves it behind. Broker startup re-discovers it with
//! `log_dir::scan_future` and re-spawns the replicator.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use krabka_log::Log;
use krabka_units::{ByteSize, Time};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::BrokerError;

mod catch_up;
mod cleanup;
mod replicator;
mod start;
#[cfg(test)]
mod test_support;

pub(crate) use self::{
    cleanup::{abort_moves, shutdown_moves},
    start::{resume_move, start_move},
};

/// One in-progress intra-broker log-dir move. Inserted into
/// `Broker.future_logs` keyed by `(topic, partition)`.
///
/// The struct holds these fields to keep ownership of the future log alive
/// and to let `DescribeLogDirs` and future cancellation paths consult the
/// move's state through the registry. The writer task consumes them
/// indirectly through the `SwapFutureLog` message, which Rust's dead-code
/// pass cannot see through.
#[derive(Debug)]
pub struct FutureLogState {
    /// Parent `log.dir` that the move targets. It is one of the broker's
    /// configured `log.dirs`. The handler uses it to make a duplicate
    /// `AlterReplicaLogDirs` for the same `(topic, partition)`
    /// idempotent, or to reject a conflicting target.
    pub target_log_dir: PathBuf,
    /// The future log's `<target>/<topic>-<partition>-future` path.
    pub future_path: PathBuf,
    /// The future log itself. Shared with the replicator task and the
    /// `SwapFutureLog` writer message so all three hold the same
    /// `Arc<Mutex<Log>>`.
    pub future_log: Arc<Mutex<Log>>,
    /// Cancelled by the swap or by a follow-up `AlterReplicaLogDirs` that
    /// redirects an in-progress move.
    pub cancel: CancellationToken,
    /// Retained so cancellation and broker shutdown can abort and await the
    /// replicator task.
    pub task: std::sync::Mutex<Option<JoinHandle<()>>>,
}

/// Why a [`start_move`] or [`resume_move`] call could not be honoured.
/// The handler translates these to the wire error codes
/// [`crate::codes::LOG_DIR_NOT_FOUND`],
/// [`crate::codes::REPLICA_NOT_AVAILABLE`], and
/// [`crate::codes::KAFKA_STORAGE_ERROR`].
#[derive(Debug)]
pub enum MoveError {
    /// Target path is not one of this broker's configured `log.dirs`.
    LogDirNotFound,
    /// The named partition is not hosted on this broker.
    ReplicaNotAvailable,
    /// `krabka_log::Log::open` or `mkdir` failed while staging the future log.
    /// The handler logs the inner error, then maps every storage failure to
    /// `KAFKA_STORAGE_ERROR` on the wire.
    Storage(BrokerError),
}

impl From<BrokerError> for MoveError {
    fn from(e: BrokerError) -> Self {
        MoveError::Storage(e)
    }
}

impl From<krabka_log::LogError> for MoveError {
    fn from(e: krabka_log::LogError) -> Self {
        MoveError::Storage(BrokerError::from(e))
    }
}

impl From<std::io::Error> for MoveError {
    fn from(e: std::io::Error) -> Self {
        MoveError::Storage(BrokerError::from(e))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MovePolicy {
    pub retry_backoff: Time,
    pub read_chunk: ByteSize,
    pub throttle: Arc<crate::throttle::TokenBucket>,
}

/// Canonicalize a path for equality comparisons. It falls back to the
/// lexical path when canonicalisation fails, because the directory may not
/// exist yet. That is correct for log-dir comparisons, because this code also
/// compares against the configured value.
fn canonicalize_or_self(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{kibibytes, millis};

    use super::*;

    #[test]
    fn move_policy_preserves_nondefault_values() {
        let policy = MovePolicy {
            retry_backoff: millis(7),
            read_chunk: kibibytes(4),
            throttle: Arc::new(crate::throttle::TokenBucket::new()),
        };

        assert!(policy.retry_backoff == millis(7));
        assert!(policy.read_chunk == kibibytes(4));
        assert!(policy.throttle.byte_rate() == krabka_units::bytes_per_sec(0));
    }
}
