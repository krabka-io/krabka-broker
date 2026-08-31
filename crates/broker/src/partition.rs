//! A single partition's runtime handle. Owned by the partition registry
//! inside `Broker`. The handle gives any task:
//!
//! - read access to the partition's [`Log`] through `Arc<Mutex<Log>>`
//! - write access through a `mpsc::Sender<WriterMessage>`. A single writer
//!   task drains the channel; see `partition_writer.rs`.
//! - a [`Notify`] that fires after every successful append. Long-poll Fetch
//!   uses it to wake when new data arrives.
//! - the partition's deliver-at-time state: a lock-free mirror of the delivery
//!   watermark and a second [`Notify`] that fires when a scheduled batch comes
//!   due. See [`crate::delivery`].

use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicU64},
    },
};

use arc_swap::ArcSwap;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, Offset, ReadOutput};
use krabka_units::ByteSize;
use tokio::{
    sync::{Notify, mpsc},
    task::JoinHandle,
};

// `std::sync::Mutex` is kept for `log` (sync hot-path callers);
// `replica_state` uses `tokio::sync::Mutex` to avoid blocking worker threads.
use crate::{delivery::DeliveryHandles, error::BrokerError, replica_state::ReplicaState};

mod commands;
mod leadership;
mod messages;
mod watermark;

#[cfg(test)]
pub(crate) mod test_support;

pub use self::messages::{ProduceData, ProduceJob, SwapOutcome, WriterMessage};
// Only watermark's own tests name crate::partition::HwTimeout.
#[cfg(test)]
pub use self::watermark::HwTimeout;
pub(crate) use self::{
    commands::ProduceBatchError,
    leadership::{ReplicationTarget, initial_replication_target},
};

/// Absolute record offset within a partition's log (base offset, log end
/// offset, high watermark, truncation points, …). This is an alias only. It
/// shows which `i64`s in signatures are offsets and not timestamps or counts.
pub type LogOffset = i64;

/// Runtime handle for a single partition.
///
/// Cheap to clone. `log`, `writer_tx`, and `append_notify` are all `Arc`-ish,
/// and the writer handle is not cloned because `Arc<JoinHandle<()>>` wraps it.
#[derive(Clone)]
// `partition_id` mirrors Kafka's wire naming and is the conventional term
// used throughout the broker; renaming to `id` would shadow `Partition`'s
// own identity at every call site.
pub struct Partition {
    pub topic: String,
    pub index: PartitionIndex,
    /// Parent `log.dir` that currently owns the partition. This is the parent
    /// of `log.lock().dir()`, that is, the configured directory and not the
    /// `<topic>-<partition>` subdirectory. Updated by
    /// [`WriterMessage::SwapFutureLog`] as the last step of a KIP-113
    /// move. It is an `ArcSwap` so that readers, such as `DescribeLogDirs`
    /// and `AlterReplicaLogDirs` validation, see the swap atomically
    /// without the `log` mutex.
    pub log_dir: Arc<ArcSwap<PathBuf>>,
    pub log: Arc<Mutex<Log>>,
    pub writer_tx: mpsc::Sender<WriterMessage>,
    pub append_notify: Arc<Notify>,
    pub(crate) replica_state: Arc<tokio::sync::Mutex<ReplicaState>>,
    pub hw_advance_notify: Arc<Notify>,
    /// Deliver-at-time state: the lock-free delivery-watermark mirror, the
    /// wake a long poll parks on until a scheduled batch comes due, and the
    /// slot the broker-wide delivery scheduler installs its poke into.
    ///
    /// The writer actor holds a clone of the same handles, so an append
    /// refreshes the mirror without reaching back through this struct.
    pub(crate) delivery: DeliveryHandles,
    /// Current leader's `NodeId` from the metadata image. Atomic for
    /// lock-free reads in the Produce/Fetch hot paths.
    pub current_leader: Arc<AtomicU64>,
    /// Current `leader_epoch` from the metadata image. The broker stamps it on
    /// every appended batch and validates it on every follower Fetch.
    pub current_leader_epoch: Arc<AtomicI32>,
    /// Serializes follower mutations against topic recreation and local
    /// leader/epoch installation. Replication holds a read guard through the
    /// writer acknowledgement; metadata reconciliation takes the write guard.
    pub(crate) replication_target: Arc<tokio::sync::RwLock<ReplicationTarget>>,
    /// True for Slice 1 diskless partitions whose client-visible HW may only
    /// advance through the WAL durable-sync path.
    pub(crate) diskless: bool,
    /// Retained so broker shutdown can abort and await the writer task after
    /// all request handlers have drained.
    pub(crate) writer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Partition {
    /// Next offset the underlying [`Log`] will assign. Cheap: takes the
    /// `Arc<Mutex<Log>>` briefly. Replicators call this before each Fetch
    /// to compute `fetch_offset`.
    ///
    /// Returns 0 if the log mutex is poisoned, that is, if the writer task
    /// panicked. The caller treats that as no progress, and the
    /// writer-died path later reports a clearer error.
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        match self.log.lock() {
            Ok(g) => g.log_end_offset(),
            Err(_) => Offset(0),
        }
    }

    /// Last Stable Offset: the highest offset at or before which all records
    /// in all in-flight transactions have been resolved (committed or aborted).
    /// Cheap: takes the `Arc<Mutex<Log>>` briefly.
    ///
    /// Returns 0 if the log mutex is poisoned, that is, if the writer task
    /// panicked. The caller treats that as no progress, and the
    /// writer-died path later reports a clearer error.
    #[must_use]
    pub fn lso(&self) -> Offset {
        match self.log.lock() {
            Ok(g) => g.lso(),
            Err(_) => Offset(0),
        }
    }

    /// First absolute offset still present in the underlying [`Log`].
    /// Cheap: takes the `Arc<Mutex<Log>>` briefly.
    ///
    /// Returns 0 if the log mutex is poisoned, that is, if the writer task
    /// panicked. `TxnCoordinator::recover` uses this to seed the replay scan
    /// offset.
    #[must_use]
    pub(crate) fn log_start_offset(&self) -> Offset {
        match self.log.lock() {
            Ok(g) => g.log_start_offset(),
            Err(_) => Offset(0),
        }
    }

    /// The additional internal stamp coordinate that covers `offset`. Returns
    /// `None` when this partition is unstamped, that is, when no
    /// [`krabka_log::StampSource`] is injected, or when no stamped range
    /// covers `offset`.
    ///
    /// Locks the `Arc<Mutex<Log>>` briefly. This is a server-side query only.
    /// No produce or fetch handler consults it, so the stamp cannot leak into
    /// any client-facing response. Returns `None` if the mutex is poisoned.
    #[cfg(test)]
    #[must_use]
    pub fn stamp_for_offset(&self, offset: Offset) -> Option<u64> {
        match self.log.lock() {
            Ok(g) => g.stamp_for_offset(offset),
            Err(_) => None,
        }
    }

    /// Remove and return the writer task handle exactly once. Broker shutdown
    /// uses this after request handlers drain, then aborts and awaits the task.
    pub(crate) fn take_writer_handle(&self) -> Option<JoinHandle<()>> {
        self.writer_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// Read batches from the underlying [`Log`] that start at `offset`, and
    /// return up to `max_size` of data.
    ///
    /// Locks the `Arc<Mutex<Log>>` for the duration of the read.
    /// `TxnCoordinator::recover` uses this to replay `__transaction_state`
    /// records.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Log`] if the underlying [`Log::read`] fails,
    /// for example when `offset < log_start_offset()`.
    pub(crate) fn read_log(
        &self,
        offset: Offset,
        max_size: ByteSize,
    ) -> Result<ReadOutput, BrokerError> {
        self.log
            .lock()
            .map_err(|_| BrokerError::Txn("log mutex poisoned".into()))?
            .read(offset, max_size)
            .map_err(BrokerError::from)
    }
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately does NOT include `log` — formatting a `Mutex<Log>`
        // would block on the mutex and dump internal segment state into
        // tracing output.
        f.debug_struct("Partition")
            .field("topic", &self.topic)
            .field("partition_id", &self.index)
            .field("delivery", &self.delivery)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, AtomicU64};

    use assert2::{assert, check};
    use krabka_log::LogConfig;
    use tempfile::tempdir;
    use tokio::sync::Notify;

    use super::*;
    use crate::partition::test_support::{append_records, test_partition};

    /// `Partition::stamp_for_offset` returns the log's actual stamp for a
    /// covered offset and `None` beyond the stamped range. It is not a
    /// constant. A distinctive stamp (`4242`) pins the delegated value, so
    /// the test catches a mutant that hard-codes `Some(0)`, `Some(1)`, or
    /// `None`.
    #[tokio::test]
    async fn stamp_for_offset_delegates_actual_stamp() {
        #[derive(Debug)]
        struct FixedStamp(u64);
        impl krabka_log::StampSource for FixedStamp {
            fn next_stamp(&self) -> u64 {
                self.0
            }
        }

        let (p, _dir) = test_partition(Arc::new(Notify::new()));
        p.log
            .lock()
            .expect("log mutex")
            .set_stamp_source(Arc::new(FixedStamp(4242)))
            .expect("set stamp source");
        append_records(&p, 3); // offsets 0..=2, each stamped 4242

        check!(p.stamp_for_offset(Offset(0)) == Some(4242));
        check!(p.stamp_for_offset(Offset(2)) == Some(4242));
        check!(p.stamp_for_offset(Offset(3)) == None); // beyond the stamped range
    }

    #[test]
    fn partition_is_clone_and_send() {
        // Compile-time check.
        fn assert_send<T: Send>() {}
        fn assert_clone<T: Clone>() {}
        assert_send::<Partition>();
        assert_clone::<Partition>();
    }

    #[tokio::test]
    async fn debug_does_not_dump_log() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let p = Partition {
            topic: "t".into(),
            index: PartitionIndex(0),
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state: Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            hw_advance_notify: Arc::new(Notify::new()),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            delivery: DeliveryHandles::new(),
            replication_target: initial_replication_target(None),
            diskless: false,
            writer_handle: Arc::new(Mutex::new(Some(writer))),
        };
        let s = format!("{p:?}");
        // topic/partition_id appear; the mutex/log internals must NOT appear
        // in Debug output.
        let cases = [
            ("topic", true),
            ("partition_id", true),
            ("Mutex", false),
            ("segments", false),
        ];
        for (needle, expected) in cases {
            assert!(s.contains(needle) == expected, "needle {needle:?} in {s:?}");
        }
    }
}
