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
#[cfg(any(test, feature = "test-helpers"))]
use crate::partition_writer::{WriterIdentity, WriterSignals, WriterStorage};
// `std::sync::Mutex` is kept for `log` (sync hot-path callers);
// `replica_state` uses `tokio::sync::Mutex` to avoid blocking worker threads.
use {
    crate::{
        config::BrokerConfig,
        diskless::hot_tail::HotTailCache,
        txn::{handlers::write_txn_markers::CommittedOffsets, marker::MarkerType},
        wal::{OffsetSequencer, quorum::registry::WalShardRegistry},
    },
    arc_swap::ArcSwap,
    krabka_ids::{LeaderEpoch, NodeId, PartitionIndex},
    krabka_log::{Log, Offset, ReadOutput},
    krabka_units::ByteSize,
    std::{
        path::PathBuf,
        sync::{
            Arc, Mutex, MutexGuard,
            atomic::{AtomicI32, AtomicI64, AtomicU64},
        },
    },
    tokio::{
        sync::{Notify, mpsc},
        task::JoinHandle,
    },
};
use {
    std::{collections::HashMap, sync::PoisonError},
    tokio::{sync, sync::RwLock},
};

/// Per producer, the offset resolution a durable `__consumer_offsets` marker
/// still owes the group actors: the marker type, the marker's offset, and
/// the offsets the transaction wrote.
pub type OwedMarkerResolutions =
    HashMap<krabka_log::ProducerId, (MarkerType, i64, CommittedOffsets)>;
use crate::{delivery::DeliveryHandles, error::BrokerError, replica_state::ReplicaState};

mod commands;
mod commit_wait;
mod follower_progress;
mod leadership;
mod messages;
mod watermark;
mod writer_gate;

pub use self::{follower_progress::FollowerPosition, writer_gate::WriterGate};

#[cfg(test)]
pub mod test_support;

#[cfg(any(test, feature = "test-helpers"))]
pub use self::leadership::initial_replication_target;
// Only watermark's own tests name crate::partition::HwTimeout.
#[cfg(test)]
pub use self::watermark::HwTimeout;
pub use self::{
    commands::ProduceBatchError,
    commit_wait::Uncommitted,
    leadership::ReplicationTarget,
    messages::{
        AppendOrigin, AppendedBatch, EncodedBatch, ProduceData, ProduceJob, ProducerAppendCheck,
        ReplicatedBatch, SwapOutcome, WriterMessage, verbatim_of,
    },
};

/// Absolute record offset within a partition's log (base offset, log end
/// offset, high watermark, truncation points, …). This is an alias only. It
/// shows which `i64`s in signatures are offsets and not timestamps or counts.
pub type LogOffset = i64;

/// Writer sizing and diskless services shared by startup and first-touch materialization.
pub struct PartitionRuntimeConfig {
    pub max_produce_group: usize,
    pub partition_writer_queue_depth: usize,
    pub diskless_wal_local_replica_count: usize,
    pub diskless: bool,
    pub hot_tail: Option<Arc<HotTailCache>>,
    pub wal_shards: Option<Arc<WalShardRegistry>>,
    pub sequencer: Option<Arc<dyn OffsetSequencer>>,
}

type PartitionWalServices = (
    Option<Arc<HotTailCache>>,
    Option<Arc<WalShardRegistry>>,
    Option<Arc<dyn OffsetSequencer>>,
);

impl PartitionRuntimeConfig {
    pub(crate) fn new(
        writer_sizes: (usize, usize, usize),
        diskless: bool,
        services: PartitionWalServices,
    ) -> Self {
        let (max_produce_group, partition_writer_queue_depth, diskless_wal_local_replica_count) =
            writer_sizes;
        let (hot_tail, wal_shards, sequencer) = services;
        Self {
            max_produce_group,
            partition_writer_queue_depth,
            diskless_wal_local_replica_count,
            diskless,
            hot_tail,
            wal_shards,
            sequencer,
        }
    }

    pub(crate) fn from_broker(
        config: &BrokerConfig,
        diskless: bool,
        services: PartitionWalServices,
    ) -> Self {
        Self::new(
            (
                config.max_produce_group,
                config.partition_writer_queue_depth,
                config.diskless_wal_local_replica_count,
            ),
            diskless,
            services,
        )
    }
}

/// The leader and leader epoch the metadata image names for a partition.
///
/// Atomic for lock-free reads in the Produce and Fetch hot paths. The broker
/// stamps the epoch on every appended batch and validates it on every follower
/// Fetch. Only the metadata listener moves them, when it publishes a
/// replication target; everything else reads them through [`Partition`].
#[derive(Debug, Default)]
pub struct LeaderSlot {
    node: AtomicU64,
    epoch: AtomicI32,
}

impl LeaderSlot {
    /// A slot that names `node` as the leader at `epoch`.
    #[must_use]
    pub const fn new(node: NodeId, epoch: LeaderEpoch) -> Self {
        Self {
            node: AtomicU64::new(node.get()),
            epoch: AtomicI32::new(epoch.get()),
        }
    }
}

/// Runtime handle for a single partition.
///
/// Cheap to clone. `log`, `writer_tx`, and `append_notify` are all `Arc`-ish,
/// and the writer handle is not cloned because `Arc<JoinHandle<()>>` wraps it.
#[derive(Clone)]
// `partition_id` mirrors Kafka's wire naming and is the conventional term
// used throughout the broker; renaming to `id` would shadow `Partition`'s
// own identity at every call site.
//
// `Debug` deliberately leaves out `log`: formatting a `Mutex<Log>` would block
// on the mutex and dump internal segment state into tracing output.
#[derive(derive_more::Debug)]
#[debug("Partition {{ topic: {topic:?}, partition_id: {index:?}, delivery: {delivery:?}, .. }}")]
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
    /// Lets a produce append on its own task while the writer is idle; see
    /// [`WriterGate`].
    pub(crate) writer_gate: Arc<WriterGate>,
    /// Serializes transaction-marker admission, append, and any offset
    /// resolution for this partition. Holds, per producer, the resolution a
    /// durable `__consumer_offsets` marker still owes the group actors: the
    /// marker type, its offset, and the offsets the transaction wrote.
    pub(crate) marker_materialization: Arc<sync::Mutex<OwedMarkerResolutions>>,
    pub append_notify: Arc<Notify>,
    pub(crate) replica_state: Arc<sync::Mutex<ReplicaState>>,
    pub hw_advance_notify: Arc<Notify>,
    /// Fired when the partition's leader or leader epoch changes, after
    /// `hw_advance_notify`. A parked follower fetch waits on this rather than
    /// on `hw_advance_notify`: it must answer `NOT_LEADER_OR_FOLLOWER` at once
    /// on a leader change, but a high-watermark advance has nothing for it,
    /// and waking it on every advance re-read the partition for nothing on the
    /// same broadcast that completes `acks=all` produces.
    pub(crate) leadership_notify: Arc<Notify>,
    /// Deliver-at-time state: the lock-free delivery-watermark mirror, the
    /// wake a long poll parks on until a scheduled batch comes due, and the
    /// slot the broker-wide delivery scheduler installs its poke into.
    ///
    /// The writer actor holds a clone of the same handles, so an append
    /// refreshes the mirror without reaching back through this struct.
    pub(crate) delivery: DeliveryHandles,
    /// The leader and leader epoch the metadata image names. Read them with
    /// [`Self::leader`], [`Self::leader_epoch`] and [`Self::is_leader_at`].
    /// A clone of the partition shares the slot.
    pub(crate) leader_slot: Arc<LeaderSlot>,
    /// Serializes follower mutations against topic recreation and local
    /// leader/epoch installation. Replication holds a read guard through the
    /// writer acknowledgement; metadata reconciliation takes the write guard.
    pub(crate) replication_target: Arc<RwLock<ReplicationTarget>>,
    /// True for diskless runtime partitions whose client-visible HW may only
    /// advance through the WAL durable-sync path.
    pub(crate) diskless: bool,
    /// The last log start this replica's leader reported below the floor this
    /// replica established, or -1 when there is none. The replicator records
    /// it rather than lowering the floor, and the remote-log manager's follower
    /// pass lowers the floor to it only when the archive does not enforce the
    /// floor: see `remote_log_manager::follow_unenforced_floor`.
    pub(crate) reported_leader_log_start: Arc<AtomicI64>,
    /// The leader epoch in which the remote-log manager last checked this
    /// replica's log start against what the tiers hold, or -1 when it has not
    /// checked one. Kafka checks once per leadership: see
    /// `remote_log_manager::raise_start_over_unheld_offsets`.
    pub(crate) log_start_checked_epoch: Arc<AtomicI64>,
    /// Retained so broker shutdown can abort and await the writer task after
    /// all request handlers have drained.
    pub(crate) writer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

/// New partitions begin with no leader log start recorded below their floor.
pub fn unreported_leader_log_start() -> Arc<AtomicI64> { Arc::new(AtomicI64::new(-1)) }

/// New partitions begin with their log start unchecked in every leader epoch.
pub fn unchecked_log_start() -> Arc<AtomicI64> { Arc::new(AtomicI64::new(-1)) }

/// New partitions begin with no transaction-marker resolutions owed to group actors.
pub fn empty_marker_materialization() -> Arc<sync::Mutex<OwedMarkerResolutions>> {
    Arc::new(sync::Mutex::new(HashMap::default()))
}

impl Partition {
    /// The complete checkpoint history, without changing the test's independent expected rows.
    #[cfg(test)]
    pub(crate) fn epoch_history(&self) -> Vec<(i32, i64)> {
        test_support::epoch_history(&self.log.lock().unwrap())
    }

    /// A disk-backed fixture with the ordinary eight-slot writer channel and zeroed leader state.
    /// The caller opens the log and selects the exact writer entry point; the writer and partition
    /// share the delivery handles whose mirror the writer refreshes.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn writer_fixture(
        topic: &str,
        log: Arc<Mutex<Log>>,
        log_dir: Arc<ArcSwap<PathBuf>>,
        spawn: impl FnOnce(
            WriterIdentity,
            WriterStorage,
            mpsc::Receiver<WriterMessage>,
            WriterSignals,
        ) -> JoinHandle<()>,
    ) -> Self {
        let (writer_tx, rx) = mpsc::channel::<WriterMessage>(8);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(sync::Mutex::new(ReplicaState::new()));
        let hw_advance_notify = Arc::new(Notify::new());
        let delivery = DeliveryHandles::new();
        let writer = spawn(
            WriterIdentity {
                topic: topic.to_owned(),
                partition: PartitionIndex(0),
            },
            WriterStorage {
                log: Arc::clone(&log),
                log_dir: Arc::clone(&log_dir),
            },
            rx,
            WriterSignals {
                append_notify: Arc::clone(&append_notify),
                replica_state: Arc::clone(&replica_state),
                hw_advance_notify: Arc::clone(&hw_advance_notify),
                delivery: delivery.clone(),
            },
        );
        Self {
            topic: topic.to_owned(),
            index: PartitionIndex(0),
            log_dir,
            log,
            writer_tx,
            writer_gate: WriterGate::shared(false),
            marker_materialization: empty_marker_materialization(),
            append_notify,
            replica_state,
            hw_advance_notify,
            leadership_notify: Arc::new(Notify::new()),
            leader_slot: Arc::default(),
            delivery,
            replication_target: initial_replication_target(None),
            diskless: false,
            reported_leader_log_start: unreported_leader_log_start(),
            log_start_checked_epoch: unchecked_log_start(),
            writer_handle: Arc::new(Mutex::new(Some(writer))),
        }
    }

    /// The partition's [`Log`], locked. Every `Partition` query on the log
    /// goes through here, so each answers a poisoned mutex the same way.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::PartitionWriterDied`] if the mutex is poisoned,
    /// that is, if a task panicked while it held the log. A query does not
    /// read what that task left half-written. The error maps to
    /// `NOT_LEADER_OR_FOLLOWER`, so a client retries against the next leader.
    pub(crate) fn lock_log(&self) -> Result<MutexGuard<'_, Log>, BrokerError> {
        self.log
            .lock()
            .map_err(|_| BrokerError::PartitionWriterDied {
                topic: self.topic.clone(),
                partition: self.index.0,
            })
    }

    /// Next offset the underlying [`Log`] will assign. Cheap: takes the
    /// `Arc<Mutex<Log>>` briefly. Replicators call this before each Fetch
    /// to compute `fetch_offset`.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::PartitionWriterDied`] if the log mutex is
    /// poisoned; see [`Partition::lock_log`].
    pub fn log_end_offset(&self) -> Result<Offset, BrokerError> {
        Ok(self.lock_log()?.log_end_offset())
    }

    /// Last Stable Offset at `high_watermark`: Kafka's
    /// `UnifiedLog.lastStableOffset`. A transaction that is open, or complete
    /// with a marker that `high_watermark` has not passed, holds it at the
    /// transaction's first offset. See [`Log::last_stable_offset`].
    /// Cheap: takes the `Arc<Mutex<Log>>` briefly.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::PartitionWriterDied`] if the log mutex is
    /// poisoned; see [`Partition::lock_log`].
    pub fn last_stable_offset(&self, high_watermark: Offset) -> Result<Offset, BrokerError> {
        Ok(self.lock_log()?.last_stable_offset(high_watermark))
    }

    /// First absolute offset still present in the underlying [`Log`].
    /// Cheap: takes the `Arc<Mutex<Log>>` briefly. `TxnCoordinator::recover`
    /// uses this to seed the replay scan offset.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::PartitionWriterDied`] if the log mutex is
    /// poisoned; see [`Partition::lock_log`].
    pub(crate) fn log_start_offset(&self) -> Result<Offset, BrokerError> {
        Ok(self.lock_log()?.log_start_offset())
    }

    /// The log start and log end offsets, read under one hold of the lock, for
    /// a caller that replays everything the log holds.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::PartitionWriterDied`] if the log mutex is
    /// poisoned; see [`Partition::lock_log`].
    pub(crate) fn log_offsets(&self) -> Result<(Offset, Offset), BrokerError> {
        let log = self.lock_log()?;
        Ok((log.log_start_offset(), log.log_end_offset()))
    }

    /// The additional internal stamp coordinate that covers `offset`. Returns
    /// `None` when this partition is unstamped, that is, when no
    /// [`krabka_log::StampSource`] is injected, or when no stamped range
    /// covers `offset`.
    ///
    /// Locks the `Arc<Mutex<Log>>` briefly. This is a server-side query only.
    /// No produce or fetch handler consults it, so the stamp cannot leak into
    /// any client-facing response. Returns `None` too if the mutex is
    /// poisoned; see [`Partition::lock_log`].
    #[cfg(test)]
    #[must_use]
    pub fn stamp_for_offset(&self, offset: Offset) -> Option<u64> {
        self.lock_log().ok()?.stamp_for_offset(offset)
    }

    /// Fence the log and then remove and return the writer task handle, for a
    /// caller that is about to delete the partition's directory.
    ///
    /// This is the one place that takes a poisoned log rather than failing:
    /// the fence is how this partition fails closed, so it must land on a
    /// log some task panicked over too.
    ///
    /// An append can run on a request's own task while the writer is idle
    /// (see [`WriterGate`]), so aborting the writer does not stop every
    /// writer of the log. The fence does: it is taken under the log's mutex,
    /// so an append either finished before it or finds the log fenced and is
    /// answered `NOT_LEADER_OR_FOLLOWER`, as a produce to a gone writer was.
    pub(crate) fn retire(&self) -> Option<JoinHandle<()>> {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fence();
        self.take_writer_handle()
    }

    /// Remove and return the writer task handle exactly once. Broker shutdown
    /// uses this after request handlers drain, then aborts and awaits the task.
    pub(crate) fn take_writer_handle(&self) -> Option<JoinHandle<()>> {
        self.writer_handle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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
    /// for example when `offset < log_start_offset()`, and
    /// [`BrokerError::PartitionWriterDied`] if the log mutex is poisoned; see
    /// [`Partition::lock_log`].
    pub(crate) fn read_log(
        &self,
        offset: Offset,
        max_size: ByteSize,
    ) -> Result<ReadOutput, BrokerError> {
        self.lock_log()?
            .read(offset, max_size)
            .map_err(BrokerError::from)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            partition::test_support::{append_records, poison_log, test_partition},
            test_support::{PartitionRecordsSetup, RecordCount},
        },
        assert2::{assert, check},
        tokio::sync::Notify,
    };

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
            fn next_stamp(&self) -> u64 { self.0 }
        }

        let (p, _dir) = test_partition(Arc::new(Notify::new()));
        p.log
            .lock()
            .expect("log mutex")
            .set_stamp_source(Arc::new(FixedStamp(4242)))
            .expect("set stamp source");
        append_records(&p, PartitionRecordsSetup {
            count: RecordCount(3),
            ..Default::default()
        }); // offsets 0..=2, each stamped 4242

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
        let (p, _dir) = test_partition(Arc::new(Notify::new()));
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

    /// Every query on a poisoned log answers `PartitionWriterDied` for this
    /// partition. None of them answers offset 0, which is a real offset and
    /// would send a replicator, a transaction-state replay or a
    /// `read_committed` fetch back to the start of the log.
    #[tokio::test]
    async fn every_query_on_a_poisoned_log_reports_the_writer_died() {
        let (p, _dir) = test_partition(Arc::new(Notify::new()));
        append_records(&p, PartitionRecordsSetup {
            count: RecordCount(3),
            ..Default::default()
        });
        poison_log(&p);
        let died = Err(BrokerError::PartitionWriterDied {
            topic: p.topic.clone(),
            partition: p.index.0,
        }
        .to_string());
        let shown = |answer: Result<(), BrokerError>| answer.map_err(|e| e.to_string());
        let answers = [
            ("lock_log", shown(p.lock_log().map(drop))),
            ("log_end_offset", shown(p.log_end_offset().map(drop))),
            (
                "last_stable_offset",
                shown(p.last_stable_offset(Offset(3)).map(drop)),
            ),
            ("log_start_offset", shown(p.log_start_offset().map(drop))),
            ("log_offsets", shown(p.log_offsets().map(drop))),
            (
                "read_log",
                shown(p.read_log(Offset(0), krabka_units::mebibytes(1)).map(drop)),
            ),
        ];
        for (query, answer) in answers {
            check!(answer == died, "{query}");
        }
    }
}
