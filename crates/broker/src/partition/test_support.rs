//! Fixture builders for unit tests that need a real partition: one over a
//! temporary log directory, one wired to a live writer task, and a helper that
//! appends records straight to the log.
//!
//! [`test_partition`] is `pub(crate)` because the fetch read path's own tests
//! drive [`crate::handlers::fetch`] against a partition too, and a second copy
//! of this fixture would be a second thing to keep in step with `Partition`.
//! [`test_partition_with_writer`] is `pub(crate)` for the same reason: the
//! group coordinator's offsets log appends through a real writer in its tests.
use {
    crate::{
        delivery::DeliveryHandles,
        partition::{
            Partition, WriterGate, WriterMessage, empty_marker_materialization,
            initial_replication_target, unchecked_log_start, unreported_leader_log_start,
        },
        partition_writer::{WriterServices, run},
        replica_state::ReplicaState,
        test_support::{RecordCount, partition_records_batch},
    },
    arc_swap::ArcSwap,
    krabka_ids::PartitionIndex,
    krabka_log::{Log, LogConfig},
    std::{
        sync::{Arc, Mutex},
        thread,
    },
    tempfile::tempdir,
    tokio::{
        sync,
        sync::{Notify, mpsc},
    },
};

// Fails leader-epoch checkpoint writes as a full disk would.
krabka_broker_macros::epoch_checkpoint_failure!(pub(crate) EpochCheckpointFull, StorageFull);

pub fn test_partition(hw_advance_notify: Arc<Notify>) -> (Partition, tempfile::TempDir) {
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
        marker_materialization: empty_marker_materialization(),
        append_notify: Arc::new(Notify::new()),
        replica_state: Arc::new(sync::Mutex::new(ReplicaState::new())),
        hw_advance_notify,
        leadership_notify: Arc::new(Notify::new()),
        writer_gate: WriterGate::shared(false),
        leader_slot: Arc::default(),
        delivery: DeliveryHandles::new(),
        replication_target: initial_replication_target(None),
        diskless: false,
        reported_leader_log_start: unreported_leader_log_start(),
        log_start_checked_epoch: unchecked_log_start(),
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };
    (p, dir)
}

/// Update a test partition's log configuration without copying the log-lock boilerplate.
pub fn update_log_config(partition: &Partition, update: impl FnOnce(&mut LogConfig)) {
    let log = partition.log.lock().expect("partition log lock");
    let mut config = log.config_snapshot();
    update(&mut config);
    log.set_config(config);
}

/// Whether a commit fixture keeps the partition's initial leader or installs the test term.
#[derive(Clone, Copy, Default)]
pub enum CommitFixtureLeader {
    #[default]
    KeepPartitionDefault,
    BrokerOneAtEpochZero,
}

/// A real temporary partition with the requested commit frontier and its own notifier.
#[bon::builder]
pub async fn commit_partition(
    #[builder(into, default)] high_watermark: krabka_log::Offset,
    #[builder(default)] leader: CommitFixtureLeader,
) -> (Arc<Partition>, tempfile::TempDir, Arc<Notify>) {
    let notify = Arc::new(Notify::new());
    let (partition, directory) = test_partition(Arc::clone(&notify));
    let partition = Arc::new(partition);
    if matches!(leader, CommitFixtureLeader::BrokerOneAtEpochZero) {
        partition.install_leader_change(1, 0).await;
    }
    partition.replica_state.lock().await.hw = high_watermark;
    (partition, directory, notify)
}

pub fn test_partition_with_writer() -> (Partition, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let log_dir = Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf()));
    let p = Partition::writer_fixture("t", log, log_dir, |identity, storage, rx, signals| {
        tokio::spawn(run(
            identity,
            storage,
            rx,
            signals,
            WriterServices::default(),
        ))
    });
    (p, dir)
}

/// Project checkpoint rows without choosing the caller's mutex-poison diagnostic.
pub fn epoch_history(log: &Log) -> Vec<(i32, i64)> {
    log.epoch_checkpoint()
        .entries()
        .iter()
        .map(|entry| (entry.epoch.0, entry.start_offset.0))
        .collect()
}

/// Install the three-replica setup; expected ISR rows remain independent in callers.
pub async fn install_three_replica_isr(partition: &Partition) {
    let replicas = [
        krabka_audit::NodeId(1),
        krabka_audit::NodeId(2),
        krabka_audit::NodeId(3),
    ];
    partition
        .install_isr(&replicas, &replicas, krabka_audit::NodeId(1))
        .await;
}

/// Append `count` records, stamped with the batch fixture's default timestamp.
#[bon::builder]
pub(super) fn append_records(
    #[builder(start_fn)] p: &Partition,
    #[builder(into, default)] count: RecordCount,
) {
    let mut batch = partition_records_batch().count(count).call();
    p.log
        .lock()
        .expect("log mutex")
        .append(&mut batch)
        .expect("append");
}

/// Panic on another thread while it holds `partition`'s log, which leaves the
/// mutex poisoned, as a task that panicked mid-append would.
pub fn poison_log(partition: &Partition) {
    let log = Arc::clone(&partition.log);
    let panicked = thread::spawn(move || {
        let _guard = log.lock();
        panic!("poison the log mutex");
    })
    .join();
    assert2::assert!(panicked.is_err());
    assert2::assert!(partition.log.is_poisoned());
}
