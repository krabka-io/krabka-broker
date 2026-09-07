//! One tick, two partitions, one of them stuck: what the copier bound buys.
//!
//! The sweep used to walk the partition snapshot serially, so a partition
//! whose upload hung held every partition behind it for as long as the object
//! store cared to hold the connection -- and, because local retention only
//! evicts what the tier already holds, their disks kept filling while they
//! waited. These cases pin the property that replaces that walk: a copy that
//! is parked is one copier slot, not the whole tick.

use std::{
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
    time::Instant,
};

use assert2::check;
use krabka_remote_storage::{
    CustomMetadata, IndexType, LogSegmentData, RemoteLogSegmentMetadata, RemoteStorageError,
};

use super::*;

/// How long a parked copy waits for the other partition before it gives up.
///
/// It is a failure deadline, not a delay: on a sweep that reaches both
/// partitions the gate opens in microseconds. It exists so a regression to a
/// serial sweep fails this suite in a second instead of hanging it forever.
const GATE_WAIT: Duration = Duration::from_secs(2);

/// The partition whose copies park.
const BLOCKED_PARTITION: i32 = 0;

/// An RSM that parks every copy of [`BLOCKED_PARTITION`] until some *other*
/// partition's copy has finished, and answers every other partition at once.
///
/// The park is a blocking wait inside `copy_log_segment_data`, which is
/// exactly where a stalled object store blocks: the copy path hands the SPI to
/// the blocking pool, so nothing in the sweep can make progress on that
/// partition until the call returns.
struct GatedRsm {
    /// Set once a copy for a partition other than [`BLOCKED_PARTITION`] has
    /// finished.
    gate: Mutex<bool>,
    opened: Condvar,
    /// Whether a parked copy ever gave up waiting, which is what a serial
    /// sweep leaves behind: nothing else can run to open the gate.
    gave_up: AtomicBool,
    /// Copies the store completed for partitions that were never parked.
    unblocked_copies: AtomicUsize,
}

impl GatedRsm {
    fn new() -> Self {
        Self {
            gate: Mutex::new(false),
            opened: Condvar::new(),
            gave_up: AtomicBool::new(false),
            unblocked_copies: AtomicUsize::new(0),
        }
    }
}

impl RemoteStorageManager for GatedRsm {
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        _data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        if metadata
            .remote_log_segment_id()
            .topic_id_partition
            .partition
            == BLOCKED_PARTITION
        {
            let open = self.gate.lock().expect("gate mutex poisoned");
            let (_open, wait) = self
                .opened
                .wait_timeout_while(open, GATE_WAIT, |open| !*open)
                .expect("gate mutex poisoned");
            if wait.timed_out() {
                self.gave_up.store(true, Ordering::Relaxed);
                return Err(RemoteStorageError::Io(std::io::Error::other(
                    "no other partition copied while this one was parked",
                )));
            }
            return Ok(None);
        }
        self.unblocked_copies.fetch_add(1, Ordering::Relaxed);
        *self.gate.lock().expect("gate mutex poisoned") = true;
        self.opened.notify_all();
        Ok(None)
    }
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        _start: u32,
        _end: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        Err(RemoteStorageError::SegmentNotFound(
            metadata.remote_log_segment_id().clone(),
        ))
    }
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        _index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        Err(RemoteStorageError::SegmentNotFound(
            metadata.remote_log_segment_id().clone(),
        ))
    }
    fn delete_log_segment_data(
        &self,
        _metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        Ok(())
    }
}

/// The `orders` topic with `partitions` partitions, so a tick has more than
/// one partition to reach.
fn image_with_orders_partitions(partitions: i32) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::from_u128(9));
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "orders".into(),
        topic_id: tp().topic_id,
        partitions,
        replication_factor: 1,
    }));
    image
}

/// `orders-index`, led by this broker, with sealed segments the tier lacks.
fn tiered_partition_at(index: i32, log_dir: &std::path::Path) -> Arc<Partition> {
    crate::remote_log_manager::test_support::rolled_tiered_partition_at(
        PartitionIndex(index),
        log_dir,
        LogConfig {
            segment_size: bytes(256),
            remote_storage_enable: true,
            retention: None,
            retention_size: None,
            ..LogConfig::default()
        },
    )
}

/// The head-of-line block this issue is about. Partition 0's copy parks
/// inside the object store; partition 1's copy has to finish anyway, in the
/// same tick, without waiting for it.
///
/// The gate is what makes that a claim about ordering rather than about
/// wall-clock timing: partition 0 cannot return until partition 1 has copied,
/// so the tick can only end at all if the two ran concurrently. A serial
/// sweep leaves `gave_up` set, because nothing else could run.
#[tokio::test(flavor = "multi_thread")]
async fn a_parked_copy_does_not_hold_the_next_partitions_copy() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let partitions = PartitionRegistry::new();
    partitions.insert(
        "orders".into(),
        PartitionIndex(0),
        tiered_partition_at(0, first_dir.path()),
    );
    partitions.insert(
        "orders".into(),
        PartitionIndex(1),
        tiered_partition_at(1, second_dir.path()),
    );

    let gated = Arc::new(GatedRsm::new());
    let rsm: Arc<dyn RemoteStorageManager> = gated.clone();
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let controller = fixed_source(image_with_orders_partitions(2));

    let started = Instant::now();
    tick_all(
        &partitions,
        &controller,
        &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        NodeId(1),
        1,
        SweepConcurrency::default(),
    )
    .await;
    let elapsed = started.elapsed();

    check!(
        !gated.gave_up.load(Ordering::Relaxed),
        "a parked copy waited out the whole tick: no other partition was copied while it sat \
         in the store"
    );
    check!(
        elapsed < GATE_WAIT,
        "the tick took {elapsed:?}, so the parked partition held it"
    );
    check!(
        gated.unblocked_copies.load(Ordering::Relaxed) >= 1,
        "the second partition never reached the store"
    );
    // Both partitions finished: the parked one resumes the moment the gate
    // opens, so a concurrent sweep costs it nothing but the wait.
    for index in [0, 1] {
        let listed = rlmm
            .list_remote_log_segments(&TopicIdPartition::new(tp().topic_id, "orders", index))
            .unwrap();
        check!(
            listed
                .iter()
                .any(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished),
            "partition {index} finished no copy"
        );
    }
}

/// The bound itself. Four partitions and two copier slots: the store may
/// never see more than two copies at once, and it must see two, so the value
/// the config carries is the number the sweep uses and not a hard-coded one.
#[tokio::test(flavor = "multi_thread")]
async fn the_copier_bound_caps_the_copies_in_flight() {
    let dirs: Vec<tempfile::TempDir> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let partitions = PartitionRegistry::new();
    for (index, dir) in dirs.iter().enumerate() {
        let index = i32::try_from(index).expect("four partitions fit in i32");
        partitions.insert(
            "orders".into(),
            PartitionIndex(index),
            tiered_partition_at(index, dir.path()),
        );
    }

    let counting = Arc::new(CountingRsm::default());
    let rsm: Arc<dyn RemoteStorageManager> = counting.clone();
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let controller = fixed_source(image_with_orders_partitions(4));

    tick_all(
        &partitions,
        &controller,
        &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        NodeId(1),
        1,
        SweepConcurrency {
            copier: 2,
            expiration: 2,
        },
    )
    .await;

    let peak = counting.peak_in_flight.load(Ordering::Relaxed);
    check!(
        peak <= 2,
        "the sweep had {peak} copies in flight under a bound of 2"
    );
    check!(
        peak == 2,
        "the sweep used only {peak} of its two copier slots"
    );
    for index in 0..4 {
        let listed = rlmm
            .list_remote_log_segments(&TopicIdPartition::new(tp().topic_id, "orders", index))
            .unwrap();
        check!(
            listed
                .iter()
                .any(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished),
            "partition {index} finished no copy"
        );
    }
}

/// How long [`CountingRsm`] holds a copy. Long enough that two copies the
/// sweep starts together overlap on any machine, short enough that a tick
/// over four partitions is still quick.
const COPY_DWELL: Duration = Duration::from_millis(100);

/// An RSM that accepts every copy after [`COPY_DWELL`] and remembers the most
/// copies it ever held at one time.
#[derive(Default)]
struct CountingRsm {
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
}

impl RemoteStorageManager for CountingRsm {
    fn copy_log_segment_data(
        &self,
        _metadata: &RemoteLogSegmentMetadata,
        _data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_in_flight.fetch_max(now, Ordering::SeqCst);
        std::thread::sleep(COPY_DWELL);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(None)
    }
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        _start: u32,
        _end: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        Err(RemoteStorageError::SegmentNotFound(
            metadata.remote_log_segment_id().clone(),
        ))
    }
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        _index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        Err(RemoteStorageError::SegmentNotFound(
            metadata.remote_log_segment_id().clone(),
        ))
    }
    fn delete_log_segment_data(
        &self,
        _metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        Ok(())
    }
}
