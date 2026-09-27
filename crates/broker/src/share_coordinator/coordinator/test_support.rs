//! Shared fixtures for the `ShareCoordinator` unit tests: a `StateBatch`
//! builder, a real `__share_group_state` partition with a live writer, a
//! coordinator that owns all of them, a leadership seed, and a reader of the
//! records a state partition holds.
//!
//! Every submodule of `coordinator` needs the same live partition logs, so the
//! builders live in one place instead of once per test module.

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, Offset};
use qubit_clock::{ManualMonotonicClock, ManualWallClock};

use super::ShareCoordinator;
use crate::{
    partition_registry::PartitionRegistry,
    share_coordinator::{
        bootstrap,
        config::ShareCoordinatorConfig,
        persistence::{
            KEY_SHARE_SNAPSHOT, ShareSnapshotValue, ShareUpdateValue, StateBatch, parse_state_key,
        },
    },
};

/// The wall-clock time, in milliseconds, at which every test coordinator
/// starts.
pub(crate) const NOW_MS: i64 = 1_700_000_000_000;

pub(super) fn batch(first: i64, last: i64) -> StateBatch {
    StateBatch {
        first_offset: Offset(first),
        last_offset: Offset(last),
        delivery_state: 0,
        delivery_count: 1,
    }
}

/// Builds a real `__share_group_state`-`p` partition and registers it.
///
/// The partition has a live writer. This function mirrors
/// `fixture_partition` in `partition_registry`.
pub(super) fn open_state_partition(reg: &PartitionRegistry, log_dir: &Path, p: i32) {
    let part_dir = crate::log_dir::partition_dir(log_dir, bootstrap::TOPIC, p);
    std::fs::create_dir_all(&part_dir).unwrap();
    let log = Log::open(&part_dir, LogConfig::default()).unwrap();
    let part = crate::broker::spawn_partition(
        bootstrap::TOPIC.to_string(),
        PartitionIndex(p),
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    reg.insert(bootstrap::TOPIC.into(), PartitionIndex(p), part);
}

/// A manual wall clock that reads [`NOW_MS`] until a test moves it.
pub(crate) fn manual_clock() -> Arc<ManualWallClock> {
    ManualMonotonicClock::new_shared()
        .new_wall_clock(UNIX_EPOCH + Duration::from_millis(NOW_MS.unsigned_abs()))
}

/// Moves `clock` to `ms` milliseconds after [`NOW_MS`].
pub(crate) fn set_clock(clock: &ManualWallClock, ms: u64) {
    clock.reanchor(UNIX_EPOCH + Duration::from_millis(NOW_MS.unsigned_abs() + ms));
}

/// A coordinator with `config` over all of its state partitions, open
/// locally, and the manual clock that drives it.
pub(crate) fn configured_coordinator(
    dir: &Path,
    config: ShareCoordinatorConfig,
) -> (
    ShareCoordinator,
    Arc<PartitionRegistry>,
    Arc<ManualWallClock>,
) {
    let reg = Arc::new(PartitionRegistry::new());
    for p in 0..config.state_topic_num_partitions {
        open_state_partition(&reg, dir, p);
    }
    let clock = manual_clock();
    let coord = ShareCoordinator::with_wall_clock(
        krabka_audit::NodeId(1),
        reg.clone(),
        config,
        Arc::clone(&clock) as Arc<dyn qubit_clock::WallClock>,
    );
    (coord, reg, clock)
}

/// A coordinator that leads every state partition it touches.
///
/// All 50 `__share_group_state` partitions are open locally.
pub(super) fn coordinator(dir: &Path) -> (ShareCoordinator, Arc<PartitionRegistry>) {
    let (coord, reg, _clock) = configured_coordinator(dir, ShareCoordinatorConfig::default());
    (coord, reg)
}

pub(super) async fn lead_all(coord: &ShareCoordinator) {
    coord.lead_all_partitions_for_test().await;
}

/// One record of a state partition log, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Logged {
    Snapshot(ShareSnapshotValue),
    Update(ShareUpdateValue),
    Tombstone,
}

/// Every record of `state_partition` from its log start, with its offset.
pub(crate) fn logged_records(
    coord: &ShareCoordinator,
    state_partition: PartitionIndex,
) -> Vec<(Offset, Logged)> {
    let part = coord
        .partitions
        .get(bootstrap::TOPIC, state_partition)
        .expect("state partition open");
    let mut records = Vec::new();
    let mut offset = part.log_start_offset();
    loop {
        let out = part
            .read_log(offset, krabka_units::mebibytes(1))
            .expect("read state partition");
        if out.batches.is_empty() {
            return records;
        }
        for batch in &out.batches {
            for rec in &batch.records {
                let key = parse_state_key(rec.key.as_ref().expect("key")).expect("state key");
                let logged = match (&rec.value, key.record_type) {
                    (None, _) => Logged::Tombstone,
                    (Some(value), KEY_SHARE_SNAPSHOT) => {
                        Logged::Snapshot(ShareSnapshotValue::decode(value).expect("snapshot"))
                    }
                    (Some(value), _) => {
                        Logged::Update(ShareUpdateValue::decode(value).expect("update"))
                    }
                };
                records.push((
                    Offset(batch.base_offset + i64::from(rec.offset_delta)),
                    logged,
                ));
            }
            offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
    }
}

/// A metadata image that holds one data topic, `t`, with id `topic_id` and
/// `partitions` partitions.
pub(crate) fn image_with_topic(
    topic_id: uuid::Uuid,
    partitions: i32,
) -> krabka_metadata::MetadataImage {
    image_with_topics(&[(topic_id, partitions)])
}

/// A metadata image that holds one data topic `t<i>` for each
/// `(topic_id, partitions)` of `topics`.
pub(crate) fn image_with_topics(topics: &[(uuid::Uuid, i32)]) -> krabka_metadata::MetadataImage {
    let mut records = Vec::new();
    for (index, (topic_id, partitions)) in topics.iter().enumerate() {
        let name = if index == 0 {
            "t".to_owned()
        } else {
            format!("t{index}")
        };
        records.push(krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: name.clone(),
                topic_id: *topic_id,
                partitions: *partitions,
                replication_factor: 1,
            },
        ));
        for partition in 0..*partitions {
            records.push(krabka_metadata::MetadataRecord::V1Partition(
                krabka_metadata::PartitionRecord {
                    topic: name.clone(),
                    partition,
                    leader: krabka_metadata::NodeId(1),
                    replicas: vec![krabka_metadata::NodeId(1)],
                    isr: vec![krabka_metadata::NodeId(1)],
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                },
            ));
        }
    }
    krabka_metadata::MetadataImage::from_records(uuid::Uuid::nil(), &records)
}

/// A `WriteShareGroupState` partition.
pub(crate) fn share_write(
    epochs: (i32, i32),
    progress: (i64, i32),
    batches: Vec<StateBatch>,
) -> super::ShareWrite {
    super::ShareWrite {
        state_epoch: epochs.0,
        leader_epoch: epochs.1,
        start_offset: Offset(progress.0),
        delivery_complete_count: progress.1,
        batches,
    }
}
