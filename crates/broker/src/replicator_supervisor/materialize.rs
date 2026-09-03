//! Race-free materialization of a partition's on-disk log and writer task.
//! Both the supervisor reconcile pass and the first-touch handler path open a
//! partition through this one helper, so a key can never be opened twice.

use std::{path::PathBuf, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig};
use krabka_units::Time;

use crate::partition_registry::PartitionRegistry;

/// Open (or recover) the on-disk `Partition` for `(topic, partition)` and
/// insert it into `partitions` with
/// `PartitionRegistry::materialize_if_vacant`.
///
/// This is the canonical, race-free materialization helper. Both the
/// `ReplicatorSupervisor` reconcile loop and the `InitProducerId` handler
/// (first-touch path) call this function. `materialize_if_vacant` runs the
/// build closure under the per-key lock, so two concurrent callers for the
/// same key can never both spawn independent writer tasks.
///
/// Returns `Ok(())` if the partition is already present, which is a no-op, or
/// if the function opened it. Returns `Err(String)` on I/O failure.
pub(crate) struct MaterializePartitionConfig<'a> {
    pub partitions: &'a PartitionRegistry,
    pub topic: &'a str,
    pub topic_id: Option<uuid::Uuid>,
    pub partition: i32,
    pub log_dirs: &'a [PathBuf],
    pub log_config: &'a LogConfig,
    pub log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    pub producer_state: &'a Arc<crate::producer_state::ProducerState>,
    pub producer_id_expiration: Time,
    pub max_produce_group: usize,
    pub partition_writer_queue_depth: usize,
    pub diskless_wal_local_replica_count: usize,
    pub diskless: bool,
    pub hot_tail: Option<Arc<crate::diskless::hot_tail::HotTailCache>>,
    pub wal_shards: Option<Arc<crate::wal::quorum::registry::WalShardRegistry>>,
    pub sequencer: Option<Arc<dyn crate::wal::OffsetSequencer>>,
}

pub(crate) fn materialize_partition(config: MaterializePartitionConfig<'_>) -> Result<(), String> {
    materialize_partition_with_replication_target(config, None)
}

pub(super) fn materialize_partition_with_replication_target(
    config: MaterializePartitionConfig<'_>,
    initial_target: Option<crate::partition::ReplicationTarget>,
) -> Result<(), String> {
    let MaterializePartitionConfig {
        partitions,
        topic,
        topic_id,
        partition,
        log_dirs,
        log_config,
        log_dir_status,
        producer_state,
        producer_id_expiration,
        max_produce_group,
        partition_writer_queue_depth,
        diskless_wal_local_replica_count,
        diskless,
        hot_tail,
        wal_shards,
        sequencer,
    } = config;
    // `materialize_if_vacant` runs `build` under the per-key write lock —
    // only one thread can be inside it for a given key at a time,
    // eliminating the TOCTOU race that existed with the old
    // `contains_key` + `insert` pattern. JBOD placement (KIP-113) happens
    // under this lock too, so two concurrent materializations of the same
    // partition can never pick two different log dirs.
    partitions.materialize_if_vacant(topic, PartitionIndex(partition), || {
        let dir = crate::log_dir::place_partition_dir(log_dirs, topic, partition);
        std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
        let open_config = crate::diskless::recovery::open_config(log_config, diskless);
        let mut log = Log::open(&dir, open_config).map_err(|e| format!("Log::open: {e}"))?;
        if let Some(stamp_source) = partitions.stamp_source() {
            log.set_stamp_source(stamp_source)
                .map_err(|e| format!("set stamp source: {e}"))?;
        }
        if diskless && let (Some(topic_id), Some(registry)) = (topic_id, wal_shards.as_ref()) {
            let shard = crate::wal::quorum::registry::ShardId {
                topic_id,
                partition: PartitionIndex(partition),
            };
            if registry.local_is_leader(shard) {
                crate::wal::quorum::follower::hydrate_on_promotion(
                    log_dirs,
                    topic,
                    shard,
                    registry.local_node_id(),
                    log_config,
                    &mut log,
                )
                .map_err(|e| format!("hydrate promoted WAL follower: {e}"))?;
            }
        }
        if diskless {
            producer_state.install_snapshot_before_materialization(
                topic,
                PartitionIndex(partition),
                log.producer_state_snapshot(),
            );
        }
        let owning_dir = dir
            .parent()
            .expect("placed partition dir always has a parent log.dir")
            .to_path_buf();
        let spawn = crate::broker::PartitionSpawnConfig {
            topic: topic.to_string(),
            topic_id,
            partition_id: PartitionIndex(partition),
            log_dir: owning_dir,
            log,
            log_dir_status: log_dir_status.clone(),
            producer_state: producer_state.clone(),
            producer_id_expiration,
            max_produce_group,
            partition_writer_queue_depth,
            diskless_wal_local_replica_count,
            diskless,
            hot_tail,
            wal_shards,
            sequencer,
        };
        match initial_target {
            Some(target) => {
                crate::broker::try_spawn_partition_with_replication_target(spawn, target)
            }
            None => crate::broker::try_spawn_partition_with_sequencer(spawn),
        }
        .map_err(|e| format!("spawn partition: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use assert2::assert;
    use krabka_units::hours;

    use super::*;

    #[derive(Debug)]
    struct TestStampSource(AtomicU64);

    impl krabka_log::StampSource for TestStampSource {
        fn next_stamp(&self) -> u64 {
            self.0.fetch_add(1, Ordering::Relaxed)
        }
    }

    fn materialize_test_partition(
        partitions: &Arc<PartitionRegistry>,
        log_dir: &std::path::Path,
        topic: &str,
    ) {
        materialize_partition(MaterializePartitionConfig {
            partitions,
            topic,
            topic_id: None,
            partition: 0,
            log_dirs: &[log_dir.to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: false,
            hot_tail: None,
            wal_shards: None,
            sequencer: None,
        })
        .expect("materialize partition");
    }

    fn append_one(partition: &crate::partition::Partition) -> krabka_log::Offset {
        use krabka_protocol::records::{Record, RecordBatch};

        let mut batch = RecordBatch {
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        partition
            .log
            .lock()
            .expect("partition log")
            .append(&mut batch)
            .expect("append")
            .0
    }

    #[tokio::test]
    async fn materialize_partition_helper_supports_isr_install() {
        use krabka_log::LogConfig;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        materialize_partition(MaterializePartitionConfig {
            partitions: &partitions,
            topic: "t",
            topic_id: None,
            partition: 0,
            log_dirs: &[dir.path().to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: false,
            hot_tail: None,
            wal_shards: None,
            sequencer: None,
        })
        .expect("materialize");
        let part = partitions.get("t", PartitionIndex(0)).expect("part");
        // Mirror what reconcile does for leader partitions.
        part.install_isr(
            &[
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            &[
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            krabka_audit::NodeId(1),
        )
        .await;
        let st = part.replica_state.lock().await;
        assert!(st.isr.len() == 3);
    }

    #[tokio::test]
    async fn materialized_partition_stamps_only_when_source_is_configured() {
        let disabled_dir = tempfile::tempdir().expect("disabled tempdir");
        let disabled = Arc::new(PartitionRegistry::new());
        materialize_test_partition(&disabled, disabled_dir.path(), "disabled");
        let disabled_part = disabled
            .get("disabled", PartitionIndex(0))
            .expect("disabled partition");
        let disabled_offset = append_one(&disabled_part);
        assert!(disabled_offset == krabka_log::Offset(0));
        assert!(
            disabled_part.stamp_for_offset(disabled_offset).is_none(),
            "Kafka-only partitions must not create internal stamps"
        );

        let enabled_dir = tempfile::tempdir().expect("enabled tempdir");
        let source: Arc<dyn krabka_log::StampSource> =
            Arc::new(TestStampSource(AtomicU64::new(100)));
        let enabled = Arc::new(PartitionRegistry::with_stamp_source(Some(source)));
        materialize_test_partition(&enabled, enabled_dir.path(), "enabled");
        let enabled_part = enabled
            .get("enabled", PartitionIndex(0))
            .expect("enabled partition");
        let enabled_offset = append_one(&enabled_part);
        assert!(enabled_offset == krabka_log::Offset(0));
        assert!(enabled_part.stamp_for_offset(enabled_offset) == Some(100));
    }

    #[tokio::test]
    async fn recovered_partition_installs_source_before_new_appends() {
        use krabka_protocol::records::{Record, RecordBatch};

        let dir = tempfile::tempdir().expect("tempdir");
        let partition_dir = crate::log_dir::partition_dir(dir.path(), "recovered", 0);
        std::fs::create_dir_all(&partition_dir).expect("partition dir");
        let mut existing = Log::open(&partition_dir, LogConfig::default()).expect("open existing");
        existing
            .append(&mut RecordBatch {
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .expect("append existing");
        drop(existing);

        let source: Arc<dyn krabka_log::StampSource> =
            Arc::new(TestStampSource(AtomicU64::new(500)));
        let partitions = Arc::new(PartitionRegistry::with_stamp_source(Some(source)));
        materialize_test_partition(&partitions, dir.path(), "recovered");
        let partition = partitions
            .get("recovered", PartitionIndex(0))
            .expect("recovered partition");

        let new_offset = append_one(&partition);
        assert!(new_offset == krabka_log::Offset(1));
        assert!(partition.stamp_for_offset(krabka_log::Offset(0)).is_none());
        assert!(partition.stamp_for_offset(new_offset) == Some(500));
    }

    #[tokio::test]
    async fn materialize_diskless_partition_registers_wal_shard() {
        use krabka_log::LogConfig;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        let topic_id = uuid::Uuid::from_u128(0xD15C);
        let hot_tail = Arc::new(crate::diskless::hot_tail::HotTailCache::default());
        let wal_shards = Arc::new(crate::wal::quorum::registry::WalShardRegistry::new(
            krabka_raft::NodeId(0),
        ));

        materialize_partition(MaterializePartitionConfig {
            partitions: &partitions,
            topic: "diskless",
            topic_id: Some(topic_id),
            partition: 0,
            log_dirs: &[dir.path().to_path_buf()],
            log_config: &LogConfig::default(),
            log_dir_status: &crate::log_dir_status::LogDirRegistry::default(),
            producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
            producer_id_expiration: hours(24),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            diskless_wal_local_replica_count: 3,
            diskless: true,
            hot_tail: Some(hot_tail),
            wal_shards: Some(wal_shards.clone()),
            sequencer: None,
        })
        .expect("materialize");

        assert!(
            wal_shards
                .get(crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(0),
                })
                .is_some()
        );
    }
}
