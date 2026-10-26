//! Construction of a partition's runtime: the diskless WAL wiring, the writer
//! task, and the notify/watermark plumbing that a `Partition` owns. Every
//! caller in the crate that materialises a partition goes through here, so the
//! spawn paths live apart from the broker's startup sequence.
use {
    crate::{
        config::BrokerConfig,
        delivery::DeliveryHandles,
        diskless::hot_tail::HotTailCache,
        error::BrokerError,
        log_dir_status::LogDirRegistry,
        partition::{
            Partition, PartitionRuntimeConfig, ReplicationTarget, WriterGate, WriterMessage,
            empty_marker_materialization, unreported_leader_log_start,
        },
        partition_writer::{
            WriterIdentity, WriterPolicy, WriterServices, WriterSignals, WriterStorage, run_gated,
        },
        producer_state::ProducerState,
        replica_state::ReplicaState,
        wal::{
            SharedWal,
            quorum::{
                QuorumWalStore,
                engine::WalShardEngine,
                registry::{ShardId, WalPlacement, WalShardRegistry},
            },
        },
    },
    krabka_ids::PartitionIndex,
    std::{
        collections::HashMap,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicI32, AtomicU64},
        },
    },
    tokio::sync::{self, Notify, RwLock, mpsc},
};

/// The diskless WAL services of a partition. Every field is `None` for a
/// partition that is not diskless.
struct PartitionWal {
    wal: Option<SharedWal>,
    /// The durable watermark the WAL recovered, which seeds the replica state.
    durable_watermark: Option<krabka_log::Offset>,
    engine: Option<Arc<WalShardEngine>>,
}

/// The topic and partition a WAL serves.
#[derive(Clone, Copy)]
struct PartitionIdentity<'a> {
    topic: &'a str,
    topic_id: Option<uuid::Uuid>,
    partition_id: PartitionIndex,
}

fn partition_wal(
    identity: PartitionIdentity<'_>,
    log: Arc<Mutex<krabka_log::Log>>,
    diskless: bool,
    hot_tail: Option<Arc<HotTailCache>>,
    wal_shards: Option<Arc<WalShardRegistry>>,
    replica_count: usize,
) -> Result<PartitionWal, BrokerError> {
    let PartitionIdentity {
        topic,
        topic_id,
        partition_id,
    } = identity;
    if !diskless {
        return Ok(PartitionWal {
            wal: None,
            durable_watermark: None,
            engine: None,
        });
    }
    let topic_id = topic_id.ok_or_else(|| {
        BrokerError::Replication(format!(
            "diskless WAL topic id is not available for {topic}-{}",
            partition_id.0
        ))
    })?;
    let registry = wal_shards.ok_or_else(|| {
        BrokerError::Replication(format!(
            "diskless WAL shard registry is not available for {topic}-{}",
            partition_id.0
        ))
    })?;
    let wal = QuorumWalStore::for_distributed_partition(
        topic_id,
        partition_id,
        log,
        hot_tail,
        replica_count,
    )?;
    registry.insert(
        ShardId {
            topic_id,
            partition: partition_id,
        },
        wal.engine(),
    );
    let durable_watermark = wal.engine().durable_watermark();
    let engine = wal.engine();
    Ok(PartitionWal {
        wal: Some(Arc::new(wal) as SharedWal),
        durable_watermark: Some(durable_watermark),
        engine: Some(engine),
    })
}

/// Create the partition runtime (mpsc channel + writer task + notify).
///
/// `log_dir` is the parent `log.dir` that owns the partition (i.e. the
/// configured directory, not the `<topic>-<partition>` subdirectory).
/// Stored on the `Partition` so KIP-113 (`AlterReplicaLogDirs`) can
/// reject moves whose target is the partition's current dir without
/// access to the `Log` mutex on the hot path, and so
/// `DescribeLogDirs` can attribute the partition to a dir even when
/// the path is not stable across canonicalisation.
///
/// # Errors
///
/// As [`try_spawn_partition_with_replication_target`].
#[krabka_macros::partition_spawn_parameters]
pub fn spawn_partition() -> Result<Arc<Partition>, BrokerError> {
    // A test diskless partition needs a topic id for its WAL shard.
    let topic_id = if cfg!(test) {
        diskless.then(uuid::Uuid::new_v4)
    } else {
        None
    };
    spawn_partition_with_replication_target()
        .topic(topic)
        .replication_target(ReplicationTarget {
            topic_id,
            leader_node_id: krabka_raft::NodeId(0),
            leader_epoch: krabka_metadata::LeaderEpoch(0),
        })
        .partition_id(partition_id)
        .log_dir(log_dir)
        .log(log)
        .log_dir_status(log_dir_status)
        .producer_state(producer_state)
        .diskless(diskless)
        .call()
}

/// [`spawn_partition`] with an explicit initial replication target.
///
/// # Errors
///
/// As [`try_spawn_partition_with_replication_target`].
#[krabka_macros::partition_spawn_parameters(target)]
#[bon::builder]
pub fn spawn_partition_with_replication_target() -> Result<Arc<Partition>, BrokerError> {
    let broker_config = BrokerConfig::default();
    let wal_shards = if cfg!(test) {
        replication_target.topic_id.map(|topic_id| {
            let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(0)));
            registry.replace_placements(&HashMap::from([(
                ShardId {
                    topic_id,
                    partition: partition_id,
                },
                WalPlacement {
                    voters: vec![krabka_raft::NodeId(0)],
                    leader_epoch: 0,
                },
            )]));
            registry
        })
    } else {
        None
    };
    let diskless_wal_local_replica_count = if cfg!(test) && diskless {
        1
    } else {
        broker_config.diskless_wal_local_replica_count
    };
    try_spawn_partition_with_replication_target(
        PartitionSpawnConfig {
            topic,
            topic_id: replication_target.topic_id,
            partition_id,
            log_dir,
            log,
            log_dir_status,
            producer_state,
            runtime: PartitionRuntimeConfig::new(
                (
                    broker_config.max_produce_group,
                    broker_config.partition_writer_queue_depth,
                    diskless_wal_local_replica_count,
                ),
                diskless,
                (None, wal_shards, None),
            ),
        },
        replication_target,
    )
}

pub struct PartitionSpawnConfig {
    pub topic: String,
    pub topic_id: Option<uuid::Uuid>,
    pub partition_id: PartitionIndex,
    pub log_dir: PathBuf,
    pub log: krabka_log::Log,
    pub log_dir_status: LogDirRegistry,
    pub producer_state: Arc<ProducerState>,
    pub runtime: PartitionRuntimeConfig,
}

pub fn try_spawn_partition_with_sequencer(
    config: PartitionSpawnConfig,
) -> Result<Arc<Partition>, BrokerError> {
    let initial_target = ReplicationTarget {
        topic_id: config.topic_id,
        leader_node_id: krabka_raft::NodeId(0),
        leader_epoch: krabka_metadata::LeaderEpoch(0),
    };
    try_spawn_partition_with_replication_target(config, initial_target)
}

/// Spawn a partition whose replicator starts at `initial_target`.
///
/// # Errors
///
/// Returns [`BrokerError::Startup`] when `config` and `initial_target` name
/// different topic ids, and any error of opening the partition runtime.
pub fn try_spawn_partition_with_replication_target(
    config: PartitionSpawnConfig,
    initial_target: ReplicationTarget,
) -> Result<Arc<Partition>, BrokerError> {
    if config.topic_id != initial_target.topic_id {
        return Err(BrokerError::Startup(format!(
            "partition {}-{}: topic id {:?} differs from the replication target's {:?}",
            config.topic, config.partition_id, config.topic_id, initial_target.topic_id
        )));
    }
    let PartitionSpawnConfig {
        topic,
        topic_id,
        partition_id,
        log_dir,
        log,
        log_dir_status,
        producer_state,
        runtime,
    } = config;
    let PartitionRuntimeConfig {
        max_produce_group,
        partition_writer_queue_depth,
        diskless_wal_local_replica_count,
        diskless,
        hot_tail,
        wal_shards,
        sequencer,
    } = runtime;
    let log = Arc::new(Mutex::new(log));
    let PartitionWal {
        wal,
        durable_watermark: recovered_durable_watermark,
        engine: wal_engine,
    } = partition_wal(
        PartitionIdentity {
            topic: &topic,
            topic_id,
            partition_id,
        },
        Arc::clone(&log),
        diskless,
        hot_tail,
        wal_shards,
        diskless_wal_local_replica_count,
    )?;
    let (tx, rx) = mpsc::channel::<WriterMessage>(partition_writer_queue_depth);
    let notify = Arc::new(Notify::new());
    let delivery = DeliveryHandles::new();
    // Seed the watermark mirror from the log the recovery walk just opened, so
    // a reader that takes no lock never sees the placeholder zero.
    delivery.publish_now(&log);
    let mut initial_replica_state = ReplicaState::new();
    initial_replica_state.current_leader_epoch =
        krabka_ids::LeaderEpoch(initial_target.leader_epoch.0);
    if let Some(durable_watermark) = recovered_durable_watermark {
        initial_replica_state.recompute_hw_for_wal_durable(durable_watermark);
    }
    let initial_wal_watermark = initial_replica_state.hw;
    let replica_state = Arc::new(sync::Mutex::new(initial_replica_state));
    let hw_advance_notify = Arc::new(Notify::new());
    let current_leader = Arc::new(AtomicU64::new(initial_target.leader_node_id.0));
    let current_leader_epoch = Arc::new(AtomicI32::new(initial_target.leader_epoch.0));
    let replication_target = Arc::new(RwLock::new(initial_target));
    let log_dir = Arc::new(arc_swap::ArcSwap::from_pointee(log_dir));
    // A partition that assigns offsets through a sequencer or acknowledges
    // through a WAL keeps every append on its writer.
    let writer_gate = WriterGate::shared(wal.is_none() && sequencer.is_none());
    let writer_future = run_gated(
        WriterIdentity {
            topic: topic.clone(),
            partition: partition_id,
        },
        WriterStorage {
            log: Arc::clone(&log),
            log_dir: Arc::clone(&log_dir),
        },
        rx,
        WriterSignals {
            append_notify: Arc::clone(&notify),
            replica_state: Arc::clone(&replica_state),
            hw_advance_notify: Arc::clone(&hw_advance_notify),
            delivery: delivery.clone(),
        },
        WriterServices {
            log_dir_status,
            producer_state,
            wal,
        },
        WriterPolicy {
            max_produce_group,
            sequencer,
            gate: Arc::clone(&writer_gate),
        },
    );
    let writer = if let Some(engine) = wal_engine {
        let replica_state = Arc::clone(&replica_state);
        let hw_advance_notify = Arc::clone(&hw_advance_notify);
        tokio::spawn(async move {
            let watermark_updates = async move {
                let mut observed = initial_wal_watermark;
                loop {
                    let durable = engine.wait_for_durable_advance(observed).await;
                    observed = durable;
                    let mut state = replica_state.lock().await;
                    let previous = state.hw;
                    state.recompute_hw_for_wal_durable(durable);
                    if state.hw != previous {
                        hw_advance_notify.notify_waiters();
                    }
                }
            };
            tokio::select! {
                () = writer_future => {}
                () = watermark_updates => {}
            }
        })
    } else {
        tokio::spawn(writer_future)
    };
    Ok(Arc::new(Partition {
        topic,
        index: partition_id,
        log_dir,
        log,
        writer_tx: tx,
        writer_gate,
        marker_materialization: empty_marker_materialization(),
        append_notify: notify,
        replica_state,
        hw_advance_notify,
        delivery,
        current_leader,
        current_leader_epoch,
        replication_target,
        diskless,
        reported_leader_log_start: unreported_leader_log_start(),
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    }))
}

#[cfg(test)]
mod tests;
