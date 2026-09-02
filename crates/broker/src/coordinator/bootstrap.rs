//! `__consumer_offsets` topic lifecycle.
//!
//! The module makes sure that the topic exists at startup. It then replays
//! every record synchronously into the in-memory `GroupCoordinator`.

use std::sync::Arc;

use krabka_ids::PartitionIndex;
use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use krabka_raft::RaftError;
use krabka_units::convert::TimeExt as _;

use crate::{
    broker::spawn_partition, config::BrokerConfig, coordinator::GroupCoordinator,
    error::BrokerError, log_dir, partition_registry::PartitionRegistry,
};

mod apply;
mod audit;
mod replay;

#[cfg(test)]
mod classic_state_tests;
#[cfg(test)]
mod log_walk_tests;
#[cfg(test)]
mod share_streams_replay_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod topic_bootstrap_tests;
#[cfg(test)]
mod upgrade_downgrade_tests;

pub use self::audit::bootstrap_audit_topic;
pub(crate) use self::replay::replay_partition;
use self::replay::{Replayed, finalize, replay_records};

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";
/// First offsets partition, used by focused compatibility tests.
#[cfg(test)]
pub const OFFSETS_PARTITION: i32 = 0;
/// Kafka's default offsets-topic partition count. Existing clusters retain the
/// partition count recorded in metadata; this value is only used at creation.
pub const OFFSETS_NUM_PARTITIONS: i32 = 50;

/// Internal topic that carries tamper-evident OCSF audit records for the
/// `FedRAMP` MLA, under its default name.
///
/// This is an alias for [`crate::config::DEFAULT_AUDIT_TOPIC`] rather than a
/// second spelling of the name: `krabka.audit.topic` renames the audit log, so
/// every code path that has to reach the live one reads
/// `BrokerConfig::audit_topic` instead. Tests that boot a default broker use
/// this.
pub const AUDIT_TOPIC: &str = crate::config::DEFAULT_AUDIT_TOPIC;

/// Ensure `__consumer_offsets` exists, open every partition assigned to this
/// broker, spawn its writer task, and replay each local log into the supplied
/// `GroupCoordinator`. Registers the topic via the metadata quorum
/// (`controller.submit_change(...)`) with Kafka's default partition count;
/// `TopicExists` is treated as success so a restart that finds the topic
/// already in the log is a no-op.
///
/// The function registers the topic through the metadata quorum with
/// `controller.submit_change(...)` as a 1-partition internal topic. It treats
/// `TopicExists` as a success, so a restart that finds the topic already in
/// the log does nothing.
///
/// `Broker::start` calls this exactly once, BEFORE the TCP listener binds and
/// AFTER the controller has elected a leader. See `Broker::start`.
pub async fn bootstrap(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    partitions: &Arc<PartitionRegistry>,
    coordinator: &Arc<GroupCoordinator>,
    log_dir_status: &crate::log_dir_status::LogDirRegistry,
    producer_state: &Arc<crate::producer_state::ProducerState>,
) -> Result<(), BrokerError> {
    // KIP-113 offline-dir handling: exclude dirs flagged offline by the
    // startup probe; placing `__consumer_offsets-N` on a known-bad dir
    // would fail immediately at `Log::open` below and leave the broker
    // unable to bootstrap the group coordinator.
    let placement_dirs = log_dir_status.online_subset(&config.all_log_dirs());
    if placement_dirs.is_empty() {
        return Err(BrokerError::Io(std::io::Error::other(
            "every configured log.dir failed the startup writability probe; \
             cannot bootstrap the group-coordinator partition",
        )));
    }
    // Register the topic via the metadata quorum, but only from a SINGLE
    // consistent writer: the controller leader. The previous `is_none()` ->
    // `submit_change` path was a TOCTOU race — when two voters boot
    // concurrently, both observe `__consumer_offsets` absent and both submit a
    // `TopicRecord` (`topic_id` is a random `Uuid::new_v4()` per node) plus a
    // `PartitionRecord` (`leader`/`replicas`/`isr` differ per node). The
    // controller's `TopicExists` dedup is apply-time, so BOTH conflicting
    // records land in the replicated metadata log, and a JVM follower
    // replicating that far fatal-faults with "Found duplicate TopicRecord for
    // __consumer_offsets with a different ID than before."
    //
    // Fix: only the leader registers the topic (one writer => one id, one
    // partition placement). Followers wait for the record to replicate into
    // their image rather than submitting a possibly-conflicting copy.
    if controller.current_image().topic(OFFSETS_TOPIC).is_none() {
        // Copy the leader id out of the watch `Ref` BEFORE any `.await` so we
        // don't hold the borrow across an await point.
        let am_leader = *controller.watch_leader().borrow() == Some(config.node_id);
        if am_leader {
            let image = controller.current_image();
            let mut brokers: Vec<_> = image.brokers().map(|broker| broker.node_id).collect();
            drop(image);
            if brokers.is_empty() {
                brokers.push(config.node_id);
            }
            brokers.sort_unstable();
            let replication_factor =
                i16::try_from(crate::bootstrap::internal_topic_replication_factor(
                    config.offsets_topic_replication_factor,
                    brokers.len(),
                ))
                .expect("effective offsets replication factor fits i16");
            let assignments = crate::handlers::create_topics::round_robin_replicas(
                &brokers,
                config.offsets_topic_num_partitions,
                replication_factor,
            );
            let mut records = Vec::with_capacity(
                1 + usize::try_from(config.offsets_topic_num_partitions).unwrap_or_default(),
            );
            records.push(MetadataRecord::V1Topic(TopicRecord {
                name: OFFSETS_TOPIC.to_string(),
                topic_id: uuid::Uuid::new_v4(),
                partitions: config.offsets_topic_num_partitions,
                replication_factor,
            }));
            for (partition, replicas) in assignments.into_iter().enumerate() {
                records.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: OFFSETS_TOPIC.to_string(),
                    partition: i32::try_from(partition)
                        .expect("offsets partition index overflows i32"),
                    leader: replicas[0],
                    replicas: replicas.clone(),
                    isr: replicas,
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                }));
            }
            match controller.submit_change(records).await {
                // An earlier boot of ours already registered it (single
                // writer, so no conflicting-id race) — treat as success.
                Ok(_)
                | Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => {}
                Err(e) => return Err(BrokerError::Startup(e.to_string())),
            }
        } else {
            // Follower: do NOT submit (that's the race). Wait for the leader's
            // record to replicate into our image. Failing loudly on timeout is
            // correct — submitting a duplicate on timeout is what caused the
            // JVM fatal fault.
            let mut images = controller.watch_image();
            let deadline =
                tokio::time::Instant::now() + config.offsets_topic_metadata_wait_timeout.to_std();
            while !offsets_topic_ready(
                &controller.current_image(),
                config.offsets_topic_num_partitions,
            ) {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(BrokerError::Startup(format!(
                        "timed out waiting for the controller leader to register \
                         {OFFSETS_TOPIC} in the metadata image"
                    )));
                }
                if tokio::time::timeout(remaining, images.changed())
                    .await
                    .is_err()
                {
                    return Err(BrokerError::Startup(format!(
                        "timed out waiting for the controller leader to register \
                         {OFFSETS_TOPIC} in the metadata image"
                    )));
                }
            }
        }
    }

    let local_records: Vec<PartitionRecord> = controller
        .current_image()
        .partitions_of(OFFSETS_TOPIC)
        .filter(|record| record.replicas.contains(&config.node_id))
        .cloned()
        .collect();
    let mut replayed = Replayed::default();
    for record in local_records {
        let partition_id = PartitionIndex(record.partition);
        if let Some(partition) = partitions.get(OFFSETS_TOPIC, partition_id) {
            if record.leader == config.node_id {
                let log = partition.log.lock().map_err(|_| {
                    BrokerError::Startup(format!(
                        "{OFFSETS_TOPIC}-{} log lock poisoned during replay",
                        record.partition
                    ))
                })?;
                replayed.merge(replay_records(&log, coordinator)?);
            }
            continue;
        }

        let topic_dir =
            log_dir::place_partition_dir(&placement_dirs, OFFSETS_TOPIC, record.partition);
        std::fs::create_dir_all(&topic_dir)?;
        let log = krabka_log::Log::open(&topic_dir, config.log_config.clone())?;
        if record.leader == config.node_id {
            replayed.merge(replay_records(&log, coordinator)?);
        }
        let owning_dir = topic_dir
            .parent()
            .expect("placed partition dir always has a parent log.dir")
            .to_path_buf();
        let partition = spawn_partition(
            OFFSETS_TOPIC.to_string(),
            partition_id,
            owning_dir,
            log,
            log_dir_status.clone(),
            producer_state.clone(),
            false,
        );
        partitions.insert(OFFSETS_TOPIC.into(), partition_id, partition);
    }
    finalize(coordinator, replayed).await;
    Ok(())
}

fn offsets_topic_ready(image: &krabka_metadata::MetadataImage, expected_partitions: i32) -> bool {
    image.topic(OFFSETS_TOPIC).is_some()
        && image.topic_partition_count(OFFSETS_TOPIC) == expected_partitions
}
