//! Per-(topic, partition) replication task.
//!
//! The task issues standard Kafka `Fetch` requests against the leader of the
//! partition, with `replica_id` set to the `node_id` of the local broker. It
//! appends each returned batch to the local `krabka-log`. On
//! `OFFSET_OUT_OF_RANGE` it truncates the local log to 0 and restarts. On
//! `NOT_LEADER_FOR_PARTITION` it returns, so that the next reconcile of the
//! supervisor evaluates the partition again.

use std::{path::PathBuf, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use krabka_raft::NodeId;
use krabka_security::ListenerProtocol;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

mod connection;
mod fetch_loop;
mod follower_throttle;
mod response;
#[cfg(test)]
mod test_support;
mod truncation;

use self::fetch_loop::run_inner;
use crate::{
    broker::spawn_partition_with_replication_target, config::ReplicationRuntimeConfig,
    partition::ReplicationTarget, partition_registry::PartitionRegistry, throttle::ThrottleState,
};

/// Configuration handed to a single replicator task.
pub(crate) struct Config {
    pub node_id: NodeId,
    pub topic: String,
    /// Wire-format `topic_id` for the partition.
    ///
    /// The `Fetch` request needs this value to fill the v13+ wire field. At
    /// v ≥ 13 Kafka drops `FetchTopic.topic` in favour of `topic_id`
    /// (KIP-516), and the handler of the leader resolves the topic name from
    /// `topic_id` only. If the replicator sends `WireUuid::ZERO` here, the
    /// leader returns `UNKNOWN_TOPIC_OR_PARTITION` for every fetch.
    pub topic_id: WireUuid,
    pub partition: PartitionIndex,
    pub leader_node_id: NodeId,
    pub leader_epoch: krabka_metadata::LeaderEpoch,
    /// The `host` portion of the leader from the metadata image.
    ///
    /// This is the inter-broker endpoint when one is available, and the legacy
    /// broker host if not.
    pub leader_host: String,
    pub leader_port: u16,
    pub partitions: Arc<PartitionRegistry>,
    pub log_dirs: Vec<PathBuf>,
    pub log_settings: LogConfig,
    pub client_id: String,
    pub shutdown: CancellationToken,
    /// Shared outbound dialer.
    ///
    /// The dialer connects through TLS and SASL when the inter-broker listener
    /// needs them. It falls back to raw TCP for PLAINTEXT.
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    pub inter_broker_listener_protocol: ListenerProtocol,
    pub inter_broker_server_name: String,
    pub replication: ReplicationRuntimeConfig,
    /// KIP-73 broker-wide throttle state.
    ///
    /// The follower-in bucket gates the outbound Fetch bytes while this
    /// partition is throttled.
    pub throttle_state: Arc<ThrottleState>,
    /// Controller handle that reads the current metadata image each Fetch
    /// round.
    ///
    /// The replicator uses the image to look up
    /// `follower.replication.throttled.replicas`.
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    /// KIP-113 runtime offline-dir registry.
    ///
    /// The replicator forwards this registry into `spawn_partition`. The
    /// per-partition writer can then set the owning dir offline after a
    /// segment-write failure or an fsync failure.
    pub log_dir_status: crate::log_dir_status::LogDirRegistry,
    /// Broker-wide producer-sequence tracker for idempotent and transactional
    /// producers.
    ///
    /// The replicator forwards this tracker into `spawn_partition` through
    /// `ensure_local_partition`. The `Compact` handler of the per-partition
    /// writer can then snapshot the active producers for KIP-534
    /// `RETAIN_EMPTY`.
    pub producer_state: Arc<crate::producer_state::ProducerState>,
    /// Broker-wide metrics handle.
    ///
    /// The replicator increments `replication_bytes_in` after a successful
    /// follower-side append.
    pub metrics: crate::metrics::BrokerMetrics,
}

/// Entry point. Drives one (topic, partition) replication loop until cancelled.
pub(crate) async fn run(cfg: Config) {
    info!(
        topic = %cfg.topic,
        partition = cfg.partition.get(),
        leader_node_id = cfg.leader_node_id.0,
        "replicator.started"
    );

    // First-run materialization of the local on-disk partition.
    if let Err(e) = ensure_local_partition(&cfg) {
        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator failed to open local partition; aborting");
        return;
    }

    if let Err(e) = run_inner(&cfg).await {
        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator stopped on unrecoverable error");
    }

    info!(topic = %cfg.topic, partition = cfg.partition.get(), "replicator.stopped");
}

/// Builds or recovers the on-disk `Partition` for this follower.
///
/// The function inserts the partition into the shared `partitions` map of the
/// broker. The function is idempotent.
fn ensure_local_partition(cfg: &Config) -> Result<(), String> {
    // `materialize_if_vacant` runs the build under the per-key lock, so two
    // concurrent replicators for the same partition can never both build it.
    cfg.partitions
        .materialize_if_vacant(&cfg.topic, cfg.partition, || {
            let dir =
                crate::log_dir::place_partition_dir(&cfg.log_dirs, &cfg.topic, cfg.partition.get());
            std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
            let log =
                Log::open(&dir, cfg.log_settings.clone()).map_err(|e| format!("Log::open: {e}"))?;
            let owning_dir = dir
                .parent()
                .expect("placed partition dir always has a parent log.dir")
                .to_path_buf();
            Ok(spawn_partition_with_replication_target(
                cfg.topic.clone(),
                task_replication_target(cfg),
                cfg.partition,
                owning_dir,
                log,
                cfg.log_dir_status.clone(),
                cfg.producer_state.clone(),
                false,
            ))
        })
}

/// `true` if this task no longer targets the leader and epoch in the committed
/// metadata image.
///
/// The cancellation of a follower-replicator on a leadership change is
/// cooperative. The run loop checks the shutdown token only between fetches.
/// The replicator can thus process an in-flight Fetch response after metadata
/// has selected another target. Applying that stale response can append,
/// truncate, or reset the local log against the wrong leader. Each response and
/// each destructive recovery path rechecks the task's immutable target.
fn replication_target_changed(cfg: &Config) -> bool {
    let image = cfg.controller.current_image();
    image
        .topic(&cfg.topic)
        .is_none_or(|topic| topic.topic_id.as_bytes() != &cfg.topic_id.0)
        || image
            .partition(&cfg.topic, cfg.partition.get())
            .is_none_or(|partition| {
                partition.leader != cfg.leader_node_id || partition.leader_epoch != cfg.leader_epoch
            })
}

fn task_replication_target(cfg: &Config) -> ReplicationTarget {
    ReplicationTarget {
        topic_id: Some(uuid::Uuid::from_bytes(cfg.topic_id.0)),
        leader_node_id: cfg.leader_node_id,
        leader_epoch: cfg.leader_epoch,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::replicator::test_support::{
        LEADER_ID, NODE_ID, PARTITION, TOPIC, WIRE_TOPIC_ID, image_with_leader,
        image_with_topic_id_and_leader, test_config,
    };

    #[test]
    fn replication_target_changed_compares_topic_leader_and_epoch() {
        let cases = [
            (LEADER_ID, krabka_metadata::LeaderEpoch(4), false),
            (NODE_ID, krabka_metadata::LeaderEpoch(4), true),
            (LEADER_ID, krabka_metadata::LeaderEpoch(3), true),
        ];
        for (leader, target_epoch, want) in cases {
            let (mut cfg, _log_dir) = test_config(image_with_leader(leader));
            cfg.leader_epoch = target_epoch;
            assert!(
                replication_target_changed(&cfg) == want,
                "metadata leader {leader}, task epoch {target_epoch:?}"
            );
        }

        let (cfg, _log_dir) = test_config(image_with_topic_id_and_leader(
            uuid::Uuid::new_v4(),
            LEADER_ID,
        ));
        assert!(
            replication_target_changed(&cfg),
            "a recreated topic with the same name and generation is a new target"
        );
    }

    #[tokio::test]
    async fn run_materializes_local_partition_before_observing_cancelled_shutdown() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.shutdown.cancel();
        let partitions = cfg.partitions.clone();

        run(cfg).await;

        let partition = partitions
            .get(TOPIC, PartitionIndex(PARTITION))
            .expect("materialized");
        assert!(
            *partition.replication_target.read().await
                == ReplicationTarget {
                    topic_id: Some(uuid::Uuid::from_bytes(WIRE_TOPIC_ID.0)),
                    leader_node_id: LEADER_ID,
                    leader_epoch: krabka_metadata::LeaderEpoch(4),
                }
        );
    }
}
