//! Creation of the `__krabka_audit` internal topic.
//!
//! The audit topic is bootstrapped on its own schedule, separate from
//! `__consumer_offsets`, because it is optional and it places one partition on
//! every registered broker instead of spreading a fixed partition count.

use std::sync::Arc;

use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use krabka_raft::RaftError;

use crate::{config::BrokerConfig, error::BrokerError};

/// Create `__krabka_audit` with one partition per registered broker at RF=1.
///
/// Broker-affinity: the i-th broker in ascending node-id order leads partition
/// `i`. Each broker therefore leads exactly one audit partition and writes to
/// it locally.
///
/// The function is idempotent. It treats a `TopicExists` error from the
/// controller as a success, because another broker or a restart already
/// created the topic.
///
/// Only the quorum leader submits the metadata records. This matches the
/// `__consumer_offsets` bootstrap path and prevents TOCTOU duplicate-id races.
/// Followers submit nothing. The leader's records replicate into their image
/// through the normal raft log.
///
/// The function returns `Ok(())` at once when `config.audit_enabled` is
/// `false`.
pub async fn bootstrap_audit_topic(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
) -> Result<(), BrokerError> {
    if !config.audit_enabled {
        return Ok(());
    }

    // Only the quorum leader submits; copy out the leader id before any `.await`
    // so we don't hold the `Ref` across an await point.
    let am_leader = *controller.watch_leader().borrow() == Some(config.node_id);
    if !am_leader {
        return Ok(());
    }

    // Already created (idempotent restart or another leader beat us).
    if controller
        .current_image()
        .topic(&config.audit_topic)
        .is_some()
    {
        return Ok(());
    }

    let image = controller.current_image();
    let mut brokers: Vec<krabka_raft::NodeId> = image.brokers().map(|b| b.node_id).collect();
    drop(image);
    if brokers.is_empty() {
        brokers.push(config.node_id);
    }
    brokers.sort_unstable();

    let num_partitions = i32::try_from(brokers.len()).unwrap_or(1);
    // RF=1: partition i → brokers[i % len] as sole replica/leader.
    // Use the crate-internal round_robin helper; falls back to explicit
    // per-broker assignment when brokers.len() == num_partitions (the common
    // single-broker test case also satisfies this).
    let assignments =
        crate::handlers::create_topics::round_robin_replicas(&brokers, num_partitions, 1);

    let mut records = Vec::with_capacity(1 + usize::try_from(num_partitions).unwrap_or(0));
    records.push(MetadataRecord::V1Topic(TopicRecord {
        name: config.audit_topic.clone(),
        topic_id: uuid::Uuid::new_v4(),
        partitions: num_partitions,
        replication_factor: 1,
    }));
    for (p, replicas) in assignments.iter().enumerate() {
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: config.audit_topic.clone(),
            partition: i32::try_from(p).expect("audit partition index overflows i32"),
            leader: replicas[0],
            replicas: replicas.clone(),
            isr: replicas.clone(),
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }

    match controller.submit_change(records).await {
        // Idempotent: another broker / a restart already created it.
        Ok(_) | Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => Ok(()),
        Err(e) => Err(BrokerError::Startup(e.to_string())),
    }
}
