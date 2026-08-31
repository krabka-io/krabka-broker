//! The `offlineReplicas` projection that `Metadata` and
//! `DescribeTopicPartitions` share (KIP-112 / KIP-858).
//!
//! `kafka-topics --describe --unavailable-partitions` and
//! `--under-replicated-partitions`, Cruise Control, Burrow and every dashboard
//! built on the `AdminClient` read this list. Kafka computes it in
//! `KRaftMetadataCache.getOfflineReplicas`: a replica is offline when its
//! broker has no registration in the metadata image, when that broker is
//! fenced, or when the log directory that holds the replica is not among the
//! broker's online directories.
//!
//! Krabka keeps the directory half of that state in the same place Kafka does.
//! [`BrokerRegistrationRecord::log_dirs`] is the broker's *online* directory
//! set, and the controller trims a reported-offline directory out of it while
//! it runs the KIP-112 failover (see
//! [`crate::handlers::broker_heartbeat::failover`]), exactly as Kafka's
//! `ReplicationControlManager.handleDirectoriesOffline` emits a
//! `BrokerRegistrationChangeRecord` carrying the surviving directories. The
//! offline set of a broker is therefore the directories named by partition
//! assignments that its registration no longer lists.
//!
//! Fencing state is not in the Krabka metadata image; the controller holds it
//! in its heartbeat liveness registry. [`unavailable_brokers`] reads that
//! registry on the controller leader and reports an empty set elsewhere.
//! `DescribeCluster` calls the same helper for its `is_fenced` column, so the
//! two answers cannot drift apart.

use std::collections::HashSet;

use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, NodeId, PartitionRecord};

use crate::broker::Broker;

/// The brokers this node knows to be fenced or past their heartbeat deadline.
///
/// Only the controller leader keeps a heartbeat registry, so a request served
/// by any other node yields an empty set and the offline projection falls back
/// to registration and directory state, both of which are quorum-replicated.
///
/// `DescribeCluster` reads the same set for `is_fenced` and for the KIP-1073
/// `include_fenced_brokers` filter.
pub(crate) async fn unavailable_brokers(broker: &Broker) -> HashSet<u64> {
    let is_controller = *broker.controller.watch_leader().borrow() == Some(broker.config.node_id);
    if is_controller {
        broker.liveness.unavailable_snapshot().await
    } else {
        HashSet::new()
    }
}

/// The offline replicas of `partition`, in replica order.
///
/// `unavailable` comes from [`unavailable_brokers`]. The result is the wire
/// value for `MetadataResponsePartition.offline_replicas` and
/// `DescribeTopicPartitionsResponsePartition.offline_replicas`.
pub(crate) fn offline_replicas(
    image: &MetadataImage,
    partition: &PartitionRecord,
    unavailable: &HashSet<u64>,
) -> Vec<i32> {
    partition
        .replicas
        .iter()
        .enumerate()
        .filter(|&(slot, &replica)| {
            let directory = partition.directories.get(slot).copied();
            is_offline(image, unavailable, replica, directory)
        })
        .map(|(_, replica)| i32::try_from(replica.0).unwrap_or(i32::MAX))
        .collect()
}

/// Whether the replica `replica` holds on `directory` is offline.
fn is_offline(
    image: &MetadataImage,
    unavailable: &HashSet<u64>,
    replica: NodeId,
    directory: Option<uuid::Uuid>,
) -> bool {
    // Unregistered: Kafka reports the replica offline rather than dropping it,
    // so the id still appears in `replica_nodes` next to it.
    let Some(registration) = image.broker(replica) else {
        return true;
    };
    unavailable.contains(&replica.0) || !has_online_dir(registration, directory)
}

/// Whether `directory` is one of the broker's online log directories.
///
/// Mirrors `DirectoryId.isOnline`: the unassigned directory id is online
/// because the owning broker has not reported its `AssignReplicasToDirs` yet,
/// and an empty registration directory list means the broker predates
/// directory assignment, so every replica on it counts as online.
fn has_online_dir(registration: &BrokerRegistrationRecord, directory: Option<uuid::Uuid>) -> bool {
    let Some(directory) = directory else {
        return true;
    };
    directory.is_nil()
        || registration.log_dirs.is_empty()
        || registration.log_dirs.contains(&directory)
}

#[cfg(test)]
mod tests;
