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
//! The fencing half is replicated too. Only the controller leader keeps a
//! heartbeat registry, so it publishes what that registry decides as the
//! [`BROKER_FENCED`](crate::config_keys::BROKER_FENCED) broker config (see
//! [`crate::heartbeat::fencing`]), the way Kafka's controller writes
//! `BrokerRegistrationChangeRecord.fenced`. [`unavailable_brokers`] reads it
//! back out of the image, so a request served by a follower answers with the
//! same set as one served by the controller. `DescribeCluster` calls the same
//! helper for its `is_fenced` column, so the two answers cannot drift apart.

use std::collections::HashSet;

use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, NodeId, PartitionRecord};

use crate::broker::Broker;

/// The brokers this node knows to be fenced or past their heartbeat deadline.
///
/// The replicated `broker.fenced` state answers this on every node. The
/// controller leader unions its live registry on top of it: the publication
/// trails that registry by at most one liveness tick, and the node that made
/// the decision must not report less than it already knows.
///
/// `DescribeCluster` reads the same set for `is_fenced` and for the KIP-1073
/// `include_fenced_brokers` filter.
pub(crate) async fn unavailable_brokers(broker: &Broker, image: &MetadataImage) -> HashSet<u64> {
    let mut unavailable = crate::config_keys::fenced_node_ids(image);
    let is_controller = *broker.controller.watch_leader().borrow() == Some(broker.config.node_id);
    if is_controller {
        unavailable.extend(broker.liveness.unavailable_snapshot().await);
    }
    unavailable
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
/// The unassigned directory id is online, as in `DirectoryId.isOnline`,
/// because the owning broker has not reported its `AssignReplicasToDirs` yet
/// and no disk can be blamed for a replica nobody has placed.
///
/// `DirectoryId.isOnline` also reads an empty directory list as "everything
/// on this broker is online", for registrations written before metadata
/// version 3.7-IV2 carried log dirs at all. Krabka has no such registration:
/// a broker publishes an id for every entry of `log.dirs` when it registers,
/// and the only writer that shortens that list is the KIP-112 retire path in
/// [`crate::handlers::broker_heartbeat::failover`]. An empty list here means
/// the broker reported its last surviving directory offline, so a concrete
/// non-nil assignment on it names a dead disk, not a broker that predates
/// directory assignment.
fn has_online_dir(registration: &BrokerRegistrationRecord, directory: Option<uuid::Uuid>) -> bool {
    let Some(directory) = directory else {
        return true;
    };
    directory.is_nil() || registration.log_dirs.contains(&directory)
}

#[cfg(test)]
mod tests;
