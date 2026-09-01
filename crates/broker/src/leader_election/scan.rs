//! The two controller failover scans. Both walk the metadata image once, ask
//! [`failover_one`] about every partition they touch, and turn the answers
//! into a [`FailoverPlan`]. [`compute_failover_changes`] reacts to a dead
//! broker; [`compute_offline_dir_failover_changes`] reacts to a live broker
//! that lost a log directory (KIP-112).

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
use krabka_raft::NodeId;
use tracing::warn;

use super::policy::{FailoverDecision, FailoverPlan, failover_one};
use crate::{
    config_keys::{
        RecoveryStrategy, resolve_recovery_strategy, resolve_unclean_leader_election_enabled,
        witness_node_ids,
    },
    elr::ElrPublisher,
    heartbeat::controller_state::ControllerLivenessState,
};

#[cfg(test)]
mod dead_broker_tests;
#[cfg(test)]
mod offline_dir_tests;

/// Compute the failover `MetadataRecord` changes for `dead` against
/// `image`. Pure: no I/O beyond `liveness.is_alive` lookups. This function is
/// separate so the failover policy, including the KIP-841 unclean toggle, is
/// unit-testable without spinning up a controller.
pub(crate) async fn compute_failover_changes(
    image: &MetadataImage,
    dead: NodeId,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();
    let mut unavailable: Vec<(String, i32)> = Vec::new();
    // Snapshot the alive set once (single lock) rather than taking the
    // liveness lock per ISR/replica entry inside the scan below.
    let alive: std::collections::HashSet<NodeId> = liveness
        .alive_snapshot()
        .await
        .into_iter()
        .map(NodeId)
        .collect();
    // Witness nodes never lead a partition. Build the set once, next to the
    // alive snapshot, so the scan stays one walk over the image.
    let witnesses = witness_node_ids(image);
    // Single O(P) walk over every partition in the image.
    for pr in image.all_partitions() {
        if !pr.replicas.contains(&dead) && !pr.isr.contains(&dead) {
            continue;
        }
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = resolve_unclean_leader_election_enabled(image, &pr.topic);
        match failover_one(pr, dead, &alive, &witnesses, strategy, unclean_enabled) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader = leader.0,
                        "unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                    );
                    // KIP-841: account this election so operators can alert on a
                    // non-zero rate of unclean failovers in their cluster.
                    metrics.record_unclean_leader_election();
                }
                // One source of truth for the bumped epoch: used by both the
                // log line and the emitted record, so the failover tests that
                // assert the incremented `leader_epoch` also pin the logged
                // value (no un-killable log-only arithmetic).
                let new_leader_epoch = pr.leader_epoch.next();
                tracing::info!(
                    topic = %pr.topic,
                    partition = pr.partition,
                    dead = dead.0,
                    old_leader = pr.leader.0,
                    new_leader = leader.0,
                    old_isr = ?pr.isr,
                    new_isr = ?isr,
                    new_leader_epoch = new_leader_epoch.0,
                    unclean,
                    "failover: re-electing partition leader (triggered by dead broker)"
                );
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: new_leader_epoch,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::ShrinkIsr { isr } => {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::Recover(strategy) => {
                // KIP-966: defer to the offset-aware Unclean Recovery Manager —
                // it polls surviving replicas and elects the most complete log.
                recoveries.push((pr.topic.clone(), pr.partition, strategy));
            }
            FailoverDecision::Unavailable => {
                unavailable.push((pr.topic.clone(), pr.partition));
            }
            FailoverDecision::NoChange => {}
        }
    }
    // KIP-966: a failover that shrinks the ISR below min ISR leaves the
    // replicas it dropped eligible to lead, and one that reaches min ISR
    // again clears them.
    ElrPublisher::new(image).extend(&mut changes);
    FailoverPlan {
        changes,
        recoveries,
        unavailable,
    }
}

/// Compute failover changes for partitions whose replica on `broker` lives
/// on a now-offline log directory (`offline_uuids`). KIP-112: a broker stays
/// alive after a disk failure, so the dead-broker scan never fires. This scan
/// does, and the broker's `offline_log_dirs` heartbeat drives it.
///
/// For each affected partition:
/// - if `broker` is the leader, elect a new leader from the alive ISR minus
///   `broker`, drop `broker` from ISR, and bump epoch. The clean / KIP-966 /
///   KIP-841 policy is the same as [`compute_failover_changes`].
/// - if `broker` is a non-leader ISR member, drop it from ISR. No epoch bump.
///
/// Pure and idempotent. After the change `broker` is neither leader nor in
/// ISR, so a repeat yields an empty plan.
pub(crate) async fn compute_offline_dir_failover_changes(
    image: &MetadataImage,
    broker: NodeId,
    offline_uuids: &std::collections::HashSet<uuid::Uuid>,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();
    let alive: std::collections::HashSet<NodeId> = liveness
        .alive_snapshot()
        .await
        .into_iter()
        .map(NodeId)
        .collect();
    let witnesses = witness_node_ids(image);
    for pr in image.all_partitions() {
        let Some(slot) = pr.replicas.iter().position(|n| *n == broker) else {
            continue;
        };
        let on_offline = pr
            .directories
            .get(slot)
            .is_some_and(|d| offline_uuids.contains(d));
        if !on_offline {
            continue;
        }
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = resolve_unclean_leader_election_enabled(image, &pr.topic);
        match failover_one(pr, broker, &alive, &witnesses, strategy, unclean_enabled) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader = leader.0,
                        "offline-dir unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                    );
                    metrics.record_unclean_leader_election();
                }
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch.next(),
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::ShrinkIsr { isr } => {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::Recover(strategy) => {
                recoveries.push((pr.topic.clone(), pr.partition, strategy));
            }
            FailoverDecision::Unavailable => {
                warn!(
                    topic = %pr.topic, partition = pr.partition,
                    "offline dir on leader, no live ISR replica; partition unavailable"
                );
            }
            FailoverDecision::NoChange => {}
        }
    }
    // KIP-966: a failover that shrinks the ISR below min ISR leaves the
    // replicas it dropped eligible to lead, and one that reaches min ISR
    // again clears them.
    ElrPublisher::new(image).extend(&mut changes);
    FailoverPlan {
        changes,
        recoveries,
        // The offline-dir scan runs once per heartbeat that reports the dir,
        // so it warns above and reports nothing here.
        unavailable: Vec::new(),
    }
}
