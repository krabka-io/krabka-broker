//! The two controller failover scans. Both walk the metadata image once, ask
//! [`failover_one`] about every partition they touch, and turn the answers
//! into a [`FailoverPlan`]. [`compute_failover_changes`] reacts to a dead
//! broker; [`compute_offline_dir_failover_changes`] reacts to a live broker
//! that lost a log directory (KIP-112).

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
use krabka_raft::NodeId;
use tracing::warn;

use super::policy::{FailoverDecision, FailoverPlan, failover_one, unclean_restart_one};
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
#[cfg(test)]
mod unclean_restart_tests;

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
                changes.push(elected(pr, leader, isr, new_leader_epoch));
            }
            FailoverDecision::ShrinkIsr { isr } => {
                changes.push(shrunk(pr, isr));
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
                changes.push(elected(pr, leader, isr, pr.leader_epoch.next()));
            }
            FailoverDecision::ShrinkIsr { isr } => {
                changes.push(shrunk(pr, isr));
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

/// The failover changes a broker that has just re-registered under a new
/// incarnation id needs, in the order they apply.
///
/// This is Apache Kafka's `handleBrokerUncleanShutdown`, whose two
/// `generateLeaderAndIsrUpdates` calls -- read out of
/// `kafka-metadata-4.3.1.jar` -- are the two halves of one answer to one
/// event: eligibility, and the ISR the next eligibility is derived from.
/// [`records_for_restarted_broker`](crate::elr::records_for_restarted_broker)
/// is the `partitionsWithBrokerInElr` call and comes first, so a replay that
/// stops mid-batch has already stopped trusting the returning log rather than
/// not yet started. [`unclean_restart_one`] is the
/// `partitionsWithBrokerInIsr` call. Then the publisher runs over the whole
/// batch with the broker named as an unclean-shutdown replica, which is what
/// stops the ISR removals this batch just made from deriving the broker
/// straight back into the eligible sets the first half withdrew it from.
///
/// The plan is otherwise shaped exactly like [`compute_failover_changes`]'s,
/// and the caller drives `recoveries` and `unavailable` the same way, because
/// a partition the returning broker was leading is a partition with a dead
/// leader whichever event the controller noticed first.
pub(crate) async fn compute_unclean_restart_changes(
    image: &MetadataImage,
    returning: NodeId,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let mut changes = crate::elr::records_for_restarted_broker(image, returning);
    let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();
    let mut unavailable: Vec<(String, i32)> = Vec::new();
    let alive: std::collections::HashSet<NodeId> = liveness
        .alive_snapshot()
        .await
        .into_iter()
        .map(NodeId)
        .collect();
    let witnesses = witness_node_ids(image);
    for pr in image.all_partitions() {
        if pr.leader != returning && !pr.isr.contains(&returning) {
            continue;
        }
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = resolve_unclean_leader_election_enabled(image, &pr.topic);
        match unclean_restart_one(pr, returning, &alive, &witnesses, strategy, unclean_enabled) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader = leader.0,
                        "unclean leader election: returning broker led an empty-ISR partition (possible data loss)"
                    );
                    metrics.record_unclean_leader_election();
                }
                let new_leader_epoch = pr.leader_epoch.next();
                tracing::info!(
                    topic = %pr.topic,
                    partition = pr.partition,
                    returning = returning.0,
                    old_leader = pr.leader.0,
                    new_leader = leader.0,
                    old_isr = ?pr.isr,
                    new_isr = ?isr,
                    new_leader_epoch = new_leader_epoch.0,
                    unclean,
                    "unclean restart: re-electing partition leader"
                );
                changes.push(elected(pr, leader, isr, new_leader_epoch));
            }
            FailoverDecision::ShrinkIsr { isr } => {
                tracing::info!(
                    topic = %pr.topic,
                    partition = pr.partition,
                    returning = returning.0,
                    old_isr = ?pr.isr,
                    new_isr = ?isr,
                    "unclean restart: dropping returning broker from ISR"
                );
                changes.push(shrunk(pr, isr));
            }
            FailoverDecision::Recover(strategy) => {
                recoveries.push((pr.topic.clone(), pr.partition, strategy));
            }
            FailoverDecision::Unavailable => {
                unavailable.push((pr.topic.clone(), pr.partition));
            }
            FailoverDecision::NoChange => {}
        }
    }
    // KIP-966, and the reason this function exists: the ISR removals above
    // are the candidate set the next eligibility is derived from, so the
    // broker they remove has to be excluded from that derivation too.
    ElrPublisher::after_unclean_shutdown(image, returning).extend(&mut changes);
    FailoverPlan {
        changes,
        recoveries,
        unavailable,
    }
}

/// The `V1Partition` record that moves leadership of `pr` to `leader` with
/// `isr`, at `leader_epoch`.
fn elected(
    pr: &PartitionRecord,
    leader: NodeId,
    isr: Vec<NodeId>,
    leader_epoch: krabka_metadata::LeaderEpoch,
) -> MetadataRecord {
    changed(pr, leader, isr, leader_epoch)
}

/// The `V1Partition` record that installs `isr` on `pr` and leaves its leader
/// and leader epoch alone. Kafka advances the partition epoch and not the
/// leader epoch for an ISR-only change, so a follower's fetch position
/// survives it.
fn shrunk(pr: &PartitionRecord, isr: Vec<NodeId>) -> MetadataRecord {
    changed(pr, pr.leader, isr, pr.leader_epoch)
}

/// `pr` with `leader`, `isr` and `leader_epoch` installed, its partition
/// epoch advanced, and every other field carried over. Every caller is
/// changing the partition, so the partition epoch always moves.
fn changed(
    pr: &PartitionRecord,
    leader: NodeId,
    isr: Vec<NodeId>,
    leader_epoch: krabka_metadata::LeaderEpoch,
) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader,
        replicas: pr.replicas.clone(),
        isr,
        leader_epoch,
        adding_replicas: pr.adding_replicas.clone(),
        removing_replicas: pr.removing_replicas.clone(),
        directories: pr.directories.clone(),
        partition_epoch: pr.partition_epoch + 1,
    })
}
