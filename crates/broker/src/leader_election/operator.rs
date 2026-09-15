//! The operator-triggered elections. [`select_new_leader_for_partition`]
//! serves the KIP-460 `ElectLeaders` request and the preferred-leader
//! rebalance. It is pure: the caller submits the returned record.

use std::collections::HashSet;

use krabka_metadata::PartitionRecord;
use krabka_raft::NodeId;

#[cfg(test)]
mod tests;

/// Operator-triggered election type per KIP-460.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectionType {
    /// Move leadership back to the first replica in `replicas[]` if it's
    /// alive and in the ISR. This is safe: no data loss is possible.
    Preferred,
    /// Allow election outside the ISR when every ISR member is dead.
    /// Operator has accepted the possible-data-loss risk.
    Unclean,
}

/// Reasons `select_new_leader_for_partition` may refuse to elect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectError {
    UnknownTopicOrPartition,
    PreferredAlreadyLeader,
    ElectionNotNeeded,
    PreferredNotInIsr,
    PreferredNotAlive,
    /// `replicas[0]` carries the witness role. A witness serves no client, so
    /// it can never take leadership. The KIP-460 auto-rebalance skips the
    /// partition, and `kafka-leader-election` reports the refusal.
    PreferredIsWitness,
    NoEligibleReplica,
    /// A safety-relevant metadata epoch reached its wire maximum.
    EpochExhausted,
}

/// Operator-triggered single-partition election. Returns the new
/// `PartitionRecord` ready to submit, or an `ElectError`.
///
/// `witnesses` is the set of witness nodes. No election of either type can
/// give leadership to a witness. The caller builds the set once per scan.
///
/// `alive` is the set of broker ids this election may elect, and the caller
/// chooses where it comes from. The controller's own loops read their
/// heartbeat registry. `ElectLeaders` cannot: `controllerId` names a rotating
/// unfenced broker rather than the quorum leader, so an `AdminClient` sends
/// the election to whichever broker that rotation last named, and only the
/// controller keeps a registry. That path passes the replicated set from
/// [`live_brokers`](crate::handlers::offline_replicas::live_brokers) instead,
/// which every node computes the same way.
///
/// Pure: no I/O, no panics. The caller must submit the returned record
/// through the controller.
pub(crate) fn select_new_leader_for_partition(
    image: &krabka_metadata::MetadataImage,
    alive: &HashSet<u64>,
    witnesses: &std::collections::HashSet<NodeId>,
    topic: &str,
    partition: i32,
    election: ElectionType,
) -> Result<PartitionRecord, ElectError> {
    let pr = image
        .partition(topic, partition)
        .ok_or(ElectError::UnknownTopicOrPartition)?;
    match election {
        ElectionType::Preferred => {
            let preferred = *pr
                .replicas
                .first()
                .ok_or(ElectError::UnknownTopicOrPartition)?;
            // Site-aware placement can put a witness first in `replicas`. The
            // preferred replica is then never electable, and the caller must
            // skip the partition rather than move leadership to it.
            if witnesses.contains(&preferred) {
                return Err(ElectError::PreferredIsWitness);
            }
            if pr.leader == preferred {
                return Err(ElectError::PreferredAlreadyLeader);
            }
            if !pr.isr.contains(&preferred) {
                return Err(ElectError::PreferredNotInIsr);
            }
            if !alive.contains(&preferred.0) {
                return Err(ElectError::PreferredNotAlive);
            }
            let (partition_epoch, leader_epoch) = crate::metadata_epoch::next_partition_change(
                pr.partition_epoch,
                pr.leader_epoch,
                true,
            )
            .ok_or(ElectError::EpochExhausted)?;
            Ok(PartitionRecord {
                topic: pr.topic.clone(),
                partition: pr.partition,
                leader: preferred,
                replicas: pr.replicas.clone(),
                isr: pr.isr.clone(),
                leader_epoch,
                adding_replicas: pr.adding_replicas.clone(),
                removing_replicas: pr.removing_replicas.clone(),
                directories: pr.directories.clone(),
                partition_epoch,
            })
        }
        ElectionType::Unclean => {
            // Bail if any ISR member is alive — UNCLEAN is meant for
            // catastrophic ISR loss, not routine rebalances. A live witness
            // does not count here: it cannot lead, so it does not make the
            // partition available, and it must not block the operator who
            // accepts the data loss.
            for &n in &pr.isr {
                if !witnesses.contains(&n) && alive.contains(&n.0) {
                    return Err(ElectError::ElectionNotNeeded);
                }
            }
            // Find the first alive replica that can serve clients, in or out
            // of ISR.
            for &n in &pr.replicas {
                if !witnesses.contains(&n) && alive.contains(&n.0) {
                    let (partition_epoch, leader_epoch) =
                        crate::metadata_epoch::next_partition_change(
                            pr.partition_epoch,
                            pr.leader_epoch,
                            true,
                        )
                        .ok_or(ElectError::EpochExhausted)?;
                    return Ok(PartitionRecord {
                        topic: pr.topic.clone(),
                        partition: pr.partition,
                        leader: n,
                        replicas: pr.replicas.clone(),
                        isr: vec![n],
                        leader_epoch,
                        adding_replicas: pr.adding_replicas.clone(),
                        removing_replicas: pr.removing_replicas.clone(),
                        directories: pr.directories.clone(),
                        partition_epoch,
                    });
                }
            }
            Err(ElectError::NoEligibleReplica)
        }
    }
}
