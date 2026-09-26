//! Replica placement for the partitions that `CreatePartitions` adds: the
//! site-aware automatic placement, and the validation of an explicit
//! `assignments` list against the live broker set and the topic's
//! replication factor.

use krabka_protocol::owned::create_partitions_request::CreatePartitionsAssignment;
use krabka_raft::NodeId;

use crate::{
    codes,
    handlers::create_topics::{placement_failure_message, validate_manual_partition_assignment},
    site_placement::{SiteBrokerView, stretch_replicas},
};

#[cfg(test)]
mod tests;

/// Resolve the replica list for each newly-added partition.
///
/// `provided` is the caller's `assignments` field. `None` selects the
/// automatic site-aware placement. `Some(...)` is used verbatim, after each
/// list passes Kafka's `validateManualPartitionAssignment` against the
/// registered `brokers` and the topic's replication factor `rf`. The caller
/// has already checked that it holds one list per new partition.
///
/// `existing` is the current partition count. `new_partition_count` is
/// `new_count - existing`. It is always above 0 by the time this helper runs,
/// because the `INVALID_PARTITIONS` check runs earlier.
///
/// On the automatic path the helper places the full `0..new_count` topic and
/// returns only the tail, so the new partitions keep rotating from where the
/// existing ones stopped. The placement of a partition depends only on its
/// index, so the tail holds exactly the lists that a topic of `new_count`
/// partitions would hold. That matches the JVM behavior of
/// `kafka-topics --alter --partitions`.
///
/// It returns one replica list per new partition, in `existing..new_count`
/// order. It returns an `(error_code, error_message)` pair with Kafka's
/// message instead when the request is invalid, and the caller stamps that
/// pair into the per-topic result.
pub(super) fn resolve_new_partition_assignments(
    provided: Option<&Vec<CreatePartitionsAssignment>>,
    brokers: &[SiteBrokerView],
    existing: i32,
    new_partition_count: usize,
    rf: i16,
    preferred_site: Option<&str>,
) -> Result<Vec<Vec<NodeId>>, (i16, String)> {
    if let Some(provided) = provided {
        let registered: Vec<NodeId> = brokers.iter().map(|broker| broker.node_id).collect();
        let replication_factor = usize::try_from(rf).ok();
        return provided
            .iter()
            .map(|assignment| {
                validate_manual_partition_assignment(
                    &assignment.broker_ids,
                    &registered,
                    replication_factor,
                )
                .map_err(|message| (codes::INVALID_REPLICA_ASSIGNMENT, message))
            })
            .collect();
    }
    let total = existing
        .checked_add(i32::try_from(new_partition_count).unwrap_or(i32::MAX))
        .unwrap_or(i32::MAX);
    let all = stretch_replicas(brokers, total, rf, preferred_site);
    if all.is_empty() {
        return Err((
            codes::INVALID_REPLICATION_FACTOR,
            placement_failure_message(rf, brokers.len()),
        ));
    }
    let start = usize::try_from(existing).unwrap_or(0);
    Ok(all.into_iter().skip(start).collect())
}
