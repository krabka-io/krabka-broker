//! Replica placement for the partitions that `CreatePartitions` adds: the
//! site-aware automatic placement, and the validation of an explicit
//! `assignments` list against the live broker set and the topic's
//! replication factor.

use krabka_protocol::owned::create_partitions_request::CreatePartitionsAssignment;
use krabka_raft::NodeId;

use crate::{
    codes,
    site_placement::{SiteBrokerView, stretch_replicas},
};

#[cfg(test)]
mod tests;

/// Resolve the replica list for each newly-added partition.
///
/// `provided` is the caller's `assignments` field. `None` selects the
/// automatic site-aware placement. `Some(...)` is used verbatim, after this
/// function validates it against `brokers` and `rf`.
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
/// order. It returns an `(error_code, error_message)` pair instead when the
/// request is invalid, and the caller stamps that pair into the per-topic
/// result.
pub(super) fn resolve_new_partition_assignments(
    provided: Option<&Vec<CreatePartitionsAssignment>>,
    brokers: &[SiteBrokerView],
    existing: i32,
    new_partition_count: usize,
    rf: i16,
    preferred_site: Option<&str>,
) -> Result<Vec<Vec<NodeId>>, (i16, String)> {
    let rf_usize = usize::try_from(rf).unwrap_or(0);
    if let Some(provided) = provided {
        // Length must match new-partition count. Empty `Some(vec![])` with
        // any new partitions fails here too — matches JVM.
        if provided.len() != new_partition_count {
            return Err((
                codes::INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "assignments.len()={} does not match new partition count={new_partition_count}",
                    provided.len()
                ),
            ));
        }
        let mut out: Vec<Vec<NodeId>> = Vec::with_capacity(new_partition_count);
        for (i, a) in provided.iter().enumerate() {
            if a.broker_ids.len() != rf_usize {
                return Err((
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    format!(
                        "assignment[{i}].broker_ids.len()={} does not match replication_factor={rf}",
                        a.broker_ids.len()
                    ),
                ));
            }
            let mut seen: std::collections::HashSet<i32> = std::collections::HashSet::new();
            let mut replicas: Vec<NodeId> = Vec::with_capacity(rf_usize);
            for b in &a.broker_ids {
                if !seen.insert(*b) {
                    return Err((
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        format!("assignment[{i}] contains duplicate broker id {b}"),
                    ));
                }
                let Ok(b_u64) = u64::try_from(*b) else {
                    return Err((
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        format!("assignment[{i}] references negative broker id {b}"),
                    ));
                };
                if !brokers.iter().any(|broker| broker.node_id == NodeId(b_u64)) {
                    return Err((
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        format!("assignment[{i}] references unknown broker id {b}"),
                    ));
                }
                replicas.push(NodeId(b_u64));
            }
            out.push(replicas);
        }
        Ok(out)
    } else {
        let total = existing
            .checked_add(i32::try_from(new_partition_count).unwrap_or(i32::MAX))
            .unwrap_or(i32::MAX);
        let all = stretch_replicas(brokers, total, rf, preferred_site);
        if all.is_empty() {
            return Err((
                codes::INVALID_REPLICATION_FACTOR,
                format!(
                    "replication_factor={rf} does not fit broker_count={}",
                    brokers.len()
                ),
            ));
        }
        let start = usize::try_from(existing).unwrap_or(0);
        Ok(all.into_iter().skip(start).collect())
    }
}
