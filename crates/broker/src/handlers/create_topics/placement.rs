//! Replica placement for `CreateTopics`: the site-aware automatic placement
//! and the validation of an explicit `assignments` field. The handler asks
//! this module for the replica list of every partition of a new topic, and
//! it reports the error code that comes back on an assignment it rejects.

use krabka_protocol::owned::create_topics_request::CreatableTopic;

use crate::{
    codes,
    config_keys::resolve_broker_witness,
    site_placement::{SiteBrokerView, stretch_replicas},
};

#[cfg(test)]
mod tests;

/// Round-robin replica placement.
///
/// Given a sorted broker set `bs = [b0, b1, …, bk-1]` and a partition
/// count `P`, this returns a `Vec<Vec<NodeId>>` of length `P`, where each
/// inner vec is `R = replication_factor` long. Partition `p`'s leader
/// is `bs[(p) % k]`, and the remaining replicas are `bs[(p + i) % k]` for
/// `i in 1..R`. The caller must guarantee `R <= k`. Otherwise this returns an
/// empty outer vec, and the caller reports `INVALID_REPLICATION_FACTOR`.
///
/// This is the placement of a cluster that declares no site. The site-aware
/// [`stretch_replicas`] calls it for such a cluster, so the two agree there.
pub(crate) fn round_robin_replicas(
    sorted_brokers: &[krabka_raft::NodeId],
    num_partitions: i32,
    replication_factor: i16,
) -> Vec<Vec<krabka_raft::NodeId>> {
    let k = sorted_brokers.len();
    let r = usize::try_from(replication_factor).unwrap_or(0);
    if r == 0 || r > k {
        return Vec::new();
    }
    let p_count = usize::try_from(num_partitions).unwrap_or(0);
    (0..p_count)
        .map(|p| {
            (0..r)
                .map(|i| sorted_brokers[(p + i) % k])
                .collect::<Vec<_>>()
        })
        .collect()
}

fn manual_replicas(
    topic: &CreatableTopic,
    brokers: &[krabka_raft::NodeId],
) -> Result<Vec<Vec<krabka_raft::NodeId>>, i16> {
    if topic.num_partitions != -1 || topic.replication_factor != -1 {
        return Err(codes::INVALID_REQUEST);
    }
    let mut by_partition = std::collections::BTreeMap::new();
    let mut replication_factor = None;
    for assignment in &topic.assignments {
        if by_partition.contains_key(&assignment.partition_index)
            || assignment.broker_ids.is_empty()
        {
            return Err(codes::INVALID_REPLICA_ASSIGNMENT);
        }
        let mut replicas = Vec::with_capacity(assignment.broker_ids.len());
        for &broker_id in &assignment.broker_ids {
            let Ok(broker_id) = u64::try_from(broker_id) else {
                return Err(codes::INVALID_REPLICA_ASSIGNMENT);
            };
            let broker_id = krabka_raft::NodeId(broker_id);
            if !brokers.contains(&broker_id) || replicas.contains(&broker_id) {
                return Err(codes::INVALID_REPLICA_ASSIGNMENT);
            }
            replicas.push(broker_id);
        }
        if replication_factor.is_some_and(|expected| expected != replicas.len()) {
            return Err(codes::INVALID_REPLICA_ASSIGNMENT);
        }
        replication_factor = Some(replicas.len());
        by_partition.insert(assignment.partition_index, replicas);
    }
    if by_partition
        .keys()
        .copied()
        .ne(0..i32::try_from(by_partition.len()).unwrap_or(i32::MAX))
    {
        return Err(codes::INVALID_REPLICA_ASSIGNMENT);
    }
    Ok(by_partition.into_values().collect())
}

/// The registered brokers as the site-aware placement sees them, in node-id
/// order.
///
/// The list keeps the race tolerance of the plain broker list. On a cluster
/// that just started, the image may not hold the self-registration record
/// yet. `local_broker` covers that window: it is this node when this node is
/// itself a broker, and the list then holds it alone. That entry declares no
/// site, so the placement stays the plain Kafka round-robin.
///
/// `local_broker` is `None` on a node whose `process.roles` exclude `broker`.
/// KIP-919 puts `CreateTopics` and `CreatePartitions` on the controller
/// listener, so a controller-only node answers both, and it never
/// self-registers as a broker -- `register_broker` skips it deliberately.
/// Substituting it here would place partitions on a node that hosts no
/// replicas and materialize them locally, leaving topic metadata that nothing
/// can ever serve. With no fallback the list stays empty, the placement
/// cannot be satisfied, and the caller reports `INVALID_REPLICATION_FACTOR`,
/// which is what a Kafka controller with no registered brokers returns.
pub(crate) fn site_broker_views(
    image: &krabka_metadata::MetadataImage,
    local_broker: Option<krabka_raft::NodeId>,
) -> Vec<SiteBrokerView> {
    let mut views = image
        .brokers()
        .map(|broker| SiteBrokerView {
            node_id: broker.node_id,
            site: broker.rack.clone(),
            is_witness: resolve_broker_witness(image, broker.node_id),
        })
        .collect::<Vec<_>>();
    if let (true, Some(node_id)) = (views.is_empty(), local_broker) {
        views.push(SiteBrokerView {
            node_id,
            site: None,
            is_witness: false,
        });
    }
    views.sort_by_key(|view| view.node_id);
    views
}

/// The replica list of each partition of a new topic.
///
/// An explicit `assignments` field wins, as it does in Kafka. The handler
/// then takes the caller's lists verbatim, after [`manual_replicas`]
/// validates them. Without that field the placement is automatic and
/// site-aware: see [`stretch_replicas`].
///
/// The result is an empty outer vec when the automatic placement cannot
/// satisfy the request, and the caller reports `INVALID_REPLICATION_FACTOR`.
/// An invalid explicit assignment gives the error code instead.
pub(super) fn resolve_assignments(
    topic: &CreatableTopic,
    brokers: &[SiteBrokerView],
    preferred_site: Option<&str>,
) -> Result<Vec<Vec<krabka_raft::NodeId>>, i16> {
    if topic.assignments.is_empty() {
        return Ok(stretch_replicas(
            brokers,
            topic.num_partitions,
            topic.replication_factor,
            preferred_site,
        ));
    }
    let node_ids = brokers
        .iter()
        .map(|broker| broker.node_id)
        .collect::<Vec<_>>();
    manual_replicas(topic, &node_ids)
}
