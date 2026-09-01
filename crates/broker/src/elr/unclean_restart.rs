//! Withdrawing a broker's eligible-leader-replica membership when it rejoins
//! without proving it stopped gracefully.
//!
//! ELR membership is a claim about a log: "this replica left the ISR while the
//! partition still had `min.insync.replicas` members, so it holds every
//! committed record and may be elected without accepting data loss". A broker
//! that died with an unflushed tail no longer holds what that claim says it
//! does. Its identity is unchanged -- same node id, and in krabka the
//! incarnation id lives in the log dir, so a crash-restart reuses it too --
//! but its log is shorter, and electing it would lose the records between the
//! two.
//!
//! So the withdrawal keys off the clean-shutdown proof rather than off
//! identity. [`crate::clean_shutdown`] holds the proof; this module holds what
//! the controller does when it is missing.
//!
//! ## What Kafka does
//!
//! `ReplicationControlManager.handleBrokerShutdown` in
//! `kafka-metadata-4.3.1.jar` is
//!
//! ```text
//! if (elrEnabled && !isCleanShutdown) {
//!     generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown", -1, -1, brokerId, records,
//!         brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
//!     generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown", -1, -1, brokerId, records,
//!         brokersToElrs.partitionsWithBrokerInElr(brokerId));
//! } else {
//!     generateLeaderAndIsrUpdates("handleBrokerShutdown", brokerId, -1, -1, records,
//!         brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
//! }
//! ```
//!
//! and the fourth argument reaches `PartitionChangeBuilder` as
//! `setUncleanShutdownReplicas`, which `maybePopulateTargetElr` applies as
//!
//! ```text
//! targetElr = candidates − targetIsr − uncleanShutdownReplicas;
//! targetLastKnownElr = (candidates ∪ lastKnownElr) − targetIsr − targetElr;
//! ```
//!
//! So an unclean replica is struck from the ELR and lands in the last-known
//! ELR: the controller stops offering it as a safe election and keeps
//! reporting it as the last replica known to have been complete, which is what
//! an operator falls back to when the partition has no leader at all. This
//! module produces exactly that move. The ISR half of the same rule is not
//! here.
//!
//! Kafka gates the whole thing on `isElrFeatureEnabled`. krabka has no such
//! feature to finalize -- see [`crate::elr::maintain`] -- so the withdrawal is
//! unconditional, the way the rest of krabka's ELR maintenance is.

use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, TopicConfigRecord};

use super::state::TopicElr;
use crate::config_keys::ELIGIBLE_LEADER_REPLICAS;

/// The `V1TopicConfig` records that take `node` out of every ELR it is named
/// in, cluster-wide.
///
/// Empty when the node is in no ELR anywhere, which is the common case: a
/// cluster at Kafka's default `min.insync.replicas` of 1 can never have a
/// non-empty ELR at all. Submit these ahead of the registration record they
/// accompany, the order `ClusterControlManager.registerBroker` uses.
pub(crate) fn withdraw_elr_membership(image: &MetadataImage, node: NodeId) -> Vec<MetadataRecord> {
    // A node id too wide for the wire could never have been published into an
    // ELR value in the first place -- `wire_node_ids` drops it -- so there is
    // nothing to withdraw.
    let Ok(node) = i32::try_from(node.0) else {
        return Vec::new();
    };
    image
        .topics()
        .filter_map(|topic| topic_record(image, &topic.name, node))
        .collect()
}

/// One topic's rewritten config, or `None` when the topic's ELR does not name
/// `node` and so does not move.
fn topic_record(image: &MetadataImage, topic: &str, node: i32) -> Option<MetadataRecord> {
    let before = image.topic_config(topic)?;
    let mut elr = TopicElr::parse(before.get(ELIGIBLE_LEADER_REPLICAS)?);

    let mut withdrawn = false;
    for partition in elr.partitions() {
        let mut state = elr.partition(partition);
        if !state.eligible_leader_replicas.contains(&node) {
            continue;
        }
        state.eligible_leader_replicas.retain(|id| *id != node);
        if !state.last_known_elr.contains(&node) {
            state.last_known_elr.push(node);
            state.last_known_elr.sort_unstable();
        }
        elr.set_partition(partition, state);
        withdrawn = true;
    }
    if !withdrawn {
        return None;
    }

    // Applying a `V1TopicConfig` replaces the topic's whole override map, so
    // the record carries every other override the topic has as well.
    let mut after = before.clone();
    let rendered = elr.render();
    if rendered.is_empty() {
        after.remove(ELIGIBLE_LEADER_REPLICAS);
    } else {
        after.insert(ELIGIBLE_LEADER_REPLICAS.to_string(), rendered);
    }
    Some(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.to_string(),
        overrides: after,
    }))
}

#[cfg(test)]
mod tests;
