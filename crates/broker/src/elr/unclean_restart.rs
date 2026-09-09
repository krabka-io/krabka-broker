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
//! module produces exactly that move.
//!
//! ## The ISR half
//!
//! The unclean branch above makes two calls, and the first one -- over
//! `partitionsWithBrokerInIsr` -- matters for a reason the withdrawal alone
//! cannot cover. [`next_partition_elr`](super::maintain) derives the next
//! eligible set from `old_isr ∪ eligible_before`, and this module clears only
//! the second term, so an image whose ISR still names the returning broker
//! would re-derive the very membership withdrawn here the next time that ISR
//! fell below `min.insync.replicas` -- and a partition whose ISR is healthy at
//! registration publishes no ELR to withdraw at all, yet feeds the same
//! `old_isr` into the same derivation later.
//!
//! [`compute_unclean_restart_changes`](crate::leader_election::compute_unclean_restart_changes)
//! is that first call, and it wraps this one: it seeds its batch with these
//! records, removes the broker from every ISR naming it, and finishes with
//! [`ElrPublisher::after_unclean_shutdown`](super::maintain::ElrPublisher::after_unclean_shutdown)
//! so the batch's own recompute cannot re-add what it just removed. The
//! registration handler calls that, not this, whenever the clean-shutdown
//! proof fails.
//!
//! Kafka gates the whole thing on `isElrFeatureEnabled`; krabka applies the
//! same feature-level gate below.

use krabka_metadata::{MetadataImage, MetadataRecord, NodeId};

use crate::features::{ELR_VERSION, feature_enabled};

/// The `V1PartitionElr` records that take `node` out of every ELR it is named
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
    if i32::try_from(node.0).is_err() {
        return Vec::new();
    }
    // Kafka's `handleBrokerShutdown` runs its unclean branch only under
    // `isElrFeatureEnabled()`. Below level 1 the controller publishes no ELR,
    // and a downgrade to 0 has already cleared whatever an earlier level 1
    // left, so there is never anything to withdraw.
    if !feature_enabled(image, ELR_VERSION, 1) {
        return Vec::new();
    }
    let topics: std::collections::BTreeSet<_> = image
        .all_partitions()
        .map(|partition| partition.topic.as_str())
        .collect();
    topics
        .into_iter()
        .flat_map(|topic| crate::elr::TopicElr::of_topic(image, topic).records(topic))
        .filter_map(|record| {
            let MetadataRecord::V1PartitionElr(mut record) = record else {
                unreachable!()
            };
            if !record.eligible_leader_replicas.contains(&node) {
                return None;
            }
            record
                .eligible_leader_replicas
                .retain(|candidate| *candidate != node);
            if !record.last_known_elr.contains(&node) {
                record.last_known_elr.push(node);
                record.last_known_elr.sort_unstable();
            }
            Some(MetadataRecord::V1PartitionElr(record))
        })
        .collect()
}

/// The records that drop every published ELR, cluster-wide.
///
/// `UpdateFeatures` emits them in the same batch as the
/// `eligible.leader.replicas.version` record that finalizes the feature back
/// to level 0, which is Kafka's `generateRecordsForCleaningElr`: the feature
/// going off does not merely stop the controller from maintaining the sets,
/// it clears the ones it already published, so no election path can elect
/// from a membership nothing is keeping true any more.
///
/// Empty when no topic carries the override, which is every cluster that
/// never turned the feature on.
pub(crate) fn clear_published_elr(image: &MetadataImage) -> Vec<MetadataRecord> {
    let topics: std::collections::BTreeSet<_> = image
        .all_partitions()
        .map(|partition| partition.topic.as_str())
        .collect();
    let mut records = Vec::new();
    for topic in topics {
        for record in crate::elr::TopicElr::of_topic(image, topic).records(topic) {
            let MetadataRecord::V1PartitionElr(mut record) = record else {
                unreachable!()
            };
            record.eligible_leader_replicas.clear();
            record.last_known_elr.clear();
            records.push(MetadataRecord::V1PartitionElr(record));
        }
        if let Some(record) = crate::elr::state::without_legacy_elr(image, topic, None) {
            records.push(record);
        }
    }
    records
}

#[cfg(test)]
mod tests;
