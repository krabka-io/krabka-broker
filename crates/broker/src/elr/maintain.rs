//! The controller half of KIP-966: recompute a partition's ELR whenever a
//! change to it is about to be submitted, and publish the result.
//!
//! ## The rules, and where they come from
//!
//! Apache Kafka keeps the rules in `PartitionChangeBuilder`, which runs
//! `maybePopulateTargetElr` while it assembles one `PartitionChangeRecord`.
//! Reconstructed from `kafka-metadata-4.3.1.jar`, that method is
//!
//! ```text
//! if (targetIsr.size() >= minISR) { targetElr = []; targetLastKnownElr = []; return; }
//! candidates = elr ∪ isr;
//! targetElr = candidates − targetIsr − uncleanShutdownReplicas;
//! targetLastKnownElr = (candidates ∪ lastKnownElr) − targetIsr − targetElr;
//! ```
//!
//! with `elr`, `isr` and `lastKnownElr` read from the partition as it stands
//! before the change and `targetIsr` the ISR the change installs. Its sibling
//! `maybeUpdateRecordElr` clears both sets outright when the change also
//! installs an ISR of its own, which in Kafka happens only on an unclean
//! election: the replica it elects need not hold every committed record, so
//! nothing that came before is still known to be complete.
//!
//! [`next_partition_elr`] is those rules, `uncleanShutdownReplicas` included:
//! [`ElrPublisher::after_unclean_shutdown`] is how a batch names one. krabka
//! subtracts a second set Kafka does not, the replicas that are no longer in
//! the replica set, because the partition can no longer elect them. One
//! difference is krabka's: the unclean-election test is "the elected leader
//! was in neither the previous ISR nor the ELR" rather than "the change
//! carries an ISR", because krabka's election paths always carry one.
//!
//! Kafka reaches `uncleanShutdownReplicas` from
//! `ReplicationControlManager.handleBrokerShutdown`, which is a broker
//! returning under a new incarnation id. krabka answers that event in two
//! places, one per Kafka call: [`records_for_restarted_broker`] withdraws the
//! published membership, and
//! [`compute_unclean_restart_changes`](crate::leader_election::compute_unclean_restart_changes)
//! drops the broker from the ISRs that still name it and runs this publisher
//! over the result with the broker excluded.
//!
//! Kafka gates the whole thing on the `eligible.leader.replicas.version`
//! feature. krabka has no such feature to finalize -- the registry in
//! `krabka_metadata` does not carry it -- so the state is maintained
//! unconditionally, which is also what the read side already assumes.
//!
//! ## What gets published
//!
//! [`ElrPublisher::extend`] reads the partition changes a controller path has
//! already built, works out the ELR each of their partitions ends up with,
//! and appends one `V1TopicConfig` per topic whose rendered value moved.
//! Applying a `V1TopicConfig` replaces a topic's whole override map, so the
//! appended record carries every other override the topic has as well; it is
//! built from the batch's own config record for that topic when the batch has
//! one, and from the image otherwise.
//!
//! Nothing is appended when nothing moved, which is the common case: a
//! cluster that leaves `min.insync.replicas` at Kafka's default of 1 can
//! never have a non-empty ELR, because an ISR that reached zero members has
//! no partition record to reach it with.

use std::collections::{BTreeMap, BTreeSet};

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord};

use super::state::{PartitionElr, TopicElr, wire_node_ids};
use crate::config_keys::{ELIGIBLE_LEADER_REPLICAS, effective_min_insync_replicas};

/// Appends the ELR state implied by a batch of controller changes to that
/// batch.
///
/// It borrows the image the batch was computed against, so the "before" side
/// of every rule is the partition as the controller saw it.
pub(crate) struct ElrPublisher<'a> {
    image: &'a MetadataImage,
    /// Kafka's `uncleanShutdownReplicas`: ids the batch may not derive back
    /// into an eligible set, whatever the ISR it is leaving says.
    unclean_shutdown: BTreeSet<i32>,
}

impl<'a> ElrPublisher<'a> {
    /// Read ELR state against `image`, the metadata as it stands before the
    /// batch applies.
    pub(crate) fn new(image: &'a MetadataImage) -> Self {
        Self {
            image,
            unclean_shutdown: BTreeSet::new(),
        }
    }

    /// Read ELR state against `image` for a batch that is reacting to `node`
    /// coming back from an unclean stop, so that no partition in the batch
    /// derives `node` back into its eligible set.
    ///
    /// This is Kafka's
    /// `PartitionChangeBuilder.setUncleanShutdownReplicas(List.of(brokerId))`,
    /// which `ReplicationControlManager.handleBrokerShutdown` sets on both of
    /// the `generateLeaderAndIsrUpdates` calls it makes for an unclean
    /// shutdown. Read out of `kafka-metadata-4.3.1.jar`,
    /// `maybePopulateTargetElr` subtracts the list from `targetElr` and from
    /// nothing else, so an excluded id still lands in `targetLastKnownElr`:
    /// it *was* the last replica known to hold every committed record, and
    /// that stays true even though the process holding the log now is a
    /// different one.
    ///
    /// Without it, [`next_partition_elr`] would re-derive `node` from the
    /// `old_isr` half of its candidate set -- the very ISR the batch is
    /// removing it from -- and the withdrawal would not survive its own
    /// batch.
    pub(crate) fn after_unclean_shutdown(
        image: &'a MetadataImage,
        node: krabka_metadata::NodeId,
    ) -> Self {
        Self {
            image,
            // An id too wide for the wire can never be named in a published
            // value, so there is nothing to exclude.
            unclean_shutdown: i32::try_from(node.0).ok().into_iter().collect(),
        }
    }

    /// Append to `changes` the `V1TopicConfig` records that carry the ELR
    /// state its partition changes imply.
    ///
    /// Call it once, after a controller path has built its whole batch and
    /// before it submits: the records are appended after the partition
    /// changes they describe, so a replay that stops between the two sees a
    /// stale ELR rather than one that names a partition state no record ever
    /// established.
    pub(crate) fn extend(&self, changes: &mut Vec<MetadataRecord>) {
        let published = self.topic_config_records(changes);
        changes.extend(published);
    }

    /// The `V1TopicConfig` records `extend` appends. Split out so the
    /// decision can be tested without the append.
    fn topic_config_records(&self, changes: &[MetadataRecord]) -> Vec<MetadataRecord> {
        let batch = Batch::of(changes);
        batch
            .partitions
            .iter()
            .filter(|(topic, _)| !batch.deleted.contains(*topic))
            .filter_map(|(topic, partitions)| self.topic_record(&batch, topic, partitions))
            .collect()
    }

    /// The one topic's `V1TopicConfig`, or `None` when its rendered ELR value
    /// is the one the topic already carries.
    fn topic_record(
        &self,
        batch: &Batch<'_>,
        topic: &str,
        partitions: &BTreeMap<i32, &PartitionRecord>,
    ) -> Option<MetadataRecord> {
        let before = batch.overrides(self.image, topic);
        let mut elr = before
            .get(ELIGIBLE_LEADER_REPLICAS)
            .map_or_else(TopicElr::default, |value| TopicElr::parse(value));

        for (partition, record) in partitions {
            let previous = self.image.partition(topic, *partition);
            let next = next_partition_elr(
                self.image,
                previous,
                record,
                &elr.partition(*partition),
                &self.unclean_shutdown,
            );
            elr.set_partition(*partition, next);
        }

        let mut after = before.clone();
        let rendered = elr.render();
        if rendered.is_empty() {
            after.remove(ELIGIBLE_LEADER_REPLICAS);
        } else {
            after.insert(ELIGIBLE_LEADER_REPLICAS.to_string(), rendered);
        }
        if after == before {
            return None;
        }
        Some(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: topic.to_string(),
            overrides: after,
        }))
    }
}

/// The parts of a change batch the publisher reads: the last partition record
/// per partition, the last topic-config record per topic, and the topics the
/// batch deletes.
struct Batch<'a> {
    partitions: BTreeMap<&'a str, BTreeMap<i32, &'a PartitionRecord>>,
    configs: BTreeMap<&'a str, &'a BTreeMap<String, String>>,
    deleted: BTreeSet<&'a str>,
}

impl<'a> Batch<'a> {
    /// Index `changes`. Later records win, which is the order the image
    /// applies them in.
    fn of(changes: &'a [MetadataRecord]) -> Self {
        let mut batch = Self {
            partitions: BTreeMap::new(),
            configs: BTreeMap::new(),
            deleted: BTreeSet::new(),
        };
        for change in changes {
            match change {
                MetadataRecord::V1Partition(record) => {
                    batch
                        .partitions
                        .entry(record.topic.as_str())
                        .or_default()
                        .insert(record.partition, record);
                }
                MetadataRecord::V1TopicConfig(record) => {
                    batch
                        .configs
                        .insert(record.topic.as_str(), &record.overrides);
                }
                MetadataRecord::V1DeleteTopic(record) => {
                    batch.deleted.insert(record.name.as_str());
                }
                _ => {}
            }
        }
        batch
    }

    /// The override map the topic ends the batch with, before the ELR key is
    /// rewritten. A `V1TopicConfig` in the batch replaces the topic's whole
    /// map, so one there wins over the image outright.
    fn overrides(&self, image: &MetadataImage, topic: &str) -> BTreeMap<String, String> {
        self.configs.get(topic).map_or_else(
            || image.topic_config(topic).cloned().unwrap_or_default(),
            |overrides| (*overrides).clone(),
        )
    }
}

/// The ELR one partition ends up with when `next` applies.
///
/// `previous` is the partition as the image holds it, `None` for a partition
/// the batch creates. `published` is the ELR the topic config currently
/// carries for it. `unclean_shutdown` is Kafka's `uncleanShutdownReplicas`:
/// replicas the batch has just stopped trusting, which no rule here may make
/// eligible again.
fn next_partition_elr(
    image: &MetadataImage,
    previous: Option<&PartitionRecord>,
    next: &PartitionRecord,
    published: &PartitionElr,
    unclean_shutdown: &BTreeSet<i32>,
) -> PartitionElr {
    // A partition the batch creates has no history, so no replica of it is
    // known to hold records the ISR does not.
    let Some(previous) = previous else {
        return PartitionElr::default();
    };

    let new_isr = wire_id_set(&next.isr);
    let old_isr = wire_id_set(&previous.isr);
    let replicas = wire_id_set(&next.replicas);
    let eligible_before: BTreeSet<i32> =
        published.eligible_leader_replicas.iter().copied().collect();
    let last_known_before: BTreeSet<i32> = published.last_known_elr.iter().copied().collect();

    // An election that installs a leader from neither the ISR nor the ELR may
    // have dropped committed records, so no earlier replica is still known to
    // be complete. Kafka reaches the same state through
    // `maybeUpdateRecordElr`, which clears both sets when a change carries an
    // ISR of its own -- in Kafka only an unclean election does.
    let unclean = i32::try_from(next.leader.0)
        .is_ok_and(|id| !old_isr.contains(&id) && !eligible_before.contains(&id));

    // KIP-966's healthy state: an ISR that meets min ISR is on its own enough
    // to hold every committed record, so nothing outside it needs remembering.
    let min_isr = effective_min_insync_replicas(image, &next.topic, next.replicas.len());
    if unclean || new_isr.len() >= min_isr {
        return PartitionElr::default();
    }

    // Everything that held every committed record before the change: the ISR
    // it is leaving plus whatever was already eligible.
    let complete: BTreeSet<i32> = old_isr.union(&eligible_before).copied().collect();
    // Kafka drops the replicas its caller named as unclean-shutdown ones, and
    // so does this: `unclean_shutdown` is that list. krabka drops replicas
    // that left the replica set as well, for a related reason: the partition
    // can no longer elect them, so calling them eligible would offer an
    // election that cannot happen.
    let eligible: BTreeSet<i32> = complete
        .difference(&new_isr)
        .copied()
        .filter(|id| replicas.contains(id) && !unclean_shutdown.contains(id))
        .collect();
    // What is left is what was last known to be complete but is not eligible
    // now -- exactly the replicas the previous filter dropped, plus any the
    // topic already carried that have not rejoined the ISR.
    let last_known: BTreeSet<i32> = complete
        .union(&last_known_before)
        .copied()
        .filter(|id| !new_isr.contains(id) && !eligible.contains(id))
        .collect();

    PartitionElr {
        eligible_leader_replicas: eligible.into_iter().collect(),
        last_known_elr: last_known.into_iter().collect(),
    }
}

/// The records that drop `node` from every partition's published ELR, for a
/// broker that has re-registered under a new incarnation id.
///
/// # Membership is a claim about one log, not about a node id
///
/// A replica becomes eligible because the log it held when it left the ISR
/// held every committed record. A broker that comes back as a new process need
/// not hold that log: an unclean stop drops the unflushed tail, a replaced
/// disk starts empty, and the id it registers under says nothing about either.
/// Left published, that membership makes
/// [`select_leader`](crate::unclean_recovery::select_leader) elect the
/// returning broker ahead of a longer surviving log *and* report the election
/// as lossless -- which is worse than the loss on its own, because the
/// break-glass check and the unclean-election meter both read that basis, so
/// the wrong answer hides itself.
///
/// Apache Kafka draws the same conclusion from the same event.
/// `ClusterControlManager.registerBroker`, read out of
/// `kafka-metadata-4.3.1.jar`, treats a registration whose incarnation id
/// differs from the one it holds as a shutdown and calls
/// `ReplicationControlManager.handleBrokerShutdown(brokerId, isCleanShutdown,
/// records)`, which is
///
/// ```text
/// if (featureControl.isElrFeatureEnabled() && !isCleanShutdown) {
///     generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown", -1, -1, brokerId,
///         records, brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
///     generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown", -1, -1, brokerId,
///         records, brokersToElrs.partitionsWithBrokerInElr(brokerId));
/// } else { ... }
/// ```
///
/// where that third int is the `uncleanShutdownReplicas` the builder subtracts
/// from `targetElr`. `isCleanShutdown` is `apiVersion >= 3 &&
/// registration.epoch() == request.previousBrokerEpoch()`, so a controller
/// that cannot prove the shutdown was clean assumes it was not. krabka can
/// prove it for no restart at all -- nothing persists a broker epoch across a
/// stop or checks one at registration -- so every returning incarnation takes
/// that branch here.
///
/// # This is one of Kafka's two halves, and the other one is here too
///
/// Kafka's unclean branch above makes two calls, not one, and the first is
/// over `partitionsWithBrokerInIsr`. This function is the second call. The
/// first is
/// [`compute_unclean_restart_changes`](crate::leader_election::compute_unclean_restart_changes),
/// which the registration handler runs in the same batch, and it matters for
/// the reason the withdrawal alone did not settle: [`next_partition_elr`]
/// derives the next eligible set from `old_isr ∪ eligible_before`, and this
/// function clears only the second term. Left there, the image's ISR would
/// still name the returning broker, so the next change that took the ISR
/// below `min.insync.replicas` would re-derive the membership withdrawn here
/// -- and a partition whose ISR is healthy at registration publishes no ELR
/// to withdraw at all, yet would feed the same `old_isr` into the same
/// derivation later.
///
/// The failover scan usually got there first, because it visits every
/// partition whose replicas or ISR name a broker that went `alive → dead` and
/// shrinks the ISR for the follower case. That was an ordering, not an
/// invariant: registration is accepted exactly when liveness says the broker
/// is dead, so a fast restart can be processed before the scan's batch
/// commits, and a controller that never saw the transition never scanned at
/// all. The registration batch now carries the removal itself, and
/// [`ElrPublisher::after_unclean_shutdown`] is what stops that same batch from
/// re-deriving what it just removed.
pub(crate) fn records_for_restarted_broker(
    image: &MetadataImage,
    node: krabka_metadata::NodeId,
) -> Vec<MetadataRecord> {
    let Ok(node) = i32::try_from(node.0) else {
        // An id too wide for the wire was never published as eligible.
        return Vec::new();
    };
    image
        .topics()
        .filter_map(|topic| {
            let mut elr = TopicElr::of_topic(image, &topic.name);
            if !elr.demote_node(node) {
                return None;
            }
            let mut overrides = image.topic_config(&topic.name).cloned().unwrap_or_default();
            let rendered = elr.render();
            if rendered.is_empty() {
                overrides.remove(ELIGIBLE_LEADER_REPLICAS);
            } else {
                overrides.insert(ELIGIBLE_LEADER_REPLICAS.to_string(), rendered);
            }
            Some(MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: topic.name.clone(),
                overrides,
            }))
        })
        .collect()
}

/// The wire ids of a node list, as a set. Node ids too wide for the wire drop,
/// the same way [`wire_node_ids`] drops them from a published value.
fn wire_id_set(nodes: &[krabka_metadata::NodeId]) -> BTreeSet<i32> {
    wire_node_ids(nodes.iter().copied()).into_iter().collect()
}

#[cfg(test)]
mod tests;
