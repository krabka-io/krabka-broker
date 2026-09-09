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
//! `ReplicationControlManager.handleBrokerShutdown`, whose unclean branch is
//! a broker rejoining without a clean-shutdown proof. krabka answers that
//! event in two places, one per Kafka call:
//! [`withdraw_elr_membership`](crate::elr::withdraw_elr_membership) withdraws
//! the published membership, and
//! [`compute_unclean_restart_changes`](crate::leader_election::compute_unclean_restart_changes)
//! drops the broker from the ISRs that still name it and runs this publisher
//! over the result with the broker excluded. A restart that does prove itself
//! clean reaches neither.
//!
//! Kafka gates the whole thing on the `eligible.leader.replicas.version`
//! feature: `ReplicationControlManager` builds every `PartitionChangeBuilder`
//! with `setEligibleLeaderReplicasEnabled(isElrEnabled())`, so at level 0 no
//! partition ever gains an eligible or last-known-eligible set. krabka
//! registers the same feature, and [`ElrPublisher::extend`] publishes nothing
//! while it is below level 1. The read side needs no gate of its own: a
//! cluster that never published an ELR has none to read, and a downgrade to 0
//! clears what an earlier level 1 left behind.
//!
//! ## What gets published
//!
//! [`ElrPublisher::extend`] reads the partition changes a controller path has
//! already built, works out the ELR each of their partitions ends up with,
//! and appends one `V1PartitionElr` per partition whose rendered value moved.
//! Legacy topic-config state is migrated first; removing its private key keeps
//! every other topic override from the batch or image intact.
//!
//! Nothing is appended when nothing moved, which is the common case: a
//! cluster that leaves `min.insync.replicas` at Kafka's default of 1 can
//! never have a non-empty ELR, because an ISR that reached zero members has
//! no partition record to reach it with.

use std::collections::{BTreeMap, BTreeSet};

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionElrRecord, PartitionRecord};

use super::state::{PartitionElr, TopicElr, legacy_elr, wire_node_ids, without_legacy_elr};
use crate::{
    config_keys::effective_min_insync_replicas,
    features::{ELR_VERSION, feature_enabled},
};

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

    /// Append to `changes` the `V1PartitionElr` records that carry the ELR
    /// state its partition changes imply.
    ///
    /// Call it once, after a controller path has built its whole batch and
    /// before it submits: the records are appended after the partition
    /// changes they describe, so a replay that stops between the two sees a
    /// stale ELR rather than one that names a partition state no record ever
    /// established.
    pub(crate) fn extend(&self, changes: &mut Vec<MetadataRecord>) {
        if feature_enabled(self.image, ELR_VERSION, 1) {
            let mut published = self.legacy_migration_records(changes);
            published.extend(self.partition_records(changes));
            changes.extend(published);
        }
        coalesce_partition_state(changes);
    }

    fn legacy_migration_records(&self, changes: &[MetadataRecord]) -> Vec<MetadataRecord> {
        let batch = Batch::of(changes);
        let mut records = Vec::new();
        for topic in batch.partitions.keys() {
            let Some(legacy) = legacy_elr(self.image, topic) else {
                continue;
            };
            records.extend(legacy.records(topic));
            let replacement = changes.iter().rev().find_map(|record| match record {
                MetadataRecord::V1TopicConfig(config) if config.topic == *topic => {
                    Some(&config.overrides)
                }
                _ => None,
            });
            if let Some(record) = without_legacy_elr(self.image, topic, replacement) {
                records.push(record);
            }
        }
        records
    }

    /// The `V1PartitionElr` records `extend` appends. Split out so the
    /// decision can be tested without the append.
    fn partition_records(&self, changes: &[MetadataRecord]) -> Vec<MetadataRecord> {
        let batch = Batch::of(changes);
        batch
            .partitions
            .iter()
            .filter(|(topic, _)| !batch.deleted.contains(*topic))
            .flat_map(|(topic, partitions)| {
                partitions.iter().filter_map(|(partition, record)| {
                    self.partition_record(topic, *partition, record)
                })
            })
            .collect()
    }

    /// The partition's `V1PartitionElr`, or `None` when its rendered ELR value
    /// is the one the topic already carries.
    fn partition_record(
        &self,
        topic: &str,
        partition: i32,
        record: &PartitionRecord,
    ) -> Option<MetadataRecord> {
        let before = TopicElr::of_topic(self.image, topic).partition(partition);
        let after = next_partition_elr(
            self.image,
            self.image.partition(topic, partition),
            record,
            &before,
            &self.unclean_shutdown,
        );
        if after == before {
            return None;
        }
        Some(MetadataRecord::V1PartitionElr(PartitionElrRecord {
            topic: topic.to_string(),
            partition,
            eligible_leader_replicas: after
                .eligible_leader_replicas
                .into_iter()
                .filter_map(|id| u64::try_from(id).ok().map(krabka_metadata::NodeId))
                .collect(),
            last_known_elr: after
                .last_known_elr
                .into_iter()
                .filter_map(|id| u64::try_from(id).ok().map(krabka_metadata::NodeId))
                .collect(),
        }))
    }
}

#[derive(Default)]
struct PendingState {
    eligible: Option<Vec<krabka_metadata::NodeId>>,
    last_known: Option<Vec<krabka_metadata::NodeId>>,
    recovery: Option<krabka_metadata::LeaderRecoveryState>,
}

/// Kafka carries structural, recovery, and ELR changes for one partition in
/// one `PartitionChangeRecord`. Keep the internal batch equally atomic when a
/// caller built those pieces independently.
fn coalesce_partition_state(changes: &mut Vec<MetadataRecord>) {
    let mut pending = BTreeMap::<(String, i32), PendingState>::new();
    changes.retain(|record| match record {
        MetadataRecord::V1PartitionElr(record) => {
            let state = pending
                .entry((record.topic.clone(), record.partition))
                .or_default();
            state.eligible = Some(record.eligible_leader_replicas.clone());
            state.last_known = Some(record.last_known_elr.clone());
            false
        }
        MetadataRecord::V1PartitionRecovery(record) => {
            pending
                .entry((record.topic.clone(), record.partition))
                .or_default()
                .recovery = Some(record.state);
            false
        }
        _ => true,
    });

    for record in changes.iter_mut() {
        let (topic, partition) = match record {
            MetadataRecord::V1Partition(record) => (&record.topic, record.partition),
            MetadataRecord::V1PartitionUpdate(record) => {
                (&record.partition.topic, record.partition.partition)
            }
            _ => continue,
        };
        let Some(state) = pending.remove(&(topic.clone(), partition)) else {
            continue;
        };
        match record {
            MetadataRecord::V1Partition(partition) => {
                *record =
                    MetadataRecord::V1PartitionUpdate(krabka_metadata::PartitionUpdateRecord {
                        partition: partition.clone(),
                        eligible_leader_replicas: state.eligible,
                        last_known_elr: state.last_known,
                        recovery_state: state.recovery,
                    });
            }
            MetadataRecord::V1PartitionUpdate(update) => {
                update.eligible_leader_replicas =
                    state.eligible.or(update.eligible_leader_replicas.take());
                update.last_known_elr = state.last_known.or(update.last_known_elr.take());
                update.recovery_state = state.recovery.or(update.recovery_state);
            }
            _ => unreachable!(),
        }
    }

    for ((topic, partition), state) in pending {
        if let (Some(eligible), Some(last_known)) = (state.eligible, state.last_known) {
            changes.push(MetadataRecord::V1PartitionElr(PartitionElrRecord {
                topic: topic.clone(),
                partition,
                eligible_leader_replicas: eligible,
                last_known_elr: last_known,
            }));
        }
        if let Some(recovery) = state.recovery {
            changes.push(MetadataRecord::V1PartitionRecovery(
                krabka_metadata::PartitionRecoveryRecord {
                    topic,
                    partition,
                    state: recovery,
                },
            ));
        }
    }
}

/// The parts of a change batch the publisher reads: the last partition record
/// per partition, the last topic-config record per topic, and the topics the
/// batch deletes.
struct Batch<'a> {
    partitions: BTreeMap<&'a str, BTreeMap<i32, &'a PartitionRecord>>,
    deleted: BTreeSet<&'a str>,
}

impl<'a> Batch<'a> {
    /// Index `changes`. Later records win, which is the order the image
    /// applies them in.
    fn of(changes: &'a [MetadataRecord]) -> Self {
        let mut batch = Self {
            partitions: BTreeMap::new(),
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
                MetadataRecord::V1DeleteTopic(record) => {
                    batch.deleted.insert(record.name.as_str());
                }
                _ => {}
            }
        }
        batch
    }
}

/// The ELR one partition ends up with when `next` applies.
///
/// `previous` is the partition as the image holds it, `None` for a partition
/// the batch creates. `published` is the ELR the metadata image currently
/// carries for it. `unclean_shutdown` is Kafka's `uncleanShutdownReplicas`:
/// replicas the batch has just stopped trusting, which no rule here may make
/// eligible again.
///
/// This is the whole KIP-966 maintenance rule, and the claim an ELR election
/// rests on is a claim about it, so the `data_path_model` stateright search
/// drives it directly: it is the one seam where the model can run the real
/// rule over a partition whose per-replica logs and committed prefix it also
/// holds.
pub(crate) fn next_partition_elr(
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

/// The wire ids of a node list, as a set. Node ids too wide for the wire drop,
/// the same way [`wire_node_ids`] drops them from a published value.
fn wire_id_set(nodes: &[krabka_metadata::NodeId]) -> BTreeSet<i32> {
    wire_node_ids(nodes.iter().copied()).into_iter().collect()
}

#[cfg(test)]
mod tests;
