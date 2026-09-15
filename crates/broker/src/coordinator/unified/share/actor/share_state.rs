//! The KIP-932 share-state lifecycle hook. It drives `Initialize` on the share
//! persister for the partitions the group gained, and it stands apart from the
//! membership state machine because it is best-effort work that runs after
//! reconciliation rather than inside it.
//!
//! The hook deletes share state only for a topic that the metadata image no
//! longer holds, as Kafka's `GroupMetadataManager.maybeCleanupShareGroupState`
//! does for a deleted topic. Kafka deletes share state otherwise only for
//! `DeleteShareGroupOffsets` and `DeleteGroups`
//! (`sharePartitionsEligibleForOffsetDeletion`,
//! `shareGroupBuildPartitionDeleteRequest`). A partition that no member is
//! assigned keeps its share-partition start offset, so consumers that
//! subscribe again continue from it.

use std::collections::{HashMap, HashSet};

use krabka_protocol::primitives::uuid::Uuid;

use super::records::{PendingShareRecords, flush_pending, state_partition_metadata_from};
use crate::{
    coordinator::unified::{
        GroupCoordinator, actor::MetadataProvider, offsets_log::OffsetsLog,
        share::state::ShareGroupState,
    },
    share_coordinator::coordinator::UNINITIALIZED_START_OFFSET,
};

/// KIP-932 lifecycle hook. It runs AFTER `reconcile`, off the
/// sync state machine. It gathers the group's full assigned `(topic_id,
/// partition)` set and drives [`SharePersister::initialize`] for each entry
/// that is not already Initialized. On success it records the partition in
/// `state.initialized`, records the topic's name from the metadata image in
/// `state.topic_names` because the record names every topic it lists, and
/// persists an updated `ShareGroupStatePartitionMetadata` (key v15) through
/// the offsets log.
///
/// The hook is best-effort. A persister error leaves the partition
/// un-recorded, so the next heartbeat retries it, and the error never fails
/// the heartbeat. `state_epoch` is the group epoch, which is monotonic and
/// bumps on every membership change. For a topic that the group sees for the
/// first time, `start_offset` is `-1`, Kafka's
/// `PartitionFactory.UNINITIALIZED_START_OFFSET`: the coordinator records that
/// the partition exists without deciding where it starts, and the share
/// partition itself resolves the group's `share.auto.offset.reset` when it is
/// first loaded, exactly as `SharePartition.maybeInitialize` does. A new
/// partition of a topic that the group already initialized starts at `0`.
pub(super) async fn reconcile_share_state(
    state: &mut ShareGroupState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) {
    let Some(persister) = coordinator.share_persister() else {
        // No persister wired (pure-coordinator unit tests): nothing to do.
        return;
    };

    // The union of every member's assigned partitions is the set of
    // (topic_id, partition) the group actively uses.
    let mut assigned: HashSet<(Uuid, i32)> = HashSet::new();
    for m in state.members.values() {
        for (tid, parts) in &m.assigned_partitions {
            for p in parts {
                assigned.insert((*tid, *p));
            }
        }
    }

    // KIP-932 names every topic the ShareGroupStatePartitionMetadata record
    // lists, and the metadata image is the authority on the name behind an id,
    // the same source Kafka's `GroupMetadataManager.attachInitValue` reads. The
    // snapshot is taken once per lifecycle pass.
    let topic_names = topic_names_by_id(coordinator.metadata.as_ref());

    let to_init: Vec<(Uuid, i32)> = assigned
        .iter()
        .copied()
        .filter(|tp| !state.initialized.contains(tp) && topic_names.contains_key(&tp.0))
        .collect();
    let to_delete = deleted_topic_partitions(&state.initialized, &topic_names);
    if to_init.is_empty() && to_delete.is_empty() {
        return;
    }

    // Kafka's `buildInitializeShareGroupStateRequest`: a new partition of a
    // topic that the group already knows starts at offset 0, so the records
    // produced to it before its share partition loads are delivered. The
    // partitions of a topic that the group sees for the first time start at
    // -1, and the share partition resolves `share.auto.offset.reset`. The set
    // is taken before this pass initializes anything.
    let known_topics: HashSet<Uuid> = state
        .initialized
        .iter()
        .map(|(topic_id, _)| *topic_id)
        .collect();
    let state_epoch = state.group_epoch;
    let mut changed = false;
    for (tid, partition) in to_init {
        let topic_uuid = uuid::Uuid::from_bytes(tid.0);
        match persister
            .initialize(
                &state.group_id,
                topic_uuid,
                partition,
                state_epoch,
                krabka_log::Offset(initial_start_offset(&known_topics, tid)),
            )
            .await
        {
            Ok(()) => {
                state.initialized.insert((tid, partition));
                if let Some(name) = topic_names.get(&tid) {
                    state.topic_names.insert(tid, name.clone());
                }
                changed = true;
            }
            Err(e) => {
                tracing::warn!(
                    group_id = %state.group_id,
                    topic_id = %topic_uuid,
                    partition,
                    error = %e,
                    "share-state Initialize failed; will retry next heartbeat",
                );
            }
        }
    }
    for (tid, partition) in to_delete {
        let topic_uuid = uuid::Uuid::from_bytes(tid.0);
        match persister
            .delete(&state.group_id, topic_uuid, partition)
            .await
        {
            Ok(()) => {
                state.initialized.remove(&(tid, partition));
                changed = true;
            }
            Err(e) => {
                tracing::warn!(
                    group_id = %state.group_id,
                    topic_id = %topic_uuid,
                    partition,
                    error = %e,
                    "share-state Delete of a deleted topic failed; will retry next heartbeat",
                );
            }
        }
    }
    if changed {
        state.forget_unused_topic_names();
        let pending = PendingShareRecords {
            state_partition_metadata: Some(state_partition_metadata_from(state)),
            ..Default::default()
        };
        if let Err(e) = flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
            tracing::warn!(
                group_id = %state.group_id,
                error = %e,
                "persisting ShareGroupStatePartitionMetadata failed; in-memory set retained",
            );
        }
    }
}

/// The start offset that a new share partition of `topic_id` is initialized
/// at: 0 for a topic in `known_topics`, [`UNINITIALIZED_START_OFFSET`] for a
/// new topic.
fn initial_start_offset(known_topics: &HashSet<Uuid>, topic_id: Uuid) -> i64 {
    if known_topics.contains(&topic_id) {
        0
    } else {
        UNINITIALIZED_START_OFFSET
    }
}

/// The initialized partitions whose topic the metadata snapshot no longer
/// holds, sorted. An empty snapshot (no image yet) deletes nothing.
fn deleted_topic_partitions(
    initialized: &HashSet<(Uuid, i32)>,
    topic_names: &HashMap<Uuid, String>,
) -> Vec<(Uuid, i32)> {
    if topic_names.is_empty() {
        return Vec::new();
    }
    let mut deleted: Vec<(Uuid, i32)> = initialized
        .iter()
        .copied()
        .filter(|(topic_id, _)| !topic_names.contains_key(topic_id))
        .collect();
    deleted.sort_unstable_by_key(|(topic_id, partition)| (topic_id.0, *partition));
    deleted
}

/// Invert the metadata snapshot's `name → id` map into the `id → name` lookup
/// the share-state record needs. A topic the image does not hold has no entry,
/// and the record writer falls back to Kafka's `<UNKNOWN>`.
fn topic_names_by_id(metadata: &dyn MetadataProvider) -> HashMap<Uuid, String> {
    metadata
        .snapshot()
        .topic_id_by_name
        .into_iter()
        .map(|(name, topic_id)| (topic_id, name))
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::reconciler::ReconcileInput;

    #[derive(Debug)]
    struct Metadata(Vec<(&'static str, Uuid)>);

    impl MetadataProvider for Metadata {
        fn snapshot(&self) -> ReconcileInput {
            ReconcileInput {
                topic_id_by_name: self
                    .0
                    .iter()
                    .map(|(name, id)| ((*name).to_owned(), *id))
                    .collect(),
                partitions_per_topic: HashMap::new(),
                partition_racks: HashMap::new(),
            }
        }
    }

    #[test]
    fn new_partitions_of_a_known_topic_start_at_zero() {
        let known = Uuid([3; 16]);
        let new = Uuid([4; 16]);
        let known_topics = HashSet::from([known]);

        assert!(initial_start_offset(&known_topics, known) == 0);
        assert!(initial_start_offset(&known_topics, new) == UNINITIALIZED_START_OFFSET);
    }

    #[test]
    fn only_partitions_of_a_topic_missing_from_the_image_are_deleted() {
        let kept = Uuid([5; 16]);
        let deleted = Uuid([6; 16]);
        let initialized = HashSet::from([(kept, 0), (deleted, 1), (deleted, 0)]);
        let image = HashMap::from([(kept, "kept".to_owned())]);
        // (topic names in the snapshot, expected deletes)
        let rows = [
            (image, vec![(deleted, 0), (deleted, 1)]),
            (HashMap::new(), vec![]),
        ];
        for (index, (topic_names, expected)) in rows.into_iter().enumerate() {
            assert!(
                deleted_topic_partitions(&initialized, &topic_names) == expected,
                "row {index}"
            );
        }
    }

    #[test]
    fn topic_names_come_from_the_metadata_snapshot() {
        let orders = Uuid([1; 16]);
        let carts = Uuid([2; 16]);
        let metadata = Metadata(vec![("orders", orders), ("carts", carts)]);

        assert!(
            topic_names_by_id(&metadata)
                == HashMap::from([(orders, "orders".to_owned()), (carts, "carts".to_owned()),])
        );
    }
}
