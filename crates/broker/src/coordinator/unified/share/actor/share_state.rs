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
        GroupCoordinator,
        offsets_log::OffsetsLog,
        reconciler::ReconcileInput,
        share::{config::ShareGroupConfig, state::ShareGroupState},
    },
    share_coordinator::coordinator::UNINITIALIZED_START_OFFSET,
};

/// KIP-932 lifecycle hook. It runs after `reconcile`, off the sync state
/// machine, and initializes the share state of every partition of a
/// subscribed topic that the group has not initialized yet, as Kafka's
/// `GroupMetadataManager.maybeCreateInitializeShareGroupStateRequest` and
/// `GroupCoordinatorService.persisterInitialize` do.
///
/// The partitions are first written to the `InitializingTopics` of
/// `ShareGroupStatePartitionMetadata` (Kafka's `addInitializingTopicsRecords`),
/// so a group delete finds any state the persister may write. Only then does
/// the hook call [`SharePersister::initialize`]. A partition the persister
/// initialized moves to `InitializedTopics` (`initializeShareGroupState`), and
/// one it failed leaves the initializing set (`uninitializeShareGroupState`),
/// so the next heartbeat retries it. A partition still initializing after a
/// restart is retried once `initialize_retry_interval` has passed. The newly
/// initialized partitions reach the assignment on the next heartbeat, which
/// sees them unassigned and bumps the group epoch.
///
/// The hook is best-effort and never fails the heartbeat. `state_epoch` is
/// the group epoch. For a topic that the group sees for the first time,
/// `start_offset` is `-1`, Kafka's `PartitionFactory.UNINITIALIZED_START_OFFSET`,
/// and the share partition resolves `share.auto.offset.reset` when it first
/// loads. A new partition of a topic that the group already initialized
/// starts at `0`.
///
/// [`SharePersister::initialize`]: crate::share_coordinator::SharePersister::initialize
pub(super) async fn reconcile_share_state(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) {
    let Some(persister) = coordinator.share_persister() else {
        // No persister wired (pure-coordinator unit tests): nothing to do.
        return;
    };

    // The metadata image is the authority on the name behind an id, the same
    // source Kafka's `GroupMetadataManager.attachInitValue` reads. The
    // snapshot is taken once per lifecycle pass.
    let input = coordinator.metadata.snapshot();
    let topic_names: HashMap<Uuid, String> = input
        .topic_id_by_name
        .iter()
        .map(|(name, topic_id)| (*topic_id, name.clone()))
        .collect();

    let retry_ms = i64::try_from(config.initialize_retry_interval.as_millis()).unwrap_or(i64::MAX);
    let to_init = partitions_to_initialize(state, &input, now_ms, retry_ms);
    let to_delete = deleted_topic_partitions(&state.initialized, &topic_names);
    if to_init.is_empty() && to_delete.is_empty() {
        return;
    }

    let mut changed = false;
    if !to_init.is_empty() {
        changed |= initialize(
            state,
            &to_init,
            &topic_names,
            offsets_log,
            coordinator,
            now_ms,
        )
        .await;
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
        write_state_partition_metadata(state, offsets_log, coordinator, now_ms).await;
    }
}

/// Records `to_init` as initializing, then initializes each partition through
/// the persister. Says whether the sets changed after the initializing record
/// was written.
async fn initialize(
    state: &mut ShareGroupState,
    to_init: &[(Uuid, i32)],
    topic_names: &HashMap<Uuid, String>,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) -> bool {
    let Some(persister) = coordinator.share_persister() else {
        return false;
    };
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

    let previous: Vec<Option<i64>> = to_init
        .iter()
        .map(|tp| state.initializing.insert(*tp, now_ms))
        .collect();
    for (tid, _) in to_init {
        if let Some(name) = topic_names.get(tid) {
            state.topic_names.insert(*tid, name.clone());
        }
    }
    if !write_state_partition_metadata(state, offsets_log, coordinator, now_ms).await {
        // The initializing record is not durable, so the persister is not
        // called: a delete could not find what it would write.
        for (tp, before) in to_init.iter().zip(previous) {
            match before {
                Some(at) => state.initializing.insert(*tp, at),
                None => state.initializing.remove(tp),
            };
        }
        state.forget_unused_topic_names();
        return false;
    }

    let state_epoch = state.group_epoch;
    for &(tid, partition) in to_init {
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
            Ok(()) => state.mark_initialized((tid, partition)),
            Err(e) => {
                state.initializing.remove(&(tid, partition));
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
    true
}

/// Writes the group's `ShareGroupStatePartitionMetadata` record, and says
/// whether the write succeeded.
async fn write_state_partition_metadata(
    state: &mut ShareGroupState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) -> bool {
    state.forget_unused_topic_names();
    let pending = PendingShareRecords {
        state_partition_metadata: Some(state_partition_metadata_from(state)),
        ..Default::default()
    };
    match flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                group_id = %state.group_id,
                error = %e,
                "persisting ShareGroupStatePartitionMetadata failed; in-memory set retained",
            );
            false
        }
    }
}

/// Kafka's `GroupMetadataManager.subscribedTopicsChangeMap`: every partition
/// of a subscribed topic in the image that is neither initialized nor
/// initializing for less than `retry_ms`, sorted. A topic whose partitions
/// are all covered contributes nothing.
fn partitions_to_initialize(
    state: &ShareGroupState,
    input: &ReconcileInput,
    now_ms: i64,
    retry_ms: i64,
) -> Vec<(Uuid, i32)> {
    let subscribed: HashSet<&String> = state
        .members
        .values()
        .flat_map(|m| m.subscribed_topic_names.iter())
        .collect();
    let covered = |tp: &(Uuid, i32)| {
        state.initialized.contains(tp)
            || state
                .initializing
                .get(tp)
                .is_some_and(|&at| now_ms.saturating_sub(at) < retry_ms)
    };
    let mut out: Vec<(Uuid, i32)> = subscribed
        .into_iter()
        .filter_map(|name| input.topic_id_by_name.get(name))
        .flat_map(|topic_id| {
            let count = input
                .partitions_per_topic
                .get(topic_id)
                .copied()
                .unwrap_or(0);
            (0..count).map(move |p| (*topic_id, p))
        })
        .filter(|tp| !covered(tp))
        .collect();
    out.sort_unstable_by_key(|(topic_id, partition)| (topic_id.0, *partition));
    out
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

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::share::state::ShareMemberState;

    /// Kafka's `subscribedTopicsChangeMap`: every partition of a subscribed
    /// topic in the image that is neither initialized nor freshly
    /// initializing.
    #[test]
    fn partitions_to_initialize_skip_initialized_and_fresh_initializing() {
        let orders = Uuid([1; 16]);
        let input = ReconcileInput {
            topic_id_by_name: HashMap::from([("orders".to_owned(), orders)]),
            partitions_per_topic: HashMap::from([(orders, 4)]),
            ..ReconcileInput::default()
        };
        // (row, initializing partition 1 recorded at, expected partitions)
        let rows = [
            ("fresh initializing entry is covered", 950, vec![2, 3]),
            ("stale initializing entry is retried", 800, vec![1, 2, 3]),
        ];
        for (row, recorded_at, expected) in rows {
            let mut state = ShareGroupState::new("g");
            state.members.insert(
                "m".to_owned(),
                ShareMemberState::joining(
                    "m",
                    "c",
                    "h",
                    HashSet::from(["orders".to_owned(), "missing".to_owned()]),
                ),
            );
            state.initialized.insert((orders, 0));
            state.initializing.insert((orders, 1), recorded_at);
            let expected: Vec<(Uuid, i32)> = expected.into_iter().map(|p| (orders, p)).collect();
            assert!(
                partitions_to_initialize(&state, &input, 1_000, 100) == expected,
                "{row}"
            );
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
}
