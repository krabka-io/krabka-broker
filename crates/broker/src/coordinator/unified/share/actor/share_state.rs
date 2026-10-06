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
/// the hook start the [`SharePersister::initialize`] calls, in a task of their
/// own: Kafka's `shareGroupHeartbeat` puts `persisterInitialize` on a timer
/// task, "async with respect to the heartbeat", so a slow share coordinator
/// does not hold up the heartbeat or the heartbeats queued behind it. The task
/// sends the outcome back to the group's actor, and [`apply_initialized`]
/// moves a partition the persister initialized to `InitializedTopics`
/// (`initializeShareGroupState`) and takes one it failed out of the
/// initializing set (`uninitializeShareGroupState`), so a later heartbeat
/// retries it. A partition still initializing after a restart is retried
/// once `initialize_retry_interval` has passed. The newly initialized
/// partitions reach the assignment on the next heartbeat, which sees them
/// unassigned and bumps the group epoch.
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

    let retry_ms = crate::time_util::duration_millis(config.initialize_retry_interval);
    let to_init = partitions_to_initialize(state, &input, now_ms, retry_ms);
    let to_delete = deleted_topic_partitions(&state.initialized, &topic_names);
    if to_init.is_empty() && to_delete.is_empty() {
        return;
    }

    if !to_init.is_empty() {
        initialize(
            state,
            &to_init,
            &topic_names,
            offsets_log,
            coordinator,
            now_ms,
        )
        .await;
    }
    let mut changed = false;
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

/// Records `to_init` as initializing, then starts the persister calls that
/// initialize them, in a task that reports back to the group's actor. The
/// outcome changes the sets later, in [`apply_initialized`].
async fn initialize(
    state: &mut ShareGroupState,
    to_init: &[(Uuid, i32)],
    topic_names: &HashMap<Uuid, String>,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) {
    let Some(persister) = coordinator.share_persister() else {
        return;
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
        return;
    }

    let calls = to_init
        .iter()
        .map(|&(tid, partition)| InitializeCall {
            topic_id: tid,
            partition,
            start_offset: initial_start_offset(&known_topics, tid),
        })
        .collect();
    spawn_initialize(
        std::sync::Arc::clone(persister),
        std::sync::Arc::clone(&coordinator.share_groups),
        state.group_id.clone(),
        state.group_epoch,
        calls,
    );
}

/// One `Initialize` call of the persister: the share partition and the start
/// offset it is initialized at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InitializeCall {
    topic_id: Uuid,
    partition: i32,
    start_offset: i64,
}

/// Run `calls` against the persister together, at `state_epoch`, and send the
/// outcome of each to the actor of `group_id` that `actors` holds then.
///
/// The calls run concurrently, as Kafka's persister batches one group's
/// partitions into one request per share coordinator, so a slow coordinator
/// costs one wait and not one wait per partition. An actor that is gone by
/// then, because this broker unloaded the group, gets nothing. The actor that
/// loads the group next retries the partitions that are still initializing,
/// once `initialize_retry_interval` has passed.
fn spawn_initialize(
    persister: std::sync::Arc<crate::share_coordinator::persister_client::SharePersister>,
    actors: std::sync::Arc<dashmap::DashMap<String, std::sync::Arc<super::ShareGroupActorHandle>>>,
    group_id: String,
    state_epoch: i32,
    calls: Vec<InitializeCall>,
) {
    tokio::spawn(async move {
        let outcomes = futures_util::future::join_all(calls.into_iter().map(|call| {
            let (persister, group_id) = (&persister, &group_id);
            async move {
                let topic_uuid = uuid::Uuid::from_bytes(call.topic_id.0);
                let result = persister
                    .initialize(
                        group_id,
                        topic_uuid,
                        call.partition,
                        state_epoch,
                        krabka_log::Offset(call.start_offset),
                    )
                    .await;
                if let Err(e) = &result {
                    tracing::warn!(
                        %group_id,
                        topic_id = %topic_uuid,
                        partition = call.partition,
                        error = %e,
                        "share-state Initialize failed; will retry next heartbeat",
                    );
                }
                ((call.topic_id, call.partition), result.is_ok())
            }
        }))
        .await;
        let actor = actors
            .get(&group_id)
            .map(|entry| std::sync::Arc::clone(entry.value()));
        if let Some(actor) = actor {
            let _ = actor
                .tx
                .send(super::ShareGroupActorMessage::ShareStateInitialized(
                    outcomes,
                ))
                .await;
        }
    });
}

/// Apply the outcome of the `Initialize` calls that [`initialize`] started:
/// each `(partition, initialized)` pair moves the partition to the
/// initialized set, or takes it out of the initializing set so a later
/// heartbeat retries it. Then write the group's
/// `ShareGroupStatePartitionMetadata`, as Kafka's `initializeShareGroupState`
/// and `uninitializeShareGroupState` write it.
pub(super) async fn apply_initialized(
    state: &mut ShareGroupState,
    outcomes: &[((Uuid, i32), bool)],
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) {
    if outcomes.is_empty() {
        return;
    }
    for &(partition, initialized) in outcomes {
        if initialized {
            state.mark_initialized(partition);
        } else {
            state.initializing.remove(&partition);
        }
    }
    write_state_partition_metadata(state, offsets_log, coordinator, now_ms).await;
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
    use krabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;

    use super::{
        super::{
            ShareGroupActorMessage,
            test_support::{heartbeat, make_coordinator, metadata_with_topic},
        },
        *,
    };
    use crate::{
        codes,
        coordinator::unified::{
            ShareGroupSeed,
            share::{
                persistence::{ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo},
                state::ShareMemberState,
            },
            test_support::{fixed_source, make_share_persister},
        },
    };

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

    /// The share-partition metadata that the last committed write of `group`
    /// holds.
    fn committed_metadata(
        coordinator: &GroupCoordinator,
        group: &str,
    ) -> ShareGroupStatePartitionMetadataValue {
        coordinator
            .cached_share_seed(group)
            .expect("the group wrote a record")
            .state_partition_metadata
    }

    fn topic_partitions(topic_id: Uuid, partitions: Vec<i32>) -> TopicPartitionsInfo {
        TopicPartitionsInfo {
            topic_id: uuid::Uuid::from_bytes(topic_id.0),
            topic_name: "t".to_owned(),
            partitions,
        }
    }

    /// Kafka's `shareGroupHeartbeat` runs `persisterInitialize` "async with
    /// respect to the heartbeat". A share coordinator that cannot answer,
    /// here because `__share_group_state` does not exist, holds the
    /// persister for its whole five-second wait, and the heartbeat must not
    /// wait with it. The partitions are initializing when it answers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_heartbeat_does_not_wait_for_the_persister() {
        let (metadata, topic_id) = metadata_with_topic("t", 2);
        let (coordinator, _log) = make_coordinator(metadata);
        coordinator.set_share_persister(make_share_persister(fixed_source(
            krabka_metadata::MetadataImage::default(),
        )));
        let handle = coordinator.get_or_create_share("g");

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            heartbeat(
                &handle,
                ShareGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    ..Default::default()
                },
            ),
        )
        .await
        .expect("the heartbeat answers before the persister does");

        assert!(response.error_code == codes::NONE);
        assert!(
            committed_metadata(&coordinator, "g")
                == ShareGroupStatePartitionMetadataValue {
                    initializing: vec![topic_partitions(topic_id, vec![0, 1])],
                    ..ShareGroupStatePartitionMetadataValue::default()
                }
        );
    }

    /// The outcome of the `Initialize` calls reaches the actor as a message.
    /// A partition the persister initialized is initialized, one it failed
    /// leaves the initializing set for a later heartbeat to retry, and one
    /// with no outcome yet stays initializing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_initialize_outcome_moves_each_partition() {
        let (metadata, topic_id) = metadata_with_topic("t", 3);
        let (coordinator, _log) = make_coordinator(metadata);
        let handle = coordinator.get_or_create_share("g");
        handle
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                    initializing: vec![topic_partitions(topic_id, vec![0, 1, 2])],
                    ..ShareGroupStatePartitionMetadataValue::default()
                },
                ..ShareGroupSeed::default()
            }))
            .await
            .expect("seed the group");

        handle
            .tx
            .send(ShareGroupActorMessage::ShareStateInitialized(vec![
                ((topic_id, 0), true),
                ((topic_id, 1), false),
            ]))
            .await
            .expect("send the outcome");
        let (reply, described) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(ShareGroupActorMessage::Describe { reply })
            .await
            .expect("describe after the outcome");
        described.await.expect("the actor answers in order");

        assert!(
            committed_metadata(&coordinator, "g")
                == ShareGroupStatePartitionMetadataValue {
                    initializing: vec![topic_partitions(topic_id, vec![2])],
                    initialized: vec![topic_partitions(topic_id, vec![0])],
                    ..ShareGroupStatePartitionMetadataValue::default()
                }
        );
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
