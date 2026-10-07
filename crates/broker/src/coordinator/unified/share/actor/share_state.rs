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
        share::{
            config::ShareGroupConfig, persistence::ShareGroupStatePartitionMetadataValue,
            state::ShareGroupState,
        },
    },
    share_coordinator::coordinator::UNINITIALIZED_START_OFFSET,
};

/// The share partitions that a heartbeat initializes, and the group epoch
/// it initializes them at.
pub(super) struct Initialize {
    calls: Vec<InitializeCall>,
    state_epoch: i32,
}

/// Kafka's `GroupMetadataManager.maybeCreateInitializeShareGroupStateRequest`:
/// every partition of a subscribed topic that the group has not initialized
/// yet, and that is not initializing since less than
/// `initialize_retry_interval`, becomes initializing, as Kafka's
/// `addInitializingTopicsRecords` records it. It returns the group's
/// `ShareGroupStatePartitionMetadata` value, which the heartbeat writes last
/// in its own batch, and the persister calls that [`start_initialize`] makes
/// once that batch is written, or `None` when there is nothing to initialize.
///
/// Every partition is initialized at the group epoch and at start offset -1,
/// Kafka's `PartitionFactory.UNINITIALIZED_START_OFFSET`, as Kafka's
/// `buildInitializeShareGroupStateRequest` asks; the share partition resolves
/// `share.auto.offset.reset` when it first loads.
pub(super) fn prepare_initialize(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) -> Option<(ShareGroupStatePartitionMetadataValue, Initialize)> {
    coordinator.share_persister()?;
    let input = coordinator.metadata.snapshot();
    let retry_ms = crate::time_util::duration_millis(config.initialize_retry_interval);
    let to_init = partitions_to_initialize(state, &input, now_ms, retry_ms);
    if to_init.is_empty() {
        return None;
    }
    let topic_names: HashMap<Uuid, String> = input
        .topic_id_by_name
        .iter()
        .map(|(name, topic_id)| (*topic_id, name.clone()))
        .collect();
    for tp in &to_init {
        state.initializing.insert(*tp, now_ms);
        if let Some(name) = topic_names.get(&tp.0) {
            state.topic_names.insert(tp.0, name.clone());
        }
    }
    state.forget_unused_topic_names();
    let calls = to_init
        .iter()
        .map(|&(topic_id, partition)| InitializeCall {
            topic_id,
            partition,
            start_offset: UNINITIALIZED_START_OFFSET,
        })
        .collect();
    Some((
        state_partition_metadata_from(state),
        Initialize {
            calls,
            state_epoch: state.group_epoch,
        },
    ))
}

/// Starts the persister calls of `initialize` after the heartbeat's batch
/// recorded the partitions as initializing, in a task that reports back to
/// the group's actor: Kafka's `shareGroupHeartbeat` puts
/// `persisterInitialize` on a timer task, "async with respect to the
/// heartbeat". [`apply_initialized`] applies the outcome.
pub(super) fn start_initialize(
    state: &ShareGroupState,
    coordinator: &GroupCoordinator,
    initialize: Initialize,
) {
    let Some(persister) = coordinator.share_persister() else {
        return;
    };
    spawn_initialize(
        std::sync::Arc::clone(persister),
        std::sync::Arc::clone(&coordinator.share_groups),
        state.group_id.clone(),
        initialize.state_epoch,
        initialize.calls,
    );
}

/// The share state of a topic that the metadata image no longer holds goes,
/// as Kafka's `maybeCleanupShareGroupState` takes a deleted topic out of the
/// group's `ShareGroupStatePartitionMetadata`.
pub(super) async fn cleanup_deleted_topics(
    state: &mut ShareGroupState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) {
    let Some(persister) = coordinator.share_persister() else {
        return;
    };
    let input = coordinator.metadata.snapshot();
    let topic_names: HashMap<Uuid, String> = input
        .topic_id_by_name
        .iter()
        .map(|(name, topic_id)| (*topic_id, name.clone()))
        .collect();
    let to_delete = deleted_topic_partitions(&state.initialized, &topic_names);
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
        let (coordinator, log) = make_coordinator(metadata);
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
        // Kafka's `shareGroupHeartbeat` writes one batch: the member (k10),
        // the group epoch (k11), the member's target (k13) and the target
        // metadata (k12), its current assignment (k14), and last the
        // partitions it initializes (k15).
        let key_versions: Vec<Vec<i16>> = log
            .batches()
            .await
            .iter()
            .map(|batch| {
                batch
                    .records
                    .iter()
                    .map(|record| {
                        let key = record.key.as_ref().expect("a record key");
                        i16::from_be_bytes([key[0], key[1]])
                    })
                    .collect()
            })
            .collect();
        assert!(key_versions == vec![vec![10, 11, 13, 12, 14, 15]]);
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

    /// Kafka's `buildInitializeShareGroupStateRequest` initializes every
    /// partition at start offset -1, a new partition of a topic that the
    /// group already initialized included.
    #[test]
    fn every_partition_initializes_at_the_uninitialized_start_offset() {
        let (metadata, topic_id) = metadata_with_topic("t", 2);
        let (coordinator, _log) = make_coordinator(metadata);
        coordinator.set_share_persister(make_share_persister(fixed_source(
            krabka_metadata::MetadataImage::default(),
        )));
        let mut state = ShareGroupState::new("g");
        state.members.insert(
            "m".into(),
            ShareMemberState::joining("m", "c", "h", ["t".to_owned()].into()),
        );
        state.mark_initialized((topic_id, 0));
        state.topic_names.insert(topic_id, "t".to_owned());

        let (value, initialize) = prepare_initialize(
            &mut state,
            &ShareGroupConfig::default(),
            &coordinator,
            1_000,
        )
        .expect("partition 1 initializes");

        assert!(
            initialize.calls
                == vec![InitializeCall {
                    topic_id,
                    partition: 1,
                    start_offset: UNINITIALIZED_START_OFFSET,
                }]
        );
        assert!(
            value
                == ShareGroupStatePartitionMetadataValue {
                    initializing: vec![topic_partitions(topic_id, vec![1])],
                    initialized: vec![topic_partitions(topic_id, vec![0])],
                    ..ShareGroupStatePartitionMetadataValue::default()
                }
        );
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
