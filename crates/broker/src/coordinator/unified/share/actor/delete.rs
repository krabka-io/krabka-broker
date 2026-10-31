//! `DeleteGroups` for a share group (KIP-932).
//!
//! Kafka's `GroupCoordinatorService.deleteGroups` runs the share group delete
//! first. A group with members answers `NON_EMPTY_GROUP`. For an empty group,
//! the group records every initialized and initializing topic as deleting,
//! and the persister deletes their share state
//! (`shareGroupBuildPartitionDeleteRequest`). A group whose state delete
//! failed is kept and answers the error. Only then does the general delete
//! write the group tombstones (`ShareGroup.createGroupTombstoneRecords`),
//! which tombstone the `ShareGroupStatePartitionMetadata` record too.

use {
    super::{
        records::{
            PendingShareRecords, chrono_now_ms, flush_pending, state_partition_metadata_from,
        },
        share_state::TopicImage,
    },
    crate::{
        codes,
        coordinator::{
            DeleteGroupError,
            unified::{
                GroupCoordinator, OffsetRecordBatchBuilder,
                share::{
                    persistence::{ShareGroupKey, encode_share_key},
                    state::{ShareGroupState, TopicPartitionList},
                },
            },
        },
        error::BrokerError,
    },
    krabka_protocol::records::RecordBatch,
};

/// Deletes the share group that `state` holds.
///
/// # Errors
///
/// Returns [`DeleteGroupError::NonEmpty`] when the group has members,
/// [`DeleteGroupError::ShareState`] when the delete of a share state fails
/// (the group is kept, without the partitions whose state is gone), and
/// [`DeleteGroupError::Internal`] when the tombstone append fails.
pub(super) async fn delete_group(
    state: &mut ShareGroupState,
    coordinator: &GroupCoordinator,
) -> Result<(), DeleteGroupError> {
    if !state.members.is_empty() {
        return Err(DeleteGroupError::NonEmpty);
    }

    delete_share_state(state, coordinator).await?;

    let appended = match tombstone_batch(&state.group_id, chrono_now_ms()) {
        Ok(batch) => coordinator.offsets_log.append(&state.group_id, batch).await,
        Err(error) => Err(error),
    };
    appended.map_err(|error| {
        tracing::warn!(
            group_id = %state.group_id,
            %error,
            "share group tombstone append failed",
        );
        DeleteGroupError::Internal
    })?;
    // The actor clears the seeds while it still owns the group, so a group
    // that a later heartbeat creates with the same id starts empty.
    coordinator.share_seeds.remove(&state.group_id);
    coordinator.share_seeds_cache.remove(&state.group_id);
    Ok(())
}

/// Kafka's `GroupMetadataManager.shareGroupBuildPartitionDeleteRequest`
/// followed by `GroupCoordinatorService.performShareGroupsDeletion`.
///
/// Every initialized and initializing partition, and every partition of a
/// topic that an earlier delete left deleting, moves to the deleting set, and
/// the group writes that `ShareGroupStatePartitionMetadata` record before the
/// persister deletes any state. When a delete fails, the group is kept with
/// the record as written, so a retry deletes every one of those topics again.
/// A group with no share-state partition metadata writes nothing here.
async fn delete_share_state(
    state: &mut ShareGroupState,
    coordinator: &GroupCoordinator,
) -> Result<(), DeleteGroupError> {
    if !state.has_state_partition_metadata() {
        return Ok(());
    }
    let Some(persister) = coordinator.share_persister() else {
        return Err(DeleteGroupError::ShareState(
            codes::COORDINATOR_NOT_AVAILABLE,
        ));
    };

    let image = TopicImage::current(coordinator);
    let before = (
        state.initialized.clone(),
        state.initializing.clone(),
        state.deleting.clone(),
        state.topic_names.clone(),
    );
    let candidates = state.move_all_to_deleting(|topic_id| image.by_id(topic_id));
    let pending = PendingShareRecords {
        state_partition_metadata: Some(state_partition_metadata_from(state)),
        ..Default::default()
    };
    if let Err(error) = flush_pending(
        state,
        pending,
        &*coordinator.offsets_log,
        coordinator,
        chrono_now_ms(),
    )
    .await
    {
        tracing::warn!(
            group_id = %state.group_id,
            %error,
            "persisting the deleting share partitions of a share group delete failed",
        );
        (
            state.initialized,
            state.initializing,
            state.deleting,
            state.topic_names,
        ) = before;
        return Err(DeleteGroupError::Internal);
    }

    let mut error_code = codes::NONE;
    for TopicPartitionList {
        topic_id,
        partitions,
    } in candidates
    {
        let topic_uuid = uuid::Uuid::from_bytes(topic_id.0);
        for partition in partitions {
            if let Err(error) = persister
                .delete(&state.group_id, topic_uuid, partition)
                .await
            {
                tracing::warn!(
                    group_id = %state.group_id,
                    topic_id = %topic_uuid,
                    partition,
                    %error,
                    "share state delete failed; the share group is kept",
                );
                if error_code == codes::NONE {
                    error_code = match error {
                        BrokerError::SharePartitionState { code, .. } => code,
                        _ => codes::COORDINATOR_NOT_AVAILABLE,
                    };
                }
            }
        }
    }
    if error_code == codes::NONE {
        Ok(())
    } else {
        Err(DeleteGroupError::ShareState(error_code))
    }
}

/// The tombstones of an empty share group, in the order of Kafka's
/// `ShareGroup.createGroupTombstoneRecords`: the target assignment metadata,
/// the `ShareGroupStatePartitionMetadata`, and the group epoch. An empty
/// group has no member records left.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when `group_id` is longer than 32767
/// bytes.
pub(super) fn tombstone_batch(group_id: &str, now_ms: i64) -> Result<RecordBatch, BrokerError> {
    let mut batch = OffsetRecordBatchBuilder::default();
    for key in [
        ShareGroupKey::TargetAssignmentMetadata {
            group_id: group_id.into(),
        },
        ShareGroupKey::StatePartitionMetadata {
            group_id: group_id.into(),
        },
        ShareGroupKey::GroupMetadata {
            group_id: group_id.into(),
        },
    ] {
        batch.push(encode_share_key(&key)?, None);
    }
    Ok(batch.finish(now_ms))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            coordinator::unified::{
                ShareGroupSeed,
                group_coordinator::GroupType,
                share::{
                    actor::ShareGroupActorMessage,
                    persistence::{
                        DeletingTopic, ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo,
                    },
                },
                test_support::{
                    AppendedRecords as Appended, appended_offsets_records as appended_records,
                    fixed_source, make_coord_with_log, make_share_persister, share_member,
                },
            },
            metrics::{BrokerMetrics, ShareGroupIdLabel},
        },
        assert2::check,
        std::{collections::HashMap, time::Duration},
        tokio::{sync::oneshot, time::timeout},
    };

    fn seed(members: usize, initialized_partitions: &[i32]) -> ShareGroupSeed {
        let initialized = if initialized_partitions.is_empty() {
            Vec::new()
        } else {
            vec![TopicPartitionsInfo {
                topic_id: uuid::Uuid::from_bytes([9; 16]),
                topic_name: "orders".to_owned(),
                partitions: initialized_partitions.to_vec(),
            }]
        };
        ShareGroupSeed {
            group_epoch: 3,
            members: (0..members)
                .map(|index| (format!("member-{index}"), share_member("client")))
                .collect::<HashMap<_, _>>(),
            state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                initializing: Vec::new(),
                initialized,
                deleting: Vec::new(),
            },
            ..ShareGroupSeed::default()
        }
    }

    /// The `(key, value)` of a `ShareGroupStatePartitionMetadata` record of
    /// `sg` holding `value`.
    fn metadata_record(value: ShareGroupStatePartitionMetadataValue) -> Appended {
        PendingShareRecords {
            state_partition_metadata: Some(value),
            ..Default::default()
        }
        .into_batch("sg", 0)
        .unwrap()
        .records
        .into_iter()
        .map(|record| (record.key, record.value))
        .collect()
    }

    /// `DeleteGroups` on a share group, per row: whether the group exists,
    /// its members and initialized or initializing partitions, and whether a share persister
    /// is wired. The persister runs over a metadata image with no brokers, so
    /// it cannot reach a share coordinator and every state delete fails.
    /// A share actor that stopped after a log-write failure leaves a closed
    /// handle and a cached seed. `DeleteGroups` respawns the actor from the
    /// seed and deletes the group, and the seeds are gone afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_respawns_a_stopped_share_actor() {
        let (coordinator, log) = make_coord_with_log();
        coordinator.mark_share("sg");
        coordinator.update_share_cache("sg", seed(0, &[]));
        let handle = coordinator.get_or_create_share("sg");
        let (stopped_tx, stopped_rx) = oneshot::channel();
        handle
            .tx
            .send(ShareGroupActorMessage::Shutdown(stopped_tx))
            .await
            .expect("stop the share actor");
        stopped_rx.await.expect("the share actor stopped");
        timeout(Duration::from_secs(5), handle.tx.closed())
            .await
            .expect("the stopped actor closes its mailbox");

        let result = coordinator.delete_group("sg").await;

        let written = appended_records(log.as_ref()).await;
        let tombstones: Appended = tombstone_batch("sg", 0)
            .unwrap()
            .records
            .into_iter()
            .map(|record| (record.key, record.value))
            .collect();
        check!(result == Ok(()));
        check!(written == tombstones);
        check!(!coordinator.share_group_ids().contains(&"sg".to_owned()));
        check!(coordinator.cached_share_seed("sg") == None);
        check!(coordinator.group_type("sg") == None);
    }

    /// A deleted share group takes its dead-letter counters with it. A delete
    /// that the group's members refuse does not, and another group's counters
    /// stay either way.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deleting_a_share_group_releases_its_dead_letter_counters() {
        let series = |metrics: &BrokerMetrics, group: &str| {
            let label = ShareGroupIdLabel {
                group_id: group.to_owned(),
            };
            [
                metrics.share_group_dlq_records.get(&label).is_some(),
                metrics
                    .share_group_dlq_produce_requests
                    .get(&label)
                    .is_some(),
                metrics
                    .share_group_dlq_failed_produce_requests
                    .get(&label)
                    .is_some(),
            ]
        };
        // (members, result, whether the group's series stay)
        let rows = [
            (1, Err(DeleteGroupError::NonEmpty), true),
            (0, Ok(()), false),
        ];

        for (members, expected, kept) in rows {
            let (coordinator, _log) = make_coord_with_log();
            let metrics = BrokerMetrics::new();
            coordinator.set_metrics(metrics.clone());
            for group in ["sg", "other"] {
                metrics.record_share_dlq_produce(group);
                metrics.record_share_dlq_records(group, 2);
                metrics.record_share_dlq_produce_failed(group);
            }
            coordinator.mark_share("sg");
            coordinator
                .get_or_create_share("sg")
                .tx
                .send(ShareGroupActorMessage::Seed(seed(members, &[])))
                .await
                .expect("seed the share group");

            let result = coordinator.delete_group("sg").await;

            check!(result == expected, "{members} members");
            check!(series(&metrics, "sg") == [kept; 3], "{members} members");
            check!(series(&metrics, "other") == [true; 3], "{members} members");
        }
    }
    use {
        crate::coordinator::unified::test_support::InMemoryOffsetsLog,
        krabka_metadata::MetadataImage,
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{DataTableRow, given, then, when},
        serde::Deserialize,
        std::sync::Arc,
    };
    #[derive(Default)]
    struct ShareDeletionBehavior {
        coordinator: Option<Arc<GroupCoordinator>>,
        log: Option<Arc<InMemoryOffsetsLog>>,
        result: Option<Result<(), DeleteGroupError>>,
        group_ids: Vec<String>,
        group_type: Option<GroupType>,
        appended: Appended,
    }
    #[derive(DataTableRow)]
    struct PersistedShareState {
        exists: bool,
        members: usize,
        initialized: String,
        initializing: String,
        deleting: bool,
    }
    #[derive(DataTableRow)]
    struct ExpectedShareRecord {
        family: String,
        value: String,
    }
    #[derive(Deserialize)]
    struct ExpectedTopicPartitions {
        topic_id: String,
        topic_name: String,
        partitions: Vec<i32>,
    }
    #[derive(Deserialize)]
    struct ExpectedDeletingTopic {
        topic_id: String,
        topic_name: String,
    }
    #[derive(Deserialize)]
    struct ExpectedStateRecord {
        initializing: Vec<ExpectedTopicPartitions>,
        initialized: Vec<ExpectedTopicPartitions>,
        deleting: Vec<ExpectedDeletingTopic>,
    }
    #[rstest::fixture]
    fn share_deletion() -> ShareDeletionBehavior {
        // Start this scenario with fresh inputs and observations.
        ShareDeletionBehavior::default()
    }
    #[given(
        "the Share coordinator has a persister {persister} and no brokers in its metadata image"
    )]
    fn share_delete_coordinator(share_deletion: &mut ShareDeletionBehavior, persister: bool) {
        let (coordinator, log) = make_coord_with_log();
        if persister {
            coordinator
                .set_share_persister(make_share_persister(fixed_source(MetadataImage::default())));
        }
        share_deletion.coordinator = Some(coordinator);
        share_deletion.log = Some(log);
    }
    #[given(
        "Share group sg at epoch 3 has orders topic UUID 09090909-0909-0909-0909-090909090909 and this persisted state"
    )]
    async fn share_delete_seed(
        share_deletion: &mut ShareDeletionBehavior,
        #[datatable] rows: Rows<PersistedShareState>,
    ) {
        let input = rows
            .into_iter()
            .next()
            .expect("persisted Share group state");
        if !input.exists {
            return;
        }
        let initialized = serde_json::from_str::<Vec<i32>>(&input.initialized)
            .expect("initialized partition numbers");
        let initializing = serde_json::from_str::<Vec<i32>>(&input.initializing)
            .expect("initializing partition numbers");
        let mut stored = seed(input.members, &initialized);
        if !initializing.is_empty() {
            stored.state_partition_metadata.initializing = vec![TopicPartitionsInfo {
                topic_id: uuid::Uuid::from_bytes([9; 16]),
                topic_name: "orders".into(),
                partitions: initializing,
            }];
        }
        if input.deleting {
            stored.state_partition_metadata.deleting = vec![DeletingTopic {
                topic_id: uuid::Uuid::from_bytes([9; 16]),
                topic_name: "orders".into(),
            }];
        }
        let coordinator = share_deletion
            .coordinator
            .as_ref()
            .expect("coordinator prepared");
        coordinator.mark_share("sg");
        coordinator
            .get_or_create_share("sg")
            .tx
            .send(ShareGroupActorMessage::Seed(stored))
            .await
            .expect("seed Share group");
    }
    #[when(
        "DeleteGroups deletes sg and the coordinator observes its registry and appended records"
    )]
    async fn delete_share_group(share_deletion: &mut ShareDeletionBehavior) {
        let coordinator = share_deletion
            .coordinator
            .as_ref()
            .expect("coordinator prepared");
        share_deletion.result = Some(coordinator.delete_group("sg").await);
        share_deletion.group_ids = coordinator.share_group_ids();
        share_deletion.group_type = coordinator.group_type("sg");
        share_deletion.appended =
            appended_records(share_deletion.log.as_ref().expect("offsets log").as_ref()).await;
    }
    #[then("Share deletion returns {outcome} and the group remains registered {kept}")]
    fn share_deletion_result(share_deletion: &ShareDeletionBehavior, outcome: String, kept: bool) {
        let expected = match outcome.as_str() {
            "deleted" => Ok(()),
            "not-found" => Err(DeleteGroupError::NotFound),
            "non-empty" => Err(DeleteGroupError::NonEmpty),
            "coordinator-unavailable" => Err(DeleteGroupError::ShareState(
                codes::COORDINATOR_NOT_AVAILABLE,
            )),
            _ => panic!("unknown Share deletion outcome"),
        };
        check!(share_deletion.result.as_ref() == Some(&expected));
        check!(share_deletion.group_ids.contains(&"sg".to_owned()) == kept);
        check!((share_deletion.group_type == Some(GroupType::Share)) == kept);
    }
    fn expected_topic_partitions(input: ExpectedTopicPartitions) -> TopicPartitionsInfo {
        TopicPartitionsInfo {
            topic_id: uuid::Uuid::parse_str(&input.topic_id).expect("expected topic UUID"),
            topic_name: input.topic_name,
            partitions: input.partitions,
        }
    }
    #[then("the complete Share deletion log contains these records in order")]
    fn share_deletion_records(
        share_deletion: &ShareDeletionBehavior,
        #[datatable] rows: Rows<ExpectedShareRecord>,
    ) {
        let mut expected = vec![];
        for row in rows {
            let key = match row.family.as_str() {
                "target-epoch" => ShareGroupKey::TargetAssignmentMetadata {
                    group_id: "sg".into(),
                },
                "state-metadata" => ShareGroupKey::StatePartitionMetadata {
                    group_id: "sg".into(),
                },
                "group-epoch" => ShareGroupKey::GroupMetadata {
                    group_id: "sg".into(),
                },
                _ => panic!("unknown Share record family"),
            };
            if row.value == "tombstone" {
                expected.push((
                    Some(encode_share_key(&key).expect("expected Share key")),
                    None,
                ));
            } else {
                let state = serde_json::from_str::<ExpectedStateRecord>(&row.value)
                    .expect("complete Share state record");
                expected.extend(metadata_record(ShareGroupStatePartitionMetadataValue {
                    initializing: state
                        .initializing
                        .into_iter()
                        .map(expected_topic_partitions)
                        .collect(),
                    initialized: state
                        .initialized
                        .into_iter()
                        .map(expected_topic_partitions)
                        .collect(),
                    deleting: state
                        .deleting
                        .into_iter()
                        .map(|topic| DeletingTopic {
                            topic_id: uuid::Uuid::parse_str(&topic.topic_id)
                                .expect("expected topic UUID"),
                            topic_name: topic.topic_name,
                        })
                        .collect(),
                }));
            }
        }
        check!(share_deletion.appended == expected);
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/delete.feature",
        name = "A missing Share group is not found"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_share_group_answers_as_kafka(share_deletion: ShareDeletionBehavior) {
        let _ = share_deletion;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/delete.feature",
        name = "A Share group with members cannot be deleted"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_share_group_2(share_deletion: ShareDeletionBehavior) { let _ = share_deletion; }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/delete.feature",
        name = "An empty Share group without state deletes without a persister"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_share_group_3(share_deletion: ShareDeletionBehavior) { let _ = share_deletion; }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/delete.feature",
        name = "Initialized Share state is marked deleting before an unavailable coordinator refuses deletion"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_share_group_4(share_deletion: ShareDeletionBehavior) { let _ = share_deletion; }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/delete.feature",
        name = "Deleting state whose topic disappeared is cleared before group tombstones"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_share_group_5(share_deletion: ShareDeletionBehavior) { let _ = share_deletion; }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/delete.feature",
        name = "Initialized Share state requires a wired persister"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_share_group_6(share_deletion: ShareDeletionBehavior) { let _ = share_deletion; }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/delete.feature",
        name = "Initializing Share state also requires a wired persister"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_share_group_7(share_deletion: ShareDeletionBehavior) { let _ = share_deletion; }
}
