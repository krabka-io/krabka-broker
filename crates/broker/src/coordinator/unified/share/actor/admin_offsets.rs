//! Serialized administrative mutations of an empty share group's offsets.

use krabka_log::Offset;
use krabka_protocol::primitives::uuid::Uuid;
use krabka_verified::{
    ShareOffsetMutationDecision, ShareOffsetMutationGate, share_offset_mutation_decision,
};

use super::{PendingShareRecords, chrono_now_ms, flush_pending, state_partition_metadata_from};
use crate::{
    codes,
    coordinator::unified::{
        GroupCoordinator,
        share::{persistence::ShareGroupMetadataValue, state::ShareGroupState},
    },
    share_coordinator::coordinator::UNINITIALIZED_START_OFFSET,
};

#[derive(Debug)]
pub struct ResetPartition {
    pub topic_id: uuid::Uuid,
    pub topic_name: String,
    pub partition: i32,
    pub start_offset: i64,
    pub observed_leader_epoch: i32,
}

#[derive(Debug)]
pub struct DeleteTopic {
    pub topic_id: uuid::Uuid,
    pub topic_name: String,
}

/// Applies an `AlterShareGroupOffsets` batch, as Kafka's
/// `GroupMetadataManager.alterShareGroupOffsets` does for an already-created,
/// empty share group: bump the group epoch once for the whole batch, persist
/// the `ShareGroupMetadata` record for that bump BEFORE any partition is
/// touched, then `Initialize` every requested partition at the NEW group
/// epoch and add it to `state.initialized` (KIP-932 `addInitializingTopicsRecords`
/// followed by `handlePersisterInitializeResponse`). A partition whose
/// `Initialize` call fails keeps its previous state and is reported
/// per-partition; the group epoch bump and its record are not rolled back for
/// a partial batch, mirroring Kafka's own all-or-nothing-at-the-record-level
/// but best-effort-at-the-persister approach.
///
/// Krabka additionally re-checks the data partition's leader epoch against
/// the value the caller observed when it planned the request, and answers
/// `FENCED_LEADER_EPOCH` on a mismatch. Kafka's `alterShareGroupOffsets` has
/// no such check; this is a krabka-only safety net kept deliberately (see
/// the PR that introduced this rewrite for the discrepancy).
pub(crate) async fn reset_offsets(
    state: &mut ShareGroupState,
    coordinator: &GroupCoordinator,
    requests: Vec<ResetPartition>,
) -> Result<Vec<i16>, i16> {
    if !state.members.is_empty() {
        return Err(codes::NON_EMPTY_GROUP);
    }
    let Some(persister) = coordinator.share_persister() else {
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    };
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let Some(new_epoch) = crate::metadata_epoch::next_i32(state.group_epoch) else {
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    };
    let prior_epoch = state.group_epoch;
    state.group_epoch = new_epoch;
    let epoch_bump = PendingShareRecords {
        group_metadata: Some(ShareGroupMetadataValue { epoch: new_epoch }),
        ..Default::default()
    };
    if flush_pending(
        state,
        epoch_bump,
        &*coordinator.offsets_log,
        coordinator,
        chrono_now_ms(),
    )
    .await
    .is_err()
    {
        state.group_epoch = prior_epoch;
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    }

    let mut results = Vec::with_capacity(requests.len());
    // `(result index, topic id, partition, topic name)` for every partition
    // this loop actually initialized, so a later durability failure (below)
    // can find and downgrade exactly those `results` entries.
    let mut newly_initialized: Vec<(usize, Uuid, i32, String)> = Vec::new();
    for request in requests {
        let Some(fresh_leader_epoch) =
            current_leader_epoch(coordinator, &request.topic_name, request.partition)
        else {
            results.push(codes::FENCED_LEADER_EPOCH);
            continue;
        };
        if fresh_leader_epoch != request.observed_leader_epoch {
            results.push(codes::FENCED_LEADER_EPOCH);
            continue;
        }
        // An upgraded broker can hold a partition whose persisted state
        // epoch was advanced independently by the pre-rewrite code path, and
        // so already sits above `new_epoch`. `ShareCoordinator::initialize`
        // rejects an epoch that does not exceed the stored one, so read the
        // existing epoch first and initialize at whichever of the two is
        // higher -- the group's own epoch is a floor here, not the only
        // input.
        let existing_state_epoch = match persister
            .read_summary(&state.group_id, request.topic_id, request.partition)
            .await
        {
            Ok(summary) => summary.map_or(0, |(state_epoch, ..)| state_epoch),
            Err(e) => {
                tracing::warn!(
                    group_id = %state.group_id,
                    topic_id = %request.topic_id,
                    partition = request.partition,
                    error = %e,
                    "AlterShareGroupOffsets read_summary failed",
                );
                results.push(codes::COORDINATOR_NOT_AVAILABLE);
                continue;
            }
        };
        let init_epoch = new_epoch.max(existing_state_epoch.saturating_add(1));
        match persister
            .initialize(
                &state.group_id,
                request.topic_id,
                request.partition,
                init_epoch,
                Offset(request.start_offset),
            )
            .await
        {
            Ok(()) => {
                results.push(codes::NONE);
                newly_initialized.push((
                    results.len() - 1,
                    Uuid(*request.topic_id.as_bytes()),
                    request.partition,
                    request.topic_name,
                ));
            }
            Err(e) => {
                tracing::warn!(
                    group_id = %state.group_id,
                    topic_id = %request.topic_id,
                    partition = request.partition,
                    error = %e,
                    "AlterShareGroupOffsets initialize failed",
                );
                results.push(codes::COORDINATOR_NOT_AVAILABLE);
            }
        }
    }

    if !newly_initialized.is_empty() {
        for (_, topic_id, partition, topic_name) in &newly_initialized {
            state.initialized.insert((*topic_id, *partition));
            state
                .topic_names
                .entry(*topic_id)
                .or_insert_with(|| topic_name.clone());
        }
        let metadata_update = PendingShareRecords {
            state_partition_metadata: Some(state_partition_metadata_from(state)),
            ..Default::default()
        };
        if flush_pending(
            state,
            metadata_update,
            &*coordinator.offsets_log,
            coordinator,
            chrono_now_ms(),
        )
        .await
        .is_err()
        {
            // The share-partition data itself was already durably
            // initialized above -- only the group's own record of which
            // partitions are initialized failed to persist. Reporting NONE
            // here would let the caller believe the whole operation
            // succeeded while a restart (or a fresh actor load) would come
            // back without these partitions in `state.initialized`, hiding
            // them from `DescribeShareGroupOffsets`/`DeleteShareGroupOffsets`
            // and letting a later heartbeat re-`Initialize` them at `-1`,
            // silently overwriting the reset offset. Roll the in-memory set
            // back to match what is actually durable, and tell the caller to
            // retry instead of claiming success.
            for (result_index, topic_id, partition, _) in &newly_initialized {
                state.initialized.remove(&(*topic_id, *partition));
                results[*result_index] = codes::COORDINATOR_NOT_AVAILABLE;
            }
            tracing::warn!(
                group_id = %state.group_id,
                "persisting ShareGroupStatePartitionMetadata after \
                 AlterShareGroupOffsets failed; reporting the affected \
                 partitions as not available for retry",
            );
        }
    }
    Ok(results)
}

pub(crate) async fn delete_offsets(
    state: &mut ShareGroupState,
    coordinator: &GroupCoordinator,
    requests: Vec<DeleteTopic>,
) -> Result<Vec<i16>, i16> {
    if !state.members.is_empty() {
        return Err(codes::NON_EMPTY_GROUP);
    }
    let Some(persister) = coordinator.share_persister() else {
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    };

    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let partitions: Vec<i32> = state
            .initialized
            .iter()
            .filter_map(|(topic_id, partition)| {
                (uuid::Uuid::from_bytes(topic_id.0) == request.topic_id).then_some(*partition)
            })
            .collect();
        let mut error_code = codes::NONE;
        for partition in partitions {
            let Some(observed_leader_epoch) =
                current_leader_epoch(coordinator, &request.topic_name, partition)
            else {
                error_code = codes::FENCED_LEADER_EPOCH;
                continue;
            };
            let Ok(summary) = persister
                .read_summary(&state.group_id, request.topic_id, partition)
                .await
            else {
                error_code = codes::COORDINATOR_NOT_AVAILABLE;
                continue;
            };
            let Some(fresh_leader_epoch) =
                current_leader_epoch(coordinator, &request.topic_name, partition)
            else {
                error_code = codes::FENCED_LEADER_EPOCH;
                continue;
            };
            let exact_retry =
                summary.is_some_and(|(_, _, start_offset, delivery_complete_count)| {
                    start_offset == UNINITIALIZED_START_OFFSET && delivery_complete_count == 0
                });
            let state_epoch = summary.map_or(0, |(state_epoch, ..)| state_epoch);
            let decision = share_offset_mutation_decision(
                ShareOffsetMutationGate::Admissible { exact_retry },
                observed_leader_epoch,
                fresh_leader_epoch,
                state_epoch,
            );
            let partition_error = apply_decision(
                persister,
                &state.group_id,
                request.topic_id,
                partition,
                UNINITIALIZED_START_OFFSET,
                decision,
            )
            .await;
            if partition_error != codes::NONE {
                error_code = partition_error;
            }
        }

        if error_code == codes::NONE {
            let removed: Vec<_> = state
                .initialized
                .iter()
                .copied()
                .filter(|(topic_id, _)| uuid::Uuid::from_bytes(topic_id.0) == request.topic_id)
                .collect();
            state
                .initialized
                .retain(|(topic_id, _)| uuid::Uuid::from_bytes(topic_id.0) != request.topic_id);
            let pending = PendingShareRecords {
                state_partition_metadata: Some(state_partition_metadata_from(state)),
                ..Default::default()
            };
            if flush_pending(
                state,
                pending,
                &*coordinator.offsets_log,
                coordinator,
                chrono_now_ms(),
            )
            .await
            .is_err()
            {
                state.initialized.extend(removed);
                error_code = codes::COORDINATOR_NOT_AVAILABLE;
            } else {
                // The record just written no longer lists the topic, so the
                // group has no further use for its name.
                state.forget_unused_topic_names();
            }
        }
        results.push(error_code);
    }
    Ok(results)
}

async fn apply_decision(
    persister: &crate::share_coordinator::persister_client::SharePersister,
    group_id: &str,
    topic_id: uuid::Uuid,
    partition: i32,
    start_offset: i64,
    decision: ShareOffsetMutationDecision,
) -> i16 {
    match decision {
        ShareOffsetMutationDecision::ExactRetry => codes::NONE,
        ShareOffsetMutationDecision::Apply { next_state_epoch } => persister
            .initialize(
                group_id,
                topic_id,
                partition,
                next_state_epoch,
                Offset(start_offset),
            )
            .await
            .map_or(codes::COORDINATOR_NOT_AVAILABLE, |()| codes::NONE),
        ShareOffsetMutationDecision::FencedLeaderEpoch => codes::FENCED_LEADER_EPOCH,
        ShareOffsetMutationDecision::NotCoordinator
        | ShareOffsetMutationDecision::NonEmptyGroup
        | ShareOffsetMutationDecision::Unrequested
        | ShareOffsetMutationDecision::StateEpochOverflow => codes::COORDINATOR_NOT_AVAILABLE,
    }
}

fn current_leader_epoch(
    coordinator: &GroupCoordinator,
    topic_name: &str,
    partition: i32,
) -> Option<i32> {
    coordinator
        .metadata_source()
        .and_then(|source| {
            source
                .current_image()
                .partition(topic_name, partition)
                .cloned()
        })
        .map(|record| record.leader_epoch.0)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::reset_offsets;
    use crate::{
        codes,
        coordinator::unified::share::{
            actor::test_support::{make_coordinator, metadata_with_topic},
            state::{ShareGroupState, ShareMemberState},
        },
    };

    #[tokio::test]
    async fn missing_persister_fails_without_mutation() {
        let (metadata, _topic_id) = metadata_with_topic("t", 1);
        let (coordinator, _log) = make_coordinator(metadata);
        let mut state = ShareGroupState::new("g");

        assert2::check!(
            reset_offsets(&mut state, &coordinator, Vec::new()).await
                == Err(codes::COORDINATOR_NOT_AVAILABLE)
        );
    }

    #[tokio::test]
    async fn nonempty_gate_precedes_persister_access() {
        let (metadata, _topic_id) = metadata_with_topic("t", 1);
        let (coordinator, _log) = make_coordinator(metadata);
        let mut state = ShareGroupState::new("g");
        state.members.insert(
            "m".into(),
            ShareMemberState::joining("m", "client", "host", HashSet::default()),
        );

        assert2::check!(
            reset_offsets(&mut state, &coordinator, Vec::new()).await
                == Err(codes::NON_EMPTY_GROUP)
        );
    }
}
