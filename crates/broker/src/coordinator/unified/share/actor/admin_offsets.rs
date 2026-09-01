//! Serialized administrative mutations of an empty share group's offsets.

use krabka_log::Offset;
use krabka_verified::{
    ShareOffsetMutationDecision, ShareOffsetMutationGate, share_offset_mutation_decision,
};

use super::{PendingShareRecords, chrono_now_ms, flush_pending, state_partition_metadata_from};
use crate::{
    codes,
    coordinator::unified::{GroupCoordinator, share::state::ShareGroupState},
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

pub(crate) async fn reset_offsets(
    state: &ShareGroupState,
    coordinator: &GroupCoordinator,
    requests: Vec<ResetPartition>,
) -> Result<Vec<i16>, i16> {
    if !state.members.is_empty() {
        return Err(codes::NON_EMPTY_GROUP);
    }
    let Some(persister) = coordinator.share_persister() else {
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    };

    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let Ok(state_value) = persister
            .read_state(&state.group_id, request.topic_id, request.partition)
            .await
        else {
            results.push(codes::COORDINATOR_NOT_AVAILABLE);
            continue;
        };
        let fresh_leader_epoch =
            current_leader_epoch(coordinator, &request.topic_name, request.partition);
        let Some(fresh_leader_epoch) = fresh_leader_epoch else {
            results.push(codes::FENCED_LEADER_EPOCH);
            continue;
        };
        let exact_retry = state_value.as_ref().is_some_and(|value| {
            value.start_offset == request.start_offset
                && value.delivery_complete_count == 0
                && value.state_batches.is_empty()
        });
        let state_epoch = state_value.map_or(0, |value| value.state_epoch);
        let decision = share_offset_mutation_decision(
            ShareOffsetMutationGate::Admissible { exact_retry },
            request.observed_leader_epoch,
            fresh_leader_epoch,
            state_epoch,
        );
        results.push(
            apply_decision(
                persister,
                &state.group_id,
                request.topic_id,
                request.partition,
                request.start_offset,
                decision,
            )
            .await,
        );
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
            let Ok(state_value) = persister
                .read_state(&state.group_id, request.topic_id, partition)
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
            let exact_retry = state_value.as_ref().is_some_and(|value| {
                value.start_offset == UNINITIALIZED_START_OFFSET
                    && value.delivery_complete_count == 0
                    && value.state_batches.is_empty()
            });
            let state_epoch = state_value.map_or(0, |value| value.state_epoch);
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
        let state = ShareGroupState::new("g");

        assert2::check!(
            reset_offsets(&state, &coordinator, Vec::new()).await
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
            ShareMemberState::joining("m", "client", "host", Default::default()),
        );

        assert2::check!(
            reset_offsets(&state, &coordinator, Vec::new()).await == Err(codes::NON_EMPTY_GROUP)
        );
    }
}
