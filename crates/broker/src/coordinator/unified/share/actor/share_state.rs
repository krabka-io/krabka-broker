//! The KIP-932 share-state lifecycle hook. It drives `Initialize` and `Delete`
//! on the share persister for the partitions the group gained or dropped, and
//! it stands apart from the membership state machine because it is
//! best-effort work that runs after reconciliation rather than inside it.

use std::collections::HashSet;

use krabka_protocol::primitives::uuid::Uuid;

use super::records::{PendingShareRecords, flush_pending, state_partition_metadata_from};
use crate::{
    coordinator::unified::{
        GroupCoordinator, offsets_log::OffsetsLog, share::state::ShareGroupState,
    },
    share_coordinator::coordinator::UNINITIALIZED_START_OFFSET,
};

/// KIP-932 lifecycle hook. It runs AFTER `reconcile`, off the
/// sync state machine. It gathers the group's full assigned `(topic_id,
/// partition)` set and drives [`SharePersister::initialize`] for each entry
/// that is not already Initialized. On success it records the partition in
/// `state.initialized` and persists an updated
/// `ShareGroupStatePartitionMetadata` (key v14) through the offsets log.
///
/// The hook is best-effort. A persister error leaves the partition
/// un-recorded, so the next heartbeat retries it, and the error never fails
/// the heartbeat. `state_epoch` is the group epoch, which is monotonic and
/// bumps on every membership change. `start_offset` is `-1`, Kafka's
/// `PartitionFactory.UNINITIALIZED_START_OFFSET`: the coordinator records that
/// the partition exists without deciding where it starts, and the share
/// partition itself resolves the group's `share.auto.offset.reset` when it is
/// first loaded, exactly as `SharePartition.maybeInitialize` does.
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

    let to_init: Vec<(Uuid, i32)> = assigned
        .iter()
        .copied()
        .filter(|tp| !state.initialized.contains(tp))
        .collect();
    // Keep initialized state while the group is empty. Its SPSO is the durable
    // queue cursor and its backlog metric is what lets KEDA scale consumers
    // back up from zero. With live members, an unassigned partition really did
    // leave the subscription and can be deleted.
    let to_delete = share_states_to_delete(state, &assigned);
    if to_init.is_empty() && to_delete.is_empty() {
        return;
    }

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
                krabka_log::Offset(UNINITIALIZED_START_OFFSET),
            )
            .await
        {
            Ok(()) => {
                state.initialized.insert((tid, partition));
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
                    "share-state Delete failed; will retry next heartbeat",
                );
            }
        }
    }

    if changed {
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

fn share_states_to_delete(
    state: &ShareGroupState,
    assigned: &HashSet<(Uuid, i32)>,
) -> Vec<(Uuid, i32)> {
    if state.members.is_empty() {
        return Vec::new();
    }
    state
        .initialized
        .iter()
        .copied()
        .filter(|tp| !assigned.contains(tp))
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::share::state::ShareMemberState;

    #[test]
    fn empty_group_preserves_initialized_share_state() {
        let topic = Uuid([8; 16]);
        let mut state = ShareGroupState::new("g");
        state.initialized.insert((topic, 0));

        assert!(share_states_to_delete(&state, &HashSet::new()).is_empty());
    }

    #[test]
    fn live_group_deletes_share_state_removed_from_subscription() {
        let topic = Uuid([9; 16]);
        let mut state = ShareGroupState::new("g");
        state.initialized.insert((topic, 0));
        state.members.insert(
            "m1".into(),
            ShareMemberState::joining("m1", "client", "host", HashSet::new()),
        );

        assert!(share_states_to_delete(&state, &HashSet::new()) == vec![(topic, 0)]);
    }
}
