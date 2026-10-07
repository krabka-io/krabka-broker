//! Durable encoding of share-group state transitions. A
//! [`PendingShareRecords`] set collects the mutations of one transition,
//! encodes them as a single `RecordBatch`, and appends that batch to
//! `__consumer_offsets`. It is its own file because every handler in this
//! module writes through it.

use std::collections::HashMap;

use krabka_protocol::primitives::uuid::Uuid;

use super::seed::snapshot_seed;
use crate::coordinator::unified::share::{
    persistence::{
        ShareGroupCurrentMemberAssignmentValue, ShareGroupKey, ShareGroupMemberMetadataValue,
        ShareGroupMetadataValue, ShareGroupStatePartitionMetadataValue,
        ShareGroupTargetAssignmentMemberValue, ShareGroupTargetAssignmentMetadataValue,
        TopicPartitionsInfo, UNKNOWN_TOPIC_NAME, encode_share_key,
    },
    state::{ShareGroupState, ShareMemberState},
};

#[derive(Debug, Default)]
pub(crate) struct PendingShareRecords {
    pub group_metadata: Option<ShareGroupMetadataValue>,
    /// `Some(value)` writes the record. `None` writes a tombstone, which is a
    /// null value.
    pub member_metadata: Vec<(String, Option<ShareGroupMemberMetadataValue>)>,
    pub target_metadata: Option<ShareGroupTargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<ShareGroupTargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<ShareGroupCurrentMemberAssignmentValue>)>,
    /// KIP-932 `ShareGroupStatePartitionMetadata` (key v15). `Some` writes the
    /// updated Initialized/deleting record after a lifecycle Initialize/Delete.
    pub state_partition_metadata: Option<ShareGroupStatePartitionMetadataValue>,
}

impl PendingShareRecords {
    fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
            && self.state_partition_metadata.is_none()
    }

    crate::coordinator::unified::persistence::encode_membership_records! {
        @method
        /// Encodes the delta as the one batch that `OffsetsLog::append` takes.
        ///
        /// # Errors
        ///
        /// Returns [`crate::error::BrokerError::Protocol`] when the group id or a member id is
        /// longer than 32767 bytes, which a non-flexible key string cannot carry.
        fn into_batch(self);
        batch, self, group_id, now_ms, owned;
            (typed, encode_share_key, ShareGroupKey);
            before_members {}
            before_target {}
            after_members {
                if let Some(v) = self.state_partition_metadata {
                    batch.push(
                        encode_share_key(&ShareGroupKey::StatePartitionMetadata {
                            group_id: group_id.into(),
                        })?,
                        Some(v.encode()),
                    );
                }
            }
    }
}

/// Build a `PendingShareRecords` set that carries the state changes for the
/// listed `affected_members`. It always includes the current group epoch, and
/// it includes the target epoch when that epoch is non-zero.
pub(super) fn snapshot_pending_after_change(
    state: &ShareGroupState,
    affected_members: &[String],
) -> PendingShareRecords {
    let mut pending = PendingShareRecords {
        group_metadata: Some(ShareGroupMetadataValue {
            epoch: state.group_epoch,
        }),
        ..Default::default()
    };
    if state.target.epoch > 0 {
        pending.target_metadata = Some(ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    crate::coordinator::unified::persistence::snapshot_members!(
        pending, state, affected_members;
        member_metadata_value, current_assignment_value;
        |mid, member| {
            if let Some(target) = state.target.per_member.get(mid) {
                pending.target_per_member.push((mid.clone(), Some(target_assignment_value(target))));
            }
        }
    );
    pending
}

pub(super) fn member_metadata_value(member: &ShareMemberState) -> ShareGroupMemberMetadataValue {
    ShareGroupMemberMetadataValue {
        rack_id: member.rack_id.clone(),
        client_id: member.client_id.clone(),
        client_host: member.client_host.clone(),
        subscribed_topic_names: member.subscribed_topic_names.iter().cloned().collect(),
    }
}

pub(super) fn current_assignment_value(
    member: &ShareMemberState,
) -> ShareGroupCurrentMemberAssignmentValue {
    ShareGroupCurrentMemberAssignmentValue {
        member_epoch: member.member_epoch,
        previous_member_epoch: member.previous_member_epoch,
        assigned_partitions: member
            .assigned_partitions
            .iter()
            .map(|(topic, parts)| (*topic, parts.clone()))
            .collect(),
    }
}

pub(super) fn target_assignment_value(
    target: &HashMap<Uuid, Vec<i32>>,
) -> ShareGroupTargetAssignmentMemberValue {
    ShareGroupTargetAssignmentMemberValue {
        topic_partitions: target
            .iter()
            .map(|(topic, parts)| (*topic, parts.clone()))
            .collect(),
    }
}

/// Build the `ShareGroupStatePartitionMetadata` (key v15) value from the live
/// initializing and initialized sets. There is one row per topic in each, and
/// the partitions are sorted for a stable encoding.
///
/// Each row names its topic. The name comes from
/// [`ShareGroupState::topic_names`], which the lifecycle hook fills from the
/// metadata image and bootstrap replay refills from the previous record. A
/// topic id with no name left — its topic was deleted from the cluster while
/// the group still held share state for it — is written as
/// [`UNKNOWN_TOPIC_NAME`], which is what Kafka's
/// `GroupMetadataManager.attachInitValue` writes in the same position.
pub(super) fn state_partition_metadata_from(
    state: &ShareGroupState,
) -> ShareGroupStatePartitionMetadataValue {
    ShareGroupStatePartitionMetadataValue {
        initializing: topic_partitions_infos(state, state.initializing.keys()),
        initialized: topic_partitions_infos(state, state.initialized.iter()),
        deleting: Vec::new(),
    }
}

/// Groups `partitions` into one named, sorted row per topic.
fn topic_partitions_infos<'a>(
    state: &ShareGroupState,
    partitions: impl Iterator<Item = &'a (Uuid, i32)>,
) -> Vec<TopicPartitionsInfo> {
    let mut by_topic: HashMap<Uuid, Vec<i32>> = HashMap::new();
    for (tid, p) in partitions {
        by_topic.entry(*tid).or_default().push(*p);
    }
    let mut topics: Vec<TopicPartitionsInfo> = by_topic
        .into_iter()
        .map(|(tid, mut parts)| {
            parts.sort_unstable();
            TopicPartitionsInfo {
                topic_id: uuid::Uuid::from_bytes(tid.0),
                topic_name: state
                    .topic_names
                    .get(&tid)
                    .map_or_else(|| UNKNOWN_TOPIC_NAME.to_owned(), Clone::clone),
                partitions: parts,
            }
        })
        .collect();
    topics.sort_by_key(|topic| topic.topic_id);
    topics
}

crate::coordinator::unified::persistence::flush_pending_records! {
    state: &ShareGroupState, pending: PendingShareRecords;
    offsets_log, coordinator, now_ms;
    group &state.group_id;
    encode pending.into_batch(&state.group_id, now_ms);
    cache coordinator.update_share_cache(&state.group_id, snapshot_seed(state));
}

/// The wall-clock reading this actor stamps share-group records with, in
/// milliseconds since the Unix epoch. It reads `std::time`, not chrono, which
/// the name predates.
///
/// This is deliberately **not** [`crate::time_util::now_ms`], for the reason
/// its twin in [`crate::coordinator::unified::actor`] gives: the two disagree
/// on the `i64`-overflow arm, which saturates to `i64::MAX` in the shared
/// helper and to `0` here.
pub(super) use crate::txn::util::now_millis as chrono_now_ms;

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn state_partition_metadata_names_every_topic_it_lists() {
        let named = Uuid([1; 16]);
        let forgotten = Uuid([2; 16]);
        let mut state = ShareGroupState::new("g");
        for partition in [1, 0] {
            state.initialized.insert((named, partition));
        }
        state.initialized.insert((forgotten, 0));
        state.initializing.insert((named, 2), 5);
        state.topic_names.insert(named, "orders".to_owned());

        // Rows sorted by topic id, partitions sorted, and the topic whose name
        // the group no longer knows gets Kafka's `<UNKNOWN>` placeholder.
        assert!(
            state_partition_metadata_from(&state)
                == ShareGroupStatePartitionMetadataValue {
                    initializing: vec![TopicPartitionsInfo {
                        topic_id: uuid::Uuid::from_bytes([1; 16]),
                        topic_name: "orders".to_owned(),
                        partitions: vec![2],
                    }],
                    initialized: vec![
                        TopicPartitionsInfo {
                            topic_id: uuid::Uuid::from_bytes([1; 16]),
                            topic_name: "orders".to_owned(),
                            partitions: vec![0, 1],
                        },
                        TopicPartitionsInfo {
                            topic_id: uuid::Uuid::from_bytes([2; 16]),
                            topic_name: UNKNOWN_TOPIC_NAME.to_owned(),
                            partitions: vec![0],
                        },
                    ],
                    deleting: Vec::new(),
                }
        );
    }

    #[test]
    fn pending_records_tombstone_omits_value() {
        let p = PendingShareRecords {
            member_metadata: vec![("m1".into(), None)],
            ..Default::default()
        };
        let batch = p.into_batch("g", 0).unwrap();
        assert!(batch.records.len() == 1);
        assert!(batch.records[0].value.is_none());
    }

    // The keys of the share-group records write the group id and the member
    // id with an `INT16` length. A string of 32767 bytes encodes, and one of
    // 32768 bytes makes the whole batch an error, not a panic in the actor.
    crate::coordinator::unified::persistence::key_string_boundaries!(PendingShareRecords, || {
        PendingShareRecords {
            group_metadata: Some(ShareGroupMetadataValue { epoch: 1 }),
            member_metadata: vec![("m".into(), None)],
            target_per_member: vec![("m".into(), None)],
            current_per_member: vec![("m".into(), None)],
            ..Default::default()
        }
    });
}
