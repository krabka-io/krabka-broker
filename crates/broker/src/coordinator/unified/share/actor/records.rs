//! Durable encoding of share-group state transitions. A
//! [`PendingShareRecords`] set collects the mutations of one transition,
//! encodes them as a single `RecordBatch`, and appends that batch to
//! `__consumer_offsets`. It is its own file because every handler in this
//! module writes through it.

use std::collections::HashMap;

use krabka_protocol::{primitives::uuid::Uuid, records::RecordBatch};

use super::seed::snapshot_seed;
use crate::coordinator::unified::{
    GroupCoordinator, OffsetRecordBatchBuilder,
    offsets_log::OffsetsLog,
    share::{
        persistence::{
            ShareGroupCurrentMemberAssignmentValue, ShareGroupKey, ShareGroupMemberMetadataValue,
            ShareGroupMetadataValue, ShareGroupStatePartitionMetadataValue,
            ShareGroupTargetAssignmentMemberValue, ShareGroupTargetAssignmentMetadataValue,
            encode_share_key,
        },
        state::ShareGroupState,
    },
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
    /// KIP-932 `ShareGroupStatePartitionMetadata` (key v14). `Some` writes the
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

    pub fn into_batch(self, group_id: &str, now_ms: i64) -> RecordBatch {
        let mut batch = OffsetRecordBatchBuilder::default();

        if let Some(v) = self.group_metadata {
            batch.push(
                encode_share_key(&ShareGroupKey::GroupMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.member_metadata {
            batch.push(
                encode_share_key(&ShareGroupKey::MemberMetadata {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.target_metadata {
            batch.push(
                encode_share_key(&ShareGroupKey::TargetAssignmentMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.target_per_member {
            batch.push(
                encode_share_key(&ShareGroupKey::TargetAssignmentMember {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        for (member_id, v) in self.current_per_member {
            batch.push(
                encode_share_key(&ShareGroupKey::CurrentMemberAssignment {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.state_partition_metadata {
            batch.push(
                encode_share_key(&ShareGroupKey::StatePartitionMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }

        batch.finish(now_ms)
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
    for mid in affected_members {
        if let Some(m) = state.members.get(mid) {
            pending.member_metadata.push((
                mid.clone(),
                Some(ShareGroupMemberMetadataValue {
                    rack_id: m.rack_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
                }),
            ));
            pending.current_per_member.push((
                mid.clone(),
                Some(ShareGroupCurrentMemberAssignmentValue {
                    member_epoch: m.member_epoch,
                    assigned_partitions: m
                        .assigned_partitions
                        .iter()
                        .map(|(tid, parts)| (*tid, parts.clone()))
                        .collect(),
                }),
            ));
            if let Some(target) = state.target.per_member.get(mid) {
                pending.target_per_member.push((
                    mid.clone(),
                    Some(ShareGroupTargetAssignmentMemberValue {
                        topic_partitions: target
                            .iter()
                            .map(|(tid, parts)| (*tid, parts.clone()))
                            .collect(),
                    }),
                ));
            }
        }
    }
    pending
}

/// Build the `ShareGroupStatePartitionMetadata` (key v14) value from the live
/// Initialized set. There is one `(topic_id, partitions)` row per topic, and
/// the partitions are sorted for a stable encoding.
pub(super) fn state_partition_metadata_from(
    state: &ShareGroupState,
) -> ShareGroupStatePartitionMetadataValue {
    let mut by_topic: HashMap<Uuid, Vec<i32>> = HashMap::new();
    for (tid, p) in &state.initialized {
        by_topic.entry(*tid).or_default().push(*p);
    }
    let mut initialized: Vec<(uuid::Uuid, Vec<i32>)> = by_topic
        .into_iter()
        .map(|(tid, mut parts)| {
            parts.sort_unstable();
            (uuid::Uuid::from_bytes(tid.0), parts)
        })
        .collect();
    initialized.sort_by_key(|(tid, _)| *tid);
    ShareGroupStatePartitionMetadataValue {
        initialized,
        deleting: Vec::new(),
    }
}

pub(super) async fn flush_pending(
    state: &ShareGroupState,
    pending: PendingShareRecords,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) -> Result<(), crate::error::BrokerError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = pending.into_batch(&state.group_id, now_ms);
    offsets_log.append(&state.group_id, batch).await?;
    coordinator.update_share_cache(&state.group_id, snapshot_seed(state));
    Ok(())
}

/// The wall-clock reading this actor stamps share-group records with, in
/// milliseconds since the Unix epoch. It reads `std::time`, not chrono, which
/// the name predates.
///
/// This is deliberately **not** [`crate::time_util::now_ms`], for the reason
/// its twin in [`crate::coordinator::unified::actor`] gives: the two disagree
/// on the `i64`-overflow arm, which saturates to `i64::MAX` in the shared
/// helper and to `0` here.
pub(super) fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn pending_records_tombstone_omits_value() {
        let p = PendingShareRecords {
            member_metadata: vec![("m1".into(), None)],
            ..Default::default()
        };
        let batch = p.into_batch("g", 0);
        assert!(batch.records.len() == 1);
        assert!(batch.records[0].value.is_none());
    }
}
