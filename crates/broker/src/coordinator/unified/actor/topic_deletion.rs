//! The actor half of topic deletion: tombstone the offsets of deleted topics.
//!
//! `coordinator::topic_deletion` watches the metadata image and sends the names
//! of the deleted topics to every group this broker coordinates. Kafka does the
//! same in `GroupCoordinatorService.onMetadataUpdate`, which schedules
//! `OffsetMetadataManager.onTopicsDeleted` on every coordinator shard.

use std::collections::BTreeSet;

use super::{chrono_now_ms, retention::tombstone_batch};
use crate::coordinator::unified::{group::CoordinatorGroup, offsets_log::OffsetsLog};

#[cfg(test)]
mod tests;

/// Tombstones every committed offset and every open transactional offset of
/// `topics` in one batch, then removes them from the group.
///
/// Returns the tombstoned keys, sorted. A group with no offset of those topics
/// writes nothing. A failed append changes nothing and returns no keys: Kafka
/// logs the failure of `onTopicsDeleted` and does not retry it either.
pub(super) async fn delete_topic_offsets(
    group: &mut CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    topics: &[String],
) -> Vec<(String, i32)> {
    let offsets = group.offsets();
    let keys: Vec<(String, i32)> = offsets
        .committed
        .into_keys()
        .chain(offsets.pending_txn)
        .filter(|(topic, _)| topics.contains(topic))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if keys.is_empty() {
        return keys;
    }
    let batch = tombstone_batch(&group.group_id, &keys, None, chrono_now_ms());
    if let Err(error) = offsets_log.append(&group.group_id, batch).await {
        tracing::error!(
            group_id = %group.group_id,
            %error,
            "could not tombstone the offsets of deleted topics",
        );
        return Vec::new();
    }
    group.drop_offsets(&keys);
    keys
}
