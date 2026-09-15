//! Committed offsets of deleted topics.
//!
//! When the metadata image removes a topic, Kafka writes an `OffsetCommit`
//! tombstone for each offset of that topic in every group
//! (`GroupCoordinatorService.onMetadataUpdate`, which schedules
//! `OffsetMetadataManager.onTopicsDeleted` on every coordinator shard).
//! Without the tombstones a topic created again with the same name gets the
//! old offsets from `OffsetFetch`, and a consumer skips records or resets.
//!
//! This module watches the image and finds the deleted topics. Each group
//! actor decides which of its offsets to tombstone and appends the batch in
//! its own turn, so a concurrent commit cannot race the tombstone.
//!
//! # Only the coordinator writes
//!
//! Every broker watches the image, but a broker tombstones only the groups
//! whose `__consumer_offsets` partition it leads, the same ownership rule as
//! the offset-retention sweep.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, NodeId};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    GroupCoordinator, partitioner::local_partition_for_group, unified::actor::GroupActorMessage,
};
use crate::metadata_source::MetadataSource;

#[cfg(test)]
mod tests;

/// Spawn the watcher. It returns when `shutdown` is cancelled or the image
/// channel closes.
pub(crate) fn spawn(
    node_id: NodeId,
    metadata: Arc<dyn MetadataSource>,
    coordinator: Arc<GroupCoordinator>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut images = metadata.watch_image();
        let mut previous = images.borrow_and_update().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                changed = images.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let image = images.borrow_and_update().clone();
                    let deleted = deleted_topics(&previous, &image);
                    if !deleted.is_empty() {
                        let owned = |group_id: &str| {
                            local_partition_for_group(&image, node_id, group_id).is_ok()
                        };
                        on_topics_deleted(&coordinator, owned, &deleted).await;
                    }
                    previous = image;
                }
            }
        }
    });
}

/// The names of the topics of `previous` whose topic id `next` does not hold.
///
/// The watch channel can skip images, so the comparison uses topic ids: a
/// topic that was deleted and created again with the same name between two
/// images the watcher reads is still a deleted topic.
pub(crate) fn deleted_topics(previous: &MetadataImage, next: &MetadataImage) -> Vec<String> {
    let mut deleted: Vec<String> = previous
        .topics()
        .filter(|topic| next.topic_by_id(&topic.topic_id).is_none())
        .map(|topic| topic.name.clone())
        .collect();
    deleted.sort_unstable();
    deleted
}

/// Sends `topics` to every group actor that `owned` accepts, one group at a
/// time, and returns the offsets that each group tombstoned.
///
/// The watcher awaits each reply before it reads the next image. A later
/// deletion of a topic with the same name therefore cannot reach an actor
/// before this one.
pub(crate) async fn on_topics_deleted(
    coordinator: &GroupCoordinator,
    owned: impl Fn(&str) -> bool,
    topics: &[String],
) -> Vec<(String, Vec<(String, i32)>)> {
    let group_ids: Vec<String> = coordinator
        .groups
        .iter()
        .map(|entry| entry.key().clone())
        .collect();
    let mut changed = Vec::new();
    for group_id in group_ids {
        if !owned(&group_id) {
            continue;
        }
        let Some(handle) = coordinator.find(&group_id) else {
            continue;
        };
        let (reply, deleted) = oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::DeleteTopicOffsets {
                topics: topics.to_vec(),
                reply,
            })
            .await
            .is_err()
        {
            continue;
        }
        let Ok(deleted) = deleted.await else {
            continue;
        };
        if !deleted.is_empty() {
            tracing::info!(
                group_id = %group_id,
                offsets = deleted.len(),
                "tombstoned the committed offsets of deleted topics",
            );
            changed.push((group_id, deleted));
        }
    }
    changed.sort_unstable();
    changed
}
