//! Metadata refresh of the groups that subscribe to a changed topic.
//!
//! Kafka's `GroupMetadataManager.onMetadataUpdate` runs on every metadata
//! image that the group coordinator applies. It collects the topics that the
//! image created, changed or deleted, and calls `requestMetadataRefresh` on
//! every group that subscribes to one of them (`groupsSubscribedToTopic`). The
//! next heartbeat of such a group computes its metadata hash again. When the
//! hash moved, that heartbeat bumps the group epoch and computes a new target
//! assignment. A member that subscribed to a topic before the topic existed
//! therefore gets its partitions at its next heartbeat. Kafka 4.3.1 has no
//! periodic refresh: `GroupMetadataManager.METADATA_REFRESH_INTERVAL_MS` is
//! `Integer.MAX_VALUE`.
//!
//! The image watcher of `coordinator::topic_deletion` reads every image,
//! finds the changed topics with [`changed_topics`], and sends them to the
//! groups with [`on_metadata_update`]. Each group actor decides whether it
//! subscribes to one of them.
//!
//! # Group kinds
//!
//! Kafka refreshes consumer, share and streams groups this way. A classic
//! group has nothing to refresh. Here the update goes to the consumer-group
//! actors. A share group computes its target assignment against the current
//! metadata on every heartbeat, and a streams group compares its metadata hash
//! on every heartbeat. Both therefore see a changed topic at their next
//! heartbeat without the update.
//!
//! # Only the coordinator refreshes
//!
//! Every broker watches the image, but a broker sends the update only to the
//! groups whose `__consumer_offsets` partition it leads. Kafka also applies an
//! image only to its active coordinator shards.

use std::{collections::BTreeSet, sync::Arc};

use krabka_metadata::MetadataImage;

use super::{GroupCoordinator, unified::actor::GroupActorMessage};

#[cfg(test)]
mod tests;

/// The names of the topics that `next` created, changed or deleted against
/// `previous`, sorted: Kafka's `CoordinatorMetadataDelta.changedTopicIds` and
/// `deletedTopicIds`.
///
/// A topic changed when its topic record or one of its partition records
/// differs, as a `TopicRecord`, a `PartitionRecord` or a
/// `PartitionChangeRecord` in a Kafka delta puts the topic in
/// `TopicsDelta.changedTopics`. The watch channel can skip images, so the
/// comparison is between whole images, by topic id. A topic that was deleted
/// and created again with the same name between two images is a changed topic.
#[must_use]
pub fn changed_topics(previous: &MetadataImage, next: &MetadataImage) -> Vec<String> {
    let mut changed: BTreeSet<&str> = next
        .topics()
        .filter(|topic| {
            previous.topic_by_id(&topic.topic_id).is_none_or(|before| {
                before != *topic
                    || !previous
                        .partitions_of(&topic.name)
                        .eq(next.partitions_of(&topic.name))
            })
        })
        .map(|topic| topic.name.as_str())
        .collect();
    changed.extend(
        previous
            .topics()
            .filter(|topic| next.topic_by_id(&topic.topic_id).is_none())
            .map(|topic| topic.name.as_str()),
    );
    changed.into_iter().map(str::to_owned).collect()
}

/// Sends `topics` to every group actor that `owned` accepts.
///
/// The update has no reply: a group only notes that its next heartbeat must
/// refresh the metadata. An actor handles its mailbox in order, so a heartbeat
/// that the actor receives after the update sees the request.
pub async fn on_metadata_update(
    coordinator: &GroupCoordinator,
    owned: impl Fn(&str) -> bool,
    topics: &[String],
) {
    let topics: Arc<[String]> = topics.into();
    let group_ids: Vec<String> = coordinator
        .groups
        .iter()
        .map(|entry| entry.key().clone())
        .collect();
    for group_id in group_ids {
        if !owned(&group_id) {
            continue;
        }
        let Some(handle) = coordinator.find(&group_id) else {
            continue;
        };
        // An actor that has stopped has no group left to refresh.
        let _ = handle
            .tx
            .send(GroupActorMessage::MetadataUpdate {
                topics: Arc::clone(&topics),
            })
            .await;
    }
}
