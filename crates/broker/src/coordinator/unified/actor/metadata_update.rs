//! The actor half of a metadata update: a consumer group that subscribes to a
//! changed topic asks its next heartbeat to refresh the metadata.
//!
//! `coordinator::metadata_update` watches the metadata image and sends the
//! names of the created, changed and deleted topics to every group that this
//! broker coordinates. Kafka does the same in
//! `GroupMetadataManager.onMetadataUpdate`, which calls
//! `requestMetadataRefresh` on every group that `groupsSubscribedToTopic`
//! names. The refresh itself runs in the heartbeat, in
//! `member_state::refresh_expired_metadata`.

use crate::coordinator::unified::group::CoordinatorGroup;

#[cfg(test)]
mod tests;

/// Requests a metadata refresh of a consumer group that subscribes to one of
/// `topics`.
///
/// A classic group has no metadata to refresh: Kafka's
/// `ClassicGroup.requestMetadataRefresh` does nothing.
pub(super) fn on_metadata_update(group: &mut CoordinatorGroup, topics: &[String]) {
    if let Some(state) = group.as_consumer_mut()
        && state.subscribes_to_any(topics)
    {
        state.request_metadata_refresh();
    }
}
