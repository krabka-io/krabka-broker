//! The group-directory operations behind the admin RPCs — `ListGroups`,
//! `DescribeGroups`, and `DeleteGroups` — and the coordinated shutdown that
//! drains every actor.
//!
//! Each one walks the registries and talks to the actors over their mailboxes
//! rather than reading group state directly, which is what separates them from
//! the registry lookups next door.

use std::sync::Arc;

use tokio::sync::oneshot;

use super::{
    actor::{GroupActorHandle, GroupActorMessage},
    group_coordinator::{GroupCoordinator, GroupType},
    share::actor::{ShareGroupActorHandle, ShareGroupActorMessage},
    streams::{
        self,
        actor::{StreamsGroupActorHandle, StreamsGroupActorMessage},
    },
};
use crate::coordinator::{DeleteGroupError, GroupSnapshot};

impl GroupCoordinator {
    /// Snapshot every **live-classic** group for the wire `ListGroups` pass
    /// that emits `group_type="classic"`.
    ///
    /// The method walks ALL handles and selects on the group's LIVE kind, not
    /// on the spawn-time `handle.kind` hint. A KIP-848 live migration can make
    /// the two differ. The `ClassicInspect` arm replies for a classic-kind
    /// group only, so a consumer group or an upgraded group drops its reply
    /// sender and this method skips it.
    ///
    /// This keeps `list_groups` the only producer of the `classic` rows. The
    /// `ListGroups` handler emits the consumer-kind groups separately through
    /// [`consumer_group_ids`](Self::consumer_group_ids) with the tag
    /// `group_type="consumer"`, so it does NOT count them twice or mislabel
    /// them. A *downgraded* group whose handle still reads `Consumer` still
    /// appears here, because its live kind is `Classic`.
    pub async fn list_groups(&self) -> Vec<GroupSnapshot> {
        let handles: Vec<Arc<GroupActorHandle>> =
            self.groups.iter().map(|e| e.value().clone()).collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            let (tx, rx) = oneshot::channel();
            // `ClassicInspect` replies only for a classic-kind group; a
            // consumer-kind group never sends, so `rx.await` errors and we skip.
            if h.tx
                .send(GroupActorMessage::ClassicInspect { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
            {
                out.push(view.snapshot());
            }
        }
        out
    }

    /// Snapshot a single group, classic OR consumer or migrated, and return
    /// `None` when the group is unknown.
    ///
    /// The method inspects the LIVE group through [`InspectAny`] and does not
    /// gate on the spawn-time `handle.kind`. An upgraded consumer group
    /// therefore still reports.
    ///
    /// [`InspectAny`]: GroupActorMessage::InspectAny
    pub async fn describe_group(&self, group_id: &str) -> Option<GroupSnapshot> {
        let handle = self.find(group_id)?;
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::InspectAny { reply: tx })
            .await
            .ok()?;
        rx.await.ok()
    }

    /// Drop a **classic** group from the registry.
    ///
    /// The actor atomically verifies that a classic group is empty and appends
    /// its durable k2 tombstone before removing it from the registry. The
    /// method returns `NonEmpty` when the group still has live members. It
    /// returns `NotFound` when the group is unknown or is a consumer group.
    /// # Errors
    /// Returns an error when the group is not deletable or the tombstone cannot
    /// be appended.
    pub async fn delete_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
        // KIP-1071: a Streams-locked group is deleted through the streams path —
        // never fall through to the classic path, which would remove the offset-home
        // `groups` entry out from under a live streams group.
        if self.group_type(group_id) == Some(GroupType::Streams) {
            return self.delete_streams_group(group_id).await;
        }
        let handle = self.find(group_id).ok_or(DeleteGroupError::NotFound)?;
        // The actor serializes this check with Join/Leave so a concurrent join
        // cannot slip between the empty check and the tombstone append.
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicDelete { reply: tx })
            .await
            .map_err(|_| DeleteGroupError::NotFound)?;
        rx.await.map_err(|_| DeleteGroupError::NotFound)??;
        self.groups.remove(group_id);
        self.group_types.remove(group_id);
        Ok(())
    }

    /// Delete a **streams** group, per KIP-1071.
    ///
    /// The method returns `NonEmpty` when the streams actor still has live
    /// members. It returns `NotFound` when no streams actor exists for the id.
    /// In every other case it tombstones the group's records k15–21, drops the
    /// streams actor, and removes the offset-home `groups` entry. It returns
    /// `Internal` when the tombstone append fails.
    async fn delete_streams_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
        // A Streams-locked id with no live streams actor reports NotFound — the
        // safe failure mode (never silently drop an offset home). In practice a
        // live streams group always has an actor (respawned by finalize_bootstrap
        // on replay), so this only guards a genuinely-absent group.
        let handle = self
            .find_streams(group_id)
            .ok_or(DeleteGroupError::NotFound)?;
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(streams::actor::StreamsGroupActorMessage::Describe { reply: tx })
            .await
            .map_err(|_| DeleteGroupError::NotFound)?;
        let view = rx.await.map_err(|_| DeleteGroupError::NotFound)?;
        if !view.members.is_empty() {
            return Err(DeleteGroupError::NonEmpty);
        }
        // Drained group: per-member records (k16/k20/k21) were already tombstoned
        // on member leave/expiry, so only the group-level keys remain.
        let batch = streams::migration::streams_records_tombstone_batch(
            group_id,
            &[],
            crate::time_util::now_ms(),
        );
        self.offsets_log
            .append(group_id, batch)
            .await
            .map_err(|_| DeleteGroupError::Internal)?;
        self.streams_groups.remove(group_id);
        self.groups.remove(group_id);
        self.streams_seeds.remove(group_id);
        self.streams_seeds_cache.remove(group_id);
        Ok(())
    }

    pub async fn shutdown_all(&self) {
        let handles: Vec<Arc<GroupActorHandle>> =
            self.groups.iter().map(|e| e.value().clone()).collect();
        for h in handles {
            let (tx, rx) = oneshot::channel();
            if h.tx.send(GroupActorMessage::Shutdown(tx)).await.is_ok() {
                let _ = tokio::time::timeout(self.config.shutdown_ack_timeout, rx).await;
            }
        }
        let share_handles: Vec<Arc<ShareGroupActorHandle>> = self
            .share_groups
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for h in share_handles {
            let (tx, rx) = oneshot::channel();
            if h.tx
                .send(ShareGroupActorMessage::Shutdown(tx))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(self.config.shutdown_ack_timeout, rx).await;
            }
        }
        let streams_handles: Vec<Arc<StreamsGroupActorHandle>> = self
            .streams_groups
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for h in streams_handles {
            let (tx, rx) = oneshot::channel();
            if h.tx
                .send(StreamsGroupActorMessage::Shutdown(tx))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(self.config.shutdown_ack_timeout, rx).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::coordinator::unified::test_support::{await_until, make_coord};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_all_closes_all_group_actors() {
        let coord = make_coord();
        let group = coord.get_or_create_classic("classic");
        let share = coord.get_or_create_share("share");
        let streams = coord.get_or_create_streams("streams");

        coord.shutdown_all().await;

        // The ack can arrive a scheduler tick before the actor task exits
        // and drops its receiver — poll instead of racing it.
        await_until("all group actor channels closed", || {
            group.tx.is_closed() && share.tx.is_closed() && streams.tx.is_closed()
        })
        .await;
        assert!(group.tx.is_closed());
        assert!(share.tx.is_closed());
        assert!(streams.tx.is_closed());
    }
}
