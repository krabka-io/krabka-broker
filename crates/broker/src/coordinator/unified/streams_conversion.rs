//! The KIP-1071 cold upgrade and downgrade that flip a drained group between
//! the classic and the streams protocol in place.
//!
//! The two directions mirror each other — inspect the live actor for members,
//! tombstone the records of the protocol being left, and force the type lock —
//! so they are easiest to keep correct side by side.

use std::sync::Arc;

use super::{
    actor::GroupActorMessage,
    group_coordinator::{GroupCoordinator, GroupType},
    streams,
};

impl GroupCoordinator {
    /// KIP-1071 cold upgrade: convert a drained classic `group_id` to a
    /// streams group in place.
    ///
    /// The method tombstones the classic k2 `GroupMetadata` and forces the
    /// type lock to `Streams`. The committed offsets survive untouched. The
    /// classic actor stays in the `groups` map, so `OffsetFetch` requests can
    /// still read back the committed offset state.
    ///
    /// The method returns `NotClassic` for a non-classic group, and the caller
    /// then serves it as normal. It returns `Converted` after a successful
    /// flip. It returns `RejectLiveMembers` when live classic members remain,
    /// because Kafka does not support an online streams migration.
    pub(crate) async fn try_convert_classic_to_streams(
        self: &Arc<Self>,
        group_id: &str,
        now_ms: i64,
    ) -> Result<streams::migration::ConvertOutcome, crate::error::BrokerError> {
        use streams::migration::{ConvertOutcome, classic_group_metadata_tombstone_batch};

        if self.group_type(group_id) != Some(GroupType::Classic) {
            return Ok(ConvertOutcome::NotClassic);
        }

        // Inspect the live classic actor (if any) for remaining members.
        if let Some(handle) = self.find(group_id) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if handle
                .tx
                .send(GroupActorMessage::ClassicInspect { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
                && !view.members.is_empty()
            {
                return Ok(ConvertOutcome::RejectLiveMembers);
            }
        }

        // Drained classic group → convert. Tombstone the classic k2 GroupMetadata
        // to clear any persisted classic metadata (defensive + matching the
        // KIP-848 upgrade flip; a no-op on replay when none was persisted). Flip
        // the type lock to Streams; the classic actor (if any) stays in
        // `self.groups` so its committed_offsets remain accessible to
        // `OffsetFetch` without a full replay cycle.
        let batch = classic_group_metadata_tombstone_batch(group_id, now_ms);
        self.offsets_log.append(group_id, batch).await?;
        self.mark_streams_after_upgrade(group_id);
        Ok(ConvertOutcome::Converted)
    }

    /// KIP-1071 cold downgrade: convert a drained streams `group_id` to a
    /// classic group in place.
    ///
    /// The method tombstones the streams records k15–21, forces the type lock
    /// to `Classic`, and drops the streams actor. The committed offsets, k0
    /// and k1, and the offset-home `groups` entry survive.
    ///
    /// The method returns `NotStreams` for a non-streams group, and the caller
    /// then serves the classic `JoinGroup` as normal. It returns `Converted`
    /// after a successful flip. It returns `RejectLiveMembers` when the
    /// streams group still has live members, because Kafka does not support an
    /// online streams migration. It is the mirror of
    /// [`Self::try_convert_classic_to_streams`].
    pub(crate) async fn try_convert_streams_to_classic(
        self: &Arc<Self>,
        group_id: &str,
        now_ms: i64,
    ) -> Result<streams::migration::DowngradeOutcome, crate::error::BrokerError> {
        use streams::{
            actor::StreamsGroupActorMessage,
            migration::{DowngradeOutcome, streams_records_tombstone_batch},
        };

        if self.group_type(group_id) != Some(GroupType::Streams) {
            return Ok(DowngradeOutcome::NotStreams);
        }

        // Reject if the streams actor (if any) still has live members; a drained
        // group falls through to convert. Mirrors slice 1's `ClassicInspect` check.
        if let Some(handle) = self.find_streams(group_id) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if handle
                .tx
                .send(StreamsGroupActorMessage::Describe { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
                && !view.members.is_empty()
            {
                return Ok(DowngradeOutcome::RejectLiveMembers);
            }
        }

        // Drained streams group → convert. Tombstone the group-level streams keys
        // (k15/k17/k18/k19), flip the lock to Classic, drop the streams actor. A
        // drained group's per-member records (k16/k20/k21) were already tombstoned
        // when those members left/expired, so no member ids are needed here. The
        // offset-home `groups` entry stays.
        let batch = streams_records_tombstone_batch(group_id, &[], now_ms);
        self.offsets_log.append(group_id, batch).await?;
        self.mark_classic_after_streams_downgrade(group_id);
        self.streams_groups.remove(group_id);
        Ok(DowngradeOutcome::Converted)
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::{DeleteGroupError, unified::test_support::make_coord_with_log};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conversion_paths_update_type_locks_and_report_missing_streams() {
        let (coord, offsets_log) = make_coord_with_log();
        assert!(
            coord
                .try_convert_classic_to_streams("fresh", 100)
                .await
                .unwrap()
                == streams::migration::ConvertOutcome::NotClassic
        );

        coord.mark_classic("g");
        check!(
            coord
                .try_convert_classic_to_streams("g", 101)
                .await
                .unwrap()
                == streams::migration::ConvertOutcome::Converted
        );
        check!(coord.group_type("g") == Some(GroupType::Streams));
        check!(offsets_log.appended.lock().await.len() == 1);

        check!(
            coord
                .try_convert_streams_to_classic("fresh", 102)
                .await
                .unwrap()
                == streams::migration::DowngradeOutcome::NotStreams
        );
        check!(
            coord
                .try_convert_streams_to_classic("g", 103)
                .await
                .unwrap()
                == streams::migration::DowngradeOutcome::Converted
        );
        check!(coord.group_type("g") == Some(GroupType::Classic));
        check!(offsets_log.appended.lock().await.len() == 2);

        coord.mark_streams("missing-streams-actor");
        assert!(
            coord.delete_group("missing-streams-actor").await == Err(DeleteGroupError::NotFound)
        );
    }
}
