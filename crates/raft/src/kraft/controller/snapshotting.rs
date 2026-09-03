//! Snapshot writing on this node: the KIP-630 interval-driven checkpoint and
//! prune, the explicit trigger, and the mandatory KIP-1155 downgrade
//! checkpoint that is retried until it succeeds.

use std::sync::Arc;

use krabka_ids::Offset;
use krabka_metadata::{MetadataImage, MetadataRecord, VotersRecord};
use krabka_units::prelude::{ByteSizeExt as _, TimeExt as _};

use super::{
    Engine,
    checkpoint::{latest_checkpoint_id, retain_latest_checkpoint, write_checkpoint},
    checkpoint_dir,
    offsets::{
        committed_records_since_snapshot, snapshot_bytes_reached, snapshot_interval_reached,
        snapshot_time_reached,
    },
    recovery::replay_committed,
};
use crate::error::RaftError;

impl Engine {
    /// (Every voter, KIP-630) once the committed offset has advanced past the
    /// last snapshot by `snapshot_interval_records` records, by
    /// `max_bytes_between_snapshots` bytes, or `max_snapshot_interval` has
    /// elapsed since the last checkpoint, serialize the current image to a
    /// checkpoint and prune the log below the snapshot boundary.
    ///
    /// This runs on every voter, not only the leader: a follower's HWM
    /// advances on every applied Fetch response just like a leader's, and
    /// Kafka's `SnapshotGenerator` is installed on every `KRaft` node,
    /// controller or broker, independently of role.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, hwm = tracing::field::Empty)
    )]
    pub fn maybe_snapshot_and_prune(&mut self) {
        if self.downgrade_snapshot_pending.is_some() {
            return;
        }
        let hwm = self.log.hwm();
        let advanced = committed_records_since_snapshot(hwm, self.last_snapshot_end_offset);
        let records_due = snapshot_interval_reached(advanced, self.snapshot_interval_records);
        let bytes_due = self.committed_bytes_since_snapshot_reached();
        let elapsed_ms = self.now().0.saturating_sub(self.last_snapshot_at_ms);
        let max_snapshot_interval_ms =
            u64::try_from(self.max_snapshot_interval.millis_i64()).unwrap_or(0);
        let time_due = snapshot_time_reached(elapsed_ms, max_snapshot_interval_ms);
        if !(records_due || bytes_due || time_due) {
            return;
        }
        tracing::Span::current().record("hwm", hwm.0);
        if let Err(error) = self.write_snapshot_and_prune() {
            tracing::error!(?error, "kraft: snapshot/prune failed");
        }
    }

    /// Whether the verbatim log bytes committed since the last snapshot have
    /// reached `max_bytes_between_snapshots`. Bounds the read at that many
    /// bytes: a read that fills the window proves at least that much data is
    /// pending without ever reading more than the threshold itself.
    fn committed_bytes_since_snapshot_reached(&self) -> bool {
        let max_bytes = self.max_bytes_between_snapshots.bytes_u64();
        if max_bytes == 0 {
            return false;
        }
        match self.log.read_committed(
            self.last_snapshot_end_offset,
            self.max_bytes_between_snapshots,
        ) {
            Ok(raw) => {
                snapshot_bytes_reached(u64::try_from(raw.total).unwrap_or(u64::MAX), max_bytes)
            }
            Err(error) => {
                tracing::error!(?error, "kraft: failed to measure bytes since last snapshot");
                false
            }
        }
    }

    pub fn write_snapshot_and_prune(&mut self) -> Result<(), RaftError> {
        let bytes = crate::snapshot::SnapshotWriter::serialize(&self.image, 0)?;
        let end_offset = self.write_snapshot_checkpoint(&bytes)?;
        self.prune_to_snapshot(end_offset)?;
        Ok(())
    }

    pub fn write_downgrade_snapshot_and_prune(&mut self) -> Result<(), RaftError> {
        let Some(pending) = self.downgrade_snapshot_pending.clone() else {
            return Ok(());
        };
        #[cfg(test)]
        if self.downgrade_snapshot_failures_remaining > 0 {
            self.downgrade_snapshot_failures_remaining -= 1;
            return Err(RaftError::Startup(
                "injected metadata downgrade snapshot failure".into(),
            ));
        }
        let bytes = crate::snapshot::SnapshotWriter::serialize(&pending.image, 0)?;
        // KIP-1155 is a snapshot reload, not only a checkpoint write. Decode
        // the exact bytes first and rebuild every metadata index from their
        // lower-version record representation before discarding the log
        // prefix. This also proves locally that the checkpoint is readable
        // before any durable state is pruned.
        let contents = crate::snapshot::SnapshotReader::read(&bytes)?;
        let mut reloaded =
            MetadataImage::from_records(pending.image.cluster_id(), &contents.metadata_records);
        if let Some(control) = &contents.control_state {
            reloaded.apply(&MetadataRecord::V1KRaftVersion(
                krabka_metadata::KRaftVersionRecord {
                    kraft_version: control.kraft_version,
                },
            ));
            reloaded.apply(&MetadataRecord::V1Voters(VotersRecord {
                voters: control.voters.clone(),
            }));
        }
        write_checkpoint(
            &checkpoint_dir(&self.data_dir),
            pending.end_offset.0,
            pending.epoch,
            &bytes,
        )?;

        // Rebuild the committed suffix before pruning. This preserves records
        // committed after the downgrade failure while keeping the checkpoint
        // itself pinned to the exact lower-version image and boundary.
        let next_pending = replay_committed(
            &self.log,
            &mut reloaded,
            pending.end_offset,
            self.metadata_raft_fetch_max,
        )?;
        reloaded.apply(&MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord {
                kraft_version: self.controls.committed_version,
            },
        ));
        reloaded.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: self.controls.committed_voters.clone(),
        }));

        self.prune_to_snapshot(pending.end_offset)?;
        self.image = reloaded;
        self.downgrade_snapshot_pending = next_pending;
        if self.downgrade_snapshot_pending.is_none() {
            let _ = self.image_tx.send(Arc::new(self.image.clone()));
        }
        Ok(())
    }

    pub fn retry_pending_downgrade_snapshot(&mut self) {
        while self.downgrade_snapshot_pending.is_some() {
            if let Err(error) = self.write_downgrade_snapshot_and_prune() {
                tracing::error!(
                    ?error,
                    "mandatory metadata downgrade snapshot remains pending"
                );
                break;
            }
        }
    }

    pub fn write_snapshot_checkpoint(&self, bytes: &[u8]) -> Result<Offset, RaftError> {
        let end_offset = self.log.hwm();
        let epoch = i32::try_from(self.core.quorum_state().leader_epoch).unwrap_or(i32::MAX);
        write_checkpoint(&checkpoint_dir(&self.data_dir), end_offset.0, epoch, bytes)?;
        Ok(end_offset)
    }

    pub fn prune_to_snapshot(&mut self, end_offset: Offset) -> Result<(), RaftError> {
        if !krabka_verified::snapshot_prune_admission(end_offset.0, self.log.hwm().0) {
            return Err(RaftError::ChangeRejected(
                "snapshot prune boundary is negative or beyond the committed frontier".into(),
            ));
        }
        self.log.prune_to(end_offset)?;
        self.last_snapshot_end_offset = end_offset;
        self.last_snapshot_at_ms = self.now().0;
        retain_latest_checkpoint(&checkpoint_dir(&self.data_dir));
        Ok(())
    }

    /// The latest local snapshot id `(end_offset, epoch)`, if any (leader's
    /// `FetchSnapshot` hint).
    pub fn latest_snapshot_id(&self) -> Option<(i64, i32)> {
        latest_checkpoint_id(&checkpoint_dir(&self.data_dir))
    }

    /// Serialize the current image into a KIP-630 checkpoint under the data dir.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, end_offset = self.log.hwm().0),
        err
    )]
    pub fn do_trigger_snapshot(&self) -> Result<(), RaftError> {
        if self.downgrade_snapshot_pending.is_some() {
            return Err(RaftError::ChangeRejected(
                "mandatory metadata downgrade snapshot is pending".into(),
            ));
        }
        let bytes = crate::snapshot::SnapshotWriter::serialize(&self.image, 0)?;
        let end_offset = self.log.hwm();
        let epoch = i32::try_from(self.core.quorum_state().leader_epoch).unwrap_or(i32::MAX);
        // Checkpoint filenames encode the raw offset (on-disk boundary).
        write_checkpoint(&checkpoint_dir(&self.data_dir), end_offset.0, epoch, &bytes)
    }
}
