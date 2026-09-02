//! The observer's durable `__cluster_metadata` state.
//!
//! A broker-only node keeps its metadata image in the same on-disk layout a
//! controller uses: KIP-630 `<end_offset>-<epoch>.checkpoint` artifacts under
//! `@metadata-0` in the metadata log directory. The store owns two writes into
//! that directory — the checkpoint the observer fetched from a controller, and
//! the one it serializes for itself once it has applied `interval` records past
//! the last — and the read that resumes from whichever is latest.
//!
//! Resuming matters because the controller prunes. An observer that always
//! restarted at offset 0 would ask a pruned leader for records that no longer
//! exist, and every restart would cost a full snapshot transfer even when the
//! node had been caught up moments earlier.
//!
//! A checkpoint here is always self-consistent: the image it holds is exactly
//! the records below the `end_offset` in its name, so restoring it and fetching
//! from that offset loses nothing. The applied offset is therefore never
//! persisted on its own — an offset ahead of the stored image would silently
//! skip the records in between.

use std::path::{Path, PathBuf};

use krabka_metadata::MetadataImage;
use krabka_raft::kraft::{
    checkpoint_dir,
    controller::checkpoint::{
        latest_checkpoint_id, load_checkpoint_by_id, retain_latest_checkpoint, write_checkpoint,
    },
};
use tracing::{debug, info, warn};

/// Epoch recorded in a checkpoint the observer wrote itself.
///
/// The observer metadata fetch (1004) carries no leader epoch, so a self-written
/// checkpoint has none to record. The id ordering compares the end offset first
/// and only breaks ties on the epoch, so this placeholder never hides a
/// checkpoint at a higher offset.
const OBSERVER_EPOCH: i32 = 0;

/// The KIP-630 checkpoint directory a broker-only observer reads and writes.
pub(super) struct ObserverStore {
    dir: PathBuf,
    /// Applied records between self-written checkpoints. `0` disables them, so
    /// the store then holds only the snapshots fetched from a controller.
    interval: u64,
    /// Id of the checkpoint currently on disk, if any.
    persisted: Option<(i64, i32)>,
}

impl ObserverStore {
    /// Open the store under `data_dir`, adopting whatever checkpoint is already
    /// there. Nothing is written until the observer installs a snapshot or
    /// advances `interval` records.
    pub(super) fn open(data_dir: &Path, interval: u64) -> Self {
        let dir = checkpoint_dir(data_dir);
        let persisted = latest_checkpoint_id(&dir);
        Self {
            dir,
            interval,
            persisted,
        }
    }

    /// The image and next fetch offset to resume from, or `None` when this node
    /// has no checkpoint and must replicate the log from its start.
    pub(super) fn resume(&self, cluster_id: uuid::Uuid) -> Option<(MetadataImage, u64)> {
        let (end_offset, epoch) = self.persisted?;
        let bytes = load_checkpoint_by_id(&self.dir, end_offset, epoch)?;
        let records = match krabka_raft::deserialize_metadata_snapshot(&bytes) {
            Ok(records) => records,
            Err(error) => {
                // A checkpoint that will not decode is not fatal: the observer
                // falls back to fetching from the start, and the controller
                // answers that with a snapshot of its own.
                warn!(%error, end_offset, epoch, "observer checkpoint is unreadable");
                return None;
            }
        };
        let fetch_offset = u64::try_from(end_offset).ok()?;
        info!(
            end_offset,
            epoch,
            records = records.len(),
            "observer resuming from its metadata checkpoint"
        );
        Some((
            MetadataImage::from_records(cluster_id, &records),
            fetch_offset,
        ))
    }

    /// Persist the exact checkpoint bytes fetched from a controller under the
    /// id the controller named them by.
    pub(super) fn save_fetched_snapshot(&mut self, id: (i64, i32), bytes: &[u8]) {
        self.write(id, bytes);
    }

    /// Serialize `image` as this observer's own checkpoint once it has applied
    /// `interval` records past the last one, so a restart resumes near where it
    /// left off instead of re-fetching a whole snapshot.
    ///
    /// `next_fetch_offset` is one past the last applied record, which is exactly
    /// the KIP-630 `end_offset` the image covers.
    pub(super) fn maybe_checkpoint(&mut self, image: &MetadataImage, next_fetch_offset: u64) {
        if self.interval == 0 {
            return;
        }
        let Ok(end_offset) = i64::try_from(next_fetch_offset) else {
            return;
        };
        let advanced = end_offset.saturating_sub(self.persisted.map_or(0, |(offset, _)| offset));
        if u64::try_from(advanced).unwrap_or(0) < self.interval {
            return;
        }
        let epoch = self.persisted.map_or(OBSERVER_EPOCH, |(_, epoch)| epoch);
        match krabka_raft::serialize_metadata_snapshot(image, 0) {
            Ok(bytes) => self.write((end_offset, epoch), &bytes),
            Err(error) => warn!(%error, end_offset, "observer failed to serialize its checkpoint"),
        }
    }

    /// Write one checkpoint and drop every older one, keeping the directory
    /// single-snapshot the way a controller's is. A failed write is not fatal:
    /// the observer keeps serving from memory and resumes from the previous
    /// checkpoint, or from the log start, after a restart.
    fn write(&mut self, id: (i64, i32), bytes: &[u8]) {
        let (end_offset, epoch) = id;
        if let Err(error) = write_checkpoint(&self.dir, end_offset, epoch, bytes) {
            warn!(%error, end_offset, epoch, "observer checkpoint write failed");
            return;
        }
        self.persisted = Some(id);
        retain_latest_checkpoint(&self.dir);
        debug!(end_offset, epoch, "observer wrote a metadata checkpoint");
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use uuid::Uuid;

    use super::*;

    fn image_with(topics: &[&str]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for name in topics {
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: (*name).to_string(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            }));
        }
        image
    }

    /// The round trip the restart depends on: what the observer persisted comes
    /// back as the same image, paired with the offset to fetch from next.
    #[test]
    fn a_written_checkpoint_resumes_as_its_image_and_offset() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = ObserverStore::open(dir.path(), 4);

        assert!(store.resume(Uuid::nil()).is_none());

        let image = image_with(&["resumed"]);
        store.maybe_checkpoint(&image, 9);

        let reopened = ObserverStore::open(dir.path(), 4);
        let (restored, fetch_offset) = reopened.resume(Uuid::nil()).expect("a checkpoint exists");
        assert!(fetch_offset == 9);
        assert!(restored.topic("resumed").is_some());
    }

    /// Checkpointing is interval-driven, so an observer applying a steady
    /// trickle of records does not re-serialize its whole image on every fetch.
    #[test]
    fn checkpoints_are_written_only_once_the_interval_has_been_applied() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = ObserverStore::open(dir.path(), 10);
        let image = image_with(&["interval"]);

        store.maybe_checkpoint(&image, 9);
        assert!(
            ObserverStore::open(dir.path(), 10)
                .resume(Uuid::nil())
                .is_none()
        );

        store.maybe_checkpoint(&image, 10);
        let (_, fetch_offset) = ObserverStore::open(dir.path(), 10)
            .resume(Uuid::nil())
            .expect("the interval was reached");
        assert!(fetch_offset == 10);

        // The next one measures from the checkpoint just written, not from 0.
        store.maybe_checkpoint(&image, 19);
        let (_, fetch_offset) = ObserverStore::open(dir.path(), 10)
            .resume(Uuid::nil())
            .expect("the previous checkpoint stands");
        assert!(fetch_offset == 10);
    }

    /// An interval of zero is the disabled setting: only snapshots fetched from
    /// a controller reach the disk.
    #[test]
    fn a_zero_interval_writes_no_checkpoint_of_its_own() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = ObserverStore::open(dir.path(), 0);

        store.maybe_checkpoint(&image_with(&["disabled"]), 100_000);

        assert!(
            ObserverStore::open(dir.path(), 0)
                .resume(Uuid::nil())
                .is_none()
        );
    }

    /// The directory stays single-snapshot: a newer checkpoint replaces the one
    /// it supersedes rather than accumulating beside it.
    #[test]
    fn writing_a_checkpoint_retains_only_the_latest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = ObserverStore::open(dir.path(), 1);
        let image = image_with(&["retained"]);

        store.maybe_checkpoint(&image, 4);
        store.maybe_checkpoint(&image, 8);

        let entries: Vec<_> = std::fs::read_dir(checkpoint_dir(dir.path()))
            .expect("checkpoint dir")
            .flatten()
            .collect();
        assert!(entries.len() == 1);
        let (_, fetch_offset) = ObserverStore::open(dir.path(), 1)
            .resume(Uuid::nil())
            .expect("the latest checkpoint");
        assert!(fetch_offset == 8);
    }

    /// A corrupt checkpoint must not wedge the node: resuming reports "nothing
    /// to resume from", and the observer replicates from the log start.
    #[test]
    fn an_unreadable_checkpoint_resumes_from_the_log_start() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = ObserverStore::open(dir.path(), 1);
        store.save_fetched_snapshot((12, 3), b"not a checkpoint");

        assert!(
            ObserverStore::open(dir.path(), 1)
                .resume(Uuid::nil())
                .is_none()
        );
    }
}
