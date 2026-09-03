//! The observer's durable `__cluster_metadata` state.
//!
//! A broker-only node keeps its metadata image as KIP-630
//! `<end_offset>-<epoch>.checkpoint` artifacts, written with the same helpers a
//! controller uses. The store owns two writes — the checkpoint the observer
//! fetched from a controller, and the one it serializes for itself once it has
//! applied `interval` records past the last — and the read that resumes from
//! whichever is latest.
//!
//! Resuming matters because the controller prunes. An observer that always
//! restarted at offset 0 would ask a pruned leader for records that no longer
//! exist, and every restart would cost a full snapshot transfer even when the
//! node had been caught up moments earlier.
//!
//! ## Why this is not the controller's checkpoint directory
//!
//! These artifacts live in [`OBSERVER_SUBDIR`], deliberately *beside* the
//! controller's `@metadata-0` rather than in it, because an observer checkpoint
//! is not a valid controller checkpoint and a controller must never load one:
//!
//! - It carries no KIP-853 control state. The observer skips control batches
//!   when it applies a fetch, and `deserialize_metadata_snapshot` drops the
//!   controls in a snapshot it installs, so the image behind these bytes has
//!   the default `kraft.version` and an empty voter set. `KraftController::open`
//!   treats a checkpoint's controls as authoritative and copies them over the
//!   durable quorum state, so a controller booting on one would come up with no
//!   voters.
//! - It has no matching log. A controller recovers the image from its
//!   checkpoint and then replays its own `KraftLog` from the log start; an
//!   observer keeps no log at all, so the boundary the checkpoint names would
//!   not exist in the log beside it.
//!
//! A checkpoint here is always self-consistent for the observer's own purpose:
//! the image it holds is exactly the records below the `end_offset` in its
//! name, so restoring it and fetching from that offset loses nothing. The
//! applied offset is therefore never persisted on its own — an offset ahead of
//! the stored image would silently skip the records in between.

use std::path::{Path, PathBuf};

use krabka_metadata::MetadataImage;
use krabka_raft::kraft::controller::checkpoint::{
    latest_checkpoint_id, load_checkpoint_by_id, retain_latest_checkpoint, write_checkpoint,
};
use tracing::{debug, info, warn};

/// Directory holding the observer's checkpoints, under the metadata log dir.
///
/// It sits beside the controller's `@metadata-0`, never inside it: see the
/// module docs for why a controller must not load one of these.
const OBSERVER_SUBDIR: &str = "observer";

/// Epoch recorded in a checkpoint the observer wrote itself.
///
/// The observer metadata fetch (1004) carries no leader epoch, so a self-written
/// checkpoint has none to record. Nothing outside this store reads the epoch —
/// it only breaks ties in the id ordering, which compares the end offset first
/// — so a fixed placeholder is honest where a copied one would not be.
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
        let dir = data_dir.join(OBSERVER_SUBDIR);
        let persisted = latest_checkpoint_id(&dir);
        Self {
            dir,
            interval,
            persisted,
        }
    }

    /// The image and next fetch offset to resume from, or `None` when this node
    /// has no checkpoint and must replicate the log from its start.
    pub(super) fn resume(&mut self, cluster_id: uuid::Uuid) -> Option<(MetadataImage, u64)> {
        let (end_offset, epoch) = self.persisted?;
        let bytes = load_checkpoint_by_id(&self.dir, end_offset, epoch)?;
        let records = match krabka_raft::deserialize_metadata_snapshot(&bytes) {
            Ok(records) => records,
            Err(error) => {
                // A checkpoint that will not decode is not fatal on its own:
                // the observer falls back to fetching from the start, and the
                // controller answers that with a snapshot of its own. But it
                // has to go, not just be skipped. It keeps the highest id in
                // the directory, so `retain_latest_checkpoint` would delete
                // the replacement in its favour and every restart would repeat
                // the whole transfer.
                warn!(%error, end_offset, epoch, "discarding an unreadable observer checkpoint");
                self.discard();
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
        match krabka_raft::serialize_metadata_snapshot(image, 0) {
            Ok(bytes) => self.write((end_offset, OBSERVER_EPOCH), &bytes),
            Err(error) => warn!(%error, end_offset, "observer failed to serialize its checkpoint"),
        }
    }

    /// Empty the store. Best-effort: what cannot be removed is left behind, and
    /// the next write supersedes it by offset anyway.
    fn discard(&mut self) {
        self.persisted = None;
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.ends_with(".checkpoint"))
            {
                let _ = std::fs::remove_file(entry.path());
            }
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

        let mut reopened = ObserverStore::open(dir.path(), 4);
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

        let entries: Vec<_> = std::fs::read_dir(dir.path().join(OBSERVER_SUBDIR))
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
    ///
    /// It must also be *gone*, not merely skipped. It holds the highest id in
    /// the directory, so leaving it there would have `retain_latest_checkpoint`
    /// delete the snapshot fetched to replace it — usually an older one, since
    /// the observer's own checkpoint runs ahead of the controller's latest —
    /// and every restart would repeat the whole transfer.
    #[test]
    fn an_unreadable_checkpoint_is_discarded_so_a_replacement_can_stick() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = ObserverStore::open(dir.path(), 1);
        store.save_fetched_snapshot((12, 3), b"not a checkpoint");

        let mut reopened = ObserverStore::open(dir.path(), 1);
        assert!(reopened.resume(Uuid::nil()).is_none());

        // An older, valid snapshot now survives being written.
        let replacement =
            krabka_raft::serialize_metadata_snapshot(&image_with(&["replacement"]), 0)
                .expect("serialize");
        reopened.save_fetched_snapshot((4, 1), &replacement);

        let (restored, fetch_offset) = ObserverStore::open(dir.path(), 1)
            .resume(Uuid::nil())
            .expect("the replacement is the store's checkpoint now");
        assert!(fetch_offset == 4);
        assert!(restored.topic("replacement").is_some());
    }

    /// The observer's checkpoints sit beside the controller's `@metadata-0`,
    /// never inside it. An observer checkpoint carries no KIP-853 control state
    /// and has no log to match its boundary, so a controller that loaded one —
    /// which `KraftController::open` would do for anything in `@metadata-0` —
    /// would come up with an empty voter set and a log that disagrees with its
    /// image.
    #[test]
    fn the_observer_never_writes_into_the_controllers_checkpoint_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = ObserverStore::open(dir.path(), 1);

        store.save_fetched_snapshot((7, 2), b"fetched");
        store.maybe_checkpoint(&image_with(&["own"]), 12);

        let controller_dir = krabka_raft::kraft::checkpoint_dir(dir.path());
        let controller_entries =
            std::fs::read_dir(&controller_dir).map_or(0, |entries| entries.flatten().count());
        assert!(
            controller_entries == 0,
            "observer wrote into {}",
            controller_dir.display()
        );
        assert!(
            std::fs::read_dir(dir.path().join(OBSERVER_SUBDIR))
                .expect("observer dir")
                .flatten()
                .count()
                > 0
        );
    }
}
