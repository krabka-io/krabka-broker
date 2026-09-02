//! The on-disk KIP-630 metadata checkpoints: the byte window a `FetchSnapshot`
//! caller reads out of the newest `<end_offset>-<epoch>.checkpoint`, the scan
//! that finds that file, and the manual snapshot trigger. The checkpoint file
//! layout is read straight from disk here rather than through the engine, so it
//! is kept apart from the rest of the handle.

use super::ControllerHandle;
use crate::error::RaftError;

/// A contiguous byte window of the latest metadata `.checkpoint`, returned by
/// [`ControllerHandle::read_snapshot_range`] to back the broker's
/// `FetchSnapshot` handler.
#[derive(Debug, PartialEq)]
pub struct SnapshotSlice {
    pub end_offset: i64,
    pub epoch: i32,
    pub total_size: i64,
    pub bytes: bytes::Bytes,
}

/// Outcome of [`ControllerHandle::read_snapshot_range`]. The broker's
/// `FetchSnapshot` handler maps each variant to its Kafka error code:
/// `NoSnapshot` → `SNAPSHOT_NOT_FOUND`, `OutOfRange` → `POSITION_OUT_OF_RANGE`.
pub enum SnapshotRange {
    /// No `.checkpoint` exists yet.
    NoSnapshot,
    /// `position` is strictly past the snapshot's end byte. A `position`
    /// exactly at the end is valid and yields an empty `Slice`.
    OutOfRange,
    /// The requested byte window.
    Slice(SnapshotSlice),
}

impl ControllerHandle {
    /// Read up to `max_bytes` of the latest metadata snapshot starting at
    /// `position`. Reads the engine's `.checkpoint` artifacts directly (the
    /// engine writes a bare KIP-630 checkpoint, no `.meta` sidecar).
    ///
    /// `position` and `max_bytes` stay the raw KIP-595 `FetchSnapshot` `int64`
    /// and `int32`: both are byte offsets into an on-disk checkpoint that the
    /// broker's handler forwards straight off the wire, so there is no domain
    /// layer between the decode and the slice index for a quantity to occupy.
    #[must_use]
    pub fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange {
        let Some((id, bytes)) =
            load_latest_checkpoint(&crate::kraft::checkpoint_dir(&self.data_dir))
        else {
            return SnapshotRange::NoSnapshot;
        };
        let pos = usize::try_from(position.max(0)).unwrap_or(0);
        if pos > bytes.len() {
            return SnapshotRange::OutOfRange;
        }
        let max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
        let slice = crate::snapshot::SnapshotReader::byte_range(&bytes, pos, max);
        SnapshotRange::Slice(SnapshotSlice {
            end_offset: id.0,
            epoch: id.1,
            total_size: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            bytes: bytes::Bytes::copy_from_slice(slice),
        })
    }

    /// Manually trigger a metadata snapshot (KIP-630 checkpoint) on this node.
    ///
    /// # Errors
    /// Returns [`RaftError`] if serialization or the file write fails.
    pub async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        self.engine.trigger_snapshot().await
    }
}

/// Scan `dir` for `<end_offset>-<epoch>.checkpoint` artifacts and return the
/// highest `(end_offset, epoch)` plus its raw bytes. Matches the bare-checkpoint
/// format the engine writes (no `.meta` sidecar).
pub(super) fn load_latest_checkpoint(dir: &std::path::Path) -> Option<((i64, i32), Vec<u8>)> {
    let ((off, ep), path) = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            let id = crate::kraft::controller::parse_checkpoint_name(name)?;
            Some((id, entry.path()))
        })
        .max_by_key(|(id, _)| *id)?;
    let bytes = std::fs::read(&path).ok()?;
    Some(((off, ep), bytes))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::{BootstrapMode, ControllerConfig},
        controller::Controller,
        types::NodeId,
    };

    #[test]
    fn load_latest_checkpoint_picks_highest_offset_then_epoch() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path()
                .join("00000000000000000001-0000000009.checkpoint"),
            b"old-offset",
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join("00000000000000000002-0000000001.checkpoint"),
            b"old-epoch",
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join("00000000000000000002-0000000003.checkpoint"),
            b"best",
        )
        .unwrap();
        std::fs::write(dir.path().join("9-9.txt"), b"ignored suffix").unwrap();
        std::fs::write(
            dir.path().join("not-a-checkpoint.checkpoint"),
            b"ignored name",
        )
        .unwrap();

        let latest = load_latest_checkpoint(dir.path()).expect("checkpoint");
        assert2::assert!(latest == ((2, 3), b"best".to_vec()));
    }

    #[tokio::test]
    async fn read_snapshot_range_allows_exact_end_but_rejects_past_end() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Join,
            initial_voters: krabka_metadata::VoterSet::from_voters(std::iter::empty()),
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("join start");
        let checkpoint_dir = crate::kraft::checkpoint_dir(dir.path());
        std::fs::create_dir_all(&checkpoint_dir).unwrap();
        std::fs::write(
            checkpoint_dir.join("00000000000000000010-0000000004.checkpoint"),
            b"abc",
        )
        .unwrap();

        match ctrl.read_snapshot_range(3, 10) {
            SnapshotRange::Slice(slice) => {
                assert2::assert!(
                    slice
                        == SnapshotSlice {
                            end_offset: 10,
                            epoch: 4,
                            total_size: 3,
                            bytes: bytes::Bytes::new(),
                        }
                );
            }
            other => panic!(
                "position exactly at snapshot end should yield an empty slice, got {:?}",
                std::mem::discriminant(&other)
            ),
        }

        assert2::assert!(matches!(
            ctrl.read_snapshot_range(4, 10),
            SnapshotRange::OutOfRange
        ));
        ctrl.shutdown().await;
    }
}
