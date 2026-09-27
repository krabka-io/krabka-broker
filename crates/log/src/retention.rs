//! Retention that `Log::tick` applies. These are free functions, so tests can
//! check the policy apart from `Log`'s mutable state.

use std::{
    path::Path,
    time::{Duration, SystemTime},
};

use krabka_ids::Offset;
use tracing::instrument;

use crate::{
    error::LogError,
    io::{IoTarget, LogIo},
    name,
};

/// Suffix a segment file wears between the moment retention claims it and the
/// moment it is unlinked, mirroring Kafka's `.deleted` rename.
pub(crate) const DELETED_SUFFIX: &str = "deleted";

/// The tombstone name for `path`: the same name with `.deleted` appended.
pub(crate) fn deleted_path(path: &Path) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".");
    name.push(DELETED_SUFFIX);
    std::path::PathBuf::from(name)
}

pub fn now_ms(now: SystemTime) -> i64 {
    let millis = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

/// Remove one segment's whole file set: the `.log`, both sparse indexes, and
/// the optional `.txnindex`, `.stampindex` and `.snapshot` sidecars.
///
/// The producer snapshot is written at this segment's base offset on every
/// roll (see `Log::roll_active_segment`), so it belongs to this segment the
/// same way its `.txnindex` does. Kafka's `UnifiedLog.deleteSegments` deletes
/// it alongside the segment (`deleteProducerSnapshots`); leaving it behind
/// piles up `.snapshot` files for the life of the partition, and every
/// `Log::open` lists and sorts all of them.
///
/// Every file is first renamed to a `<name>.deleted` tombstone and only then
/// unlinked, the way Kafka's `deleteSegments` renames before it deletes. A
/// failure part-way through therefore leaves names that say what they are: a
/// tombstone nothing reads, or a file still under its live name. Neither is a
/// segment whose `.log` is gone and whose sidecars are invisible to the
/// `.log`-keyed directory scan in [`crate::Log::open`], and
/// [`crate::recovery::deleted_orphan_recover`] reclaims both on the next open.
///
/// # Errors
/// Returns the first rename or unlink error. A missing file is not one: a
/// segment need not carry any of the optional sidecars, and a retried
/// deletion finds part of the set already gone.
#[instrument(level = "debug", skip_all, fields(dir = %dir.display(), base_offset = base_offset.0), err)]
pub fn delete_segment_files(
    io: &dyn LogIo,
    dir: &Path,
    base_offset: Offset,
) -> Result<(), LogError> {
    let mut tombstones = Vec::with_capacity(6);
    for path in [
        name::log_path(dir, base_offset.0),
        name::index_path(dir, base_offset.0),
        name::timeindex_path(dir, base_offset.0),
        name::txnindex_path(dir, base_offset.0),
        name::stampindex_path(dir, base_offset.0),
        name::producer_snapshot_path(dir, base_offset.0),
    ] {
        let tombstone = deleted_path(&path);
        match io.rename(IoTarget::SegmentDeletion, &path, &tombstone) {
            Ok(()) => tombstones.push(tombstone),
            // Absent under its live name: either this segment never had the
            // sidecar, or an earlier attempt already renamed it.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if tombstone.exists() {
                    tombstones.push(tombstone);
                }
            }
            Err(error) => return Err(LogError::Io(error)),
        }
    }
    for tombstone in tombstones {
        remove_optional(io, &tombstone)?;
    }
    Ok(())
}

fn remove_optional(io: &dyn LogIo, path: &Path) -> Result<(), LogError> {
    match io.remove_file(IoTarget::SegmentDeletion, path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LogError::Io(error)),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tempfile::tempdir;

    use super::*;
    use crate::io::FileIo;

    #[test]
    fn delete_segment_files_removes_required_and_optional_sidecars() {
        let dir = tempdir().unwrap();
        let base = Offset(7);
        let paths = [
            name::log_path(dir.path(), base.0),
            name::index_path(dir.path(), base.0),
            name::timeindex_path(dir.path(), base.0),
            name::txnindex_path(dir.path(), base.0),
            name::stampindex_path(dir.path(), base.0),
            name::producer_snapshot_path(dir.path(), base.0),
        ];
        for path in &paths {
            std::fs::write(path, []).unwrap();
        }

        delete_segment_files(&FileIo, dir.path(), base).unwrap();

        assert2::assert!(paths.iter().all(|path| !path.exists()));
    }

    #[test]
    fn delete_segment_files_accepts_missing_optional_sidecars() {
        let dir = tempdir().unwrap();
        let base = Offset(8);
        for path in [
            name::log_path(dir.path(), base.0),
            name::index_path(dir.path(), base.0),
            name::timeindex_path(dir.path(), base.0),
        ] {
            std::fs::write(path, []).unwrap();
        }

        delete_segment_files(&FileIo, dir.path(), base).unwrap();
    }

    /// Every file is renamed to its `.deleted` tombstone before any of them is
    /// unlinked, so a failure part-way through leaves names that say what they
    /// are rather than a `.log` that is gone and sidecars nothing can see.
    #[test]
    fn deletion_renames_every_file_to_a_tombstone_before_it_unlinks_any() {
        /// Lets every rename through and refuses every unlink.
        #[derive(Debug)]
        struct NoUnlink;

        impl LogIo for NoUnlink {
            fn remove_file(&self, _target: IoTarget, _path: &Path) -> std::io::Result<()> {
                Err(std::io::ErrorKind::PermissionDenied.into())
            }
        }

        let dir = tempdir().unwrap();
        let base = Offset(9);
        let live = [
            name::log_path(dir.path(), base.0),
            name::index_path(dir.path(), base.0),
            name::timeindex_path(dir.path(), base.0),
            name::stampindex_path(dir.path(), base.0),
        ];
        for path in &live {
            std::fs::write(path, b"bytes").unwrap();
        }

        let error = delete_segment_files(&NoUnlink, dir.path(), base)
            .expect_err("the refused unlink must be reported");

        check!(matches!(error, LogError::Io(_)));
        for path in &live {
            check!(!path.exists(), "renamed away: {}", path.display());
            check!(
                deleted_path(path).exists(),
                "tombstoned: {}",
                deleted_path(path).display()
            );
        }

        // The retry finds the live names gone and the tombstones present, and
        // finishes the job from there.
        delete_segment_files(&FileIo, dir.path(), base).unwrap();
        for path in &live {
            check!(!deleted_path(path).exists());
        }
    }

    #[test]
    fn remove_optional_propagates_non_missing_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("not-a-file");
        std::fs::create_dir(&path).unwrap();

        assert2::assert!(let Err(LogError::Io(_)) = remove_optional(&FileIo, &path));
    }

    #[test]
    fn remove_optional_ignores_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent-file");
        assert2::assert!(remove_optional(&FileIo, &path).is_ok());
    }
}
