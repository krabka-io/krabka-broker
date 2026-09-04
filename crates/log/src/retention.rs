//! Retention that `Log::tick` applies. These are free functions, so tests can
//! check the policy apart from `Log`'s mutable state.

use std::{
    path::Path,
    time::{Duration, SystemTime},
};

use krabka_ids::Offset;
use krabka_units::prelude::TimeExt as _;
use tracing::instrument;

use crate::{
    config::LogConfig,
    error::LogError,
    io::{IoTarget, LogIo},
    name,
    segment::Segment,
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

#[instrument(
    level = "debug",
    skip_all,
    fields(sealed = sealed.len(), evicted = tracing::field::Empty),
)]
pub fn time_based_evict(sealed: &[&Segment], config: &LogConfig, now: SystemTime) -> Vec<Offset> {
    let Some(retention) = config.retention else {
        return Vec::new();
    };
    // Truncating, not rounding: the cutoff is compared against on-disk batch
    // timestamps, so a sub-millisecond retention window must not round up into
    // evicting a segment a millisecond early.
    let cutoff_ms = now_ms(now).saturating_sub(retention.millis_i64_trunc());
    let out: Vec<Offset> = sealed
        .iter()
        .take_while(|s| s.max_timestamp() < cutoff_ms)
        .map(|s| s.base_offset())
        .collect();
    tracing::Span::current().record("evicted", out.len());
    out
}

/// Remove one segment's whole file set: the `.log`, both sparse indexes, and
/// the optional `.txnindex` and `.stampindex` sidecars.
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
/// segment need not carry either optional sidecar, and a retried deletion
/// finds part of the set already gone.
#[instrument(level = "debug", skip_all, fields(dir = %dir.display(), base_offset = base_offset.0), err)]
pub fn delete_segment_files(
    io: &dyn LogIo,
    dir: &Path,
    base_offset: Offset,
) -> Result<(), LogError> {
    let mut tombstones = Vec::with_capacity(5);
    for path in [
        name::log_path(dir, base_offset.0),
        name::index_path(dir, base_offset.0),
        name::timeindex_path(dir, base_offset.0),
        name::txnindex_path(dir, base_offset.0),
        name::stampindex_path(dir, base_offset.0),
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
    use krabka_units::prelude::{ByteSize, ByteSizeExt as _};
    use tempfile::tempdir;

    use super::*;
    use crate::io::FileIo;

    /// A sealed segment carrying `n` two-record batches, timestamps from
    /// `ts_base`.
    fn sealed_segment(dir: &std::path::Path, base: i64, batches: i64, ts_base: i64) -> Segment {
        use bytes::Bytes;
        use krabka_protocol::records::{Record, RecordBatch};

        let mut seg = Segment::create(dir, Offset(base)).expect("create segment");
        for i in 0..batches {
            let first = base + i * 2;
            let ts = ts_base + i;
            let mut batch = RecordBatch {
                base_offset: first,
                base_timestamp: ts,
                max_timestamp: ts,
                last_offset_delta: 1,
                ..RecordBatch::default()
            };
            for delta in 0..2i32 {
                batch.records.push(Record {
                    offset_delta: delta,
                    key: Some(Bytes::from(format!("k{delta}"))),
                    value: Some(Bytes::from(vec![b'v'; 128])),
                    ..Default::default()
                });
            }
            seg.append(&batch, ByteSize::ZERO).expect("append");
        }
        seg.seal();
        seg
    }

    /// The time cutoff is exclusive: a segment whose newest record sits
    /// exactly on it is still inside the window and is kept.
    #[test]
    fn time_eviction_keeps_a_segment_sitting_exactly_on_the_cutoff() {
        let dir = tempdir().unwrap();
        let seg = sealed_segment(dir.path(), 0, 1, 5_000);
        let refs = [&seg];
        let retention = krabka_units::prelude::millis(1_000);
        let config = LogConfig {
            retention: Some(retention),
            ..LogConfig::default()
        };

        // now - retention == the segment's max timestamp: on the cutoff.
        let on_cutoff = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(6);
        check!(
            time_based_evict(&refs, &config, on_cutoff).is_empty(),
            "a segment on the cutoff is still within retention"
        );

        // One millisecond later it is outside.
        let past = SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(6_001);
        check!(time_based_evict(&refs, &config, past) == vec![Offset(0)]);
    }

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
}
