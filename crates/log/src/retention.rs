//! Retention that `Log::tick` applies. These are free functions, so tests can
//! check the policy apart from `Log`'s mutable state.

use std::{
    path::Path,
    time::{Duration, SystemTime},
};

use crabka_ids::Offset;
use crabka_units::prelude::{ByteSize, ByteSizeExt as _, TimeExt as _};
use tracing::instrument;

use crate::{config::LogConfig, error::LogError, name, segment::Segment};

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

#[instrument(
    level = "debug",
    skip_all,
    fields(
        sealed = sealed.len(),
        active_size = active_size.bytes_u64(),
        evicted = tracing::field::Empty,
    ),
)]
pub fn size_based_evict(
    sealed: &[&Segment],
    active_size: ByteSize,
    config: &LogConfig,
) -> Vec<Offset> {
    let Some(budget) = config.retention_size else {
        return Vec::new();
    };
    let total: ByteSize = sealed.iter().fold(active_size, |acc, s| acc + s.size());
    if total <= budget {
        return Vec::new();
    }
    let mut deletable = total - budget;
    let mut out = Vec::new();
    for seg in sealed {
        if deletable <= ByteSize::ZERO {
            break;
        }
        out.push(seg.base_offset());
        deletable -= seg.size();
    }
    tracing::Span::current().record("evicted", out.len());
    out
}

#[instrument(level = "debug", skip_all, fields(dir = %dir.display(), base_offset = base_offset.0), err)]
pub fn delete_segment_files(dir: &Path, base_offset: Offset) -> Result<(), LogError> {
    std::fs::remove_file(name::log_path(dir, base_offset.0))?;
    std::fs::remove_file(name::index_path(dir, base_offset.0))?;
    std::fs::remove_file(name::timeindex_path(dir, base_offset.0))?;
    remove_optional(name::txnindex_path(dir, base_offset.0))?;
    remove_optional(name::stampindex_path(dir, base_offset.0))?;
    Ok(())
}

fn remove_optional(path: std::path::PathBuf) -> Result<(), LogError> {
    match std::fs::remove_file(path) {
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

    /// A sealed segment carrying `n` two-record batches, timestamps from
    /// `ts_base`.
    fn sealed_segment(dir: &std::path::Path, base: i64, batches: i64, ts_base: i64) -> Segment {
        use bytes::Bytes;
        use crabka_protocol::records::{Record, RecordBatch};

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

    /// Only as many of the oldest segments are evicted as the overrun needs.
    ///
    /// Four things decide that and each is invisible on its own: the running
    /// total, the `total <= budget` early return, the `deletable <= 0` stop,
    /// and subtracting each evicted segment from the overrun. Getting any of
    /// them wrong evicts everything or nothing, so the case that pins them all
    /// down is a budget that admits some segments and not others.
    #[test]
    fn size_eviction_takes_only_as_many_segments_as_the_overrun_needs() {
        let dir = tempdir().unwrap();
        let segments: Vec<Segment> = (0..4)
            .map(|i| sealed_segment(dir.path(), i * 100, 3, 1_000 + i))
            .collect();
        let refs: Vec<&Segment> = segments.iter().collect();

        let each = segments[0].size();
        let total: ByteSize = refs.iter().fold(ByteSize::ZERO, |acc, s| acc + s.size());
        check!(each > ByteSize::ZERO, "a segment should occupy bytes");

        // A budget two segments under the total: two must go, and only two.
        let config = LogConfig {
            retention_size: Some(total - each - each),
            ..LogConfig::default()
        };
        let evicted = size_based_evict(&refs, ByteSize::ZERO, &config);
        check!(
            evicted == vec![Offset(0), Offset(100)],
            "expected the two oldest, got {evicted:?}"
        );

        // A budget that already fits evicts nothing.
        let roomy = LogConfig {
            retention_size: Some(total),
            ..LogConfig::default()
        };
        check!(size_based_evict(&refs, ByteSize::ZERO, &roomy).is_empty());
    }

    /// The time cutoff is exclusive: a segment whose newest record sits
    /// exactly on it is still inside the window and is kept.
    #[test]
    fn time_eviction_keeps_a_segment_sitting_exactly_on_the_cutoff() {
        let dir = tempdir().unwrap();
        let seg = sealed_segment(dir.path(), 0, 1, 5_000);
        let refs = [&seg];
        let retention = crabka_units::prelude::millis(1_000);
        let config = LogConfig {
            retention: Some(retention),
            ..LogConfig::default()
        };

        // now - retention == the segment's max timestamp: on the cutoff.
        let on_cutoff = SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(6_000);
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

        delete_segment_files(dir.path(), base).unwrap();

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

        delete_segment_files(dir.path(), base).unwrap();
    }

    #[test]
    fn remove_optional_propagates_non_missing_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("not-a-file");
        std::fs::create_dir(&path).unwrap();

        assert2::assert!(let Err(LogError::Io(_)) = remove_optional(path));
    }
}
