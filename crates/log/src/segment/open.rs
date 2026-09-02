//! Constructors for a segment and the tail recovery that an active segment
//! needs when it is reopened.
//!
//! These are the three ways a [`Segment`] comes into existence -- a fresh
//! create, a no-scan open, and an active open that walks the trailing bytes --
//! together with the walk itself.

use std::{fs::OpenOptions, path::Path, sync::Arc};

use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;

use super::{Segment, io::seek_to_log_size};
use crate::{
    error::LogError,
    index::{OffsetIndex, TimeIndex},
    io::FileIo,
    name,
};

struct TailScan {
    valid_end: u64,
    last_offset: Offset,
    max_timestamp: i64,
    index_entries: Vec<(u32, u32, i64)>,
}

impl Segment {
    /// Create a fresh active segment at the given base offset. This fails if
    /// the `.log` file already exists.
    #[instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn create(dir: &Path, base_offset: Offset) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset.0);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&log_path)?;
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset.0))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset.0))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file: Arc::new(log_file),
            io: Arc::new(FileIo),
            log_size: 0,
            offset_index,
            time_index,
            sealed: false,
            max_timestamp: i64::MIN,
            last_offset: base_offset - 1,
        })
    }

    /// Open as the active segment.
    ///
    /// When `validate` is true, this method scans from byte zero to EOF. It
    /// truncates a partial or invalid trailing batch, rebuilds both sparse
    /// indexes, and updates `last_offset` and `max_timestamp` from the same
    /// maximal valid prefix.
    #[instrument(
        level = "info",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0, validate),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open_active(dir: &Path, base_offset: Offset, validate: bool) -> Result<Self, LogError> {
        Self::open_active_with_index_interval(
            dir,
            base_offset,
            validate,
            crate::config::DEFAULT_INDEX_INTERVAL,
        )
    }

    pub(crate) fn open_active_with_index_interval(
        dir: &Path,
        base_offset: Offset,
        validate: bool,
        index_interval: ByteSize,
    ) -> Result<Self, LogError> {
        let mut seg = Self::open(dir, base_offset)?;
        if validate {
            seg.recover_active_tail(index_interval)?;
        }
        Ok(seg)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            base_offset = self.base_offset.0,
            log_size = self.log_size,
            recovered_last_offset = tracing::field::Empty,
        ),
        err,
    )]
    fn recover_active_tail(&mut self, index_interval: ByteSize) -> Result<(), LogError> {
        let recovered = self.scan_valid_tail(index_interval)?;
        u32::try_from(recovered.valid_end)
            .map_err(|_| LogError::Corrupt("recovered segment position exceeds u32".into()))?;
        krabka_verified::local_recovery_index_frontier(self.base_offset.0, recovered.last_offset.0)
            .ok_or_else(|| {
                LogError::Corrupt("recovered segment offset exceeds index range".into())
            })?;
        if recovered.valid_end < self.log_size {
            self.log_file.set_len(recovered.valid_end)?;
        }
        self.log_size = recovered.valid_end;
        seek_to_log_size(&self.log_file, self.log_size)?;
        self.offset_index.truncate_by_position(0)?;
        self.time_index.truncate_by_relative_offset(0)?;
        for (relative, position, max_timestamp) in recovered.index_entries {
            self.offset_index.append(relative, position)?;
            self.time_index.append(max_timestamp, relative)?;
        }
        self.last_offset = recovered.last_offset;
        self.max_timestamp = recovered.max_timestamp;
        tracing::Span::current().record("recovered_last_offset", self.last_offset.0);
        Ok(())
    }

    fn scan_valid_tail(&self, index_interval: ByteSize) -> Result<TailScan, LogError> {
        let mut buf = Vec::new();
        let to_read = usize::try_from(self.log_size).unwrap_or(usize::MAX);
        self.read_log_range(0, &mut buf, to_read)?;

        let mut cur: &[u8] = &buf;
        let mut valid_end = 0;
        let mut next_offset = self.base_offset.0;
        let mut last_offset = self
            .base_offset
            .0
            .checked_sub(1)
            .map(Offset)
            .ok_or_else(|| LogError::Corrupt("recovery offset underflow".into()))?;
        let mut max_timestamp = i64::MIN;
        let mut index_entries: Vec<(u32, u32, i64)> = Vec::new();
        while !cur.is_empty() {
            let batch_position = valid_end;
            let before = cur.len();
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                break;
            };
            let encoded_len = u64::try_from(before - cur.len())
                .map_err(|_| LogError::Corrupt("decoded batch length exceeds u64".into()))?;
            let Some(step) = krabka_verified::local_recovery_batch_step(
                valid_end,
                self.log_size,
                next_offset,
                batch.base_offset,
                batch.last_offset_delta,
                encoded_len,
            ) else {
                break;
            };
            valid_end = step.valid_end;
            last_offset = Offset(step.last_offset);
            next_offset = step.next_offset;
            max_timestamp = max_timestamp.max(batch.max_timestamp);
            let should_index = match index_entries.last() {
                None => true,
                Some((_, previous_position, _)) => {
                    batch_position.saturating_sub(u64::from(*previous_position))
                        >= index_interval.bytes_u64()
                }
            };
            if should_index {
                let relative = krabka_verified::truncation_relative_offset(
                    self.base_offset.0,
                    batch.base_offset,
                )
                .ok_or_else(|| {
                    LogError::Corrupt("recovered batch offset exceeds index range".into())
                })?;
                let position = u32::try_from(batch_position).map_err(|_| {
                    LogError::Corrupt("recovered batch position exceeds index range".into())
                })?;
                index_entries.push((relative, position, max_timestamp));
            }
        }
        Ok(TailScan {
            valid_end,
            last_offset,
            max_timestamp,
            index_entries,
        })
    }

    /// Open an existing segment for reading. This is lightweight and does no
    /// full scan.
    ///
    /// The log and index files must already exist on disk. The segment starts
    /// with `last_offset = base_offset - 1` and `max_timestamp = i64::MIN`
    /// until tail recovery through [`Segment::open_active`] fills them in.
    #[instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open(dir: &Path, base_offset: Offset) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset.0);
        let log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let log_size = log_file.metadata()?.len();
        seek_to_log_size(&log_file, log_size)?;
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset.0))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset.0))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file: Arc::new(log_file),
            io: Arc::new(FileIo),
            log_size,
            offset_index,
            time_index,
            sealed: false,
            max_timestamp: i64::MIN,
            last_offset: base_offset - 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::{DENSE_INDEX, NO_LIMIT, sample_batch};

    #[test]
    fn append_after_open_active_writes_at_eof() {
        let dir = tempdir().unwrap();
        {
            let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
            seg.append(&sample_batch(0, 1, 100), DENSE_INDEX).unwrap();
            seg.append(&sample_batch(1, 1, 200), DENSE_INDEX).unwrap();
        }

        let mut seg = Segment::open_active(dir.path(), Offset(0), true).unwrap();
        let position = seg.append(&sample_batch(2, 1, 300), DENSE_INDEX).unwrap();

        let read = seg.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(position > 0);
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(
            read == vec![
                sample_batch(0, 1, 100),
                sample_batch(1, 1, 200),
                sample_batch(2, 1, 300),
            ]
        );
    }

    /// Tail recovery must physically truncate a partial or garbage tail and
    /// rebuild both indexes from exactly the batches in the retained prefix.
    #[test]
    fn recover_active_tail_truncates_trailing_garbage() {
        let dir = tempdir().unwrap();
        let valid_size = {
            let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
            seg.append(&sample_batch(0, 3, 100), DENSE_INDEX).unwrap();
            seg.append(&sample_batch(3, 2, 200), DENSE_INDEX).unwrap();
            seg.flush().unwrap();
            let valid_size = seg.log_size;
            let stale_position = u32::try_from(valid_size).unwrap();
            seg.offset_index.append(999, stale_position).unwrap();
            seg.time_index.append(i64::MAX, 999).unwrap();
            valid_size
        };

        // Append 16 bytes of garbage (an undecodable partial batch tail).
        let log_path = name::log_path(dir.path(), 0);
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(&[0xCD; 16]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        // Reopen with validation: the tail scan must clip the garbage.
        let seg =
            Segment::open_active_with_index_interval(dir.path(), Offset(0), true, DENSE_INDEX)
                .unwrap();
        assert2::assert!(seg.last_offset() == Offset(4));
        assert2::assert!(seg.log_size == valid_size);
        assert2::assert!(seg.offset_index.entry_count() == 2);
        assert2::assert!(seg.time_index.entry_count() == 2);
        assert2::assert!(u64::from(seg.offset_index.lookup(999)) < valid_size);
        assert2::assert!(seg.time_index.last_entry().unwrap().1 < 999);
        drop(seg);

        // Recovery is idempotent once the file and both indexes share the
        // proved frontiers.
        let retry =
            Segment::open_active_with_index_interval(dir.path(), Offset(0), true, DENSE_INDEX)
                .unwrap();
        assert2::assert!(retry.last_offset() == Offset(4));
        assert2::assert!(retry.log_size == valid_size);
        assert2::assert!(retry.offset_index.entry_count() == 2);
        assert2::assert!(retry.time_index.entry_count() == 2);
    }
}
