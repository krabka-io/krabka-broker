//! Constructors for a segment and the tail recovery that an active segment
//! needs when it is reopened.
//!
//! These are the three ways a [`Segment`] comes into existence -- a fresh
//! create, a no-scan open, and an active open that walks the trailing bytes --
//! together with the walk itself.

use std::{fs::OpenOptions, path::Path, sync::Arc};

use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use tracing::instrument;

use super::{Segment, io::seek_to_log_size};
use crate::{
    error::LogError,
    index::{OffsetIndex, TimeIndex},
    name,
};

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
    /// When `validate` is true, this method scans from the last-indexed
    /// position to EOF. It truncates a partial trailing batch, and also a
    /// batch that fails to decode. Cleanly decoded batches update
    /// `last_offset` and `max_timestamp`.
    #[instrument(
        level = "info",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0, validate),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open_active(dir: &Path, base_offset: Offset, validate: bool) -> Result<Self, LogError> {
        let mut seg = Self::open(dir, base_offset)?;
        if validate {
            seg.recover_active_tail()?;
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
    fn recover_active_tail(&mut self) -> Result<(), LogError> {
        let scan_start = self
            .offset_index
            .last_entry()
            .map_or(0u64, |(_, pos)| u64::from(pos));
        if scan_start >= self.log_size {
            return Ok(());
        }

        let mut buf = Vec::new();
        let to_read = usize::try_from(self.log_size - scan_start).unwrap_or(usize::MAX);
        self.read_log_range(scan_start, &mut buf, to_read)?;

        let mut cur: &[u8] = &buf;
        let mut consumed: u64 = 0;
        let mut last_offset = self.last_offset;
        let mut max_ts = self.max_timestamp;
        while !cur.is_empty() {
            let before = cur.len();
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                break;
            };
            consumed += (before - cur.len()) as u64;
            last_offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
            if batch.max_timestamp > max_ts {
                max_ts = batch.max_timestamp;
            }
        }

        let valid_end = scan_start + consumed;
        if valid_end < self.log_size {
            self.log_file.set_len(valid_end)?;
            self.log_size = valid_end;
        }
        seek_to_log_size(&self.log_file, self.log_size)?;
        self.last_offset = last_offset;
        self.max_timestamp = max_ts;
        tracing::Span::current().record("recovered_last_offset", last_offset.0);
        Ok(())
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

    /// Tail recovery must PHYSICALLY truncate a partial or garbage trailing
    /// tail. It finds the valid end with `consumed += before - cur.len()`,
    /// which is the exact number of bytes each valid batch decode advanced. A
    /// mutation of `-` to `+` inflates `consumed` and pushes `valid_end` past
    /// `log_size`, so the garbage is never truncated and the file keeps its
    /// trailing bytes.
    #[test]
    fn recover_active_tail_truncates_trailing_garbage() {
        let dir = tempdir().unwrap();
        let valid_size = {
            let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
            seg.append(&sample_batch(0, 3, 100), DENSE_INDEX).unwrap();
            seg.append(&sample_batch(3, 2, 200), DENSE_INDEX).unwrap();
            seg.flush().unwrap();
            seg.size()
        };

        // Append 16 bytes of garbage (an undecodable partial batch tail).
        let log_path = name::log_path(dir.path(), 0);
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(&[0xCD; 16]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        // Reopen with validation: the tail scan must clip the garbage.
        let seg = Segment::open_active(dir.path(), Offset(0), true).unwrap();
        assert2::assert!(seg.last_offset() == Offset(4));
        assert2::assert!(seg.size() == valid_size);
    }
}
