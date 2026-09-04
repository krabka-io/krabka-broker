//! Sparse offset index. Each entry is 8 bytes: `relative_offset` as u32 BE
//! and position as u32 BE. Entries increase monotonically.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use tracing::instrument;
use zerocopy::{
    BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned, byteorder::U32,
};

use crate::{
    error::LogError,
    io::{IoTarget, LogIo},
};

/// 8 bytes per entry.
pub const OFFSET_ENTRY_SIZE: usize = 8;

/// On-disk byte layout of one offset-index entry.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct OffsetEntryRaw {
    relative_offset: U32<BigEndian>,
    position: U32<BigEndian>,
}

const _: [(); OFFSET_ENTRY_SIZE] = [(); std::mem::size_of::<OffsetEntryRaw>()];

#[derive(Debug)]
pub struct OffsetIndex {
    file: File,
    io: std::sync::Arc<dyn LogIo>,
    /// Entries currently in the file. The constructor loads them into memory
    /// lazily.
    entries: Vec<(u32, u32)>,
}

impl OffsetIndex {
    /// Open or create an offset-index file. If the file exists, this method
    /// loads its entries into memory. If it does not exist, this method
    /// creates an empty file.
    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path.display(), entries = tracing::field::Empty),
        err,
    )]
    pub fn open(path: &Path) -> Result<Self, LogError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let truncated_len = (buf.len() / OFFSET_ENTRY_SIZE) * OFFSET_ENTRY_SIZE;
        let raws = <[OffsetEntryRaw]>::ref_from_bytes(&buf[..truncated_len])
            .expect("length is a multiple of OFFSET_ENTRY_SIZE and OffsetEntryRaw is Unaligned");
        // Byte positions strictly increase across real entries. A Kafka
        // index file is preallocated to `segment.index.bytes` and only
        // truncated on clean roll/shutdown, so an unclean copy carries
        // trailing zero-padding that decodes to `(0, 0)`. Stop at the
        // first non-increasing position to keep `lookup`'s binary search
        // operating over a monotonic slice.
        let mut entries: Vec<(u32, u32)> = Vec::with_capacity(raws.len());
        for r in raws {
            let (rel, pos) = (r.relative_offset.get(), r.position.get());
            if let Some(&(_, prev_pos)) = entries.last()
                && pos <= prev_pos
            {
                break;
            }
            entries.push((rel, pos));
        }
        tracing::Span::current().record("entries", entries.len());
        Ok(Self {
            file,
            io: crate::io::file_io(),
            entries,
        })
    }

    /// Append a new entry. The caller must keep the entries monotonic.
    pub fn append(&mut self, relative_offset: u32, position: u32) -> Result<(), LogError> {
        let raw = OffsetEntryRaw {
            relative_offset: U32::new(relative_offset),
            position: U32::new(position),
        };
        self.file.seek(SeekFrom::End(0))?;
        crate::io::write_all(&*self.io, IoTarget::OffsetIndex, &self.file, raw.as_bytes())?;
        self.entries.push((relative_offset, position));
        Ok(())
    }

    /// Find the byte position where a read for a given relative offset must
    /// start. This method returns the position of the largest entry with
    /// `relative_offset <= target`, or 0 when there are no entries.
    #[must_use]
    pub fn lookup(&self, target: u32) -> u32 {
        krabka_verified::offset_index_lookup(&self.entries, target)
    }

    /// Truncate the entries, and the on-disk file, so that no entry with
    /// `position >= max_position_exclusive` remains.
    #[instrument(level = "debug", skip(self), fields(entries = tracing::field::Empty), err)]
    pub fn truncate_by_position(&mut self, max_position_exclusive: u32) -> Result<(), LogError> {
        let new_len = self
            .entries
            .iter()
            .take_while(|(_, pos)| *pos < max_position_exclusive)
            .count();
        self.entries.truncate(new_len);
        let new_file_len = (new_len * OFFSET_ENTRY_SIZE) as u64;
        self.file.set_len(new_file_len)?;
        self.file.seek(SeekFrom::End(0))?;
        tracing::Span::current().record("entries", new_len);
        Ok(())
    }

    /// Byte position of the first entry whose `relative_offset >= target`, or
    /// `None` when every entry is below `target`. Every batch that covers an
    /// offset `< target` lies strictly below this position, so the position
    /// bounds a scan from the start that must stop at `target`.
    #[must_use]
    pub fn position_at_or_after(&self, target: u32) -> Option<u32> {
        krabka_verified::offset_index_position_at_or_after(&self.entries, target)
    }

    #[must_use]
    pub fn last_entry(&self) -> Option<(u32, u32)> {
        self.entries.last().copied()
    }

    #[must_use]
    #[cfg(test)]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    #[instrument(level = "debug", skip_all, err)]
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.io
            .sync_file(IoTarget::OffsetIndex, &self.file)
            .map_err(LogError::Io)
    }

    /// Route this index's writes and syncs through `io`.
    pub(crate) fn set_io(&mut self, io: std::sync::Arc<dyn LogIo>) {
        self.io = io;
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tempfile::tempdir;

    use super::*;

    /// Truncation drops every entry at or past the bound and shortens the file
    /// to match.
    ///
    /// The bound is exclusive, so an entry sitting exactly on it goes. And the
    /// file has to end up exactly `kept * ENTRY_SIZE` bytes long: a stale tail
    /// is read back as entries on the next open, which is how a torn index
    /// resurrects offsets the log no longer has.
    #[test]
    fn offset_index_truncation_drops_entries_at_the_bound_and_shortens_the_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        for i in 0..5u32 {
            idx.append(i * 10, i * 100).unwrap();
        }
        check!(std::fs::metadata(&path).unwrap().len() == (5 * OFFSET_ENTRY_SIZE) as u64);

        // Positions are 0, 100, 200, 300, 400; the bound is exclusive, so the
        // entry at 200 goes with everything past it.
        idx.truncate_by_position(200).unwrap();
        check!(
            std::fs::metadata(&path).unwrap().len() == (2 * OFFSET_ENTRY_SIZE) as u64,
            "file should hold exactly the two surviving entries"
        );
        // Reopening reads the file back: the dropped entries must not return.
        let reopened = OffsetIndex::open(&path).unwrap();
        check!(reopened.lookup(9999) == 100, "the last surviving position");
    }

    #[test]
    fn append_and_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(100, 4096).unwrap();
        idx.append(200, 8192).unwrap();
        for (name, offset, want) in [
            ("floor first", 50, 0),
            ("exact middle", 100, 4096),
            ("floor middle", 150, 4096),
            ("exact last", 200, 8192),
            ("past last", 9999, 8192),
        ] {
            check!(idx.lookup(offset) == want, "case {name}: offset={offset}");
        }
    }

    #[test]
    fn empty_index_returns_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let idx = OffsetIndex::open(&path).unwrap();
        for (_name, offset) in [("zero", 0), ("positive", 1000)] {
            assert2::assert!(idx.lookup(offset) == 0);
        }
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        {
            let mut idx = OffsetIndex::open(&path).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(100, 4096).unwrap();
            idx.flush().unwrap();
        }
        let idx = OffsetIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(idx.lookup(100) == 4096);
    }

    #[test]
    fn ignores_trailing_zero_padding() {
        // Kafka preallocates `.index` to `segment.index.bytes` and only
        // truncates on clean shutdown; an unclean copy carries trailing
        // zero entries. Loading must stop at the real data so the binary
        // search stays monotonic.
        use std::io::Write;
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        {
            let mut idx = OffsetIndex::open(&path).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(100, 4096).unwrap();
            idx.flush().unwrap();
        }
        // Append two zero-filled entries (preallocation padding).
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0u8; OFFSET_ENTRY_SIZE * 2]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        let idx = OffsetIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(idx.last_entry() == Some((100, 4096)));
        assert2::assert!(idx.lookup(150) == 4096);
    }

    #[test]
    fn position_at_or_after_finds_ceiling() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(100, 4096).unwrap();
        idx.append(200, 8192).unwrap();
        for (name, offset, want) in [
            ("exact", 100, Some(4096)),
            ("ceiling", 150, Some(8192)),
            ("first", 0, Some(0)),
            ("past last", 201, None),
        ] {
            check!(
                idx.position_at_or_after(offset) == want,
                "case {name}: offset={offset}"
            );
        }
    }

    #[test]
    fn truncate_by_position() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(100, 4096).unwrap();
        idx.append(200, 8192).unwrap();
        idx.truncate_by_position(8192).unwrap();
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(idx.last_entry() == Some((100, 4096)));
    }
}
