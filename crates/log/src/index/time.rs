//! Sparse time index. Each entry is 12 bytes: a timestamp as i64 BE and a
//! `relative_offset` as u32 BE. The offset column increases monotonically.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use tracing::instrument;
use zerocopy::{
    BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::{I64, U32},
};

use crate::error::LogError;

/// 12 bytes per entry: timestamp as i64 BE and `relative_offset` as u32 BE.
pub const TIME_ENTRY_SIZE: usize = 12;

/// On-disk byte layout of one time-index entry.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TimeEntryRaw {
    timestamp: I64<BigEndian>,
    relative_offset: U32<BigEndian>,
}

const _: [(); TIME_ENTRY_SIZE] = [(); std::mem::size_of::<TimeEntryRaw>()];

#[derive(Debug)]
pub struct TimeIndex {
    file: File,
    entries: Vec<(i64, u32)>,
}

impl TimeIndex {
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
        let truncated_len = (buf.len() / TIME_ENTRY_SIZE) * TIME_ENTRY_SIZE;
        let raws = <[TimeEntryRaw]>::ref_from_bytes(&buf[..truncated_len])
            .expect("length is a multiple of TIME_ENTRY_SIZE and TimeEntryRaw is Unaligned");
        // Relative offsets strictly increase across real entries; trailing
        // `(0, 0)` padding from a preallocated Kafka index decodes as a
        // non-increasing offset. Stop there. (Timestamps may repeat when
        // `max_timestamp` is unchanged between index points, so the offset
        // column — not the timestamp — is the monotonic discriminator.)
        let mut entries: Vec<(i64, u32)> = Vec::with_capacity(raws.len());
        for r in raws {
            let (ts, rel) = (r.timestamp.get(), r.relative_offset.get());
            if let Some(&(_, prev_rel)) = entries.last()
                && rel <= prev_rel
            {
                break;
            }
            entries.push((ts, rel));
        }
        tracing::Span::current().record("entries", entries.len());
        Ok(Self { file, entries })
    }

    /// Append an entry. The caller must keep the entries monotonic.
    pub fn append(&mut self, timestamp: i64, relative_offset: u32) -> Result<(), LogError> {
        let raw = TimeEntryRaw {
            timestamp: I64::new(timestamp),
            relative_offset: U32::new(relative_offset),
        };
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(raw.as_bytes())?;
        self.entries.push((timestamp, relative_offset));
        Ok(())
    }

    /// Find the relative offset at or after the given timestamp. This method
    /// returns the relative offset of the largest entry with
    /// `timestamp <= target`, or 0 when there are no entries.
    #[must_use]
    pub fn lookup(&self, target_timestamp: i64) -> u32 {
        match self
            .entries
            .binary_search_by_key(&target_timestamp, |&(ts, _)| ts)
        {
            Ok(i) => self.entries[i].1,
            Err(0) => 0,
            Err(i) => self.entries[i - 1].1,
        }
    }

    #[instrument(level = "debug", skip(self), fields(entries = tracing::field::Empty), err)]
    pub fn truncate_by_relative_offset(&mut self, max_rel_exclusive: u32) -> Result<(), LogError> {
        let new_len = self
            .entries
            .iter()
            .take_while(|(_, rel)| *rel < max_rel_exclusive)
            .count();
        self.entries.truncate(new_len);
        self.file.set_len((new_len * TIME_ENTRY_SIZE) as u64)?;
        self.file.seek(SeekFrom::End(0))?;
        tracing::Span::current().record("entries", new_len);
        Ok(())
    }

    /// Newest `(timestamp, relative_offset)` entry, or `None` when the index
    /// holds none.
    ///
    /// The entry's timestamp is the running maximum as of the batch it
    /// indexes, so it is the floor a reopened segment restores its
    /// `max_timestamp` from.
    #[must_use]
    pub fn last_entry(&self) -> Option<(i64, u32)> {
        self.entries.last().copied()
    }

    #[must_use]
    #[cfg(test)]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    #[instrument(level = "debug", skip_all, err)]
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.file.sync_data().map_err(LogError::Io)
    }
}

#[cfg(test)]
mod time_tests {
    use assert2::check;
    use tempfile::tempdir;

    use super::*;

    /// The time index truncates on relative offset, with the same exclusive
    /// bound and the same file-length obligation.
    #[test]
    fn time_index_truncation_drops_entries_at_the_bound_and_shortens_the_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.timeindex");
        let mut idx = TimeIndex::open(&path).unwrap();
        for i in 0..5u32 {
            idx.append(1_000 + i64::from(i), i * 10).unwrap();
        }
        check!(std::fs::metadata(&path).unwrap().len() == (5 * TIME_ENTRY_SIZE) as u64);

        // Relative offsets are 0, 10, 20, 30, 40. Exclusive at 20: two survive.
        idx.truncate_by_relative_offset(20).unwrap();
        check!(
            std::fs::metadata(&path).unwrap().len() == (2 * TIME_ENTRY_SIZE) as u64,
            "file should hold exactly the two surviving entries"
        );

        // Truncating to zero clears it; truncating past the end keeps everything.
        let mut idx = TimeIndex::open(&path).unwrap();
        idx.truncate_by_relative_offset(9_999).unwrap();
        check!(
            std::fs::metadata(&path).unwrap().len() == (2 * TIME_ENTRY_SIZE) as u64,
            "a bound past the end drops nothing"
        );
        idx.truncate_by_relative_offset(0).unwrap();
        check!(
            std::fs::metadata(&path).unwrap().len() == 0,
            "a bound of zero drops everything"
        );
    }

    #[test]
    fn append_and_lookup_time() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.timeindex");
        let mut idx = TimeIndex::open(&path).unwrap();
        idx.append(1_000_000, 0).unwrap();
        idx.append(2_000_000, 100).unwrap();
        idx.append(3_000_000, 200).unwrap();
        for (name, ts, want) in [
            ("before first", 0, 0),
            ("floor first", 1_500_000, 0),
            ("exact middle", 2_000_000, 100),
            ("floor middle", 2_500_000, 100),
            ("past last", 5_000_000, 200),
        ] {
            check!(idx.lookup(ts) == want, "case {name}: ts={ts}");
        }
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.timeindex");
        {
            let mut idx = TimeIndex::open(&path).unwrap();
            idx.append(1, 0).unwrap();
            idx.append(2, 50).unwrap();
            idx.flush().unwrap();
        }
        let idx = TimeIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
    }

    #[test]
    fn ignores_trailing_zero_padding() {
        use std::io::Write;
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.timeindex");
        {
            let mut idx = TimeIndex::open(&path).unwrap();
            idx.append(1_000, 0).unwrap();
            idx.append(2_000, 100).unwrap();
            idx.flush().unwrap();
        }
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0u8; TIME_ENTRY_SIZE * 2]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        let idx = TimeIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(idx.last_entry() == Some((2_000, 100)));
        assert2::assert!(idx.lookup(2_500) == 100);
    }
}
