//! Per-partition `.stampindex` sidecar. One fixed-width record per
//! stamped offset range in the segment:
//!
//!   `base_offset`: i64 (big-endian)
//!   `last_offset`: i64 (big-endian)
//!   `stamp`:       u64 (big-endian)
//!
//! The stamp is an additional internal coordinate, a packed
//! `TimestampSource` reading, stored beside the wire-exact `.log`. Nothing
//! ever touches the `.log` bytes. The stampindex is internal metadata that is
//! retained and truncated with its segment. It never leaves the broker on any
//! client-facing API. This mirrors the `.txnindex` sidecar pattern.

use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

use krabka_ids::Offset;
use tracing::instrument;
use zerocopy::{
    BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::{I64, U64},
};

use crate::{
    error::LogError,
    index::open_index,
    io::{IoTarget, LogIo},
};

const ENTRY_BYTES: usize = 24;

/// One stamped offset range. The inclusive offsets
/// `[base_offset, last_offset]` all carry `stamp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StampEntry {
    pub base_offset: Offset,
    pub last_offset: Offset,
    pub stamp: u64,
}

/// On-disk byte layout of one `StampEntry`. `zerocopy` reinterprets it in
/// place from the file bytes.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct StampEntryRaw {
    base_offset: I64<BigEndian>,
    last_offset: I64<BigEndian>,
    stamp: U64<BigEndian>,
}

impl StampEntryRaw {
    fn new(entry: StampEntry) -> Self {
        Self {
            base_offset: I64::new(entry.base_offset.0),
            last_offset: I64::new(entry.last_offset.0),
            stamp: U64::new(entry.stamp),
        }
    }
}

const _: [(); ENTRY_BYTES] = [(); std::mem::size_of::<StampEntryRaw>()];

#[derive(Debug)]
pub struct StampIndex {
    path: PathBuf,
    io: Arc<dyn LogIo>,
    entries: Vec<StampEntry>,
}

impl StampIndex {
    open_index! {
        /// Open or recover a `.stampindex` file at the given path. This method
        /// reads the entire file into memory at startup. An empty file or a
        /// missing file is acceptable and means zero stamped ranges.
        /// # Errors
        /// Returns an error when log I/O fails or the file's length is not a
        /// whole number of fixed-width entries.
        /// # Panics
        /// Panics if the in-place reinterpretation of a length-validated,
        /// `Unaligned` byte buffer fails. That invariant cannot be false.
        pub fn open(path: PathBuf) -> Result<Self, LogError> {
            let entries = crate::index::read_sidecar::<StampEntryRaw, _>(
                &path,
                "stampindex",
                "length is a multiple of ENTRY_BYTES and StampEntryRaw is Unaligned",
                |raws| {
                    let mut entries = Vec::with_capacity(raws.len());
                    for raw in raws {
                        entries.push(StampEntry {
                            base_offset: Offset(raw.base_offset.get()),
                            last_offset: Offset(raw.last_offset.get()),
                            stamp: raw.stamp.get(),
                        });
                    }
                    entries
                        .sort_unstable_by_key(|entry| (entry.base_offset.0, entry.last_offset.0, entry.stamp));
                    // A write followed by an uncertain sync can be retried and
                    // leave an exact duplicate on disk. Canonicalize that retry,
                    // but reject a duplicate range with a different stamp below.
                    entries.dedup();
                    if !Self::entries_valid(&entries) {
                        return Err(LogError::Corrupt(format!(
                            "stampindex {} contains inverted or overlapping ranges",
                            path.display()
                        )));
                    }
                    Ok(entries)
                },
            )?;
            tracing::Span::current().record("entries", entries.len());
            Ok(Self {
                path,
                io: crate::io::file_io(),
                entries,
            })
        }
    }

    /// Append one stamped-range entry.
    ///
    /// Entries need not arrive in offset order. Transactional ranges are
    /// added when their commit marker lands, and two interleaved transactions
    /// can commit in either order. The in-memory index is kept in canonical
    /// offset order, and ranges themselves must not overlap.
    #[instrument(
        level = "debug",
        skip(self),
        fields(stamp = entry.stamp),
        err,
    )]
    /// # Errors
    /// Returns an error when appending to or syncing the file fails.
    pub fn append(&mut self, entry: StampEntry) -> Result<(), LogError> {
        let (bases, lasts) = Self::coordinates(&self.entries);
        if let Some(position) = Self::exact_range_index(&bases, &lasts, entry) {
            if self.entries[position] == entry {
                return Ok(());
            }
            return Err(LogError::Corrupt(format!(
                "stamp range {}..={} conflicts with its existing stamp in {}",
                entry.base_offset,
                entry.last_offset,
                self.path.display()
            )));
        }
        let Some(position) = krabka_verified::stamp_range_insertion_index(
            &bases,
            &lasts,
            entry.base_offset.0,
            entry.last_offset.0,
        ) else {
            if entry.last_offset < entry.base_offset {
                return Err(LogError::InvalidArgument(format!(
                    "stamp range {}..={} is inverted",
                    entry.base_offset, entry.last_offset
                )));
            }
            return Err(LogError::Corrupt(format!(
                "stamp range {}..={} overlaps an existing range in {}",
                entry.base_offset,
                entry.last_offset,
                self.path.display()
            )));
        };
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(LogError::Io)?;
        let raw = StampEntryRaw::new(entry);
        crate::io::write_all(&*self.io, IoTarget::StampIndex, &f, raw.as_bytes())
            .map_err(LogError::Io)?;
        self.io
            .sync_file(IoTarget::StampIndex, &f)
            .map_err(LogError::Io)?;
        self.entries.insert(position, entry);
        Ok(())
    }

    fn exact_range_index(bases: &[i64], lasts: &[i64], entry: StampEntry) -> Option<usize> {
        krabka_verified::exact_stamp_range_index(
            bases,
            lasts,
            entry.base_offset.0,
            entry.last_offset.0,
        )
    }

    /// Insert a committed transactional range or replace the same exact
    /// range. Replacement upgrades append-time entries written by older
    /// brokers to the marker-time commit stamp.
    ///
    /// # Errors
    /// Returns an error for a partial overlap or when rewriting the sidecar
    /// fails.
    pub fn upsert(&mut self, entry: StampEntry) -> Result<(), LogError> {
        let (bases, lasts) = Self::coordinates(&self.entries);
        if let Some(position) = Self::exact_range_index(&bases, &lasts, entry) {
            if self.entries[position] != entry {
                let mut entries = self.entries.clone();
                entries[position] = entry;
                self.rewrite(&entries)?;
                self.entries = entries;
            }
            return Ok(());
        }
        self.append(entry)
    }

    /// Remove entries that cover offsets at or after `offset` and rewrite the
    /// sidecar. Log truncation calls this after truncating the segment bytes.
    ///
    /// # Errors
    /// Returns an error when rewriting or syncing the sidecar fails.
    pub fn truncate_from(&mut self, offset: Offset) -> Result<(), LogError> {
        let Some(entries) =
            crate::index::changed_entries(&self.entries, |entry| entry.last_offset < offset)
        else {
            return Ok(());
        };
        self.rewrite(&entries)?;
        self.entries = entries;
        Ok(())
    }

    /// Remove exact ranges, if present. Startup uses this to hide
    /// append-time transactional entries created by older brokers while the
    /// transaction is still open.
    ///
    /// # Errors
    /// Returns an error when rewriting or syncing the sidecar fails.
    pub fn remove_ranges(&mut self, ranges: &[(Offset, Offset)]) -> Result<(), LogError> {
        let mut entries = self.entries.clone();
        for &(base, last) in ranges {
            let (bases, lasts) = Self::coordinates(&entries);
            if let Some(position) =
                krabka_verified::exact_stamp_range_index(&bases, &lasts, base.0, last.0)
            {
                entries.remove(position);
            }
        }
        if entries.len() == self.entries.len() {
            return Ok(());
        }
        self.rewrite(&entries)?;
        self.entries = entries;
        Ok(())
    }

    fn rewrite(&self, entries: &[StampEntry]) -> Result<(), LogError> {
        let file = crate::index::rewrite_sidecar_file(&self.path)?;
        for entry in entries {
            let raw = StampEntryRaw::new(*entry);
            crate::io::write_all(&*self.io, IoTarget::StampIndex, &file, raw.as_bytes())
                .map_err(LogError::Io)?;
        }
        self.io
            .sync_file(IoTarget::StampIndex, &file)
            .map_err(LogError::Io)
    }

    /// Route this sidecar's writes and syncs through `io`.
    pub(crate) fn set_io(&mut self, io: Arc<dyn LogIo>) {
        self.io = io;
    }

    #[must_use]
    pub fn entries(&self) -> &[StampEntry] {
        &self.entries
    }

    fn coordinates(entries: &[StampEntry]) -> (Vec<i64>, Vec<i64>) {
        entries
            .iter()
            .map(|entry| (entry.base_offset.0, entry.last_offset.0))
            .unzip()
    }

    fn entries_valid(entries: &[StampEntry]) -> bool {
        let (bases, lasts) = Self::coordinates(entries);
        krabka_verified::stamp_ranges_valid(&bases, &lasts)
    }

    /// The stamp of the entry whose inclusive `[base_offset, last_offset]`
    /// range contains `offset`, or `None` when no entry covers it.
    #[must_use]
    pub fn stamp_for_offset(&self, offset: Offset) -> Option<u64> {
        let (bases, lasts) = Self::coordinates(&self.entries);
        krabka_verified::covering_stamp_range_index(&bases, &lasts, offset.0)
            .map(|index| self.entries[index].stamp)
    }
}

#[cfg(test)]
mod tests {

    use tempfile::TempDir;

    use super::*;

    fn index_with(entries: &[StampEntry]) -> (TempDir, PathBuf, StampIndex) {
        let (dir, path, mut index) = crate::index::index_fixture("00.stampindex", |path| {
            StampIndex::open(path.to_path_buf())
        });
        for entry in entries {
            index.append(*entry).unwrap();
        }
        (dir, path, index)
    }

    fn write_entries(path: &std::path::Path, entries: &[StampEntry]) {
        let mut bytes = Vec::with_capacity(entries.len() * ENTRY_BYTES);
        for entry in entries {
            let raw = StampEntryRaw::new(*entry);
            bytes.extend_from_slice(raw.as_bytes());
        }
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn stamp_empty_file_yields_empty_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        let idx = StampIndex::open(path).unwrap();
        assert2::assert!(idx.entries() == &[]);
    }

    #[test]
    fn stamp_append_round_trips_through_disk() {
        let (_dir, path, _idx) = index_with(&[
            crate::test_support::stamp_entry(5, 7, 1_000),
            crate::test_support::stamp_entry(10, 12, 2_000),
        ]);

        let idx2 = StampIndex::open(path).unwrap();
        assert2::assert!(
            idx2.entries()
                == &[
                    crate::test_support::stamp_entry(5, 7, 1_000),
                    crate::test_support::stamp_entry(10, 12, 2_000),
                ]
        );
    }

    /// Only a real `NotFound` means "no index yet". Every other I/O error
    /// must surface as `LogError::Io`. Here the path is a directory, so the
    /// read fails with a kind other than `NotFound`. To swallow that error
    /// would return an empty index over a real failure.
    #[test]
    fn open_surfaces_non_notfound_io_error() {
        let dir = TempDir::new().unwrap();
        let err = StampIndex::open(dir.path().to_path_buf()).unwrap_err();
        assert2::assert!(let LogError::Io(_) = err);
    }

    #[test]
    fn stamp_corrupt_length_is_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        std::fs::write(&path, [0_u8; ENTRY_BYTES + 1]).unwrap();
        let err = StampIndex::open(path).unwrap_err();
        assert2::assert!(let LogError::Corrupt(_) = err);
    }

    #[test]
    fn open_canonicalizes_retries_and_rejects_malformed_ranges() {
        let dir = TempDir::new().unwrap();
        let first = crate::test_support::stamp_entry(0, 2, 100);
        let second = crate::test_support::stamp_entry(10, 12, 200);
        let path = dir.path().join("retries.stampindex");
        write_entries(&path, &[second, first, second]);
        assert2::assert!(StampIndex::open(path).unwrap().entries() == [first, second]);

        for (name, entries) in [
            ("inverted", vec![crate::test_support::stamp_entry(7, 6, 1)]),
            (
                "overlap",
                vec![
                    crate::test_support::stamp_entry(0, 4, 1),
                    crate::test_support::stamp_entry(4, 8, 2),
                ],
            ),
            (
                "conflicting-retry",
                vec![
                    crate::test_support::stamp_entry(0, 4, 1),
                    crate::test_support::stamp_entry(0, 4, 2),
                ],
            ),
        ] {
            let path = dir.path().join(format!("{name}.stampindex"));
            write_entries(&path, &entries);
            assert2::assert!(let LogError::Corrupt(_) = StampIndex::open(path).unwrap_err());
        }
    }

    #[test]
    fn out_of_order_append_stays_canonical_across_reopen() {
        let (_dir, path, mut idx) = index_with(&[]);
        let first = crate::test_support::stamp_entry(0, 2, 100);
        let second = crate::test_support::stamp_entry(10, 12, 200);
        idx.append(second).unwrap();
        idx.append(first).unwrap();
        assert2::assert!(idx.entries() == [first, second]);
        assert2::assert!(StampIndex::open(path).unwrap().entries() == [first, second]);
    }

    #[test]
    fn mutation_io_failures_leave_the_in_memory_index_unchanged() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        let original = crate::test_support::stamp_entry(0, 2, 100);
        let mut idx = StampIndex::open(path.clone()).unwrap();
        idx.append(original).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();

        let replacement = StampEntry {
            stamp: 200,
            ..original
        };
        assert2::assert!(let LogError::Io(_) = idx.upsert(replacement).unwrap_err());
        assert2::assert!(idx.entries() == [original]);
        assert2::assert!(
            let LogError::Io(_) = idx.remove_ranges(&[(Offset(0), Offset(2))]).unwrap_err()
        );
        assert2::assert!(idx.entries() == [original]);
        assert2::assert!(
            let LogError::Io(_) = idx
                .append(crate::test_support::stamp_entry(4, 5, 300))
                .unwrap_err()
        );
        assert2::assert!(idx.entries() == [original]);
    }

    #[test]
    fn stamp_for_offset_finds_covering_range() {
        let (_dir, _path, idx) = index_with(&[
            crate::test_support::stamp_entry(0, 4, 100),
            crate::test_support::stamp_entry(10, 14, 200),
        ]);

        // Inclusive endpoints and interior offsets resolve to their range.
        for (offset, expected) in [
            (0, Some(100)),
            (4, Some(100)),
            (10, Some(200)),
            (14, Some(200)),
            // Offsets in the gap and past the end are uncovered.
            (5, None),
            (15, None),
        ] {
            assert2::assert!(idx.stamp_for_offset(Offset(offset)) == expected);
        }
    }

    #[test]
    fn append_rejects_overlapping_ranges_but_accepts_exact_retry() {
        let (_dir, _path, mut idx) = index_with(&[]);
        let entry = crate::test_support::stamp_entry(5, 7, 100);
        idx.append(entry).unwrap();
        idx.append(entry).unwrap();
        assert2::assert!(idx.entries() == [entry]);

        let error = idx
            .append(crate::test_support::stamp_entry(7, 9, 200))
            .unwrap_err();
        assert2::assert!(let LogError::Corrupt(_) = error);

        let error = idx
            .append(crate::test_support::stamp_entry(10, 9, 300))
            .unwrap_err();
        assert2::assert!(let LogError::InvalidArgument(_) = error);
    }

    #[test]
    fn upsert_replaces_only_an_exact_range() {
        let (_dir, path, mut idx) = index_with(&[crate::test_support::stamp_entry(5, 7, 100)]);

        idx.upsert(crate::test_support::stamp_entry(5, 7, 200))
            .unwrap();
        assert2::assert!(idx.stamp_for_offset(Offset(6)) == Some(200));
        assert2::assert!(StampIndex::open(path).unwrap().entries() == idx.entries());

        for (base, last) in [(5, 8), (4, 7)] {
            let error = idx
                .upsert(crate::test_support::stamp_entry(base, last, 300))
                .unwrap_err();
            assert2::assert!(let LogError::Corrupt(_) = error);
        }
    }

    #[test]
    fn truncate_from_removes_tail_entries_on_disk() {
        let (_dir, path, mut idx) = index_with(&[
            crate::test_support::stamp_entry(0, 2, 100),
            crate::test_support::stamp_entry(3, 6, 103),
            crate::test_support::stamp_entry(10, 12, 110),
        ]);

        idx.truncate_from(Offset(6)).unwrap();
        assert2::assert!(
            StampIndex::open(path).unwrap().entries()
                == [crate::test_support::stamp_entry(0, 2, 100)]
        );
    }

    #[test]
    fn remove_ranges_requires_both_exact_boundaries() {
        let (_dir, path, mut idx) = index_with(&[
            crate::test_support::stamp_entry(0, 1, 10),
            crate::test_support::stamp_entry(2, 3, 20),
        ]);

        idx.remove_ranges(&[(Offset(0), Offset(9)), (Offset(9), Offset(3))])
            .unwrap();
        assert2::assert!(idx.entries().len() == 2);
        idx.remove_ranges(&[(Offset(2), Offset(3))]).unwrap();

        assert2::assert!(
            StampIndex::open(path).unwrap().entries()
                == [crate::test_support::stamp_entry(0, 1, 10)]
        );
    }

    #[test]
    fn append_rejects_overlapping_single_offset_range_as_corrupt() {
        let (_dir, _path, mut idx) = index_with(&[crate::test_support::stamp_entry(0, 5, 10)]);

        // Overlapping single-offset range (last_offset == base_offset)
        let err = idx
            .append(crate::test_support::stamp_entry(2, 2, 20))
            .unwrap_err();
        assert2::assert!(let LogError::Corrupt(_) = err);
    }

    #[test]
    fn set_io_routes_stamp_index_writes() {
        use std::sync::atomic::{AtomicBool, Ordering};
        #[derive(Debug)]
        struct SpyIo(Arc<AtomicBool>);
        impl LogIo for SpyIo {
            fn write_at(
                &self,
                _t: IoTarget,
                file: &std::fs::File,
                buf: &[u8],
            ) -> std::io::Result<usize> {
                use std::io::Write;
                self.0.store(true, Ordering::SeqCst);
                (&*file).write(buf)
            }
        }
        let called = Arc::new(AtomicBool::new(false));
        let (_dir, _path, mut idx) = index_with(&[]);
        idx.set_io(Arc::new(SpyIo(called.clone())));
        idx.append(crate::test_support::stamp_entry(0, 0, 1))
            .unwrap();
        assert2::assert!(called.load(Ordering::SeqCst));
    }
}
