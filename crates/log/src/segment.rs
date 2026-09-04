//! A single segment: the `.log`, `.index`, and `.timeindex` files that share
//! a base offset.

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_units::prelude::{ByteSize, ByteSizeExt};

mod activation;
mod append;
mod header_walk;
mod io;
mod lifecycle;
mod open;
mod read;
mod read_raw;
crate::sendfile_cfg! {
    mod read_raw_desc;
}
#[cfg(test)]
mod test_support;
mod timestamp_scan;

use crate::{
    index::{OffsetIndex, TimeIndex},
    io::LogIo,
};

/// A single log segment: the `.log` data file paired with its sparse
/// `.index` (offset → byte position) and `.timeindex` (timestamp →
/// relative offset) sidecars.
///
/// The `base_offset` identifies a segment. It is the absolute offset of the
/// segment's first record, encoded into the segment's 20-digit zero-padded
/// filename. [`Segment::create`] makes a new active segment.
/// [`Segment::open`] opens a read-only sealed segment, and
/// [`Segment::open_active`] opens an active segment with tail recovery.
#[derive(Debug)]
pub struct Segment {
    dir: PathBuf,
    base_offset: Offset,
    /// The `.log` data file, wrapped in `Arc`.
    ///
    /// The `Arc` lets the zero-copy fetch path (Increment D) hand a
    /// `FileRegion { file: Arc<File>, .. }` to the connection's async
    /// `sendfile` loop. The `Arc` pins the inode through the send even when
    /// retention rolls or removes this segment in the meantime, because the
    /// open fd keeps the inode alive on Unix. Writes and data syncs go through
    /// `io`, which normally delegates straight to this file.
    log_file: Arc<File>,
    io: Arc<dyn LogIo>,
    log_size: u64,
    offset_index: OffsetIndex,
    time_index: TimeIndex,
    /// `true` once a new segment has started after this one. Sealed segments
    /// do not accept appends.
    sealed: bool,
    /// Highest timestamp observed across all batches written here.
    max_timestamp: i64,
    /// Last absolute offset (inclusive) of any batch in this segment.
    last_offset: Offset,
}

/// Verbatim, decode-free output of [`Segment::read_raw`].
#[derive(Debug, Clone)]
pub struct RawSegmentRead {
    /// `base_offset` of the first included batch (≤ requested offset).
    pub start_offset: Offset,
    /// Last absolute offset covered by `bytes` (`start_offset - 1` if empty).
    pub last_offset: Offset,
    /// Verbatim `.log` bytes: one or more complete v2 batches.
    pub bytes: Bytes,
}

crate::sendfile_cfg! {
    /// Descriptor form of [`Segment::read_raw`] for the zero-copy fetch path,
    /// Increments D + E.
    ///
    /// This type carries the same offset and boundary metadata, but the
    /// records run is a [`krabka_protocol::records::FileRegion`] descriptor of
    /// the form `(Arc<File>, offset, len)`, not an owned `Bytes` slice. The
    /// broker can therefore `sendfile(2)` the run straight from the page cache
    /// with no userspace copy. This type is compiled on the SENDFILE alias:
    /// Linux, Apple, and FreeBSD/DragonFly.
    #[derive(Debug, Clone)]
    pub struct RawSegmentDesc {
        /// `base_offset` of the first included batch (≤ requested offset).
        pub start_offset: Offset,
        /// Last absolute offset covered by the region (`start_offset - 1` if empty).
        pub last_offset: Offset,
        /// The records run, as a file-backed descriptor. `None` when the walk
        /// found no complete batch in range.
        pub region: Option<krabka_protocol::records::FileRegion>,
    }

    impl RawSegmentDesc {
        fn empty() -> Self {
            Self {
                start_offset: Offset(0),
                last_offset: Offset(-1),
                region: None,
            }
        }

        /// Byte length of the region. It is 0 when the region is empty.
        #[must_use]
        pub fn len(&self) -> usize {
            self.region.as_ref().map_or(0, |r| r.len)
        }

        /// `true` when no batch bytes were described.
        #[must_use]
        pub fn is_empty(&self) -> bool {
            self.region.is_none()
        }
    }
}

impl RawSegmentRead {
    // cargo-mutants: the `last_offset` of an empty read is a never-read sentinel: every
    // consumer guards on `is_empty()` (which checks `bytes`) before touching
    // `last_offset`, so `Offset(-1)` vs `Offset(1)` is unobservable.
    #[cfg_attr(test, mutants::skip)]
    fn empty() -> Self {
        Self {
            start_offset: Offset(0),
            last_offset: Offset(-1),
            bytes: Bytes::new(),
        }
    }

    /// `true` when no batch bytes were returned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// What an activation walk found in one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActivationScan {
    /// Exclusive end of the run of active batches that begins at the offset
    /// the walk started from. It equals that offset when the first batch is
    /// already waiting.
    pub active_end: Offset,
    /// Activation time of the first batch that is not active yet, or `None`
    /// when every batch from the starting offset to the end of this segment
    /// is active.
    pub pending_at: Option<i64>,
}

impl Segment {
    /// Absolute offset of the first record this segment can hold.
    #[must_use]
    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    /// Path to this segment's `.txnindex` file (may not exist yet).
    #[must_use]
    pub fn txn_index_path(&self) -> std::path::PathBuf {
        crate::name::txnindex_path(&self.dir, self.base_offset.0)
    }

    /// Path to this segment's `.stampindex` sidecar (may not exist yet).
    #[must_use]
    pub fn stamp_index_path(&self) -> std::path::PathBuf {
        crate::name::stampindex_path(&self.dir, self.base_offset.0)
    }

    /// Path to the per-partition `.leader-epoch-checkpoint` file in this
    /// segment's directory. All segments in a partition share the checkpoint,
    /// and epoch history accumulates over the log's lifetime.
    #[must_use]
    pub fn leader_epoch_checkpoint_path(&self) -> std::path::PathBuf {
        crate::name::leader_epoch_checkpoint_path(&self.dir)
    }

    /// Highest absolute offset (inclusive) of any batch appended to this
    /// segment. Returns `base_offset - 1` for an empty segment.
    #[must_use]
    pub fn last_offset(&self) -> Offset {
        self.last_offset
    }

    /// Current `.log` file size.
    ///
    /// The field itself stays a raw `u64`, because it is a file position and
    /// goes directly into a `pread` or `seek` argument. It converts here,
    /// where the value becomes a magnitude that the roll and retention
    /// policies compare.
    #[must_use]
    pub fn size(&self) -> ByteSize {
        ByteSize::from_bytes(self.log_size)
    }

    /// Highest timestamp observed across all batches in this segment.
    /// Returns `i64::MIN` for an empty segment.
    #[must_use]
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// `true` once [`Segment::seal`] has sealed the segment. Sealed segments
    /// reject appends.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Directory that holds this segment's `.log`, `.index`, and `.timeindex`
    /// files. The compactor uses it to read the `.log` file directly and to
    /// avoid the `Segment::read` path. That path depends on the in-memory
    /// `last_offset`, which is stale for sealed segments loaded through
    /// `Segment::open`.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}
