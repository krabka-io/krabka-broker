//! Decoders for the Kafka-format index and log bytes that a remote segment
//! carries.
//!
//! The copy path writes a segment's `.index`, `.timeindex`, and `.txnindex`
//! verbatim, so these decoders mirror the on-disk layouts that
//! `krabka_log::index::{OffsetIndex, TimeIndex}` and
//! `krabka_log::txn_index::TxnIndex` wrote locally.
//!
//! Every decoder here returns a [`RemoteStorageError`] on malformed input and
//! never panics. The bytes arrive from an object store, which can return
//! truncated or corrupt data, so a panic would be a denial-of-service surface
//! on the read path.
//!
//! One submodule per on-disk artifact: `offset` for the `.index` layout and
//! the byte range a fetch derives from it, `time` for the `.timeindex` layout
//! and its timestamp floor, `txn` for the `.txnindex` layout and its
//! range-overlap test, and `batch` for the scans over the `.log` bytes those
//! sparse indexes can only point near. The offset vocabulary and the two error
//! constructors they share stay here.

use crate::error::RemoteStorageError;

mod batch;
mod offset;
mod time;
mod txn;

pub use self::{
    batch::{first_batch_at_or_after, first_record_at_or_after_timestamp},
    offset::{
        OffsetIndexEntry, end_position_for, parse_offset_index, position_for_relative_offset,
    },
    time::{TimeIndexEntry, parse_time_index, relative_offset_floor_for_timestamp},
    txn::{AbortedTxnIndexEntry, parse_txn_index, txn_overlaps},
};

/// Absolute (partition-level) log offset.
pub type LogOffset = i64;
/// Record timestamp in milliseconds since the Unix epoch.
pub type TimestampMs = i64;
/// Offset relative to a segment's base offset. This is the offset-index key.
pub type RelativeOffset = u32;
/// Byte position within a segment's `.log` file. This is the offset-index
/// value.
pub type BytePosition = u32;

/// Helper for the `ref_from_bytes` parse error on the remote-read path.
///
/// The `zerocopy` cast can fail only on a length mismatch. The bytes come from
/// the object store, such as S3, which can return corrupt or truncated data.
/// This helper therefore returns a `RemoteStorageError` instead of a panic,
/// because a panic would be a `DoS` surface.
fn corrupt_index(kind: &str) -> RemoteStorageError {
    RemoteStorageError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("corrupt remote {kind} index bytes"),
    ))
}

/// Wraps a decode failure on a remote segment's `.log` bytes.
#[must_use]
pub fn corrupt_log(detail: impl std::fmt::Display) -> RemoteStorageError {
    RemoteStorageError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("corrupt remote log bytes: {detail}"),
    ))
}
