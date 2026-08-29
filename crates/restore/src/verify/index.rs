//! The consistency checks for a segment's three sparse sidecar indexes, the
//! `.index`, the `.timeindex`, and the `.txnindex`. They share one shape --
//! fixed-width entries whose fields must be monotonic and must stay inside the
//! extent the `.log` walk established -- and the diagnostics that report a bad
//! entry by its byte position, so they sit together in one file.

use krabka_ids::Offset;
use krabka_remote_storage::index::{
    AbortedTxnIndexEntry, OffsetIndexEntry, TimeIndexEntry, parse_offset_index, parse_time_index,
    parse_txn_index,
};
use object_store::path::Path;

use super::offset_as_u64;
use crate::error::RestoreError;

/// Byte length of one serialized offset-index entry. `krabka_remote_storage`
/// keeps its own copy of this constant private, so it is recomputed here from
/// the zerocopy struct it mirrors.
const OFFSET_INDEX_ENTRY_LEN: usize = std::mem::size_of::<OffsetIndexEntry>();

/// Byte length of one serialized time-index entry. See
/// [`OFFSET_INDEX_ENTRY_LEN`].
const TIME_INDEX_ENTRY_LEN: usize = std::mem::size_of::<TimeIndexEntry>();

/// Byte length of one serialized aborted-transaction index entry. See
/// [`OFFSET_INDEX_ENTRY_LEN`].
const TXN_INDEX_ENTRY_LEN: usize = std::mem::size_of::<AbortedTxnIndexEntry>();

/// The highest relative offset an index entry may legally point at: the
/// segment's last offset, expressed relative to its base offset.
fn max_relative_offset(base_offset: Offset, end_offset: Offset) -> u32 {
    u32::try_from((end_offset.0 - base_offset.0).max(0)).unwrap_or(u32::MAX)
}

/// Byte position of one fixed-width index entry, for a
/// [`RestoreError::TruncatedSegment`].
fn index_position(entry_index: usize, entry_len: usize) -> u64 {
    u64::try_from(entry_index.saturating_mul(entry_len)).unwrap_or(u64::MAX)
}

/// Build the [`RestoreError::TruncatedSegment`] one bad index entry reports.
fn index_error(
    key: &Path,
    entry_index: usize,
    entry_len: usize,
    declared: u64,
    available: u64,
) -> RestoreError {
    RestoreError::TruncatedSegment {
        key: key.to_string(),
        position: index_position(entry_index, entry_len),
        declared,
        available,
    }
}

/// Check that `bytes`, parsed as a Kafka `.index`, is internally consistent
/// with the segment the `.log` walk established: `relative_offset` and
/// `position` both non-decreasing across entries, every `position` inside the
/// verified `.log`, and every `relative_offset` at or before the segment's
/// last offset.
pub(super) fn validate_offset_index(
    key: &Path,
    bytes: &[u8],
    base_offset: Offset,
    end_offset: Offset,
    log_bytes: u64,
) -> Result<(), RestoreError> {
    let entries = parse_offset_index(bytes)?;
    let max_relative = max_relative_offset(base_offset, end_offset);
    let mut previous: Option<(u32, u32)> = None;

    for (entry_index, entry) in entries.iter().enumerate() {
        let relative_offset = entry.relative_offset.get();
        let position = entry.position.get();

        if let Some((previous_relative, previous_position)) = previous {
            if relative_offset < previous_relative {
                return Err(index_error(
                    key,
                    entry_index,
                    OFFSET_INDEX_ENTRY_LEN,
                    u64::from(previous_relative),
                    u64::from(relative_offset),
                ));
            }
            if position < previous_position {
                return Err(index_error(
                    key,
                    entry_index,
                    OFFSET_INDEX_ENTRY_LEN,
                    u64::from(previous_position),
                    u64::from(position),
                ));
            }
        }
        if u64::from(position) >= log_bytes {
            return Err(index_error(
                key,
                entry_index,
                OFFSET_INDEX_ENTRY_LEN,
                log_bytes,
                u64::from(position),
            ));
        }
        if relative_offset > max_relative {
            return Err(index_error(
                key,
                entry_index,
                OFFSET_INDEX_ENTRY_LEN,
                u64::from(max_relative),
                u64::from(relative_offset),
            ));
        }

        previous = Some((relative_offset, position));
    }
    Ok(())
}

/// Check that `bytes`, parsed as a Kafka `.timeindex`, is internally
/// consistent: `relative_offset` and `timestamp` both non-decreasing across
/// entries, and every `relative_offset` at or before the segment's last
/// offset.
///
/// A time-index entry has no on-disk byte position to check against
/// `log_bytes`, unlike an offset-index entry, so that check does not apply
/// here; `timestamp` is checked non-decreasing in its place, the analogous
/// invariant the broker's append-only writer also holds for this index.
pub(super) fn validate_time_index(
    key: &Path,
    bytes: &[u8],
    base_offset: Offset,
    end_offset: Offset,
) -> Result<(), RestoreError> {
    let entries = parse_time_index(bytes)?;
    let max_relative = max_relative_offset(base_offset, end_offset);
    let mut previous: Option<(i64, u32)> = None;

    for (entry_index, entry) in entries.iter().enumerate() {
        let timestamp = entry.timestamp.get();
        let relative_offset = entry.relative_offset.get();

        if let Some((previous_timestamp, previous_relative)) = previous {
            if relative_offset < previous_relative {
                return Err(index_error(
                    key,
                    entry_index,
                    TIME_INDEX_ENTRY_LEN,
                    u64::from(previous_relative),
                    u64::from(relative_offset),
                ));
            }
            if timestamp < previous_timestamp {
                return Err(index_error(
                    key,
                    entry_index,
                    TIME_INDEX_ENTRY_LEN,
                    offset_as_u64(previous_timestamp),
                    offset_as_u64(timestamp),
                ));
            }
        }
        if relative_offset > max_relative {
            return Err(index_error(
                key,
                entry_index,
                TIME_INDEX_ENTRY_LEN,
                u64::from(max_relative),
                u64::from(relative_offset),
            ));
        }

        previous = Some((timestamp, relative_offset));
    }
    Ok(())
}

/// Check that `bytes`, parsed as a Kafka `.txnindex`, is internally
/// consistent: `start_offset` non-decreasing across entries, `start_offset <=
/// last_offset` within each entry, and both ends of every entry inside
/// `[base_offset, end_offset]`.
pub(super) fn validate_txn_index(
    key: &Path,
    bytes: &[u8],
    base_offset: Offset,
    end_offset: Offset,
) -> Result<(), RestoreError> {
    let entries = parse_txn_index(bytes)?;
    let mut previous_start: Option<i64> = None;

    for (entry_index, entry) in entries.iter().enumerate() {
        let start_offset = entry.start_offset.get();
        let last_offset = entry.last_offset.get();

        if let Some(previous) = previous_start
            && start_offset < previous
        {
            return Err(index_error(
                key,
                entry_index,
                TXN_INDEX_ENTRY_LEN,
                offset_as_u64(previous),
                offset_as_u64(start_offset),
            ));
        }
        if start_offset > last_offset {
            return Err(index_error(
                key,
                entry_index,
                TXN_INDEX_ENTRY_LEN,
                offset_as_u64(last_offset),
                offset_as_u64(start_offset),
            ));
        }
        if start_offset < base_offset.0 || last_offset > end_offset.0 {
            return Err(index_error(
                key,
                entry_index,
                TXN_INDEX_ENTRY_LEN,
                offset_as_u64(end_offset.0),
                offset_as_u64(last_offset.max(start_offset)),
            ));
        }

        previous_start = Some(start_offset);
    }
    Ok(())
}
