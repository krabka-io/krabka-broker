//! The Kafka `TimeIndex` on-disk layout and the relative-offset floor a
//! timestamp lookup scans from.
//!
//! An entry maps a timestamp to a relative offset. The index is sparse and
//! preallocated, so the floor lookup deliberately stops short of the first
//! entry at or above the target and treats non-increasing relative offsets as
//! trailing padding.

use zerocopy::{
    BigEndian, FromBytes, Immutable, KnownLayout, Unaligned,
    byteorder::{I64, U32},
};

use super::{RelativeOffset, TimestampMs, corrupt_index};
use crate::error::RemoteStorageError;

/// 12 bytes per entry: ts i64 BE, then rel u32 BE. It mirrors
/// `krabka_log::index::TimeEntryRaw`.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct TimeIndexEntry {
    /// Largest record timestamp at or before the indexed offset.
    pub timestamp: I64<BigEndian>,
    /// Offset that carries the timestamp, relative to the segment's base
    /// offset.
    pub relative_offset: U32<BigEndian>,
}

/// Byte length of one serialized time-index entry.
const TIME_INDEX_ENTRY_LEN: usize = std::mem::size_of::<TimeIndexEntry>();

const _: [(); 12] = [(); TIME_INDEX_ENTRY_LEN];

/// Borrows Kafka's `TimeIndex` on-disk format as a zero-copy
/// `&[TimeIndexEntry]`, at 12 bytes per entry: ts i64 BE, then rel u32 BE. It
/// ignores trailing bytes that do not complete a 12-byte entry. The result
/// borrows from `bytes`.
///
/// # Errors
///
/// Returns [`RemoteStorageError::Io`] when the object store returned bytes that
/// do not form an entry array.
pub fn parse_time_index(bytes: &[u8]) -> Result<&[TimeIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / TIME_INDEX_ENTRY_LEN) * TIME_INDEX_ENTRY_LEN;
    <[TimeIndexEntry]>::ref_from_bytes(&bytes[..truncated_len]).map_err(|_| corrupt_index("time"))
}

/// Returns a safe relative-offset floor for an exact timestamp scan.
///
/// The last entry strictly below `target_ts` is used rather than an entry at
/// or above it: a sparse index entry is only a seek hint and may follow the
/// earliest qualifying record. Non-increasing relative offsets mark trailing
/// preallocation padding and end the usable index.
#[must_use]
pub fn relative_offset_floor_for_timestamp(
    entries: &[TimeIndexEntry],
    target_ts: TimestampMs,
) -> RelativeOffset {
    let mut decoded = Vec::new();
    for entry in entries {
        let relative_offset = entry.relative_offset.get();
        let previous_relative_offset = decoded.last().map(|&(_, offset)| offset);
        if !krabka_verified::remote_time_index_offset_usable(
            previous_relative_offset,
            relative_offset,
        ) {
            break;
        }
        decoded.push((entry.timestamp.get(), relative_offset));
    }
    let candidate_count = krabka_verified::remote_time_index_candidate_count(&decoded, target_ts);
    candidate_count
        .checked_sub(1)
        .map_or(0, |index| decoded[index].1)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn time_entries(pairs: &[(i64, u32)]) -> Vec<TimeIndexEntry> {
        pairs
            .iter()
            .map(|&(ts, rel)| TimeIndexEntry {
                timestamp: I64::new(ts),
                relative_offset: U32::new(rel),
            })
            .collect()
    }

    #[test]
    fn parse_time_index_round_trips_known_entries() {
        let mut buf = Vec::new();
        for (ts, rel) in [(1_000_i64, 0_u32), (2_000, 10), (3_000, 20)] {
            buf.extend_from_slice(&ts.to_be_bytes());
            buf.extend_from_slice(&rel.to_be_bytes());
        }
        let entries = parse_time_index(&buf).expect("valid time index");
        let decoded: Vec<(i64, u32)> = entries
            .iter()
            .map(|e| (e.timestamp.get(), e.relative_offset.get()))
            .collect();
        assert!(decoded == vec![(1_000, 0), (2_000, 10), (3_000, 20)]);
    }

    #[test]
    fn timestamp_floor_covers_strict_predecessor_boundaries() {
        let entries = time_entries(&[(1_000, 0), (2_000, 10), (2_000, 20), (3_000, 30)]);
        for (ts, want) in [
            (500, 0),   // before first
            (1_000, 0), // exact first match scans from the segment start
            (1_500, 0), // between entries uses the strict predecessor
            (2_000, 0), // duplicate exact matches are both excluded
            (2_500, 20),
            (4_000, 30), // after last
        ] {
            assert!(
                relative_offset_floor_for_timestamp(&entries, ts) == want,
                "ts {ts}"
            );
        }
        assert!(relative_offset_floor_for_timestamp(&[], 1_000) == 0);
    }

    #[test]
    fn timestamp_floor_ignores_trailing_index_padding() {
        let entries = time_entries(&[(1_000, 0), (2_000, 10), (0, 0), (0, 0)]);
        assert!(relative_offset_floor_for_timestamp(&entries, 3_000) == 10);
    }
}
