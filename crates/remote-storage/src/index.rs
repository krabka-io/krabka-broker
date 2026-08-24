//! Decoders for the Kafka-format index and log bytes that a remote segment
//! carries.
//!
//! The copy path writes a segment's `.index`, `.timeindex`, and `.txnindex`
//! verbatim, so these decoders mirror the on-disk layouts that
//! `crabka_log::index::{OffsetIndex, TimeIndex}` and
//! `crabka_log::txn_index::TxnIndex` wrote locally.
//!
//! Every decoder here returns a [`RemoteStorageError`] on malformed input and
//! never panics. The bytes arrive from an object store, which can return
//! truncated or corrupt data, so a panic would be a denial-of-service surface
//! on the read path.

use crabka_protocol::records::RecordBatch;
use zerocopy::{
    BigEndian, FromBytes, Immutable, KnownLayout, Unaligned,
    byteorder::{I64, U32},
};

use crate::error::RemoteStorageError;

/// Absolute (partition-level) log offset.
pub type LogOffset = i64;
/// Record timestamp in milliseconds since the Unix epoch.
pub type TimestampMs = i64;
/// Offset relative to a segment's base offset. This is the offset-index key.
pub type RelativeOffset = u32;
/// Byte position within a segment's `.log` file. This is the offset-index
/// value.
pub type BytePosition = u32;

/// 8 bytes per entry: rel u32 BE, then pos u32 BE. It mirrors
/// `crabka_log::index::OffsetEntryRaw`, so the remote-tier copy of an
/// `OffsetIndex` file decodes through the same byte layout that wrote the
/// local index.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct OffsetIndexEntry {
    /// Offset of the indexed record, relative to the segment's base offset.
    pub relative_offset: U32<BigEndian>,
    /// Byte position of that record's batch within the `.log` file.
    pub position: U32<BigEndian>,
}

/// Byte length of one serialized offset-index entry.
const OFFSET_INDEX_ENTRY_LEN: usize = std::mem::size_of::<OffsetIndexEntry>();

const _: () = assert!(OFFSET_INDEX_ENTRY_LEN == 8);

/// 12 bytes per entry: ts i64 BE, then rel u32 BE. It mirrors
/// `crabka_log::index::TimeEntryRaw`.
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

const _: () = assert!(TIME_INDEX_ENTRY_LEN == 12);

/// 24 bytes per entry: `start_offset` i64 BE, `last_offset` i64 BE, then
/// `producer_id` i64 BE. It mirrors `crabka_log::txn_index::AbortedTxnRaw`, so
/// the remote-tier copy of a `.txnindex` file decodes through the same byte
/// layout that wrote the local index.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct AbortedTxnIndexEntry {
    /// First offset the aborted transaction wrote.
    pub start_offset: I64<BigEndian>,
    /// Last offset the aborted transaction wrote.
    pub last_offset: I64<BigEndian>,
    /// Producer that wrote, and then aborted, the transaction.
    pub producer_id: I64<BigEndian>,
}

/// Byte length of one serialized aborted-transaction index entry.
const TXN_INDEX_ENTRY_LEN: usize = std::mem::size_of::<AbortedTxnIndexEntry>();

const _: () = assert!(TXN_INDEX_ENTRY_LEN == 24);

/// Computes the inclusive `end_position` for a remote byte-range fetch.
///
/// It returns `None`, which means read to the end of the segment, when
/// `start_position` plus `max_bytes` would reach or pass `segment_size`. In
/// every other case it returns the inclusive last byte to read.
#[must_use]
pub fn end_position_for(
    start_position: BytePosition,
    segment_size: u32,
    max_bytes: usize,
) -> Option<BytePosition> {
    if max_bytes == 0 {
        return None;
    }
    let max_bytes_u32 = u32::try_from(max_bytes).unwrap_or(u32::MAX);
    let exclusive_end = start_position.saturating_add(max_bytes_u32);
    if exclusive_end >= segment_size {
        None
    } else {
        Some(exclusive_end.saturating_sub(1))
    }
}

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

/// Borrows Kafka's `OffsetIndex` on-disk format as a zero-copy
/// `&[OffsetIndexEntry]`, at 8 bytes per entry: rel u32 BE, then pos u32 BE.
/// It ignores trailing bytes that do not complete an 8-byte entry. The result
/// borrows from `bytes`.
///
/// # Errors
///
/// Returns [`RemoteStorageError::Io`] when the object store returned bytes that
/// do not form an entry array.
pub fn parse_offset_index(bytes: &[u8]) -> Result<&[OffsetIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / OFFSET_INDEX_ENTRY_LEN) * OFFSET_INDEX_ENTRY_LEN;
    <[OffsetIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .map_err(|_| corrupt_index("offset"))
}

/// Floor lookup: the byte position of the largest entry with
/// `rel <= target_rel`. It returns 0 when the index is empty, and when the
/// target is before the first entry. It runs directly against the borrowed
/// zero-copy slice and builds no owned `Vec`.
#[must_use]
pub fn position_for_relative_offset(
    entries: &[OffsetIndexEntry],
    target_rel: RelativeOffset,
) -> BytePosition {
    match entries.binary_search_by_key(&target_rel, |e| e.relative_offset.get()) {
        Ok(i) => entries[i].position.get(),
        Err(0) => 0,
        Err(i) => entries[i - 1].position.get(),
    }
}

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

/// Borrows Kafka's transaction-index format as a zero-copy
/// `&[AbortedTxnIndexEntry]`, at 24 bytes per entry: `start_offset` i64 BE,
/// `last_offset` i64 BE, then `producer_id` i64 BE. It ignores trailing bytes
/// that do not complete a 24-byte entry. The result borrows from `bytes`.
///
/// # Errors
///
/// Returns [`RemoteStorageError::Io`] when the object store returned bytes that
/// do not form an entry array.
pub fn parse_txn_index(bytes: &[u8]) -> Result<&[AbortedTxnIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / TXN_INDEX_ENTRY_LEN) * TXN_INDEX_ENTRY_LEN;
    <[AbortedTxnIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .map_err(|_| corrupt_index("transaction"))
}

/// Reports whether an aborted-transaction entry overlaps the inclusive offset
/// range `[from_offset, to_offset]`. It mirrors the overlap test in
/// `TxnIndex::aborted_in_range` against an inclusive range: the entry's
/// `[start, last]` intersects `[from, to]` if and only if
/// `start <= to && last >= from`.
#[must_use]
pub fn txn_overlaps(
    entry: &AbortedTxnIndexEntry,
    from_offset: LogOffset,
    to_offset: LogOffset,
) -> bool {
    entry.start_offset.get() <= to_offset && entry.last_offset.get() >= from_offset
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
    let mut floor = 0;
    let mut previous_relative_offset = None;
    for entry in entries {
        let relative_offset = entry.relative_offset.get();
        if previous_relative_offset.is_some_and(|previous| relative_offset <= previous)
            || entry.timestamp.get() >= target_ts
        {
            break;
        }
        floor = relative_offset;
        previous_relative_offset = Some(relative_offset);
    }
    floor
}

/// Decodes remote log batches and returns the earliest record at or after both
/// `floor_offset` and `target_timestamp`.
///
/// # Errors
///
/// Returns [`RemoteStorageError::Io`] when a batch does not decode, or when a
/// record's offset or timestamp delta overflows its base.
pub fn first_record_at_or_after_timestamp(
    data: &[u8],
    floor_offset: LogOffset,
    target_timestamp: TimestampMs,
) -> Result<Option<(LogOffset, TimestampMs)>, RemoteStorageError> {
    let mut cur = data;
    while !cur.is_empty() {
        let batch = RecordBatch::decode(&mut cur).map_err(corrupt_log)?;
        for record in &batch.records {
            let offset = batch
                .base_offset
                .checked_add(i64::from(record.offset_delta))
                .ok_or_else(|| corrupt_log("record offset overflow"))?;
            if offset < floor_offset {
                continue;
            }
            let timestamp = batch
                .base_timestamp
                .checked_add(record.timestamp_delta)
                .ok_or_else(|| corrupt_log("record timestamp overflow"))?;
            if timestamp >= target_timestamp {
                return Ok(Some((offset, timestamp)));
            }
        }
    }
    Ok(None)
}

/// Decodes batches from `data` and returns the first one whose last offset is
/// `>= floor`. It skips the batches at the start of the returned byte range
/// that the offset index pointed at but that do not cover the requested
/// offset. Kafka offset indexes are sparse, so such batches occur.
#[must_use]
pub fn first_batch_at_or_after(data: &[u8], floor: LogOffset) -> Option<RecordBatch> {
    let mut cur: &[u8] = data;
    while !cur.is_empty() {
        let Ok(batch) = RecordBatch::decode(&mut cur) else {
            break;
        };
        let last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
        if last_offset >= floor {
            return Some(batch);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use crabka_protocol::records::Record;

    use super::*;

    fn offset_entries(pairs: &[(u32, u32)]) -> Vec<OffsetIndexEntry> {
        pairs
            .iter()
            .map(|&(rel, pos)| OffsetIndexEntry {
                relative_offset: U32::new(rel),
                position: U32::new(pos),
            })
            .collect()
    }

    fn time_entries(pairs: &[(i64, u32)]) -> Vec<TimeIndexEntry> {
        pairs
            .iter()
            .map(|&(ts, rel)| TimeIndexEntry {
                timestamp: I64::new(ts),
                relative_offset: U32::new(rel),
            })
            .collect()
    }

    fn test_batch_at(base_offset: i64, record_count: i32, value_byte: u8) -> RecordBatch {
        let mut batch = RecordBatch {
            base_offset,
            last_offset_delta: record_count - 1,
            ..RecordBatch::default()
        };
        for offset_delta in 0..record_count {
            batch.records.push(Record {
                offset_delta,
                value: Some(Bytes::from(vec![value_byte; 4])),
                ..Default::default()
            });
        }
        batch
    }

    fn timestamped_batch_at(base_offset: i64, timestamps: &[i64], value_byte: u8) -> RecordBatch {
        let base_timestamp = timestamps.first().copied().unwrap_or_default();
        RecordBatch {
            base_offset,
            last_offset_delta: i32::try_from(timestamps.len().saturating_sub(1)).unwrap(),
            base_timestamp,
            max_timestamp: timestamps.iter().copied().max().unwrap_or_default(),
            records: timestamps
                .iter()
                .enumerate()
                .map(|(offset_delta, timestamp)| Record {
                    timestamp_delta: timestamp - base_timestamp,
                    offset_delta: i32::try_from(offset_delta).unwrap(),
                    value: Some(Bytes::from(vec![value_byte; 4])),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn encoded(batches: &[RecordBatch]) -> Bytes {
        let mut buf = BytesMut::new();
        for batch in batches {
            batch.encode(&mut buf).unwrap();
        }
        buf.freeze()
    }

    #[test]
    fn parse_offset_index_round_trips_known_entries() {
        // Mirror OffsetIndex::append: 4B rel BE, 4B pos BE.
        let mut buf = Vec::new();
        for (rel, pos) in [(0_u32, 0_u32), (10, 256), (20, 512)] {
            buf.extend_from_slice(&rel.to_be_bytes());
            buf.extend_from_slice(&pos.to_be_bytes());
        }
        let entries = parse_offset_index(&buf).expect("valid offset index");
        let decoded: Vec<(u32, u32)> = entries
            .iter()
            .map(|e| (e.relative_offset.get(), e.position.get()))
            .collect();
        assert!(decoded == vec![(0, 0), (10, 256), (20, 512)]);
    }

    #[test]
    fn position_for_relative_offset_returns_floor() {
        let entries = offset_entries(&[(0, 0), (10, 256), (20, 512), (30, 1024)]);
        let cases: [(&[OffsetIndexEntry], u32, u32); 5] = [
            (&entries, 10, 256),   // exact
            (&entries, 15, 256),   // between
            (&entries, 0, 0),      // first entry exact
            (&entries, 100, 1024), // after last
            (&[], 50, 0),          // empty
        ];
        for (entries, rel, want) in cases {
            assert!(
                position_for_relative_offset(entries, rel) == want,
                "rel {rel}"
            );
        }
    }

    #[test]
    fn position_for_relative_offset_below_first() {
        // Synthetic: first entry isn't at rel=0. Floor below it returns 0.
        let entries = offset_entries(&[(5, 100), (10, 200)]);
        assert!(position_for_relative_offset(&entries, 3) == 0);
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
    fn timestamp_floor_stays_before_sparse_match() {
        let entries = time_entries(&[(1_000, 0), (2_000, 10), (3_000, 20)]);
        let cases: [(&[TimeIndexEntry], i64, u32); 4] = [
            (&entries, 1_000, 0), // exact match scans from the segment start
            (&entries, 1_500, 0), // between entries scans from the lower hint
            (&entries, 4_000, 20),
            (&[], 1_000, 0),
        ];
        for (entries, ts, want) in cases {
            assert!(
                relative_offset_floor_for_timestamp(entries, ts) == want,
                "ts {ts} entries_len {}",
                entries.len()
            );
        }
    }

    #[test]
    fn timestamp_floor_ignores_trailing_index_padding() {
        let entries = time_entries(&[(1_000, 0), (2_000, 10), (0, 0), (0, 0)]);
        assert!(relative_offset_floor_for_timestamp(&entries, 3_000) == 10);
    }

    #[test]
    fn end_position_for_caps_with_max_bytes() {
        let cases = [
            // start=0, segment=1024, max_bytes=256 → exclusive_end=256 →
            // inclusive=255.
            (0, 1024, 256, Some(255)),
            // max_bytes >= remaining → read to end.
            (512, 1024, 999_999, None),
            // max_bytes=0 → read to end (zero is a no-cap sentinel).
            (0, 1024, 0, None),
            // start past the segment-end cap still safe via saturating add.
            (u32::MAX, 1024, 100, None),
        ];
        for (start, segment, max_bytes, want) in cases {
            assert!(
                end_position_for(start, segment, max_bytes) == want,
                "start {start} segment {segment} max_bytes {max_bytes}"
            );
        }
    }

    #[test]
    fn first_batch_at_or_after_decodes_and_skips() {
        // Two adjacent batches; floor=10 should skip the first (last=9) and
        // return the second.
        let bytes = encoded(&[test_batch_at(0, 10, b'a'), test_batch_at(10, 10, b'b')]);

        let cases = [
            // Floor=10 skips the first batch (last=9), returns the second.
            (10, Some(10)),
            // Floor below everything → first batch.
            (0, Some(0)),
            // Floor above everything → None.
            (1_000, None),
        ];
        for (floor, want_base) in cases {
            assert!(
                first_batch_at_or_after(&bytes, floor).map(|b| b.base_offset) == want_base,
                "floor {floor}"
            );
        }

        // Empty buffer → None.
        assert!(first_batch_at_or_after(&[], 0).is_none());
    }

    #[test]
    fn first_batch_at_or_after_rejects_floor_past_base_plus_delta() {
        let bytes = encoded(&[test_batch_at(3, 4, b'z')]);

        assert!(
            first_batch_at_or_after(&bytes, 7).is_none(),
            "batch 3..6 must not cover floor 7"
        );
    }

    #[test]
    fn first_record_at_or_after_timestamp_honours_both_floors() {
        let bytes = encoded(&[
            timestamped_batch_at(10, &[1_000, 1_100, 1_600, 1_700], b'a'),
            timestamped_batch_at(14, &[2_000, 2_200, 2_400], b'b'),
        ]);

        let cases = [
            // Both floors satisfied inside the first batch.
            (10, 1_600, Some((12, 1_600))),
            // The offset floor skips qualifying records in the first batch.
            (14, 1_000, Some((14, 2_000))),
            // No record reaches the timestamp floor.
            (10, 9_999, None),
        ];
        for (floor_offset, target, want) in cases {
            assert!(
                first_record_at_or_after_timestamp(&bytes, floor_offset, target).unwrap() == want,
                "floor_offset {floor_offset} target {target}"
            );
        }
    }

    #[test]
    fn first_record_at_or_after_timestamp_reports_corrupt_bytes() {
        // A truncated batch header decodes to an error, never a panic.
        let bytes = encoded(&[test_batch_at(0, 2, b'a')]);
        let error = first_record_at_or_after_timestamp(&bytes[..12], 0, 0).unwrap_err();
        assert!(matches!(error, RemoteStorageError::Io(_)));
    }

    #[test]
    fn parse_txn_index_round_trips_known_entries() {
        // Mirror TxnIndex::append: 8B start_offset BE, 8B last_offset BE,
        // 8B producer_id BE.
        let mut buf = Vec::new();
        for (start, last, pid) in [(0_i64, 4_i64, 1000_i64), (10, 14, 2000)] {
            buf.extend_from_slice(&start.to_be_bytes());
            buf.extend_from_slice(&last.to_be_bytes());
            buf.extend_from_slice(&pid.to_be_bytes());
        }
        let entries = parse_txn_index(&buf).expect("valid txn index");
        let decoded: Vec<(i64, i64, i64)> = entries
            .iter()
            .map(|e| {
                (
                    e.start_offset.get(),
                    e.last_offset.get(),
                    e.producer_id.get(),
                )
            })
            .collect();
        assert!(decoded == vec![(0, 4, 1000), (10, 14, 2000)]);
    }

    #[test]
    fn parse_txn_index_truncates_trailing_partial_bytes() {
        let mut buf = Vec::new();
        for v in [0_i64, 4, 1000] {
            buf.extend_from_slice(&v.to_be_bytes());
        }
        // 5 trailing bytes that don't complete a 24-byte entry.
        buf.extend_from_slice(&[0xAA; 5]);
        let entries = parse_txn_index(&buf).expect("valid txn index");
        assert!(entries.len() == 1, "partial trailing entry ignored");
        assert!(entries[0].producer_id.get() == 1000);
    }

    #[test]
    fn parse_txn_index_empty_is_empty() {
        assert!(parse_txn_index(&[]).expect("empty is valid").is_empty());
    }

    #[test]
    fn txn_overlaps_boundaries() {
        let e = AbortedTxnIndexEntry {
            start_offset: I64::new(10),
            last_offset: I64::new(14),
            producer_id: I64::new(1),
        };
        let cases = [
            // Range fully before the entry → excluded.
            (0, 9, false),
            // Range touching the entry's first offset → included.
            (0, 10, true),
            // Range fully inside the entry → included.
            (11, 13, true),
            // Range touching the entry's last offset → included.
            (14, 100, true),
            // Range fully after the entry → excluded.
            (15, 100, false),
            // Range fully covering the entry → included.
            (0, 100, true),
        ];
        for (start, end, want) in cases {
            assert!(
                txn_overlaps(&e, start, end) == want,
                "range [{start},{end}]"
            );
        }
    }
}
