//! Scans over a remote segment's `.log` bytes, which the offset index can only
//! point near.
//!
//! Kafka offset and time indexes are sparse, so a byte range fetched from the
//! object store usually starts before the record the caller asked for. These
//! scans decode the batches in that range and pick the first one, or the first
//! record, that satisfies the request.

use krabka_protocol::records::RecordBatch;

use super::{LogOffset, TimestampMs, corrupt_log};
use crate::error::RemoteStorageError;

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
    use krabka_protocol::records::Record;

    use super::*;

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
}
