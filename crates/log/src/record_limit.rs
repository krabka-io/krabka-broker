//! Kafka trunk's `max.decompressed.message.bytes` at the places the log
//! decompresses a batch.
//!
//! Trunk hands `LogConfig.maxDecompressedMessageBytes()` to the iterator that
//! decompresses a compressed batch, and `DefaultRecord.readFrom` throws
//! `InvalidRecordException` for a record whose declared body is larger,
//! before it allocates the body. Besides `Produce` validation, which the broker
//! runs, three log operations iterate compressed batches that way: the
//! cleaner's offset-map build and rewrite (`Cleaner.buildOffsetMapForSegment`
//! and `MemoryRecords.filterTo`), and the by-timestamp lookups
//! (`FileRecords.searchForTimestamp`, `RecordBatch.offsetOfMaxTimestamp`).
//! Follower appends do not: `UnifiedLog.appendAsFollower` skips
//! `LogValidator`, so a record that got past the leader's limit is replicated
//! as it is.
//!
//! An uncompressed batch is never held to the limit: Kafka reads it in place
//! and bounds it by `max.message.bytes` alone.
//!
//! krabka's reads have decoded the whole batch by the time these checks run, so
//! the check is not an allocation guard here. It is the verdict the operator
//! configured, reached on the same records Kafka reaches it on.

use krabka_compression::CompressionType;
use krabka_protocol::{
    primitives::varint::varlong_len,
    records::{Record, RecordBatch},
};
use krabka_units::prelude::{ByteSize, ByteSizeExt as _};

use crate::error::LogError;

/// The size of `record`'s body as Kafka's `DefaultRecord` writes it: the
/// `sizeOfBodyInBytes` its length prefix declares, which is what
/// `DefaultRecord.readFrom` compares to the limit.
///
/// `Record::encoded_len` is that body behind its varlong length prefix, and the
/// prefix takes a size that grows with the body, so the body is the one length
/// whose prefix accounts for the rest.
#[must_use]
pub(crate) fn record_body_len(record: &Record) -> usize {
    let total = record.encoded_len();
    (1..=10)
        .find_map(|prefix| {
            let body = total.checked_sub(prefix)?;
            (varlong_len(i64::try_from(body).ok()?) == prefix).then_some(body)
        })
        .unwrap_or(total)
}

/// Refuse `batch` if Kafka's iterator would refuse it after reading its first
/// `read` records, under `limit`.
///
/// `read` is how many records the caller's scan decodes before it stops:
/// Kafka's iterators decode lazily, so a lookup that returns at the third
/// record never sees a fourth that is oversized. A scan that reads the whole
/// batch passes `batch.records.len()`. A batch the producer did not compress,
/// or a `limit` of `None`, passes.
///
/// # Errors
///
/// Returns [`LogError::RecordTooLarge`] for the first record among those read
/// whose body is larger than `limit`.
pub(crate) fn check_records_read(
    batch: &RecordBatch,
    read: usize,
    limit: Option<ByteSize>,
) -> Result<(), LogError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    if batch.attributes.compression() == CompressionType::None {
        return Ok(());
    }
    let limit = limit.bytes_usize();
    for record in batch.records.iter().take(read) {
        let size = record_body_len(record);
        if size > limit {
            return Err(LogError::RecordTooLarge { size, limit });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use krabka_protocol::records::{Attributes, RecordHeader};
    use krabka_units::prelude::bytes as byte_size;

    use super::*;

    /// A record with a `value_len`-byte value, a key, a header and a
    /// two-byte timestamp delta.
    fn record(value_len: usize) -> Record {
        Record {
            timestamp_delta: 300,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from(vec![7_u8; value_len])),
            headers: vec![RecordHeader {
                key: "h".into(),
                value: None,
            }],
            ..Default::default()
        }
    }

    fn batch(codec: CompressionType, records: Vec<Record>) -> RecordBatch {
        RecordBatch {
            attributes: Attributes::default().with_compression(codec),
            last_offset_delta: i32::try_from(records.len()).unwrap() - 1,
            records,
            ..Default::default()
        }
    }

    /// Kafka's `sizeOfBodyInBytes` of the record: the attributes byte, a
    /// two-byte timestamp delta, a one-byte offset delta, `1 + 3` bytes of key,
    /// the value behind its own length prefix, a one-byte header count and one
    /// header of `1 + 1` bytes of key with a null value. The record's own length
    /// prefix grows with its body, so the sizes straddle its widths: a body of
    /// 63 bytes takes a one-byte prefix and one of 64 takes two, and 8191 takes
    /// two where 8192 takes three.
    #[test]
    fn a_record_body_is_measured_the_way_kafka_encodes_it() {
        for (value_len, body) in [
            (0, 13),
            (50, 63),
            (51, 64),
            (100, 114),
            (8_177, 8_191),
            (8_178, 8_192),
            (70_000, 70_015),
        ] {
            assert2::check!(record_body_len(&record(value_len)) == body, "{value_len}");
        }
    }

    /// The body size of the record `check_records_read` refuses under `limit`,
    /// or `None` when it passes the batch.
    fn refused_size(
        codec: CompressionType,
        records: Vec<Record>,
        read: usize,
        limit: Option<ByteSize>,
    ) -> Option<usize> {
        match check_records_read(&batch(codec, records), read, limit) {
            Ok(()) => None,
            Err(LogError::RecordTooLarge { size, limit }) => {
                assert2::check!(limit == 100);
                Some(size)
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    /// Only a compressed batch is held to the limit, and the first oversized
    /// record among the ones read decides.
    #[test]
    fn only_a_compressed_batch_is_held_to_the_limit() {
        let small = record(10);
        let large = record(1_000);
        for (name, codec, records, read, expected) in [
            (
                "compressed, oversized",
                CompressionType::Gzip,
                vec![small.clone(), large.clone()],
                2,
                Some(1_014),
            ),
            (
                "compressed, oversized record not read",
                CompressionType::Gzip,
                vec![small.clone(), large.clone()],
                1,
                None,
            ),
            (
                "compressed, within the limit",
                CompressionType::Snappy,
                vec![small.clone(), small.clone()],
                2,
                None,
            ),
            (
                "uncompressed, oversized",
                CompressionType::None,
                vec![small, large],
                2,
                None,
            ),
        ] {
            assert2::check!(
                refused_size(codec, records, read, Some(byte_size(100))) == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn no_limit_holds_a_compressed_batch_to_nothing() {
        assert2::check!(refused_size(CompressionType::Gzip, vec![record(1_000)], 1, None) == None);
    }
}
