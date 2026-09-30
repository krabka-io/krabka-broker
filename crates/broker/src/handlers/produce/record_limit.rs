//! Kafka trunk's `max.decompressed.message.bytes` on the Produce path.
//!
//! `LogValidator` hands the topic's limit to the batch iterator that
//! decompresses a produced batch, and `DefaultRecord.readFrom` refuses a
//! record whose declared body is larger before it allocates the body. The
//! `InvalidRecordException` reaches the client as `INVALID_RECORD`, with no
//! per-record errors and no message, and nothing is appended. Only a
//! compressed batch is iterated that way: an uncompressed one is bounded by
//! `max.message.bytes` alone, and so is a record that the producer sent
//! uncompressed however large it is.
//!
//! krabka has already decompressed the batch by the time this runs
//! ([`super::prepare::prepare_batch`] parses every record of it under the
//! broker-wide `RecordDecompressionPolicy`), so the check is not an allocation
//! guard here. It is the limit the operator set, answered the way Kafka
//! answers it. A verbatim batch is walked a second time to read the record
//! sizes; that second decompression is paid only by a compressed batch on a
//! topic whose limit is below Kafka's default, which is a limit an operator
//! chose on purpose. ponytail: threading the limit into
//! `ValidatedBatch::validate_records_with` would make it one pass, at the cost
//! of a change in krabka-protocol.

use krabka_compression::{CompressionType, RecordDecompressionPolicy};
use krabka_protocol::{
    primitives::varint::{varint_len, varlong_len},
    records::validate_one_v2_batch,
};

use super::prepare::{PreparedBatch, PreparedSource};

/// The size of one record's body as Kafka's `DefaultRecord` writes it, which
/// is the `sizeOfBodyInBytes` its varint length prefix declares: the
/// attributes byte, the two deltas, and the length-prefixed key, value and
/// headers. `None` lengths are Kafka's `-1` null marker.
fn record_body_len(
    (timestamp_delta, offset_delta): (i64, i32),
    key: Option<usize>,
    value: Option<usize>,
    headers: impl ExactSizeIterator<Item = (usize, Option<usize>)>,
) -> usize {
    let field = |len: Option<usize>| match len {
        None => varint_len(-1),
        Some(len) => varint_len(i32::try_from(len).unwrap_or(i32::MAX)) + len,
    };
    let header_count = varint_len(i32::try_from(headers.len()).unwrap_or(i32::MAX));
    let headers: usize = headers
        .map(|(key, value)| field(Some(key)) + field(value))
        .sum();
    1 + varlong_len(timestamp_delta)
        + varint_len(offset_delta)
        + field(key)
        + field(value)
        + header_count
        + headers
}

/// Whether `prepared` breaks the topic's `max.decompressed.message.bytes`,
/// which `(image, node, topic)` resolve: a compressed batch with a record whose
/// body is larger. Kafka 4.3.1 has no such limit, so a broker not serving
/// trunk's keys (`unstable`) never refuses on it, and the lookup is skipped for
/// a batch the producer did not compress.
pub(super) fn exceeds_decompressed_limit(
    prepared: &PreparedBatch,
    (image, node, topic): (
        &krabka_metadata::MetadataImage,
        krabka_metadata::NodeId,
        &str,
    ),
    unstable: crate::api_catalog::UnstableApiVersions,
    policy: RecordDecompressionPolicy,
) -> bool {
    unstable == crate::api_catalog::UnstableApiVersions::Enabled
        && prepared.attributes.compression() != CompressionType::None
        && crate::config_keys::resolve_max_decompressed_record_bytes(image, node, topic)
            .is_some_and(|limit| has_oversized_record(prepared, limit, policy))
}

/// Whether `prepared` is a compressed batch with a record whose body is larger
/// than `limit` bytes.
///
/// A batch the producer did not compress never has one: Kafka's iterator reads
/// it in place and bounds it by `max.message.bytes` alone. `policy` is the
/// decompression bound the verbatim walk runs under; a batch that fails it
/// here already failed [`super::prepare::prepare_batch`], so a failure reads as
/// "not oversized" and leaves the verdict to the paths that own it.
fn has_oversized_record(
    prepared: &PreparedBatch,
    limit: usize,
    policy: RecordDecompressionPolicy,
) -> bool {
    if prepared.attributes.compression() == CompressionType::None {
        return false;
    }
    match &prepared.source {
        PreparedSource::Owned(batch) => batch.records.iter().any(|record| {
            record_body_len(
                (record.timestamp_delta, record.offset_delta),
                record.key.as_ref().map(bytes::Bytes::len),
                record.value.as_ref().map(bytes::Bytes::len),
                record.headers.iter().map(|header| {
                    (
                        header.key.len(),
                        header.value.as_ref().map(bytes::Bytes::len),
                    )
                }),
            ) > limit
        }),
        PreparedSource::Verbatim(bytes) => {
            let Ok(batch) = validate_one_v2_batch(bytes) else {
                return false;
            };
            let mut oversized = false;
            let walked = batch.validate_records_with(policy, |record| {
                oversized |= record_body_len(
                    (record.timestamp_delta, record.offset_delta),
                    record.key.map(<[u8]>::len),
                    record.value.map(<[u8]>::len),
                    record
                        .headers
                        .iter()
                        .map(|header| (header.key.len(), header.value.map(<[u8]>::len))),
                ) > limit;
            });
            walked.is_ok() && oversized
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::{Bytes, BytesMut};
    use krabka_protocol::records::{Attributes, Record, RecordBatch, RecordHeader};

    use super::*;

    /// A batch of one record with a `value_len`-byte value and a header, encoded
    /// under `codec`.
    fn batch(codec: CompressionType, value_len: usize) -> RecordBatch {
        RecordBatch {
            attributes: Attributes::default().with_compression(codec),
            last_offset_delta: 0,
            records: vec![Record {
                timestamp_delta: 300,
                key: Some(Bytes::from_static(b"key")),
                value: Some(Bytes::from(vec![7_u8; value_len])),
                headers: vec![RecordHeader {
                    key: "h".into(),
                    value: None,
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn prepared(batch: &RecordBatch, verbatim: bool) -> PreparedBatch {
        let source = if verbatim {
            let mut buf = BytesMut::new();
            batch.encode(&mut buf).expect("encode");
            PreparedSource::Verbatim(buf.freeze())
        } else {
            PreparedSource::Owned(batch.clone())
        };
        PreparedBatch {
            attributes: batch.attributes,
            last_offset_delta: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            keyless_records: Vec::new(),
            invalid_timestamp_records: Vec::new(),
            source,
        }
    }

    /// The record's body size, 114 bytes: its attributes byte, a two-byte
    /// timestamp delta, a one-byte offset delta, `1 + 3` bytes of key,
    /// `2 + 100` of value, a one-byte header count, and one header of `1 + 1`
    /// bytes of key with a null value. Its encoding is that behind a two-byte
    /// length prefix, which is what the record's own `encoded_len` says.
    #[test]
    fn a_record_body_is_measured_the_way_kafka_encodes_it() {
        let record = &batch(CompressionType::None, 100).records[0];
        check!(record.encoded_len() == 2 + 114);
        check!(
            record_body_len(
                (record.timestamp_delta, record.offset_delta),
                record.key.as_ref().map(Bytes::len),
                record.value.as_ref().map(Bytes::len),
                record
                    .headers
                    .iter()
                    .map(|header| (header.key.len(), header.value.as_ref().map(Bytes::len))),
            ) == 114
        );
    }

    /// The limit refuses a compressed record above it, on the verbatim and the
    /// owned path alike, and never an uncompressed one.
    #[test]
    fn only_a_compressed_batch_is_held_to_the_limit() {
        let policy = RecordDecompressionPolicy::default();
        // The record body is 114 bytes with a 100-byte value.
        for (codec, verbatim, limit, expected) in [
            (CompressionType::Gzip, true, 113, true),
            (CompressionType::Gzip, true, 114, false),
            (CompressionType::Gzip, false, 113, true),
            (CompressionType::Gzip, false, 114, false),
            (CompressionType::Zstd, true, 50, true),
            (CompressionType::None, true, 50, false),
            (CompressionType::None, false, 50, false),
        ] {
            check!(
                has_oversized_record(&prepared(&batch(codec, 100), verbatim), limit, policy)
                    == expected,
                "{codec:?} verbatim={verbatim} limit={limit}"
            );
        }
    }
}
