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
//! krabka parses every record of a batch under the broker-wide
//! `RecordDecompressionPolicy` anyway, so the check is not an allocation guard
//! here. It is the limit the operator set, answered the way Kafka answers it.
//! [`super::prepare::prepare_batch_within`] applies it in the walk that
//! validates the records, so a verbatim batch is CRC-checked and decompressed
//! once, and it answers only once every other check of that function passed.

use {
    crate::{api_catalog::UnstableApiVersions, config_keys::resolve_max_decompressed_record_bytes},
    krabka_metadata::{MetadataImage, NodeId},
};

/// The topic's `max.decompressed.message.bytes`, which `(image, node, topic)`
/// resolve, or `None` when nothing limits a record. Kafka 4.3.1 has no such
/// limit, so a broker not serving trunk's keys (`unstable`) never refuses on
/// it and skips the lookup.
pub(super) fn decompressed_record_limit(
    image: &MetadataImage,
    node: NodeId,
    topic: &str,
    unstable: UnstableApiVersions,
) -> Option<usize> {
    if unstable == UnstableApiVersions::Enabled {
        resolve_max_decompressed_record_bytes(image, node, topic)
    } else {
        None
    }
}

/// Whether a record whose body is `body_len` bytes breaks `limit`: Kafka
/// refuses a body strictly larger than the limit.
pub(super) fn exceeds_record_limit(body_len: usize, limit: usize) -> bool { body_len > limit }

#[cfg(test)]
mod tests {
    use {
        super::super::{
            framing::PartitionPayload,
            prepare::{DecodeEnv, prepare_batch_within},
            test_support::encode_batch,
            topic_settings::TimestampPolicy,
        },
        crate::{codes, metrics::BrokerMetrics},
        krabka_compression::{CompressionType, RecordDecompressionPolicy},
        krabka_protocol::records::{Attributes, Record, RecordBatch},
        std::{cell::Cell, sync::Arc},
    };

    krabka_macros::record_limit_fixture!(record);

    /// A batch of one record with a `value_len`-byte value and a header, encoded
    /// under `codec`.
    fn batch(codec: CompressionType, value_len: usize) -> RecordBatch {
        RecordBatch {
            attributes: Attributes::default().with_compression(codec),
            last_offset_delta: 0,
            records: vec![record(value_len)],
            ..Default::default()
        }
    }

    /// What [`prepare_batch_within`] answers for `wire` under `limit`, and how
    /// many times it asked for the limit. `topic_compression` other than the
    /// batch's own codec sends the batch down the owned path.
    fn prepare(
        wire: &RecordBatch,
        topic_compression: Option<CompressionType>,
        version: i16,
        limit: Option<usize>,
    ) -> (Result<(), i16>, usize) {
        let metrics = BrokerMetrics::new();
        let topic: Arc<str> = Arc::from("t");
        let lookups = Cell::new(0);
        let answer = prepare_batch_within(
            PartitionPayload::Slice(encode_batch(wire)),
            topic_compression,
            TimestampPolicy::default(),
            false,
            DecodeEnv {
                topic_name: &topic,
                metrics: &metrics,
                policy: RecordDecompressionPolicy::default(),
            },
            version,
            || {
                lookups.set(lookups.get() + 1);
                limit
            },
        );
        (answer.map(|_| ()), lookups.get())
    }

    /// The limit refuses a compressed record above it, on the verbatim and the
    /// owned path alike, and never an uncompressed one, whose path does not
    /// even look the limit up.
    ///
    /// The record body is 114 bytes with a 100-byte value: its attributes
    /// byte, a two-byte timestamp delta, a one-byte offset delta, `1 + 3` bytes
    /// of key, `2 + 100` of value, a one-byte header count, and one header of
    /// `1 + 1` bytes of key with a null value. A limit of 114 admits it and 113
    /// refuses it.
    #[yare::parameterized(
        gzip_verbatim_below_limit = { CompressionType::Gzip, None, Some(113), Err(codes::INVALID_RECORD), 1 },
        gzip_verbatim_at_limit = { CompressionType::Gzip, None, Some(114), Ok(()), 1 },
        gzip_verbatim_no_limit = { CompressionType::Gzip, None, None, Ok(()), 1 },
        gzip_owned_below_limit = { CompressionType::Gzip, Some(CompressionType::Zstd), Some(113), Err(codes::INVALID_RECORD), 1 },
        gzip_owned_at_limit = { CompressionType::Gzip, Some(CompressionType::Zstd), Some(114), Ok(()), 1 },
        zstd_verbatim_below_limit = { CompressionType::Zstd, None, Some(50), Err(codes::INVALID_RECORD), 1 },
        uncompressed_verbatim = { CompressionType::None, None, Some(50), Ok(()), 0 },
        uncompressed_owned = { CompressionType::None, Some(CompressionType::Gzip), Some(50), Ok(()), 0 },
    )]
    fn only_a_compressed_batch_is_held_to_the_limit(
        codec: CompressionType,
        topic_compression: Option<CompressionType>,
        limit: Option<usize>,
        expected: Result<(), i16>,
        expected_lookups: usize,
    ) {
        let found = prepare(&batch(codec, 100), topic_compression, 13, limit);
        assert2::assert!(found == (expected, expected_lookups));
    }

    /// A batch the other checks refuse keeps their code: the limit answers
    /// last, as it did when it ran after `prepare_batch`.
    #[yare::parameterized(
        zstd_before_first_zstd_version = { CompressionType::Zstd, None, 6, Err(codes::UNSUPPORTED_COMPRESSION_TYPE), 0 },
        zstd_owned_before_first_zstd_version = { CompressionType::Zstd, Some(CompressionType::Gzip), 6, Err(codes::UNSUPPORTED_COMPRESSION_TYPE), 0 },
    )]
    fn other_refusals_come_before_the_limit(
        codec: CompressionType,
        topic_compression: Option<CompressionType>,
        version: i16,
        expected: Result<(), i16>,
        expected_lookups: usize,
    ) {
        let found = prepare(&batch(codec, 100), topic_compression, version, Some(1));
        assert2::assert!(found == (expected, expected_lookups));
    }

    /// A refused record anywhere in a batch refuses it, after the records
    /// before and after it parsed.
    #[test]
    fn a_later_oversized_record_refuses_the_batch() {
        let wire = RecordBatch {
            attributes: Attributes::default().with_compression(CompressionType::Lz4),
            last_offset_delta: 2,
            records: [1, 100, 1]
                .into_iter()
                .zip(0..)
                .map(|(value_len, offset_delta)| Record {
                    offset_delta,
                    ..record(value_len)
                })
                .collect(),
            ..Default::default()
        };
        assert2::assert!(prepare(&wire, None, 13, Some(113)) == (Err(codes::INVALID_RECORD), 1));
        assert2::assert!(prepare(&wire, None, 13, Some(114)) == (Ok(()), 1));
    }
}
