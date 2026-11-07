//! Encoding a v0/v1 `MessageSet`, flat or wrapped in a single compressed
//! outer message.
//!
//! The two entry points are separate because the wire layouts are: a flat set
//! writes one outer message per record, while a compressed set writes exactly
//! one outer message whose value is a complete inner `MessageSet`. The
//! wrapper's own offset and timestamp, and whether the inner offsets are
//! absolute or relative, are the KIP-32 conventions that differ between v0
//! and v1.

use {
    super::ParsedRecord,
    crate::{
        error::LegacyRecordsError,
        message::{Magic, Message, attrs_with_compression, wire_len},
    },
    bytes::{BufMut, BytesMut},
    krabka_compression::CompressionType,
};

fn message_for_record(record: ParsedRecord, magic: Magic) -> Message {
    Message {
        magic,
        attributes: 0,
        timestamp: match magic {
            Magic::V0 => None,
            Magic::V1 => Some(record.timestamp.unwrap_or(-1)),
        },
        key: record.key,
        value: record.value,
    }
}

/// Write one `MessageSet` entry: the offset, the message size, then the
/// message.
///
/// # Errors
/// Returns `Malformed`, and writes nothing, when the message is longer than
/// its `int32` size field can say.
fn put_framed<B: BufMut>(
    buf: &mut B,
    offset: i64,
    message: &Message,
) -> Result<(), LegacyRecordsError> {
    let size = wire_len(message.encoded_len())?;
    buf.put_i64(offset);
    buf.put_i32(size);
    // A message whose size fits has a key and a value that fit, so this
    // cannot fail after the offset and size are written.
    message.encode_into(buf)
}

/// Encode a flat `MessageSet` of magic `magic` into `buf`.
///
/// This function writes one outer message per record. Use it to emit an
/// uncompressed batch.
///
/// # Errors
/// Returns `Malformed` when a record's message is longer than its `int32`
/// size field can say. `buf` then holds the entries before that record, and
/// the caller discards it.
pub fn encode_flat_message_set<B: BufMut, I: IntoIterator<Item = ParsedRecord>>(
    records: I,
    magic: Magic,
    buf: &mut B,
) -> Result<(), LegacyRecordsError> {
    for r in records {
        let offset = r.offset.0;
        put_framed(buf, offset, &message_for_record(r, magic))?;
    }
    Ok(())
}

/// Encode a `MessageSet` wrapped in a single compressed outer message.
///
/// The inner set is uncompressed and contains one message per record. It
/// follows the KIP-32 conventions: v1 inner offsets are relative to the
/// first record, preserving gaps left by compaction.
/// # Errors
/// Returns `Malformed` if `codec` is `CompressionType::None`, whose set
/// [`encode_flat_message_set`] writes.
/// Returns `Malformed` if `codec` is `CompressionType::Zstd`, which v0/v1 cannot represent.
/// Returns `Malformed` if a message is longer than its `int32` size field can say.
/// Returns `Malformed` if a v1 relative offset exceeds `i64`.
/// Returns a compression error if the codec cannot compress the inner set.
pub fn encode_compressed_message_set<B: BufMut>(
    records: &[ParsedRecord],
    magic: Magic,
    codec: CompressionType,
    buf: &mut B,
) -> Result<(), LegacyRecordsError> {
    if codec == CompressionType::None {
        return Err(LegacyRecordsError::Malformed(
            "an uncompressed set has no wrapper; use encode_flat_message_set".into(),
        ));
    }
    if matches!(codec, CompressionType::Zstd) {
        return Err(LegacyRecordsError::Malformed(
            "zstd compression not representable in v0/v1".into(),
        ));
    }
    let (Some(first), Some(last)) = (records.first(), records.last()) else {
        // Nothing to wrap. Encode as a zero-message wrapper would be
        // ambiguous; emit nothing instead.
        return Ok(());
    };

    // Build inner uncompressed MessageSet.
    let mut inner = BytesMut::new();
    let base_offset = first.offset.0;
    for r in records {
        let inner_offset = match magic {
            Magic::V0 => r.offset.0,
            // KIP-32 preserves the original distance between retained records.
            Magic::V1 => r.offset.0.checked_sub(base_offset).ok_or_else(|| {
                LegacyRecordsError::Malformed("relative inner offset exceeds i64".into())
            })?,
        };
        put_framed(
            &mut inner,
            inner_offset,
            &message_for_record(r.clone(), magic),
        )?;
    }

    // Compress.
    let compressed = krabka_compression::compress(codec, &inner)?;

    // Wrapper message.
    let wrapper_attributes = attrs_with_compression(0, codec);
    let wrapper_timestamp = match magic {
        Magic::V0 => None,
        Magic::V1 => Some(
            records
                .iter()
                .filter_map(|r| r.timestamp)
                .max()
                .unwrap_or(-1),
        ),
    };
    let wrapper = Message {
        magic,
        attributes: wrapper_attributes,
        timestamp: wrapper_timestamp,
        key: None,
        value: Some(compressed),
    };

    // The wrapper carries the absolute offset of its last record in both
    // magics, as Kafka's `MemoryRecordsBuilder` writes it.
    put_framed(buf, last.offset.0, &wrapper)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::set::{
            decode_message_set,
            test_support::{compressed_bytes, decode_bytes, sample_records_v0, sample_records_v1},
        },
        bytes::{Buf, Bytes},
        krabka_ids::Offset,
    };

    #[test]
    fn compressed_v1_writes_relative_sparse_offsets() {
        let mut records = sample_records_v1();
        records[0].offset = Offset(100);
        records[1].offset = Offset(102);
        records[2].offset = Offset(105);
        let wire = compressed_bytes(&records, Magic::V1, CompressionType::Gzip);
        let mut cur = &wire[..];
        assert2::assert!(cur.get_i64() == 105);
        let size = usize::try_from(cur.get_i32()).unwrap();
        let wrapper = Message::decode_from(&mut cur, size).unwrap();
        let inner = krabka_compression::decompress(
            CompressionType::Gzip,
            &wrapper.value.unwrap(),
            krabka_units::mebibytes(1),
        )
        .unwrap();
        let decoded = decode_bytes(&inner);
        let mut expected = records.clone();
        for (record, offset) in expected.iter_mut().zip([0, 2, 5]) {
            record.offset = Offset(offset);
        }
        assert2::assert!(decoded == expected);
        assert2::assert!(decode_bytes(&wire) == records);
    }

    #[test]
    fn compressed_refuses_an_uncompressed_codec() {
        let mut wire = BytesMut::new();
        assert2::assert!(matches!(
            encode_compressed_message_set(
                &sample_records_v1(),
                Magic::V1,
                CompressionType::None,
                &mut wire
            ),
            Err(LegacyRecordsError::Malformed(_))
        ));
        assert2::assert!(wire.is_empty());
    }

    /// A message longer than its `int32` size field is refused rather than
    /// framed with a saturated size that disagrees with its bytes. The value
    /// is zeroed memory the encoder never touches, so the test stays cheap.
    #[yare::parameterized(
        value_fits_but_message_does_not = { usize::try_from(i32::MAX).unwrap() },
        value_does_not_fit = { usize::try_from(i32::MAX).unwrap() + 1 },
    )]
    fn flat_refuses_a_message_its_size_field_cannot_say(value_len: usize) {
        let record = ParsedRecord {
            offset: Offset(0),
            timestamp: None,
            key: None,
            value: Some(Bytes::from(vec![0u8; value_len])),
        };
        let mut wire = BytesMut::new();
        assert2::assert!(matches!(
            encode_flat_message_set([record], Magic::V0, &mut wire),
            Err(LegacyRecordsError::Malformed(_))
        ));
        assert2::assert!(wire.is_empty());
    }

    #[test]
    fn compressed_v1_rejects_unrepresentable_relative_span() {
        let mut records = sample_records_v1();
        records[0].offset = Offset(i64::MIN);
        records[1].offset = Offset(0);
        records[2].offset = Offset(i64::MAX);
        let mut wire = BytesMut::new();
        assert2::assert!(matches!(
            encode_compressed_message_set(&records, Magic::V1, CompressionType::Gzip, &mut wire),
            Err(LegacyRecordsError::Malformed(_))
        ));
        assert2::assert!(wire.is_empty());
    }

    #[yare::parameterized(
        flat_v0 = { Magic::V0, None },
        flat_v1 = { Magic::V1, None },
        gzip_v0 = { Magic::V0, Some(CompressionType::Gzip) },
        gzip_v1 = { Magic::V1, Some(CompressionType::Gzip) },
        snappy_v1 = { Magic::V1, Some(CompressionType::Snappy) },
    )]
    fn message_set_roundtrips(magic: Magic, codec: Option<CompressionType>) {
        let records = match magic {
            Magic::V0 => sample_records_v0(),
            Magic::V1 => sample_records_v1(),
        };
        let mut buffer = BytesMut::new();
        if let Some(codec) = codec {
            encode_compressed_message_set(&records, magic, codec, &mut buffer).unwrap();
        } else {
            encode_flat_message_set(records.clone(), magic, &mut buffer).unwrap();
        }

        let decoded = decode_message_set(&mut &buffer[..], buffer.len()).unwrap();
        assert2::assert!(decoded == records);
    }

    #[test]
    fn flat_v1_missing_timestamp_encodes_minus_one() {
        let recs = vec![ParsedRecord {
            offset: Offset(7),
            timestamp: None,
            key: None,
            value: Some(Bytes::from_static(b"v")),
        }];
        let mut buf = BytesMut::new();
        encode_flat_message_set(recs, Magic::V1, &mut buf).unwrap();
        let decoded = decode_bytes(&buf);
        assert2::assert!(
            decoded
                == vec![ParsedRecord {
                    offset: Offset(7),
                    timestamp: Some(-1),
                    key: None,
                    value: Some(Bytes::from_static(b"v")),
                }]
        );
    }

    #[test]
    fn compressed_v1_missing_timestamps_default_to_minus_one() {
        // Records with no timestamps: inner messages encode ts = -1, and the
        // wrapper's own timestamp (max over records, none present) is -1.
        let recs = vec![ParsedRecord {
            offset: Offset(9),
            timestamp: None,
            key: None,
            value: Some(Bytes::from_static(b"v")),
        }];
        let buf = compressed_bytes(&recs, Magic::V1, CompressionType::Gzip);

        // Inspect the raw wrapper message's own timestamp before unwrapping.
        let mut cur: &[u8] = &buf[..];
        let _wrapper_offset = cur.get_i64();
        let wrapper_size = usize::try_from(cur.get_i32()).unwrap();
        let wrapper = Message::decode_from(&mut cur, wrapper_size).unwrap();
        // The inner record's timestamp survives the unwrap as -1.
        let decoded = decode_bytes(&buf);
        assert2::assert!(wrapper.timestamp == Some(-1));
        assert2::assert!(decoded[0].timestamp == Some(-1));
    }
}
