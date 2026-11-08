//! Helpers that down-convert v2 `RecordBatch` values to v0 or v1
//! `MessageSet` bytes for a requester on Fetch v<4.
//!
//! The helpers drop control batches, that is txn markers, entirely. They refuse
//! a zstd-compressed batch, because v0 and v1 cannot carry zstd: as in Kafka's
//! `RecordsUtil.downConvert`, the batch fails with `UNSUPPORTED_COMPRESSION_TYPE`
//! and is not re-compressed with another codec. When batches before it
//! converted, the partition answers with those batches alone, as Kafka's lazy
//! down-conversion stops at the zstd batch. The consumer then fetches from the
//! zstd batch's offset and gets the error.

use {
    crate::codes,
    bytes::BytesMut,
    krabka_protocol::records::{RecordBatch, RecordsPayload},
    krabka_records_legacy::{LegacyRecordsError, Magic, v2_to_legacy},
    std::borrow::Cow,
};

/// Builds the records-field payload for one batch.
///
/// It returns `Ok(None)` when it drops the whole batch, which happens for a
/// control batch on the legacy path. It returns `Err(error_code)` for a
/// down-conversion failure that the caller should report as a per-partition
/// error: `UNSUPPORTED_COMPRESSION_TYPE` for a zstd batch, which v0 and v1
/// cannot carry, and `CORRUPT_MESSAGE` for any other failure.
pub fn down_convert_for_fetch(
    batch: &RecordBatch,
    request_version: i16,
) -> Result<Option<RecordsPayload>, i16> {
    if request_version >= 4 {
        return Ok(Some(RecordsPayload::V2(vec![batch.clone()])));
    }
    if batch.attributes.is_control_batch() {
        return Ok(None);
    }
    // Fetch v0-1 → Magic::V0 (no per-message timestamps)
    // Fetch v2-3 → Magic::V1 (KIP-32 timestamps)
    let target = if request_version >= 2 {
        Magic::V1
    } else {
        Magic::V0
    };
    let bytes = v2_to_legacy(batch, target).map_err(|e| match e {
        // Kafka traces this case rather than warning: it is a property of the
        // data and the client's version, not a broker fault.
        LegacyRecordsError::UnsupportedCompression { codec } => {
            tracing::trace!(?codec, "batch codec cannot be down-converted for fetch");
            codes::UNSUPPORTED_COMPRESSION_TYPE
        }
        e => {
            tracing::warn!(error = %e, "v2_to_legacy failed during fetch down-conversion");
            codes::CORRUPT_MESSAGE
        }
    })?;
    Ok(Some(RecordsPayload::Legacy(bytes)))
}

/// Down-converts a whole records-field payload for a Fetch v<4 requester.
///
/// The function gets the batch list, down-converts each batch it keeps, and
/// concatenates the resulting legacy `MessageSet` bytes. It decodes `Raw`
/// here, the only place that parses `Raw`, and only for legacy clients. It
/// returns `Ok(None)` when it dropped every batch, for example when they are
/// all control batches.
///
/// A batch that fails with `UNSUPPORTED_COMPRESSION_TYPE` ends the payload.
/// When batches before it converted, the function returns those batches, as
/// Kafka's `LazyDownConversionRecords` sends the chunks it converted before
/// the zstd batch and pads the rest with an overflow batch that the consumer
/// ignores. The consumer's next fetch starts at the zstd batch, which then
/// fails as the first batch. Only a zstd batch with nothing converted before it
/// turns into the partition's error code.
pub fn down_convert_payload_for_fetch(
    payload: &RecordsPayload,
    request_version: i16,
) -> Result<Option<RecordsPayload>, i16> {
    let batches: Cow<'_, [RecordBatch]> = krabka_macros::records_payload_match! { match payload {
        RecordsPayload::V2(b) => Cow::Borrowed(b),
        RecordsPayload::Raw(bytes) => match RecordsPayload::from_bytes(bytes.clone()) {
            Ok(RecordsPayload::V2(b)) => Cow::Owned(b),
            _ => return Err(codes::CORRUPT_MESSAGE),
        },
        RecordsPayload::Legacy(_) => return Ok(Some(payload.clone())),
        // Unreachable: the zero-copy `FileRegions` payload is only emitted for
        // Fetch v4+, which never down-converts (down-conversion is a v0–v3 path).
        RecordsPayload::FileRegions(_) => return Err(codes::CORRUPT_MESSAGE),
    } };

    let mut out = BytesMut::new();
    for batch in batches.iter() {
        match down_convert_for_fetch(batch, request_version) {
            Ok(Some(RecordsPayload::Legacy(b))) => out.extend_from_slice(&b),
            Ok(Some(_)) => return Err(codes::CORRUPT_MESSAGE),
            Ok(None) => {}
            Err(codes::UNSUPPORTED_COMPRESSION_TYPE) if !out.is_empty() => break,
            Err(code) => return Err(code),
        }
    }
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RecordsPayload::Legacy(out.freeze())))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        bytes::Bytes,
        krabka_compression::CompressionType,
        krabka_protocol::records::{Attributes, Record, RecordBatch},
        krabka_records_legacy::{ParsedRecord, decode_message_set},
    };

    fn make_batch(codec: CompressionType, records: Vec<Record>) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default().with_compression(codec),
            last_offset_delta: i32::try_from(records.len().saturating_sub(1)).unwrap_or(i32::MAX),
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
        }
    }

    fn sample_record(key: &'static str, value: &'static str) -> Record {
        Record {
            attributes: 0,
            offset_delta: 0,
            timestamp_delta: 0,
            key: Some(Bytes::from_static(key.as_bytes())),
            value: Some(Bytes::from_static(value.as_bytes())),
            headers: vec![],
        }
    }

    fn legacy_records(batch: &RecordBatch, version: i16) -> Vec<ParsedRecord> {
        let payload = down_convert_for_fetch(batch, version)
            .unwrap()
            .expect("should have Some payload");
        let RecordsPayload::Legacy(bytes) = payload else {
            panic!("expected Legacy");
        };
        let mut cursor: &[u8] = &bytes;
        decode_message_set(&mut cursor, bytes.len()).unwrap()
    }

    #[test]
    fn version_gte_4_returns_v2_unchanged() {
        let batch = make_batch(CompressionType::None, vec![sample_record("k", "v")]);
        let result = down_convert_for_fetch(&batch, 5).unwrap();
        let payload = result.expect("should have Some payload");
        match payload {
            RecordsPayload::V2(v) => assert!(v == vec![batch]),
            _ => panic!("expected V2 for version >= 4"),
        }
    }

    /// Version 3 with a control batch returns `Ok(None)`.
    #[test]
    fn control_batch_returns_none() {
        let mut batch = make_batch(CompressionType::None, vec![sample_record("k", "v")]);
        batch.attributes = batch.attributes.with_control(true);
        let result = down_convert_for_fetch(&batch, 3).unwrap();
        assert!(
            result.is_none(),
            "control batch must be dropped on legacy path"
        );
    }

    fn zstd_records() -> Vec<Record> {
        (0..50)
            .map(|i| Record {
                attributes: 0,
                offset_delta: i,
                timestamp_delta: i64::from(i) * 1000,
                key: Some(Bytes::from(format!("key-{i}"))),
                value: Some(Bytes::from(format!("val-{i} hello world this is a test"))),
                headers: vec![],
            })
            .collect()
    }

    /// A zstd batch fails with `UNSUPPORTED_COMPRESSION_TYPE` on every legacy
    /// version, as Kafka's `RecordsUtil.downConvert` refuses it.
    #[rstest::rstest]
    #[case::fetch_v0(0)]
    #[case::fetch_v1(1)]
    #[case::fetch_v2(2)]
    #[case::fetch_v3(3)]
    fn zstd_batch_unsupported_compression(#[case] version: i16) {
        let batch = make_batch(CompressionType::Zstd, zstd_records());
        let result = down_convert_for_fetch(&batch, version);
        assert!(result == Err(codes::UNSUPPORTED_COMPRESSION_TYPE));
    }

    /// A zstd control batch is dropped before its codec is checked, as Kafka
    /// skips control batches first.
    #[test]
    fn zstd_control_batch_returns_none() {
        let mut batch = make_batch(CompressionType::Zstd, vec![sample_record("k", "v")]);
        batch.attributes = batch.attributes.with_control(true);
        assert!(down_convert_for_fetch(&batch, 3) == Ok(None));
    }

    /// A payload that starts with a zstd batch fails the partition, even when
    /// convertible batches follow it.
    #[test]
    fn payload_leading_zstd_batch_fails_partition() {
        let zstd = make_batch(CompressionType::Zstd, zstd_records());
        let plain = make_batch(CompressionType::None, vec![sample_record("a", "1")]);
        let payload = RecordsPayload::V2(vec![zstd, plain]);
        let result = down_convert_payload_for_fetch(&payload, 3);
        assert!(result == Err(codes::UNSUPPORTED_COMPRESSION_TYPE));
    }

    /// A payload with a control batch and then a zstd batch fails the
    /// partition: the dropped control batch converted nothing.
    #[test]
    fn payload_control_then_zstd_fails_partition() {
        let mut control = make_batch(CompressionType::None, vec![sample_record("c", "")]);
        control.attributes = control.attributes.with_control(true);
        let zstd = make_batch(CompressionType::Zstd, zstd_records());
        let payload = RecordsPayload::V2(vec![control, zstd]);
        let result = down_convert_payload_for_fetch(&payload, 3);
        assert!(result == Err(codes::UNSUPPORTED_COMPRESSION_TYPE));
    }

    /// A payload with an uncompressed batch and then a zstd batch returns the
    /// uncompressed batch alone and stops at the zstd batch, as Kafka's lazy
    /// down-conversion does. The consumer's next fetch starts at the zstd batch.
    #[test]
    fn payload_stops_before_zstd_batch() {
        let plain = make_batch(CompressionType::None, vec![sample_record("a", "1")]);
        let zstd = make_batch(CompressionType::Zstd, zstd_records());
        let trailing = make_batch(CompressionType::None, vec![sample_record("b", "2")]);
        let expected = down_convert_for_fetch(&plain, 3).unwrap();
        let payload = RecordsPayload::V2(vec![plain, zstd, trailing]);
        let result = down_convert_payload_for_fetch(&payload, 3);
        assert!(result == Ok(expected));
    }

    /// Version 3 with an uncompressed batch returns Legacy bytes that decode
    /// to the original records.
    #[test]
    fn uncompressed_batch_decodes_correctly() {
        let batch = make_batch(CompressionType::None, vec![sample_record("hello", "world")]);
        let recs = legacy_records(&batch, 3);
        assert!(recs.len() == 1);
        check!(recs[0].key.as_deref() == Some(b"hello".as_ref()));
        check!(recs[0].value.as_deref() == Some(b"world".as_ref()));
    }

    /// Version 0 uses `Magic::V0`, which has no timestamps.
    #[test]
    fn version_0_uses_magic_v0() {
        let mut batch = make_batch(CompressionType::None, vec![sample_record("k", "v")]);
        batch.base_timestamp = 1_700_000_000;
        batch.records[0].timestamp_delta = 500;
        let recs = legacy_records(&batch, 0);
        // v0 has no timestamps; all timestamps are None
        assert!(recs[0].timestamp == None, "v0 should have no timestamps");
    }

    /// Payload-level conversion concatenates the legacy `MessageSet` of each
    /// batch.
    #[test]
    fn payload_multi_batch_concats_legacy() {
        let b0 = make_batch(CompressionType::None, vec![sample_record("a", "1")]);
        let b1 = make_batch(CompressionType::None, vec![sample_record("b", "2")]);
        let payload = RecordsPayload::V2(vec![b0, b1]);
        let out = down_convert_payload_for_fetch(&payload, 3)
            .unwrap()
            .expect("some");
        let RecordsPayload::Legacy(bytes) = out else {
            panic!("expected Legacy");
        };
        let mut cur: &[u8] = &bytes;
        let recs = krabka_records_legacy::decode_message_set(&mut cur, bytes.len()).unwrap();
        assert!(recs.len() == 2);
    }
}
