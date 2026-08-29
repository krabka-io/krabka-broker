//! The sample record sets both halves of the codec's tests round-trip.
//!
//! `message_set_roundtrips` encodes them and `rejects_nested_compression`
//! decodes a wrapper built from them, so the fixtures sit beside the two
//! modules rather than inside either one.

use bytes::Bytes;
use krabka_ids::Offset;

use super::ParsedRecord;

pub(super) fn sample_records_v1() -> Vec<ParsedRecord> {
    vec![
        ParsedRecord {
            offset: Offset(100),
            timestamp: Some(1_700_000_000),
            key: Some(Bytes::from_static(b"a")),
            value: Some(Bytes::from_static(b"1")),
        },
        ParsedRecord {
            offset: Offset(101),
            timestamp: Some(1_700_000_010),
            key: Some(Bytes::from_static(b"b")),
            value: Some(Bytes::from_static(b"2")),
        },
        ParsedRecord {
            offset: Offset(102),
            timestamp: Some(1_700_000_020),
            key: None,
            value: Some(Bytes::from_static(b"3")),
        },
    ]
}

pub(super) fn sample_records_v0() -> Vec<ParsedRecord> {
    sample_records_v1()
        .into_iter()
        .map(|r| ParsedRecord {
            timestamp: None,
            ..r
        })
        .collect()
}
