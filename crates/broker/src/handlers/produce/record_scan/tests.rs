//! The no-allocation scan against krabka-protocol's record walk: both must
//! accept and refuse the same bodies and report the same records.

use {
    super::{read_varint, read_varlong, uncompressed_max_timestamp},
    bytes::Bytes,
    krabka_compression::RecordDecompressionPolicy,
    krabka_protocol::{
        primitives::varint::{get_varint, get_varlong, put_varint, put_varlong},
        records::{HEADER_LEN, Record, RecordBatch, RecordHeader, validate_one_v2_batch},
    },
    proptest::prelude::*,
};

/// What a walk that accepted a body reports: Kafka's maximum timestamp and
/// each record's timestamp and key presence, in order.
#[derive(Debug, PartialEq, Eq)]
struct Walked {
    max_timestamp: i64,
    records: Vec<(i64, bool)>,
}

/// The fast scan's verdict, `None` when it refuses the body.
fn fast(body: &[u8], count: i32, base: i64) -> Option<Walked> {
    let mut records = Vec::new();
    uncompressed_max_timestamp(body, count, base, |timestamp, has_key| {
        records.push((timestamp, has_key));
        true
    })
    .map(|max_timestamp| Walked {
        max_timestamp,
        records,
    })
}

/// krabka-protocol's verdict on the same body behind an uncompressed v2
/// header with a valid CRC, `None` when its walk refuses the body.
fn reference(body: &[u8], count: i32, base: i64) -> Option<Walked> {
    let mut bytes = Vec::new();
    RecordBatch {
        base_timestamp: base,
        ..RecordBatch::default()
    }
    .encode_batch(&mut bytes)
    .unwrap();
    bytes.truncate(HEADER_LEN);
    bytes.extend_from_slice(body);
    let length = i32::try_from(bytes.len() - 12).unwrap();
    bytes[8..12].copy_from_slice(&length.to_be_bytes());
    bytes[57..61].copy_from_slice(&count.to_be_bytes());
    let crc = crc32c::crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
    let batch = validate_one_v2_batch(&bytes).unwrap();
    let mut records = Vec::new();
    batch
        .validate_records_with(RecordDecompressionPolicy::default(), |record| {
            records.push((
                base.wrapping_add(record.timestamp_delta),
                record.key.is_some(),
            ));
        })
        .ok()?;
    let max_timestamp = records
        .iter()
        .fold(-1, |maximum, &(timestamp, _)| timestamp.max(maximum));
    Some(Walked {
        max_timestamp,
        records,
    })
}

/// The scan and the protocol walk agree on `body`: both refuse it, or both
/// accept it and report the same records.
fn compare(body: &[u8], count: i32, base: i64) {
    assert2::assert!(fast(body, count, base) == reference(body, count, base));
}

fn varint(value: i32) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, value);
    out
}

/// A nullable field: its length, `-1` for null, then its bytes.
fn nullable(field: Option<&[u8]>) -> Vec<u8> {
    field.map_or_else(
        || varint(-1),
        |bytes| [varint(i32::try_from(bytes.len()).unwrap()), bytes.to_vec()].concat(),
    )
}

/// One header as the wire carries it, a null key included.
fn header(key: Option<&[u8]>, value: Option<&[u8]>) -> Vec<u8> {
    [nullable(key), nullable(value)].concat()
}

/// A record section of `count` declared headers followed by `headers`.
fn headers(count: i32, headers: &[Vec<u8>]) -> Vec<u8> {
    [varint(count), headers.concat()].concat()
}

/// A length-prefixed record with `key` and the value `v`, its header
/// section supplied raw.
fn record(timestamp_delta: i64, key: Option<&[u8]>, header_section: &[u8]) -> Vec<u8> {
    let mut body = vec![0];
    put_varlong(&mut body, timestamp_delta);
    put_varint(&mut body, 0);
    body.extend(nullable(key));
    body.extend(nullable(Some(b"v")));
    body.extend_from_slice(header_section);
    let mut out = Vec::new();
    put_varlong(&mut out, i64::try_from(body.len()).unwrap());
    out.extend(body);
    out
}

const BASE: i64 = 1_000;

/// Hand-built bodies with zero, one and many headers, valid and malformed:
/// the scan returns `expected` and agrees with the protocol walk.
#[yare::parameterized(
    no_headers = { record(5, Some(b"k"), &headers(0, &[])), 1, Some(BASE + 5) },
    noncanonical_zero_header_count = { record(5, Some(b"k"), &[0x80, 0x00]), 1, Some(BASE + 5) },
    one_header = { record(7, Some(b"k"), &headers(1, &[header(Some(b"trace"), Some(b"abc"))])), 1, Some(BASE + 7) },
    null_header_value = { record(7, None, &headers(1, &[header(Some(b"k"), None)])), 1, Some(BASE + 7) },
    empty_header_key_and_value = { record(7, Some(b"k"), &headers(1, &[header(Some(b""), Some(b""))])), 1, Some(BASE + 7) },
    multibyte_utf8_key = { record(7, Some(b"k"), &headers(1, &[header(Some("héllo-✓".as_bytes()), Some(b"x"))])), 1, Some(BASE + 7) },
    two_byte_key_length = { record(7, Some(b"k"), &headers(1, &[header(Some(&[b'a'; 200]), Some(&[1; 300]))])), 1, Some(BASE + 7) },
    three_byte_key_length = { record(7, Some(b"k"), &headers(1, &[header(Some(&[b'a'; 9000]), None)])), 1, Some(BASE + 7) },
    three_headers = {
        record(-3, Some(b"k"), &headers(3, &[header(Some(b"a"), Some(b"1")), header(Some(b"b"), None), header(Some(b"c"), Some(b"333"))])),
        1, Some(BASE - 3)
    },
    many_headers = {
        record(9, Some(b"k"), &headers(40, &vec![header(Some(b"key"), Some(b"value")); 40])),
        1, Some(BASE + 9)
    },
    mixed_records = {
        [
            record(1, Some(b"k"), &headers(0, &[])),
            record(30, None, &headers(2, &[header(Some(b"a"), None), header(Some(b"b"), Some(b"c"))])),
            record(2, Some(b"k"), &headers(1, &[header(Some(b"z"), Some(b""))])),
        ].concat(),
        3, Some(BASE + 30)
    },
    null_header_key = { record(5, Some(b"k"), &headers(1, &[header(None, Some(b"v"))])), 1, None },
    null_header_key_after_valid_header = {
        record(5, Some(b"k"), &headers(2, &[header(Some(b"a"), None), header(None, None)])),
        1, None
    },
    invalid_utf8_key = { record(5, Some(b"k"), &headers(1, &[header(Some(&[0xff]), None)])), 1, None },
    split_utf8_key = { record(5, Some(b"k"), &headers(1, &[header(Some(&"é".as_bytes()[..1]), None)])), 1, None },
    negative_header_count = { record(5, Some(b"k"), &headers(-1, &[])), 1, None },
    header_count_above_headers = { record(5, Some(b"k"), &headers(2, &[header(Some(b"a"), None)])), 1, None },
    header_count_below_headers = {
        record(5, Some(b"k"), &headers(1, &[header(Some(b"a"), None), header(Some(b"b"), None)])),
        1, None
    },
    huge_header_count = { record(5, Some(b"k"), &headers(i32::MAX, &[header(Some(b"a"), None)])), 1, None },
    header_key_overruns_record = { record(5, Some(b"k"), &[varint(1), varint(5), b"ab".to_vec()].concat()), 1, None },
    header_value_overruns_record = { record(5, Some(b"k"), &[varint(1), varint(1), b"k".to_vec(), varint(10), b"ab".to_vec()].concat()), 1, None },
    header_value_length_missing = { record(5, Some(b"k"), &[varint(1), varint(1), b"k".to_vec()].concat()), 1, None },
    header_count_missing = { record(5, Some(b"k"), &[]), 1, None },
    trailing_byte_after_headers = { record(5, Some(b"k"), &[headers(1, &[header(Some(b"a"), None)]), vec![0]].concat()), 1, None },
    trailing_record = { [record(5, Some(b"k"), &headers(0, &[])), record(6, Some(b"k"), &headers(0, &[]))].concat(), 1, None },
    missing_record = { record(5, Some(b"k"), &headers(1, &[header(Some(b"a"), None)])), 2, None },
    negative_record_count = { Vec::new(), -1, None },
)]
fn scan_matches_the_protocol_walk(body: Vec<u8>, count: i32, expected: Option<i64>) {
    let found = fast(&body, count, BASE);
    assert2::assert!(found.as_ref().map(|walked| walked.max_timestamp) == expected);
    assert2::assert!(found == reference(&body, count, BASE));
}

#[test]
fn short_varlongs_match_the_protocol_decoder_and_consumed_bytes() {
    for encoded in 0..=u16::MAX {
        let bytes = encoded.to_be_bytes();
        for length in 0..=2 {
            let mut actual = &bytes[..length];
            let mut expected = actual;
            let want = get_varlong(&mut expected).ok();
            assert2::assert!(read_varlong(&mut actual) == want);
            assert2::assert!(actual == expected);
        }
    }
}

#[test]
fn short_varints_match_the_protocol_decoder_and_consumed_bytes() {
    for encoded in 0..=u16::MAX {
        let bytes = encoded.to_be_bytes();
        for length in 0..=2 {
            let mut actual = &bytes[..length];
            let mut expected = actual;
            let want = get_varint(&mut expected).ok();
            assert2::assert!(read_varint(&mut actual) == want);
            assert2::assert!(actual == expected);
        }
    }
}

type ArbitraryHeader = (String, Option<Vec<u8>>);
type ArbitraryRecord = (
    i64,
    i32,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    Vec<ArbitraryHeader>,
);

fn arbitrary_record() -> impl Strategy<Value = ArbitraryRecord> {
    (
        any::<i64>(),
        any::<i32>(),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..20)),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..200)),
        prop::collection::vec(
            (
                ".{0,12}",
                prop::option::of(prop::collection::vec(any::<u8>(), 0..24)),
            ),
            0..5,
        ),
    )
}

proptest! {
    #[test]
    fn arbitrary_bytes_match_the_protocol_validator(
        body in prop::collection::vec(any::<u8>(), 0..256),
        count in -1_i32..40,
        base in any::<i64>(),
    ) {
        compare(&body, count, base);
    }

    #[test]
    fn valid_records_and_mutations_match_the_protocol_validator(
        records in prop::collection::vec(arbitrary_record(), 1..30),
        base in any::<i64>(),
        mutation in any::<usize>(),
        replacement in any::<u8>(),
    ) {
        let count = i32::try_from(records.len()).unwrap();
        let expected = Walked {
            max_timestamp: records
                .iter()
                .fold(-1_i64, |maximum, r| maximum.max(base.wrapping_add(r.0))),
            records: records
                .iter()
                .map(|r| (base.wrapping_add(r.0), r.2.is_some()))
                .collect(),
        };
        let batch = RecordBatch {
            base_timestamp: base,
            records: records
                .into_iter()
                .map(|(timestamp_delta, offset_delta, key, value, headers)| Record {
                    timestamp_delta,
                    offset_delta,
                    key: key.map(Into::into),
                    value: value.map(Into::into),
                    headers: headers
                        .into_iter()
                        .map(|(key, value)| RecordHeader { key, value: value.map(Bytes::from) })
                        .collect(),
                    ..Record::default()
                })
                .collect(),
            ..RecordBatch::default()
        };
        let mut bytes = Vec::new();
        batch.encode_batch(&mut bytes).unwrap();
        let body = &mut bytes[HEADER_LEN..];
        assert2::assert!(fast(body, count, base) == Some(expected));
        compare(body, count, base);
        let at = mutation % body.len();
        body[at] = replacement;
        compare(body, count, base);
        compare(&body[..at], count, base);
        compare(body, count + 1, base);
        compare(body, count - 1, base);
    }
}
