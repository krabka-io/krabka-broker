//! Structural validation for headerless, uncompressed Produce records.
//!
//! The broker needs timestamps and key presence, not a materialized borrowed
//! record. Any unsupported shape or failed check returns to the protocol
//! parser, which retains ownership of error reporting and header validation.

use krabka_protocol::primitives::varint::{get_varint, get_varlong};

/// Validate every record boundary and field, returning Kafka's maximum
/// timestamp (folded from -1). `accept` must be side-effect free: a later
/// record may require the caller to validate the whole batch normally.
pub(super) fn headerless_max_timestamp(
    mut body: &[u8],
    count: i32,
    base_timestamp: i64,
    mut accept: impl FnMut(i64, bool) -> bool,
) -> Option<i64> {
    let count = usize::try_from(count).ok()?;
    // Even a null key and value need seven bytes including the record length.
    if count > body.len() / 7 {
        return None;
    }
    let mut maximum = -1;
    for _ in 0..count {
        let length = usize::try_from(read_varlong(&mut body)?).ok()?;
        let (record, rest) = body.split_at_checked(length)?;
        body = rest;
        let (_, mut fields) = record.split_first()?; // attributes
        let timestamp = base_timestamp.wrapping_add(read_varlong(&mut fields)?);
        let _offset_delta = get_varint(&mut fields).ok()?;
        let key_length = get_varint(&mut fields).ok()?;
        skip_nullable(&mut fields, key_length)?;
        let value_length = get_varint(&mut fields).ok()?;
        skip_nullable(&mut fields, value_length)?;
        // Header keys require UTF-8 validation. Headers and noncanonical
        // encodings of zero use the protocol parser; the common zero byte
        // must be the entire remainder of this record.
        if fields != [0] || !accept(timestamp, key_length >= 0) {
            return None;
        }
        maximum = maximum.max(timestamp);
    }
    body.is_empty().then_some(maximum)
}

/// Most deltas and lengths occupy one or two bytes. Keep those reads small
/// enough to inline even when the caller also checks topic timestamp policy.
/// Longer encodings retain the protocol decoder's limits and overflow rules.
#[inline]
fn short_integer(buf: &mut &[u8]) -> Option<i16> {
    let first = *buf.first()?;
    let (encoded, length) = if first < 128 {
        (i16::from(first), 1)
    } else {
        let second = *buf.get(1)?;
        if second >= 128 {
            return None;
        }
        (i16::from(first & 127) | (i16::from(second) << 7), 2)
    };
    *buf = &buf[length..];
    Some((encoded >> 1) ^ -(encoded & 1))
}

#[inline]
fn read_varlong(buf: &mut &[u8]) -> Option<i64> {
    short_integer(buf)
        .map(i64::from)
        .or_else(|| get_varlong(buf).ok())
}

/// The protocol decoder treats every negative nullable length as null.
fn skip_nullable(fields: &mut &[u8], length: i32) -> Option<()> {
    if let Ok(length) = usize::try_from(length) {
        *fields = fields.get(length..)?;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use {
        super::{headerless_max_timestamp, read_varlong},
        krabka_protocol::records::{HEADER_LEN, Record, RecordBatch, validate_one_v2_batch},
        proptest::prelude::*,
    };

    fn compare(body: &[u8], count: i32, base: i64) {
        let fast = headerless_max_timestamp(body, count, base, |_, _| true);
        let mut bytes = Vec::new();
        RecordBatch {
            base_timestamp: base,
            ..RecordBatch::default()
        }
        .encode(&mut bytes)
        .unwrap();
        bytes.truncate(HEADER_LEN);
        bytes.extend_from_slice(body);
        let length = i32::try_from(bytes.len() - 12).unwrap();
        bytes[8..12].copy_from_slice(&length.to_be_bytes());
        bytes[57..61].copy_from_slice(&count.to_be_bytes());
        let crc = crc32c::crc32c(&bytes[21..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        let batch = validate_one_v2_batch(&bytes).unwrap();
        let mut maximum = -1;
        let reference = batch.validate_records_with(Default::default(), |record| {
            maximum = maximum.max(base.wrapping_add(record.timestamp_delta));
        });
        if let Some(found) = fast {
            assert2::assert!(reference.is_ok());
            assert2::assert!(found == maximum);
        }
    }

    #[test]
    fn short_varlongs_match_the_protocol_decoder_and_consumed_bytes() {
        for encoded in 0..=u16::MAX {
            let bytes = encoded.to_be_bytes();
            for length in 0..=2 {
                let mut actual = &bytes[..length];
                let mut expected = actual;
                let want = krabka_protocol::primitives::varint::get_varlong(&mut expected).ok();
                assert2::assert!(read_varlong(&mut actual) == want);
                assert2::assert!(actual == expected);
            }
        }
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_never_bypass_the_protocol_validator(
            body in prop::collection::vec(any::<u8>(), 0..256),
            count in -1_i32..40,
            base in any::<i64>(),
        ) {
            compare(&body, count, base);
        }

        #[test]
        fn valid_records_and_mutations_match_the_protocol_validator(
            records in prop::collection::vec((any::<i64>(), any::<i32>(), prop::option::of(prop::collection::vec(any::<u8>(), 0..20)), prop::option::of(prop::collection::vec(any::<u8>(), 0..200))), 1..30),
            base in any::<i64>(),
            mutation in any::<usize>(),
            replacement in any::<u8>(),
        ) {
            let count = i32::try_from(records.len()).unwrap();
            let expected = records.iter().fold(-1_i64, |maximum, r| maximum.max(base.wrapping_add(r.0)));
            let batch = RecordBatch {
                base_timestamp: base,
                records: records.into_iter().map(|(timestamp_delta, offset_delta, key, value)| Record {
                    timestamp_delta, offset_delta, key: key.map(Into::into), value: value.map(Into::into), ..Record::default()
                }).collect(),
                ..RecordBatch::default()
            };
            let mut bytes = Vec::new();
            batch.encode(&mut bytes).unwrap();
            let body = &mut bytes[HEADER_LEN..];
            assert2::assert!(headerless_max_timestamp(body, count, base, |_, _| true) == Some(expected));
            compare(body, count, base);
            let at = mutation % body.len();
            body[at] = replacement;
            compare(body, count, base);
            compare(&body[..at], count, base);
            compare(body, count + 1, base);
            compare(body, count - 1, base);
        }
    }
}
