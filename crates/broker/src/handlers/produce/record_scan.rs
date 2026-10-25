//! Structural validation for uncompressed Produce records, headers included.
//!
//! The broker needs timestamps and key presence, not a materialized borrowed
//! record. The scan below walks a record body in place and allocates nothing,
//! where krabka-protocol's walk builds a `Vec` of headers per record and a
//! label per header. It accepts and rejects exactly what that walk does; a
//! rejected batch, and one whose records `accept` declines, returns to the
//! protocol parser, which keeps ownership of error reporting.

use {
    krabka_protocol::primitives::varint::{get_varint, get_varlong},
    std::str,
};

/// Validate every record boundary and field, returning Kafka's maximum
/// timestamp (folded from -1). `accept` sees each record's timestamp and key
/// presence, in order, and must be side-effect free: a later record may
/// require the caller to validate the whole batch normally.
///
/// `Some` exactly when krabka-protocol's `validate_records_with` would accept
/// the same uncompressed body and `accept` takes every record.
pub(super) fn uncompressed_max_timestamp(
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
        let _offset_delta = read_varint(&mut fields)?;
        let key_length = read_varint(&mut fields)?;
        skip_nullable(&mut fields, key_length)?;
        let value_length = read_varint(&mut fields)?;
        skip_nullable(&mut fields, value_length)?;
        skip_headers(&mut fields)?;
        if !fields.is_empty() || !accept(timestamp, key_length >= 0) {
            return None;
        }
        maximum = maximum.max(timestamp);
    }
    body.is_empty().then_some(maximum)
}

/// Skip a record's header count and headers the way the protocol walk parses
/// them: a negative count is refused, a header key is never null and must be
/// UTF-8, and a header value is nullable.
#[inline]
fn skip_headers(fields: &mut &[u8]) -> Option<()> {
    // Most records carry no headers, encoded as the single byte zero.
    if let [0, rest @ ..] = *fields {
        *fields = rest;
        return Some(());
    }
    let count = usize::try_from(read_varint(fields)?).ok()?;
    // A header needs a byte for its key length and one for its value length,
    // so a larger count cannot fit and the protocol walk runs out of bytes.
    if count > fields.len() / 2 {
        return None;
    }
    for _ in 0..count {
        let key_length = usize::try_from(read_varint(fields)?).ok()?;
        let (key, rest) = fields.split_at_checked(key_length)?;
        // ASCII is UTF-8; the word-at-a-time check settles the common key.
        if !key.is_ascii() {
            str::from_utf8(key).ok()?;
        }
        *fields = rest;
        let value_length = read_varint(fields)?;
        skip_nullable(fields, value_length)?;
    }
    Some(())
}

/// Most deltas and lengths occupy one or two bytes. Keep those reads small
/// enough to inline even when the caller also checks topic timestamp policy.
/// Longer encodings retain the protocol decoder's limits and overflow rules.
#[inline]
fn short_integer(buf: &mut &[u8]) -> Option<i16> {
    let (encoded, rest) = match *buf {
        [first, rest @ ..] if *first < 128 => (i16::from(*first), rest),
        [first, second, rest @ ..] if *second < 128 => {
            (i16::from(*first & 127) | (i16::from(*second) << 7), rest)
        }
        _ => return None,
    };
    *buf = rest;
    Some((encoded >> 1) ^ -(encoded & 1))
}

#[inline]
fn read_varlong(buf: &mut &[u8]) -> Option<i64> {
    short_integer(buf)
        .map(i64::from)
        .or_else(|| get_varlong(buf).ok())
}

#[inline]
fn read_varint(buf: &mut &[u8]) -> Option<i32> {
    short_integer(buf)
        .map(i32::from)
        .or_else(|| get_varint(buf).ok())
}

/// The protocol decoder treats every negative nullable length as null.
fn skip_nullable(fields: &mut &[u8], length: i32) -> Option<()> {
    if let Ok(length) = usize::try_from(length) {
        *fields = fields.get(length..)?;
    }
    Some(())
}

#[cfg(test)]
mod tests;
