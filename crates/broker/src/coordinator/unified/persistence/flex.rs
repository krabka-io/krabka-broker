//! KIP-482 flexible-version leaf codecs for the `__consumer_offsets` record
//! values.
//!
//! # Which records are flexible
//!
//! The layouts came from the Apache Kafka schemas at tag `4.3.1`, under
//! `group-coordinator/src/main/resources/common/message/`. Every
//! `coordinator-key` there declares `"flexibleVersions": "none"`, and every
//! `coordinator-value` of the KIP-848, KIP-932 and KIP-1071 families declares
//! `"flexibleVersions": "0+"`. So:
//!
//! - **Keys** keep the legacy encoding: an `i16` length for a string, an `i32`
//!   count for an array, and no tagged-field trailer. They use the
//!   `super::{get_string, put_string}` helpers, not this module.
//! - **Values** of those three families are flexible at version 0: a string is
//!   a compact string (unsigned varint `len + 1`), an array is a compact array
//!   (unsigned varint `count + 1`), a null takes the `0` prefix, and the
//!   message **and every nested struct** end with an unsigned-varint
//!   tagged-field count.
//!
//! The two classic families are the exception. `OffsetCommitValue` and
//! `GroupMetadataValue` declare `"flexibleVersions": "4+"`, and the broker
//! writes versions 1, 3 and 3, so those stay on the legacy encoding in
//! [`super`].
//!
//! A dropped trailing tagged-field count is not a lenient-reader nit: Kafka's
//! generated reader consumes it unconditionally, so its absence surfaces as a
//! `BufferUnderflowException` and takes down `kafka-console-consumer` in the
//! middle of the topic.
//!
//! # What this module adds
//!
//! `krabka_protocol::primitives` already implements every leaf, and
//! `krabka_protocol::tagged_fields` implements the trailer. These wrappers only
//! bind them to the broker's `&mut &[u8]` reader and map `ProtocolError` into
//! [`BrokerError`], so a record codec reads as a list of fields.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::{
    ProtocolError,
    primitives::{array, string_bytes, uuid::Uuid, varint},
    tagged_fields::{UnknownTaggedFields, WriteTaggedFields, read_tagged_fields},
};

use crate::error::BrokerError;

fn protocol(e: ProtocolError) -> BrokerError {
    BrokerError::Protocol(e)
}

// ───────────────────────────────────────────────────────────────── writers ──

pub(crate) fn put_compact_string(buf: &mut BytesMut, s: &str) {
    string_bytes::put_compact_string(buf, s);
}

pub(crate) fn put_compact_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    string_bytes::put_compact_nullable_string(buf, s);
}

pub(crate) fn put_compact_bytes(buf: &mut BytesMut, b: &[u8]) {
    string_bytes::put_compact_bytes(buf, b);
}

/// Writes the compact `count + 1` prefix of a non-null array.
pub(crate) fn put_compact_array_len(buf: &mut BytesMut, n: usize) {
    array::put_array_len(buf, n, true);
}

/// Writes a `uuid` field: sixteen raw bytes, with no length prefix.
pub(crate) fn put_uuid(buf: &mut BytesMut, bytes: [u8; 16]) {
    krabka_protocol::primitives::uuid::put_uuid(buf, Uuid(bytes));
}

/// Writes the tagged-field trailer of a message or a nested struct that sets no
/// tag: a single `0` byte.
pub(crate) fn put_empty_tagged_fields(buf: &mut BytesMut) {
    varint::put_uvarint(buf, 0);
}

/// Writes a tagged-field trailer that carries `entries`, each a `(tag,
/// payload)` pair. [`WriteTaggedFields`] sorts them, which the wire format
/// requires.
pub(crate) fn put_tagged_fields(buf: &mut BytesMut, entries: Vec<(u32, Bytes)>) {
    let mut w = WriteTaggedFields::new();
    for (tag, payload) in entries {
        w.add(tag, payload);
    }
    w.write(buf, &UnknownTaggedFields::default());
}

// ───────────────────────────────────────────────────────────────── readers ──

/// Reads a fixed-width `int8`.
pub(crate) fn get_i8(buf: &mut &[u8]) -> Result<i8, BrokerError> {
    if buf.is_empty() {
        return Err(protocol(ProtocolError::UnexpectedEof { needed: 1 }));
    }
    Ok(bytes::Buf::get_i8(buf))
}

/// Reads a fixed-width `uint16`, the type of the streams member's advertised
/// port.
pub(crate) fn get_u16(buf: &mut &[u8]) -> Result<u16, BrokerError> {
    if buf.len() < 2 {
        return Err(protocol(ProtocolError::UnexpectedEof {
            needed: 2 - buf.len(),
        }));
    }
    Ok(bytes::Buf::get_u16(buf))
}

pub(crate) fn get_compact_string(buf: &mut &[u8]) -> Result<String, BrokerError> {
    string_bytes::get_compact_string_owned(buf).map_err(protocol)
}

pub(crate) fn get_compact_nullable_string(buf: &mut &[u8]) -> Result<Option<String>, BrokerError> {
    string_bytes::get_compact_nullable_string_owned(buf).map_err(protocol)
}

pub(crate) fn get_compact_bytes(buf: &mut &[u8]) -> Result<Bytes, BrokerError> {
    string_bytes::get_compact_bytes_owned(buf).map_err(protocol)
}

/// Reads the compact `count + 1` prefix of a non-null array.
pub(crate) fn get_compact_array_len(buf: &mut &[u8]) -> Result<usize, BrokerError> {
    array::get_array_len(buf, true).map_err(protocol)
}

pub(crate) fn get_uuid(buf: &mut &[u8]) -> Result<[u8; 16], BrokerError> {
    krabka_protocol::primitives::uuid::get_uuid(buf)
        .map(|u| u.0)
        .map_err(protocol)
}

/// Reads and discards a tagged-field trailer whose tags the broker does not
/// interpret. Every tag Kafka defines on these records is optional with a
/// default, so dropping them loses nothing the broker keeps state for.
pub(crate) fn skip_tagged_fields(buf: &mut &[u8]) -> Result<(), BrokerError> {
    read_tagged_fields(buf, |_, _| Ok(false))
        .map(|_| ())
        .map_err(protocol)
}

/// Reads a tagged-field trailer, handing each entry to `known`. The closure
/// returns `true` when it recognized the tag and consumed the whole payload;
/// every other entry is skipped.
pub(crate) fn read_tagged<F>(buf: &mut &[u8], known: F) -> Result<(), BrokerError>
where
    F: FnMut(u32, &mut &[u8]) -> Result<bool, ProtocolError>,
{
    read_tagged_fields(buf, known).map(|_| ()).map_err(protocol)
}

// ───────────────────────────────────────────────────── array conveniences ──

/// Writes a compact array of `i32`, the `[]int32` of the schemas.
pub(crate) fn put_i32_array(buf: &mut BytesMut, items: &[i32]) {
    put_compact_array_len(buf, items.len());
    for v in items {
        buf.put_i32(*v);
    }
}

/// Reads the `[]int32` written by [`put_i32_array`].
pub(crate) fn get_i32_array(buf: &mut &[u8]) -> Result<Vec<i32>, BrokerError> {
    let n = get_compact_array_len(buf)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(super::get_i32(buf)?);
    }
    Ok(out)
}

/// Writes a compact array of compact strings, the `[]string` of the schemas.
pub(crate) fn put_string_array(buf: &mut BytesMut, items: &[String]) {
    put_compact_array_len(buf, items.len());
    for s in items {
        put_compact_string(buf, s);
    }
}

/// Reads the `[]string` written by [`put_string_array`].
pub(crate) fn get_string_array(buf: &mut &[u8]) -> Result<Vec<String>, BrokerError> {
    let n = get_compact_array_len(buf)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(get_compact_string(buf)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn compact_string_is_length_plus_one() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "abc");
        assert!(&buf[..] == b"\x04abc");
    }

    #[test]
    fn compact_null_string_is_zero() {
        let mut buf = BytesMut::new();
        put_compact_nullable_string(&mut buf, None);
        assert!(&buf[..] == b"\x00");
    }

    #[test]
    fn empty_tagged_fields_is_one_zero_byte() {
        let mut buf = BytesMut::new();
        put_empty_tagged_fields(&mut buf);
        assert!(&buf[..] == b"\x00");
    }

    #[test]
    fn i32_array_prefix_is_count_plus_one() {
        let mut buf = BytesMut::new();
        put_i32_array(&mut buf, &[7]);
        assert!(&buf[..] == b"\x02\x00\x00\x00\x07");
        let mut r = &buf[..];
        assert!(get_i32_array(&mut r).unwrap() == vec![7]);
        assert!(r.is_empty());
    }

    #[test]
    fn tagged_fields_round_trip_in_tag_order() {
        let mut buf = BytesMut::new();
        put_tagged_fields(
            &mut buf,
            vec![(9, Bytes::from_static(b"z")), (1, Bytes::from_static(b"a"))],
        );
        assert!(&buf[..] == b"\x02\x01\x01a\x09\x01z");
    }

    #[test]
    fn skip_tagged_fields_consumes_the_trailer() {
        let mut buf = BytesMut::new();
        put_tagged_fields(&mut buf, vec![(3, Bytes::from_static(b"xy"))]);
        buf.put_u8(0xff);
        let mut r = &buf[..];
        skip_tagged_fields(&mut r).unwrap();
        assert!(r == b"\xff");
    }
}
