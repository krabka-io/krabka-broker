//! The leaf encoders and decoders that the streams record values share.
//!
//! A task map, a string list, and an `i16` list each encode as an `i32`
//! element count followed by that many elements. `get_i8` and `get_u32` are
//! the two fixed-width readers that the shared
//! `crate::coordinator::unified::persistence` helpers do not provide.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, BytesMut};
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{get_i16, get_i32, get_string, put_string},
    error::BrokerError,
};

/// Encodes a role's task assignment: an `i32` count of subtopologies, then for
/// each entry a `string subtopology_id`, an `i32` partition count, and that
/// many `i32` partitions. [`decode_task_map`] decodes the same layout.
pub(super) fn encode_task_map(buf: &mut BytesMut, map: &BTreeMap<String, Vec<i32>>) {
    let n = i32::try_from(map.len()).expect("fits");
    buf.put_i32(n);
    for (subtopology_id, partitions) in map {
        put_string(buf, subtopology_id);
        let pn = i32::try_from(partitions.len()).expect("fits");
        buf.put_i32(pn);
        for p in partitions {
            buf.put_i32(*p);
        }
    }
}

pub(super) fn decode_task_map(buf: &mut &[u8]) -> Result<BTreeMap<String, Vec<i32>>, BrokerError> {
    let n = get_i32(buf)?;
    let mut map = BTreeMap::new();
    for _ in 0..n.max(0) {
        let subtopology_id = get_string(buf)?;
        let pn = get_i32(buf)?;
        let pcap = usize::try_from(pn.max(0)).expect("non-negative");
        let mut partitions = Vec::with_capacity(pcap);
        for _ in 0..pn.max(0) {
            partitions.push(get_i32(buf)?);
        }
        map.insert(subtopology_id, partitions);
    }
    Ok(map)
}

pub(super) fn encode_string_list(buf: &mut BytesMut, items: &[String]) {
    let n = i32::try_from(items.len()).expect("fits");
    buf.put_i32(n);
    for s in items {
        put_string(buf, s);
    }
}

pub(super) fn decode_string_list(buf: &mut &[u8]) -> Result<Vec<String>, BrokerError> {
    let n = get_i32(buf)?;
    let cap = usize::try_from(n.max(0)).expect("non-negative");
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        out.push(get_string(buf)?);
    }
    Ok(out)
}

pub(super) fn encode_i16_list(buf: &mut BytesMut, items: &[i16]) {
    let n = i32::try_from(items.len()).expect("fits");
    buf.put_i32(n);
    for v in items {
        buf.put_i16(*v);
    }
}

pub(super) fn decode_i16_list(buf: &mut &[u8]) -> Result<Vec<i16>, BrokerError> {
    let n = get_i32(buf)?;
    let cap = usize::try_from(n.max(0)).expect("non-negative");
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        out.push(get_i16(buf)?);
    }
    Ok(out)
}

pub(super) fn get_i8(buf: &mut &[u8]) -> Result<i8, BrokerError> {
    if buf.remaining() < 1 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "missing i8",
        )));
    }
    Ok(buf.get_i8())
}

pub(super) fn get_u32(buf: &mut &[u8]) -> Result<u32, BrokerError> {
    if buf.remaining() < 4 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "missing u32",
        )));
    }
    Ok(buf.get_u32())
}
