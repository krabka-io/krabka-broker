//! The leaf encoders and decoders that the streams record values share.
//!
//! Every streams value is flexible (`"flexibleVersions": "0+"` in the Apache
//! Kafka schemas at tag `4.3.1`), so an array is a compact array and a string
//! is a compact string, and every nested struct ends with its own tagged-field
//! count. The general leaves live in
//! [`persistence::flex`](crate::coordinator::unified::persistence::flex); what
//! is here is the two shapes that only streams records use.

use std::collections::BTreeMap;

use bytes::BytesMut;

use crate::{
    coordinator::unified::persistence::flex::{
        get_compact_array_len, get_compact_string, get_i32_array, put_compact_array_len,
        put_compact_string, put_empty_tagged_fields, put_i32_array, skip_tagged_fields,
    },
    error::BrokerError,
};

/// Encodes a role's task assignment as Kafka's `[]TaskIds`: a compact count,
/// then per entry the compact `SubtopologyId`, the compact `Partitions`
/// (`[]int32`) and the struct's tagged-field count. The `TaskIds` of
/// `StreamsGroupCurrentMemberAssignmentValue` also declares a tagged
/// `AssignmentEpochs` (tag 0, nullable, default null), which the broker does
/// not set and therefore omits. [`decode_task_map`] reads the same layout.
pub(super) fn encode_task_map(buf: &mut BytesMut, map: &BTreeMap<String, Vec<i32>>) {
    put_compact_array_len(buf, map.len());
    for (subtopology_id, partitions) in map {
        put_compact_string(buf, subtopology_id);
        put_i32_array(buf, partitions);
        put_empty_tagged_fields(buf);
    }
}

pub(super) fn decode_task_map(buf: &mut &[u8]) -> Result<BTreeMap<String, Vec<i32>>, BrokerError> {
    let n = get_compact_array_len(buf)?;
    let mut map = BTreeMap::new();
    for _ in 0..n {
        let subtopology_id = get_compact_string(buf)?;
        let partitions = get_i32_array(buf)?;
        skip_tagged_fields(buf)?;
        map.insert(subtopology_id, partitions);
    }
    Ok(map)
}

/// Encodes a `[]KeyValue`-shaped list: a compact count, then per entry two
/// compact strings and the struct's tagged-field count. Kafka's `ClientTags`
/// and `TopicConfigs` both have this shape.
pub(super) fn encode_key_value_list(buf: &mut BytesMut, items: &[(String, String)]) {
    put_compact_array_len(buf, items.len());
    for (k, v) in items {
        put_compact_string(buf, k);
        put_compact_string(buf, v);
        put_empty_tagged_fields(buf);
    }
}

pub(super) fn decode_key_value_list(buf: &mut &[u8]) -> Result<Vec<(String, String)>, BrokerError> {
    let n = get_compact_array_len(buf)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let k = get_compact_string(buf)?;
        let v = get_compact_string(buf)?;
        skip_tagged_fields(buf)?;
        out.push((k, v));
    }
    Ok(out)
}

/// Encodes an `[]int16`, which the copartition groups of the topology record
/// use for their indices into the subtopology's own topic lists.
pub(super) fn encode_i16_list(buf: &mut BytesMut, items: &[i16]) {
    put_compact_array_len(buf, items.len());
    for v in items {
        bytes::BufMut::put_i16(buf, *v);
    }
}

pub(super) fn decode_i16_list(buf: &mut &[u8]) -> Result<Vec<i16>, BrokerError> {
    let n = get_compact_array_len(buf)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(crate::coordinator::unified::persistence::get_i16(buf)?);
    }
    Ok(out)
}
