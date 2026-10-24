//! The barrier marker control record, shared by the broker and log readers.

use {
    crate::BARRIER_CONTROL_TYPE,
    krabka_protocol::{
        ProtocolError,
        primitives::{
            fixed::{get_i16, get_i64},
            string_bytes::get_string_owned,
        },
        records::Record,
    },
};

/// The contents of one barrier marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarrierMarker {
    /// The barrier group this marker belongs to.
    pub group: String,
    /// The epoch this marker stamps.
    pub epoch: i64,
    /// When the coordinator started the injection, in milliseconds since the
    /// Unix epoch.
    pub triggered_at: i64,
}

/// Read the contents of a barrier marker back out of its record.
///
/// # Errors
/// Returns [`ProtocolError`] when the record carries no key or no value,
/// when the key is not a barrier control-record key, when either the key
/// version or the value version is unsupported, or when the value is
/// truncated, holds a non-UTF-8 group name, or has trailing bytes.
pub fn parse_barrier_marker(record: &Record) -> Result<BarrierMarker, ProtocolError> {
    let key = record
        .key
        .as_deref()
        .ok_or(ProtocolError::InvalidValue("barrier marker has no key"))?;
    let mut cur = key;
    if get_i16(&mut cur)? != 0 {
        return Err(ProtocolError::InvalidValue(
            "unsupported control record key version",
        ));
    }
    if get_i16(&mut cur)? != BARRIER_CONTROL_TYPE {
        return Err(ProtocolError::InvalidValue(
            "control record is not a barrier marker",
        ));
    }

    let value = record
        .value
        .as_deref()
        .ok_or(ProtocolError::InvalidValue("barrier marker has no value"))?;
    let mut cur = value;
    if get_i16(&mut cur)? != 0 {
        return Err(ProtocolError::InvalidValue(
            "unsupported barrier marker version",
        ));
    }
    let group = get_string_owned(&mut cur)?;
    let epoch = get_i64(&mut cur)?;
    let triggered_at = get_i64(&mut cur)?;
    if !cur.is_empty() {
        return Err(ProtocolError::InvalidValue(
            "trailing bytes after barrier marker",
        ));
    }
    Ok(BarrierMarker {
        group,
        epoch,
        triggered_at,
    })
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, bytes::Bytes, krabka_protocol::records::Record};

    fn record(group: &str, epoch: i64, triggered_at: i64) -> Record {
        let mut key = Vec::with_capacity(4);
        key.extend_from_slice(&0i16.to_be_bytes());
        key.extend_from_slice(&BARRIER_CONTROL_TYPE.to_be_bytes());

        let mut value = Vec::with_capacity(20 + group.len());
        value.extend_from_slice(&0i16.to_be_bytes());
        value.extend_from_slice(&(group.len() as i16).to_be_bytes());
        value.extend_from_slice(group.as_bytes());
        value.extend_from_slice(&epoch.to_be_bytes());
        value.extend_from_slice(&triggered_at.to_be_bytes());
        Record {
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Record::default()
        }
    }

    #[test]
    fn reads_a_barrier_marker_from_its_control_record() {
        let marker = parse_barrier_marker(&record("orders-cut", 7, 1_724_500_000_000))
            .expect("valid marker");
        assert!(
            marker
                == BarrierMarker {
                    group: "orders-cut".to_owned(),
                    epoch: 7,
                    triggered_at: 1_724_500_000_000,
                }
        );
    }

    #[test]
    fn refuses_a_record_without_a_key() {
        let error = parse_barrier_marker(&Record::default()).expect_err("key is required");
        assert!(matches!(
            error,
            ProtocolError::InvalidValue("barrier marker has no key")
        ));
    }
}
