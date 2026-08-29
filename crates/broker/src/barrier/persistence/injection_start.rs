//! The injection-start record: the target set an injection freezes before the
//! coordinator appends its first marker.
//!
//! Recovery reads this record to learn which partitions an interrupted
//! injection was meant to reach, so its shape is independent of the cut that
//! the injection eventually publishes.

use krabka_protocol::{
    ProtocolError,
    primitives::{
        array::put_array_len,
        fixed::{get_i32, get_i64, put_i16, put_i32, put_i64},
        string_bytes::{get_string_owned, put_string},
    },
};

use super::{
    RECORD_VERSION,
    primitives::{decode_vec, expect_end, expect_version},
};

/// One topic in a frozen target set, and how many partitions it had.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicTarget {
    pub(crate) topic: String,
    pub(crate) partition_count: i32,
}

/// The frozen target set of one injection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InjectionStartValue {
    /// Fences a coordinator that lost and regained leadership.
    pub(crate) coordinator_epoch: i32,
    pub(crate) triggered_at: i64,
    pub(crate) targets: Vec<TopicTarget>,
}

/// Encode a frozen target set.
#[must_use]
pub(crate) fn encode_injection_start(value: &InjectionStartValue) -> Vec<u8> {
    let mut out = Vec::new();
    put_i16(&mut out, RECORD_VERSION);
    put_i32(&mut out, value.coordinator_epoch);
    put_i64(&mut out, value.triggered_at);
    put_array_len(&mut out, value.targets.len(), false);
    for target in &value.targets {
        put_string(&mut out, &target.topic);
        put_i32(&mut out, target.partition_count);
    }
    out
}

/// Decode a frozen target set.
///
/// # Errors
/// Returns a [`ProtocolError`] when the value is truncated, carries a version
/// other than [`RECORD_VERSION`], holds a negative array length, holds a
/// non-UTF-8 topic name, or has trailing bytes.
pub(crate) fn decode_injection_start(bytes: &[u8]) -> Result<InjectionStartValue, ProtocolError> {
    let mut cur = bytes;
    expect_version(&mut cur)?;
    let coordinator_epoch = get_i32(&mut cur)?;
    let triggered_at = get_i64(&mut cur)?;
    let targets = decode_vec(&mut cur, |c| {
        let topic = get_string_owned(c)?;
        let partition_count = get_i32(c)?;
        Ok(TopicTarget {
            topic,
            partition_count,
        })
    })?;
    expect_end(cur)?;
    Ok(InjectionStartValue {
        coordinator_epoch,
        triggered_at,
        targets,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::barrier::persistence::test_support::sample_injection_start;

    #[test]
    fn an_injection_start_round_trips() {
        let value = sample_injection_start();
        assert!(decode_injection_start(&encode_injection_start(&value)).ok() == Some(value));
    }
}
