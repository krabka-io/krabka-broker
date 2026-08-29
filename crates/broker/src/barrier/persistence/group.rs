//! The group-definition record: the topics a barrier group cuts across, its
//! injection interval, and its cut retention.
//!
//! A group record is the only record kind whose value carries an interval, so
//! it is the only one that touches the millisecond conversion for [`Time`].

use krabka_protocol::{
    ProtocolError,
    primitives::{
        array::put_array_len,
        fixed::{get_i32, get_i64, put_i16, put_i32, put_i64},
        string_bytes::{get_string_owned, put_string},
    },
};
use krabka_units::{
    Time,
    convert::wire::{opt_time_from_millis_i64, opt_time_to_millis_i64},
};

use super::{
    RECORD_VERSION,
    primitives::{decode_vec, expect_end, expect_version},
};

/// A barrier group definition.
///
/// The type is [`PartialEq`] but not [`Eq`], because [`Time`] is backed by a
/// float.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupValue {
    /// The topics the group cuts across.
    pub(crate) topics: Vec<String>,
    /// How often the coordinator injects without a trigger request. `None`
    /// turns periodic injection off.
    pub(crate) interval: Option<Time>,
    /// How many cuts the coordinator keeps before it trims the older ones.
    pub(crate) retained_cuts: i32,
    /// The highest epoch this group has allocated.
    pub(crate) last_epoch: i64,
}

/// Encode a group definition.
#[must_use]
pub(crate) fn encode_group(value: &GroupValue) -> Vec<u8> {
    let mut out = Vec::new();
    put_i16(&mut out, RECORD_VERSION);
    put_array_len(&mut out, value.topics.len(), false);
    for topic in &value.topics {
        put_string(&mut out, topic);
    }
    put_i64(&mut out, opt_time_to_millis_i64(value.interval));
    put_i32(&mut out, value.retained_cuts);
    put_i64(&mut out, value.last_epoch);
    out
}

/// Decode a group definition.
///
/// # Errors
/// Returns a [`ProtocolError`] when the value is truncated, carries a version
/// other than [`RECORD_VERSION`], holds a negative array length, holds a
/// non-UTF-8 topic name, or has trailing bytes.
pub(crate) fn decode_group(bytes: &[u8]) -> Result<GroupValue, ProtocolError> {
    let mut cur = bytes;
    expect_version(&mut cur)?;
    let topics = decode_vec(&mut cur, |c| get_string_owned(c))?;
    let interval = opt_time_from_millis_i64(get_i64(&mut cur)?);
    let retained_cuts = get_i32(&mut cur)?;
    let last_epoch = get_i64(&mut cur)?;
    expect_end(cur)?;
    Ok(GroupValue {
        topics,
        interval,
        retained_cuts,
        last_epoch,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::convert::TimeExt;

    use super::*;
    use crate::barrier::persistence::test_support::sample_group;

    #[test]
    fn a_group_value_round_trips() {
        let value = sample_group();
        assert!(decode_group(&encode_group(&value)).ok() == Some(value));
    }

    #[test]
    fn an_absent_interval_round_trips_as_none() {
        let value = GroupValue {
            interval: None,
            ..sample_group()
        };
        let decoded = decode_group(&encode_group(&value)).expect("decodes");
        assert!(decoded == value);
        assert!(decoded.interval.is_none());
    }

    #[test]
    fn an_interval_keeps_its_millisecond_value() {
        let value = sample_group();
        let decoded = decode_group(&encode_group(&value)).expect("decodes");
        assert!(decoded.interval.map(TimeExt::millis_i64) == Some(60_000));
    }

    #[test]
    fn an_empty_topic_list_round_trips() {
        let value = GroupValue {
            topics: Vec::new(),
            ..sample_group()
        };
        assert!(decode_group(&encode_group(&value)).ok() == Some(value));
    }
}
