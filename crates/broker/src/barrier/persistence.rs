//! Byte-exact codec for the `__barrier_state` topic records.
//!
//! The topic carries three record kinds, and the key names which one a record
//! is. A group record holds a barrier group's definition. An injection-start
//! record freezes the target set of one injection before the coordinator
//! appends any marker. A cut record holds the published offsets of one epoch.
//!
//! This module is a codec only. The barrier coordinator owns the runtime
//! wiring.
//!
//! The layout is deliberately plain, because `krabka-streams-java` and
//! `krabka-streams-go` decode cut records by hand. Every integer is
//! big-endian. A string is an `i16` byte length and then UTF-8 bytes. An `i32`
//! count precedes every array. There are no compact lengths and no tagged
//! fields.
//!
//! Wire, in field order:
//!
//! ```text
//! key:
//!   version i16 = 0
//!   kind    i16                0 group, 1 injection start, 2 cut
//!   group   string
//!   epoch   i64                -1 for a group record
//!
//! group value:
//!   version       i16 = 0
//!   topics        i32 [ topic string ]
//!   interval_ms   i64          -1 turns off periodic injection
//!   retained_cuts i32
//!   last_epoch    i64
//!
//! injection-start value:
//!   version           i16 = 0
//!   coordinator_epoch i32
//!   triggered_at      i64
//!   targets           i32 [ topic string | partition_count i32 ]
//!
//! cut value:
//!   version      i16 = 0
//!   triggered_at i64
//!   completed_at i64
//!   status       i8            0 complete, 1 partial
//!   topics       i32 [ topic string | partitions i32 [ partition i32 | offset i64 ] ]
//!   missing      i32 [ topic string | partition i32 ]
//! ```
//!
//! A group record with a null value is a tombstone, and it deletes the group.

use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_protocol::{
    ProtocolError,
    primitives::{
        array::{get_array_len, put_array_len},
        fixed::{get_i8, get_i16, get_i32, get_i64, put_i8, put_i16, put_i32, put_i64},
        string_bytes::{get_string_owned, put_string},
    },
};
use krabka_units::{
    Time,
    convert::wire::{opt_time_from_millis_i64, opt_time_to_millis_i64},
};

/// The record version that every `__barrier_state` record carries.
///
/// krabka is greenfield, so there is one version and the decoder rejects any
/// other value.
pub(crate) const RECORD_VERSION: i16 = 0;

/// The epoch that a group record writes, because a group definition belongs to
/// no single epoch.
pub(crate) const NO_EPOCH: i64 = -1;

/// Which of the three record kinds a key names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordKind {
    /// A barrier group definition.
    Group,
    /// The frozen target set of one injection, written before the first
    /// marker append.
    InjectionStart,
    /// The published offsets of one epoch.
    Cut,
}

impl RecordKind {
    /// The `i16` that this kind writes into a key.
    pub(crate) const fn code(self) -> i16 {
        match self {
            Self::Group => 0,
            Self::InjectionStart => 1,
            Self::Cut => 2,
        }
    }
}

impl TryFrom<i16> for RecordKind {
    type Error = ProtocolError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Group),
            1 => Ok(Self::InjectionStart),
            2 => Ok(Self::Cut),
            _ => Err(ProtocolError::InvalidValue("unknown barrier record kind")),
        }
    }
}

/// Whether an injection reached every partition of its frozen target set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CutStatus {
    /// Every target partition carries the marker.
    Complete,
    /// At least one target partition carries no marker, and it never will.
    /// The `missing` list of the cut names those partitions.
    Partial,
}

impl CutStatus {
    /// The `i8` that this status writes into a cut value.
    pub(crate) const fn code(self) -> i8 {
        match self {
            Self::Complete => 0,
            Self::Partial => 1,
        }
    }
}

impl TryFrom<i8> for CutStatus {
    type Error = ProtocolError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Complete),
            1 => Ok(Self::Partial),
            _ => Err(ProtocolError::InvalidValue("unknown barrier cut status")),
        }
    }
}

/// The key of any `__barrier_state` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordKey {
    pub(crate) kind: RecordKind,
    pub(crate) group: String,
    /// The epoch this record belongs to, or [`NO_EPOCH`] for a group record.
    pub(crate) epoch: i64,
}

impl RecordKey {
    /// The key of the group record for `group`.
    pub(crate) fn group(group: impl Into<String>) -> Self {
        Self {
            kind: RecordKind::Group,
            group: group.into(),
            epoch: NO_EPOCH,
        }
    }

    /// The key of the injection-start record for one epoch.
    pub(crate) fn injection_start(group: impl Into<String>, epoch: i64) -> Self {
        Self {
            kind: RecordKind::InjectionStart,
            group: group.into(),
            epoch,
        }
    }

    /// The key of the cut record for one epoch.
    pub(crate) fn cut(group: impl Into<String>, epoch: i64) -> Self {
        Self {
            kind: RecordKind::Cut,
            group: group.into(),
            epoch,
        }
    }
}

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

/// One partition of a cut, and the offset its marker took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionOffset {
    pub(crate) partition: PartitionIndex,
    pub(crate) offset: Offset,
}

/// The marker offsets of one topic in a cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicOffsets {
    pub(crate) topic: String,
    pub(crate) partitions: Vec<PartitionOffset>,
}

/// A partition that an injection did not reach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MissingPartition {
    pub(crate) topic: String,
    pub(crate) partition: PartitionIndex,
}

/// The published offsets of one epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CutValue {
    pub(crate) triggered_at: i64,
    pub(crate) completed_at: i64,
    pub(crate) status: CutStatus,
    pub(crate) topics: Vec<TopicOffsets>,
    /// The partitions that carry no marker for this epoch. It is empty when
    /// the status is [`CutStatus::Complete`].
    pub(crate) missing: Vec<MissingPartition>,
}

/// Encode a record key.
#[must_use]
pub(crate) fn encode_key(key: &RecordKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + key.group.len());
    put_i16(&mut out, RECORD_VERSION);
    put_i16(&mut out, key.kind.code());
    put_string(&mut out, &key.group);
    put_i64(&mut out, key.epoch);
    out
}

/// Decode a record key.
///
/// # Errors
/// Returns a [`ProtocolError`] when the key is truncated, carries a version
/// other than [`RECORD_VERSION`], names an unknown record kind, holds a
/// non-UTF-8 group name, or has trailing bytes.
pub(crate) fn decode_key(bytes: &[u8]) -> Result<RecordKey, ProtocolError> {
    let mut cur = bytes;
    expect_version(&mut cur)?;
    let kind = RecordKind::try_from(get_i16(&mut cur)?)?;
    let group = get_string_owned(&mut cur)?;
    let epoch = get_i64(&mut cur)?;
    expect_end(cur)?;
    Ok(RecordKey { kind, group, epoch })
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

/// Encode a cut.
#[must_use]
pub(crate) fn encode_cut(value: &CutValue) -> Vec<u8> {
    let mut out = Vec::new();
    put_i16(&mut out, RECORD_VERSION);
    put_i64(&mut out, value.triggered_at);
    put_i64(&mut out, value.completed_at);
    put_i8(&mut out, value.status.code());
    put_array_len(&mut out, value.topics.len(), false);
    for topic in &value.topics {
        put_string(&mut out, &topic.topic);
        put_array_len(&mut out, topic.partitions.len(), false);
        for entry in &topic.partitions {
            put_i32(&mut out, entry.partition.get());
            put_i64(&mut out, entry.offset.0);
        }
    }
    put_array_len(&mut out, value.missing.len(), false);
    for entry in &value.missing {
        put_string(&mut out, &entry.topic);
        put_i32(&mut out, entry.partition.get());
    }
    out
}

/// Decode a cut.
///
/// # Errors
/// Returns a [`ProtocolError`] when the value is truncated, carries a version
/// other than [`RECORD_VERSION`], names an unknown status, holds a negative
/// array length, holds a non-UTF-8 topic name, or has trailing bytes.
pub(crate) fn decode_cut(bytes: &[u8]) -> Result<CutValue, ProtocolError> {
    let mut cur = bytes;
    expect_version(&mut cur)?;
    let triggered_at = get_i64(&mut cur)?;
    let completed_at = get_i64(&mut cur)?;
    let status = CutStatus::try_from(get_i8(&mut cur)?)?;
    let topics = decode_vec(&mut cur, |c| {
        let topic = get_string_owned(c)?;
        let partitions = decode_vec(c, |p| {
            let partition = PartitionIndex(get_i32(p)?);
            let offset = Offset(get_i64(p)?);
            Ok(PartitionOffset { partition, offset })
        })?;
        Ok(TopicOffsets { topic, partitions })
    })?;
    let missing = decode_vec(&mut cur, |c| {
        let topic = get_string_owned(c)?;
        let partition = PartitionIndex(get_i32(c)?);
        Ok(MissingPartition { topic, partition })
    })?;
    expect_end(cur)?;
    Ok(CutValue {
        triggered_at,
        completed_at,
        status,
        topics,
        missing,
    })
}

/// Read and check the leading record version.
fn expect_version(cur: &mut &[u8]) -> Result<(), ProtocolError> {
    if get_i16(cur)? == RECORD_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::InvalidValue(
            "unsupported barrier record version",
        ))
    }
}

/// Reject a record that carries bytes past its last field.
fn expect_end(cur: &[u8]) -> Result<(), ProtocolError> {
    if cur.is_empty() {
        Ok(())
    } else {
        Err(ProtocolError::InvalidValue(
            "trailing bytes after barrier record",
        ))
    }
}

/// Read an `i32`-counted array, and read each element with `element`.
fn decode_vec<T>(
    cur: &mut &[u8],
    element: impl Fn(&mut &[u8]) -> Result<T, ProtocolError>,
) -> Result<Vec<T>, ProtocolError> {
    let len = get_array_len(cur, false)?;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(element(cur)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::convert::TimeExt;

    use super::*;

    fn sample_group() -> GroupValue {
        GroupValue {
            topics: vec!["orders".to_owned(), "payments".to_owned()],
            interval: Some(Time::from_millis(60_000)),
            retained_cuts: 32,
            last_epoch: 7,
        }
    }

    fn sample_injection_start() -> InjectionStartValue {
        InjectionStartValue {
            coordinator_epoch: 4,
            triggered_at: 1_724_500_000_000,
            targets: vec![
                TopicTarget {
                    topic: "orders".to_owned(),
                    partition_count: 3,
                },
                TopicTarget {
                    topic: "payments".to_owned(),
                    partition_count: 1,
                },
            ],
        }
    }

    fn sample_cut() -> CutValue {
        CutValue {
            triggered_at: 1_724_500_000_000,
            completed_at: 1_724_500_000_042,
            status: CutStatus::Partial,
            topics: vec![
                TopicOffsets {
                    topic: "orders".to_owned(),
                    partitions: vec![
                        PartitionOffset {
                            partition: PartitionIndex(0),
                            offset: Offset(1024),
                        },
                        PartitionOffset {
                            partition: PartitionIndex(1),
                            offset: Offset(2048),
                        },
                    ],
                },
                TopicOffsets {
                    topic: "payments".to_owned(),
                    partitions: vec![PartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(9),
                    }],
                },
            ],
            missing: vec![MissingPartition {
                topic: "orders".to_owned(),
                partition: PartitionIndex(2),
            }],
        }
    }

    #[test]
    fn every_key_kind_round_trips() {
        let cases = [
            ("group", RecordKey::group("orders-cut")),
            (
                "injection start",
                RecordKey::injection_start("orders-cut", 7),
            ),
            ("cut", RecordKey::cut("orders-cut", 7)),
        ];
        for (case, key) in cases {
            let decoded = decode_key(&encode_key(&key)).ok();
            assert!(decoded.as_ref() == Some(&key), "{case}");
        }
    }

    #[test]
    fn a_group_key_carries_no_epoch() {
        let key = RecordKey::group("orders-cut");
        assert!(key.epoch == NO_EPOCH);
        assert!(decode_key(&encode_key(&key)).ok() == Some(key));
    }

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

    #[test]
    fn an_injection_start_round_trips() {
        let value = sample_injection_start();
        assert!(decode_injection_start(&encode_injection_start(&value)).ok() == Some(value));
    }

    #[test]
    fn a_cut_round_trips() {
        let value = sample_cut();
        assert!(decode_cut(&encode_cut(&value)).ok() == Some(value));
    }

    #[test]
    fn a_complete_cut_names_no_missing_partition() {
        let value = CutValue {
            status: CutStatus::Complete,
            missing: Vec::new(),
            ..sample_cut()
        };
        let decoded = decode_cut(&encode_cut(&value)).expect("decodes");
        assert!(decoded == value);
        assert!(decoded.missing.is_empty());
    }

    #[test]
    fn a_cut_with_no_topic_round_trips() {
        let value = CutValue {
            topics: Vec::new(),
            missing: Vec::new(),
            status: CutStatus::Complete,
            ..sample_cut()
        };
        assert!(decode_cut(&encode_cut(&value)).ok() == Some(value));
    }

    #[test]
    fn every_decoder_rejects_a_wrong_version() {
        let mut key = encode_key(&RecordKey::cut("orders-cut", 7));
        key[1] = 1;
        assert!(decode_key(&key).is_err());

        let mut group = encode_group(&sample_group());
        group[1] = 1;
        assert!(decode_group(&group).is_err());

        let mut start = encode_injection_start(&sample_injection_start());
        start[1] = 1;
        assert!(decode_injection_start(&start).is_err());

        let mut cut = encode_cut(&sample_cut());
        cut[1] = 1;
        assert!(decode_cut(&cut).is_err());
    }

    #[test]
    fn a_key_rejects_an_unknown_record_kind() {
        let mut bytes = encode_key(&RecordKey::cut("orders-cut", 7));
        bytes[3] = 9;
        assert!(decode_key(&bytes).is_err());
    }

    #[test]
    fn a_cut_rejects_an_unknown_status() {
        let mut bytes = encode_cut(&sample_cut());
        // version i16, triggered_at i64, completed_at i64, then the status i8.
        bytes[18] = 9;
        assert!(decode_cut(&bytes).is_err());
    }

    #[test]
    fn every_decoder_rejects_trailing_bytes() {
        let mut key = encode_key(&RecordKey::cut("orders-cut", 7));
        key.push(0);
        assert!(decode_key(&key).is_err());

        let mut group = encode_group(&sample_group());
        group.push(0);
        assert!(decode_group(&group).is_err());

        let mut start = encode_injection_start(&sample_injection_start());
        start.push(0);
        assert!(decode_injection_start(&start).is_err());

        let mut cut = encode_cut(&sample_cut());
        cut.push(0);
        assert!(decode_cut(&cut).is_err());
    }

    #[test]
    fn every_decoder_rejects_a_truncated_record() {
        let cut = encode_cut(&sample_cut());
        for len in 0..cut.len() {
            assert!(decode_cut(&cut[..len]).is_err(), "length {len}");
        }
    }

    /// The cut format is frozen across this crate, `krabka-streams-rs`,
    /// `krabka-streams-java` and `krabka-streams-go`. These bytes are encoded
    /// straight from the layout this module's own documentation states,
    /// independently of all four implementations, so an encoder or a decoder
    /// that drifts fails here. The same vector is asserted in the other three.
    ///
    /// Key: version 0, kind 2, group `orders-cut`, epoch 7.
    /// Value: version 0, triggered 1724500000000, completed 1724500000042,
    /// status complete, topic `orders` with partition 0 at offset 1024 and
    /// partition 1 at offset 2048, and no missing partitions.
    const GOLDEN_CUT_KEY: &[u8] = &[
        0x00, 0x00, 0x00, 0x02, 0x00, 0x0a, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x73, 0x2d, 0x63, 0x75,
        0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
    ];

    const GOLDEN_CUT_VALUE: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x01, 0x91, 0x84, 0x35, 0xbd, 0x00, 0x00, 0x00, 0x01, 0x91, 0x84,
        0x35, 0xbd, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x6f, 0x72, 0x64, 0x65, 0x72,
        0x73, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00,
        0x00, 0x00, 0x00,
    ];

    fn golden_cut() -> CutValue {
        CutValue {
            triggered_at: 1_724_500_000_000,
            completed_at: 1_724_500_000_042,
            status: CutStatus::Complete,
            topics: vec![TopicOffsets {
                topic: "orders".to_owned(),
                partitions: vec![
                    PartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(1024),
                    },
                    PartitionOffset {
                        partition: PartitionIndex(1),
                        offset: Offset(2048),
                    },
                ],
            }],
            missing: Vec::new(),
        }
    }

    #[test]
    fn the_golden_cut_key_decodes_and_re_encodes_byte_for_byte() {
        let expected = RecordKey::cut("orders-cut", 7);
        assert!(decode_key(GOLDEN_CUT_KEY).ok() == Some(expected.clone()));
        assert!(encode_key(&expected) == GOLDEN_CUT_KEY);
    }

    #[test]
    fn the_golden_cut_value_decodes_and_re_encodes_byte_for_byte() {
        let expected = golden_cut();
        assert!(decode_cut(GOLDEN_CUT_VALUE).ok() == Some(expected.clone()));
        assert!(encode_cut(&expected) == GOLDEN_CUT_VALUE);
    }

    #[test]
    fn a_kind_survives_its_wire_code() {
        for kind in [
            RecordKind::Group,
            RecordKind::InjectionStart,
            RecordKind::Cut,
        ] {
            assert!(RecordKind::try_from(kind.code()).ok() == Some(kind));
        }
    }

    #[test]
    fn a_status_survives_its_wire_code() {
        for status in [CutStatus::Complete, CutStatus::Partial] {
            assert!(CutStatus::try_from(status.code()).ok() == Some(status));
        }
    }
}
