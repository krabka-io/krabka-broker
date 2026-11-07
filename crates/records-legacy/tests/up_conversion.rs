#![cfg(test)]
//! Scenarios for `tests/features/up_conversion`: a legacy message set from a
//! Produce v0 to v2 producer up-converts to one v2 record batch.

use {
    assert2::assert,
    bytes::{Bytes, BytesMut},
    krabka_bdd::Given,
    krabka_ids::Offset,
    krabka_protocol::records::RecordBatch,
    krabka_records_legacy::{Magic, ParsedRecord, encode_flat_message_set, legacy_to_v2},
    rstest_bdd_macros::{given, then, when},
};

const GIVEN_MESSAGE_SET: &str = "Given a message set of magic <magic> messages";
const WHEN_UP_CONVERTS: &str = "When the broker up-converts the message set";

/// The offsets of a message set whose offsets the scenario does not name.
const OFFSETS: [i64; 3] = [40, 41, 42];

/// The create times of a magic 1 message set whose timestamps the scenario
/// does not name.
const TIMESTAMPS: [i64; 3] = [1_700_000_000_000, 1_700_000_000_010, 1_700_000_000_020];

/// A magic byte as a scenario writes it.
#[derive(Debug, Clone, Copy, krabka_bdd::StepParam)]
enum ProducerMagic {
    #[step_param("0")]
    Zero,
    #[step_param("1")]
    One,
}

impl From<ProducerMagic> for Magic {
    fn from(magic: ProducerMagic) -> Self {
        match magic {
            ProducerMagic::Zero => Self::V0,
            ProducerMagic::One => Self::V1,
        }
    }
}

/// A legacy producer that writes one uncompressed message per offset.
struct LegacyProducer;

impl LegacyProducer {
    /// A message set with one keyed message at each offset. A magic 1
    /// message carries the timestamp at the same position; a magic 0 message
    /// has none.
    fn message_set(magic: Magic, offsets: [i64; 3], timestamps: [i64; 3]) -> Bytes {
        let records = offsets
            .iter()
            .zip(timestamps)
            .map(|(&offset, timestamp)| ParsedRecord {
                offset: Offset(offset),
                timestamp: match magic {
                    Magic::V0 => None,
                    Magic::V1 => Some(timestamp),
                },
                key: Some(Bytes::from(format!("key-{offset}"))),
                value: Some(Bytes::from(format!("value-{offset}"))),
            });
        let mut wire = BytesMut::new();
        encode_flat_message_set(records, magic, &mut wire);
        wire.freeze()
    }
}

/// The broker's Produce path for a producer that writes a legacy magic.
struct Broker;

impl Broker {
    fn up_convert(message_set: &[u8]) -> RecordBatch {
        legacy_to_v2(message_set).expect("a well-formed legacy message set up-converts")
    }
}

/// The producer fields that make a batch idempotent.
#[derive(Debug, PartialEq, Eq)]
struct ProducerFields {
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

/// Where a batch sits in the log and where each record sits in the batch.
#[derive(Debug, PartialEq, Eq)]
struct OffsetLayout {
    base_offset: i64,
    last_offset_delta: i32,
    offset_deltas: Vec<i32>,
}

/// The batch timestamps and each record's timestamp delta.
#[derive(Debug, PartialEq, Eq)]
struct TimestampLayout {
    base_timestamp: i64,
    max_timestamp: i64,
    timestamp_deltas: Vec<i64>,
}

fn producer_fields(batch: &RecordBatch) -> ProducerFields {
    ProducerFields {
        producer_id: batch.producer_id,
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
    }
}

fn offset_layout(batch: &RecordBatch) -> OffsetLayout {
    OffsetLayout {
        base_offset: batch.base_offset,
        last_offset_delta: batch.last_offset_delta,
        offset_deltas: batch.records.iter().map(|r| r.offset_delta).collect(),
    }
}

fn timestamp_layout(batch: &RecordBatch) -> TimestampLayout {
    TimestampLayout {
        base_timestamp: batch.base_timestamp,
        max_timestamp: batch.max_timestamp,
        timestamp_deltas: batch.records.iter().map(|r| r.timestamp_delta).collect(),
    }
}

struct UpConversion {
    message_set: Given<Bytes>,
    batch: Given<RecordBatch>,
}

#[rstest::fixture]
fn up_conversion() -> UpConversion {
    UpConversion {
        message_set: Given::new("message_set", GIVEN_MESSAGE_SET),
        batch: Given::new("batch", WHEN_UP_CONVERTS),
    }
}

#[given("a message set of magic {magic} messages")]
fn message_set_of_magic(up_conversion: &mut UpConversion, magic: ProducerMagic) {
    up_conversion.message_set.set(LegacyProducer::message_set(
        magic.into(),
        OFFSETS,
        TIMESTAMPS,
    ));
}

#[given("a message set of magic 1 messages at offsets {first}, {second} and {third}")]
fn message_set_at_offsets(up_conversion: &mut UpConversion, first: i64, second: i64, third: i64) {
    up_conversion.message_set.set(LegacyProducer::message_set(
        Magic::V1,
        [first, second, third],
        TIMESTAMPS,
    ));
}

#[given("a message set of magic 1 messages with timestamps {first}, {second} and {third}")]
fn message_set_with_timestamps(
    up_conversion: &mut UpConversion,
    first: i64,
    second: i64,
    third: i64,
) {
    up_conversion
        .message_set
        .set(LegacyProducer::message_set(Magic::V1, OFFSETS, [
            first, second, third,
        ]));
}

#[when("the broker up-converts the message set")]
fn up_convert(up_conversion: &mut UpConversion) {
    let batch = Broker::up_convert(up_conversion.message_set.get());
    up_conversion.batch.set(batch);
}

#[then("the batch is not idempotent")]
fn batch_is_not_idempotent(up_conversion: &UpConversion) {
    assert!(
        producer_fields(up_conversion.batch.get())
            == ProducerFields {
                producer_id: -1,
                producer_epoch: -1,
                base_sequence: -1,
            }
    );
}

#[then("the batch has base offset {base_offset} and offset deltas {first}, {second} and {third}")]
fn batch_has_offset_deltas(
    up_conversion: &UpConversion,
    base_offset: i64,
    first: i32,
    second: i32,
    third: i32,
) {
    assert!(
        offset_layout(up_conversion.batch.get())
            == OffsetLayout {
                base_offset,
                last_offset_delta: third,
                offset_deltas: vec![first, second, third],
            }
    );
}

#[then(
    "the batch has base timestamp {base_timestamp}, max timestamp {max_timestamp} and timestamp deltas {first}, {second} and {third}"
)]
fn batch_has_timestamp_deltas(
    up_conversion: &UpConversion,
    base_timestamp: i64,
    max_timestamp: i64,
    first: i64,
    second: i64,
    third: i64,
) {
    assert!(
        timestamp_layout(up_conversion.batch.get())
            == TimestampLayout {
                base_timestamp,
                max_timestamp,
                timestamp_deltas: vec![first, second, third],
            }
    );
}

/// A magic 0 message has no timestamp, so each record delta is 0.
#[then("the batch has base timestamp {base_timestamp} and max timestamp {max_timestamp}")]
fn batch_has_no_timestamps(up_conversion: &UpConversion, base_timestamp: i64, max_timestamp: i64) {
    assert!(
        timestamp_layout(up_conversion.batch.get())
            == TimestampLayout {
                base_timestamp,
                max_timestamp,
                timestamp_deltas: vec![0; 3],
            }
    );
}

krabka_bdd::scenarios!(
    "tests/features/up_conversion",
    fixtures = [up_conversion: UpConversion],
    tags = "not @formulated and not @inprogress",
);
