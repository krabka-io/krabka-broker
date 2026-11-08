#![cfg(test)]
//! Scenarios for the magic 0 and magic 1 legacy message layout: the fields a
//! message carries, the CRC over them, and how a null field differs from an
//! empty one.
//!
//! The golden bytes in the feature come from an independent CRC-32, and the
//! wire checks here read those bytes with their own big-endian parser, so the
//! codec is never its own oracle.

use {
    assert2::assert,
    bytes::{Bytes, BytesMut},
    krabka_bdd::{Given, StepParam},
    krabka_ids::Offset,
    krabka_records_legacy::{
        LegacyRecordsError, Magic, ParsedRecord, decode_message_set, encode_flat_message_set,
    },
    rstest_bdd_macros::{given, then, when},
};

/// The magic 1 message of the golden scenario: offset 0, timestamp
/// 1700000000000, a null key and the value `hello`.
const GOLDEN_V1_ENTRY: &str =
    "0000000000000000 0000001b 8ee30bba 01 00 0000018bcfe56800 ffffffff 00000005 68656c6c6f";

/// A magic 2 message in the magic 1 layout, with a CRC that an independent
/// CRC-32 (IEEE) computed over its bytes from magic through value.
const MAGIC_2_ENTRY: &str =
    "0000000000000000 0000001b f8063287 02 00 0000018bcfe56800 ffffffff 00000005 68656c6c6f";

/// The offset a tampered entry is moved to.
const CHANGED_OFFSET: i64 = 7;

const GIVEN_RECORD: &str =
    "Given a record at offset <offset> with a null key and the value <value>";
const GIVEN_MESSAGE_SET: &str = "Given a message set from a legacy producer";
const DECODED: &str = "When the broker decodes the message set";

/// A magic byte as a scenario writes it.
#[derive(Debug, Clone, Copy, StepParam)]
enum MagicByte {
    #[step_param("0")]
    Zero,
    #[step_param("1")]
    One,
}

impl From<MagicByte> for Magic {
    fn from(byte: MagicByte) -> Self {
        match byte {
            MagicByte::Zero => Self::V0,
            MagicByte::One => Self::V1,
        }
    }
}

/// The part of a stored message that a scenario changes after its CRC.
#[derive(Debug, Clone, Copy, StepParam)]
enum TamperedPart {
    /// The magic byte, the first byte the CRC covers.
    Magic,
    /// The last byte of the value, the last byte the CRC covers.
    Value,
    /// The stored checksum itself.
    Crc,
    /// The entry offset, which sits outside the CRC.
    Offset,
}

/// A nullable message field.
#[derive(Debug, Clone, Copy, StepParam)]
enum Field {
    Key,
    Value,
}

/// What a nullable field holds.
#[derive(Debug, Clone, Copy, StepParam)]
enum Content {
    Null,
    Empty,
}

impl Content {
    const fn bytes(self) -> Option<Bytes> {
        match self {
            Self::Null => None,
            Self::Empty => Some(Bytes::new()),
        }
    }
}

/// Why the broker refused a message set.
#[derive(Debug, PartialEq, Eq)]
enum Rejection {
    Corrupt,
    UnsupportedMagic(i8),
    Other(String),
}

impl From<&LegacyRecordsError> for Rejection {
    fn from(error: &LegacyRecordsError) -> Self {
        match error {
            LegacyRecordsError::CrcMismatch { .. } => Self::Corrupt,
            LegacyRecordsError::UnsupportedMagic { found } => Self::UnsupportedMagic(*found),
            other => Self::Other(other.to_string()),
        }
    }
}

/// One message set entry, read field by field from the wire without the codec.
#[derive(Debug, PartialEq, Eq)]
struct WireEntry {
    timestamp: Option<i64>,
    key_length: i32,
    value_length: i32,
}

impl WireEntry {
    /// Offset(8) and size(4), then CRC(4), magic(1) and attributes(1).
    const MAGIC_AT: usize = 16;

    fn read(wire: &[u8]) -> Self {
        let magic = wire[Self::MAGIC_AT];
        let mut at = Self::MAGIC_AT + 2;
        let timestamp = (magic == 1).then(|| {
            let timestamp = i64::from_be_bytes(wire[at..at + 8].try_into().unwrap());
            at += 8;
            timestamp
        });
        let key_length = i32::from_be_bytes(wire[at..at + 4].try_into().unwrap());
        at += 4 + usize::try_from(key_length).unwrap_or(0);
        let value_length = i32::from_be_bytes(wire[at..at + 4].try_into().unwrap());
        Self {
            timestamp,
            key_length,
            value_length,
        }
    }

    const fn length(&self, field: Field) -> i32 {
        match field {
            Field::Key => self.key_length,
            Field::Value => self.value_length,
        }
    }
}

/// A pre-KIP-98 producer that writes one uncompressed legacy message per record.
struct LegacyProducer;

impl LegacyProducer {
    fn encode(record: ParsedRecord, magic: Magic) -> Bytes {
        let mut wire = BytesMut::new();
        assert!(let Ok(()) = encode_flat_message_set([record], magic, &mut wire));
        wire.freeze()
    }
}

/// The broker side of a Produce request carrying a legacy message set.
struct Broker;

impl Broker {
    fn decode(message_set: &[u8]) -> Result<Vec<ParsedRecord>, LegacyRecordsError> {
        decode_message_set(&mut &message_set[..], message_set.len())
    }
}

/// The golden magic 1 message with `part` changed after its CRC was computed.
fn tampered_v1_message(part: TamperedPart) -> Bytes {
    let mut wire = hex_bytes(GOLDEN_V1_ENTRY);
    match part {
        TamperedPart::Magic => wire[WireEntry::MAGIC_AT] ^= 0xff,
        TamperedPart::Value => *wire.last_mut().unwrap() ^= 0xff,
        TamperedPart::Crc => wire[12] ^= 0xff,
        TamperedPart::Offset => wire[..8].copy_from_slice(&CHANGED_OFFSET.to_be_bytes()),
    }
    Bytes::from(wire)
}

/// Decodes hex grouped by whitespace, as the feature writes it.
fn hex_bytes(grouped: &str) -> Vec<u8> {
    let digits: String = grouped.split_whitespace().collect();
    hex::decode(digits).unwrap()
}

fn record_with(field: Field, content: Content) -> ParsedRecord {
    let mut record = ParsedRecord {
        offset: Offset(0),
        timestamp: None,
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
    };
    match field {
        Field::Key => record.key = content.bytes(),
        Field::Value => record.value = content.bytes(),
    }
    record
}

struct MessageScenario {
    record: Given<ParsedRecord>,
    message_set: Given<Bytes>,
    decoded: Given<Result<Vec<ParsedRecord>, LegacyRecordsError>>,
}

#[rstest::fixture]
fn message() -> MessageScenario {
    MessageScenario {
        record: Given::new("record", GIVEN_RECORD),
        message_set: Given::new("message_set", GIVEN_MESSAGE_SET),
        decoded: Given::new("decoded", DECODED),
    }
}

impl MessageScenario {
    fn rejection(&self) -> Rejection {
        assert!(let Err(error) = self.decoded.get());
        Rejection::from(error)
    }
}

#[given("a record at offset {offset} with a null key and the value {value}")]
fn record_without_timestamp(message: &mut MessageScenario, offset: i64, value: String) {
    message.record.set(ParsedRecord {
        offset: Offset(offset),
        timestamp: None,
        key: None,
        value: Some(Bytes::from(value)),
    });
}

#[given("a record at offset {offset} with a null key, the value {value} and timestamp {timestamp}")]
fn record_with_timestamp(
    message: &mut MessageScenario,
    offset: i64,
    value: String,
    timestamp: i64,
) {
    message.record.set(ParsedRecord {
        offset: Offset(offset),
        timestamp: Some(timestamp),
        key: None,
        value: Some(Bytes::from(value)),
    });
}

#[given("a record whose {field} is {content}")]
fn record_with_field(message: &mut MessageScenario, field: Field, content: Content) {
    message.record.set(record_with(field, content));
}

#[given("a message set from a legacy producer")]
fn message_set_from_producer(message: &mut MessageScenario, docstring: String) {
    message.message_set.set(Bytes::from(hex_bytes(&docstring)));
}

#[given("a magic 1 message whose {part} has been changed after its CRC was computed")]
fn tampered_message(message: &mut MessageScenario, part: TamperedPart) {
    message.message_set.set(tampered_v1_message(part));
}

#[given("a message with magic 2 and a valid CRC")]
fn magic_2_message(message: &mut MessageScenario) {
    message
        .message_set
        .set(Bytes::from(hex_bytes(MAGIC_2_ENTRY)));
}

#[when("a legacy producer encodes it with magic {magic}")]
fn producer_encodes(message: &mut MessageScenario, magic: MagicByte) {
    let record = message.record.get().clone();
    message
        .message_set
        .set(LegacyProducer::encode(record, magic.into()));
}

#[when("the broker decodes the message set")]
fn broker_decodes(message: &mut MessageScenario) {
    let decoded = Broker::decode(message.message_set.get());
    message.decoded.set(decoded);
}

#[then("the message set bytes are")]
fn message_set_bytes_are(message: &MessageScenario, docstring: String) {
    assert!(hex::encode(message.message_set.get()) == hex::encode(hex_bytes(&docstring)));
}

#[then("the message carries timestamp {timestamp}")]
fn message_carries_timestamp(message: &MessageScenario, timestamp: i64) {
    assert!(WireEntry::read(message.message_set.get()).timestamp == Some(timestamp));
}

#[then(
    "the broker reads a record at offset {offset} with no timestamp, a null key and the value {value}"
)]
fn reads_record_without_timestamp(message: &MessageScenario, offset: i64, value: String) {
    assert!(let Ok(records) = message.decoded.get());
    assert!(
        *records
            == [ParsedRecord {
                offset: Offset(offset),
                timestamp: None,
                key: None,
                value: Some(Bytes::from(value)),
            }]
    );
}

#[then(
    "the broker reads a record at offset {offset} with timestamp {timestamp}, a null key and the value {value}"
)]
fn reads_record_with_timestamp(
    message: &MessageScenario,
    offset: i64,
    timestamp: i64,
    value: String,
) {
    assert!(let Ok(records) = message.decoded.get());
    assert!(
        *records
            == [ParsedRecord {
                offset: Offset(offset),
                timestamp: Some(timestamp),
                key: None,
                value: Some(Bytes::from(value)),
            }]
    );
}

#[then("the broker reads the record at the changed offset")]
fn reads_record_at_changed_offset(message: &MessageScenario) {
    assert!(let Ok(records) = message.decoded.get());
    assert!(
        *records
            == [ParsedRecord {
                offset: Offset(CHANGED_OFFSET),
                timestamp: Some(1_700_000_000_000),
                key: None,
                value: Some(Bytes::from_static(b"hello")),
            }]
    );
}

#[then("the message set is rejected as corrupt")]
fn rejected_as_corrupt(message: &MessageScenario) {
    assert!(message.rejection() == Rejection::Corrupt);
}

#[then("the message set is rejected for unsupported magic {found}")]
fn rejected_for_unsupported_magic(message: &MessageScenario, found: i8) {
    assert!(message.rejection() == Rejection::UnsupportedMagic(found));
}

#[then("the {field} length on the wire is {length}")]
fn field_length_on_wire(message: &MessageScenario, field: Field, length: i32) {
    assert!(WireEntry::read(message.message_set.get()).length(field) == length);
}

#[then("the {field} reads back as {content}")]
fn field_reads_back(message: &MessageScenario, field: Field, content: Content) {
    let decoded = Broker::decode(message.message_set.get()).unwrap();
    assert!(decoded == [record_with(field, content)]);
}

krabka_bdd::scenarios!(
    "tests/features/message_format",
    fixtures = [message: MessageScenario],
);
