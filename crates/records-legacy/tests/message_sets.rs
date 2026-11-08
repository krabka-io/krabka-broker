#![cfg(test)]
//! Scenarios for the framing of a legacy message set: entries back to back,
//! a partial trailing entry, and entry sizes no message can have.

use {
    assert2::assert,
    bytes::{BufMut, Bytes, BytesMut},
    krabka_bdd::{Given, StepParam},
    krabka_ids::Offset,
    krabka_records_legacy::{
        LegacyRecordsError, Magic, ParsedRecord, decode_message_set, encode_flat_message_set,
    },
    rstest_bdd_macros::{given, then, when},
};

/// The offset and size that frame every entry.
const ENTRY_HEADER_LEN: usize = 12;

const GIVEN_MESSAGE_SET: &str = "Given a message set of three magic 1 messages";
const GIVEN_ENTRY_SIZE: &str = "Given a message set whose only entry has message size <size>";
const DECODED: &str = "When the broker decodes the message set";

/// Where a truncated message set stops inside its third entry.
#[derive(Debug, Clone, Copy, StepParam)]
enum CutPoint {
    /// Inside the message bytes, after the whole entry header.
    Message,
    /// Inside the 12-byte offset and size.
    #[step_param("entry header")]
    EntryHeader,
}

/// A record count as a scenario writes it.
#[derive(Debug, Clone, Copy, StepParam)]
enum Count {
    Two,
    Three,
}

impl From<Count> for usize {
    fn from(count: Count) -> Self {
        match count {
            Count::Two => 2,
            Count::Three => 3,
        }
    }
}

/// Why the broker refused a message set.
#[derive(Debug, PartialEq, Eq)]
enum Rejection {
    /// An entry size below the smallest message.
    BelowMessageOverhead,
    /// An entry size below zero.
    NegativeSize(i32),
    /// Anything else, with the codec's description.
    Other(String),
}

impl From<&LegacyRecordsError> for Rejection {
    fn from(error: &LegacyRecordsError) -> Self {
        match error {
            LegacyRecordsError::Malformed(_) => Self::BelowMessageOverhead,
            LegacyRecordsError::NegativeLength {
                label: "message_size",
                len,
            } => Self::NegativeSize(*len),
            other => Self::Other(other.to_string()),
        }
    }
}

impl Rejection {
    /// The rejection Kafka's `ByteBufferLogInputStream` gives an entry of
    /// `size` bytes: every size below the 14-byte magic 0 overhead is corrupt.
    const fn for_entry_size(size: i32) -> Self {
        if size < 0 {
            Self::NegativeSize(size)
        } else {
            Self::BelowMessageOverhead
        }
    }
}

/// A pre-KIP-98 producer that writes one uncompressed magic 1 message per
/// record.
struct LegacyProducer;

impl LegacyProducer {
    fn encode(records: &[ParsedRecord]) -> Bytes {
        let mut wire = BytesMut::new();
        assert!(let Ok(()) = encode_flat_message_set(records.iter().cloned(), Magic::V1, &mut wire));
        wire.freeze()
    }

    /// Three records at consecutive offsets.
    fn three_records() -> Vec<ParsedRecord> {
        (0..3_i64)
            .map(|n| ParsedRecord {
                offset: Offset(n),
                timestamp: Some(1_700_000_000_000 + n),
                key: Some(Bytes::from(format!("key-{n}"))),
                value: Some(Bytes::from(format!("value-{n}"))),
            })
            .collect()
    }

    /// The message set of `records`, cut at `cut` inside its last entry.
    fn encode_cut(records: &[ParsedRecord], cut: CutPoint) -> Bytes {
        let (_, complete) = records.split_last().unwrap();
        let last_entry_start = Self::encode(complete).len();
        let cut_at = last_entry_start
            + match cut {
                CutPoint::Message => ENTRY_HEADER_LEN + 5,
                CutPoint::EntryHeader => ENTRY_HEADER_LEN / 2,
            };
        let whole = Self::encode(records);
        assert!(cut_at < whole.len());
        whole.slice(..cut_at)
    }
}

/// An entry with offset 0, the given message size, and no message bytes.
fn entry_with_size(size: i32) -> Bytes {
    let mut wire = BytesMut::new();
    wire.put_i64(0);
    wire.put_i32(size);
    wire.freeze()
}

/// The broker side of a Produce request carrying a legacy message set.
struct Broker;

impl Broker {
    fn decode(message_set: &[u8]) -> Result<Vec<ParsedRecord>, LegacyRecordsError> {
        decode_message_set(&mut &message_set[..], message_set.len())
    }
}

struct MessageSetScenario {
    produced: Given<Vec<ParsedRecord>>,
    message_set: Given<Bytes>,
    entry_size: Given<i32>,
    decoded: Given<Result<Vec<ParsedRecord>, LegacyRecordsError>>,
}

#[rstest::fixture]
fn message_set() -> MessageSetScenario {
    MessageSetScenario {
        produced: Given::new("produced", GIVEN_MESSAGE_SET),
        message_set: Given::new("message_set", GIVEN_MESSAGE_SET),
        entry_size: Given::new("entry_size", GIVEN_ENTRY_SIZE),
        decoded: Given::new("decoded", DECODED),
    }
}

#[given("a message set of three magic 1 messages")]
fn three_messages(message_set: &mut MessageSetScenario) {
    let records = LegacyProducer::three_records();
    message_set
        .message_set
        .set(LegacyProducer::encode(&records));
    message_set.produced.set(records);
}

#[given("a message set of three magic 1 messages cut inside the third {cut}")]
fn three_messages_cut(message_set: &mut MessageSetScenario, cut: CutPoint) {
    let records = LegacyProducer::three_records();
    message_set
        .message_set
        .set(LegacyProducer::encode_cut(&records, cut));
    message_set.produced.set(records);
}

#[given("a message set whose only entry has message size {size}")]
fn only_entry_has_size(message_set: &mut MessageSetScenario, size: i32) {
    message_set.message_set.set(entry_with_size(size));
    message_set.entry_size.set(size);
}

#[when("the broker decodes the message set")]
fn broker_decodes(message_set: &mut MessageSetScenario) {
    let decoded = Broker::decode(message_set.message_set.get());
    message_set.decoded.set(decoded);
}

#[then("the broker reads {count} records")]
fn broker_reads_records(message_set: &MessageSetScenario, count: Count) {
    let expected = &message_set.produced.get()[..usize::from(count)];
    assert!(let Ok(records) = message_set.decoded.get());
    assert!(records == expected);
}

#[then("the message set is rejected as invalid")]
fn rejected_as_invalid(message_set: &MessageSetScenario) {
    assert!(let Err(error) = message_set.decoded.get());
    assert!(Rejection::from(error) == Rejection::for_entry_size(*message_set.entry_size.get()));
}

krabka_bdd::scenarios!(
    "tests/features/message_sets",
    fixtures = [message_set: MessageSetScenario],
);
