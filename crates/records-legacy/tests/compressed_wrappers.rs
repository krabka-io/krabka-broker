//! Scenarios for compressed legacy wrappers: one compressed outer message
//! around a nested message set, as Kafka's `AbstractLegacyRecordBatch` and
//! `MemoryRecordsBuilder` read and write it.
//!
//! The decode scenarios assemble the wrapper on the wire by hand with
//! [`Wrapper`], so they do not depend on the compressed-set encoder. The
//! producer scenarios drive that encoder through [`LegacyProducer`] and read
//! the raw wrapper back with [`ProducedWrapper::read`].

use {
    assert2::assert,
    bytes::{Buf, BufMut, Bytes, BytesMut},
    krabka_bdd::{Given, StepParam},
    krabka_compression::{CompressionType, compress, decompress},
    krabka_ids::Offset,
    krabka_records_legacy::{
        LegacyRecordsError, Magic, Message, ParsedRecord, attrs, attrs_with_compression,
        decode_message_set, encode_compressed_message_set, encode_flat_message_set,
    },
    krabka_units::mebibytes,
    rstest_bdd_macros::{given, then, when},
    std::{num::ParseIntError, str::FromStr},
};

const GIVEN_MESSAGE_SET: &str =
    "Given a magic 1 wrapper at offset <wrapper> around inner offsets <inner>";
const GIVEN_RECORDS: &str = "Given records at offsets <offsets>";
const GIVEN_WRAPPED: &str = "Given a magic 1 wrapper compressed with <codec>";
const WHEN_DECODED: &str = "When the broker decodes the message set";
const WHEN_PRODUCED: &str =
    "When a legacy producer compresses them with <codec> under magic <magic>";

/// A codec that the three compression bits of a legacy message can name.
#[derive(Clone, Copy, Debug, StepParam)]
enum Codec {
    Gzip,
    Snappy,
    Lz4,
}

impl From<Codec> for CompressionType {
    fn from(codec: Codec) -> Self {
        match codec {
            Codec::Gzip => Self::Gzip,
            Codec::Snappy => Self::Snappy,
            Codec::Lz4 => Self::Lz4,
        }
    }
}

/// The magic byte as a scenario writes it.
#[derive(Clone, Copy, Debug, StepParam)]
enum MagicByte {
    #[step_param("0")]
    Zero,
    #[step_param("1")]
    One,
}

impl From<MagicByte> for Magic {
    fn from(magic: MagicByte) -> Self {
        match magic {
            MagicByte::Zero => Self::V0,
            MagicByte::One => Self::V1,
        }
    }
}

/// The timestamp type bit of a magic 1 wrapper.
#[derive(Clone, Copy, Debug, StepParam)]
enum TimestampType {
    #[step_param("CreateTime")]
    CreateTime,
    #[step_param("LogAppendTime")]
    LogAppendTime,
}

impl TimestampType {
    const fn attribute_bits(self) -> i8 {
        match self {
            Self::CreateTime => 0,
            Self::LogAppendTime => attrs::TIMESTAMP_TYPE_BIT,
        }
    }
}

/// Parses a scenario list such as `0, 1, 2` or `100, 101 and 102`.
fn parse_list(text: &str) -> Result<Vec<i64>, ParseIntError> {
    text.replace(" and ", ", ")
        .split(',')
        .map(|item| item.trim().parse())
        .collect()
}

/// Offsets as a scenario lists them.
#[derive(Debug)]
struct Offsets(Vec<i64>);

impl FromStr for Offsets {
    type Err = ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> { parse_list(text).map(Self) }
}

/// Timestamps as a scenario lists them.
#[derive(Debug)]
struct Timestamps(Vec<i64>);

impl FromStr for Timestamps {
    type Err = ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> { parse_list(text).map(Self) }
}

/// One record per offset, each with its own value and timestamp.
fn records(offsets: &[i64], timestamps: &[i64]) -> Vec<ParsedRecord> {
    offsets
        .iter()
        .zip(timestamps)
        .enumerate()
        .map(|(position, (&offset, &timestamp))| ParsedRecord {
            offset: Offset(offset),
            timestamp: Some(timestamp),
            key: None,
            value: Some(Bytes::from(format!("record {position}"))),
        })
        .collect()
}

/// The same records at other offsets.
fn moved_to(records: &[ParsedRecord], offsets: &[i64]) -> Vec<ParsedRecord> {
    records
        .iter()
        .zip(offsets)
        .map(|(record, &offset)| ParsedRecord {
            offset: Offset(offset),
            ..record.clone()
        })
        .collect()
}

/// Timestamps that do not change the outcome: one per record, in order.
fn timestamps_for(offsets: &[i64]) -> Vec<i64> {
    offsets.iter().map(|offset| 1_000 + offset).collect()
}

/// Writes one `(offset, size, message)` frame.
fn put_frame(offset: i64, message: &Message, buf: &mut BytesMut) {
    assert!(let Ok(size) = i32::try_from(message.encoded_len()));
    buf.put_i64(offset);
    buf.put_i32(size);
    message.encode_into(buf);
}

/// A compressed wrapper message, assembled on the wire by hand.
struct Wrapper {
    magic: Magic,
    codec: Codec,
    compression_code: Option<i8>,
    timestamp_type: TimestampType,
    offset: i64,
    timestamp: i64,
    inner_set: Bytes,
}

impl Wrapper {
    /// An empty `CreateTime` wrapper at offset 0 with timestamp 0.
    fn new(magic: Magic, codec: Codec) -> Self {
        Self {
            magic,
            codec,
            compression_code: None,
            timestamp_type: TimestampType::CreateTime,
            offset: 0,
            timestamp: 0,
            inner_set: Bytes::new(),
        }
    }

    fn at_offset(self, offset: i64) -> Self { Self { offset, ..self } }

    fn with_timestamp(self, timestamp_type: TimestampType, timestamp: i64) -> Self {
        Self {
            timestamp_type,
            timestamp,
            ..self
        }
    }

    /// Writes `code` into the compression bits instead of the codec's code.
    /// The value is still compressed with the codec.
    fn with_compression_code(self, code: i8) -> Self {
        Self {
            compression_code: Some(code),
            ..self
        }
    }

    /// Wraps `inner` as inner messages of `inner_magic`, at the offsets the
    /// records carry.
    fn around(self, inner: &[ParsedRecord], inner_magic: Magic) -> Self {
        let mut set = BytesMut::new();
        encode_flat_message_set(inner.to_vec(), inner_magic, &mut set);
        self.around_message_set(set.freeze())
    }

    fn around_message_set(self, inner_set: Bytes) -> Self { Self { inner_set, ..self } }

    fn to_wire(&self) -> Bytes {
        let codec_bits =
            attrs_with_compression(self.timestamp_type.attribute_bits(), self.codec.into());
        let attributes = self.compression_code.map_or(codec_bits, |code| {
            (codec_bits & !attrs::COMPRESSION_MASK) | code
        });
        assert!(let Ok(value) = compress(self.codec.into(), &self.inner_set));
        let message = Message {
            magic: self.magic,
            attributes,
            timestamp: match self.magic {
                Magic::V0 => None,
                Magic::V1 => Some(self.timestamp),
            },
            key: None,
            value: Some(value),
        };
        let mut wire = BytesMut::new();
        put_frame(self.offset, &message, &mut wire);
        wire.freeze()
    }
}

/// A legacy producer that writes one compressed wrapper per batch.
struct LegacyProducer;

impl LegacyProducer {
    fn compress(records: &[ParsedRecord], codec: Codec, magic: Magic) -> Bytes {
        let mut wire = BytesMut::new();
        assert!(let
            Ok(()) = encode_compressed_message_set(records, magic, codec.into(), &mut wire)
        );
        wire.freeze()
    }
}

/// What a produced wrapper carries on the wire, before the broker rewrites
/// any inner offset.
struct ProducedWrapper {
    offset: i64,
    timestamp: Option<i64>,
    inner_offsets: Vec<i64>,
}

impl ProducedWrapper {
    fn read(wire: &[u8]) -> Self {
        let mut cur = wire;
        let offset = cur.get_i64();
        assert!(let Ok(size) = usize::try_from(cur.get_i32()));
        assert!(let Ok(wrapper) = Message::decode_from(&mut cur, size));
        assert!(let Some(value) = wrapper.value.as_ref());
        assert!(let Ok(inner_set) = decompress(wrapper.compression(), value, mebibytes(1)));
        let mut inner = &inner_set[..];
        let mut inner_offsets = Vec::new();
        while inner.has_remaining() {
            inner_offsets.push(inner.get_i64());
            assert!(let Ok(inner_size) = usize::try_from(inner.get_i32()));
            inner.advance(inner_size);
        }
        Self {
            offset,
            timestamp: wrapper.timestamp,
            inner_offsets,
        }
    }
}

struct CompressedWrapperScenario {
    message_set: Given<Bytes>,
    wrapped: Given<Vec<ParsedRecord>>,
    records: Given<Vec<ParsedRecord>>,
    decoded: Given<Result<Vec<ParsedRecord>, LegacyRecordsError>>,
    produced: Given<ProducedWrapper>,
}

#[rstest::fixture]
fn compressed_wrappers() -> CompressedWrapperScenario {
    CompressedWrapperScenario {
        message_set: Given::new("message_set", GIVEN_MESSAGE_SET),
        wrapped: Given::new("wrapped", GIVEN_WRAPPED),
        records: Given::new("records", GIVEN_RECORDS),
        decoded: Given::new("decoded", WHEN_DECODED),
        produced: Given::new("produced", WHEN_PRODUCED),
    }
}

#[given("a magic 1 wrapper compressed with {codec}")]
fn magic_1_wrapper_compressed_with(
    compressed_wrappers: &mut CompressedWrapperScenario,
    codec: Codec,
) {
    let inner = records(&[0, 1, 2], &[1_000, 1_001, 1_002]);
    let wire = Wrapper::new(Magic::V1, codec)
        .at_offset(102)
        .around(&inner, Magic::V1)
        .to_wire();
    compressed_wrappers.message_set.set(wire);
    compressed_wrappers
        .wrapped
        .set(moved_to(&inner, &[100, 101, 102]));
}

#[given("a magic 1 message with compression code {code}")]
fn magic_1_message_with_compression_code(
    compressed_wrappers: &mut CompressedWrapperScenario,
    code: i8,
) {
    let inner = records(&[0], &[1_000]);
    let wire = Wrapper::new(Magic::V1, Codec::Gzip)
        .with_compression_code(code)
        .around(&inner, Magic::V1)
        .to_wire();
    compressed_wrappers.message_set.set(wire);
}

#[given("a magic 1 wrapper at offset {wrapper} around inner offsets {inner}")]
fn magic_1_wrapper_at_offset(
    compressed_wrappers: &mut CompressedWrapperScenario,
    wrapper: i64,
    inner: Offsets,
) {
    let inner = records(&inner.0, &timestamps_for(&inner.0));
    let wire = Wrapper::new(Magic::V1, Codec::Gzip)
        .at_offset(wrapper)
        .around(&inner, Magic::V1)
        .to_wire();
    compressed_wrappers.message_set.set(wire);
}

#[given("a gzip wrapper whose inner message set is itself a gzip wrapper")]
fn gzip_wrapper_around_gzip_wrapper(compressed_wrappers: &mut CompressedWrapperScenario) {
    let inner = records(&[0, 1], &[1_000, 1_001]);
    let nested = Wrapper::new(Magic::V1, Codec::Gzip)
        .at_offset(1)
        .around(&inner, Magic::V1)
        .to_wire();
    let wire = Wrapper::new(Magic::V1, Codec::Gzip)
        .at_offset(1)
        .around_message_set(nested)
        .to_wire();
    compressed_wrappers.message_set.set(wire);
}

#[given("a magic 1 gzip wrapper around magic 0 messages")]
fn magic_1_wrapper_around_magic_0_messages(compressed_wrappers: &mut CompressedWrapperScenario) {
    let inner = records(&[0, 1, 2], &[1_000, 1_001, 1_002]);
    let wire = Wrapper::new(Magic::V1, Codec::Gzip)
        .at_offset(2)
        .around(&inner, Magic::V0)
        .to_wire();
    compressed_wrappers.message_set.set(wire);
}

#[given(
    "a magic 1 {timestamp_type} wrapper with timestamp {wrapper_timestamp} around records with timestamps {timestamps}"
)]
fn magic_1_wrapper_with_timestamp(
    compressed_wrappers: &mut CompressedWrapperScenario,
    timestamp_type: TimestampType,
    wrapper_timestamp: i64,
    timestamps: Timestamps,
) {
    let offsets: Vec<i64> = (0..).zip(&timestamps.0).map(|(offset, _)| offset).collect();
    let inner = records(&offsets, &timestamps.0);
    let last = offsets.last().copied().unwrap_or_default();
    let wire = Wrapper::new(Magic::V1, Codec::Gzip)
        .at_offset(last)
        .with_timestamp(timestamp_type, wrapper_timestamp)
        .around(&inner, Magic::V1)
        .to_wire();
    compressed_wrappers.message_set.set(wire);
}

#[given("records at offsets {offsets}")]
fn records_at_offsets(compressed_wrappers: &mut CompressedWrapperScenario, offsets: Offsets) {
    compressed_wrappers
        .records
        .set(records(&offsets.0, &timestamps_for(&offsets.0)));
}

#[given("records with timestamps {timestamps}")]
fn records_with_timestamps(
    compressed_wrappers: &mut CompressedWrapperScenario,
    timestamps: Timestamps,
) {
    let offsets: Vec<i64> = (0..).zip(&timestamps.0).map(|(offset, _)| offset).collect();
    compressed_wrappers
        .records
        .set(records(&offsets, &timestamps.0));
}

#[when("the broker decodes the message set")]
fn broker_decodes(compressed_wrappers: &mut CompressedWrapperScenario) {
    let wire = compressed_wrappers.message_set.get();
    let outcome = decode_message_set(&mut &wire[..], wire.len());
    compressed_wrappers.decoded.set(outcome);
}

#[when("a legacy producer compresses them with {codec} under magic {magic}")]
fn legacy_producer_compresses(
    compressed_wrappers: &mut CompressedWrapperScenario,
    codec: Codec,
    magic: MagicByte,
) {
    let wire = LegacyProducer::compress(compressed_wrappers.records.get(), codec, magic.into());
    compressed_wrappers
        .produced
        .set(ProducedWrapper::read(&wire));
}

#[then("the broker reads every inner record")]
fn broker_reads_every_inner_record(compressed_wrappers: &CompressedWrapperScenario) {
    assert!(let Ok(decoded) = compressed_wrappers.decoded.get());
    assert!(decoded == compressed_wrappers.wrapped.get());
}

#[then("the broker reads records at offsets {offsets}")]
fn broker_reads_records_at_offsets(
    compressed_wrappers: &CompressedWrapperScenario,
    offsets: Offsets,
) {
    assert!(let Ok(decoded) = compressed_wrappers.decoded.get());
    let read: Vec<i64> = decoded.iter().map(|record| record.offset.0).collect();
    assert!(read == offsets.0);
}

#[then("the broker reads records with timestamps {timestamps}")]
fn broker_reads_records_with_timestamps(
    compressed_wrappers: &CompressedWrapperScenario,
    timestamps: Timestamps,
) {
    assert!(let Ok(decoded) = compressed_wrappers.decoded.get());
    let read: Vec<Option<i64>> = decoded.iter().map(|record| record.timestamp).collect();
    let expected: Vec<Option<i64>> = timestamps.0.into_iter().map(Some).collect();
    assert!(read == expected);
}

#[then("the message set is rejected as invalid")]
fn rejected_as_invalid(compressed_wrappers: &CompressedWrapperScenario) {
    assert!(let Err(LegacyRecordsError::Malformed(_)) = compressed_wrappers.decoded.get());
}

#[then("the message set is rejected for nested compression")]
fn rejected_for_nested_compression(compressed_wrappers: &CompressedWrapperScenario) {
    assert!(let Err(LegacyRecordsError::NestedCompression) = compressed_wrappers.decoded.get());
}

#[then("the message set is rejected for inconsistent magic")]
fn rejected_for_inconsistent_magic(compressed_wrappers: &CompressedWrapperScenario) {
    assert!(
        let Err(LegacyRecordsError::InconsistentMagic { outer: 1, inner: 0 }) = compressed_wrappers.decoded.get()
    );
}

#[then("the wrapper offset is {offset}")]
fn wrapper_offset_is(compressed_wrappers: &CompressedWrapperScenario, offset: i64) {
    assert!(compressed_wrappers.produced.get().offset == offset);
}

#[then("the inner offsets are {offsets}")]
fn inner_offsets_are(compressed_wrappers: &CompressedWrapperScenario, offsets: Offsets) {
    assert!(compressed_wrappers.produced.get().inner_offsets == offsets.0);
}

#[then("the wrapper timestamp is {timestamp}")]
fn wrapper_timestamp_is(compressed_wrappers: &CompressedWrapperScenario, timestamp: i64) {
    assert!(compressed_wrappers.produced.get().timestamp == Some(timestamp));
}

krabka_bdd::scenarios!(
    "tests/features/compressed_wrappers",
    fixtures = [compressed_wrappers: CompressedWrapperScenario],
    tags = "not @formulated and not @inprogress"
);
