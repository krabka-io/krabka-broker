#![cfg(test)]
//! Scenarios for `tests/features/down_conversion`: a stored v2 record batch
//! down-converts to the legacy message set a Fetch v0 to v3 consumer reads.

use {
    assert2::assert,
    bytes::{Buf, BufMut, Bytes, BytesMut},
    krabka_bdd::Given,
    krabka_compression::CompressionType,
    krabka_protocol::records::{Attributes, Record, RecordBatch, RecordHeader, TimestampType},
    krabka_records_legacy::{LegacyRecordsError, Magic, Message, attrs, v2_to_legacy},
    rstest_bdd_macros::{given, then, when},
};

const GIVEN_BATCH: &str = "Given a v2 batch";
const WHEN_DOWN_CONVERTS: &str = "When the broker down-converts the batch to magic <magic>";

/// The transaction coordinator epoch the commit marker names.
const COORDINATOR_EPOCH: i32 = 7;

/// The producer that wrote the transactional batch the marker commits.
const TRANSACTIONAL_PRODUCER_ID: i64 = 4000;

/// Where the batches with no named base offset start.
const BASE_OFFSET: i64 = 500;

/// The create time of the batches with no named base timestamp.
const BASE_TIMESTAMP: i64 = 1_700_000_000_000;

/// A magic byte as a scenario writes it.
#[derive(Debug, Clone, Copy, krabka_bdd::StepParam)]
enum TargetMagic {
    #[step_param("0")]
    Zero,
    #[step_param("1")]
    One,
}

impl From<TargetMagic> for Magic {
    fn from(magic: TargetMagic) -> Self {
        match magic {
            TargetMagic::Zero => Self::V0,
            TargetMagic::One => Self::V1,
        }
    }
}

/// A codec that a v2 batch can name.
#[derive(Debug, Clone, Copy, krabka_bdd::StepParam)]
enum BatchCodec {
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl From<BatchCodec> for CompressionType {
    fn from(codec: BatchCodec) -> Self {
        match codec {
            BatchCodec::Gzip => Self::Gzip,
            BatchCodec::Snappy => Self::Snappy,
            BatchCodec::Lz4 => Self::Lz4,
            BatchCodec::Zstd => Self::Zstd,
        }
    }
}

/// A codec that both a v2 batch and a legacy wrapper can name.
#[derive(Debug, Clone, Copy, krabka_bdd::StepParam)]
enum LegacyCodec {
    Gzip,
    Snappy,
    Lz4,
}

impl From<LegacyCodec> for CompressionType {
    fn from(codec: LegacyCodec) -> Self {
        match codec {
            LegacyCodec::Gzip => Self::Gzip,
            LegacyCodec::Snappy => Self::Snappy,
            LegacyCodec::Lz4 => Self::Lz4,
        }
    }
}

/// The broker's Fetch path for a consumer that reads a legacy magic.
struct Broker;

impl Broker {
    fn down_convert(batch: &RecordBatch, magic: Magic) -> Result<Bytes, LegacyRecordsError> {
        v2_to_legacy(batch, magic)
    }
}

/// One top-level `(offset, message)` entry of a legacy message set.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LegacyEntry {
    offset: i64,
    message: Message,
}

impl LegacyEntry {
    /// The timestamp type that bit 3 of the attributes byte names.
    fn timestamp_type(&self) -> TimestampType {
        if self.message.attributes & attrs::TIMESTAMP_TYPE_BIT == 0 {
            TimestampType::CreateTime
        } else {
            TimestampType::LogAppendTime
        }
    }
}

/// A legacy message set as a consumer reads it off the wire, entry by entry,
/// without unwrapping a compressed wrapper.
struct LegacyMessageSet {
    entries: Vec<LegacyEntry>,
}

impl LegacyMessageSet {
    fn read(wire: &[u8]) -> Self {
        let mut cur = wire;
        let mut entries = Vec::new();
        while !cur.is_empty() {
            let offset = cur.get_i64();
            let size = usize::try_from(cur.get_i32()).expect("a non-negative message size");
            let message =
                Message::decode_from(&mut cur, size).expect("a well-formed legacy message");
            entries.push(LegacyEntry { offset, message });
        }
        Self { entries }
    }

    fn entry_at(&self, offset: i64) -> Option<&LegacyEntry> {
        self.entries.iter().find(|entry| entry.offset == offset)
    }

    fn compressions(&self) -> Vec<CompressionType> {
        self.entries
            .iter()
            .map(|entry| entry.message.compression())
            .collect()
    }

    fn timestamps(&self) -> Vec<Option<i64>> {
        self.entries
            .iter()
            .map(|entry| entry.message.timestamp)
            .collect()
    }

    fn timestamp_stamps(&self) -> Vec<(TimestampType, Option<i64>)> {
        self.entries
            .iter()
            .map(|entry| (entry.timestamp_type(), entry.message.timestamp))
            .collect()
    }
}

/// A plain record with a key and a value at the given deltas.
fn record(offset_delta: i32, timestamp_delta: i64) -> Record {
    Record {
        attributes: 0,
        timestamp_delta,
        offset_delta,
        key: Some(Bytes::from(format!("key-{offset_delta}"))),
        value: Some(Bytes::from(format!("value-{offset_delta}"))),
        headers: Vec::new(),
    }
}

/// A non-transactional v2 batch around `records`, whose deltas set the last
/// offset delta and the max timestamp.
fn batch_of(
    base_offset: i64,
    base_timestamp: i64,
    attributes: Attributes,
    records: Vec<Record>,
) -> RecordBatch {
    RecordBatch {
        base_offset,
        partition_leader_epoch: 3,
        attributes,
        last_offset_delta: records.iter().map(|r| r.offset_delta).max().unwrap_or(0),
        base_timestamp,
        max_timestamp: base_timestamp
            + records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0),
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records,
    }
}

/// Three records at offset deltas 0, 1 and 2.
fn three_records() -> Vec<Record> { vec![record(0, 0), record(1, 10), record(2, 20)] }

/// A control batch holding one `EndTransactionMarker` of type COMMIT, as
/// Kafka's transaction coordinator writes it.
fn commit_marker_batch() -> RecordBatch {
    let mut key = BytesMut::new();
    key.put_i16(0); // marker key version
    key.put_i16(1); // ControlRecordType.COMMIT
    let mut value = BytesMut::new();
    value.put_i16(0); // marker value version
    value.put_i32(COORDINATOR_EPOCH);
    RecordBatch {
        producer_id: TRANSACTIONAL_PRODUCER_ID,
        producer_epoch: 0,
        ..batch_of(
            BASE_OFFSET,
            BASE_TIMESTAMP,
            Attributes::default()
                .with_transactional(true)
                .with_control(true),
            vec![Record {
                key: Some(key.freeze()),
                value: Some(value.freeze()),
                ..record(0, 0)
            }],
        )
    }
}

struct DownConversion {
    batch: Given<RecordBatch>,
    legacy: Given<Result<Bytes, LegacyRecordsError>>,
}

impl DownConversion {
    /// The legacy bytes of a down-conversion that the scenario expects to
    /// succeed.
    fn legacy_bytes(&self) -> &Bytes {
        self.legacy
            .get()
            .as_ref()
            .expect("a well-formed v2 batch down-converts")
    }

    fn message_set(&self) -> LegacyMessageSet { LegacyMessageSet::read(self.legacy_bytes()) }
}

#[rstest::fixture]
fn down_conversion() -> DownConversion {
    DownConversion {
        batch: Given::new("batch", GIVEN_BATCH),
        legacy: Given::new("legacy", WHEN_DOWN_CONVERTS),
    }
}

#[given("a v2 control batch with a commit marker")]
fn control_batch_with_commit_marker(down_conversion: &mut DownConversion) {
    down_conversion.batch.set(commit_marker_batch());
}

#[given("a v2 zstd control batch with a commit marker")]
fn zstd_control_batch_with_commit_marker(down_conversion: &mut DownConversion) {
    let marker = commit_marker_batch();
    down_conversion.batch.set(RecordBatch {
        attributes: marker.attributes.with_compression(CompressionType::Zstd),
        ..marker
    });
}

#[given(
    "a v2 batch at base offset {base_offset} whose record at offset delta {offset_delta} has a key, a value and headers"
)]
fn batch_with_headers(down_conversion: &mut DownConversion, base_offset: i64, offset_delta: i32) {
    let mut records: Vec<Record> = (0..=offset_delta).map(|delta| record(delta, 0)).collect();
    if let Some(last) = records.last_mut() {
        last.headers = vec![
            RecordHeader {
                key: "trace-id".to_owned(),
                value: Some(Bytes::from_static(b"abc")),
            },
            RecordHeader {
                key: "tombstone-reason".to_owned(),
                value: None,
            },
        ];
    }
    down_conversion.batch.set(batch_of(
        base_offset,
        BASE_TIMESTAMP,
        Attributes::default(),
        records,
    ));
}

#[given("a v2 batch with base timestamp {base_timestamp} whose record has timestamp delta {delta}")]
fn batch_with_timestamp_delta(
    down_conversion: &mut DownConversion,
    base_timestamp: i64,
    delta: i64,
) {
    down_conversion.batch.set(batch_of(
        BASE_OFFSET,
        base_timestamp,
        Attributes::default(),
        vec![record(0, delta)],
    ));
}

#[given("a v2 batch compressed with {codec}")]
fn compressed_batch(down_conversion: &mut DownConversion, codec: BatchCodec) {
    down_conversion.batch.set(batch_of(
        BASE_OFFSET,
        BASE_TIMESTAMP,
        Attributes::default().with_compression(codec.into()),
        three_records(),
    ));
}

#[given("a v2 batch compressed with zstd that holds no records")]
fn empty_zstd_batch(down_conversion: &mut DownConversion) {
    down_conversion.batch.set(batch_of(
        BASE_OFFSET,
        BASE_TIMESTAMP,
        Attributes::default().with_compression(CompressionType::Zstd),
        Vec::new(),
    ));
}

#[given("a v2 batch of three uncompressed records")]
fn uncompressed_batch(down_conversion: &mut DownConversion) {
    down_conversion.batch.set(batch_of(
        BASE_OFFSET,
        BASE_TIMESTAMP,
        Attributes::default(),
        three_records(),
    ));
}

/// The records keep the create-time deltas the producer sent, and the max
/// timestamp holds the log append time, as Kafka's `LogValidator` leaves a
/// `LOG_APPEND_TIME` batch.
#[given("a v2 LogAppendTime batch with max timestamp {max_timestamp}")]
fn log_append_time_batch(down_conversion: &mut DownConversion, max_timestamp: i64) {
    down_conversion.batch.set(RecordBatch {
        max_timestamp,
        ..batch_of(
            BASE_OFFSET,
            BASE_TIMESTAMP,
            Attributes::default().with_timestamp_type(TimestampType::LogAppendTime),
            three_records(),
        )
    });
}

#[when("the broker down-converts the batch to magic {magic}")]
fn down_convert(down_conversion: &mut DownConversion, magic: TargetMagic) {
    let legacy = Broker::down_convert(down_conversion.batch.get(), magic.into());
    down_conversion.legacy.set(legacy);
}

#[then("the legacy message set is empty")]
fn message_set_is_empty(down_conversion: &DownConversion) {
    assert!(*down_conversion.legacy_bytes() == Bytes::new());
}

/// Kafka's `KafkaApis` answers the partition with this error code when
/// `RecordsUtil.downConvert` throws `UnsupportedCompressionTypeException`.
#[then("the down-conversion is refused with UNSUPPORTED_COMPRESSION_TYPE")]
fn refused_as_unsupported_compression(down_conversion: &DownConversion) {
    assert!(
        let Err(LegacyRecordsError::UnsupportedCompression {
            codec: CompressionType::Zstd
        }) = down_conversion.legacy.get()
    );
}

#[then("the legacy record at offset {offset} has the same key and value")]
fn record_keeps_key_and_value(down_conversion: &DownConversion, offset: i64) {
    let batch = down_conversion.batch.get();
    let original = batch
        .records
        .iter()
        .find(|r| batch.base_offset + i64::from(r.offset_delta) == offset)
        .expect("the given batch holds a record at that offset");
    let set = down_conversion.message_set();
    let legacy = set
        .entry_at(offset)
        .map(|entry| (entry.message.key.clone(), entry.message.value.clone()));
    assert!(legacy == Some((original.key.clone(), original.value.clone())));
}

#[then("the legacy record has no timestamp")]
fn record_has_no_timestamp(down_conversion: &DownConversion) {
    assert!(down_conversion.message_set().timestamps() == vec![None]);
}

#[then("the legacy record has timestamp {timestamp}")]
fn record_has_timestamp(down_conversion: &DownConversion, timestamp: i64) {
    assert!(down_conversion.message_set().timestamps() == vec![Some(timestamp)]);
}

#[then("the legacy message set is one wrapper compressed with {codec}")]
fn one_compressed_wrapper(down_conversion: &DownConversion, codec: LegacyCodec) {
    assert!(down_conversion.message_set().compressions() == vec![CompressionType::from(codec)]);
}

#[then("the legacy message set is three uncompressed messages")]
fn three_uncompressed_messages(down_conversion: &DownConversion) {
    assert!(down_conversion.message_set().compressions() == vec![CompressionType::None; 3]);
}

#[then("every legacy message is LogAppendTime with timestamp {timestamp}")]
fn every_message_is_log_append_time(down_conversion: &DownConversion, timestamp: i64) {
    let records = down_conversion.batch.get().records.len();
    let expected = vec![(TimestampType::LogAppendTime, Some(timestamp)); records];
    assert!(down_conversion.message_set().timestamp_stamps() == expected);
}

krabka_bdd::scenarios!(
    "tests/features/down_conversion",
    fixtures = [down_conversion: DownConversion],
);
