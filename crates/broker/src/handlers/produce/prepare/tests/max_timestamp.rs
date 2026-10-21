//! The `max_timestamp` a produced `CreateTime` batch is stored with.
//!
//! Kafka's `LogValidator` does not keep the producer's header value. Both
//! `assignOffsetsNonCompressed` and the in-place branch of
//! `validateMessagesAndAssignOffsetsCompressed` fold the record timestamps from
//! `RecordBatch.NO_TIMESTAMP` and call `batch.setMaxTimestamp` with the
//! result, and a re-encoded batch gets the same value from
//! `MemoryRecordsBuilder`. A KFC-1 scheduled topic is the one exception: its
//! header field is the delivery time.

use {
    crate::{
        config_keys::{
            DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, MESSAGE_TIMESTAMP_AFTER_MAX_MS,
            MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
        },
        handlers::produce::{
            framing::PartitionPayload,
            prepare::{DecodeEnv, PreparedSource, prepare_batch},
            test_support::{ConfigOverride, encode_batch, image_with_overrides},
            topic_settings::{
                TimestampPolicy, kafka_stock_broker_default, resolve_timestamp_policy,
            },
        },
        metrics::BrokerMetrics,
    },
    assert2::check,
    bytes::Bytes,
    krabka_compression::{CompressionType, RecordDecompressionPolicy},
    krabka_protocol::records::{Attributes, Record, RecordBatch},
    std::sync::Arc,
};

/// The policy of a topic carrying `overrides`, resolved the way the produce
/// handler resolves it, with no timestamp window so that no case is refused.
fn policy(overrides: &[ConfigOverride<'_>]) -> TimestampPolicy {
    let open_windows = [
        ConfigOverride {
            key: MESSAGE_TIMESTAMP_AFTER_MAX_MS,
            value: "9223372036854775807",
        },
        ConfigOverride {
            key: MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
            value: "9223372036854775807",
        },
    ];
    let all: Vec<ConfigOverride<'_>> = open_windows.iter().chain(overrides).copied().collect();
    resolve_timestamp_policy(
        &image_with_overrides("t", &all),
        "t",
        kafka_stock_broker_default(),
    )
}

/// How the batch reaches the writer.
#[derive(Debug, Clone, Copy)]
enum Shape {
    /// The producer's bytes, under this codec, on a pass-through topic.
    Verbatim(CompressionType),
    /// Uncompressed, on a topic that stores zstd, so the batch is decoded and
    /// re-encoded.
    Owned,
}

/// A batch whose header claims `header_max` and whose records carry
/// `timestamps`, stored as the v2 format stores them: the first timestamp as
/// `base_timestamp` and a delta per record.
fn batch(shape: Shape, header_max: i64, timestamps: &[i64]) -> RecordBatch {
    let base = timestamps.first().copied().unwrap_or(0);
    let codec = match shape {
        Shape::Verbatim(codec) => codec,
        Shape::Owned => CompressionType::None,
    };
    RecordBatch {
        attributes: Attributes::default().with_compression(codec),
        last_offset_delta: i32::try_from(timestamps.len()).unwrap() - 1,
        base_timestamp: base,
        max_timestamp: header_max,
        records: timestamps
            .iter()
            .zip(0_i32..)
            .map(|(timestamp, offset_delta)| Record {
                offset_delta,
                timestamp_delta: timestamp - base,
                value: Some(Bytes::from_static(b"v")),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
}

/// What one case stores: the batch the writer receives, decoded from the
/// verbatim bytes (which re-checks their CRC) or taken as the owned batch,
/// the `max_timestamp` the produce gates see, and whether the verbatim bytes
/// are still the producer's own buffer.
#[derive(Debug, PartialEq)]
struct Stored {
    batch: RecordBatch,
    max_timestamp: i64,
    zero_copy: Option<bool>,
}

fn store(shape: Shape, timestamps: TimestampPolicy, produced: &RecordBatch) -> Stored {
    let wire = encode_batch(produced);
    let topic_compression = match shape {
        Shape::Verbatim(_) => None,
        Shape::Owned => Some(CompressionType::Zstd),
    };
    let prepared = prepare_batch(
        PartitionPayload::Slice(wire.clone()),
        topic_compression,
        timestamps,
        false,
        DecodeEnv {
            topic_name: &Arc::from("t"),
            metrics: &BrokerMetrics::new(),
            policy: RecordDecompressionPolicy::default(),
        },
        13,
    )
    .expect("a well-formed batch");
    let (batch, zero_copy) = match prepared.source {
        PreparedSource::Verbatim(bytes) => (
            RecordBatch::decode(&mut &bytes[..]).expect("the stored bytes carry a valid CRC"),
            Some(bytes.as_ptr() == wire.as_ptr()),
        ),
        PreparedSource::Owned(batch) => (batch, None),
    };
    Stored {
        batch,
        max_timestamp: prepared.max_timestamp,
        zero_copy,
    }
}

/// One case: the topic's policy overrides, the append shape, the producer's
/// header value and record timestamps, and the `max_timestamp` stored.
struct Case {
    label: &'static str,
    overrides: &'static [ConfigOverride<'static>],
    shape: Shape,
    header_max: i64,
    timestamps: &'static [i64],
    want_max: i64,
}

const SCHEDULED: &[ConfigOverride<'static>] = &[ConfigOverride {
    key: DELIVERY_MODE,
    value: DELIVERY_MODE_SCHEDULED,
}];

#[test]
fn a_create_time_batch_stores_its_largest_record_timestamp() {
    let lz4 = Shape::Verbatim(CompressionType::Lz4);
    let plain = Shape::Verbatim(CompressionType::None);
    let cases = [
        Case {
            label: "#1388: an LZ4 header of 7777 over one record at 0 stores 0",
            overrides: &[],
            shape: lz4,
            header_max: 7_777,
            timestamps: &[0],
            want_max: 0,
        },
        Case {
            label: "an uncompressed header that overstates its records",
            overrides: &[],
            shape: plain,
            header_max: 7_777,
            timestamps: &[100, 300, 200],
            want_max: 300,
        },
        Case {
            label: "a header that understates its records",
            overrides: &[],
            shape: lz4,
            header_max: 0,
            timestamps: &[100, 300, 200],
            want_max: 300,
        },
        Case {
            label: "a header that agrees keeps the producer's own bytes",
            overrides: &[],
            shape: lz4,
            header_max: 300,
            timestamps: &[100, 300, 200],
            want_max: 300,
        },
        Case {
            label: "records all below NO_TIMESTAMP fold to NO_TIMESTAMP",
            overrides: &[],
            shape: plain,
            header_max: -5,
            timestamps: &[-7, -5],
            want_max: -1,
        },
        Case {
            label: "the owned path re-encodes with the largest record timestamp",
            overrides: &[],
            shape: Shape::Owned,
            header_max: 7_777,
            timestamps: &[0],
            want_max: 0,
        },
        Case {
            label: "KFC-1: a scheduled topic keeps the header as the delivery time",
            overrides: SCHEDULED,
            shape: lz4,
            header_max: 7_777,
            timestamps: &[0],
            want_max: 7_777,
        },
        Case {
            label: "KFC-1: a scheduled topic keeps it on the owned path too",
            overrides: SCHEDULED,
            shape: Shape::Owned,
            header_max: 7_777,
            timestamps: &[0],
            want_max: 7_777,
        },
    ];
    for Case {
        label,
        overrides,
        shape,
        header_max,
        timestamps,
        want_max,
    } in cases
    {
        let produced = batch(shape, header_max, timestamps);
        let actual = store(shape, policy(overrides), &produced);
        let expected = Stored {
            batch: RecordBatch {
                max_timestamp: want_max,
                ..produced
            },
            max_timestamp: want_max,
            zero_copy: match shape {
                Shape::Verbatim(_) => Some(want_max == header_max),
                Shape::Owned => None,
            },
        };
        check!(actual == expected, "{label}");
    }
}
