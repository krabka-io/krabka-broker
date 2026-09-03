//! `message.timestamp.before.max.ms` and `message.timestamp.after.max.ms`
//! applied to a produced batch, on both the verbatim and the owned path.
//!
//! Every case resolves its policy through
//! [`resolve_timestamp_policy`](crate::handlers::produce::topic_settings), the
//! function the produce handler resolves it with, so a case fails if the
//! config key stops reaching the gate as much as if the gate stops applying.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use assert2::check;
use bytes::Bytes;
use krabka_compression::{CompressionType, RecordDecompressionPolicy};
use krabka_metadata::{MetadataRecord, TopicConfigRecord};
use krabka_protocol::records::{Record, RecordBatch};

use crate::{
    codes,
    config_keys::{
        MESSAGE_TIMESTAMP_AFTER_MAX_MS, MESSAGE_TIMESTAMP_BEFORE_MAX_MS, MESSAGE_TIMESTAMP_TYPE,
    },
    handlers::produce::{
        framing::PartitionPayload,
        prepare::prepare_batch,
        test_support::{encode_batch, image_with_topic},
        topic_settings::{TimestampPolicy, resolve_timestamp_policy},
    },
};

/// An hour, the window every case configures.
const WINDOW_MS: i64 = 3_600_000;

/// This broker's wall clock, which is the one the gate reads.
fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is at or after the epoch")
            .as_millis(),
    )
    .expect("a millisecond clock reading fits in i64")
}

/// The policy a topic carrying `overrides` resolves to.
fn policy(overrides: &[(&str, &str)]) -> TimestampPolicy {
    let mut img = image_with_topic("t", &[1]);
    let mut map = BTreeMap::new();
    for (key, value) in overrides {
        map.insert((*key).to_string(), (*value).to_string());
    }
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "t".into(),
        overrides: map,
    }));
    resolve_timestamp_policy(&img, "t")
}

/// A two-record batch whose records carry `first_ms` and `second_ms`, stored
/// the way the v2 format stores them: one `base_timestamp` plus a delta each.
fn batch(first_ms: i64, second_ms: i64, compression: CompressionType) -> RecordBatch {
    RecordBatch {
        attributes: krabka_protocol::records::Attributes::default().with_compression(compression),
        last_offset_delta: 1,
        base_timestamp: first_ms,
        max_timestamp: first_ms.max(second_ms),
        records: vec![
            Record {
                offset_delta: 0,
                timestamp_delta: 0,
                value: Some(Bytes::from_static(b"a")),
                ..Record::default()
            },
            Record {
                offset_delta: 1,
                timestamp_delta: second_ms - first_ms,
                value: Some(Bytes::from_static(b"b")),
                ..Record::default()
            },
        ],
        ..RecordBatch::default()
    }
}

/// Run one batch through the real prepare decision under `timestamps`.
///
/// `topic_compression` is what decides the append shape: `None` is producer
/// pass-through, which keeps the batch verbatim, and a codec that differs from
/// the batch's own forces the owned fallback.
fn prepare(
    batch: &RecordBatch,
    timestamps: TimestampPolicy,
    topic_compression: Option<CompressionType>,
) -> Result<(), i16> {
    let metrics = crate::metrics::BrokerMetrics::new();
    prepare_batch(
        PartitionPayload::Slice(encode_batch(batch)),
        topic_compression,
        timestamps,
        &Arc::from("t"),
        &metrics,
        RecordDecompressionPolicy::default(),
    )
    .map(|_| ())
}

/// The window refuses a record outside it and admits one inside it, and it
/// does so on both append shapes: the same batch, the same policy, and the
/// same verdict whether the bytes pass through or are decoded.
#[test]
fn the_window_refuses_the_same_batch_on_both_append_paths() {
    let now = now_ms();
    let cases = [
        (
            "both records inside the window",
            [now - 1_000, now + 1_000],
            Ok(()),
        ),
        (
            "the first record is older than before.max.ms",
            [now - 2 * WINDOW_MS, now],
            Err(codes::INVALID_TIMESTAMP),
        ),
        (
            "the second record is older than before.max.ms",
            [now, now - 2 * WINDOW_MS],
            Err(codes::INVALID_TIMESTAMP),
        ),
        (
            "the second record is further ahead than after.max.ms",
            [now, now + 2 * WINDOW_MS],
            Err(codes::INVALID_TIMESTAMP),
        ),
    ];
    let bounded = policy(&[
        (MESSAGE_TIMESTAMP_BEFORE_MAX_MS, &WINDOW_MS.to_string()),
        (MESSAGE_TIMESTAMP_AFTER_MAX_MS, &WINDOW_MS.to_string()),
    ]);
    for (label, [first, second], want) in cases {
        let produced = batch(first, second, CompressionType::None);
        check!(
            prepare(&produced, bounded, None) == want,
            "verbatim: {label}"
        );
        check!(
            prepare(&produced, bounded, Some(CompressionType::Zstd)) == want,
            "owned: {label}"
        );
    }
}

/// A topic that configured neither window admits a timestamp from any era,
/// which is Kafka's default and every topic that did not ask for the check.
#[test]
fn the_default_policy_admits_every_timestamp() {
    let unbounded = TimestampPolicy::default();
    let ancient = batch(1_000, 2_000, CompressionType::None);
    let distant = batch(i64::MAX / 2, i64::MAX / 2, CompressionType::None);

    check!(prepare(&ancient, unbounded, None) == Ok(()));
    check!(prepare(&distant, unbounded, None) == Ok(()));
    check!(prepare(&ancient, unbounded, Some(CompressionType::Zstd)) == Ok(()));
}

/// A `LogAppendTime` topic ignores the window, because the append overwrites
/// every producer timestamp anyway. Kafka's `validateTimestamp` tests a
/// record's timestamp only under `CreateTime`, for the same reason.
#[test]
fn log_append_time_ignores_the_window() {
    let stamping = policy(&[
        (MESSAGE_TIMESTAMP_TYPE, "LogAppendTime"),
        (MESSAGE_TIMESTAMP_BEFORE_MAX_MS, &WINDOW_MS.to_string()),
    ]);
    let ancient = batch(1_000, 2_000, CompressionType::None);

    check!(prepare(&ancient, stamping, None) == Ok(()));
    check!(prepare(&ancient, stamping, Some(CompressionType::Zstd)) == Ok(()));
}

/// A compressed batch keeps its verbatim bytes, and the window still reads the
/// timestamps inside the compressed body rather than admitting it unchecked.
#[test]
fn the_window_reads_a_compressed_body_on_the_verbatim_path() {
    let now = now_ms();
    let bounded = policy(&[(MESSAGE_TIMESTAMP_BEFORE_MAX_MS, &WINDOW_MS.to_string())]);
    let refused = batch(now, now - 2 * WINDOW_MS, CompressionType::Lz4);
    let admitted = batch(now, now - 1_000, CompressionType::Lz4);

    check!(prepare(&refused, bounded, None) == Err(codes::INVALID_TIMESTAMP));
    check!(prepare(&admitted, bounded, None) == Ok(()));
}
