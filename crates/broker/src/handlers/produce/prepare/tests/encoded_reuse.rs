//! The owned batch's encode, kept by the size gates for the next gate and the
//! writer.
//!
//! An owned batch is measured by encoding it, first against
//! `max.message.bytes` and again against `segment.bytes`, and the writer
//! encodes it a third time to store it. The first gate keeps its bytes; the
//! second gate and the writer reuse them while the codec and the compression
//! level are the ones they were encoded under, and encode afresh when the
//! topic's config moved in between.

use {
    crate::{
        handlers::produce::{
            append::build_produce_data,
            framing::PartitionPayload,
            prepare::{DecodeEnv, PreparedBatch, PreparedSource, prepare_batch},
            test_support::encode_batch,
            topic_settings::TimestampPolicy,
        },
        metrics::BrokerMetrics,
        partition::ProduceData,
    },
    assert2::{assert, check},
    bytes::{Bytes, BytesMut},
    krabka_compression::{CompressionType, RecordDecompressionPolicy},
    krabka_log::LogConfig,
    krabka_protocol::records::{Record, RecordBatch},
    std::sync::Arc,
};

/// Three records whose values every codec shrinks, as a producer sends them:
/// uncompressed, with no producer id.
fn producer_batch() -> RecordBatch {
    let value = Bytes::from(b"a value that repeats, a value that repeats".repeat(8));
    RecordBatch {
        last_offset_delta: 2,
        max_timestamp: 1_000,
        base_timestamp: 1_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: (0..3)
            .map(|offset_delta| Record {
                offset_delta,
                value: Some(value.clone()),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
}

fn prepared(wire: Bytes, topic_compression: Option<CompressionType>) -> PreparedBatch {
    let prepared = prepare_batch(
        PartitionPayload::Slice(wire),
        topic_compression,
        TimestampPolicy::default(),
        false,
        DecodeEnv {
            topic_name: &Arc::from("t"),
            metrics: &BrokerMetrics::new(),
            policy: RecordDecompressionPolicy::default(),
        },
        13,
    )
    .expect("a valid produce payload");
    assert!(matches!(prepared.source, PreparedSource::Owned(_)));
    prepared
}

fn owned(prepared: &PreparedBatch) -> &RecordBatch {
    let PreparedSource::Owned(batch) = &prepared.source else {
        unreachable!("asserted owned");
    };
    batch
}

/// What `batch` encodes to under `codec` at `level`, measured from scratch.
fn fresh(batch: &RecordBatch, codec: CompressionType, level: Option<i32>) -> Bytes {
    let mut target = batch.clone();
    target.attributes = target.attributes.with_compression(codec);
    let mut buf = BytesMut::new();
    target
        .encode_with_compression_level(&mut buf, level)
        .expect("encode");
    buf.freeze()
}

/// `config` with `compression.type` set to `codec`.
fn forcing(codec: Option<CompressionType>) -> LogConfig {
    LogConfig {
        compression_type: codec,
        ..LogConfig::default()
    }
}

/// The two gates measure the stored form, the second reuses the first's
/// bytes exactly when its codec and level are the first's, and the writer then
/// gets bytes that decode to the batch it would have encoded itself.
///
/// Forced gzip and zstd conversion and the legacy up-conversion are the three
/// shapes that reach the owned path through the produce handler. The last two
/// cases change the topic's config between the gates: the second gate then
/// measures what the new config stores, as it did before it kept any bytes.
#[test]
fn the_segment_gate_and_the_writer_reuse_the_message_gate_encode_while_the_config_holds() {
    struct Case {
        name: &'static str,
        wire: Bytes,
        topic_compression: Option<CompressionType>,
        /// The partition's config when `max.message.bytes` is checked.
        message_config: LogConfig,
        /// The partition's config when `segment.bytes` is checked, and when
        /// the writer appends.
        segment_config: LogConfig,
        reused: bool,
    }
    let v2 = encode_batch(&producer_batch());
    let legacy =
        krabka_records_legacy::v2_to_legacy(&producer_batch(), krabka_records_legacy::Magic::V1)
            .expect("down-convert to a v1 message set");
    let gzip = forcing(Some(CompressionType::Gzip));
    let zstd = forcing(Some(CompressionType::Zstd));
    let cases = [
        Case {
            name: "forced gzip",
            wire: v2.clone(),
            topic_compression: Some(CompressionType::Gzip),
            message_config: gzip.clone(),
            segment_config: gzip.clone(),
            reused: true,
        },
        Case {
            name: "forced zstd",
            wire: v2.clone(),
            topic_compression: Some(CompressionType::Zstd),
            message_config: zstd.clone(),
            segment_config: zstd.clone(),
            reused: true,
        },
        Case {
            name: "legacy up-conversion on a producer topic",
            wire: legacy,
            topic_compression: None,
            message_config: forcing(None),
            segment_config: forcing(None),
            reused: true,
        },
        Case {
            name: "gzip level changed between the gates",
            wire: v2.clone(),
            topic_compression: Some(CompressionType::Gzip),
            message_config: LogConfig {
                compression_gzip_level: 1,
                ..gzip.clone()
            },
            segment_config: LogConfig {
                compression_gzip_level: 9,
                ..gzip
            },
            reused: false,
        },
        Case {
            name: "compression.type changed between the gates",
            wire: v2,
            topic_compression: Some(CompressionType::Gzip),
            message_config: forcing(Some(CompressionType::Gzip)),
            segment_config: zstd,
            reused: false,
        },
    ];
    for case in cases {
        let name = case.name;
        let mut prepared = prepared(case.wire, case.topic_compression);
        let decoded = owned(&prepared).clone();
        let message_codec = case
            .topic_compression
            .unwrap_or_else(|| decoded.attributes.compression());
        let segment_codec = case
            .segment_config
            .compression_type
            .unwrap_or_else(|| decoded.attributes.compression());
        let segment_level = case.segment_config.compression_level(segment_codec);

        let measured = prepared.stored_len(case.topic_compression, Some(&case.message_config));
        let first = prepared
            .encoded
            .as_ref()
            .expect("the gate kept its encode")
            .bytes()
            .clone();
        check!(
            measured
                == Some(
                    fresh(
                        &decoded,
                        message_codec,
                        case.message_config.compression_level(message_codec)
                    )
                    .len()
                ),
            "{name}"
        );

        let expected = fresh(&decoded, segment_codec, segment_level);
        check!(
            prepared.appended_len(&case.segment_config) == expected.len(),
            "{name}"
        );
        let second = prepared
            .encoded
            .as_ref()
            .expect("the gate kept its encode")
            .bytes()
            .clone();
        check!(second == expected, "{name}");
        check!((second.as_ptr() == first.as_ptr()) == case.reused, "{name}");
        // The codec is the target's only for the encode: the writer applies
        // whatever `compression.type` is in force when it appends.
        check!(*owned(&prepared) == decoded, "{name}");

        let data = build_produce_data(prepared, 4).resolve_for_append(&case.segment_config);
        let ProduceData::Verbatim(verbatim) = data else {
            panic!("{name}: the writer encoded again: {data:?}");
        };
        check!(verbatim.bytes == expected, "{name}");
        check!(
            verbatim.leader_epoch == krabka_log::LeaderEpoch(4),
            "{name}"
        );
        check!(
            verbatim.last_offset_delta == decoded.last_offset_delta,
            "{name}"
        );
        check!(verbatim.max_timestamp == decoded.max_timestamp, "{name}");
    }
}

/// A verbatim batch keeps nothing: its bytes are the producer's, and neither
/// gate encodes them.
#[test]
fn a_verbatim_batch_keeps_no_encode() {
    let mut prepared = prepare_batch(
        PartitionPayload::Slice(encode_batch(&producer_batch())),
        None,
        TimestampPolicy::default(),
        false,
        DecodeEnv {
            topic_name: &Arc::from("t"),
            metrics: &BrokerMetrics::new(),
            policy: RecordDecompressionPolicy::default(),
        },
        13,
    )
    .expect("a valid produce payload");
    assert!(matches!(prepared.source, PreparedSource::Verbatim(_)));

    check!(prepared.stored_len(None, Some(&LogConfig::default())) == None);
    check!(prepared.appended_len(&LogConfig::default()) == encode_batch(&producer_batch()).len());
    check!(prepared.encoded.is_none());
    check!(matches!(
        build_produce_data(prepared, 4),
        ProduceData::Verbatim(_)
    ));
}
