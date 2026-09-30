//! Unit tests for Kafka trunk's `max.decompressed.message.bytes` on the
//! cleaner's two passes: the offset-map build and the rewrite.

use std::collections::HashMap;

use bytes::Bytes;
use krabka_compression::CompressionType;
use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_units::prelude::{ByteSize, ByteSizeExt as _, bytes};

use super::{
    CleanedTransactionMetadata, CleaningRound, ProducerLastRecord, RewriteRetention,
    rewrite_segments,
};
use crate::{
    compact::{
        build_offset_map,
        test_support::{RETENTION, control_batch, write_sealed_batches},
    },
    error::LogError,
    txn_index::AbortedTxn,
};

const LIMIT: ByteSize = bytes(100);

/// A batch of one keyed record with a `value_len`-byte value, compressed with
/// `codec`, that a transaction of `producer_id` wrote when it is not `-1`.
fn keyed_batch(
    base_offset: i64,
    value_len: usize,
    codec: CompressionType,
    producer_id: i64,
) -> RecordBatch {
    RecordBatch {
        base_offset,
        last_offset_delta: 0,
        producer_id,
        attributes: Attributes::default()
            .with_compression(codec)
            .with_transactional(producer_id >= 0),
        records: vec![Record {
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from(vec![7_u8; value_len])),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

/// Whether the pass refused a record above `LIMIT`; any other failure fails the
/// test.
fn refused<T>(result: Result<T, LogError>) -> bool {
    match result {
        Ok(_) => false,
        Err(LogError::RecordTooLarge { limit, .. }) => {
            assert2::check!(limit == LIMIT.bytes_usize());
            true
        }
        Err(other) => panic!("unexpected error: {other}"),
    }
}

/// Both passes read every record of a compressed batch that the log keeps, and
/// only of one: a batch the producer did not compress is never held to the
/// limit.
#[test]
fn the_passes_refuse_a_compressed_record_above_the_limit() {
    for (name, codec, limit, refuses) in [
        (
            "compressed, oversized",
            CompressionType::Gzip,
            Some(LIMIT),
            true,
        ),
        ("compressed, no limit", CompressionType::Gzip, None, false),
        (
            "uncompressed, oversized",
            CompressionType::None,
            Some(LIMIT),
            false,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let seg = write_sealed_batches(dir.path(), &[keyed_batch(0, 1_000, codec, -1)]);
        let segments = [&seg];

        let map = build_offset_map(&segments, vec![], limit);
        assert2::check!(refused(map) == refuses, "{name}: offset map");

        let unchecked = build_offset_map(&segments, vec![], None).unwrap();
        let mut txn = CleanedTransactionMetadata::default();
        let active = HashMap::new();
        let rewritten = rewrite_segments(
            &crate::io::FileIo,
            dir.path(),
            &segments,
            &unchecked,
            &mut txn,
            RewriteRetention {
                now_ms: 0,
                delete_retention: RETENTION,
            },
            CleaningRound {
                active_producers: &active,
                upper_bound: Offset(1),
                max_decompressed_record: limit,
            },
        );
        assert2::check!(refused(rewritten) == refuses, "{name}: rewrite");
    }
}

/// `MemoryRecords.filterTo` skips a batch the pass deletes outright, which is
/// an aborted batch that neither keeps an active producer's state nor ends the
/// round, so such a batch is not read; one the pass keeps empty is read.
#[test]
fn an_aborted_batch_is_read_only_when_the_pass_keeps_it_empty() {
    let producer = ProducerId(2000);
    let last_data_offset = |offset| ProducerLastRecord {
        last_data_offset: Some(Offset(offset)),
        producer_epoch: 0,
    };
    for (name, upper_bound, active_offset, refuses) in [
        ("deleted outright", 10, None, false),
        ("the last batch of the round", 1, None, true),
        ("an active producer's last data batch", 10, Some(0), true),
        ("another batch of an active producer", 10, Some(5), false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        // An oversized compressed batch of an aborted transaction, then the
        // abort marker in the next segment (past the pass), which the
        // aborted-transaction list names all the same.
        let seg = write_sealed_batches(
            dir.path(),
            &[keyed_batch(0, 1_000, CompressionType::Gzip, producer.get())],
        );
        let segments = [&seg];
        let aborted = AbortedTxn {
            start_offset: Offset(0),
            last_offset: Offset(1),
            producer_id: producer,
            last_stable_offset: Offset(2),
        };
        // The offset-map build skips the aborted batch whatever it holds.
        let map = build_offset_map(&segments, vec![aborted], Some(LIMIT)).unwrap();

        let mut txn = CleanedTransactionMetadata::default();
        txn.add_aborted_transactions([aborted]);
        let active: HashMap<_, _> = active_offset
            .map(|offset| (producer, last_data_offset(offset)))
            .into_iter()
            .collect();
        let rewritten = rewrite_segments(
            &crate::io::FileIo,
            dir.path(),
            &segments,
            &map,
            &mut txn,
            RewriteRetention {
                now_ms: 0,
                delete_retention: RETENTION,
            },
            CleaningRound {
                active_producers: &active,
                upper_bound: Offset(upper_bound),
                max_decompressed_record: Some(LIMIT),
            },
        );
        assert2::check!(refused(rewritten) == refuses, "{name}");
    }
}

/// A control batch is never compressed, so the limit has nothing to say about
/// it.
#[test]
fn a_control_batch_is_never_held_to_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let seg = write_sealed_batches(dir.path(), &[control_batch(0, 1000, 1)]);
    assert2::check!(!refused(build_offset_map(&[&seg], vec![], Some(bytes(1)))));
}
