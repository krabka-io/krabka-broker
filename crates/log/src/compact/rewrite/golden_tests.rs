//! Golden-bytes characterization of the rewrite's on-disk output. One fixed
//! log holds every case the rewrite treats differently: a superseded record,
//! a `LogAppendTime` batch, a tombstone that gets a delete horizon, a
//! tombstone whose horizon has passed, an aborted transaction, a committed
//! one, and an active producer's emptied batch that `RETAIN_EMPTY` keeps. The
//! test compares the exact bytes of every `.cleaned` file the rewrite writes
//! against bytes an earlier build wrote.

use {
    super::{CleanedTransactionMetadata, ProducerLastRecord, RewriteRetention},
    crate::{
        compact::{
            build_offset_map,
            test_support::{
                RETENTION, control_batch, inputs, make_record, rewrite_with_map, round_over,
                transactional_record, write_sealed_batches,
            },
        },
        test_support::KeyValue,
        txn_index::AbortedTxn,
    },
    krabka_ids::{Offset, ProducerId},
    krabka_protocol::records::{Attributes, Record, RecordBatch, TimestampType},
    std::{collections::HashMap, fs, path::Path},
};

/// The wall clock of the pass: past the stamped horizon of the old tombstone
/// at offset 5, and before the one the pass stamps on the tombstone at 4.
const NOW_MS: i64 = 10_000;

/// The idempotent producer whose only batch the pass empties.
const ACTIVE_PRODUCER: i64 = 3000;

/// A non-transactional record `timestamp_delta` after its batch's base.
fn timed_record(offset_delta: i32, key: &[u8], value: Option<&[u8]>, delta: i64) -> Record {
    Record {
        timestamp_delta: delta,
        ..make_record(offset_delta, Some(key), value)
    }
}

/// The aborted transaction of producer 2000: data at 6, abort marker at 7.
fn aborted_txn() -> AbortedTxn {
    AbortedTxn {
        start_offset: Offset(6),
        last_offset: Offset(7),
        producer_id: ProducerId(2000),
        last_stable_offset: Offset(8),
    }
}

fn fixture_batches() -> Vec<RecordBatch> {
    // 0..=2, CreateTime: loses k1 (superseded at 3), so its max timestamp
    // falls from 1_030 to the retained records' 1_020.
    let create_time = RecordBatch {
        base_offset: 0,
        last_offset_delta: 2,
        base_timestamp: 1_000,
        max_timestamp: 1_030,
        records: vec![
            timed_record(0, b"k1", Some(b"v1"), 30),
            timed_record(1, b"k2", Some(b"v2"), 20),
            timed_record(2, b"k3", Some(b"v3"), 10),
        ],
        ..RecordBatch::default()
    };
    // 3..=4, LogAppendTime: keeps its own max timestamp, and its tombstone
    // for k4 gets the delete horizon NOW_MS + RETENTION.
    let log_append_time = RecordBatch {
        base_offset: 3,
        last_offset_delta: 1,
        base_timestamp: 4_000,
        max_timestamp: 5_000,
        attributes: Attributes::default().with_timestamp_type(TimestampType::LogAppendTime),
        records: vec![
            timed_record(0, b"k1", Some(b"v4"), 0),
            timed_record(1, b"k4", None, 5),
        ],
        ..RecordBatch::default()
    };
    // 5: a tombstone stamped with horizon 100 by an earlier pass. NOW_MS is
    // past it, so the record goes and the emptied batch with it.
    let old_tombstone = RecordBatch {
        base_offset: 5,
        last_offset_delta: 0,
        base_timestamp: 100,
        attributes: Attributes::default().with_delete_horizon(true),
        records: vec![timed_record(0, b"k5", None, 0)],
        ..RecordBatch::default()
    };
    // 6..=7: producer 2000's aborted transaction.
    let aborted = transactional_record()
        .offset(6)
        .producer(2000)
        .payload(KeyValue {
            key: b"k6",
            value: b"aborted",
        })
        .call();
    let abort_marker = control_batch(7, 2000, 0 /* ABORT */);
    // 8: the active producer's last data batch. k7 at 9 supersedes it, and
    // RETAIN_EMPTY writes it again as a bare header.
    let active_producer = RecordBatch {
        base_offset: 8,
        last_offset_delta: 0,
        producer_id: ACTIVE_PRODUCER,
        producer_epoch: 4,
        base_sequence: 11,
        records: vec![timed_record(0, b"k7", Some(b"old"), 0)],
        ..RecordBatch::default()
    };
    let supersedes_k7 = RecordBatch {
        base_offset: 9,
        last_offset_delta: 0,
        records: vec![timed_record(0, b"k7", Some(b"new"), 0)],
        ..RecordBatch::default()
    };
    // 10..=11: producer 1000's committed transaction.
    let committed = transactional_record()
        .offset(10)
        .payload(KeyValue {
            key: b"k8",
            value: b"committed",
        })
        .call();
    let commit_marker = control_batch(11, 1000, 1 /* COMMIT */);
    vec![
        create_time,
        log_append_time,
        old_tombstone,
        aborted,
        abort_marker,
        active_producer,
        supersedes_k7,
        committed,
        commit_marker,
    ]
}

/// The bytes of every `.cleaned` file one rewrite of the fixture writes.
#[derive(Debug, PartialEq, Eq)]
struct CleanedFiles {
    log: Vec<u8>,
    index: Vec<u8>,
    timeindex: Vec<u8>,
    txnindex: Option<Vec<u8>>,
}

fn rewrite_fixture(dir: &Path) -> CleanedFiles {
    let seg = write_sealed_batches(dir, &fixture_batches());
    let segment_refs = vec![&seg];
    let map = build_offset_map(&inputs(&segment_refs), vec![aborted_txn()], None).unwrap();
    let mut txn = CleanedTransactionMetadata::default();
    txn.add_aborted_transactions([aborted_txn()]);
    let active = HashMap::from([(ProducerId(ACTIVE_PRODUCER), ProducerLastRecord {
        last_data_offset: Some(Offset(8)),
        producer_epoch: 4,
    })]);
    let out = rewrite_with_map(
        dir,
        &segment_refs,
        &map,
        &mut txn,
        RewriteRetention {
            now_ms: NOW_MS,
            delete_retention: RETENTION,
        },
        round_over(&segment_refs, &active),
    );
    CleanedFiles {
        log: fs::read(&out.log_swap).unwrap(),
        index: fs::read(&out.index_swap).unwrap(),
        timeindex: fs::read(&out.timeindex_swap).unwrap(),
        txnindex: out.txnindex_swap.map(|path| fs::read(path).unwrap()),
    }
}

/// The `.log.cleaned` bytes the rewrite of the fixture wrote before its phases
/// were extracted.
const GOLDEN_LOG: &[u8] = &[
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 71, 0, 0, 0, 0, 2, 77, 156, 222, 177, 0, 0, 0, 0, 0, 2, 0, 0,
    0, 0, 0, 0, 3, 232, 0, 0, 0, 0, 0, 0, 3, 252, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 0, 0, 0, 2, 20, 0, 40, 2, 4, 107, 50, 4, 118, 50, 0, 20, 0, 20, 4, 4, 107,
    51, 4, 118, 51, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 71, 0, 0, 0, 0, 2, 79, 88, 63, 69, 0, 72,
    0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 42, 248, 0, 0, 0, 0, 0, 0, 19, 136, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 2, 22, 0, 175, 109, 0, 4, 107, 49, 4, 118, 52,
    0, 18, 0, 165, 109, 2, 4, 107, 52, 1, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 60, 0, 0, 0, 0, 2,
    191, 194, 226, 81, 0, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 7, 208, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 20, 0, 0, 0, 8, 0, 0, 0, 0, 1, 0, 0,
    0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 49, 0, 0, 0, 0, 2, 75, 208, 196, 61, 0, 0, 0, 0, 0, 0, 255, 255,
    255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 184, 0, 4, 0, 0, 0,
    11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 61, 0, 0, 0, 0, 2, 237, 211, 246, 117, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 22, 0, 0, 0, 4, 107, 55, 6, 110, 101, 119, 0, 0,
    0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 67, 0, 0, 0, 0, 2, 108, 236, 183, 28, 0, 16, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 232, 255, 255, 255, 255, 255,
    255, 0, 0, 0, 1, 34, 0, 0, 0, 4, 107, 56, 18, 99, 111, 109, 109, 105, 116, 116, 101, 100, 0, 0,
    0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 60, 0, 0, 0, 0, 2, 83, 190, 226, 18, 0, 48, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 232, 255, 255, 255, 255, 255, 255,
    0, 0, 0, 1, 20, 0, 0, 0, 8, 0, 0, 0, 1, 1, 0,
];

/// The `.timeindex.cleaned` bytes: one entry, timestamp 5_000 at offset 4.
/// The `.index.cleaned` stays empty, because the output is shorter than one
/// `index.interval.bytes`.
const GOLDEN_TIMEINDEX: &[u8] = &[0, 0, 0, 0, 0, 0, 19, 136, 0, 0, 0, 4];

/// The `.txnindex.cleaned` bytes: the aborted transaction whose marker stays.
const GOLDEN_TXNINDEX: &[u8] = &[
    0, 0, 0, 0, 0, 0, 0, 0, 7, 208, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0,
    0, 0, 8,
];

/// The rewrite's output files are a persisted format: the bytes stay the same
/// across a refactor of the rewrite.
#[test]
fn the_rewrite_of_the_fixture_writes_the_golden_bytes() {
    let dir = tempfile::tempdir().unwrap();
    assert2::assert!(
        rewrite_fixture(dir.path())
            == CleanedFiles {
                log: GOLDEN_LOG.to_vec(),
                index: vec![],
                timeindex: GOLDEN_TIMEINDEX.to_vec(),
                txnindex: Some(GOLDEN_TXNINDEX.to_vec()),
            }
    );
}
