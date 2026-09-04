//! Unit tests for the compaction rewrite pass: superseded-record removal,
//! tombstone and marker delete-horizon stamping, and `RETAIN_EMPTY`.

use std::fs;

use krabka_ids::Offset;
use krabka_protocol::records::{Attributes, Record};
use krabka_units::prelude::millis;

use super::*;
use crate::compact::{
    build_offset_map,
    test_support::{
        RETENTION, control_batch, make_record, write_sealed_batches, write_sealed_segment,
    },
};

/// A far-future `now` so nothing in the simple tests ages out, plus an
/// empty active-producer set and no surviving transactions.
const NEVER_AGE_NOW_MS: i64 = 0;

fn rewrite_simple(dir: &Path, segment_refs: &[&Segment]) -> RewriteOutput {
    let map = build_offset_map(segment_refs).unwrap();
    let txn = CleanedTransactionMetadata::build(segment_refs, &map).unwrap();
    let active: HashMap<ProducerId, Offset> = HashMap::new();
    rewrite_segments(
        &crate::io::FileIo,
        dir,
        segment_refs,
        &map,
        &txn,
        RewriteRetention {
            now_ms: NEVER_AGE_NOW_MS,
            delete_retention: RETENTION,
        },
        &active,
    )
    .unwrap()
}

fn decode_all(bytes: &[u8]) -> Vec<RecordBatch> {
    let mut cursor = bytes;
    let mut out = Vec::new();
    while !cursor.is_empty() {
        let Ok(b) = RecordBatch::decode(&mut cursor) else {
            break;
        };
        out.push(b);
    }
    out
}

#[test]
fn rewrite_drops_superseded_records() {
    let dir = tempfile::tempdir().unwrap();
    let first_segment = write_sealed_segment(
        dir.path(),
        0,
        vec![
            make_record(0, Some(b"k1"), Some(b"v1")),
            make_record(1, Some(b"k2"), Some(b"v2")),
            make_record(2, Some(b"k1"), Some(b"v3")),
        ],
    );
    let segment_refs = vec![&first_segment];
    let out = rewrite_simple(dir.path(), &segment_refs);
    let bytes = fs::read(&out.log_swap).unwrap();
    let batches = decode_all(&bytes);
    assert2::assert!(out.new_base_offset == Offset(0));
    assert2::assert!(out.new_last_offset == Offset(2));
    assert2::assert!(
        batches
            == vec![RecordBatch {
                base_offset: 0,
                last_offset_delta: 2,
                records: vec![
                    make_record(1, Some(b"k2"), Some(b"v2")),
                    make_record(2, Some(b"k1"), Some(b"v3")),
                ],
                ..RecordBatch::default()
            }]
    );
}

#[test]
fn rewrite_keeps_tombstone_as_latest() {
    let dir = tempfile::tempdir().unwrap();
    let first_segment = write_sealed_segment(
        dir.path(),
        0,
        vec![
            make_record(0, Some(b"k1"), Some(b"v1")),
            make_record(1, Some(b"k1"), None), // tombstone
        ],
    );
    let segment_refs = vec![&first_segment];
    let out = rewrite_simple(dir.path(), &segment_refs);
    let bytes = fs::read(&out.log_swap).unwrap();
    let mut cursor = &bytes[..];
    let batch = RecordBatch::decode(&mut cursor).unwrap();
    let mut record = make_record(1, Some(b"k1"), None);
    record.timestamp_delta = -1_000;
    assert2::assert!(out.new_base_offset == Offset(0));
    assert2::assert!(out.new_last_offset == Offset(1));
    assert2::assert!(
        batch
            == RecordBatch {
                base_offset: 0,
                last_offset_delta: 1,
                base_timestamp: RETENTION.millis_i64(),
                attributes: Attributes::default().with_delete_horizon(true),
                records: vec![record],
                ..RecordBatch::default()
            }
    );
}

#[test]
fn rewrite_preserves_absolute_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let first_segment = write_sealed_segment(
        dir.path(),
        100,
        vec![
            make_record(0, Some(b"k1"), Some(b"v1")), // abs 100
            make_record(1, Some(b"k2"), Some(b"v2")), // abs 101
            make_record(2, Some(b"k1"), Some(b"v3")), // abs 102 — kept
            make_record(3, None, Some(b"unkeyed")),   // abs 103 — dropped
        ],
    );
    let segment_refs = vec![&first_segment];
    let out = rewrite_simple(dir.path(), &segment_refs);
    let bytes = std::fs::read(&out.log_swap).unwrap();
    let batches = decode_all(&bytes);
    assert2::assert!(out.new_base_offset == Offset(100));
    assert2::assert!(out.new_last_offset == Offset(102));
    assert2::assert!(
        batches
            == vec![RecordBatch {
                base_offset: 100,
                last_offset_delta: 2,
                records: vec![
                    make_record(1, Some(b"k2"), Some(b"v2")),
                    make_record(2, Some(b"k1"), Some(b"v3")),
                ],
                ..RecordBatch::default()
            }]
    );
}

/// (a) End-to-end control-batch bug fix. Two commit markers at different
/// offsets BOTH survive when the data of their transactions survives.
#[test]
fn rewrite_both_commit_markers_survive_when_data_survives() {
    let dir = tempfile::tempdir().unwrap();
    // pid 1000: data batch at offset 0 (key k1), commit marker at offset 1.
    // pid 2000: data batch at offset 2 (key k2), commit marker at offset 3.
    let data1 = RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        producer_id: 1000,
        attributes: krabka_protocol::records::Attributes::default().with_transactional(true),
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::copy_from_slice(b"k1")),
            value: Some(Bytes::copy_from_slice(b"v1")),
            ..Default::default()
        }],
        ..RecordBatch::default()
    };
    let marker1 = control_batch(1, 1000, 1 /* COMMIT */);
    let data2 = RecordBatch {
        base_offset: 2,
        last_offset_delta: 0,
        producer_id: 2000,
        attributes: krabka_protocol::records::Attributes::default().with_transactional(true),
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::copy_from_slice(b"k2")),
            value: Some(Bytes::copy_from_slice(b"v2")),
            ..Default::default()
        }],
        ..RecordBatch::default()
    };
    let marker2 = control_batch(3, 2000, 1 /* COMMIT */);
    let expected = vec![
        data1.clone(),
        marker1.clone(),
        data2.clone(),
        marker2.clone(),
    ];
    let seg = write_sealed_batches(dir.path(), &[data1, marker1, data2, marker2]);
    let segment_refs = vec![&seg];
    let out = rewrite_simple(dir.path(), &segment_refs);

    let bytes = fs::read(&out.log_swap).unwrap();
    let batches = decode_all(&bytes);
    assert2::assert!(out.new_base_offset == Offset(0));
    assert2::assert!(out.new_last_offset == Offset(3));
    assert2::assert!(batches == expected);
}

/// (b) A newest-for-key tombstone with no existing horizon gets bit 6 set
/// and `base_timestamp == now + delete_retention_ms`.
#[test]
fn rewrite_tombstone_gets_horizon_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let first_segment = write_sealed_segment(
        dir.path(),
        0,
        vec![make_record(0, Some(b"k1"), None)], // tombstone, newest for k1
    );
    let segment_refs = vec![&first_segment];
    let map = build_offset_map(&segment_refs).unwrap();
    let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
    let now = 5_000i64;
    let ret = 50i64;
    let retention = Time::from_millis(ret);
    let out = rewrite_segments(
        &crate::io::FileIo,
        dir.path(),
        &segment_refs,
        &map,
        &txn,
        RewriteRetention {
            now_ms: now,
            delete_retention: retention,
        },
        &HashMap::new(),
    )
    .unwrap();
    let bytes = fs::read(&out.log_swap).unwrap();
    let batches = decode_all(&bytes);
    let mut record = make_record(0, Some(b"k1"), None);
    record.timestamp_delta = -(now + ret);
    assert2::assert!(out.new_base_offset == Offset(0));
    assert2::assert!(out.new_last_offset == Offset(0));
    assert2::assert!(
        batches
            == vec![RecordBatch {
                base_offset: 0,
                last_offset_delta: 0,
                base_timestamp: now + ret,
                attributes: Attributes::default().with_delete_horizon(true),
                records: vec![record],
                ..RecordBatch::default()
            }]
    );
}

/// (c) The rewrite drops a commit marker when the data of its transaction
/// is fully gone and its existing horizon has elapsed.
#[test]
fn rewrite_marker_dropped_when_data_gone_and_horizon_elapsed() {
    let dir = tempfile::tempdir().unwrap();
    // A standalone commit marker for pid 1000 with NO surviving data, and
    // an already-stamped delete horizon at base_timestamp = 100.
    let mut marker = control_batch(0, 1000, 1 /* COMMIT */);
    marker.base_timestamp = 100;
    marker.attributes = marker.attributes.with_delete_horizon(true);
    // A second data batch (pid -1) so the marker is not the last batch
    // (otherwise RETAIN_EMPTY would keep a bare header).
    let data = RecordBatch {
        base_offset: 1,
        last_offset_delta: 0,
        records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
        ..RecordBatch::default()
    };
    let seg = write_sealed_batches(dir.path(), &[marker, data]);
    let segment_refs = vec![&seg];
    let map = build_offset_map(&segment_refs).unwrap();
    let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
    // now=200 >= horizon 100 → marker deleted.
    let out = rewrite_segments(
        &crate::io::FileIo,
        dir.path(),
        &segment_refs,
        &map,
        &txn,
        RewriteRetention {
            now_ms: 200,
            delete_retention: millis(50),
        },
        &HashMap::new(),
    )
    .unwrap();
    let bytes = fs::read(&out.log_swap).unwrap();
    let batches = decode_all(&bytes);
    assert2::assert!(out.new_base_offset == Offset(0));
    assert2::assert!(out.new_last_offset == Offset(1));
    assert2::assert!(
        batches
            == vec![RecordBatch {
                base_offset: 1,
                last_offset_delta: 0,
                records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
                ..RecordBatch::default()
            }]
    );
}

/// (d) `RETAIN_EMPTY`: the rewrite writes the fully-emptied batch of an
/// active producer again as a bare header with no records. `producer_id`,
/// `epoch`, and `sequence` survive.
#[test]
fn rewrite_retain_empty_for_active_producer() {
    let dir = tempfile::tempdir().unwrap();
    // pid 1000 data batch under k1 at offset 0, then a NEWER data batch
    // (pid -1) under k1 at offset 1 that supersedes it — so pid 1000's
    // only record is dropped, emptying its batch. pid 1000 is active.
    let data1 = RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        producer_id: 1000,
        producer_epoch: 7,
        base_sequence: 3,
        records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
        ..RecordBatch::default()
    };
    let data2 = RecordBatch {
        base_offset: 1,
        last_offset_delta: 0,
        producer_id: -1,
        records: vec![make_record(0, Some(b"k1"), Some(b"v2"))], // newest for k1
        ..RecordBatch::default()
    };
    let seg = write_sealed_batches(dir.path(), &[data1, data2]);
    let segment_refs = vec![&seg];
    let map = build_offset_map(&segment_refs).unwrap();
    let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
    let mut active = HashMap::new();
    active.insert(ProducerId(1000), Offset(0)); // pid 1000 active, last batch base 0
    let out = rewrite_segments(
        &crate::io::FileIo,
        dir.path(),
        &segment_refs,
        &map,
        &txn,
        RewriteRetention {
            now_ms: 0,
            delete_retention: RETENTION,
        },
        &active,
    )
    .unwrap();
    let bytes = fs::read(&out.log_swap).unwrap();
    let batches = decode_all(&bytes);
    assert2::assert!(out.new_base_offset == Offset(0));
    assert2::assert!(out.new_last_offset == Offset(1));
    assert2::assert!(
        batches
            == vec![
                RecordBatch {
                    base_offset: 0,
                    last_offset_delta: 0,
                    producer_id: 1000,
                    producer_epoch: 7,
                    base_sequence: 3,
                    ..RecordBatch::default()
                },
                RecordBatch {
                    base_offset: 1,
                    last_offset_delta: 0,
                    producer_id: -1,
                    records: vec![make_record(0, Some(b"k1"), Some(b"v2"))],
                    ..RecordBatch::default()
                },
            ]
    );
}

// `RETAIN_EMPTY` last-offset arithmetic: an emptied output-last batch is
// re-emitted as a bare header, and its `base_offset + last_offset_delta`
// must extend `new_last_offset`. The emptied batch sits at base_offset 100
// with `last_offset_delta` 5, so its last absolute offset is `100 + 5 =
// 105`. This pins the `+` in `Offset(base_offset + last_offset_delta)`:
// mutating it to `-` would report `new_last_offset == 95`.
#[test]
fn rewrite_retain_empty_extends_last_offset() {
    let dir = tempfile::tempdir().unwrap();
    // Batch 0 (base 0): one surviving keyed record (abs offset 0).
    let data0 = RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        producer_id: -1,
        records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
        ..RecordBatch::default()
    };
    // Batch 1 (base 100, last_offset_delta 5): only NULL-key records, all
    // dropped, so the batch is emptied. As the output-last batch it is
    // re-emitted as a bare header spanning abs offsets 100..=105.
    let data1 = RecordBatch {
        base_offset: 100,
        last_offset_delta: 5,
        producer_id: -1,
        records: vec![
            make_record(0, None, Some(b"n1")),
            make_record(5, None, Some(b"n2")),
        ],
        ..RecordBatch::default()
    };
    let seg = write_sealed_batches(dir.path(), &[data0, data1]);
    let segment_refs = vec![&seg];
    let out = rewrite_simple(dir.path(), &segment_refs);

    // The emptied batch is re-emitted as a bare header at base_offset 100.
    let bytes = fs::read(&out.log_swap).unwrap();
    let batches = decode_all(&bytes);
    assert2::assert!(out.new_base_offset == Offset(0));
    assert2::assert!(out.new_last_offset == Offset(105));
    assert2::assert!(
        batches
            == vec![
                RecordBatch {
                    base_offset: 0,
                    last_offset_delta: 0,
                    producer_id: -1,
                    records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
                    ..RecordBatch::default()
                },
                RecordBatch {
                    base_offset: 100,
                    last_offset_delta: 5,
                    producer_id: -1,
                    ..RecordBatch::default()
                },
            ]
    );
}
