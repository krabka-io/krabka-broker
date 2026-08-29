//! Unit tests for the zero-copy passthrough append: byte-exactness
//! against the owned path, the offset each variant assigns, and the
//! transaction state it tracks.

use assert2::check;
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::{kibibytes, mebibytes};
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    leader_epoch_checkpoint::EpochEntry,
    log::test_support::{sample_batch, test_batch_at, test_log, verbatim_from},
    stamp_index::{StampEntry, StampIndex},
};

#[test]
fn append_verbatim_assigns_offsets_and_is_byte_exact() {
    let (dir, mut log) = test_log();

    // Append three single-record batches verbatim. Each producer batch
    // carries a bogus base_offset (999) that the log must overwrite.
    let mut expected_wire = bytes::BytesMut::new();
    for _ in 0..3 {
        let mut producer = test_batch_at(0);
        producer.base_offset = 999;
        producer.partition_leader_epoch = -1;
        let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));
        log.append_verbatim(&vb).unwrap();
        // Re-encode the expectation with the assigned offset + epoch.
        let mut stamped = producer.clone();
        stamped.base_offset = (log.log_end_offset() - 1).0;
        stamped.partition_leader_epoch = 4;
        stamped.encode(&mut expected_wire).unwrap();
    }
    assert2::assert!(log.log_end_offset() == 3);

    let log_end = log.log_end_offset();
    let r = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
    assert2::assert!(&r.bytes[..] == &expected_wire[..]);

    // Decodes cleanly (CRC valid) with the assigned offsets.
    let mut cur: &[u8] = &r.bytes;
    let mut bases = Vec::new();
    while !cur.is_empty() {
        bases.push(Offset(RecordBatch::decode(&mut cur).unwrap().base_offset));
    }
    assert2::assert!(bases == vec![Offset(0), Offset(1), Offset(2)]);
    drop(dir);
}

#[test]
fn append_verbatim_at_stamps_base_byte_exact() {
    let (dir, mut log) = test_log();

    let mut prefix = test_batch_at(0);
    prefix.partition_leader_epoch = 2;
    log.append(&mut prefix).unwrap();

    let mut producer = test_batch_at(0);
    producer.base_offset = 999;
    producer.partition_leader_epoch = -1;
    let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));

    let appended = log.append_verbatim_at(&vb, Offset(1)).unwrap();

    assert_eq!(appended, Offset(1));
    assert_eq!(log.log_end_offset(), Offset(2));
    assert_eq!(
        log.epoch_checkpoint().entries(),
        &[
            EpochEntry {
                epoch: LeaderEpoch(2),
                start_offset: Offset(0),
            },
            EpochEntry {
                epoch: LeaderEpoch(4),
                start_offset: Offset(1),
            },
        ]
    );

    let mut expected_wire = bytes::BytesMut::new();
    prefix.encode(&mut expected_wire).unwrap();
    let mut stamped = producer.clone();
    stamped.base_offset = 1;
    stamped.partition_leader_epoch = 4;
    stamped.encode(&mut expected_wire).unwrap();

    let r = log
        .read_raw(Offset(0), log.log_end_offset(), mebibytes(10))
        .unwrap();
    assert_eq!(
        r.bytes[..],
        expected_wire[..],
        "verbatim append_at must be byte-exact after supplied base+epoch stamping"
    );
    drop(dir);
}

#[test]
fn append_verbatim_at_rejects_non_leo_base() {
    let (dir, mut log) = test_log();

    let producer = test_batch_at(0);
    let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));

    let err = log.append_verbatim_at(&vb, Offset(1)).unwrap_err();

    assert!(
        matches!(
            err,
            LogError::OffsetMismatch {
                expected: Offset(0),
                actual: Offset(1)
            }
        ),
        "non-LEO append_verbatim_at must report OffsetMismatch"
    );
    assert_eq!(log.log_end_offset(), Offset(0));
    assert!(
        log.read_raw(Offset(0), Offset(0), kibibytes(1))
            .unwrap()
            .bytes
            .is_empty()
    );
    drop(dir);
}

#[test]
fn append_verbatim_at_uses_reconciled_frontier_floor() {
    let (dir, mut log) = test_log();

    log.reconcile_next_offset(Offset(5));
    let producer = test_batch_at(0);
    let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));

    let appended = log.append_verbatim_at(&vb, Offset(5)).unwrap();

    assert_eq!(appended, Offset(5));
    assert_eq!(log.log_end_offset(), Offset(6));
    drop(dir);
}

#[test]
fn append_verbatim_matches_owned_append_bytes() {
    // The verbatim path and the owned path must write byte-identical
    // .log bytes for the same logical batch — proving passthrough does
    // not perturb the stored representation.
    let dir_owned = tempdir().unwrap();
    let mut log_owned = Log::open(dir_owned.path(), LogConfig::default()).unwrap();
    let dir_verb = tempdir().unwrap();
    let mut log_verb = Log::open(dir_verb.path(), LogConfig::default()).unwrap();

    let mut producer = test_batch_at(0);
    producer.base_offset = 12345; // overwritten by both paths
    producer.partition_leader_epoch = -1;

    // Owned path: stamp epoch like the produce handler does, then append.
    let mut owned = producer.clone();
    owned.partition_leader_epoch = 9;
    log_owned.append(&mut owned).unwrap();

    // Verbatim path: same epoch via the meta.
    let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(9));
    log_verb.append_verbatim(&vb).unwrap();

    let end_owned = log_owned.log_end_offset();
    let end_verb = log_verb.log_end_offset();
    assert2::assert!(end_owned == end_verb);
    let r_owned = log_owned
        .read_raw(Offset(0), end_owned, mebibytes(10))
        .unwrap();
    let r_verb = log_verb
        .read_raw(Offset(0), end_verb, mebibytes(10))
        .unwrap();
    assert2::assert!(&r_owned.bytes[..] == &r_verb.bytes[..]);
    drop(dir_owned);
    drop(dir_verb);
}

#[test]
fn append_verbatim_transactional_holds_lso() {
    let (dir, mut log) = test_log();
    // A transactional batch must hold the LSO at the batch's base offset
    // (it isn't stable until a commit/abort marker arrives).
    let mut producer = test_batch_at(0);
    producer.last_offset_delta = 1; // spans offsets 0..=1
    producer.producer_id = 77;
    producer.producer_epoch = 0;
    producer.attributes = producer.attributes.with_transactional(true);
    let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(0));
    log.append_verbatim(&vb).unwrap();
    // LSO stays at 0 (the open txn's first offset), not log_end (2).
    assert2::assert!(log.log_end_offset() == Offset(2));
    assert2::assert!(log.lso() == Offset(0));
    drop(dir);
}

/// The verbatim replication append path stamps the *full* offset span of
/// the batch. A multi-record batch appended through `append_verbatim`
/// records `last_offset == base_offset + last_offset_delta`. Interior
/// offsets and the inclusive end offset therefore resolve, and one offset
/// past the end does not. This test guards the `base + delta` arithmetic
/// on that path, which the owned-append tests above do not exercise.
#[test]
fn append_verbatim_stamps_full_offset_range() {
    let (dir, mut log) = test_log();
    log.set_stamp_source(std::sync::Arc::new(
        crate::stamp_source::MonotonicStampSource::new(500, 1),
    ))
    .unwrap();

    // A four-record producer batch, appended verbatim, spans offsets 0..=3.
    let mut producer = sample_batch(4);
    producer.base_offset = 999; // bogus; the log overwrites it with 0
    producer.partition_leader_epoch = -1;
    let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));
    log.append_verbatim(&vb).unwrap();

    // last_offset is base(0) + delta(3) = 3.
    check!(log.stamp_for_offset(Offset(0)) == Some(500));
    check!(log.stamp_for_offset(Offset(3)) == Some(500));
    check!(log.stamp_for_offset(Offset(4)) == None);

    let idx = StampIndex::open(dir.path().join("00000000000000000000.stampindex")).unwrap();
    assert2::assert!(
        idx.entries()
            == [StampEntry {
                base_offset: Offset(0),
                last_offset: Offset(3),
                stamp: 500,
            }]
    );
}

// Verbatim counterpart of `non_txn_batch_with_valid_pid_advances_lso`,
// pinning the `&&` in the verbatim LSO-tracking branch. A non-transactional
// verbatim batch with a valid producer_id must advance LSO; `&&`→`||`
// would hold it at the batch base (0).
#[test]
fn non_txn_verbatim_batch_with_valid_pid_advances_lso() {
    let (dir, mut log) = test_log();
    let mut producer = test_batch_at(0);
    producer.last_offset_delta = 1; // spans offsets 0..=1
    producer.producer_id = 55; // valid pid, but NOT transactional
    producer.producer_epoch = 0;
    assert2::assert!(!producer.attributes.is_transactional());
    let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(0));
    log.append_verbatim(&vb).unwrap();
    assert2::assert!(log.log_end_offset() == Offset(2));
    assert2::assert!(log.lso() == Offset(2));
    drop(dir);
}
