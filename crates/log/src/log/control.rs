//! Classification of Kafka control batches and of the krabka barrier
//! marker, which the append path and both recovery walks share.
//!
//! The control-record type lives in the key of a control batch's first
//! record, so every caller that must tell a transaction end marker from a
//! barrier marker reads it through this module.

use krabka_protocol::records::RecordBatch;

/// Control-record type of a krabka barrier marker.
///
/// Kafka assigns the control-record types 0 to 6: `ABORT`, `COMMIT`,
/// `LEADER_CHANGE`, `SNAPSHOT_HEADER`, `SNAPSHOT_FOOTER`, `KRAFT_VERSION`, and
/// `VOTERS`. The value 1000 starts a krabka-private range that Kafka cannot
/// reach by normal growth. `krabka-raft` uses the same convention for its
/// private api keys at 1003 and 1004.
///
/// A barrier marker is a control batch with one record. The record key holds
/// this type. The batch sets `producer_id` to -1, it sets `producer_epoch` to
/// -1, and it clears the transactional attribute bit. The log keeps no
/// transaction state and no producer state for such a batch. Kafka's
/// `ControlRecordType.parse` reports an unknown type for the value 1000 and
/// skips the batch, so a JVM consumer never sees the record.
///
/// The broker builds the marker batch and the log classifies it. Both read
/// this one constant.
pub const BARRIER_CONTROL_TYPE: i16 = 1000;

/// Control-record type of Kafka's ABORT end marker.
pub const ABORT_CONTROL_TYPE: i16 = 0;

/// Control-record type of Kafka's COMMIT end marker.
pub const COMMIT_CONTROL_TYPE: i16 = 1;

/// What the log does with one control batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlBatchKind {
    /// A transaction end marker. The log closes the producer's open
    /// transaction, it stamps the committed data, and it records an aborted
    /// range. Every control type outside the krabka-private range lands here,
    /// because Kafka writes no other control type into a data partition.
    Transaction,
    /// A krabka barrier marker, control type [`BARRIER_CONTROL_TYPE`]. The log
    /// leaves `pending`, `pending_stamp_ranges`, `coordinator_epochs`, the
    /// active `.txnindex`, and the producer state unchanged for it.
    Barrier,
}

/// Classify one batch for the append path and for the two recovery paths.
///
/// Returns `None` for a data batch. The control-record type comes from the key
/// of the batch's first record, in the layout that
/// [`parse_control_marker_type`] reads. A control batch with no records is a
/// [`ControlBatchKind::Transaction`], which is the classification such a batch
/// had before the barrier type existed. Compaction emits one for the
/// `RETAIN_EMPTY` rule.
pub fn control_batch_kind(batch: &RecordBatch) -> Option<ControlBatchKind> {
    if !batch.attributes.is_control_batch() {
        return None;
    }
    let marker_type = batch
        .records
        .first()
        .and_then(|record| record.key.as_deref())
        .and_then(parse_control_marker_type);
    if marker_type == Some(BARRIER_CONTROL_TYPE) {
        Some(ControlBatchKind::Barrier)
    } else {
        Some(ControlBatchKind::Transaction)
    }
}

/// Parse the control-marker type from the key of the first record in a
/// control batch. The key encodes `(version: i16, type: i16)` in
/// big-endian. Returns [`ABORT_CONTROL_TYPE`] for ABORT,
/// [`COMMIT_CONTROL_TYPE`] for COMMIT, and [`BARRIER_CONTROL_TYPE`] for a
/// krabka barrier marker. Returns `None` if the key is shorter than 4 bytes.
pub fn parse_control_marker_type(key: &[u8]) -> Option<i16> {
    if key.len() < 4 {
        return None;
    }
    let _version = i16::from_be_bytes([key[0], key[1]]);
    Some(i16::from_be_bytes([key[2], key[3]]))
}

/// Parse the transaction coordinator epoch from an end-marker value.
/// Kafka encodes `(version: i16, coordinator_epoch: i32)` in big-endian.
/// Older krabka markers had no value, so malformed or absent values are
/// ignored for backward compatibility.
pub fn parse_control_marker_coordinator_epoch(value: &[u8]) -> Option<i32> {
    if value.len() < 6 {
        return None;
    }
    let _version = i16::from_be_bytes([value[0], value[1]]);
    Some(i32::from_be_bytes([value[2], value[3], value[4], value[5]]))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;
    use krabka_ids::{Offset, ProducerId};
    use krabka_protocol::records::Record;
    use krabka_units::prelude::{bytes, mebibytes};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::LogConfig,
        log::{
            Log,
            test_support::{
                PartitionState, abort_marker, barrier_marker, barrier_marker_from_producer,
                commit_marker, compaction_ctx, keyed_batch, partition_state, sample_batch,
                transactional_batch,
            },
        },
        producer_snapshot::ProducerSnapshotEntry,
        segment::Segment,
        txn_index::AbortedTxn,
    };

    /// An end marker's coordinator epoch is bytes 2..6 of its value, and a
    /// value too short to hold both fields is ignored rather than read past.
    ///
    /// Older krabka markers carried no value at all, so a short one has to be
    /// absent-not-malformed -- the transaction still resolves, it just carries
    /// no coordinator epoch.
    #[test]
    fn a_coordinator_epoch_needs_a_six_byte_value() {
        check!(
            parse_control_marker_coordinator_epoch(&[0, 0, 0, 0, 0, 7]) == Some(7),
            "version then epoch"
        );
        check!(
            parse_control_marker_coordinator_epoch(&[0, 1, 0, 0, 0, 7, 9]) == Some(7),
            "a longer value reads the same"
        );
        for short in 0..6usize {
            let value = vec![0u8; short];
            check!(
                parse_control_marker_coordinator_epoch(&value).is_none(),
                "a {short}-byte value is too short"
            );
        }
    }

    /// A control marker's type comes from bytes 2..4 of its key, and a key too
    /// short to hold both fields yields nothing rather than reading past it.
    #[test]
    fn a_control_marker_type_needs_a_four_byte_key() {
        // (version, type) big-endian; the type is what comes back.
        check!(parse_control_marker_type(&[0, 0, 0, 0]) == Some(0), "ABORT");
        check!(
            parse_control_marker_type(&[0, 0, 0, 1]) == Some(1),
            "COMMIT"
        );
        // Exactly four bytes is enough; anything longer is read the same way.
        check!(
            parse_control_marker_type(&[0, 1, 0, 1, 9, 9]) == Some(1),
            "a longer key"
        );
        for short in [&[][..], &[0][..], &[0, 0][..], &[0, 0, 0][..]] {
            check!(
                parse_control_marker_type(short).is_none(),
                "a {}-byte key is too short",
                short.len()
            );
        }
    }

    // ---- barrier-marker tests ----

    /// A barrier marker changes no transaction state and no producer state
    /// while a transaction is open.
    ///
    /// The log routes a control batch by its control-record type, not by its
    /// producer id, so the second case carries the open transaction's
    /// producer id and still closes nothing.
    #[test]
    fn a_barrier_marker_leaves_an_open_transaction_untouched() {
        for (name, mut barrier) in [
            ("no producer id", barrier_marker("nightly", 7)),
            (
                "with a producer id",
                barrier_marker_from_producer("nightly", 7, 1000, 2),
            ),
        ] {
            let dir = tempdir().unwrap();
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.set_stamp_source(std::sync::Arc::new(
                crate::stamp_source::MonotonicStampSource::new(40, 1),
            ))
            .unwrap();

            let mut data = transactional_batch(1000, 2, &["a", "b"]);
            data.base_sequence = 0;
            log.append(&mut data).unwrap(); // offsets 0 and 1
            let before = partition_state(&log, &[1000, -1]);

            log.append(&mut barrier).unwrap(); // offset 2

            check!(partition_state(&log, &[1000, -1]) == before, "case {name}");
            // The marker still takes an offset of its own, and it carries no
            // stamp, because a control batch is never stamped.
            check!(log.log_end_offset() == Offset(3), "case {name}");
            check!(log.stamp_for_offset(Offset(2)) == None, "case {name}");
        }
        // The marker's identity fields give no producer tail, so the append
        // writes no producer entry for it.
        check!(Log::data_producer_tail(ProducerId(-1), -1, 0, Offset(2)).unwrap() == None);
        check!(Log::data_producer_tail(ProducerId(1000), -1, 0, Offset(2)).unwrap() == None);
    }

    /// A barrier marker moves the last-stable-offset exactly as an ordinary
    /// non-transactional data batch moves it.
    #[test]
    fn a_barrier_marker_moves_the_lso_like_a_data_batch() {
        for (name, open_transaction, want_lso, want_log_end) in [
            ("no open transaction", false, Offset(1), Offset(1)),
            ("open transaction", true, Offset(0), Offset(2)),
        ] {
            for (kind, is_barrier) in [("data batch", false), ("barrier marker", true)] {
                let dir = tempdir().unwrap();
                let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
                if open_transaction {
                    let mut data = transactional_batch(1000, 0, &["a"]);
                    data.base_sequence = 0;
                    log.append(&mut data).unwrap();
                }
                let mut batch = if is_barrier {
                    barrier_marker("nightly", 1)
                } else {
                    sample_batch(1)
                };
                log.append(&mut batch).unwrap();
                check!(log.lso() == want_lso, "case {name} / {kind}");
                check!(log.log_end_offset() == want_log_end, "case {name} / {kind}");
            }
        }
    }

    /// A barrier marker between a transaction's data and its end marker does
    /// not disturb what the end marker does.
    #[test]
    fn a_barrier_marker_does_not_disturb_a_following_end_marker() {
        let committed = ProducerSnapshotEntry {
            producer_id: ProducerId(1000),
            producer_epoch: 2,
            last_sequence: 1,
            last_offset: Offset(1),
            offset_delta: 1,
            timestamp: 0,
            coordinator_epoch: 17,
            current_txn_first_offset: None,
        };
        for (name, mut marker, want_aborted, want_stamps) in [
            (
                "commit",
                commit_marker(1000, 2),
                Vec::new(),
                vec![Some(40), Some(40), None, None],
            ),
            (
                "abort",
                abort_marker(1000, 2),
                vec![AbortedTxn {
                    start_offset: Offset(0),
                    last_offset: Offset(3),
                    producer_id: ProducerId(1000),
                }],
                vec![None, None, None, None],
            ),
        ] {
            let dir = tempdir().unwrap();
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.set_stamp_source(std::sync::Arc::new(
                crate::stamp_source::MonotonicStampSource::new(40, 1),
            ))
            .unwrap();

            let mut data = transactional_batch(1000, 2, &["a", "b"]);
            data.base_sequence = 0;
            log.append(&mut data).unwrap(); // offsets 0 and 1
            log.append(&mut barrier_marker("nightly", 9)).unwrap(); // offset 2
            check!(
                log.lso() == Offset(0),
                "case {name}: the barrier holds the LSO"
            );
            log.append(&mut marker).unwrap(); // offset 3

            check!(
                partition_state(&log, &[1000])
                    == PartitionState {
                        lso: Offset(4),
                        transactions: vec![(1000, (17, None))],
                        aborted: want_aborted,
                        producers: vec![committed],
                    },
                "case {name}"
            );
            let stamps: Vec<Option<u64>> =
                (0..4).map(|o| log.stamp_for_offset(Offset(o))).collect();
            check!(stamps == want_stamps, "case {name}");
        }
    }

    /// Recovery mirrors the append path. A reopened log rebuilds the same
    /// producer state and the same transaction state across barrier markers.
    #[test]
    fn barrier_markers_rebuild_identical_state_after_reopen() {
        let dir = tempdir().unwrap();
        let ids = [1000, 2000, 3000, -1];
        let before = {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&mut barrier_marker("nightly", 1)).unwrap(); // 0

            let mut committed = transactional_batch(1000, 2, &["a"]);
            committed.base_sequence = 0;
            log.append(&mut committed).unwrap(); // 1
            log.append(&mut barrier_marker("nightly", 2)).unwrap(); // 2
            log.append(&mut commit_marker(1000, 2)).unwrap(); // 3

            let mut rolled_back = transactional_batch(2000, 5, &["b", "c"]);
            rolled_back.base_sequence = 0;
            log.append(&mut rolled_back).unwrap(); // 4 and 5
            log.append(&mut barrier_marker("nightly", 3)).unwrap(); // 6
            log.append(&mut abort_marker(2000, 5)).unwrap(); // 7

            let mut still_open = transactional_batch(3000, 1, &["d"]);
            still_open.base_sequence = 0;
            log.append(&mut still_open).unwrap(); // 8
            // A marker that carries the open transaction's producer id closes
            // nothing on the append path, and recovery reaches the same
            // result.
            log.append(&mut barrier_marker_from_producer("nightly", 4, 3000, 1))
                .unwrap(); // 9

            partition_state(&log, &ids)
        };
        // The open transaction of producer 3000 holds the LSO at its first
        // offset, and the barrier that follows does not release it.
        check!(before.lso == Offset(8));

        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        check!(partition_state(&reopened, &ids) == before);
        check!(reopened.log_end_offset() == Offset(10));
    }

    /// Recovery rebuilds a transaction's stamp ranges across a barrier
    /// marker, so a commit that lands after a restart still stamps the
    /// transaction's data.
    #[test]
    fn a_barrier_marker_keeps_stamp_ranges_across_a_restart() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            let mut data = transactional_batch(1000, 2, &["a", "b"]);
            data.base_sequence = 0;
            log.append(&mut data).unwrap(); // offsets 0 and 1
            // A marker that carries the open transaction's producer id clears
            // no stamp range, on the append path or on the recovery path.
            log.append(&mut barrier_marker_from_producer("nightly", 5, 1000, 2))
                .unwrap(); // offset 2
        }

        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        reopened
            .set_stamp_source(std::sync::Arc::new(
                crate::stamp_source::MonotonicStampSource::new(40, 1),
            ))
            .unwrap();
        check!(reopened.lso() == Offset(0));

        reopened.append(&mut commit_marker(1000, 2)).unwrap(); // offset 3

        check!(reopened.lso() == Offset(4));
        let stamps: Vec<Option<u64>> = (0..4)
            .map(|offset| reopened.stamp_for_offset(Offset(offset)))
            .collect();
        check!(stamps == vec![Some(40), Some(40), None, None]);
    }

    /// Compaction keeps every barrier marker, and the marker key never enters
    /// the dedup map.
    #[test]
    fn compaction_keeps_barrier_markers_and_never_indexes_their_key() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(1), // one batch per segment: every append rolls
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        log.append(&mut keyed_batch(0, &[(0, b"k1", b"v0")]))
            .unwrap(); // 0
        log.append(&mut barrier_marker("nightly", 1)).unwrap(); // 1
        log.append(&mut keyed_batch(0, &[(0, b"k1", b"v1")]))
            .unwrap(); // 2
        log.append(&mut barrier_marker("nightly", 2)).unwrap(); // 3
        log.append(&mut keyed_batch(0, &[(0, b"tail", b"t")]))
            .unwrap(); // 4, active

        // The dedup map holds the data key only. `should_index_key` keeps the
        // marker key out of it, so no barrier can shadow another.
        let sealed: Vec<&Segment> = log.segments.iter().collect();
        let mut indexed: Vec<Bytes> = crate::compact::build_offset_map(&sealed)
            .unwrap()
            .into_keys()
            .collect();
        indexed.sort();
        check!(indexed == vec![Bytes::from_static(b"k1")]);

        log.compact(&compaction_ctx()).unwrap();

        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        // Both markers survive the pass, unchanged.
        let kept_markers: Vec<Record> = out
            .batches
            .iter()
            .filter(|batch| batch.attributes.is_control_batch())
            .flat_map(|batch| batch.records.iter().cloned())
            .collect();
        let want_markers: Vec<Record> = [1, 2]
            .into_iter()
            .flat_map(|epoch| barrier_marker("nightly", epoch).records)
            .collect();
        check!(kept_markers == want_markers);
        // The data still dedups newest-wins around them.
        let kept_data: Vec<(Bytes, Bytes)> = out
            .batches
            .iter()
            .filter(|batch| !batch.attributes.is_control_batch())
            .flat_map(|batch| batch.records.iter())
            .map(|record| (record.key.clone().unwrap(), record.value.clone().unwrap()))
            .collect();
        check!(
            kept_data
                == vec![
                    (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"tail"), Bytes::from_static(b"t")),
                ]
        );
    }
}
