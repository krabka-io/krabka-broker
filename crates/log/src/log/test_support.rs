//! Fixtures shared by the unit tests of the `log` submodules: batch
//! builders, marker builders, and the small log harnesses they open.
//!
//! The builders live here rather than beside one test module because the
//! same transactional batch and the same control marker are what several
//! submodules assert on.

use std::{collections::HashMap, time::SystemTime};

use bytes::Bytes;
use krabka_ids::{LeaderEpoch, Offset, ProducerId};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_units::prelude::{ByteSize, bytes, gibibytes};
use tempfile::tempdir;

use super::{BARRIER_CONTROL_TYPE, CompactionContext, Log, VerbatimBatch};
use crate::{config::LogConfig, producer_snapshot::ProducerSnapshotEntry, txn_index::AbortedTxn};

/// A read budget larger than anything these tests write, so the byte
/// budget never clips the result.
pub const NO_LIMIT: ByteSize = gibibytes(4);

pub fn sample_batch(n: i32) -> RecordBatch {
    let mut b = RecordBatch {
        base_offset: 0, // overwritten by Log::append
        max_timestamp: 0,
        last_offset_delta: n - 1,
        ..RecordBatch::default()
    };
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(format!("v{i}"))),
            ..Default::default()
        });
    }
    b
}

pub fn test_log() -> (tempfile::TempDir, Log) {
    let dir = tempdir().unwrap();
    let log = Log::open(dir.path(), LogConfig::default()).unwrap();
    (dir, log)
}

pub fn test_batch_at(_off: i64) -> RecordBatch {
    // `Log::append` overwrites `base_offset`; one record per batch.
    let mut b = RecordBatch {
        base_offset: 0,
        base_timestamp: 1_000,
        max_timestamp: 1_000,
        last_offset_delta: 0,
        ..RecordBatch::default()
    };
    b.records.push(Record {
        offset_delta: 0,
        value: Some(Bytes::from("v")),
        ..Default::default()
    });
    b
}

/// Encode a "producer" batch with a producer-chosen `base_offset` and
/// leader epoch. Return both the wire bytes and a `VerbatimBatch`.
pub fn verbatim_from(producer: &RecordBatch, leader_epoch: LeaderEpoch) -> (Bytes, VerbatimBatch) {
    let mut wire = bytes::BytesMut::new();
    producer.encode(&mut wire).unwrap();
    let wire = wire.freeze();
    let vb = VerbatimBatch {
        bytes: wire.clone(),
        last_offset_delta: producer.last_offset_delta,
        max_timestamp: producer.max_timestamp,
        leader_epoch,
        producer_id: ProducerId(producer.producer_id),
        producer_epoch: producer.producer_epoch,
        base_sequence: producer.base_sequence,
        is_transactional: producer.attributes.is_transactional(),
    };
    (wire, vb)
}

// ---- helpers for transactional tests ----

/// A transactional (non-control) batch for the given pid/epoch containing `values`.
pub fn transactional_batch(pid: i64, epoch: i16, values: &[&str]) -> RecordBatch {
    let last_offset_delta = i32::try_from(values.len()).unwrap() - 1;
    let mut records = Vec::new();
    for (i, v) in values.iter().enumerate() {
        records.push(Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    RecordBatch {
        base_offset: 0, // overwritten by Log::append
        last_offset_delta,
        producer_id: pid,
        producer_epoch: epoch,
        attributes: Attributes::default().with_transactional(true),
        records,
        ..RecordBatch::default()
    }
}

/// Build a 4-byte control-marker key: (version=0: i16, `marker_type`: i16) BE.
pub fn control_key(marker_type: i16) -> Bytes {
    let mut buf = [0u8; 4];
    buf[0..2].copy_from_slice(&0i16.to_be_bytes()); // version = 0
    buf[2..4].copy_from_slice(&marker_type.to_be_bytes());
    Bytes::from(buf.to_vec())
}

/// Build an end-marker value: (version=0: i16, coordinator epoch: i32) BE.
pub fn control_value(coordinator_epoch: i32) -> Bytes {
    let mut buf = [0u8; 6];
    buf[0..2].copy_from_slice(&0i16.to_be_bytes());
    buf[2..6].copy_from_slice(&coordinator_epoch.to_be_bytes());
    Bytes::from(buf.to_vec())
}

/// A commit control batch (`marker_type=1`) for the given pid and epoch.
/// `Log::append` rewrites the offsets.
pub fn commit_marker(pid: i64, epoch: i16) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        producer_id: pid,
        producer_epoch: epoch,
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        records: vec![Record {
            offset_delta: 0,
            key: Some(control_key(1 /* COMMIT */)),
            value: Some(control_value(17)),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

/// An abort control batch (`marker_type=0`) for the given pid and epoch.
/// `Log::append` rewrites the offsets.
pub fn abort_marker(pid: i64, epoch: i16) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        producer_id: pid,
        producer_epoch: epoch,
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        records: vec![Record {
            offset_delta: 0,
            key: Some(control_key(0 /* ABORT */)),
            value: Some(control_value(17)),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

/// Build a barrier-marker value: `(version=0: i16, group: string,
/// epoch: i64, triggered_at: i64)` big-endian, where a string is an `i16`
/// byte length and then UTF-8 bytes.
pub fn barrier_value(group: &str, epoch: i64) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(&0i16.to_be_bytes());
    buf.extend_from_slice(&i16::try_from(group.len()).unwrap().to_be_bytes());
    buf.extend_from_slice(group.as_bytes());
    buf.extend_from_slice(&epoch.to_be_bytes());
    buf.extend_from_slice(&1_700_000_000_000i64.to_be_bytes());
    Bytes::from(buf)
}

/// A barrier control batch, control type [`BARRIER_CONTROL_TYPE`].
///
/// `RecordBatch::default` already carries the marker's identity: a
/// `producer_id` of -1, a `producer_epoch` of -1, and a `base_sequence`
/// of -1. The attributes set the control bit and leave the transactional
/// bit clear. `Log::append` rewrites the offsets.
pub fn barrier_marker(group: &str, epoch: i64) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        attributes: Attributes::default().with_control(true),
        records: vec![Record {
            offset_delta: 0,
            key: Some(control_key(BARRIER_CONTROL_TYPE)),
            value: Some(barrier_value(group, epoch)),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

/// A barrier control batch that carries `producer_id` and
/// `producer_epoch`. The wire format sets both to -1, so this shape is
/// hostile input. It exists to state that the log decides by
/// control-record type alone.
pub fn barrier_marker_from_producer(
    group: &str,
    epoch: i64,
    producer_id: i64,
    producer_epoch: i16,
) -> RecordBatch {
    RecordBatch {
        producer_id,
        producer_epoch,
        ..barrier_marker(group, epoch)
    }
}

/// Every piece of per-partition state that a barrier marker leaves alone,
/// in one comparable value.
#[derive(Debug, PartialEq, Eq)]
pub struct PartitionState {
    pub lso: Offset,
    pub transactions: Vec<(i64, (i32, Option<Offset>))>,
    pub aborted: Vec<AbortedTxn>,
    pub producers: Vec<ProducerSnapshotEntry>,
}

/// Collect the [`PartitionState`] of `log`, with one transaction entry per
/// id in `producer_ids` and the producer entries in id order.
pub fn partition_state(log: &Log, producer_ids: &[i64]) -> PartitionState {
    let mut producers = log.producer_state_snapshot();
    producers.sort_by_key(|entry| entry.producer_id.get());
    PartitionState {
        lso: log.lso(),
        transactions: producer_ids
            .iter()
            .map(|pid| (*pid, log.producer_transaction_state(ProducerId(*pid))))
            .collect(),
        aborted: log.aborted_in_range(Offset(0), Offset(i64::MAX)),
        producers,
    }
}

pub fn sample_batch_with_epoch(n: i32, epoch: i32) -> RecordBatch {
    let mut b = sample_batch(n);
    b.partition_leader_epoch = epoch;
    b
}

pub fn keyed_batch(base: i64, items: &[(i32, &[u8], &[u8])]) -> RecordBatch {
    let records: Vec<Record> = items
        .iter()
        .map(|(d, k, v)| Record {
            offset_delta: *d,
            key: Some(Bytes::copy_from_slice(k)),
            value: Some(Bytes::copy_from_slice(v)),
            ..Default::default()
        })
        .collect();
    let last_delta = items.iter().map(|(d, _, _)| *d).max().unwrap_or(0);
    RecordBatch {
        base_offset: base,
        last_offset_delta: last_delta,
        max_timestamp: 0,
        records,
        ..RecordBatch::default()
    }
}

/// A `CompactionContext` with a fixed, deterministic epoch and no active
/// producers. The in-crate compaction tests use it where tombstone and
/// marker age are not under test.
pub fn compaction_ctx() -> CompactionContext {
    CompactionContext {
        now: SystemTime::UNIX_EPOCH,
        active_producers: HashMap::new(),
    }
}

/// Build a log rolled into several sealed segments under `dir`. This
/// mirrors the `remote_log_manager` test helper and stays local to this
/// module.
pub fn rolled_log(dir: &std::path::Path, extra: &LogConfig) -> Log {
    let mut log = Log::open(
        dir,
        LogConfig {
            segment_size: bytes(200),
            ..extra.clone()
        },
    )
    .unwrap();
    for _ in 0..16 {
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
    }
    log
}

pub fn ts_batch(ts: i64) -> RecordBatch {
    let mut b = RecordBatch {
        base_offset: 0, // overwritten by Log::append
        base_timestamp: ts,
        max_timestamp: ts,
        last_offset_delta: 0,
        ..RecordBatch::default()
    };
    b.records.push(Record {
        offset_delta: 0,
        timestamp_delta: 0,
        value: Some(Bytes::from("v")),
        ..Default::default()
    });
    b
}
