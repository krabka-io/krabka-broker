//! Unit tests for the offline verifier, driven end to end through
//! [`verify_partition_dir`] over partitions built on disk.

use std::sync::Arc;

use assert2::check;
use bytes::Bytes;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch, RecordHeader};

use super::*;
use crate::{
    chain::{ChainState, GENESIS_HEAD},
    checkpoint::Checkpoint,
    ids::EpochMs,
    signing::FileEd25519Signer,
    sink::AuditRecord,
};

fn signer() -> (Arc<FileEd25519Signer>, Vec<u8>) {
    use ring::signature::{Ed25519KeyPair, KeyPair};
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
    let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
    let pubkey = kp.public_key().as_ref().to_vec();
    (
        Arc::new(FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "k1".into()).unwrap()),
        pubkey,
    )
}

fn audit_record_to_batch(rec: &AuditRecord, base_offset: i64) -> RecordBatch {
    let headers = rec
        .headers
        .iter()
        .map(|(k, v)| RecordHeader {
            key: k.clone(),
            value: Some(Bytes::from(v.clone())),
        })
        .collect();
    let mut batch = RecordBatch {
        base_offset,
        last_offset_delta: 0,
        ..RecordBatch::default()
    };
    batch.records.push(Record {
        offset_delta: 0,
        value: Some(Bytes::from(rec.value.clone())),
        headers,
        ..Default::default()
    });
    batch
}

/// Several records inside one batch, each at its own `offset_delta`.
///
/// [`audit_record_to_batch`] puts one record per batch at delta 0, where a
/// record's offset is its batch's base offset. This is the other shape: the
/// offset is the base plus the delta, and the two differ.
fn audit_records_to_batch(recs: &[AuditRecord], base_offset: i64) -> RecordBatch {
    let last = i32::try_from(recs.len()).expect("record count fits i32") - 1;
    let mut batch = RecordBatch {
        base_offset,
        last_offset_delta: last,
        ..RecordBatch::default()
    };
    for (i, rec) in recs.iter().enumerate() {
        let headers = rec
            .headers
            .iter()
            .map(|(k, v)| RecordHeader {
                key: k.clone(),
                value: Some(Bytes::from(v.clone())),
            })
            .collect();
        batch.records.push(Record {
            offset_delta: i32::try_from(i).expect("delta fits i32"),
            value: Some(Bytes::from(rec.value.clone())),
            headers,
            ..Default::default()
        });
    }
    batch
}

/// Build a valid chained and checkpointed partition on disk, and return the
/// public key.
fn build_partition(tmp: &std::path::Path) -> Vec<u8> {
    let (s, pubkey) = signer();
    let mut log = Log::open(tmp, LogConfig::default()).unwrap();
    let mut chain = ChainState::new();
    let mut offset = 0i64;
    for i in 0..3u8 {
        let mut rec = AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: format!("{{\"i\":{i}}}").into_bytes(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        };
        let (seq, prev) = chain.extend(&rec.value);
        rec.push_chain_headers(seq, &prev);
        let mut b = audit_record_to_batch(&rec, offset);
        log.append(&mut b).unwrap();
        offset += 1;
    }
    // checkpoint over the chain head
    let cp = Checkpoint::signed(
        s.as_ref(),
        Seq(chain.next_seq() - 1),
        &chain.head(),
        EpochMs(123),
    );
    let mut b = audit_record_to_batch(&cp.to_record(), offset);
    log.append(&mut b).unwrap();
    pubkey
}

#[test]
fn valid_partition_verifies_ok() {
    let tmp = tempfile::tempdir().unwrap();
    let pubkey = build_partition(tmp.path());
    let trusted = TrustedKeys::single("k1".into(), pubkey);
    let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
    // build_partition writes 3 records (seq 0..2) + 1 checkpoint (seq_high=2),
    // so all records are covered.
    check!(
        (
            report.ok,
            report.records.0,
            report.checkpoints.0,
            report.first_break.is_none(),
            report.unanchored_records.0,
        ) == (true, 3, 1, true, 0)
    );
}

#[test]
fn wrong_trusted_key_fails_at_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let _pubkey = build_partition(tmp.path());
    let (_other, other_pub) = signer();
    let trusted = TrustedKeys::single("k1".into(), other_pub);
    let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
    check!(!report.ok);
    let b = report.first_break.unwrap();
    check!(b.reason.contains("signature"));
}

// ── Fix 1 tests: unanchored_records field ─────────────────────────────────

/// Partition with a trailing tail of 2 records beyond the last checkpoint.
#[test]
fn unanchored_tail_records_are_counted() {
    let tmp = tempfile::tempdir().unwrap();
    let (s, pubkey) = signer();
    let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
    let mut chain = ChainState::new();
    let mut offset = 0i64;

    // 3 records + checkpoint (seq_high=2)
    for i in 0..3u8 {
        let mut rec = crate::sink::AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: format!("{{\"i\":{i}}}").into_bytes(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        };
        let (seq, prev) = chain.extend(&rec.value);
        rec.push_chain_headers(seq, &prev);
        let mut b = audit_record_to_batch(&rec, offset);
        log.append(&mut b).unwrap();
        offset += 1;
    }
    let cp = Checkpoint::signed(
        s.as_ref(),
        Seq(chain.next_seq() - 1),
        &chain.head(),
        EpochMs(100),
    );
    let mut b = audit_record_to_batch(&cp.to_record(), offset);
    log.append(&mut b).unwrap();
    offset += 1;

    // 2 more records WITHOUT a trailing checkpoint
    for i in 3..5u8 {
        let mut rec = crate::sink::AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: format!("{{\"i\":{i}}}").into_bytes(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        };
        let (seq, prev) = chain.extend(&rec.value);
        rec.push_chain_headers(seq, &prev);
        let mut b = audit_record_to_batch(&rec, offset);
        log.append(&mut b).unwrap();
        offset += 1;
    }

    let trusted = TrustedKeys::single("k1".into(), pubkey);
    let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
    check!(
        (
            report.ok,
            report.checkpoints.0,
            report.records.0,
            report.unanchored_records.0,
        ) == (true, 1, 5, 2)
    );
}

/// Chain-only partition with no signing key and no checkpoints. All records
/// are unanchored.
#[test]
fn chain_only_partition_all_records_unanchored() {
    let tmp = tempfile::tempdir().unwrap();
    let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
    let mut chain = ChainState::new();

    for (offset, i) in (0..3u8).enumerate() {
        let mut rec = crate::sink::AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: format!("{{\"i\":{i}}}").into_bytes(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        };
        let (seq, prev) = chain.extend(&rec.value);
        rec.push_chain_headers(seq, &prev);
        let mut b = audit_record_to_batch(&rec, i64::try_from(offset).unwrap());
        log.append(&mut b).unwrap();
    }

    // No trusted key needed — no checkpoints present
    let trusted = TrustedKeys::default();
    let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
    check!(
        (
            report.ok,
            report.checkpoints.0,
            report.records.0,
            report.unanchored_records.0,
        ) == (true, 0, 3, 3)
    );
}

// ── Fix 2 tests: direct tamper-detection (chain-inconsistent fixtures) ────

/// Dropped record creates a seq gap that the verifier detects as a break.
#[test]
fn dropped_record_detected_as_seq_gap() {
    let tmp = tempfile::tempdir().unwrap();
    let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
    let mut chain = ChainState::new();

    // Build 3 records into memory first, then write only [0] and [2] (skip [1]).
    let mut records: Vec<crate::sink::AuditRecord> = (0..3u8)
        .map(|i| crate::sink::AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: format!("{{\"i\":{i}}}").into_bytes(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        })
        .collect();

    for rec in &mut records {
        let (seq, prev) = chain.extend(&rec.value);
        rec.push_chain_headers(seq, &prev);
    }

    // Write records[0] (seq=0) then records[2] (seq=2) — skip records[1]
    let mut b = audit_record_to_batch(&records[0], 0);
    log.append(&mut b).unwrap();
    let mut b = audit_record_to_batch(&records[2], 1);
    log.append(&mut b).unwrap();

    let trusted = TrustedKeys::default();
    let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
    check!(!report.ok, "dropped record must be detected as tamper");
    let reason = &report.first_break.unwrap().reason;
    check!(
        reason.contains("seq"),
        "reason should mention seq gap, got: {reason}"
    );
}

/// The verifier detects a record with the wrong `prev_hash` as a chain break.
#[test]
fn wrong_prev_hash_detected() {
    let tmp = tempfile::tempdir().unwrap();
    let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
    let mut chain = ChainState::new();

    // Record 0: correct chain
    let mut rec0 = crate::sink::AuditRecord {
        class: crate::event::AuditEventClass::ApplicationLifecycle,
        value: b"{\"i\":0}".to_vec(),
        headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
    };
    let (seq0, prev0) = chain.extend(&rec0.value);
    rec0.push_chain_headers(seq0, &prev0);
    let mut b = audit_record_to_batch(&rec0, 0);
    log.append(&mut b).unwrap();

    // Record 1: stamped with GENESIS_HEAD as prev (wrong — should be head after rec0)
    let mut rec1 = crate::sink::AuditRecord {
        class: crate::event::AuditEventClass::ApplicationLifecycle,
        value: b"{\"i\":1}".to_vec(),
        headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
    };
    // Advance chain to get the correct seq, but use GENESIS_HEAD as wrong prev
    let (seq1, _correct_prev) = chain.extend(&rec1.value);
    rec1.push_chain_headers(seq1, &GENESIS_HEAD); // wrong prev
    let mut b = audit_record_to_batch(&rec1, 1);
    log.append(&mut b).unwrap();

    let trusted = TrustedKeys::default();
    let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
    check!(!report.ok, "wrong prev_hash must be detected as tamper");
    let reason = &report.first_break.unwrap().reason;
    check!(
        reason.contains("prev_hash"),
        "reason should mention prev_hash, got: {reason}"
    );
}

/// A break in the second record of a batch is reported at
/// `base_offset + offset_delta`.
///
/// Every other test writes one record per batch at delta 0, where the base
/// alone is the offset and so a sum is indistinguishable from a difference
/// or a product. Here the record sits at base 1, delta 1: the offset is 2,
/// and 1 and 0 are what the other arithmetic would give.
#[test]
fn break_offset_within_a_batch_is_base_plus_delta() {
    let tmp = tempfile::tempdir().unwrap();
    let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
    let mut chain = ChainState::new();

    let record = |value: &str, chain: &mut ChainState, tamper: bool| {
        let mut rec = AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: value.as_bytes().to_vec(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        };
        let (seq, prev) = chain.extend(&rec.value);
        rec.push_chain_headers(seq, if tamper { &GENESIS_HEAD } else { &prev });
        rec
    };

    // Offset 0, its own batch, chain intact.
    let rec0 = record("{\"i\":0}", &mut chain, false);
    log.append(&mut audit_record_to_batch(&rec0, 0)).unwrap();

    // Offsets 1 and 2 in one batch. The second carries a wrong prev hash,
    // so the walk breaks on it rather than on the batch's first record.
    let rec1 = record("{\"i\":1}", &mut chain, false);
    let rec2 = record("{\"i\":2}", &mut chain, true);
    log.append(&mut audit_records_to_batch(&[rec1, rec2], 1))
        .unwrap();

    let report = verify_partition_dir(tmp.path(), &TrustedKeys::default()).unwrap();
    check!(!report.ok, "a wrong prev_hash must break the walk");
    let brk = report.first_break.expect("a break was reported");
    check!(brk.offset == 2, "break offset, got {}", brk.offset);
    check!(
        brk.reason.contains("prev_hash"),
        "reason should name prev_hash, got: {}",
        brk.reason
    );
}

/// A checkpoint signed over the original chain head does not match after
/// records are replaced with different values, because the re-stamped chain
/// head differs.
#[test]
fn stale_checkpoint_chain_head_mismatch_detected() {
    let tmp_orig = tempfile::tempdir().unwrap();
    let tmp_tampered = tempfile::tempdir().unwrap();

    let (s, pubkey) = signer();

    // Build original partition: 2 records + checkpoint
    let mut orig_chain = ChainState::new();
    let mut orig_records: Vec<crate::sink::AuditRecord> = (0..2u8)
        .map(|i| crate::sink::AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: format!("{{\"i\":{i}}}").into_bytes(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        })
        .collect();
    for rec in &mut orig_records {
        let (seq, prev) = orig_chain.extend(&rec.value);
        rec.push_chain_headers(seq, &prev);
    }
    let orig_cp = Checkpoint::signed(
        s.as_ref(),
        Seq(orig_chain.next_seq() - 1),
        &orig_chain.head(),
        EpochMs(42),
    );

    // Build tampered partition: same structure but different values → different chain head
    // but reuse the OLD checkpoint (signed over the original head)
    let mut tampered_chain = ChainState::new();
    let mut tampered_records: Vec<crate::sink::AuditRecord> = (0..2u8)
        .map(|i| crate::sink::AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: format!("{{\"i\":{},\"tampered\":true}}", i + 10).into_bytes(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        })
        .collect();
    for rec in &mut tampered_records {
        let (seq, prev) = tampered_chain.extend(&rec.value);
        rec.push_chain_headers(seq, &prev);
    }

    let mut log = Log::open(tmp_tampered.path(), LogConfig::default()).unwrap();
    let mut offset = 0i64;
    for rec in &tampered_records {
        let mut b = audit_record_to_batch(rec, offset);
        log.append(&mut b).unwrap();
        offset += 1;
    }
    // Reuse the OLD checkpoint (signed over original chain head — won't match tampered head)
    let mut b = audit_record_to_batch(&orig_cp.to_record(), offset);
    log.append(&mut b).unwrap();

    let _ = tmp_orig; // keep alive

    let trusted = TrustedKeys::single("k1".into(), pubkey);
    let report = verify_partition_dir(tmp_tampered.path(), &trusted).unwrap();
    check!(
        !report.ok,
        "stale checkpoint over wrong chain_head must be detected"
    );
    let reason = &report.first_break.unwrap().reason;
    check!(
        reason.contains("chain_head"),
        "reason should mention chain_head, got: {reason}"
    );
}
