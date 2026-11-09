//! Unit tests for the offline verifier, driven end to end through
//! [`verify_partition_dir`] over partitions built on disk.

use {
    super::*,
    crate::{
        FileEd25519Signer,
        chain::{ChainLink, ChainState, GENESIS_HEAD},
        checkpoint::Checkpoint,
        event::AuditEventClass,
        ids::EpochMs,
        sink::AuditRecord,
        test_support::{TestSigner, shared_signer as signer},
    },
    assert2::check,
    bytes::Bytes,
    krabka_log::{Log, LogConfig},
    krabka_protocol::records::{Record, RecordBatch, RecordHeader},
    std::{ops::Range, path::Path},
};

fn audit_record_to_batch(rec: &AuditRecord, base_offset: i64) -> RecordBatch {
    let mut batch = RecordBatch {
        base_offset,
        last_offset_delta: 0,
        ..RecordBatch::default()
    };
    batch.records.push(record_at_delta(rec, 0));
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
        batch.records.push(record_at_delta(
            rec,
            i32::try_from(i).expect("delta fits i32"),
        ));
    }
    batch
}

fn record_at_delta(rec: &AuditRecord, offset_delta: i32) -> Record {
    let headers = rec
        .headers
        .iter()
        .map(|(k, v)| RecordHeader {
            key: k.clone(),
            value: Some(Bytes::from(v.clone())),
        })
        .collect();
    Record {
        offset_delta,
        value: Some(Bytes::from(rec.value.clone())),
        headers,
        ..Default::default()
    }
}

fn lifecycle_record(value: Vec<u8>) -> AuditRecord {
    AuditRecord {
        class: AuditEventClass::ApplicationLifecycle,
        value,
        headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
    }
}

/// An empty partition log and the temporary directory that backs it.
struct TempPartition {
    dir: tempfile::TempDir,
    log: Log,
}

fn partition() -> TempPartition {
    let dir = tempfile::tempdir().unwrap();
    let log = Log::open(dir.path(), LogConfig::default()).unwrap();
    TempPartition { dir, log }
}

fn untrusted_report(directory: &Path) -> VerifyReport {
    verify_partition_dir(directory, &TrustedKeys::default()).unwrap()
}

fn trusted_report(directory: &Path, public_key: Vec<u8>) -> VerifyReport {
    verify_partition_dir(directory, &TrustedKeys::single("k1".into(), public_key)).unwrap()
}

fn signed_head(chain: &ChainState, signer: &FileEd25519Signer, time: i64) -> Checkpoint {
    Checkpoint::signed(
        signer,
        Seq(chain.next_seq() - 1),
        &chain.head(),
        EpochMs(time),
    )
}

fn reserved_marker_report(count: u64, relabel: bool) -> VerifyReport {
    let TempPartition {
        dir: directory,
        mut log,
    } = partition();
    let mut marker = AuditRecord::records_lost(count, 1);
    if relabel {
        marker.headers[0].1 = b"application_lifecycle".to_vec();
    }
    marker.push_chain_headers(0, &GENESIS_HEAD);
    append_record(&mut log, &marker, 0);
    untrusted_report(directory.path())
}

fn append_record(log: &mut Log, record: &AuditRecord, offset: i64) {
    log.append(&mut audit_record_to_batch(record, offset))
        .unwrap();
}

fn chain_record(chain: &mut ChainState, record: &mut AuditRecord) {
    let ChainLink { seq, prev_head } = chain.extend(&record.value);
    record.push_chain_headers(seq, &prev_head);
}

fn check_break(report: &VerifyReport, missing: &str, reason: &str) {
    check!(
        report
            .first_break
            .as_ref()
            .expect(missing)
            .reason
            .contains(reason)
    );
}

fn append_lifecycle_chain(
    log: &mut Log,
    chain: &mut ChainState,
    offset: &mut i64,
    values: Range<u8>,
) {
    for i in values {
        let mut record = lifecycle_record(format!("{{\"i\":{i}}}").into_bytes());
        chain_record(chain, &mut record);
        append_record(log, &record, *offset);
        *offset += 1;
    }
}

/// A chained partition that ends in a signed checkpoint.
struct CheckpointedPartition {
    public_key: Vec<u8>,
    log: Log,
}

fn checkpointed_partition(directory: &Path, time: i64) -> CheckpointedPartition {
    let TestSigner { signer, public_key } = signer();
    let mut log = Log::open(directory, LogConfig::default()).unwrap();
    let mut chain = ChainState::new();
    let mut offset = 0i64;
    append_lifecycle_chain(&mut log, &mut chain, &mut offset, 0..3);
    let checkpoint = signed_head(&chain, signer.as_ref(), time);
    append_record(&mut log, &checkpoint.to_record(), offset);
    CheckpointedPartition { public_key, log }
}

fn append_and_verify(log: &mut Log, record: &AuditRecord, directory: &Path) -> VerifyReport {
    append_record(log, record, 0);
    untrusted_report(directory)
}

#[test]
fn records_lost_marker_is_reported_without_breaking_the_chain() {
    let tmp = tempfile::tempdir().unwrap();
    let TestSigner { signer, public_key } = signer();
    let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
    let mut chain = ChainState::new();
    let mut marker = AuditRecord::records_lost(3, 1);
    chain_record(&mut chain, &mut marker);
    append_record(&mut log, &marker, 0);
    let checkpoint = signed_head(&chain, signer.as_ref(), 123);
    log.append(&mut audit_record_to_batch(&checkpoint.to_record(), 1))
        .unwrap();

    let report = trusted_report(tmp.path(), public_key);
    check!(
        (
            report.ok,
            report.records,
            report.checkpoints,
            report.losses,
            report.unanchored_records,
        ) == (
            true,
            RecordCount(1),
            CheckpointCount(1),
            vec![VerifyLoss {
                offset: 0,
                seq: Seq(0),
                records: RecordCount(3),
            }],
            RecordCount(0),
        )
    );
}

#[test]
fn records_lost_body_cannot_be_hidden_by_changing_its_header() {
    let report = reserved_marker_report(3, true);
    check!(!report.ok);
    check_break(
        &report,
        "body/header mismatch",
        "body/event_class header mismatch",
    );
    check!(report.losses.is_empty());
}

#[test]
fn records_lost_body_without_a_generation_is_rejected() {
    let TempPartition { dir: tmp, mut log } = partition();
    let mut chain = ChainState::new();
    let mut marker = AuditRecord::records_lost(4, 1);
    marker.value = br#"{"records_lost":4}"#.to_vec();
    chain_record(&mut chain, &mut marker);
    let report = append_and_verify(&mut log, &marker, tmp.path());
    check!((report.ok, report.records, report.losses.is_empty()) == (false, RecordCount(0), true));
    check_break(&report, "generation-less marker", "records-lost");
}

#[test]
fn persisted_loss_generation_must_advance() {
    let TempPartition { dir: tmp, mut log } = partition();
    let mut chain = ChainState::new();
    for (offset, generation) in [2_u64, 2].into_iter().enumerate() {
        let mut marker = AuditRecord::records_lost(4, generation);
        chain_record(&mut chain, &mut marker);
        log.append(&mut audit_record_to_batch(
            &marker,
            i64::try_from(offset).unwrap(),
        ))
        .unwrap();
    }

    let report = untrusted_report(tmp.path());
    check!(!report.ok);
    check!(report.records == RecordCount(1));
    check!(report.losses.len() == 1);
    check_break(&report, "stale loss generation", "records-lost");
}

#[test]
fn malformed_reserved_loss_body_cannot_hide_behind_another_header() {
    let report = reserved_marker_report(0, true);
    check!(!report.ok);
    check!(report.records == RecordCount(0));
}

#[test]
fn records_lost_marker_does_not_excuse_a_sequence_gap() {
    let TempPartition { dir: tmp, mut log } = partition();
    let mut chain = ChainState::new();
    let _ = chain.extend(b"missing");
    let mut marker = AuditRecord::records_lost(1, 1);
    chain_record(&mut chain, &mut marker);
    let report = append_and_verify(&mut log, &marker, tmp.path());
    check!(!report.ok);
    check_break(&report, "sequence gap", "seq gap");
    check!(report.losses.is_empty());
}

#[test]
fn records_lost_marker_requires_a_positive_count() {
    let report = reserved_marker_report(0, false);
    check!(!report.ok);
    check_break(&report, "malformed marker", "records-lost");
    check!(report.records == RecordCount(0));
}

#[test]
fn signed_checkpoint_cannot_claim_an_empty_chain() {
    let tmp = tempfile::tempdir().unwrap();
    let TestSigner { signer, public_key } = signer();
    let checkpoint = Checkpoint::signed(signer.as_ref(), Seq(0), &GENESIS_HEAD, EpochMs(123));
    let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
    log.append(&mut audit_record_to_batch(&checkpoint.to_record(), 0))
        .unwrap();

    let report = trusted_report(tmp.path(), public_key);
    check!(!report.ok);
    check_break(&report, "empty checkpoint", "seq_high");
}

// ── Fix 2 tests: direct tamper-detection (chain-inconsistent fixtures) ────

/// A break in the second record of a batch is reported at
/// `base_offset + offset_delta`.
///
/// Every other test writes one record per batch at delta 0, where the base
/// alone is the offset and so a sum is indistinguishable from a difference
/// or a product. Here the record sits at base 1, delta 1: the offset is 2,
/// and 1 and 0 are what the other arithmetic would give.
#[test]
fn break_offset_within_a_batch_is_base_plus_delta() {
    let TempPartition { dir: tmp, mut log } = partition();
    let mut chain = ChainState::new();

    let record = |value: &str, chain: &mut ChainState, tamper: bool| {
        let mut rec = lifecycle_record(value.as_bytes().to_vec());
        let ChainLink { seq, prev_head } = chain.extend(&rec.value);
        rec.push_chain_headers(seq, if tamper { &GENESIS_HEAD } else { &prev_head });
        rec
    };

    // Offset 0, its own batch, chain intact.
    let rec0 = record("{\"i\":0}", &mut chain, false);
    append_record(&mut log, &rec0, 0);

    // Offsets 1 and 2 in one batch. The second carries a wrong prev hash,
    // so the walk breaks on it rather than on the batch's first record.
    let rec1 = record("{\"i\":1}", &mut chain, false);
    let rec2 = record("{\"i\":2}", &mut chain, true);
    log.append(&mut audit_records_to_batch(&[rec1, rec2], 1))
        .unwrap();

    let report = untrusted_report(tmp.path());
    check!(!report.ok, "a wrong prev_hash must break the walk");
    let brk = report.first_break.expect("a break was reported");
    check!(brk.offset == 2, "break offset, got {}", brk.offset);
    check!(
        brk.reason.contains("prev_hash"),
        "reason should name prev_hash, got: {}",
        brk.reason
    );
}

/// Each key id is trusted with the public key at its own position.
#[test]
fn paired_keys_trust_each_key_under_its_own_key_id() {
    let trusted = TrustedKeys::paired(vec!["old".into(), "new".into()], vec![
        b"old-key".to_vec(),
        b"new-key".to_vec(),
    ])
    .unwrap();
    check!(
        (trusted.get("old"), trusted.get("new"), trusted.get("other"))
            == (Some(&b"old-key"[..]), Some(&b"new-key"[..]), None)
    );
}

/// A key id without a public key is refused rather than paired off by one.
#[test]
fn paired_keys_refuse_a_count_mismatch() {
    let error = TrustedKeys::paired(vec!["old".into(), "new".into()], vec![b"old-key".to_vec()])
        .unwrap_err();
    check!(
        error.to_string()
            == "audit key: 2 key id(s) but 1 public key(s): each key id pairs with the public key \
                at the same position"
    );
}

/// The kind of bytes a torn or tampered write leaves after the last batch.
#[derive(Debug, Clone, Copy)]
enum CorruptTail {
    Garbage,
    HalfBatch,
}

/// A checkpointed partition whose only segment ends in `tail`, and the byte
/// position where the valid batches end.
fn partition_with_corrupt_tail(directory: &Path, tail: CorruptTail) -> (Vec<u8>, u64) {
    let CheckpointedPartition { public_key, log } = checkpointed_partition(directory, 100);
    drop(log);
    let segment = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|x| x == "log"))
        .expect("the partition has a segment");
    let mut bytes = fs::read(&segment).unwrap();
    let valid = u64::try_from(bytes.len()).unwrap();
    let appended = match tail {
        CorruptTail::Garbage => vec![0xde; 40],
        CorruptTail::HalfBatch => bytes[..bytes.len() / 8].to_vec(),
    };
    bytes.extend_from_slice(&appended);
    fs::write(&segment, bytes).unwrap();
    (public_key, valid)
}

#[test]
fn undecodable_tail_fails_verification_at_its_byte_position() {
    for tail in [CorruptTail::Garbage, CorruptTail::HalfBatch] {
        let tmp = tempfile::tempdir().unwrap();
        let (public_key, valid) = partition_with_corrupt_tail(tmp.path(), tail);

        let report = trusted_report(tmp.path(), public_key);

        let found = report
            .first_break
            .as_ref()
            .map(|found| (found.offset, found.seq, found.byte_position));
        check!(
            (report.ok, report.records.0, report.checkpoints.0, found)
                == (false, 3, 1, Some((4, None, Some(valid)))),
            "tail: {tail:?}"
        );
        check_break(&report, "an undecodable tail is a break", "undecodable");
    }
}
