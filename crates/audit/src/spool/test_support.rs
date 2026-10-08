//! Fixtures shared by the unit tests of the `spool` module tree.
//!
//! The module holds the roomy byte cap that keeps a test from tripping the
//! spool's overflow check by accident, and the builder that stamps the chain
//! headers onto a record the way the writer does.

use krabka_units::prelude::{ByteSize, mebibytes};

use crate::{event::AuditEventClass, sink::AuditRecord};

/// A cap that is large enough that no test reaches it by accident.
pub const ROOMY_CAP: ByteSize = mebibytes(1);

pub fn chained_record(seq: u64, prev: &[u8; 32], value: &[u8]) -> AuditRecord {
    let mut r = AuditRecord {
        class: AuditEventClass::ApplicationLifecycle,
        value: value.to_vec(),
        headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
    };
    r.push_chain_headers(seq, prev);
    r
}

pub fn seeded_spool(value: &[u8]) -> (tempfile::TempDir, AuditRecord, crate::Spool) {
    let dir = tempfile::tempdir().unwrap();
    let record = chained_record(0, &crate::GENESIS_HEAD, value);
    let mut spool = crate::Spool::open(dir.path(), ROOMY_CAP).unwrap();
    spool.append(&record).unwrap();
    (dir, record, spool)
}

pub(crate) fn spool_with_losses(
    cap: ByteSize,
    count: u64,
) -> (
    tempfile::TempDir,
    crate::Spool,
    std::sync::Arc<super::PendingLosses>,
) {
    let directory = tempfile::tempdir().unwrap();
    let spool = crate::Spool::open(directory.path(), cap).unwrap();
    let losses = spool.pending_losses();
    losses.add(count);
    (directory, spool, losses)
}

pub(crate) fn reopen_after_losses(
    directory: &std::path::Path,
    spool: crate::Spool,
    losses: std::sync::Arc<super::PendingLosses>,
) -> crate::Spool {
    drop(losses);
    drop(spool);
    crate::Spool::open(directory, ROOMY_CAP).unwrap()
}

/// Checks that the spool file holds its version header and no records.
pub fn check_empty_file(directory: &std::path::Path) {
    assert2::check!(
        std::fs::metadata(directory.join(super::SPOOL_FILE))
            .unwrap()
            .len()
            == super::header_len_u64()
    );
}
