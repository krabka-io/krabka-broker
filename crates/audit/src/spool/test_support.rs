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
