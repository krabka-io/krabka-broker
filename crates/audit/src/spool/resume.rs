//! Recovery of the hash-chain position from the records still in the spool.
//!
//! After a crash the writer has to continue the chain where the spooled
//! records left off, so this module walks the spool backwards to the last
//! chained record and recomputes the sequence number and the head hash that
//! follow it. Checkpoints carry no chain headers and are skipped.

use super::Spool;
use crate::{
    chain::{chain_hash, from_hex32},
    event::AuditEventClass,
    ids::Seq,
    sink::{AuditError, AuditRecord, HEADER_PREV_HASH, HEADER_SEQ},
};

impl Spool {
    /// The `(next_seq, head)` from the last chained record in the spool.
    ///
    /// A chained record is a record that is not a checkpoint. Returns `None` if
    /// the spool has no chained record.
    #[tracing::instrument(level = "debug", skip_all, fields(count = self.count.0), err)]
    /// # Errors
    /// Returns an error if the spool file cannot be opened or read.
    pub fn resume_point(&self) -> Result<Option<(u64, [u8; 32])>, AuditError> {
        let records = self.read_all()?;
        Ok(records
            .iter()
            .rev()
            .find_map(resume_from_record)
            .map(|(seq, head)| (seq.0, head)))
    }
}

/// Compute `(next_seq, head_after)` from one chained record.
///
/// The computation uses the record's headers and its value. Returns `None` for
/// a checkpoint, and for a record without the chain headers.
fn resume_from_record(rec: &AuditRecord) -> Option<(Seq, [u8; 32])> {
    if rec.class == AuditEventClass::Checkpoint {
        return None;
    }
    let mut seq: Option<u64> = None;
    let mut prev: Option<[u8; 32]> = None;
    for (k, v) in &rec.headers {
        if k == HEADER_SEQ {
            seq = std::str::from_utf8(v).ok().and_then(|s| s.parse().ok());
        } else if k == HEADER_PREV_HASH {
            prev = std::str::from_utf8(v).ok().and_then(from_hex32);
        }
    }
    let (seq, prev) = (seq?, prev?);
    let head = chain_hash(&prev, seq, &rec.value);
    Some((Seq(seq + 1), head))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::{
        chain::{GENESIS_HEAD, to_hex},
        spool::test_support::{ROOMY_CAP, chained_record},
    };

    #[test]
    fn resume_point_is_from_last_chained_record_skipping_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let prev0 = GENESIS_HEAD;
        let r0 = chained_record(0, &prev0, b"v0");
        let head0 = chain_hash(&prev0, 0, b"v0");
        let r1 = chained_record(1, &head0, b"v1");
        let head1 = chain_hash(&head0, 1, b"v1");
        // a checkpoint record (no seq/prev_hash) must be skipped
        let cp = AuditRecord {
            class: AuditEventClass::Checkpoint,
            value: b"{\"type\":\"checkpoint\"}".to_vec(),
            headers: vec![("event_class".into(), b"checkpoint".to_vec())],
        };
        s.append(&r0).unwrap();
        s.append(&r1).unwrap();
        s.append(&cp).unwrap();
        let (next_seq, head) = s.resume_point().unwrap().unwrap();
        // after seq 1; the hex projection also checks r1's chain math.
        check!((next_seq, head, to_hex(&head)) == (2, head1, to_hex(&head1)));
        let _ = (HEADER_SEQ, HEADER_PREV_HASH); // used by impl
    }
}
