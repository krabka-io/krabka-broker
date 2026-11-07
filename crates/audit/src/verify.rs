//! Offline verification of an audit partition's hash-chain and signed checkpoints.
//!
//! The verifier reads `<dir>/*.log` segment files directly. It does NO recovery
//! and NO truncation, so tail corruption stays visible. It recomputes the chain
//! with the same primitives the writer used, and it validates each checkpoint
//! signature against a trusted key. This file holds the report types and the
//! segment walk; the per-record chain and checkpoint checks live in `walk`.

use {
    self::walk::{
        WalkState, broke, check_chained, check_checkpoint, check_records_lost, header,
        records_lost_body_has_field,
    },
    crate::{
        checkpoint::EVENT_CLASS_CHECKPOINT,
        event::AuditEventClass,
        ids::{CheckpointCount, RecordCount, Seq},
        sink::AuditError,
    },
    krabka_protocol::records::RecordBatch,
    std::{collections::HashMap, fs, path::Path},
    tracing::field::Empty,
};

mod walk;

#[cfg(test)]
mod tests;

/// Trusted public keys, keyed by `key_id`.
#[derive(Debug, Default)]
pub struct TrustedKeys {
    keys: HashMap<String, Vec<u8>>,
}

impl TrustedKeys {
    #[must_use]
    pub fn single(key_id: String, public_key: Vec<u8>) -> Self {
        let mut keys = HashMap::new();
        keys.insert(key_id, public_key);
        Self { keys }
    }

    #[must_use]
    pub fn get(&self, key_id: &str) -> Option<&[u8]> { self.keys.get(key_id).map(Vec::as_slice) }
}

/// First detected break in the chain or signatures.
#[derive(Debug, Clone)]
pub struct VerifyBreak {
    pub offset: i64,
    pub seq: Option<Seq>,
    pub reason: String,
    /// The position, in bytes from the start of its segment file, of an
    /// undecodable remainder. `None` for a break inside a decoded record.
    pub byte_position: Option<u64>,
}

/// A valid chain marker that declares fail-open audit loss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyLoss {
    pub offset: i64,
    pub seq: Seq,
    pub records: RecordCount,
}

/// Result of a partition verification.
///
/// `unanchored_records` is only meaningful when `ok` is `true`. It counts the
/// records with a seq greater than the highest seq that the last valid signed
/// checkpoint covers. When `ok` is `false`, this field is 0: the walk stopped
/// at the break, before it could establish a reliable count.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub records: RecordCount,
    pub checkpoints: CheckpointCount,
    pub ok: bool,
    pub first_break: Option<VerifyBreak>,
    /// Valid, hash-chained declarations of fail-open record loss.
    pub losses: Vec<VerifyLoss>,
    /// Number of records that a signed checkpoint does NOT cover, that is, the
    /// unsigned tail. Zero means the chain is fully attested. This field is
    /// only meaningful when `ok` is `true`.
    pub unanchored_records: RecordCount,
}

/// Verify the audit partition under `dir`.
///
/// This function reads all `*.log` segment files in base-offset order, which is
/// the filename order. It decodes each `RecordBatch` directly and not through
/// `Log::open`: that path runs recovery and truncation, which would silently
/// mask tail corruption. The function then recomputes the hash-chain, and it
/// validates every checkpoint signature against `trusted`.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(dir = %dir.display(), records = Empty, checkpoints = Empty, ok = Empty),
    err
)]
/// # Errors
/// Returns an error if the verifier cannot read `dir`. Returns an error if the
/// verifier cannot read one of the segment files. A detected break in the chain
/// or in a signature is not an error: it is an `Ok` report with `ok` set to
/// `false`. So are bytes that do not decode as a batch, such as a torn tail
/// after a crash: the break carries their byte position in the segment.
pub fn verify_partition_dir(dir: &Path, trusted: &TrustedKeys) -> Result<VerifyReport, AuditError> {
    let read_dir_error = |e| AuditError::Sink(format!("read dir {}: {e}", dir.display()));
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir).map_err(read_dir_error)? {
        let path = entry.map_err(read_dir_error)?.path();
        if path.extension().is_some_and(|x| x == "log") {
            segments.push(path);
        }
    }
    segments.sort();

    let mut state = WalkState::new();
    let mut next_offset = 0i64;

    for seg in segments {
        let bytes = fs::read(&seg)
            .map_err(|e| AuditError::Sink(format!("read segment {}: {e}", seg.display())))?;
        let mut cur: &[u8] = &bytes;
        while !cur.is_empty() {
            let position = bytes.len() - cur.len();
            let Ok(batch) = RecordBatch::decode_batch(&mut cur) else {
                let report = undecodable_tail(&state, next_offset, &seg, position);
                record_report(&report);
                return Ok(report);
            };
            next_offset = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
            for rec in &batch.records {
                let offset = batch.base_offset + i64::from(rec.offset_delta);
                let class = header(rec, "event_class").unwrap_or_default();
                let records_lost_header =
                    class == AuditEventClass::RecordsLost.as_header().as_bytes();
                let records_lost_body = records_lost_body_has_field(rec);
                let result = if class == EVENT_CLASS_CHECKPOINT.as_bytes() {
                    check_checkpoint(rec, offset, &mut state, trusted)
                } else if records_lost_header || records_lost_body {
                    check_records_lost(rec, offset, &mut state)
                } else {
                    check_chained(rec, offset, &mut state)
                };
                if let Err(report) = result {
                    record_report(&report);
                    return Ok(report);
                }
            }
        }
    }

    let unanchored_records = match state.last_checkpoint_seq_high {
        Some(seq_high) => RecordCount(state.records.0.saturating_sub(seq_high.0 + 1)),
        None => state.records,
    };

    let report = VerifyReport {
        records: state.records,
        checkpoints: state.checkpoints,
        ok: true,
        first_break: None,
        losses: state.losses,
        unanchored_records,
    };
    record_report(&report);
    Ok(report)
}

/// The break for bytes at `position` in `segment` that do not decode as a
/// batch. A torn write after a crash looks the same as tampering here, so the
/// verifier reports both rather than stop silently at the last good batch.
fn undecodable_tail(
    state: &WalkState,
    next_offset: i64,
    segment: &Path,
    position: usize,
) -> VerifyReport {
    let mut report = broke(
        state,
        next_offset,
        None,
        &format!(
            "undecodable bytes at byte {position} of segment {}",
            segment.display()
        ),
    );
    if let Some(found) = report.first_break.as_mut() {
        found.byte_position = u64::try_from(position).ok();
    }
    report
}

fn record_report(report: &VerifyReport) {
    let span = tracing::Span::current();
    span.record("records", report.records.0);
    span.record("checkpoints", report.checkpoints.0);
    span.record("ok", report.ok);
}
