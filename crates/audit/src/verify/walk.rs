//! The per-record walk: the running chain state, and the two checks that
//! advance it.
//!
//! [`check_chained`] recomputes a data record's hash link and [`check_checkpoint`]
//! validates a signed checkpoint against that running chain, both against the
//! same [`WalkState`] the segment walk in [`super::verify_partition_dir`] carries
//! from record to record. They are the primitives the writer's own chaining
//! mirrors, kept in one file because a break found by either one is reported the
//! same way.

use krabka_protocol::records::Record;

use super::{TrustedKeys, VerifyBreak, VerifyLoss, VerifyReport};
use crate::{
    chain::{GENESIS_HEAD, chain_hash, from_hex32},
    checkpoint::Checkpoint,
    event::AuditEventClass,
    ids::{CheckpointCount, RecordCount, Seq},
    sink::{HEADER_PREV_HASH, HEADER_SEQ},
};

pub(super) fn header<'a>(record: &'a Record, key: &str) -> Option<&'a [u8]> {
    record
        .headers
        .iter()
        .find(|h| h.key == key)
        .and_then(|h| h.value.as_deref())
}

fn broke(state: &WalkState, offset: i64, seq: Option<Seq>, reason: &str) -> VerifyReport {
    VerifyReport {
        records: state.records,
        checkpoints: state.checkpoints,
        ok: false,
        first_break: Some(VerifyBreak {
            offset,
            seq,
            reason: reason.to_string(),
        }),
        losses: state.losses.clone(),
        // unanchored_records is 0 when ok=false; the count is not meaningful
        // after a break since the walk stopped early.
        unanchored_records: RecordCount(0),
    }
}

/// Mutable per-record walk state that the helper functions share.
pub(super) struct WalkState {
    head: [u8; 32],
    expected_seq: Seq,
    pub(super) records: RecordCount,
    pub(super) checkpoints: CheckpointCount,
    pub(super) losses: Vec<VerifyLoss>,
    /// The `seq_high` of the most-recently validated checkpoint, if any.
    pub(super) last_checkpoint_seq_high: Option<Seq>,
}

impl WalkState {
    pub(super) fn new() -> Self {
        Self {
            head: GENESIS_HEAD,
            expected_seq: Seq(0),
            records: RecordCount(0),
            checkpoints: CheckpointCount(0),
            losses: Vec::new(),
            last_checkpoint_seq_high: None,
        }
    }
}

/// Validate a checkpoint record against the running chain and trusted keys.
///
/// Returns `Err(VerifyReport)` on the first detected break.
pub(super) fn check_checkpoint(
    rec: &Record,
    offset: i64,
    state: &mut WalkState,
    trusted: &TrustedKeys,
) -> Result<(), VerifyReport> {
    state.checkpoints.0 += 1;
    let value = rec.value.as_deref().unwrap_or_default();
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(value) else {
        return Err(broke(state, offset, None, "checkpoint value is not JSON"));
    };
    let Some(cp) = Checkpoint::from_value(&json) else {
        return Err(broke(state, offset, None, "malformed checkpoint"));
    };
    let Some(pubkey) = trusted.get(&cp.key_id) else {
        return Err(broke(
            state,
            offset,
            None,
            &format!("no trusted key for key_id '{}'", cp.key_id),
        ));
    };
    if !cp.verify(pubkey) {
        return Err(broke(state, offset, None, "checkpoint signature invalid"));
    }
    if cp.chain_head != state.head {
        return Err(broke(
            state,
            offset,
            None,
            "checkpoint chain_head does not match recomputed chain",
        ));
    }
    if cp.seq_high != Seq(state.expected_seq.0.saturating_sub(1)) {
        return Err(broke(
            state,
            offset,
            None,
            "checkpoint seq_high does not match record count",
        ));
    }
    state.last_checkpoint_seq_high = Some(cp.seq_high);
    Ok(())
}

/// Validate a chained data record and advance the chain head.
///
/// Returns `Err(VerifyReport)` on the first detected break.
pub(super) fn check_chained(
    rec: &Record,
    offset: i64,
    state: &mut WalkState,
) -> Result<(), VerifyReport> {
    let value = rec.value.as_deref().unwrap_or_default();
    let seq = header(rec, HEADER_SEQ)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Seq);
    let prev = header(rec, HEADER_PREV_HASH)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(from_hex32);
    let (Some(seq), Some(prev)) = (seq, prev) else {
        return Err(broke(state, offset, seq, "missing/invalid chain headers"));
    };
    if seq != state.expected_seq {
        return Err(broke(
            state,
            offset,
            Some(seq),
            &format!("seq gap: expected {}, found {seq}", state.expected_seq),
        ));
    }
    if prev != state.head {
        return Err(broke(
            state,
            offset,
            Some(seq),
            "prev_hash does not match recomputed chain head",
        ));
    }
    state.head = chain_hash(&state.head, seq.0, value);
    state.expected_seq.0 += 1;
    state.records.0 += 1;
    Ok(())
}

/// Validate and record a chained fail-open loss marker.
pub(super) fn check_records_lost(
    rec: &Record,
    offset: i64,
    state: &mut WalkState,
) -> Result<(), VerifyReport> {
    let header_matches =
        header(rec, "event_class") == Some(AuditEventClass::RecordsLost.as_header().as_bytes());
    let (true, Some(count)) = (header_matches, records_lost_count_from_body(rec)) else {
        return Err(broke(
            state,
            offset,
            None,
            "records-lost body/event_class header mismatch",
        ));
    };

    check_chained(rec, offset, state)?;
    state.losses.push(VerifyLoss {
        offset,
        seq: Seq(state.expected_seq.0 - 1),
        records: RecordCount(count),
    });
    Ok(())
}

/// Return the declared loss count for either supported reserved marker body.
///
/// The legacy shape contains only `records_lost`. Persisted loss state adds a
/// positive `loss_generation`; no other fields are part of the reserved shape.
pub(super) fn records_lost_count_from_body(rec: &Record) -> Option<u64> {
    let value = rec.value.as_deref()?;
    let json = serde_json::from_slice::<serde_json::Value>(value).ok()?;
    let object = json.as_object()?;
    let count = object
        .get("records_lost")
        .and_then(serde_json::Value::as_u64)
        .filter(|count| *count > 0)?;

    match object.len() {
        1 => Some(count),
        2 => object
            .get("loss_generation")
            .and_then(serde_json::Value::as_u64)
            .filter(|generation| *generation > 0)
            .map(|_| count),
        _ => None,
    }
}
