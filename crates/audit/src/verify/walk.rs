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

use super::{TrustedKeys, VerifyBreak, VerifyReport};
use crate::{
    chain::{GENESIS_HEAD, chain_hash, from_hex32},
    checkpoint::Checkpoint,
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

fn broke(
    records: RecordCount,
    checkpoints: CheckpointCount,
    offset: i64,
    seq: Option<Seq>,
    reason: &str,
) -> VerifyReport {
    VerifyReport {
        records,
        checkpoints,
        ok: false,
        first_break: Some(VerifyBreak {
            offset,
            seq,
            reason: reason.to_string(),
        }),
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
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "checkpoint value is not JSON",
        ));
    };
    let Some(cp) = Checkpoint::from_value(&json) else {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "malformed checkpoint",
        ));
    };
    let Some(pubkey) = trusted.get(&cp.key_id) else {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            &format!("no trusted key for key_id '{}'", cp.key_id),
        ));
    };
    if !cp.verify(pubkey) {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "checkpoint signature invalid",
        ));
    }
    if cp.chain_head != state.head {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "checkpoint chain_head does not match recomputed chain",
        ));
    }
    if cp.seq_high != Seq(state.expected_seq.0.saturating_sub(1)) {
        return Err(broke(
            state.records,
            state.checkpoints,
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
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            seq,
            "missing/invalid chain headers",
        ));
    };
    if seq != state.expected_seq {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            Some(seq),
            &format!("seq gap: expected {}, found {seq}", state.expected_seq),
        ));
    }
    if prev != state.head {
        return Err(broke(
            state.records,
            state.checkpoints,
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
