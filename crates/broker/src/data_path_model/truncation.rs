//! The seam onto the real leader-epoch divergence core: it turns a model log
//! into the leader-epoch entries the log layer indexes, then asks the
//! production code how much of a follower's log survives reconciliation.
//!
//! This is the `OffsetForLeaderEpoch` answer that a follower acts on before it
//! fetches again, so the model gets Kafka's real truncation point rather than
//! a hand-written prefix comparison.

use krabka_log::{EpochEntry, Offset, epoch_and_offset_for_entries};

use super::bounds::model_offset;

/// The leader-epoch entries for a log: one entry per epoch change.
fn epoch_entries(log: &[u8]) -> Vec<EpochEntry> {
    let mut out = Vec::new();
    let mut last: Option<u8> = None;
    for (off, &e) in log.iter().enumerate() {
        if last != Some(e) {
            out.push(EpochEntry {
                epoch: krabka_log::LeaderEpoch(i32::from(e)),
                start_offset: Offset(model_offset(off)),
            });
            last = Some(e);
        }
    }
    out
}

/// Drive the REAL divergence core. It returns the exclusive offset that
/// `follower` keeps when it reconciles against `leader_log`.
pub(super) fn real_truncation_offset(follower_log: &[u8], leader_log: &[u8]) -> i64 {
    let leader_entries = epoch_entries(leader_log);
    let follower_latest = follower_log.last().map_or(-1, |&e| i32::from(e));
    let (_, end) = epoch_and_offset_for_entries(
        &leader_entries,
        krabka_log::LeaderEpoch(follower_latest),
        Offset(model_offset(leader_log.len())),
    );
    // Unwrap the log-layer `Offset` into this model's `i64` world at the seam.
    end.0.min(model_offset(follower_log.len()))
}
