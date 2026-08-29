//! The `OffsetCommit` half of the composition: the independent fence oracle
//! and the transition that drives the real
//! `GroupState::validate_commit_decision` against it.
//!
//! The oracle and the driven call sit in one file because the cross-check
//! between them is the point of this model, and a divergence between the two
//! is what a failure reports.

use std::cmp::Ordering;

use krabka_log::Offset;

use super::{
    MAX_OFFSET,
    projection::rebuild_group,
    state::{CgcState, EpochKind, committed_map, member},
};
use crate::coordinator::unified::consumer_state::GroupState;

/// INDEPENDENT oracle for the `OffsetCommit` fence: the expected decision for a
/// member that presents `epoch`. It deliberately uses a different structure
/// (`Ordering`) from the real `validate_commit_decision`'s if-guards. The model
/// therefore drives the real fn and asserts equality as a genuine cross-check.
/// A fence regression diverges.
fn oracle_commit(g: &GroupState, id: &str, epoch: i32) -> Result<(), i16> {
    match g.members.get(id) {
        None => Err(crate::codes::UNKNOWN_MEMBER_ID),
        Some(m) => match epoch.cmp(&m.member_epoch) {
            Ordering::Less => Err(crate::codes::STALE_MEMBER_EPOCH),
            Ordering::Greater => Err(crate::codes::FENCED_MEMBER_EPOCH),
            Ordering::Equal => Ok(()),
        },
    }
}

/// Drive the REAL `OffsetCommit` epoch fence (`validate_commit_decision`) for
/// the epoch `kind` the member presents, and cross-check it against the
/// independent oracle. Only on accept, that is a current-epoch member, this
/// function advances the bounded committed offset. The member's CURRENT epoch is
/// whatever the real reconciliation last set, so a `Stale` commit after a
/// rebalance is a zombie and the fence stops it. Kafka does NOT check partition
/// ownership here (at-least-once).
pub(super) fn do_commit(last: &CgcState, id: &str, part: i32, kind: EpochKind) -> Option<CgcState> {
    let g = rebuild_group(last);
    let cur = member(last, id).map(|m| m.member_epoch);
    let epoch = match (cur, kind) {
        (Some(e), EpochKind::Current) => e,
        (Some(e), EpochKind::Stale) => e - 1,
        (Some(e), EpochKind::Forward) => e + 1,
        (None, _) => 0,
    };
    let real = g.validate_commit_decision(id, epoch);
    let oracle = oracle_commit(&g, id, epoch);
    assert_eq!(
        real, oracle,
        "OffsetCommit fence diverges from oracle: member={id} epoch={epoch}"
    );
    if real.is_err() {
        return None; // fenced (stale/forward/unknown) — cannot touch the offset
    }
    let mut committed = committed_map(last);
    let off = committed.entry(part).or_insert(Offset(0));
    if *off >= MAX_OFFSET {
        return None;
    }
    *off += 1;
    let mut next = last.clone();
    next.committed = committed.iter().map(|(&k, &v)| (k, v)).collect();
    Some(next)
}
