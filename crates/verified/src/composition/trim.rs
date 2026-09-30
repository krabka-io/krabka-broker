use creusot_std::prelude::*;

use super::{
    DeleteRecordsTrimApplication, DeleteRecordsTrimDecision, DeleteRecordsTrimFacts,
    ProducerReloadRange, delete_records_trim_application, delete_records_trim_decision,
    diskless_trim_decision, producer_snapshot_latest_index, producer_snapshot_reload_keeps,
    producer_snapshot_replay_start,
};

/// Fold completed durable steps, including arbitrary pauses/failed attempts.
/// A true trace entry means the selected store reached the planned frontier;
/// it does not mean an RPC acknowledged it. I/O/atomic checkpointing are external.
/// One completed step catches WAL up; two catch both stores up. The global
/// frontier is fixed throughout, and replay after completion cannot advance it.
#[requires(requested@ >= 0 && wal_start@ >= 0 && local_start@ >= 0)]
#[ensures(wal_start@ <= result.0@ && local_start@ <= result.1@)]
#[ensures(result.1@ > local_start@ ==> result.0@ == requested@.max(wal_start@).max(local_start@))]
#[ensures(result.0@ <= requested@.max(wal_start@).max(local_start@)
    && result.1@ <= requested@.max(wal_start@).max(local_start@))]
#[ensures((exists<i: Int> 0 <= i && i < applied@.len() && applied@[i])
    ==> result.0@ == requested@.max(wal_start@).max(local_start@))]
#[ensures((exists<i: Int, j: Int> 0 <= i && i < j && j < applied@.len()
    && applied@[i] && applied@[j])
    ==> result.0 == result.1
        && result.1@ == requested@.max(wal_start@).max(local_start@))]
#[ensures((forall<i: Int> 0 <= i && i < applied@.len() ==> !applied@[i])
    ==> result == (wal_start, local_start))]
pub(super) fn trim_steps_converge(
    requested: i64,
    wal_start: i64,
    local_start: i64,
    applied: &[bool],
) -> (i64, i64) {
    let _frontier = requested.max(wal_start).max(local_start);
    let mut wal = wal_start;
    let mut local = local_start;
    let mut i = 0usize;
    #[invariant(i@ <= applied@.len())]
    #[invariant(wal_start@ <= wal@ && wal@ <= _frontier@)]
    #[invariant(local_start@ <= local@ && local@ <= _frontier@)]
    #[invariant(requested@.max(wal@).max(local@) == _frontier@)]
    #[invariant(local@ > local_start@ ==> wal == _frontier)]
    #[invariant((exists<j: Int> 0 <= j && j < i@ && applied@[j]) ==> wal == _frontier)]
    #[invariant((exists<j: Int, k: Int> 0 <= j && j < k && k < i@
        && applied@[j] && applied@[k]) ==> local == _frontier)]
    #[invariant((forall<j: Int> 0 <= j && j < i@ ==> !applied@[j])
        ==> wal == wal_start && local == local_start)]
    #[variant(applied@.len() - i@)]
    while i < applied.len() {
        if applied[i] {
            match delete_records_trim_application(requested, wal, local) {
                DeleteRecordsTrimApplication::TrimWal { frontier } => wal = frontier,
                DeleteRecordsTrimApplication::TrimLocal { frontier } => local = frontier,
                DeleteRecordsTrimApplication::Complete { .. } => {}
                DeleteRecordsTrimApplication::RejectMalformed => unreachable!(),
            }
        }
        i += 1;
    }
    (wal, local)
}

/// Admission and two completed reconciliation steps preserve HWM/delivery
/// bounds, make the same request a no-op, and bound producer reload selection
/// and its cursor above the new floor. Prior store frontiers must themselves be
/// bounded; admission alone cannot establish that. Snapshot contents and whole
/// batches read around a cursor may still contain historical producer metadata.
#[requires(0 <= wal_start@ && wal_start@ <= facts.high_watermark@
    && wal_start@ <= facts.log_end@)]
#[requires(0 <= local_start@ && local_start@ <= facts.high_watermark@
    && local_start@ <= facts.log_end@)]
#[requires(facts.has_delivery_watermark ==> wal_start@ <= facts.delivery_watermark@
    && local_start@ <= facts.delivery_watermark@)]
#[ensures(result)]
pub(super) fn admitted_trim_bounds_reload_and_retry(
    facts: DeleteRecordsTrimFacts,
    wal_start: i64,
    local_start: i64,
    snapshots: &[i64],
) -> bool {
    let target = match delete_records_trim_decision(facts) {
        DeleteRecordsTrimDecision::Apply { frontier }
        | DeleteRecordsTrimDecision::Noop { frontier } => frontier,
        DeleteRecordsTrimDecision::RejectMalformed
        | DeleteRecordsTrimDecision::RejectOutOfRange => return true,
    };
    let (wal, local) = trim_steps_converge(target, wal_start, local_start, &[true, true]);
    if wal != local
        || local > facts.high_watermark
        || local > facts.log_end
        || (facts.has_delivery_watermark && local > facts.delivery_watermark)
    {
        return false;
    }
    match delete_records_trim_decision(DeleteRecordsTrimFacts {
        current_start: local,
        ..facts
    }) {
        DeleteRecordsTrimDecision::Noop { frontier } if frontier == local => {}
        _ => return false,
    }
    match delete_records_trim_application(target, wal, local) {
        DeleteRecordsTrimApplication::Complete { frontier } if frontier == local => {}
        _ => return false,
    }
    let range = ProducerReloadRange {
        log_start: local,
        local_start: local,
        log_end: facts.log_end,
    };
    let selected = producer_snapshot_latest_index(snapshots, range);
    let snapshot = selected.map(|index| snapshots[index]);
    if let Some(offset) = snapshot
        && (!producer_snapshot_reload_keeps(offset, range) || offset <= local)
    {
        return false;
    }
    match producer_snapshot_replay_start(range, snapshot) {
        Some(cursor) => local <= cursor && cursor <= facts.log_end,
        None => false,
    }
}

/// Eviction of the local WAL/cache stays inside committed object coverage and
/// the HWM safety lag after arbitrary completed/paused reconciliation steps.
/// Previous WAL eviction must obey those same bounds. This changes physical
/// availability; it does not change the logical `DeleteRecords` floor.
#[requires(0 <= wal_start@ && wal_start@ <= indexed_frontier@)]
#[requires(wal_start@ + (if safety_lag@ < 0 { 0 } else { safety_lag@ }) <= high_watermark@)]
#[ensures(result)]
pub(super) fn diskless_trim_reconciliation_preserves_coverage(
    indexed_frontier: i64,
    high_watermark: i64,
    safety_lag: i64,
    wal_start: i64,
    local_start: i64,
    applied: &[bool],
) -> bool {
    let plan = diskless_trim_decision(indexed_frontier, high_watermark, safety_lag, local_start);
    if !plan.should_trim {
        return true;
    }
    let lag = safety_lag.max(0);
    let (wal, local) = trim_steps_converge(plan.target, wal_start, local_start, applied);
    wal <= indexed_frontier
        && local <= indexed_frontier
        && wal <= high_watermark - lag
        && local <= high_watermark - lag
}
