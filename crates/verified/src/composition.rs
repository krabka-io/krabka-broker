//! Cross-module safety theorems, compiled only for proofs and tests.
//!
//! Each theorem states an aggregate guarantee under explicit preconditions.
//! Calls use the kernels' contracts; no implementation is copied into a model.

#![cfg_attr(creusot, allow(dead_code))] // Theorems are checked without a runtime caller.

use creusot_std::prelude::*;
use krabka_ids::{LeaderEpoch, Offset};

use crate::{
    audit::{AuditLosses, settle_loss_batch},
    broker::{
        DeleteRecordsTrimApplication, DeleteRecordsTrimDecision, DeleteRecordsTrimFacts,
        FetchWatermarks, ReplicaFetchFacts, ReplicaFetchMutation, delete_records_trim_application,
        delete_records_trim_decision, fetch_visibility, replica_fetch_mutation,
    },
    consensus::election_has_quorum,
    delivery::{delivery_watermark_advance, scheduled_delivery_visible},
    diskless::diskless_trim_decision,
    leader_epoch::{EpochEntry, epoch_and_offset_for_entries},
    local_recovery::local_recovery_batch_step,
    log_index::{
        offset_index_lookup, offset_index_position_at_or_after, time_index_lookup,
        time_index_scan_start,
    },
    offset_allocator::{reserve_offsets, wal_reservation_frontier},
    produce::produce_durability_frontier,
    producer_snapshot::{
        ProducerReloadRange, producer_snapshot_latest_index, producer_snapshot_reload_keeps,
        producer_snapshot_replay_start,
    },
    raft::{advance_high_watermark, in_half_open_window},
    remote_read::remote_time_index_candidate_count,
    restore::{
        RestoreBatchFrame, RestoreExclusions, RestoreFilterDecision, RestoreRecordDeltas,
        restore_batch_filter_decision, restore_batch_past_offset_bound, restore_batch_step,
        restore_record_coordinates, restore_record_selected,
    },
    restore_sidecar::{
        RestoreAbortedTxn, RestoreSegmentExtent, restore_index_frontier,
        restore_leader_epoch_entry_valid, restore_offset_index_entry_valid,
        restore_time_index_entry_valid, restore_txn_index_entry_valid,
    },
    storage::{local_append_coordinates, truncation_batch_retained, truncation_frontier},
    timestamp::{earliest_max_timestamp_index, first_timestamp_index, timestamp_scan_next},
    transaction::{
        LogBatchKind, aborted_transaction_interval, aborted_transaction_overlaps,
        first_unstable_offset, log_batch_kind, transaction_marker_closes,
    },
    wal::{
        select_wal_voters, wal_batch_equal, wal_checkpoint_range_valid, wal_covering_batch_range,
        wal_voter_set_valid,
    },
};

/// Allocation, live append, recovery, scanning, and acknowledgement agree on
/// one exclusive frontier, including rejection of invalid or overflowing ranges.
#[ensures(result)]
fn append_frontiers_agree(base: i64, delta: i32) -> bool {
    let append = local_append_coordinates(base, base, delta);
    let acknowledgement = produce_durability_frontier(base, delta);
    let reservation = if delta >= 0 {
        reserve_offsets(base, i64::from(delta) + 1)
    } else {
        None
    };
    match (append, acknowledgement, reservation) {
        (Some((last, next)), Some(ack), Some((first, end))) => {
            let recovery = local_recovery_batch_step(0, 1, base, base, delta, 1);
            first == base
                && base <= last
                && last < ack
                && next == ack
                && end == ack
                && timestamp_scan_next(base, base, delta) == Some(ack)
                && match recovery {
                    Some(step) => step.last_offset == last && step.next_offset == ack,
                    None => false,
                }
        }
        (None, None, None) => true,
        _ => false,
    }
}

/// Extending the pending controller chain and reserving again leaves no gap
/// or overlap. This assumes serialized use of the returned frontier, not a lock.
#[ensures(result)]
fn reservations_do_not_overlap(base: i64, first_count: i64, second_count: i64) -> bool {
    let Some((first, end)) = reserve_offsets(base, first_count) else {
        return true;
    };
    if wal_reservation_frontier(base, first, first_count) != Some(end) {
        return false;
    }
    match reserve_offsets(end, second_count) {
        Some((second, next)) => first < end && end == second && second < next,
        None => true,
    }
}

/// Computing LSO from transaction starts and passing it to read-committed
/// Fetch excludes every unstable transaction as well as undelivered offsets.
/// The transaction starts must be the complete live/unreplicated set.
#[ensures(result)]
fn committed_fetch_excludes_unstable(starts: &[i64], w: FetchWatermarks) -> bool {
    let Some(lso) = first_unstable_offset(starts, w.log_end) else {
        // The caller rejects this corrupt transaction state.
        return true;
    };
    let visibility = fetch_visibility(false, true, FetchWatermarks { lso, ..w }, w.log_start);
    if visibility.limit_offset > w.hw || visibility.limit_offset > w.deliverable {
        return false;
    }
    let mut i = 0usize;
    #[invariant(i@ <= starts@.len())]
    #[variant(starts@.len() - i@)]
    while i < starts.len() {
        if visibility.limit_offset > starts[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// Decode the actual control key, close only its producer, keep a completed
/// transaction unstable until HW passes the entire marker, and expose the
/// largest read-committed prefix allowed by all remaining transactions.
/// `other_starts` must enumerate every other live/unreplicated transaction.
/// The marker span is already admitted by the append-coordinate guard.
#[requires(0 <= span.0@ && 0 <= span.1@ && span.0@ + span.1@ < i64::MAX@)]
#[requires(producer.0@ >= 0 && 0 <= producer.2@ && producer.2@ <= span.0@)]
#[requires(forall<i: Int> 0 <= i && i < other_starts@.len()
    ==> 0 <= other_starts@[i]@ && other_starts@[i]@ <= span.0@)]
#[ensures(result.0@ <= span.0@ + span.1@ + 1
    && result.0@ <= bounds.0@ && result.0@ <= bounds.1@)]
#[ensures(forall<i: Int> 0 <= i && i < other_starts@.len()
    ==> result.0@ <= other_starts@[i]@)]
#[ensures(!(is_control && key@.len() >= 4 && key@[2]@ == 0
    && (key@[3]@ == 0 || key@[3]@ == 1) && producer.0 == producer.1
    && bounds.0@ > span.0@ + span.1@) ==> result.0@ <= producer.2@)]
#[ensures(match result.1 {
    Some((start, last)) => is_control && key@.len() >= 4 && key@[2]@ == 0
        && key@[3]@ == 0 && producer.0 == producer.1
        && start@ == producer.2@ && last@ == span.0@ + span.1@,
    None => !(is_control && key@.len() >= 4 && key@[2]@ == 0
        && key@[3]@ == 0 && producer.0 == producer.1),
})]
#[ensures(forall<v: Int> v <= span.0@ + span.1@ + 1 && v <= bounds.0@ && v <= bounds.1@
    && (forall<i: Int> 0 <= i && i < other_starts@.len() ==> v <= other_starts@[i]@)
    && ((is_control && key@.len() >= 4 && key@[2]@ == 0
        && (key@[3]@ == 0 || key@[3]@ == 1) && producer.0 == producer.1
        && bounds.0@ > span.0@ + span.1@) || v <= producer.2@)
    ==> v <= result.0@)]
fn control_marker_bounds_committed_fetch(
    key: &[u8],
    is_control: bool,
    producer: (i64, i64, i64), // pending PID, marker PID, pending start
    span: (i64, i32),          // marker base and last-offset delta
    other_starts: &[i64],
    bounds: (i64, i64), // HW and deliverable frontier
) -> (i64, Option<(i64, i64)>) {
    let Some((last, end)) = local_append_coordinates(span.0, span.0, span.1) else {
        return (i64::MIN, None);
    };
    let kind = log_batch_kind(is_control, key);
    let is_abort = matches!(kind, LogBatchKind::Abort);
    let is_commit = matches!(kind, LogBatchKind::Commit);
    let closes = transaction_marker_closes(is_abort, is_commit, producer.0 == producer.1);
    let aborted = if closes && is_abort {
        aborted_transaction_interval(Some(producer.2), last, producer.1)
    } else {
        None
    };
    let Some(mut lso) = first_unstable_offset(other_starts, end) else {
        return (i64::MIN, None);
    };
    if !closes || last >= bounds.0 {
        lso = lso.min(producer.2);
    }
    let visibility = fetch_visibility(
        false,
        true,
        FetchWatermarks {
            log_start: 0,
            log_end: end,
            hw: bounds.0,
            lso,
            deliverable: bounds.1,
        },
        0,
    );
    (visibility.limit_offset, aborted)
}

/// A newly advanced consensus watermark gives every consumer fetch limit
/// quorum support. An inherited, unchanged watermark needs prior-epoch evidence.
#[cfg_attr(creusot, requires(1 <= majority@ && majority@ <= followers@.len() + 1))]
#[cfg_attr(creusot, requires(leader_counts || majority@ <= followers@.len()))]
#[cfg_attr(creusot, requires(current@ <= w.log_end@))]
#[cfg_attr(creusot, requires(forall<i: Int> 0 <= i && i < followers@.len()
    ==> followers@[i]@ <= w.log_end@))]
#[cfg_attr(creusot, ensures(current@ <= result.0@ && result.0@ <= w.log_end@))]
#[cfg_attr(creusot, ensures(result.1@ <= result.0@))]
#[cfg_attr(creusot, ensures(result.1@ == result.0@.min(w.lso@).min(w.deliverable@)))]
#[cfg_attr(creusot, ensures(forall<v: Int> v > epoch_start@
    && crate::consensus::count_ge(w.log_end@, followers@, v, leader_counts) >= majority@
    ==> v <= result.0@))]
#[cfg_attr(creusot, ensures(result.0@ > current@ ==>
    crate::consensus::count_ge(w.log_end@, followers@, result.1@, leader_counts) >= majority@))]
fn quorum_commit_bounds_fetch(
    followers: &[i64],
    majority: usize,
    epoch_start: i64,
    current: i64,
    leader_counts: bool,
    w: FetchWatermarks,
) -> (i64, i64) {
    let hw = crate::consensus::recompute_high_watermark(
        w.log_end,
        followers,
        majority,
        epoch_start,
        current,
        leader_counts,
    );
    let limit =
        fetch_visibility(false, true, FetchWatermarks { hw, ..w }, w.log_start).limit_offset;
    #[cfg(creusot)]
    if hw > current {
        proof_assert!({
            crate::consensus::lemma_count_ge_prefix_monotone(
                w.log_end@, followers@, limit@, hw@, followers@.len() + 1, leader_counts,
            );
            crate::consensus::count_ge(w.log_end@, followers@, limit@, leader_counts) >= majority@
        });
    }
    (hw, limit)
}

type WalFetchSupport = (i64, i64, Vec<(u64, i64)>);

/// The actual installer, explicit durable votes, quorum kernel, and consumer
/// Fetch compose into a concrete set of distinct supporting nodes. A floor
/// raised only to log start exposes no retained records; an inherited watermark
/// still needs prior durability evidence. Reported offsets must name actual
/// durable prefixes of the same log, including the local node's fsynced vote.
#[requires(w.log_start@ <= w.log_end@ && current@ <= w.log_end@)]
#[ensures((match result { None => false, Some(_) => true })
    == (voters@.len() == reported@.len() && voters@.len() == expected@ && expected@ > 0
        && voters@[0] == local_node
        && forall<i: Int, j: Int> 0 <= i && i < j && j < voters@.len()
            ==> voters@[i] != voters@[j]))]
#[ensures(match result {
    None => true,
    Some((hw, limit, _)) => current@ <= hw@ && w.log_start@ <= hw@
        && hw@ <= w.log_end@ && limit@ == hw@.min(w.lso@).min(w.deliverable@),
})]
#[ensures(match result {
    None => true,
    Some((_, _, supporters)) => forall<i: Int, j: Int>
        0 <= i && i < j && j < supporters@.len()
            ==> supporters@[i].0 != supporters@[j].0,
})]
#[ensures(match result {
    None => true,
    Some((_, limit, supporters)) => forall<i: Int> 0 <= i && i < supporters@.len()
        ==> supporters@[i].1@ >= limit@
            && exists<j: Int> 0 <= j && j < voters@.len()
                && supporters@[i].0 == voters@[j]
                && supporters@[i].1@ == reported@[j]@.min(w.log_end@),
})]
#[ensures(match result {
    None => true,
    Some((hw, limit, supporters)) => hw@ > current@ && limit@ > w.log_start@
        ==> supporters@.len() >= voters@.len() / 2 + 1,
})]
#[ensures(match result {
    None => true,
    Some((hw, _, _)) => forall<v: Int> current@ < v && w.log_start@ < v && v <= w.log_end@
        && crate::consensus::count_ge(w.log_end@, reported@, v, false) >= voters@.len() / 2 + 1
        ==> v <= hw@,
})]
fn installed_wal_quorum_bounds_fetch(
    voters: &[u64],
    reported: &[i64],
    local_node: u64,
    expected: usize,
    current: i64,
    w: FetchWatermarks,
) -> Option<WalFetchSupport> {
    if voters.len() != reported.len() || !wal_voter_set_valid(voters, local_node, expected) {
        return None;
    }
    let mut ends: Vec<i64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= voters@.len() && ends@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> ends@[j]@ == reported@[j]@.min(w.log_end@))]
    #[variant(voters@.len() - i@)]
    while i < voters.len() {
        ends.push(reported[i].min(w.log_end));
        i += 1;
    }
    #[cfg(creusot)]
    proof_assert!(forall<v: Int> v <= w.log_end@ ==> {
        crate::consensus::lemma_explicit_vote_count_equal(
            w.log_end@, reported@, ends@, v, ends@.len() + 1,
        );
        crate::consensus::count_ge(w.log_end@, reported@, v, false)
            == crate::consensus::count_ge(w.log_end@, ends@, v, false)
    });
    let floor = current.max(w.log_start);
    let majority = crate::consensus::majority_size(voters.len());
    let (hw, limit) = quorum_commit_bounds_fetch(&ends, majority, floor, floor, false, w);
    let mut supporters: Vec<(u64, i64)> = Vec::new();
    i = 0;
    #[invariant(i@ <= voters@.len())]
    #[invariant(supporters@.len() <= i@)]
    #[invariant(supporters@.len()
        == crate::consensus::count_ge_prefix(w.log_end@, ends@, limit@, i@ + 1, false))]
    #[invariant(forall<j: Int> 0 <= j && j < supporters@.len()
        ==> supporters@[j].1@ >= limit@
            && exists<k: Int> 0 <= k && k < i@
                && supporters@[j].0 == voters@[k] && supporters@[j].1 == ends@[k])]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < supporters@.len()
        ==> supporters@[j].0 != supporters@[k].0)]
    #[variant(voters@.len() - i@)]
    while i < voters.len() {
        if ends[i] >= limit {
            supporters.push((voters[i], ends[i]));
        }
        i += 1;
    }
    Some((hw, limit, supporters))
}

type WalCopyBatch = (i64, i32, Vec<u8>);

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= count && count <= batches.len())]
#[ensures(result >= 0)]
#[variant(count)]
fn wal_copy_byte_count(batches: Seq<WalCopyBatch>, count: Int) -> Int {
    pearlite! {
        if count == 0 { 0 } else {
            wal_copy_byte_count(batches, count - 1) + batches[count - 1].2@.len()
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= lower && lower <= upper && upper <= batches.len())]
#[ensures(wal_copy_byte_count(batches, lower) <= wal_copy_byte_count(batches, upper))]
#[variant(upper)]
fn lemma_wal_copy_byte_count_monotone(batches: Seq<WalCopyBatch>, lower: Int, upper: Int) {
    if lower < upper {
        lemma_wal_copy_byte_count_monotone(batches, lower, upper - 1);
    }
}

/// Checking actual batch bytes, appending, acknowledging, and replaying any
/// length of copied prefix agree on both logical and byte extents. Metadata
/// must faithfully decode the observed bytes; reads, copying, and fsync are
/// host obligations. Admission completeness excludes an always-reject proof.
#[ensures((match result { None => false, Some(_) => true }) == (
    start@ >= 0 && source@.len() == stored@.len() && position@ <= file_end@
    && position@ + wal_copy_byte_count(source@, source@.len()) <= file_end@
    && (forall<i: Int> 0 <= i && i < source@.len() ==>
        source@[i].0@ >= 0 && source@[i].1@ >= 0
        && source@[i].0@ + source@[i].1@ + 1 <= i64::MAX@
        && source@[i].0@ == (if i == 0 { start@ }
            else { source@[i - 1].0@ + source@[i - 1].1@ + 1 })
        && source@[i].2@.len() > 0
        && source@[i].0 == stored@[i].0 && source@[i].1 == stored@[i].1
        && source@[i].2@ == stored@[i].2@)
    && target@ == (if source@.len() == 0 { start@ }
        else { source@[source@.len() - 1].0@ + source@[source@.len() - 1].1@ + 1 })
))]
#[ensures(match result {
    None => true,
    Some((end, bytes_end)) => end == target && start@ <= end@
        && (source@.len() > 0 ==> start@ < end@ && position@ < bytes_end@)
        && bytes_end@ == position@ + wal_copy_byte_count(source@, source@.len())
        && position@ <= bytes_end@ && bytes_end@ <= file_end@,
})]
#[ensures(match result {
    None => true,
    Some(_) => source@.len() == stored@.len()
        && forall<i: Int> 0 <= i && i < source@.len()
            ==> source@[i].0 == stored@[i].0 && source@[i].1 == stored@[i].1
                && source@[i].2@ == stored@[i].2@,
})]
fn checked_wal_copy_replays_exactly(
    source: &[WalCopyBatch],
    stored: &[WalCopyBatch],
    start: i64,
    target: i64,
    position: u64,
    file_end: u64,
) -> Option<(i64, u64)> {
    if source.len() != stored.len() || start < 0 || position > file_end {
        return None;
    }
    let mut cursor = start;
    let mut byte_cursor = position;
    let mut i = 0usize;
    #[invariant(i@ <= source@.len())]
    #[invariant(cursor@ >= 0 && start@ <= cursor@)]
    #[invariant(i@ > 0 ==> start@ < cursor@ && position@ < byte_cursor@)]
    #[invariant(cursor@ == if i@ == 0 { start@ }
        else { source@[i@ - 1].0@ + source@[i@ - 1].1@ + 1 })]
    #[invariant(byte_cursor@ == position@ + wal_copy_byte_count(source@, i@))]
    #[invariant(position@ <= byte_cursor@ && byte_cursor@ <= file_end@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        source@[j].0@ >= 0 && source@[j].1@ >= 0
        && source@[j].0@ + source@[j].1@ + 1 <= i64::MAX@
        && source@[j].0@ == (if j == 0 { start@ }
            else { source@[j - 1].0@ + source@[j - 1].1@ + 1 })
        && source@[j].2@.len() > 0
        && source@[j].0 == stored@[j].0 && source@[j].1 == stored@[j].1
        && source@[j].2@ == stored@[j].2@)]
    #[variant(source@.len() - i@)]
    while i < source.len() {
        #[cfg(creusot)]
        proof_assert!({
            lemma_wal_copy_byte_count_monotone(source@, i@ + 1, source@.len());
            wal_copy_byte_count(source@, i@ + 1) <= wal_copy_byte_count(source@, source@.len())
        });
        // General replication admits compaction holes; a WAL copy requires
        // the exact contiguous prefix checked by the production raw reader.
        if source[i].0 != cursor {
            return None;
        }
        let (last, next) = local_append_coordinates(cursor, source[i].0, source[i].1)?;
        let (stored_last, _) = local_append_coordinates(cursor, stored[i].0, stored[i].1)?;
        if !wal_batch_equal(
            (source[i].0, last, &source[i].2),
            (stored[i].0, stored_last, &stored[i].2),
        ) {
            return None;
        }
        let acknowledgement = produce_durability_frontier(source[i].0, source[i].1)?;
        let step = local_recovery_batch_step(
            byte_cursor,
            file_end,
            cursor,
            stored[i].0,
            stored[i].1,
            stored[i].2.len() as u64,
        )?;
        if step.next_offset != next || acknowledgement != next {
            return None;
        }
        cursor = step.next_offset;
        byte_cursor = step.valid_end;
        i += 1;
    }
    if cursor != target {
        return None;
    }
    Some((cursor, byte_cursor))
}

/// Covering-read validation, actual-byte copy, append/ack/recovery geometry,
/// and logical Fetch admission compose even when retention cuts inside a batch.
/// Every visible offset has a concrete byte-identical copied-batch witness;
/// offsets before either retained floor have none. Metadata and host I/O must
/// faithfully represent the compared bytes; this does not prove crash publication.
#[ensures((match result { None => false, Some(_) => true }) == (
    0 <= floors.0@ && 0 <= floors.1@ && floors.0@ <= target@ && floors.1@ <= target@
    && source@.len() == stored@.len() && position@ <= file_end@
    && position@ + wal_copy_byte_count(source@, source@.len()) <= file_end@
    && (if source@.len() == 0 { floors.0@.max(floors.1@) == target@ } else {
        floors.0@.max(floors.1@) < target@ && source@[0].0@ <= floors.0@.max(floors.1@)
        && floors.0@.max(floors.1@) < source@[0].0@ + source@[0].1@ + 1
        && target@ == source@[source@.len() - 1].0@ + source@[source@.len() - 1].1@ + 1
    })
    && (forall<i: Int> 0 <= i && i < source@.len() ==>
        source@[i].0@ >= 0 && source@[i].1@ >= 0
        && source@[i].0@ + source@[i].1@ + 1 <= i64::MAX@
        && (i > 0 ==> source@[i].0@ == source@[i - 1].0@ + source@[i - 1].1@ + 1)
        && source@[i].2@.len() > 0
        && source@[i].0 == stored@[i].0 && source@[i].1 == stored@[i].1
        && source@[i].2@ == stored@[i].2@)
))]
#[ensures(match result {
    None => true,
    Some((physical, bytes_end, selected)) =>
        physical@ == (if source@.len() == 0 { floors.0@.max(floors.1@) } else { source@[0].0@ })
        && 0 <= physical@ && physical@ <= floors.0@.max(floors.1@)
        && bytes_end@ == position@ + wal_copy_byte_count(source@, source@.len())
        && bytes_end@ <= file_end@
        && (match selected { None => false, Some(_) => true }) ==
            (floors.0@ <= requested@ && floors.1@ <= requested@ && requested@ < target@)
        && (match selected {
            None => true,
            Some(i) => i@ < source@.len() && i@ < stored@.len()
                && source@[i@].0@ <= requested@
                && requested@ < source@[i@].0@ + source@[i@].1@ + 1
                && source@[i@].0 == stored@[i@].0 && source@[i@].1 == stored@[i@].1
                && source@[i@].2@ == stored@[i@].2@,
        }),
})]
fn covering_copy_preserves_logical_fetch(
    source: &[WalCopyBatch],
    stored: &[WalCopyBatch],
    floors: (i64, i64),
    target: i64,
    position: u64,
    file_end: u64,
    requested: i64,
) -> Option<(i64, u64, Option<usize>)> {
    let floor = match delete_records_trim_application(0, floors.0, floors.1) {
        DeleteRecordsTrimApplication::RejectMalformed => return None,
        DeleteRecordsTrimApplication::TrimWal { frontier }
        | DeleteRecordsTrimApplication::TrimLocal { frontier }
        | DeleteRecordsTrimApplication::Complete { frontier } => frontier,
    };
    if target < floor {
        return None;
    }
    let mut bases: Vec<i64> = Vec::new();
    let mut lasts: Vec<i64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= source@.len() && bases@.len() == i@ && lasts@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        bases@[j] == source@[j].0 && lasts@[j]@ == source@[j].0@ + source@[j].1@
        && source@[j].0@ >= 0 && source@[j].1@ >= 0
        && source@[j].0@ + source@[j].1@ + 1 <= i64::MAX@)]
    #[variant(source@.len() - i@)]
    while i < source.len() {
        let (last, _) = local_append_coordinates(source[i].0, source[i].0, source[i].1)?;
        bases.push(source[i].0);
        lasts.push(last);
        i += 1;
    }
    let physical = wal_covering_batch_range(&bases, &lasts, floor, target)?;
    let (end, bytes_end) =
        checked_wal_copy_replays_exactly(source, stored, physical, target, position, file_end)?;
    let visibility = fetch_visibility(
        true,
        false,
        FetchWatermarks {
            log_start: floor,
            log_end: end,
            hw: end,
            lso: end,
            deliverable: end,
        },
        requested,
    );
    if visibility.out_of_range || visibility.empty {
        return Some((physical, bytes_end, None));
    }
    let mut j = 0usize;
    #[invariant(j@ <= source@.len())]
    #[invariant(requested@ >= if j@ == 0 { physical@ }
        else { source@[j@ - 1].0@ + source@[j@ - 1].1@ + 1 })]
    #[variant(source@.len() - j@)]
    while j < source.len() {
        let next = produce_durability_frontier(source[j].0, source[j].1)?;
        if in_half_open_window(requested, source[j].0, next) {
            return Some((physical, bytes_end, Some(j)));
        }
        j += 1;
    }
    Some((physical, bytes_end, None))
}

/// Recovery admission is complete for whole-batch ends, permits interior
/// logical floors, and discards exactly the uncertain suffix. Empty checkpoints
/// reset at their floor. Clamping visibility to the actual retained end then
/// prevents Fetch from exposing that suffix. `ends` must be the complete,
/// accurately decoded local batch sequence; publication/fsync remain host effects.
#[requires(0 <= physical_start@ && physical_start@ <= w.log_start@)]
#[requires(forall<i: Int> 0 <= i && i < ends@.len() ==> physical_start@ < ends@[i]@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < ends@.len() ==> ends@[i]@ < ends@[j]@)]
#[requires(w.log_start@ <= w.log_end@)]
#[requires(w.log_end@ == if ends@.len() == 0 { physical_start@ }
    else { ends@[ends@.len() - 1]@ })]
#[ensures((match result { None => false, Some(_) => true }) == (
    w.log_start@ <= start@ && start@ <= cut@
    && cut@ <= (if ends@.len() == 0 { physical_start@ } else { ends@[ends@.len() - 1]@ })
    && (start == cut || exists<i: Int> 0 <= i && i < ends@.len() && ends@[i] == cut)
))]
#[ensures(match result {
    None => true,
    Some((kept, limit)) => kept@ <= ends@.len()
        && (start == cut ==> kept@ == 0)
        && (start != cut ==> kept@ > 0 && ends@[kept@ - 1] == cut
            && forall<i: Int> 0 <= i && i < ends@.len() ==>
                (i < kept@) == (ends@[i]@ <= cut@))
        && limit@ == w.hw@.min(cut@).min(w.lso@.min(cut@)).min(w.deliverable@.min(cut@))
        && limit@ <= cut@ && limit@ <= w.hw@ && limit@ <= w.lso@ && limit@ <= w.deliverable@,
})]
fn checkpoint_truncation_bounds_fetch(
    ends: &[i64],
    physical_start: i64,
    w: FetchWatermarks,
    start: i64,
    cut: i64,
) -> Option<(usize, i64)> {
    let recovered_end = w.log_end;
    if w.log_start > start || start > cut || cut > recovered_end {
        return None;
    }
    let mut kept = 0usize;
    let mut actual_end = physical_start;
    let mut observed_last: Option<i64> = None;
    let mut i = 0usize;
    #[invariant(i@ <= ends@.len() && kept@ <= i@)]
    #[invariant(0 <= actual_end@)]
    #[invariant(actual_end@ == if kept@ == 0 { physical_start@ } else { ends@[kept@ - 1]@ })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> (j < kept@) == (ends@[j]@ <= cut@))]
    #[invariant((match observed_last { Some(last) => last@ == cut@ - 1, None => false }) ==
        (exists<j: Int> 0 <= j && j < i@ && ends@[j] == cut))]
    #[invariant((observed_last == None) ==
        (forall<j: Int> 0 <= j && j < i@ ==> ends@[j]@ < cut@))]
    #[invariant(observed_last != None ==> exists<j: Int> 0 <= j && j < i@
        && (match observed_last { Some(last) => last@ == ends@[j]@ - 1, None => false }) && cut@ <= ends@[j]@)]
    #[variant(ends@.len() - i@)]
    while i < ends.len() {
        let last = ends[i] - 1;
        if observed_last.is_none() && cut <= ends[i] {
            observed_last = Some(last);
        }
        if truncation_batch_retained(last, cut) {
            actual_end = ends[i];
            kept += 1;
        }
        i += 1;
    }
    if !wal_checkpoint_range_valid(w.log_start, recovered_end, start, cut, observed_last) {
        return None;
    }
    if start == cut {
        kept = 0;
        actual_end = start;
    }
    proof_assert!(actual_end == cut);
    let w = FetchWatermarks {
        log_start: start,
        log_end: actual_end,
        hw: truncation_frontier(w.hw, actual_end),
        lso: truncation_frontier(w.lso, actual_end),
        deliverable: truncation_frontier(w.deliverable, actual_end),
    };
    Some((kept, fetch_visibility(false, true, w, start).limit_offset))
}

/// The actual per-row archive validator establishes both binary-search
/// ordering and byte bounds. The floor cannot lie after a present ceiling.
#[ensures(result)]
fn validated_index_bounds_lookup(
    entries: &[(u32, u32)],
    target: u32,
    max_relative: i64,
    log_bytes: u64,
) -> bool {
    let mut i = 0usize;
    let mut previous = None;
    #[invariant(i@ <= entries@.len())]
    #[invariant(previous == if i@ == 0 { None } else { Some(entries@[i@ - 1]) })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].1@ < log_bytes@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].0@ < entries@[k].0@ && entries@[j].1@ < entries@[k].1@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let (relative, position) = entries[i];
        if !restore_offset_index_entry_valid(previous, relative, position, max_relative, log_bytes)
        {
            // An invalid archive never reaches lookup.
            return true;
        }
        previous = Some((relative, position));
        i += 1;
    }
    let floor = offset_index_lookup(entries, target);
    if !matches!(entries.len(), 0) && u64::from(floor) >= log_bytes {
        return false;
    }
    match offset_index_position_at_or_after(entries, target) {
        Some(ceiling) => floor <= ceiling && u64::from(ceiling) < log_bytes,
        None => true,
    }
}

/// Replaying a durable loss marker twice cannot subtract pending losses twice.
/// Generation exhaustion is a real ceiling of the saturating host protocol.
#[requires(state.generation@ < u64::MAX@)]
#[ensures(result)]
fn loss_settlement_is_idempotent(state: AuditLosses, marker: AuditLosses) -> bool {
    let settled = settle_loss_batch(state, marker);
    let replayed = settle_loss_batch(settled, marker);
    settled.count == replayed.count && settled.generation == replayed.generation
}

/// Restored time-index cursors stay inside the segment and never move backwards
/// when the target increases, even when timestamps repeat.
#[requires(lower_target@ <= upper_target@)]
#[ensures(result)]
fn validated_time_cursors_are_monotone(
    entries: &[(i64, u32)],
    segment_base: i64,
    segment_end: i64,
    lower_target: i64,
    upper_target: i64,
) -> bool {
    let Some(max_relative) = restore_index_frontier(segment_base, segment_end) else {
        return true;
    };
    let mut i = 0usize;
    let mut previous = None;
    #[invariant(i@ <= entries@.len())]
    #[invariant(previous == if i@ == 0 { None } else { Some(entries@[i@ - 1]) })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].1@ <= max_relative@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].0@ <= entries@[k].0@ && entries@[j].1@ < entries@[k].1@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let (timestamp, relative) = entries[i];
        if !restore_time_index_entry_valid(previous, timestamp, relative, max_relative) {
            return true;
        }
        previous = Some((timestamp, relative));
        i += 1;
    }
    let lower = time_index_lookup(entries, lower_target);
    let upper = time_index_lookup(entries, upper_target);
    match (
        segment_base.checked_add(i64::from(lower)),
        segment_base.checked_add(i64::from(upper)),
    ) {
        (Some(lower_cursor), Some(upper_cursor)) => {
            segment_base <= lower_cursor
                && lower_cursor <= upper_cursor
                && upper_cursor <= segment_end
        }
        _ => false,
    }
}

/// Restored epoch rows establish the reconciliation lookup's global ordering.
/// A resolved cut cannot grow the log, and clamped consumer frontiers cannot
/// expose its discarded tail. The host must apply the cut durably.
#[ensures(result)]
fn validated_epochs_bound_truncated_fetch(
    entries: &[EpochEntry],
    requested: i32,
    segment_base: i64,
    w: FetchWatermarks,
) -> bool {
    let mut i = 0usize;
    let mut previous = None;
    #[invariant(i@ <= entries@.len())]
    #[invariant(i@ > 0 ==> segment_base@ >= 0)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].epoch.0@ >= 0)]
    #[invariant(previous == if i@ == 0 { None } else {
        Some((entries@[i@ - 1].epoch.0, entries@[i@ - 1].start_offset.0))
    })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        segment_base@ <= entries@[j].start_offset.0@
            && entries@[j].start_offset.0@ <= w.log_end@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@ ==>
        entries@[j].epoch.0@ < entries@[k].epoch.0@
            && entries@[j].start_offset.0@ < entries@[k].start_offset.0@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let epoch = entries[i].epoch.0;
        let start = entries[i].start_offset.0;
        if !restore_leader_epoch_entry_valid(previous, epoch, start, segment_base, w.log_end) {
            return true;
        }
        previous = Some((epoch, start));
        i += 1;
    }
    let (found, end) =
        epoch_and_offset_for_entries(entries, LeaderEpoch(requested), Offset(w.log_end));
    if end.0 == -1 {
        return found.0 == -1;
    }
    proof_assert!(segment_base@ <= end.0@ && end.0@ <= w.log_end@);
    proof_assert!(found.0@ <= requested@);
    let bounded = FetchWatermarks {
        log_end: end.0,
        hw: truncation_frontier(w.hw, end.0),
        lso: truncation_frontier(w.lso, end.0),
        deliverable: truncation_frontier(w.deliverable, end.0),
        ..w
    };
    let visibility = fetch_visibility(false, true, bounded, w.log_start);
    segment_base <= end.0
        && end.0 <= w.log_end
        && found.0 <= requested
        && visibility.limit_offset <= end.0
        && visibility.response_hw <= end.0
        && visibility.response_lso <= end.0
}

/// After truncation, selecting a surviving snapshot always yields a valid
/// replay cursor inside the shortened log; a snapshot in the discarded tail
/// cannot suppress replay. Snapshot contents and persistence remain external.
#[requires(0 <= range.log_start@ && range.log_start@ <= cut@ && cut@ <= range.log_end@)]
#[requires(0 <= range.local_start@ && range.local_start@ <= cut@)]
#[ensures(result)]
fn truncated_snapshot_selection_bounds_replay(
    offsets: &[i64],
    range: ProducerReloadRange,
    cut: i64,
) -> bool {
    let shortened = ProducerReloadRange {
        log_end: truncation_frontier(range.log_end, cut),
        ..range
    };
    let selected = producer_snapshot_latest_index(offsets, shortened);
    let snapshot = match selected {
        Some(index) => {
            if !producer_snapshot_reload_keeps(offsets[index], shortened) || offsets[index] > cut {
                return false;
            }
            Some(offsets[index])
        }
        None => None,
    };
    match producer_snapshot_replay_start(shortened, snapshot) {
        Some(cursor) => {
            range.log_start <= cursor
                && range.local_start <= cursor
                && cursor <= cut
                && match snapshot {
                    Some(offset) => offset <= cursor,
                    None => true,
                }
        }
        None => false,
    }
}

/// Every admitted archive transaction is a usable inclusive abort interval.
/// Narrowing the consumer Fetch limit cannot introduce a new abort entry;
/// selected entries start before the visible limit and replicated frontier.
#[ensures(result)]
fn restored_aborts_remain_bounded_when_fetch_shrinks(
    entries: &[RestoreAbortedTxn],
    segment: RestoreSegmentExtent,
    w: FetchWatermarks,
    fetch_start: i64,
    cut: i64,
) -> bool {
    let visibility = fetch_visibility(false, true, w, fetch_start);
    let narrowed = truncation_frontier(visibility.limit_offset, cut);
    let mut i = 0usize;
    let mut previous_last = None;
    #[invariant(i@ <= entries@.len())]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let entry = entries[i];
        if !restore_txn_index_entry_valid(previous_last, entry, segment) {
            return true;
        }
        if aborted_transaction_interval(
            Some(entry.start_offset),
            entry.last_offset,
            entry.producer_id,
        ) != Some((entry.start_offset, entry.last_offset))
        {
            return false;
        }
        if aborted_transaction_overlaps(
            entry.start_offset,
            entry.last_offset,
            fetch_start,
            narrowed,
        ) && (!aborted_transaction_overlaps(
            entry.start_offset,
            entry.last_offset,
            fetch_start,
            visibility.limit_offset,
        ) || entry.start_offset >= w.hw
            || entry.start_offset >= w.lso
            || entry.start_offset >= w.deliverable
            || entry.last_offset < segment.base_offset
            || entry.last_offset > segment.last_offset)
        {
            return false;
        }
        previous_last = Some(entry.last_offset);
        i += 1;
    }
    true
}

/// The maximum over actual batch activation times is due iff every batch is
/// due. This justifies the whole-segment activation shortcut, including empty
/// segments, signed timestamp extremes, and deadline overflow.
#[ensures(result == (forall<i: Int> 0 <= i && i < activations@.len() ==>
    uncertainty@ >= 0 && activations@[i]@ + uncertainty@ <= i64::MAX@
        && activations@[i]@ + uncertainty@ <= now@))]
fn segment_maximum_proves_delivery(activations: &[i64], uncertainty: i64, now: i64) -> bool {
    match earliest_max_timestamp_index(activations) {
        Some(index) => scheduled_delivery_visible(true, uncertainty, activations[index], now),
        None => true,
    }
}

/// A complete batch walk derives a delivery frontier that hides every waiting
/// batch from consumers while leaving follower replication ungated. Compacted
/// offset gaps are allowed; the window begins on a batch boundary. This
/// recomputes from the start, not a cached cursor.
#[requires(batches@.len() == activations@.len())]
#[requires(0 <= w.log_start@ && w.log_start@ <= w.log_end@)]
#[ensures(result)]
fn scheduled_prefix_bounds_fetch(
    batches: &[(i64, i32)],
    activations: &[i64],
    uncertainty: i64,
    now: i64,
    w: FetchWatermarks,
) -> bool {
    let all_due = segment_maximum_proves_delivery(activations, uncertainty, now);
    let mut cursor = w.log_start;
    let mut candidate = w.log_end;
    let mut i = 0usize;
    #[invariant(i@ <= batches@.len())]
    #[invariant(w.log_start@ <= cursor@ && cursor@ <= w.log_end@)]
    #[invariant(w.log_start@ <= candidate@ && candidate@ <= w.log_end@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ && !(uncertainty@ >= 0
        && activations@[j]@ + uncertainty@ <= i64::MAX@
        && activations@[j]@ + uncertainty@ <= now@) ==> candidate@ <= batches@[j].0@)]
    #[variant(batches@.len() - i@)]
    while i < batches.len() {
        let (base, delta) = batches[i];
        let Some(next) = restore_batch_step(cursor, base, delta) else {
            return true;
        };
        if next > w.log_end {
            return true;
        }
        if !all_due
            && !scheduled_delivery_visible(true, uncertainty, activations[i], now)
            && base < candidate
        {
            candidate = base;
        }
        cursor = next;
        i += 1;
    }
    if cursor != w.log_end {
        return true;
    }
    let deliverable = delivery_watermark_advance(w.log_start, w.log_start, candidate, w.log_end);
    let bounded = FetchWatermarks { deliverable, ..w };
    let consumer = fetch_visibility(false, true, bounded, w.log_start);
    let follower = fetch_visibility(true, true, bounded, w.log_start);
    if deliverable != candidate
        || consumer.limit_offset > candidate
        || follower.limit_offset != w.log_end
    {
        return false;
    }
    i = 0;
    #[invariant(i@ <= batches@.len())]
    #[variant(batches@.len() - i@)]
    while i < batches.len() {
        if !scheduled_delivery_visible(true, uncertainty, activations[i], now)
            && consumer.limit_offset > batches[i].0
        {
            return false;
        }
        i += 1;
    }
    true
}

/// A fenced/error replica response leaves the log end and HWM unchanged.
/// Divergence can only shrink them; admitted append coordinates plus monotone
/// HWM advancement keep consumer Fetch inside the new local log.
#[requires(w.hw@ <= w.log_end@)]
#[ensures(result.1@ <= result.0@ && result.2@ <= result.1@)]
#[ensures(if crate::broker::replica_fetch_fenced(facts) || facts.error_code@ != 0 {
    result.0 == w.log_end && result.1 == w.hw
} else { true })]
#[ensures(facts.diverging_epoch@ >= 0
    ==> result.0@ <= w.log_end@ && result.1@ <= w.hw@)]
#[ensures(result.0@ > w.log_end@ ==> !crate::broker::replica_fetch_fenced(facts)
    && facts.error_code@ == 0 && facts.diverging_epoch@ < 0
    && result.0@ == supplied_base@ + delta@ + 1)]
fn fenced_replication_bounds_fetch(
    facts: ReplicaFetchFacts,
    diverging_end_offset: i64,
    supplied_base: i64,
    delta: i32,
    reported_hw: i64,
    w: FetchWatermarks,
) -> (i64, i64, i64) {
    let (end, hw) = match replica_fetch_mutation(facts) {
        ReplicaFetchMutation::Truncate => {
            let end = truncation_frontier(w.log_end, diverging_end_offset);
            (end, truncation_frontier(w.hw, end))
        }
        ReplicaFetchMutation::Append => {
            match local_append_coordinates(w.log_end, supplied_base, delta) {
                Some((_, next)) => (next, advance_high_watermark(w.hw, reported_hw, next)),
                None => (w.log_end, w.hw),
            }
        }
        ReplicaFetchMutation::Reject | ReplicaFetchMutation::Retry => (w.log_end, w.hw),
    };
    let visibility = fetch_visibility(
        false,
        true,
        FetchWatermarks {
            log_end: end,
            hw,
            ..w
        },
        w.log_start,
    );
    (end, hw, visibility.limit_offset)
}

/// Place and select every decoded record, then classify the entire batch.
/// Kept records lie in the allocated half-open span and respect inclusive
/// offset/exclusive timestamp bounds; classification agrees with the count.
#[ensures(result)]
fn restore_selection_respects_batch_extent(
    frame: RestoreBatchFrame,
    records: &[(RestoreRecordDeltas, RestoreExclusions)],
    offset_bound: Option<i64>,
    timestamp_bound: Option<i64>,
) -> bool {
    let Some((_, next)) = local_append_coordinates(
        frame.base_offset,
        frame.base_offset,
        frame.last_offset_delta,
    ) else {
        return true;
    };
    let mut i = 0usize;
    let mut kept = 0usize;
    #[invariant(kept@ <= i@ && i@ <= records@.len())]
    #[variant(records@.len() - i@)]
    while i < records.len() {
        let (record, exclusions) = records[i];
        let Some((offset, timestamp)) = restore_record_coordinates(frame, record) else {
            return true;
        };
        if restore_record_selected(offset, offset_bound, timestamp, timestamp_bound, exclusions) {
            if !in_half_open_window(offset, frame.base_offset, next)
                || restore_batch_past_offset_bound(offset, offset_bound)
                || match timestamp_bound {
                    Some(bound) => timestamp >= bound,
                    None => false,
                }
            {
                return false;
            }
            kept += 1;
        }
        i += 1;
    }
    match restore_batch_filter_decision(kept > 0, kept < records.len()) {
        RestoreFilterDecision::Keep => kept == records.len(),
        RestoreFilterDecision::Empty => kept == 0 && !matches!(records.len(), 0),
        RestoreFilterDecision::Filter => kept > 0 && kept < records.len(),
    }
}

/// Full production placement establishes the installer's identity invariant;
/// incomplete/zero placements fail closed. No supplied uniqueness boolean is used.
#[ensures(result)]
fn constructed_wal_placement_is_installable(
    candidates: &[(u64, u64)],
    local_node: u64,
    requested: usize,
) -> bool {
    let selected = select_wal_voters(candidates, local_node, requested);
    let mut nodes: Vec<u64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= selected@.len() && nodes@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> nodes@[j] == selected@[j].0)]
    #[variant(selected@.len() - i@)]
    while i < selected.len() {
        nodes.push(selected[i].0);
        i += 1;
    }
    wal_voter_set_valid(&nodes, local_node, requested)
        == (requested > 0 && selected.len() == requested)
}

/// A complete placement of at least three voters leaves the original majority
/// satisfiable after any one configured rack disappears. This connects the
/// actual placement, not a round-robin model, to quorum arithmetic. Remaining
/// brokers must still communicate and fsync; rack labels must name real domains.
#[requires(requested@ >= 3)]
#[ensures(result)]
fn wal_placement_survives_one_rack_loss(
    candidates: &[(u64, u64)],
    local_node: u64,
    requested: usize,
    failed_rack: u64,
) -> bool {
    let selected = select_wal_voters(candidates, local_node, requested);
    if selected.len() != requested {
        // An incomplete placement cannot be installed by the other theorem.
        return true;
    }
    let mut surviving = 0usize;
    let mut removed: Option<usize> = None;
    let mut i = 0usize;
    #[invariant(i@ <= selected@.len())]
    #[invariant(surviving@ + (if removed == None { 0 } else { 1 }) == i@)]
    #[invariant(match removed {
        Some(index) => index@ < i@ && selected@[index@].1 == failed_rack,
        None => true,
    })]
    #[variant(selected@.len() - i@)]
    while i < selected.len() {
        if selected[i].1 == failed_rack {
            if removed.is_some() {
                return false;
            }
            removed = Some(i);
        } else {
            surviving += 1;
        }
        i += 1;
    }
    election_has_quorum(selected.len(), surviving)
}

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
fn trim_steps_converge(
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
fn admitted_trim_bounds_reload_and_retry(
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
fn diskless_trim_reconciliation_preserves_coverage(
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

/// Construct a sparse row from the actual prefix maximum. The indexed record
/// can be a batch base while `through` includes the batch's remaining records.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(indexed@ <= through@ && through@ < timestamps@.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < offsets@.len()
    ==> offsets@[i]@ < offsets@[j]@)]
#[ensures(result.1 == offsets@[indexed@])]
#[ensures(exists<i: Int> 0 <= i && i <= through@ && timestamps@[i] == result.0)]
#[ensures(forall<i: Int> 0 <= i && i <= through@ ==> timestamps@[i]@ <= result.0@)]
#[ensures(forall<i: Int> 0 <= i && i < offsets@.len() && offsets@[i]@ <= result.1@
    ==> timestamps@[i]@ <= result.0@)]
fn running_maximum_index_entry(
    offsets: &[u32],
    timestamps: &[i64],
    indexed: usize,
    through: usize,
) -> (i64, u32) {
    let prefix = &timestamps[..=through];
    match earliest_max_timestamp_index(prefix) {
        Some(index) => (prefix[index], offsets[indexed]),
        None => unreachable!(),
    }
}

/// A strict-predecessor sparse start followed by the existing record selector
/// finds the global first match, even with nonmonotone timestamps and offset
/// gaps. Each sparse timestamp must bound all records before its offset.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < offsets@.len()
    ==> offsets@[i]@ < offsets@[j]@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ <= entries@[j].0@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < entries@.len()
    && 0 <= j && j < offsets@.len() && offsets@[j]@ < entries@[i].1@
    ==> timestamps@[j]@ <= entries@[i].0@)]
#[ensures(match result {
    Some(index) => index@ < timestamps@.len() && timestamps@[index@]@ >= target@
        && forall<i: Int> 0 <= i && i < index@ ==> timestamps@[i]@ < target@,
    None => forall<i: Int> 0 <= i && i < timestamps@.len() ==> timestamps@[i]@ < target@,
})]
fn indexed_timestamp_scan_finds_first(
    entries: &[(i64, u32)],
    offsets: &[u32],
    timestamps: &[i64],
    target: i64,
) -> Option<usize> {
    let relative = time_index_scan_start(entries, target);
    let mut start = 0usize;
    #[invariant(start@ <= offsets@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < start@ ==> timestamps@[i]@ < target@)]
    #[variant(offsets@.len() - start@)]
    while start < offsets.len() && offsets[start] < relative {
        proof_assert!(timestamps@[start@]@ < target@);
        start += 1;
    }
    let index = first_timestamp_index(&timestamps[start..], target)?;
    Some(start + index)
}

/// The remote prefix selector and floor adapter preserve the global first
/// record match. Unlike a binary search, this accepts unsorted index timestamps
/// and offset padding. Index rows must bound earlier record timestamps; a
/// zero-offset padding row has no earlier record to bound.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < offsets@.len()
    ==> offsets@[i]@ < offsets@[j]@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < entries@.len()
    && 0 <= j && j < offsets@.len() && offsets@[j]@ < entries@[i].1@
    ==> timestamps@[j]@ <= entries@[i].0@)]
#[ensures(result)]
fn remote_timestamp_scan_preserves_first(
    entries: &[(i64, u32)],
    offsets: &[u32],
    timestamps: &[i64],
    target: i64,
) -> bool {
    let count = remote_time_index_candidate_count(entries, target);
    let (relative, selected) = if count == 0 {
        (0, &entries[..0])
    } else {
        (entries[count - 1].1, &entries[count - 1..count])
    };
    // A selected remote row is strictly below the target, so the one-row
    // strict search starts at exactly the floor the remote adapter returns.
    relative == time_index_scan_start(selected, target)
        && indexed_timestamp_scan_finds_first(selected, offsets, timestamps, target)
            == first_timestamp_index(timestamps, target)
}

/// Archive row validation makes the remote prefix floor and local binary
/// scan start agree. Raw trailing padding must be excluded before validation;
/// the separate remote scan theorem admits its zero-offset rows directly.
#[ensures(result)]
fn validated_remote_and_local_time_starts_agree(
    entries: &[(i64, u32)],
    max_relative: i64,
    target: i64,
) -> bool {
    let mut i = 0usize;
    let mut previous = None;
    #[invariant(i@ <= entries@.len())]
    #[invariant(previous == if i@ == 0 { None } else { Some(entries@[i@ - 1]) })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].1@ <= max_relative@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].0@ <= entries@[k].0@ && entries@[j].1@ < entries@[k].1@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let (timestamp, relative) = entries[i];
        if !restore_time_index_entry_valid(previous, timestamp, relative, max_relative) {
            return true;
        }
        previous = Some((timestamp, relative));
        i += 1;
    }
    let count = remote_time_index_candidate_count(entries, target);
    let remote = if count == 0 { 0 } else { entries[count - 1].1 };
    remote == time_index_scan_start(entries, target)
}

/// Build an arbitrary sparse index from real prefix maxima, then use it to
/// search. No trusted upper-bound boolean or assumed timestamp ordering of
/// records is needed; the indexed answer agrees with a full record scan.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < offsets@.len()
    ==> offsets@[i]@ < offsets@[j]@)]
#[requires(forall<i: Int> 0 <= i && i < rows@.len()
    ==> rows@[i].0@ <= rows@[i].1@ && rows@[i].1@ < timestamps@.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < rows@.len()
    ==> rows@[i].0@ < rows@[j].0@ && rows@[i].1@ <= rows@[j].1@)]
#[ensures(result)]
fn constructed_time_index_preserves_first(
    offsets: &[u32],
    timestamps: &[i64],
    rows: &[(usize, usize)],
    target: i64,
) -> bool {
    let mut entries: Vec<(i64, u32)> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= rows@.len() && entries@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].1 == offsets@[rows@[j].0@])]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        exists<k: Int> 0 <= k && k <= rows@[j].1@ && timestamps@[k] == entries@[j].0)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < i@ && 0 <= k && k <= rows@[j].1@
        ==> timestamps@[k]@ <= entries@[j].0@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].0@ <= entries@[k].0@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < i@ && 0 <= k && k < offsets@.len()
        && offsets@[k]@ <= entries@[j].1@ ==> timestamps@[k]@ <= entries@[j].0@)]
    #[variant(rows@.len() - i@)]
    while i < rows.len() {
        let (indexed, through) = rows[i];
        entries.push(running_maximum_index_entry(
            offsets, timestamps, indexed, through,
        ));
        i += 1;
    }
    indexed_timestamp_scan_finds_first(&entries, offsets, timestamps, target)
        == first_timestamp_index(timestamps, target)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn marker_visibility_matches_wire_and_remaining_transaction_oracle(
            marker_type in prop_oneof![Just(0i16), Just(1), Just(1000), any::<i16>()],
            version in any::<i16>(),
            key_len in 0usize..=7,
            is_control in any::<bool>(),
            matches_pid in any::<bool>(),
            other_starts in proptest::collection::vec(0i64..=10, 0..16),
            hw in prop_oneof![Just(12i64), Just(13), any::<i64>()],
            deliverable in any::<i64>(),
        ) {
            let mut key = Vec::from(version.to_be_bytes());
            key.extend_from_slice(&marker_type.to_be_bytes());
            key.resize(key_len, 255);
            let decoded = key.get(2..4).map(|bytes| i16::from_be_bytes([bytes[0], bytes[1]]));
            let closes = is_control && matches_pid && matches!(decoded, Some(0 | 1));
            let released = closes && hw > 12;
            let expected_limit = other_starts.iter().copied()
                .chain((!released).then_some(2)).chain([13, hw, deliverable]).min().unwrap();
            let expected_abort = (closes && decoded == Some(0)).then_some((2, 12));
            assert!(control_marker_bounds_committed_fetch(&key, is_control,
                (7, if matches_pid { 7 } else { 8 }, 2), (10, 2), &other_starts,
                (hw, deliverable)) == (expected_limit, expected_abort));
        }
    }

    #[test]
    fn marker_release_is_strict_and_cannot_release_another_producer() {
        for marker_type in [0i16, 1, 1000, -1, 23] {
            let mut key = Vec::from(0i16.to_be_bytes());
            key.extend_from_slice(&marker_type.to_be_bytes());
            for hw in [12, 13] {
                let release = matches!(marker_type, 0 | 1) && hw == 13;
                let aborted = (marker_type == 0).then_some((2, 12));
                assert!(
                    control_marker_bounds_committed_fetch(
                        &key,
                        true,
                        (7, 7, 2),
                        (10, 2),
                        &[],
                        (hw, 13)
                    ) == (if release { 13 } else { 2 }, aborted)
                );
                assert!(
                    control_marker_bounds_committed_fetch(
                        &key,
                        true,
                        (7, 8, 2),
                        (10, 2),
                        &[],
                        (hw, 13)
                    ) == (2, None)
                );
                assert!(
                    control_marker_bounds_committed_fetch(
                        &key,
                        true,
                        (7, 7, 2),
                        (10, 2),
                        &[1],
                        (hw, 13)
                    ) == (1, aborted)
                );
            }
        }
        assert!(
            control_marker_bounds_committed_fetch(
                &[0, 0, 0, 1],
                true,
                (7, 7, i64::MAX - 2),
                (i64::MAX - 1, 0),
                &[],
                (i64::MAX, i64::MAX)
            ) == (i64::MAX, None)
        );
    }

    proptest! {
        #[test]
        fn sparse_timestamp_composition_matches_full_record_oracle(
            records in proptest::collection::btree_map(any::<u32>(), any::<i64>(), 0..32),
            target in any::<i64>(),
            step in 1usize..8,
            span in 0usize..5,
        ) {
            let offsets: Vec<_> = records.keys().copied().collect();
            let timestamps: Vec<_> = records.values().copied().collect();
            let rows: Vec<_> = (0..records.len()).step_by(step)
                .map(|indexed| (indexed, (indexed + span).min(records.len() - 1)))
                .collect();
            let entries: Vec<_> = rows.iter().map(|(indexed, through)|
                running_maximum_index_entry(&offsets, &timestamps, *indexed, *through)).collect();
            let expected = timestamps.iter().position(|timestamp| *timestamp >= target);
            assert!(indexed_timestamp_scan_finds_first(&entries, &offsets, &timestamps, target) == expected);
            assert!(constructed_time_index_preserves_first(&offsets, &timestamps, &rows, target));
            assert!(remote_timestamp_scan_preserves_first(&entries, &offsets, &timestamps, target));
            assert!(validated_remote_and_local_time_starts_agree(&entries, i64::from(u32::MAX), target));
        }
    }

    proptest! {
        #[test]
        fn trim_compositions_match_frontier_and_progress_oracles(
            requested in 0i64..=i64::MAX,
            wal in 0i64..=i64::MAX,
            local in 0i64..=i64::MAX,
            applied in proptest::collection::vec(any::<bool>(), 0..32),
            snapshots in proptest::collection::vec(any::<i64>(), 0..16),
        ) {
            let frontier = requested.max(wal).max(local);
            let completed = applied.iter().filter(|applied| **applied).count();
            let expected = (
                if completed > 0 { frontier } else { wal },
                if completed > 1 || (completed > 0 && wal == frontier) { frontier } else { local },
            );
            assert!(trim_steps_converge(requested, wal, local, &applied) == expected);
            let facts = DeleteRecordsTrimFacts {
                requested,
                current_start: local,
                high_watermark: frontier,
                log_end: frontier,
                has_delivery_watermark: true,
                delivery_watermark: wal.max(local),
            };
            assert!(admitted_trim_bounds_reload_and_retry(facts, wal, local, &snapshots));
            assert!(diskless_trim_reconciliation_preserves_coverage(frontier, frontier, 0, wal, local, &applied));
        }
    }

    proptest! {
        #[test]
        fn checked_wal_copy_matches_independent_extent_and_byte_oracles(
            frames in proptest::collection::vec((0i32..8, proptest::collection::vec(any::<u8>(), 1..16)), 0..12),
            start in 0i64..64,
            position in any::<u64>(),
            capacity in 0u64..128,
            mutation in 0usize..5,
            matching_target in any::<bool>(),
            other_target in 0i64..128,
        ) {
            let mut cursor = start;
            let source: Vec<WalCopyBatch> = frames.into_iter().map(|(delta, bytes)| {
                let base = cursor;
                cursor += i64::from(delta) + 1;
                (base, delta, bytes)
            }).collect();
            let mut stored = source.clone();
            if let Some(first) = stored.first_mut() {
                match mutation {
                    1 => first.2[0] ^= 1,
                    2 => first.0 += 1,
                    3 => first.1 += 1,
                    _ => {},
                }
            }
            if mutation == 4 { stored.pop(); }
            let target = if matching_target { cursor } else { other_target };
            let file_end = position.saturating_add(capacity);
            let bytes: u128 = source.iter().map(|(_, _, bytes)| bytes.len() as u128).sum();
            let admitted = source == stored && target == cursor
                && u128::from(position) + bytes <= u128::from(file_end);
            let expected = admitted.then(|| (target, u64::try_from(u128::from(position) + bytes).unwrap()));
            assert!(checked_wal_copy_replays_exactly(&source, &stored, start, target, position, file_end) == expected);
        }
    }

    proptest! {
        #[test]
        fn covering_copy_matches_independent_bytes_extents_and_visible_offset_oracles(
            widths in prop::collection::vec(1i32..7, 0..16),
            physical in 0i64..20,
            floor_seed in any::<u16>(), prior_seed in any::<u16>(), request_seed in any::<u16>(),
            position in any::<u64>(), shortfall in 0u64..3, divergent in any::<bool>(),
        ) {
            let mut target = physical;
            let source: Vec<WalCopyBatch> = widths.iter().enumerate().map(|(i, width)| {
                let base = target;
                target += i64::from(*width);
                (base, width - 1, std::vec![u8::try_from(i).unwrap(), 42])
            }).collect();
            let start = physical + i64::from(floor_seed) % i64::from(widths.first().copied().unwrap_or(1));
            let prior = physical + i64::from(prior_seed) % (target - physical + 2);
            let source: Vec<_> = source.into_iter().filter(|batch|
                batch.0 + i64::from(batch.1) + 1 > start.max(prior)).collect();
            let mut stored = source.clone();
            if divergent && !stored.is_empty() { stored[0].2[0] ^= 1; }
            let copied_base = source.first().map_or(start.max(prior), |batch| batch.0);
            let requested = physical - 1 + i64::from(request_seed) % (target - physical + 3);
            let total = u64::try_from(source.len()).unwrap() * 2;
            let file_end = position.saturating_add(total).saturating_sub(shortfall);
            let expected = if prior <= target && source == stored
                && position.checked_add(total).is_some_and(|end| end <= file_end) {
                let selected = (requested >= start.max(prior) && requested < target)
                    .then(|| source.iter().position(|batch| batch.0 <= requested
                        && requested < batch.0 + i64::from(batch.1) + 1).unwrap());
                Some((copied_base, position + total, selected))
            } else { None };
            assert!(covering_copy_preserves_logical_fetch(&source, &stored, (start, prior), target,
                position, file_end, requested) == expected);
        }
    }

    #[test]
    fn covering_copy_hides_trimmed_records_without_losing_whole_batch_bytes() {
        let source = [(0, 2, std::vec![1, 2]), (3, 2, std::vec![3, 4])];
        for requested in -1..=7 {
            let selected = match requested {
                2 => Some(0),
                3..=5 => Some(1),
                _ => None,
            };
            assert!(
                covering_copy_preserves_logical_fetch(
                    &source,
                    &source,
                    (1, 2),
                    6,
                    10,
                    14,
                    requested
                ) == Some((0, 14, selected))
            );
        }
        assert!(
            covering_copy_preserves_logical_fetch(&source, &source, (1, 2), 5, 10, 14, 2).is_none()
        );
        assert!(
            covering_copy_preserves_logical_fetch(&source[1..], &source[1..], (1, 4), 6, 10, 12, 4)
                == Some((3, 12, Some(0)))
        );
        assert!(
            covering_copy_preserves_logical_fetch(&source, &source, (3, 2), 6, 10, 14, 3).is_none()
        );
        assert!(
            covering_copy_preserves_logical_fetch(
                &[],
                &[],
                (i64::MAX, i64::MAX),
                i64::MAX,
                u64::MAX,
                u64::MAX,
                i64::MAX
            ) == Some((i64::MAX, u64::MAX, None))
        );
    }

    proptest! {
        #[test]
        fn checkpoint_recovery_matches_independent_batch_boundary_oracle(
            widths in prop::collection::vec(1i64..8, 0..24),
            physical_start in 0i64..20,
            floor_seed in any::<u16>(),
            start in -1i64..210,
            cut in -1i64..210,
            hw in any::<i64>(), lso in any::<i64>(), deliverable in any::<i64>(),
        ) {
            let mut end = physical_start;
            let ends: Vec<_> = widths.into_iter().map(|width| { end += width; end }).collect();
            let floor = physical_start + i64::from(floor_seed) % (end - physical_start + 1);
            let expected = if floor <= start && start <= cut && cut <= end
                && (start == cut || ends.contains(&cut)) {
                Some((if start == cut { 0 } else { ends.iter().filter(|next| **next <= cut).count() },
                    hw.min(lso).min(deliverable).min(cut)))
            } else { None };
            assert!(checkpoint_truncation_bounds_fetch(&ends, physical_start,
                FetchWatermarks { log_start: floor, log_end: end, hw, lso, deliverable }, start, cut) == expected);
        }
    }

    #[test]
    fn checkpoint_recovery_handles_empty_interior_and_extreme_frontiers() {
        for (ends, physical_start, floor, start, cut, expected) in [
            (&[3, 6][..], 0, 0, 0, 1, None),
            (&[3, 6][..], 0, 0, 1, 3, Some((1, 3))),
            (&[3, 6][..], 0, 1, 1, 1, Some((0, 1))),
            (
                &[][..],
                i64::MAX,
                i64::MAX,
                i64::MAX,
                i64::MAX,
                Some((0, i64::MAX)),
            ),
            (
                &[i64::MAX][..],
                i64::MAX - 3,
                i64::MAX - 2,
                i64::MAX - 2,
                i64::MAX,
                Some((1, i64::MAX)),
            ),
        ] {
            assert!(
                checkpoint_truncation_bounds_fetch(
                    ends,
                    physical_start,
                    FetchWatermarks {
                        log_start: floor,
                        log_end: ends.last().copied().unwrap_or(physical_start),
                        hw: i64::MAX,
                        lso: i64::MAX,
                        deliverable: i64::MAX
                    },
                    start,
                    cut
                ) == expected
            );
        }
    }

    #[test]
    fn checked_wal_copy_boundaries() {
        let source = [(0, 1, std::vec![1, 2]), (2, 0, std::vec![3, 4, 5])];
        assert!(checked_wal_copy_replays_exactly(&source, &source, 0, 3, 10, 15) == Some((3, 15)));
        assert!(checked_wal_copy_replays_exactly(&source, &source, 0, 3, 10, 14).is_none());
        assert!(checked_wal_copy_replays_exactly(&source, &source, 0, 2, 10, 15).is_none());
        assert!(checked_wal_copy_replays_exactly(&source, &source[..1], 0, 3, 10, 15).is_none());
        let mut different = source.clone();
        different[1].2[2] ^= 1;
        assert!(checked_wal_copy_replays_exactly(&source, &different, 0, 3, 10, 15).is_none());
        assert!(
            checked_wal_copy_replays_exactly(&[], &[], i64::MAX, i64::MAX, u64::MAX, u64::MAX)
                == Some((i64::MAX, u64::MAX))
        );
        assert!(checked_wal_copy_replays_exactly(&[], &[], 0, 0, 2, 1).is_none());
        for source in [
            [(0, -1, std::vec![1])],
            [(-1, 0, std::vec![1])],
            [(i64::MAX, 0, std::vec![1])],
            [(0, 0, std::vec![])],
        ] {
            assert!(
                checked_wal_copy_replays_exactly(&source, &source, source[0].0, 1, 0, 16).is_none()
            );
        }
        let gap = [(0, 0, std::vec![1]), (2, 0, std::vec![2])];
        assert!(checked_wal_copy_replays_exactly(&gap, &gap, 0, 3, 0, 16).is_none());
        let last = [(i64::MAX - 1, 0, std::vec![1])];
        assert!(
            checked_wal_copy_replays_exactly(
                &last,
                &last,
                i64::MAX - 1,
                i64::MAX,
                u64::MAX - 1,
                u64::MAX
            ) == Some((i64::MAX, u64::MAX))
        );
        assert!(
            checked_wal_copy_replays_exactly(
                &last,
                &last,
                i64::MAX - 1,
                i64::MAX,
                u64::MAX,
                u64::MAX
            )
            .is_none()
        );
    }

    proptest! {
        #[test]
        fn installed_wal_fetch_support_matches_identity_and_sorted_offset_oracles(
            votes in proptest::collection::vec((0u64..16, any::<i64>()), 0..12),
            configured in 0usize..12,
            matching_count in any::<bool>(),
            end in 0i64..=i64::MAX,
            start in 0i64..=i64::MAX,
            current in 0i64..=i64::MAX,
            lso in any::<i64>(),
            deliverable in any::<i64>(),
        ) {
            let (voters, reported): (Vec<_>, Vec<_>) = votes.into_iter().unzip();
            let local = voters.first().copied().unwrap_or(0);
            let expected = if matching_count { voters.len() } else { configured };
            let distinct: std::collections::HashSet<_> = voters.iter().copied().collect();
            let current = current.min(end);
            let w = FetchWatermarks { log_start: start.min(end), log_end: end, hw: 0, lso, deliverable };
            let result = installed_wal_quorum_bounds_fetch(&voters, &reported, local, expected, current, w);
            assert!(result.is_some() == (expected > 0 && voters.len() == expected && distinct.len() == voters.len()));
            if let Some((hw, limit, supporters)) = result {
                let mut sorted: Vec<_> = reported.iter().map(|offset| (*offset).min(end)).collect();
                sorted.sort_unstable_by(|a, b| b.cmp(a));
                let expected_hw = current.max(w.log_start).max(sorted[voters.len() / 2]);
                assert!(hw == expected_hw && limit == hw.min(lso).min(deliverable));
                let expected_support: Vec<_> = voters.iter().copied().zip(reported.iter().copied())
                    .map(|(node, offset)| (node, offset.min(end)))
                    .filter(|(_, offset)| *offset >= limit).collect();
                assert!(supporters == expected_support);
                let support_nodes: std::collections::HashSet<_> = supporters.iter().map(|(node, _)| *node).collect();
                assert!(support_nodes.len() == supporters.len());
                if hw > current && limit > w.log_start { assert!(support_nodes.len() >= voters.len() / 2 + 1); }
            }
        }
    }

    #[test]
    fn installed_wal_fetch_support_boundaries() {
        let w = FetchWatermarks {
            log_start: 0,
            log_end: 10,
            hw: 0,
            lso: 10,
            deliverable: 10,
        };
        assert!(
            installed_wal_quorum_bounds_fetch(&[1, 2, 3], &[0, 10, 10], 1, 3, 0, w)
                == Some((10, 10, std::vec![(2, 10), (3, 10)]))
        );
        // The unsynced leader end is not a second vote for a single follower.
        assert!(
            installed_wal_quorum_bounds_fetch(&[1, 2, 3], &[0, 10, 0], 1, 3, 0, w)
                == Some((0, 0, std::vec![(1, 0), (2, 10), (3, 0)]))
        );
        assert!(installed_wal_quorum_bounds_fetch(&[1, 2, 2], &[0, 10, 10], 1, 3, 0, w).is_none());
        assert!(installed_wal_quorum_bounds_fetch(&[1, 2, 3], &[0, 10], 1, 3, 0, w).is_none());
        assert!(installed_wal_quorum_bounds_fetch(&[2, 1, 3], &[10, 10, 10], 1, 3, 0, w).is_none());
        assert!(installed_wal_quorum_bounds_fetch(&[], &[], 1, 3, 0, w).is_none());
        assert!(installed_wal_quorum_bounds_fetch(&[1, 2], &[10, 10], 1, 3, 0, w).is_none());
        // Raising the floor alone exposes no retained records and need not
        // obtain new support. An inherited HWM needs prior durability evidence.
        for (current, start) in [(0, 5), (5, 0)] {
            assert!(
                installed_wal_quorum_bounds_fetch(
                    &[1, 2, 3],
                    &[0, 0, 0],
                    1,
                    3,
                    current,
                    FetchWatermarks {
                        log_start: start,
                        ..w
                    }
                ) == Some((5, 5, std::vec![]))
            );
        }
        let maximum = FetchWatermarks {
            log_end: i64::MAX,
            lso: i64::MAX,
            deliverable: i64::MAX,
            ..w
        };
        assert!(
            installed_wal_quorum_bounds_fetch(&[1], &[i64::MAX], 1, 1, 0, maximum)
                == Some((i64::MAX, i64::MAX, std::vec![(1, i64::MAX)]))
        );
        assert!(
            installed_wal_quorum_bounds_fetch(&[1, 2, 3], &[0, i64::MAX, i64::MAX], 1, 3, 0, w)
                == Some((10, 10, std::vec![(2, 10), (3, 10)]))
        );
        assert!(
            installed_wal_quorum_bounds_fetch(
                &[1, 2, 3],
                &[3, 8, 10],
                1,
                3,
                0,
                FetchWatermarks {
                    lso: 5,
                    deliverable: 6,
                    ..w
                }
            ) == Some((8, 5, std::vec![(2, 8), (3, 10)]))
        );
    }

    proptest! {
        #[test]
        fn wal_placement_compositions_match_set_and_rack_loss_oracles(
            candidates in proptest::collection::vec((0u64..8, 0u64..8), 0..32),
            local in 0u64..8,
            requested in 3usize..10,
            failed_rack in 0u64..8,
        ) {
            let selected = select_wal_voters(&candidates, local, requested);
            assert!(constructed_wal_placement_is_installable(&candidates, local, requested));
            assert!(wal_placement_survives_one_rack_loss(&candidates, local, requested, failed_rack));
            if selected.len() == requested {
                let survivors: std::collections::HashSet<_> = selected.iter()
                    .filter(|(_, rack)| *rack != failed_rack).map(|(node, _)| *node).collect();
                assert!(survivors.len() >= requested / 2 + 1);
            }
        }
    }

    #[test]
    fn wal_placement_composition_boundaries() {
        let candidates = [(1, 10), (1, 20), (2, 10), (3, 20), (4, 30), (4, 40)];
        for requested in [0, 1, 2, 3, 4, usize::MAX] {
            assert!(constructed_wal_placement_is_installable(
                &candidates,
                1,
                requested
            ));
            assert!(constructed_wal_placement_is_installable(
                &candidates,
                99,
                requested
            ));
            if requested >= 3 {
                for failed in [10, 20, 30, 40, u64::MAX] {
                    assert!(wal_placement_survives_one_rack_loss(
                        &candidates,
                        1,
                        requested,
                        failed
                    ));
                }
            }
        }
        assert!(!wal_voter_set_valid(&[1, 2, 2], 1, 3));
        assert!(election_has_quorum(3, 2));
        assert!(!election_has_quorum(2, 1));
        // Greedy placement is maximal, not globally maximum on conflicting
        // duplicate metadata rows: another rack for node 1 could admit node 2.
        assert!(select_wal_voters(&[(1, 10), (1, 20), (2, 10)], 1, 2) == [(1, 10)]);
    }

    #[test]
    fn trim_composition_boundaries_expose_inherited_frontier_requirement() {
        let facts = DeleteRecordsTrimFacts {
            requested: 5,
            current_start: 0,
            high_watermark: 5,
            log_end: 10,
            has_delivery_watermark: true,
            delivery_watermark: 5,
        };
        // An admitted request cannot bound an already-unbounded WAL start.
        assert!(
            delete_records_trim_decision(facts) == DeleteRecordsTrimDecision::Apply { frontier: 5 }
        );
        assert!(
            delete_records_trim_application(5, 8, 0)
                == DeleteRecordsTrimApplication::TrimLocal { frontier: 8 }
        );
        for (requested, wal, local) in [(5, 0, 0), (5, 8, 3), (5, 3, 8), (i64::MAX, 0, 0)] {
            for applied in [
                &[][..],
                &[false, false],
                &[true],
                &[false, true, false, true, true],
            ] {
                let frontier = requested.max(wal).max(local);
                let result = trim_steps_converge(requested, wal, local, applied);
                assert!(result.0 >= wal && result.1 >= local);
                assert!(result.0 <= frontier && result.1 <= frontier);
            }
        }
        for requested in [-2, -1, 0, 3, 5, 6] {
            assert!(admitted_trim_bounds_reload_and_retry(
                DeleteRecordsTrimFacts { requested, ..facts },
                3,
                2,
                &[0, 3, 5, 10, 11]
            ));
        }
        // Snapshot validity preserves historical producer state, even when
        // its last batch is below the new logical start. It is not a data read.
        let historical = crate::producer_snapshot::ProducerSnapshotEntryFacts {
            producer_id: 1,
            producer_epoch: 0,
            last_sequence: 0,
            last_offset: 0,
            offset_delta: 0,
            coordinator_epoch: -1,
            current_txn_first_offset: -1,
        };
        assert!(crate::producer_snapshot::producer_snapshot_entry_valid(
            10, historical
        ));
        for lag in [-1, 0, 2, 5, 6, i64::MAX] {
            assert!(diskless_trim_reconciliation_preserves_coverage(
                5,
                i64::MAX,
                lag,
                0,
                1,
                &[false, true, true]
            ));
        }
    }

    #[test]
    fn remote_timestamp_composition_handles_padding_and_conservative_rows() {
        let offsets = [0, 3, 6];
        let timestamps = [100, 300, 200];
        for entries in [
            &[][..],
            &[(100, 0), (300, 3), (300, 6)],
            &[(100, 0), (300, 3), (300, 6), (0, 0), (0, 0)],
            // Conservative bounds may decrease without skipping a match;
            // this is accepted by remote scanning but fails row validation.
            &[(500, 0), (100, 3), (300, 6)],
            &[(i64::MIN, 0), (0, 0)],
        ] {
            for target in [i64::MIN, 100, 200, 300, 301, 500, i64::MAX] {
                assert!(remote_timestamp_scan_preserves_first(
                    entries,
                    &offsets,
                    &timestamps,
                    target
                ));
                assert!(validated_remote_and_local_time_starts_agree(
                    entries, 6, target
                ));
            }
        }
        // Structural validity alone cannot establish a truthful running maximum.
        // This sorted, in-range row skips a real earlier match.
        assert!(restore_time_index_entry_valid(None, 0, 3, 6));
        let misleading = [(0, 3)];
        let count = remote_time_index_candidate_count(&misleading, 100);
        assert!(count == 1 && misleading[count - 1].1 == 3);
        assert!(first_timestamp_index(&timestamps, 100) == Some(0));
        assert!(first_timestamp_index(&timestamps[1..], 100).map(|index| index + 1) == Some(1));
    }

    #[test]
    fn delivery_replication_and_restore_composition_boundaries() {
        for activations in [
            &[][..],
            &[10, 20, 5],
            &[i64::MIN],
            &[i64::MAX - 1, i64::MAX],
        ] {
            for uncertainty in [-1, 0, 2, i64::MAX] {
                for now in [i64::MIN, 19, 20, i64::MAX] {
                    let all_due = activations.iter().all(|activation| {
                        uncertainty >= 0
                            && i128::from(*activation) + i128::from(uncertainty) <= i128::from(now)
                    });
                    assert!(
                        segment_maximum_proves_delivery(activations, uncertainty, now) == all_due
                    );
                }
            }
        }
        let w = FetchWatermarks {
            log_start: 0,
            log_end: 6,
            hw: 6,
            lso: 6,
            deliverable: 6,
        };
        for (batches, activations) in [
            (&[(0, 1), (2, 1), (4, 1)][..], &[0, 100, 0][..]),
            (&[(0, 1), (4, 1)][..], &[10, 20][..]),
            (&[(0, -1)][..], &[10][..]),
            (&[(0, 1), (1, 1)][..], &[10, 20][..]),
        ] {
            for (uncertainty, now) in [(-1, 100), (0, 0), (0, 100), (2, 100), (i64::MAX, i64::MAX)]
            {
                assert!(scheduled_prefix_bounds_fetch(
                    batches,
                    activations,
                    uncertainty,
                    now,
                    w
                ));
            }
        }
        assert!(scheduled_prefix_bounds_fetch(
            &[],
            &[],
            0,
            0,
            FetchWatermarks { log_end: 0, ..w }
        ));
        let facts = ReplicaFetchFacts {
            request_leader_epoch: 2,
            current_leader_epoch: 2,
            target_matches: true,
            reported_target_matches: true,
            error_code: 0,
            diverging_epoch: -1,
        };
        // Success clamps an over-reported HWM to the exact appended log end.
        assert!(fenced_replication_bounds_fetch(facts, -1, 6, 1, i64::MAX, w) == (8, 8, 6));
        // Divergence never appends even when valid append coordinates are present.
        assert!(
            fenced_replication_bounds_fetch(
                ReplicaFetchFacts {
                    diverging_epoch: 0,
                    ..facts
                },
                3,
                6,
                1,
                8,
                w
            ) == (3, 3, 3)
        );
        for fenced in [
            ReplicaFetchFacts {
                request_leader_epoch: 1,
                ..facts
            },
            ReplicaFetchFacts {
                target_matches: false,
                ..facts
            },
            ReplicaFetchFacts {
                reported_target_matches: false,
                ..facts
            },
            ReplicaFetchFacts {
                error_code: 6,
                ..facts
            },
        ] {
            assert!(fenced_replication_bounds_fetch(fenced, -1, 6, 1, i64::MAX, w) == (6, 6, 6));
        }
        // Compacted replication may skip offset 6; the physical end advances,
        // while read-committed Fetch stays capped by the remaining watermarks.
        assert!(fenced_replication_bounds_fetch(facts, -1, 7, 1, 8, w) == (9, 8, 6));
        assert!(fenced_replication_bounds_fetch(facts, -1, 6, -1, 8, w) == (6, 6, 6));
        let exclusions = RestoreExclusions {
            producer: false,
            offset: false,
            content: crate::restore::RestoreContentExclusions {
                key: false,
                header: false,
            },
        };
        let records = [
            (
                RestoreRecordDeltas {
                    offset_delta: 0,
                    timestamp_delta: 0,
                },
                exclusions,
            ),
            (
                RestoreRecordDeltas {
                    offset_delta: 1,
                    timestamp_delta: 1,
                },
                exclusions,
            ),
        ];
        let frame = RestoreBatchFrame {
            base_offset: 4,
            last_offset_delta: 1,
            timestamp_type: crate::restore::RestoreTimestampType::CreateTime,
            base_timestamp: 10,
            max_timestamp: 11,
        };
        for records in [
            &[][..],
            &records,
            &[(
                records[0].0,
                RestoreExclusions {
                    producer: true,
                    ..exclusions
                },
            )],
        ] {
            for offset_bound in [None, Some(3), Some(4), Some(5)] {
                for timestamp_bound in [None, Some(10), Some(11), Some(12)] {
                    assert!(restore_selection_respects_batch_extent(
                        frame,
                        records,
                        offset_bound,
                        timestamp_bound
                    ));
                }
            }
        }
        assert!(restore_selection_respects_batch_extent(
            RestoreBatchFrame {
                timestamp_type: crate::restore::RestoreTimestampType::LogAppendTime,
                base_timestamp: i64::MAX,
                ..frame
            },
            &records,
            Some(5),
            Some(12),
        ));
        assert!(restore_selection_respects_batch_extent(
            frame,
            &[(
                RestoreRecordDeltas {
                    offset_delta: 2,
                    timestamp_delta: 0
                },
                exclusions
            )],
            None,
            None,
        ));
    }

    #[test]
    fn restored_state_composition_boundaries() {
        let time_entries = [(10, 1), (10, 3), (20, 7)];
        // Equal timestamps select the final equal entry, not the first one.
        assert!(time_index_lookup(&time_entries, 10) == 3);
        for entries in [
            &[][..],
            &time_entries,
            &[(20, 1), (10, 3)],
            &[(10, 3), (20, 3)],
            &[(10, 11)],
        ] {
            for (lower, upper) in [(i64::MIN, 9), (10, 10), (10, 20), (20, i64::MAX)] {
                assert!(validated_time_cursors_are_monotone(
                    entries, 100, 110, lower, upper
                ));
            }
        }
        assert!(validated_time_cursors_are_monotone(
            &[(10, u32::MAX)],
            0,
            i64::from(u32::MAX),
            0,
            10,
        ));
        assert!(validated_time_cursors_are_monotone(
            &[(10, 1)],
            i64::MAX - 1,
            i64::MAX,
            0,
            10,
        ));
        let epochs = [
            EpochEntry {
                epoch: LeaderEpoch(2),
                start_offset: Offset(100),
            },
            EpochEntry {
                epoch: LeaderEpoch(5),
                start_offset: Offset(105),
            },
        ];
        let w = FetchWatermarks {
            log_start: 100,
            log_end: 110,
            hw: 109,
            lso: 108,
            deliverable: 107,
        };
        assert!(
            epoch_and_offset_for_entries(&epochs, LeaderEpoch(3), Offset(110))
                == (LeaderEpoch(2), Offset(105))
        );
        for requested in [-1, 0, 2, 3, 5, 6] {
            assert!(validated_epochs_bound_truncated_fetch(
                &epochs, requested, 100, w
            ));
            assert!(validated_epochs_bound_truncated_fetch(
                &[],
                requested,
                100,
                w
            ));
        }
        let malformed_epochs = [
            EpochEntry {
                epoch: LeaderEpoch(5),
                start_offset: Offset(100),
            },
            EpochEntry {
                epoch: LeaderEpoch(2),
                start_offset: Offset(105),
            },
        ];
        assert!(validated_epochs_bound_truncated_fetch(
            &malformed_epochs,
            3,
            100,
            w
        ));
        let range = ProducerReloadRange {
            log_start: 0,
            local_start: 2,
            log_end: 20,
        };
        let snapshots = [20, 5, 0, 11, -1, 4];
        let shortened = ProducerReloadRange {
            log_end: 10,
            ..range
        };
        assert!(producer_snapshot_latest_index(&snapshots, shortened) == Some(1));
        assert!(producer_snapshot_replay_start(shortened, Some(5)) == Some(5));
        for offsets in [&[][..], &snapshots, &[20, 11], &[2, 3, 10]] {
            assert!(truncated_snapshot_selection_bounds_replay(
                offsets, range, 10
            ));
        }
        let segment = RestoreSegmentExtent {
            base_offset: 100,
            last_offset: 110,
        };
        // Starts can decrease and precede this segment; abort markers must advance.
        let aborts = [
            RestoreAbortedTxn {
                producer_id: 2,
                start_offset: 105,
                last_offset: 108,
            },
            RestoreAbortedTxn {
                producer_id: 1,
                start_offset: 90,
                last_offset: 110,
            },
        ];
        assert!(aborted_transaction_overlaps(90, 110, 100, 107));
        assert!(!aborted_transaction_overlaps(105, 108, 100, 105));
        for entries in [&[][..], &aborts, &[aborts[1], aborts[0]]] {
            for cut in [i64::MIN, 100, 105, 107, 110, i64::MAX] {
                assert!(restored_aborts_remain_bounded_when_fetch_shrinks(
                    entries, segment, w, 100, cut
                ));
            }
        }
    }

    #[test]
    fn composition_boundary_witnesses() {
        for (base, delta) in [
            (0, 0),
            (10, 2),
            (i64::MAX - 1, 0),
            (i64::MAX, 0),
            (i64::MAX - 1, 1),
            (-1, 0),
            (0, -1),
            (0, i32::MAX),
        ] {
            assert!(append_frontiers_agree(base, delta));
        }
        for (base, first, second) in [(0, 2, 3), (i64::MAX - 2, 1, 1), (0, 0, 1), (0, 1, -1)] {
            assert!(reservations_do_not_overlap(base, first, second));
        }
        for starts in [&[][..], &[9, 3, 14], &[20], &[21], &[-1]] {
            for deliverable in [0, 2, 20] {
                assert!(committed_fetch_excludes_unstable(
                    starts,
                    FetchWatermarks {
                        log_start: 0,
                        log_end: 20,
                        hw: 15,
                        lso: 20,
                        deliverable,
                    },
                ));
            }
        }
        for (followers, epoch_start, leader_counts) in [
            (&[10, 5][..], 0, true),
            (&[3, 5][..], 0, true),
            (&[3, 5][..], 6, true),
            (&[10, 5][..], 0, false),
        ] {
            let (hw, limit) = quorum_commit_bounds_fetch(
                followers,
                2,
                epoch_start,
                0,
                leader_counts,
                FetchWatermarks {
                    log_start: 0,
                    log_end: 10,
                    hw: 0,
                    lso: 8,
                    deliverable: 9,
                },
            );
            assert!(limit <= hw && hw <= 10);
            if hw > 0 {
                let supporters = usize::from(leader_counts)
                    + followers.iter().filter(|offset| **offset >= limit).count();
                assert!(supporters >= 2);
            }
        }
        for entries in [
            &[][..],
            &[(1, 2), (3, 5), (8, 9)],
            &[(1, 2), (1, 3)],
            &[(1, 10)],
        ] {
            for target in [0, 1, 2, 3, 8, u32::MAX] {
                assert!(validated_index_bounds_lookup(entries, target, 8, 10));
            }
        }
        for (generation, count, marker_generation, marker_count) in [
            (1, 5, 1, 2),
            (1, 2, 1, 2),
            (1, 2, 1, 3),
            (1, 5, 2, 2),
            (u64::MAX - 1, 5, u64::MAX - 1, 2),
        ] {
            assert!(loss_settlement_is_idempotent(
                AuditLosses { generation, count },
                AuditLosses {
                    generation: marker_generation,
                    count: marker_count,
                },
            ));
        }
        // Witness the theorem's generation-exhaustion boundary explicitly.
        let saturated = AuditLosses {
            generation: u64::MAX,
            count: 5,
        };
        let marker = AuditLosses {
            generation: u64::MAX,
            count: 2,
        };
        let settled = settle_loss_batch(saturated, marker);
        let replayed = settle_loss_batch(settled, marker);
        assert!(settled.count == 3 && replayed.count == 1);
    }
}
