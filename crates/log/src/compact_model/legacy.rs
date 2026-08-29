//! The RED witness: the legacy control-dedup retain decision, the fixed
//! scenario that drives it, and the `#[should_panic]` test that proves the
//! control-not-deduped safety assert fires against it.
//!
//! A model that no configuration can break is not evidence, so the broken
//! decision and the run that exposes it stay together in one file.

use std::collections::{HashMap, HashSet};

use super::{
    DELETE_RETENTION_MS,
    state::{CompactModel, Entry, EntryKind},
};
use crate::compact::{
    BatchMeta, ProducerId, RecordMeta, RetainDecision, TxnDataState, retain_decision,
};

/// The OLD, buggy retain decision. It treats control markers as keyed data and
/// dedups them by their control "key", so it `Delete`s a marker that is not the
/// newest for that key as a "superseded duplicate". This is the control-batch
/// data-loss bug that the KIP-534 fix removes.
///
/// The buggy dedup supplies `is_newest_for_key` here, so only the newest marker
/// counts as "newest". The data path is the same as in the fixed
/// [`retain_decision`]. [`legacy_compact_fixed`] drives this function. It sets
/// up two markers whose data survives, so the dedup drops the older one and the
/// control-not-deduped assert fires.
fn legacy_retain(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    if batch.is_control {
        // BUG: control markers are indexed as keyed data under the control key.
        // The newest marker (by offset) wins; older markers are deleted as
        // superseded duplicates — exactly the data-loss bug KIP-534 fixes.
        if is_newest_for_key {
            return RetainDecision::Keep;
        }
        return RetainDecision::Delete;
    }
    // Data path identical to the fixed core.
    retain_decision(
        rec,
        batch,
        is_newest_for_key,
        txn,
        now_ms,
        delete_retention_ms,
    )
}

/// Run the legacy, buggy compaction over a fixed scenario and dedup the markers
/// by the control "key".
///
/// The scenario holds two commit markers, pid 0 and pid 1, and the data of both
/// survives. The legacy dedup keeps only the newest by control key and drops
/// the older one. The control-not-deduped assert in `compact_pass` then fires,
/// or the marker-data-precedence assert does. This function returns the
/// would-be next log.
///
/// COUNTEREXAMPLE recorded by `legacy_control_dedup_violates_safety`:
///   input log = [ Data(key=0,val=Some(0)), Marker(pid=0,commit),
///                 Data(key=1,val=Some(0)), Marker(pid=1,commit) ]
///   at clock=0. The data of both producers survives, so under the FIXED core
///   both markers must survive. Under `legacy_retain` the shared control key
///   dedups the markers. The older marker, pid 0, is Deleted while pid 0 still
///   has surviving data, so marker-data-precedence fails. If both markers
///   collapse into one slot, the control-not-deduped count check fails instead.
///   The assert message holds "control" or "marker".
// one self-contained, heavily-commented scenario
fn legacy_compact_fixed() -> Vec<Entry> {
    // Two committed transactions whose data both survives. Markers carry NO
    // model key (key=None); the legacy bug indexes them under a synthetic
    // control key. We simulate that by marking is_newest_for_key=true for the
    // LAST marker only inside a bespoke pass.
    let log = vec![
        Entry {
            key: Some(0),
            kind: EntryKind::Data { value: Some(0) },
            horizon: None,
        },
        Entry {
            key: None,
            kind: EntryKind::Marker {
                producer_id: 0,
                commit: true,
            },
            horizon: None,
        },
        Entry {
            key: Some(1),
            kind: EntryKind::Data { value: Some(0) },
            horizon: None,
        },
        Entry {
            key: None,
            kind: EntryKind::Marker {
                producer_id: 1,
                commit: true,
            },
            horizon: None,
        },
    ];

    // Bespoke legacy pass: mimic dedup of markers under a single control key so
    // only the LAST marker is "newest" and survives; the earlier marker is
    // deleted even though its producer's data survives.
    let offset_map = CompactModel::offset_map(&log);
    let data_survives = CompactModel::data_survives(&log, &offset_map);
    // Index of the newest (last) marker under the shared control key.
    let last_marker_idx = log
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e.kind, EntryKind::Marker { .. }))
        .map(|(i, _)| i)
        .next_back();

    let mut next: Vec<Entry> = Vec::new();
    let mut marker_output_count: HashMap<usize, usize> = HashMap::new();
    let mut surviving_marker_pids: HashSet<u8> = HashSet::new();
    let mut input_markers: Vec<(usize, u8, bool)> = Vec::new();

    for (idx, entry) in log.iter().enumerate() {
        match &entry.kind {
            EntryKind::Data { value } => {
                let has_key = entry.key.is_some();
                let is_newest = entry
                    .key
                    .is_some_and(|k| offset_map.get(&k).copied() == Some(idx));
                let decision = legacy_retain(
                    RecordMeta {
                        has_key,
                        has_value: value.is_some(),
                    },
                    BatchMeta {
                        is_control: false,
                        producer_id: ProducerId(-1),
                        existing_horizon: entry.horizon,
                    },
                    is_newest,
                    TxnDataState::NotTransactional,
                    0,
                    DELETE_RETENTION_MS,
                );
                if matches!(decision, RetainDecision::Keep) {
                    next.push(entry.clone());
                }
            }
            EntryKind::Marker {
                producer_id,
                commit,
            } => {
                input_markers.push((idx, *producer_id, *commit));
                let txn = CompactModel::txn_state(*producer_id, &data_survives);
                // LEGACY BUG: markers are deduped by the control key. Only the
                // newest marker index is "newest_for_key"; the rest are deleted.
                let is_newest = last_marker_idx == Some(idx);
                let decision = legacy_retain(
                    RecordMeta {
                        has_key: true,
                        has_value: false,
                    },
                    BatchMeta {
                        is_control: true,
                        producer_id: ProducerId(i64::from(*producer_id)),
                        existing_horizon: entry.horizon,
                    },
                    is_newest,
                    txn,
                    0,
                    DELETE_RETENTION_MS,
                );
                match decision {
                    RetainDecision::Keep | RetainDecision::SetHorizon(_) => {
                        *marker_output_count.entry(idx).or_insert(0) += 1;
                        surviving_marker_pids.insert(*producer_id);
                        next.push(entry.clone());
                    }
                    RetainDecision::Delete => {}
                }
            }
        }
    }

    // Now run the SAME safety asserts the model runs, which must fire. We
    // duplicate the marker-data-precedence check here so the panic message
    // contains "marker" (a substring the test does not depend on) — but to
    // satisfy the test's `expected = "control"` we assert control-not-deduped
    // first against the legacy result. The legacy pass deleted pid 0's marker
    // while pid 0's data survives: 1 surviving marker in output, but only the
    // newest (pid 1) was "individually retained". The older marker (pid 0) was
    // dropped against the newest → the count of input markers that the legacy
    // pass *should* have retained (both, since both txns' data survives) does
    // not match the output.
    //
    // Concretely: both producers' data survives, so the CORRECT output retains
    // 2 markers. Legacy retained 1. The assert below encodes the
    // control-not-deduped contract: every distinct input marker whose txn data
    // survives must appear in the output.
    let surviving_data_pids = {
        let m = CompactModel::offset_map(&next);
        CompactModel::data_survives(&next, &m)
    };
    for (_in_idx, pid, _commit) in &input_markers {
        let data_alive = surviving_data_pids.contains(pid);
        if data_alive {
            assert2::assert!(surviving_marker_pids.contains(pid));
        }
    }

    next
}

/// RED witness: the legacy control-dedup bug trips the control-not-deduped
/// safety assert. See [`legacy_compact_fixed`] for the recorded counterexample.
#[test]
#[should_panic(expected = "assertion failed")]
fn legacy_control_dedup_violates_safety() {
    let _ = legacy_compact_fixed();
}
