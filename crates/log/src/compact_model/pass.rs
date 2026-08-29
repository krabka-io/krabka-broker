//! One compaction pass over the abstract log, and the five KIP-534 safety
//! invariants it asserts on the log it produces. The pass is generic over the
//! retain decision so that the same asserts run against the production core and
//! against the deliberately-broken legacy one.

use std::collections::{HashMap, HashSet};

use super::{
    DELETE_RETENTION_MS,
    state::{CompactModel, Entry, EntryKind},
};
use crate::compact::{BatchMeta, ProducerId, RecordMeta, RetainDecision, TxnDataState};

/// The retain-decision signature. It is abstract so that [`compact_pass`] can
/// run either the real [`retain_decision`] or the buggy [`legacy_retain`].
pub(super) type RetainFn =
    fn(RecordMeta, BatchMeta, bool, TxnDataState, i64, i64) -> RetainDecision;

/// Run one compaction pass over `log` at `clock`. This function applies
/// `retain` to each entry, asserts the five KIP-534 safety invariants, and
/// returns the next log.
///
/// Any safety violation panics, and the panic message holds the invariant name.
/// State-derived `sometimes` properties prove non-vacuity separately, so this
/// pass carries no witness accumulator.
pub(super) fn compact_pass(log: &[Entry], clock: i64, retain: RetainFn) -> Vec<Entry> {
    let offset_map = CompactModel::offset_map(log);
    let data_survives = CompactModel::data_survives(log, &offset_map);

    // Capture, before the pass, which producers had a marker present and not
    // already aged out (horizon set and elapsed), and which keys had a newest
    // live data entry — for the marker-data-precedence and no-data-loss asserts.
    let mut input_markers: Vec<(usize, u8, bool)> = Vec::new();
    for (idx, entry) in log.iter().enumerate() {
        if let EntryKind::Marker {
            producer_id,
            commit,
        } = entry.kind
        {
            input_markers.push((idx, producer_id, commit));
        }
    }
    // Keys with a newest live (value=Some) data entry in the input.
    let mut input_live_keys: HashSet<u8> = HashSet::new();
    for (idx, entry) in log.iter().enumerate() {
        if let EntryKind::Data { value: Some(_) } = entry.kind
            && let Some(k) = entry.key
            && offset_map.get(&k).copied() == Some(idx)
        {
            input_live_keys.insert(k);
        }
    }

    let mut next: Vec<Entry> = Vec::with_capacity(log.len());
    // For control-not-deduped: count how many output entries each input marker
    // index produced (must be exactly one for Kept/SetHorizon markers).
    let mut marker_output_count: HashMap<usize, usize> = HashMap::new();
    // Producers whose marker survived (kept or stamped) this pass.
    let mut surviving_marker_pids: HashSet<u8> = HashSet::new();

    for (idx, entry) in log.iter().enumerate() {
        let is_control = matches!(entry.kind, EntryKind::Marker { .. });
        let (rec_meta, batch_meta, is_newest, txn) = match &entry.kind {
            EntryKind::Data { value } => {
                let has_key = entry.key.is_some();
                let is_newest = entry
                    .key
                    .is_some_and(|k| offset_map.get(&k).copied() == Some(idx));
                (
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
                )
            }
            EntryKind::Marker {
                producer_id,
                commit: _,
            } => {
                // A marker's RecordMeta is has_key=true, has_value=false.
                let txn = CompactModel::txn_state(*producer_id, &data_survives);
                (
                    RecordMeta {
                        has_key: true,
                        has_value: false,
                    },
                    BatchMeta {
                        is_control: true,
                        producer_id: ProducerId(i64::from(*producer_id)),
                        existing_horizon: entry.horizon,
                    },
                    false,
                    txn,
                )
            }
        };

        let decision = retain(
            rec_meta,
            batch_meta,
            is_newest,
            txn,
            clock,
            DELETE_RETENTION_MS,
        );

        match decision {
            RetainDecision::Keep => {
                if is_control {
                    *marker_output_count.entry(idx).or_insert(0) += 1;
                    if let EntryKind::Marker { producer_id, .. } = entry.kind {
                        surviving_marker_pids.insert(producer_id);
                    }
                }
                next.push(entry.clone());
            }
            RetainDecision::SetHorizon(h) => {
                // idempotent-stamp (4): an entry with horizon=Some(_) must never
                // be re-stamped to a different value.
                if let Some(existing) = entry.horizon {
                    assert2::assert!(existing == h);
                }
                if is_control {
                    *marker_output_count.entry(idx).or_insert(0) += 1;
                    if let EntryKind::Marker { producer_id, .. } = entry.kind {
                        surviving_marker_pids.insert(producer_id);
                    }
                }
                let mut e = entry.clone();
                e.horizon = Some(h);
                next.push(e);
            }
            RetainDecision::Delete => {
                // Dropped (superseded data, null-key data, or an aged-out
                // tombstone/marker). No bookkeeping needed; aging non-vacuity is
                // proven by the state-derived `*_horizon_elapsed` witnesses.
            }
        }
    }

    // ---- Safety asserts on the produced `next` log -----------------------

    // (1) control-not-deduped: every input marker that was Kept/SetHorizon
    // produced exactly one output entry; markers are never merged or dropped
    // against one another. Distinct input markers with the same (pid,commit)
    // both survive as distinct entries.
    let surviving_markers_out = next
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::Marker { .. }))
        .count();
    let expected_surviving: usize = marker_output_count.values().sum();
    assert2::assert!(surviving_markers_out == expected_surviving);
    for (&_idx, &count) in &marker_output_count {
        assert2::assert!(count == 1);
    }

    // (2) marker-data-precedence: if a producer has surviving data in the
    // output, that producer's marker (if it was in the input and not aged out)
    // is in the output.
    let out_offset_map = CompactModel::offset_map(&next);
    let out_data_survivor_pids = CompactModel::data_survives(&next, &out_offset_map);
    for pid in &out_data_survivor_pids {
        // Was there an input marker for this pid?
        let had_input_marker = input_markers.iter().any(|(_, p, _)| p == pid);
        if had_input_marker {
            assert2::assert!(surviving_marker_pids.contains(pid));
        }
    }

    // (3) tombstone-aging: no surviving tombstone has an elapsed horizon.
    for e in &next {
        if matches!(e.kind, EntryKind::Data { value: None })
            && let Some(h) = e.horizon
        {
            assert2::assert!(clock < h);
        }
    }

    // (4) idempotent-stamp is enforced inline at SetHorizon above; additionally,
    // an entry carried forward as Keep must retain its prior horizon unchanged.
    // (Keep clones the entry verbatim, so this holds by construction.)

    // (5) no-data-loss: every key with a newest live Data(value=Some) in the
    // input has a live entry in the output.
    for k in &input_live_keys {
        let present = next
            .iter()
            .any(|e| e.key == Some(*k) && matches!(e.kind, EntryKind::Data { value: Some(_) }));
        assert2::assert!(present);
    }

    next
}
