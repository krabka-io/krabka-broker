//! The abstract state the compaction model enumerates: a log [`Entry`], the
//! [`CompactState`] the checker fingerprints, the [`CompactAction`] alphabet,
//! and the [`CompactModel`] bounds together with the three derivations that the
//! transition relation and the safety asserts both need.

use std::collections::{HashMap, HashSet};

use crate::compact::{TxnDataState, should_index_key};

/// What a log entry carries downstream of the compaction decision.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum EntryKind {
    /// A data record. `value: None` is a tombstone.
    Data { value: Option<u8> },
    /// A transaction control marker, commit or abort, for `producer_id`.
    Marker { producer_id: u8, commit: bool },
}

/// One abstract log entry. `horizon` mirrors the batch's KIP-534
/// delete-horizon stamp. It is `None` until the stamp, and then
/// `Some(now + delete.retention.ms)`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct Entry {
    pub(super) key: Option<u8>,
    pub(super) kind: EntryKind,
    pub(super) horizon: Option<i64>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct CompactState {
    pub(super) log: Vec<Entry>,
    /// Abstract wall clock in ms. Entries hold horizons as absolute stamp
    /// values, and the model compares them against this clock. The state does
    /// NOT hold the non-vacuity witnesses. [`Model::properties`] derives them
    /// from `(log, clock)`. The fingerprint therefore stays free of the
    /// monotonic witness bools, which would otherwise multiply the reachable
    /// state space by about 32.
    pub(super) clock: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum CompactAction {
    AppendData(u8, u8),
    AppendTombstone(u8),
    AppendCommit(u8),
    Tick(i64),
    Compact,
}

pub(super) struct CompactModel {
    /// Maximum log length the actions generator and `within_boundary` enforce.
    pub(super) max_len: usize,
    /// Maximum value `clock` may reach.
    pub(super) max_clock: i64,
}

impl CompactModel {
    /// Build the key→newest-index dedup map over data entries that have a
    /// key. It uses the production [`should_index_key`] filter, so control
    /// entries are never indexed. Later positions overwrite earlier ones, so
    /// the newest wins.
    pub(super) fn offset_map(log: &[Entry]) -> HashMap<u8, usize> {
        let mut map: HashMap<u8, usize> = HashMap::new();
        for (idx, entry) in log.iter().enumerate() {
            if !matches!(entry.kind, EntryKind::Data { .. }) {
                continue;
            }
            let Some(k) = entry.key else { continue };
            // Data entries are never control batches.
            if should_index_key(Some(&[k]), false) {
                map.insert(k, idx);
            }
        }
        map
    }

    /// Producers whose newest-for-key data entry would be Kept, that is,
    /// producers whose transactional data survives this compaction.
    ///
    /// A data entry belongs to a producer only if it carries a producer id. In
    /// this abstract model data entries are anonymous, so survival depends only
    /// on whether *any* keyed live data entry, one with `value=Some`, is
    /// newest-for-key. Markers reference producers by id, and a producer's data
    /// "survives" if and only if at least one surviving keyed live data entry
    /// has a key that maps to that producer.
    ///
    /// The model associates producers with data by key. Marker `pid` goes with
    /// the data entries under key `pid`. The alphabet is small: `pid ∈ {0,1}`
    /// and `key ∈ {0,1}`. The abstraction stays faithful, because a marker's
    /// data survives if and only if key == pid has a surviving live data
    /// entry.
    pub(super) fn data_survives(log: &[Entry], offset_map: &HashMap<u8, usize>) -> HashSet<u8> {
        let mut survivors: HashSet<u8> = HashSet::new();
        for (idx, entry) in log.iter().enumerate() {
            let EntryKind::Data { value } = entry.kind else {
                continue;
            };
            let Some(k) = entry.key else { continue };
            if value.is_none() {
                continue; // tombstones do not constitute surviving data
            }
            if offset_map.get(&k).copied() == Some(idx) {
                // This live data entry survives; associate it with producer `k`.
                survivors.insert(k);
            }
        }
        survivors
    }

    /// The transactional-data state for a marker's producer.
    pub(super) fn txn_state(producer_id: u8, data_survives: &HashSet<u8>) -> TxnDataState {
        if data_survives.contains(&producer_id) {
            TxnDataState::DataSurvives
        } else {
            TxnDataState::DataFullyGone
        }
    }
}
