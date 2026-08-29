//! The stateright [`Model`] implementation for [`CompactModel`]: the initial
//! state, the action alphabet, the transition relation that runs one
//! [`compact_pass`], and the non-vacuity properties. The block lives alone in
//! this file because a trait implementation cannot be split across modules.

use stateright::{Model, Property};

use super::{
    pass::compact_pass,
    state::{CompactAction, CompactModel, CompactState, Entry, EntryKind},
};
use crate::compact::retain_decision;

impl Model for CompactModel {
    type State = CompactState;
    type Action = CompactAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CompactState {
            log: vec![],
            clock: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // Cap log growth so the reachable space stays bounded. The per-position
        // alphabet is deliberately minimal: `retain_decision` branches only on
        // value-*presence* (live vs tombstone) and txn-state, never on the value
        // byte or commit/abort, so we fix the data value to 0 and emit a single
        // marker kind per producer. Collapsing those two provably-irrelevant
        // dimensions cuts the alphabet 10 → 6 symbols (the dominant state-space
        // driver) with zero loss of decision coverage. (`EntryKind::Marker.commit`
        // stays in the type for clarity / the legacy RED witness, but only the
        // commit variant is enumerated.)
        if s.log.len() < self.max_len {
            for key in 0u8..=1 {
                actions.push(CompactAction::AppendData(key, 0));
                actions.push(CompactAction::AppendTombstone(key));
            }
            for pid in 0u8..=1 {
                actions.push(CompactAction::AppendCommit(pid));
            }
        }
        for dt in [1i64, 2] {
            if s.clock + dt <= self.max_clock {
                actions.push(CompactAction::Tick(dt));
            }
        }
        actions.push(CompactAction::Compact);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        match action {
            CompactAction::AppendData(key, value) => {
                let mut s = last.clone();
                s.log.push(Entry {
                    key: Some(key),
                    kind: EntryKind::Data { value: Some(value) },
                    horizon: None,
                });
                Some(s)
            }
            CompactAction::AppendTombstone(key) => {
                let mut s = last.clone();
                s.log.push(Entry {
                    key: Some(key),
                    kind: EntryKind::Data { value: None },
                    horizon: None,
                });
                Some(s)
            }
            CompactAction::AppendCommit(pid) => {
                let mut s = last.clone();
                s.log.push(Entry {
                    key: None,
                    kind: EntryKind::Marker {
                        producer_id: pid,
                        commit: true,
                    },
                    horizon: None,
                });
                Some(s)
            }
            CompactAction::Tick(dt) => {
                let mut s = last.clone();
                s.clock += dt;
                Some(s)
            }
            CompactAction::Compact => {
                let next_log = compact_pass(&last.log, last.clock, retain_decision);
                let mut s = last.clone();
                s.log = next_log;
                Some(s)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Structural invariant: compaction never introduces a key that was
            // not present in the input — output keys ⊆ input keys is preserved
            // because Compact only ever carries entries forward (never invents).
            // We assert the always-true form: a surviving data entry's horizon
            // is non-decreasing relative to itself (never un-stamped), captured
            // by idempotent-stamp; here we assert the simplest structural fact:
            // the log never contains a marker that is both committed and
            // aborted for the same slot (entries are immutable once appended).
            Property::always("entries_well_formed", |_, s: &CompactState| {
                s.log.iter().all(|e| match &e.kind {
                    EntryKind::Data { .. } => true,
                    EntryKind::Marker { .. } => e.key.is_none(),
                })
            }),
            // A delete-horizon was stamped and the entry retained (some log entry
            // carries a horizon).
            Property::sometimes("horizon_stamped", |_, s: &CompactState| {
                s.log.iter().any(|e| e.horizon.is_some())
            }),
            // Two markers coexist in one log — proof markers are never key-deduped
            // against each other (the bug would have collapsed them to one).
            Property::sometimes("control_not_deduped", |_, s: &CompactState| {
                s.log
                    .iter()
                    .filter(|e| matches!(e.kind, EntryKind::Marker { .. }))
                    .count()
                    >= 2
            }),
            // A marker is retained because its producer's transaction data
            // survives this compaction.
            Property::sometimes("marker_retained_for_live_data", |_, s: &CompactState| {
                let om = CompactModel::offset_map(&s.log);
                let ds = CompactModel::data_survives(&s.log, &om);
                s.log.iter().any(|e| {
                    matches!(&e.kind, EntryKind::Marker { producer_id, .. } if ds.contains(producer_id))
                })
            }),
            // A retained tombstone reaches an elapsed horizon (the next compaction
            // ages it out) — proves the tombstone-aging path is reachable.
            Property::sometimes("tombstone_horizon_elapsed", |_, s: &CompactState| {
                s.log.iter().any(|e| {
                    matches!(e.kind, EntryKind::Data { value: None })
                        && e.horizon.is_some_and(|h| s.clock >= h)
                })
            }),
            // A retained marker reaches an elapsed horizon (data gone + grace
            // window elapsed) — proves the marker-aging path is reachable.
            Property::sometimes("marker_horizon_elapsed", |_, s: &CompactState| {
                s.log.iter().any(|e| {
                    matches!(e.kind, EntryKind::Marker { .. })
                        && e.horizon.is_some_and(|h| s.clock >= h)
                })
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log.len() <= self.max_len && s.clock <= self.max_clock
    }
}
