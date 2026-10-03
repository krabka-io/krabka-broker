//! The five KIP-534 safety rules one compaction pass must obey, stated over the
//! pass's input log, its output log and the clock it ran at.
//!
//! Every rule is computed from the two logs alone. None of them calls the
//! production cores or reads bookkeeping from the pass, so a pass cannot
//! satisfy a rule by construction: the rules are the specification the pass is
//! checked against.
//!
//! A pass may only delete entries and stamp a horizon on an entry that has
//! none, so its output must be an order-preserving subsequence of its input
//! under that one permitted rewrite. [`Invariant::IdempotentStamp`] checks that
//! shape. The retention rules each name the input entries a pass must *not*
//! delete, and hold when those entries appear, in order, in the output. See
//! [`retains`]. The two checks are independent, so a pass that breaks one rule
//! is reported against that rule alone.

use std::collections::{HashMap, HashSet};

use super::{
    DELETE_RETENTION_MS,
    state::{Entry, EntryKind},
};

/// One named pass rule. The name is the `always` property that checks it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Invariant {
    /// A control marker leaves the log only through its delete horizon. Every
    /// input marker whose horizon has not elapsed is in the output. Markers
    /// are never deduped against one another by their shared control key.
    ControlNotDeduped,
    /// A marker never leaves the log while a data entry of its transaction is
    /// in the pass's input: every input marker with a data entry of its
    /// producer in front of it, since that producer's previous marker, is in
    /// the output, whether or not its horizon has elapsed. Kafka decides this
    /// per transaction, so a marker whose transaction's data is gone ages out
    /// even when the same producer has newer live data.
    MarkerDataPrecedence,
    /// A tombstone stays for `delete.retention.ms` and then goes. No output
    /// tombstone has an elapsed horizon, and every input tombstone that is the
    /// newest for its key and has not reached its horizon is in the output.
    TombstoneAging,
    /// The pass only deletes entries and stamps unstamped horizons with
    /// `now + delete.retention.ms`. It never changes an existing horizon and
    /// never invents or reorders an entry.
    IdempotentStamp,
    /// The set of keys whose newest value is live is unchanged. No live newest
    /// value is lost, and deleting a tombstone never resurrects a superseded
    /// value.
    NoDataLoss,
}

impl Invariant {
    pub(super) const ALL: [Self; 5] = [
        Self::ControlNotDeduped,
        Self::MarkerDataPrecedence,
        Self::TombstoneAging,
        Self::IdempotentStamp,
        Self::NoDataLoss,
    ];

    /// Whether this rule holds for a pass at `clock` that turned `input` into
    /// `output`.
    pub(super) fn holds(self, input: &[Entry], output: &[Entry], clock: i64) -> bool {
        match self {
            Self::ControlNotDeduped => retains(input, output, |_, e| {
                matches!(e.kind, EntryKind::Marker { .. }) && !e.horizon_elapsed(clock)
            }),
            Self::MarkerDataPrecedence => {
                let held = markers_behind_data(input);
                retains(input, output, |idx, _| held.contains(&idx))
            }
            Self::TombstoneAging => {
                let newest = newest_data_indices(input);
                output.iter().all(|e| {
                    !(matches!(e.kind, EntryKind::Data { value: None }) && e.horizon_elapsed(clock))
                }) && retains(input, output, |idx, e| {
                    matches!(e.kind, EntryKind::Data { value: None })
                        && newest.contains(&idx)
                        && !e.horizon_elapsed(clock)
                })
            }
            Self::IdempotentStamp => is_stamp_only_subsequence(input, output, clock),
            Self::NoDataLoss => live_keys(input) == live_keys(output),
        }
    }

    /// Every rule `input → output` breaks, in [`Invariant::ALL`] order.
    pub(super) fn violations(input: &[Entry], output: &[Entry], clock: i64) -> Vec<Self> {
        Self::ALL
            .into_iter()
            .filter(|inv| !inv.holds(input, output, clock))
            .collect()
    }
}

#[cfg(test)]
#[path = "invariants/tests.rs"]
mod tests;

#[path = "invariants/helpers.rs"]
mod helpers;
pub(super) use helpers::live_keys;
use helpers::{is_stamp_only_subsequence, markers_behind_data, newest_data_indices, retains};
