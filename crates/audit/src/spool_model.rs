//! Bounded Stateright model of audit-spool append, replay, crash recovery, and
//! loss accounting.
//!
//! DRIVEN: every spool append, a record's or a loss marker's, calls the
//! production `spool_append_decision` kernel used by `Spool::append`. `Reopen`
//! calls the production `replay_recovery` classifier used by `Spool::open`.
//! `Lose` calls the production `add_loss_state` helper used by
//! `PendingLosses::add`. `CommitLossMarker` and the loss reconciliation in
//! `Reopen` call the production `settle_loss_batch` kernel that
//! `PendingLosses::commit` and `PendingLosses::reconcile` apply.
//!
//! MODELED: filesystem writes are split into volatile and durable records;
//! `Sync` publishes all completed appends; `Crash` drops volatile and torn
//! writes; replay poison, sink delivery, cursor persistence, and poison removal
//! are separate actions. A poison at the current cursor makes `Reopen` stop for
//! explicit recovery, before loss reconciliation, as `Spool::open` returns
//! early. A poison behind the cursor is cleared.
//!
//! The loss path is `PendingLosses` as the writer drives it. A lost event adds
//! to the in-memory count, from the writer or concurrently from
//! `AuditHandle::emit`. `PersistLosses` writes the sidecar. The writer's
//! `persist_with` is one step per call it makes: take the snapshot, persist the
//! sidecar, append the marker (a spool record, subject to the spool's capacity),
//! sync it, commit in memory, and persist the commit. Only a concurrent loss or
//! a crash can fall between two of those steps, because the writer is serial.
//!
//! `superseded_settlement_loses_and_rereports_losses` keeps the settlement
//! `PendingLosses` applied before `settle_loss_batch` (the commit left a
//! remainder in the marker's generation, and reconcile zeroed the count) and
//! records the counterexamples the checker finds for it.
//!
//! Bounds: three record IDs, two loss events, a two-record spool, sync cadence
//! two, at most two crashes, and depth 64. Properties require every admitted
//! record to be delivered or durably pending, at-most-once automatic delivery,
//! and exact loss accounting: every loss is reported by exactly one durable
//! marker, still pending, or was held only in memory at a crash. Reachability
//! witnesses cover torn append recovery, definite replay retry, uncertain
//! replay poison, committed-poison cleanup, a marker refused by a full spool,
//! loss-marker reconciliation, and a concurrent loss carried past a marker.

use krabka_verified::{
    audit::{AuditLosses as Losses, settle_loss_batch},
    spool_append_decision,
};
use stateright::{Checker, Model, Property};

use crate::spool::{ReplayRecovery, add_loss_state, replay_recovery};

const MAX_DEPTH: usize = 64;

const MAX_STATES: usize = 4_000_000;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// It grew from 11,184 when the loss path became the production sequence of
// steps, the marker a spool record, and a third record id was added so that a
// marker competes with records for capacity.
const PINNED_UNIQUE_STATES: usize = 71_351;

const MAX_RECORDS: u8 = 3;

const MAX_LOSSES: u8 = 2;

const MAX_BYTES: u64 = 2;

const SYNC_EVERY: u64 = 2;

const SAW_TORN_APPEND: u8 = 1;

const SAW_RETRY: u8 = 1 << 1;

const SAW_UNCERTAIN_POISON: u8 = 1 << 2;

const SAW_COMMITTED_POISON: u8 = 1 << 3;

const SAW_LOSS_RECONCILE: u8 = 1 << 4;

const SAW_MARKER_REFUSED: u8 = 1 << 5;

const SAW_REMAINDER_CARRIED: u8 = 1 << 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Runtime {
    Open,
    Closed,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ReplayPhase {
    Idle,
    Poisoned { record: u8, offset: u8 },
    Delivered { record: u8, offset: u8 },
    CursorCommitted { offset: u8 },
}

/// Where the writer's `persist_with` call is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum MarkerFlow {
    Idle,
    /// `snapshot` returned `batch`.
    Snapshotted {
        batch: Losses,
    },
    /// `persist` wrote the sidecar.
    Persisted {
        batch: Losses,
    },
    /// `append_loss_marker` appended the marker as a spool record.
    Appended {
        batch: Losses,
    },
    /// `append_loss_marker` synced the spool.
    Synced {
        batch: Losses,
    },
    /// `commit` settled the in-memory count and has yet to persist it.
    Committed {
        batch: Losses,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SpoolState {
    runtime: Runtime,
    next_record: u8,
    durable_history: u8,
    volatile: u8,
    durable: u8,
    deliveries: [u8; MAX_RECORDS as usize],
    cursor: u8,
    unsynced: u64,
    replay: ReplayPhase,
    crashes: u8,
    /// Ghost: every loss event so far.
    loss_events: u8,
    /// `PendingLosses`'s in-memory state.
    memory: Losses,
    /// The `audit.losses` sidecar.
    sidecar: Losses,
    /// Ghost: losses added to memory since the last sidecar write.
    unpersisted: u8,
    /// Ghost: losses that only memory held when the process crashed.
    forgotten_at_crash: u8,
    flow: MarkerFlow,
    /// The loss marker each spool record carries, if it is one.
    markers: [Option<Losses>; MAX_RECORDS as usize],
    /// Ghost: losses taken off the pending count because a durable marker
    /// reports them, credited when that settlement is durable (the commit's
    /// sidecar write, or open-time reconciliation).
    accounted: u64,
    witnesses: u8,
}

impl SpoolState {
    /// Ghost: the losses that durable markers report, whether or not they
    /// have been replayed yet.
    fn reported(&self) -> u64 {
        self.markers
            .iter()
            .enumerate()
            .filter(|&(record, _)| self.durable_history & (1 << record) != 0)
            .filter_map(|(_, marker)| marker.map(|m| m.count))
            .sum()
    }

    fn durable_markers(&self) -> impl Iterator<Item = Losses> + '_ {
        self.markers
            .iter()
            .enumerate()
            .filter(|&(record, _)| self.durable_history & (1 << record) != 0)
            .filter_map(|(_, marker)| *marker)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Action {
    Append,
    Sync,
    TearAppend,
    BeginReplay,
    Deliver,
    DefiniteFailure,
    CommitCursor,
    ClearPoison,
    Crash,
    Reopen,
    Lose,
    PersistLosses,
    SnapshotLosses,
    PersistSnapshot,
    AppendLossMarker,
    SyncLossMarker,
    CommitLossMarker,
    PersistCommit,
}

/// The two settlement functions a model run applies.
#[derive(Clone, Copy)]
struct SpoolModel {
    commit: fn(Losses, Losses) -> Losses,
    reconcile: fn(Losses, Losses) -> Losses,
}

impl SpoolModel {
    const SETTLE: Self = Self {
        commit: settle_loss_batch,
        reconcile: settle_loss_batch,
    };
}

#[path = "spool_model/helpers.rs"]
mod helpers;
use helpers::{bit, persist_sidecar, spool_append, superseded_commit, superseded_reconcile, sync};

#[path = "spool_model/checker.rs"]
mod checker;

#[path = "spool_model/transitions.rs"]
mod transitions;

#[path = "spool_model/checks.rs"]
mod checks;
use checks::check;

#[cfg(test)]
#[path = "spool_model/tests.rs"]
mod tests;
