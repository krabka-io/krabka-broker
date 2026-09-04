//! Bounded Stateright model of audit-spool append, replay, crash recovery, and
//! loss accounting.
//!
//! DRIVEN: `Append` calls the production `spool_append_decision` kernel used by
//! `Spool::append`. `Reopen` calls the production `replay_recovery` classifier
//! used by `Spool::open`. `Lose` calls the production `add_loss_state` helper
//! used by `PendingLosses::add`.
//!
//! MODELED: filesystem writes are split into volatile and durable records;
//! `Sync` publishes all completed appends; `Crash` drops volatile and torn
//! writes; replay poison, sink delivery, cursor persistence, and poison removal
//! are separate actions. A poison at the current cursor makes `Reopen` stop for
//! explicit recovery. A poison behind the cursor is cleared. Loss sidecars and
//! marker appends are separate actions, including a crash after the marker is
//! durable but before the sidecar is cleared.
//!
//! Bounds: two record IDs, two loss events, a two-record spool, sync cadence
//! two, at most two crashes, and depth 48. Properties require every admitted
//! record to be delivered or durably pending, at-most-once automatic delivery,
//! monotonic loss generations, and complete loss accounting. Reachability
//! witnesses cover torn append recovery, definite replay retry, uncertain
//! replay poison, committed-poison cleanup, and loss-marker reconciliation.

use krabka_verified::spool_append_decision;
use stateright::{Checker, Model, Property};

use crate::spool::{ReplayRecovery, add_loss_state, replay_recovery};

const MAX_DEPTH: usize = 48;
const MAX_STATES: usize = 500_000;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 11_184;
const MAX_RECORDS: u8 = 2;
const MAX_LOSSES: u8 = 2;
const MAX_BYTES: u64 = 2;
const SYNC_EVERY: u64 = 2;
const SAW_TORN_APPEND: u8 = 1;
const SAW_RETRY: u8 = 1 << 1;
const SAW_UNCERTAIN_POISON: u8 = 1 << 2;
const SAW_COMMITTED_POISON: u8 = 1 << 3;
const SAW_LOSS_RECONCILE: u8 = 1 << 4;

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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SpoolState {
    runtime: Runtime,
    next_record: u8,
    durable_history: u8,
    volatile: u8,
    durable: u8,
    deliveries: [u8; 2],
    cursor: u8,
    unsynced: u64,
    replay: ReplayPhase,
    crashes: u8,
    loss_events: u8,
    loss_generation: u64,
    pending_losses: u64,
    persisted_losses: u64,
    marker_generation: u64,
    marker_losses: u64,
    accounted_losses: u64,
    witnesses: u8,
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
    AppendLossMarker,
    CommitLossMarker,
}

struct SpoolModel;

fn bit(record: u8) -> u8 {
    1 << record
}

fn pending_mask(state: &SpoolState) -> u8 {
    state.volatile | state.durable
}

impl Model for SpoolModel {
    type State = SpoolState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![SpoolState {
            runtime: Runtime::Open,
            next_record: 0,
            durable_history: 0,
            volatile: 0,
            durable: 0,
            deliveries: [0; 2],
            cursor: 0,
            unsynced: 0,
            replay: ReplayPhase::Idle,
            crashes: 0,
            loss_events: 0,
            loss_generation: 0,
            pending_losses: 0,
            persisted_losses: 0,
            marker_generation: 0,
            marker_losses: 0,
            accounted_losses: 0,
            witnesses: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.runtime == Runtime::Stopped {
            return;
        }
        if state.runtime == Runtime::Closed {
            actions.push(Action::Reopen);
            return;
        }
        if state.next_record < MAX_RECORDS {
            actions.push(Action::Append);
            actions.push(Action::TearAppend);
        }
        if state.volatile != 0 {
            actions.push(Action::Sync);
        }
        match state.replay {
            ReplayPhase::Idle if state.durable != 0 => actions.push(Action::BeginReplay),
            ReplayPhase::Poisoned { .. } => {
                actions.push(Action::Deliver);
                actions.push(Action::DefiniteFailure);
            }
            ReplayPhase::Delivered { .. } => actions.push(Action::CommitCursor),
            ReplayPhase::CursorCommitted { .. } => actions.push(Action::ClearPoison),
            ReplayPhase::Idle => {}
        }
        if state.crashes < 2 {
            actions.push(Action::Crash);
        }
        if state.loss_events < MAX_LOSSES {
            actions.push(Action::Lose);
        }
        if state.pending_losses > state.persisted_losses {
            actions.push(Action::PersistLosses);
        }
        if state.persisted_losses > 0 && state.marker_losses == 0 {
            actions.push(Action::AppendLossMarker);
        }
        if state.marker_losses > 0 && state.pending_losses > 0 {
            actions.push(Action::CommitLossMarker);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Append => {
                let record = bit(state.next_record);
                let decision = spool_append_decision(
                    u64::from(pending_mask(&state).count_ones()),
                    1,
                    MAX_BYTES,
                    state.unsynced,
                    SYNC_EVERY,
                );
                if !decision.accepted {
                    return None;
                }
                state.next_record += 1;
                if decision.sync {
                    state.durable |= state.volatile | record;
                    state.durable_history |= state.volatile | record;
                    state.volatile = 0;
                } else {
                    state.volatile |= record;
                }
                state.unsynced = decision.next_unsynced;
            }
            Action::Sync => {
                state.durable |= state.volatile;
                state.durable_history |= state.volatile;
                state.volatile = 0;
                state.unsynced = 0;
            }
            Action::TearAppend => {
                state.next_record += 1;
                state.witnesses |= SAW_TORN_APPEND;
            }
            Action::BeginReplay => {
                let record = u8::try_from(state.durable.trailing_zeros()).unwrap_or(u8::MAX);
                state.replay = ReplayPhase::Poisoned {
                    record,
                    offset: state.cursor,
                };
            }
            Action::Deliver => {
                let ReplayPhase::Poisoned { record, offset } = state.replay else {
                    return None;
                };
                state.deliveries[usize::from(record)] += 1;
                state.replay = ReplayPhase::Delivered { record, offset };
            }
            Action::DefiniteFailure => {
                if !matches!(state.replay, ReplayPhase::Poisoned { .. }) {
                    return None;
                }
                state.replay = ReplayPhase::Idle;
                state.witnesses |= SAW_RETRY;
            }
            Action::CommitCursor => {
                let ReplayPhase::Delivered { record, offset } = state.replay else {
                    return None;
                };
                state.durable &= !bit(record);
                state.cursor += 1;
                state.replay = ReplayPhase::CursorCommitted { offset };
            }
            Action::ClearPoison => state.replay = ReplayPhase::Idle,
            Action::Crash => {
                state.volatile = 0;
                state.unsynced = 0;
                state.crashes += 1;
                state.runtime = Runtime::Closed;
            }
            Action::Reopen => {
                state.runtime = Runtime::Open;
                return reopen(state);
            }
            Action::Lose => {
                (state.loss_generation, state.pending_losses) =
                    add_loss_state(state.loss_generation, state.pending_losses, 1);
                state.loss_events += 1;
            }
            Action::PersistLosses => state.persisted_losses = state.pending_losses,
            Action::AppendLossMarker => {
                state.marker_generation = state.loss_generation;
                state.marker_losses = state.persisted_losses;
                state.accounted_losses = state
                    .accounted_losses
                    .saturating_add(state.persisted_losses);
            }
            Action::CommitLossMarker => {
                if state.marker_generation == state.loss_generation {
                    state.pending_losses = state.pending_losses.saturating_sub(state.marker_losses);
                    state.persisted_losses = state.pending_losses;
                }
                state.marker_losses = 0;
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("delivered_or_durably_pending", |_, state: &SpoolState| {
                let delivered = state
                    .deliveries
                    .iter()
                    .enumerate()
                    .fold(0_u8, |mask, (record, count)| {
                        mask | u8::from(*count > 0) << record
                    });
                state.durable_history & !(delivered | state.durable) == 0
            }),
            Property::always(
                "automatic_delivery_at_most_once",
                |_, state: &SpoolState| state.deliveries.iter().all(|count| *count <= 1),
            ),
            Property::always("loss_generation_monotonic", |_, state: &SpoolState| {
                state.marker_generation <= state.loss_generation
            }),
            Property::always("losses_accounted", |_, state: &SpoolState| {
                u64::from(state.loss_events)
                    <= state.accounted_losses.saturating_add(state.pending_losses)
            }),
            Property::sometimes("torn_append_recovered", |_, state: &SpoolState| {
                state.witnesses & SAW_TORN_APPEND != 0 && state.crashes > 0
            }),
            Property::sometimes("definite_failure_can_retry", |_, state: &SpoolState| {
                state.witnesses & SAW_RETRY != 0
            }),
            Property::sometimes("uncertain_delivery_stops", |_, state: &SpoolState| {
                state.witnesses & SAW_UNCERTAIN_POISON != 0 && state.runtime == Runtime::Stopped
            }),
            Property::sometimes("committed_poison_clears", |_, state: &SpoolState| {
                state.witnesses & SAW_COMMITTED_POISON != 0
                    && matches!(state.replay, ReplayPhase::Idle)
            }),
            Property::sometimes("durable_loss_marker_reconciles", |_, state: &SpoolState| {
                state.witnesses & SAW_LOSS_RECONCILE != 0
            }),
        ]
    }
}

fn reopen(mut state: SpoolState) -> Option<SpoolState> {
    match state.replay {
        ReplayPhase::Idle => {}
        ReplayPhase::Poisoned { offset, .. } | ReplayPhase::Delivered { offset, .. } => {
            let decision = replay_recovery(true, Some(u64::from(offset)), u64::from(state.cursor));
            if decision == ReplayRecovery::RequireExplicitRecovery {
                state.runtime = Runtime::Stopped;
                state.witnesses |= SAW_UNCERTAIN_POISON;
            }
        }
        ReplayPhase::CursorCommitted { offset } => {
            let decision = replay_recovery(true, Some(u64::from(offset)), u64::from(state.cursor));
            if decision != ReplayRecovery::ClearPoison {
                return None;
            }
            state.replay = ReplayPhase::Idle;
            state.witnesses |= SAW_COMMITTED_POISON;
        }
    }
    if state.marker_losses > 0
        && state.marker_generation == state.loss_generation
        && state.pending_losses > 0
    {
        state.pending_losses = state.pending_losses.saturating_sub(state.marker_losses);
        state.persisted_losses = state.pending_losses;
        state.marker_losses = 0;
        state.witnesses |= SAW_LOSS_RECONCILE;
    }
    Some(state)
}

#[test]
fn audit_spool_crash_and_replay_interleavings() {
    let checker = SpoolModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[audit-spool] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    assert2::assert!(checker.state_count() < MAX_STATES);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

#[test]
fn loss_accounting_saturates_without_wrapping() {
    assert2::check!(add_loss_state(u64::MAX, 0, 1) == (u64::MAX, 1));
    assert2::check!(add_loss_state(7, u64::MAX, 1) == (7, u64::MAX));
}

#[test]
fn malformed_or_stale_replay_poison_requires_recovery() {
    assert2::check!(replay_recovery(false, Some(0), 1) == ReplayRecovery::RequireExplicitRecovery);
    assert2::check!(replay_recovery(true, None, 1) == ReplayRecovery::RequireExplicitRecovery);
    assert2::check!(replay_recovery(true, Some(1), 1) == ReplayRecovery::RequireExplicitRecovery);
    assert2::check!(replay_recovery(true, Some(0), 1) == ReplayRecovery::ClearPoison);
}
