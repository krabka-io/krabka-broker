//! Exhaustive stateright model of the pure KIP-932 share-partition acquisition
//! core (`AcquisitionState`).
//!
//! The model state holds the REAL `AcquisitionState` and drives the production
//! `materialize`, `acquire`, `acknowledge`, `renew`, `expire_locks`,
//! `defer_internal`, `promote_deferred`, `to_persist_batches`, and
//! `load_from`. The BFS checker explores every interleaving of consumer
//! operations, time advance, KFC-1 deferral, and, in the failover config,
//! leader-reload. It asserts that the share-group delivery-safety invariants
//! never break. Design:
//! `docs/superpowers/specs/2026-06-13-krabka-share-group-model-design.md`.
//!
//! The model does not carry delivery times. It defers an arbitrary offset at
//! an arbitrary point instead, which covers every deferral a log and a clock
//! could produce and many they could not. That over-approximation is the point:
//! the safety claims must hold for any of them.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary`, `target_state_count`, and
//! `timeout`. While bounds are tuned, every run MUST execute under the host
//! memory watchdog. Never run one unguarded, because a runaway space exhausts
//! host RAM.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_log::Offset;
use stateright::{Checker, Model, Property};

use super::{AckType, AcquisitionState, RecordState};

/// The single acquisition-lock duration used by the model. A lock taken at
/// logical time `clock` has deadline `t0 + LOCK*(clock + 1)`, so it expires once
/// the clock reaches `clock + 1`.
const LOCK: Duration = Duration::from_secs(1);

/// Hard backstop on generated states. It bounds host memory even if
/// `within_boundary` is looser than intended. Set it well above each config's
/// true bounded count, so a real exhaustive run never truncates.
const MAX_STATES: usize = 200_000;
/// Depth backstop. It must exceed each config's reachable-graph diameter.
/// Otherwise the search is depth-truncated and incomplete, and the `run`
/// harness fails.
const MAX_DEPTH: usize = 80;
/// Wall-clock backstop.
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

/// Bounded model config. It lives here, not in the fingerprinted state.
struct ShareModel {
    /// Base instant. All `now` values are `t0 + LOCK*clock`. The model captures
    /// it once per run, so deadlines come from a finite, hashable set.
    t0: Instant,
    /// Number of consumer members (named `m0`..`m{members-1}`).
    members: u8,
    /// High-watermark and window cap: records produced over a path.
    max_offset: Offset,
    /// Logical-clock cap.
    max_tick: u8,
    /// Delivery-attempt limit before a record is archived as a poison pill.
    max_attempts: i16,
    /// Max records `materialize` pulls into the window at once.
    max_inflight: i32,
    /// Whether the model generates the leader-failover `Reload` action
    /// (Task 3).
    allow_reload: bool,
    /// Whether the model generates the KFC-1 `Defer` and `PromoteDeferred`
    /// actions.
    allow_defer: bool,
}

/// The fingerprinted model state. It holds the REAL machine plus the small
/// finite clock and the produced-record high-watermark.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ShareState {
    sm: AcquisitionState,
    clock: u8,
    hwm: Offset,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum ShareAction {
    /// Append one record to the log (raise the produced high-watermark).
    Produce,
    /// Leader pulls produced-but-unmaterialized records into the window.
    Materialize,
    /// `member` acquires up to `max_records` Available records.
    Acquire { member: u8, max_records: i32 },
    /// `member` acknowledges `[first, last]` it holds.
    Acknowledge {
        member: u8,
        first: Offset,
        last: Offset,
        ack: AckType,
    },
    /// `member` renews, that is, extends, the lock on `[first, last]` it holds.
    Renew {
        member: u8,
        first: Offset,
        last: Offset,
    },
    /// KFC-1: hold `[first, last]` back because its delivery time has not
    /// arrived.
    Defer { first: Offset, last: Offset },
    /// KFC-1: drop the whole deferral, as an acquire pass does before it
    /// re-derives one from the log and the clock.
    PromoteDeferred,
    /// Sweep expired acquisition locks back to Available.
    ExpireLocks,
    /// Advance the logical clock by one lock-duration.
    Tick,
    /// Leader failover: persist and reload. Acquired drops to Available, and
    /// the locks are lost.
    Reload,
}

impl ShareModel {
    /// Concurrency config: the full action set EXCEPT `Reload`. Bounds start
    /// small, at a proven memory-safe size. Task 4 scales `max_offset`
    /// empirically.
    fn concurrency(max_offset: i64, max_inflight: i32) -> Self {
        Self {
            t0: Instant::now(),
            members: 2,
            max_offset: Offset(max_offset),
            max_tick: 2,
            max_attempts: 2,
            max_inflight,
            allow_reload: false,
            allow_defer: false,
        }
    }

    /// Failover config: it adds `Reload` over a small window. It focuses on the
    /// `acknowledged_is_terminal` durability invariant across crash-recovery.
    fn failover() -> Self {
        Self {
            t0: Instant::now(),
            members: 2,
            max_offset: Offset(2),
            max_tick: 2,
            max_attempts: 2,
            max_inflight: 2,
            allow_reload: true,
            allow_defer: false,
        }
    }

    /// Deferral-across-failover config: the failover config plus KFC-1 `Defer`
    /// and `PromoteDeferred`. It is what checks the persist-and-reload round
    /// trip over a deferred window.
    ///
    /// The deferral coordinate is a subset of the Available offsets, so it
    /// roughly doubles the reachable space per offset in the window. Two
    /// offsets is the smallest window that still lets a due record sit behind a
    /// waiting one, and it is as wide as this config goes: three offsets with
    /// `Reload` generates 205016 states, which is past `MAX_STATES` and so
    /// proves nothing. [`ShareModel::deferral_wide`] takes the wider window
    /// instead, and pays for it elsewhere.
    fn deferral() -> Self {
        Self {
            allow_defer: true,
            ..Self::failover()
        }
    }

    /// Wide deferral config: three offsets, so a due record can sit two behind
    /// a waiting one and a deferral can span a range rather than a single
    /// offset.
    ///
    /// It buys that width with one member and no `Reload`. Both are covered
    /// elsewhere: the concurrency configs hold the two-member interleavings,
    /// and [`ShareModel::deferral`] holds the reload. Keeping either here puts
    /// the generated count within 300 states of `MAX_STATES`, which is not a
    /// bound anyone can build on.
    fn deferral_wide() -> Self {
        Self {
            t0: Instant::now(),
            members: 1,
            max_offset: Offset(3),
            max_tick: 2,
            max_attempts: 2,
            max_inflight: 3,
            allow_reload: false,
            allow_defer: true,
        }
    }

    fn now(&self, clock: u8) -> Instant {
        self.t0 + LOCK * u32::from(clock)
    }

    fn member_name(member: u8) -> String {
        format!("m{member}")
    }
}

// ---- observability helpers (descendant-module private access) --------------

/// Delivery state of `off`, if it currently lies in a batch.
fn offset_state(sm: &AcquisitionState, off: Offset) -> Option<RecordState> {
    sm.batches
        .iter()
        .find(|b| b.first_offset <= off && off <= b.last_offset)
        .map(|b| b.state)
}

/// Delivery count of `off`, if it currently lies in a batch.
fn offset_dc(sm: &AcquisitionState, off: Offset) -> Option<i16> {
    sm.batches
        .iter()
        .find(|b| b.first_offset <= off && off <= b.last_offset)
        .map(|b| b.delivery_count)
}

/// Every offset in the window that the schedule currently holds back.
fn deferred_offsets(sm: &AcquisitionState) -> Vec<Offset> {
    sm.batches
        .iter()
        .filter(|b| b.state == RecordState::Deferred)
        .flat_map(|b| (b.first_offset.0..=b.last_offset.0).map(Offset))
        .collect()
}

/// Maximal contiguous offset runs currently Acquired by `member`. Adjacent
/// same-owner batches with different lock deadlines do not coalesce, so this
/// function stitches them back into one run. The whole run is then
/// ack-able and renew-able at once.
fn acquired_runs(sm: &AcquisitionState, member: &str) -> Vec<(Offset, Offset)> {
    let mut runs: Vec<(Offset, Offset)> = Vec::new();
    let mut cur: Option<(Offset, Offset)> = None;
    for b in &sm.batches {
        let mine = b.state == RecordState::Acquired && b.acquired_by.as_deref() == Some(member);
        match (mine, cur) {
            (true, Some((f, l))) if b.first_offset == l + 1 => cur = Some((f, b.last_offset)),
            (true, Some((f, l))) => {
                runs.push((f, l));
                cur = Some((b.first_offset, b.last_offset));
            }
            (true, None) => cur = Some((b.first_offset, b.last_offset)),
            (false, Some((f, l))) => {
                runs.push((f, l));
                cur = None;
            }
            (false, None) => {}
        }
    }
    if let Some((f, l)) = cur {
        runs.push((f, l));
    }
    runs
}

// ---- state-level invariants (Property::always predicates) ------------------

/// Batches are sorted, gap-free, non-overlapping, and exactly cover
/// `[start_offset, end_offset)`. Also, `start_offset <= end_offset`.
fn window_integrity(sm: &AcquisitionState) -> bool {
    if sm.start_offset > sm.end_offset {
        return false;
    }
    if sm.batches.is_empty() {
        return sm.start_offset == sm.end_offset;
    }
    if sm.batches[0].first_offset != sm.start_offset {
        return false;
    }
    for w in sm.batches.windows(2) {
        if w[0].first_offset > w[0].last_offset || w[0].last_offset + 1 != w[1].first_offset {
            return false;
        }
    }
    let last = sm.batches.last().expect("non-empty checked above");
    last.first_offset <= last.last_offset && last.last_offset + 1 == sm.end_offset
}

/// Every Acquired batch carries exactly one owner. With
/// `window_integrity`'s non-overlap, no offset is concurrently held by two
/// members. That is the main share-group guarantee.
fn mutual_exclusion(sm: &AcquisitionState) -> bool {
    sm.batches
        .iter()
        .all(|b| b.state != RecordState::Acquired || b.acquired_by.is_some())
}

/// Lock bookkeeping matches the delivery state. Acquired ⇒ both owner and
/// deadline present. Every other state ⇒ neither present.
fn lock_consistency(sm: &AcquisitionState) -> bool {
    sm.batches.iter().all(|b| match b.state {
        RecordState::Acquired => b.acquired_by.is_some() && b.lock_deadline.is_some(),
        _ => b.acquired_by.is_none() && b.lock_deadline.is_none(),
    })
}

// ---- transition-level invariants (asserted in next_state) ------------------

/// Compare a parent machine to its child after one operation, and panic on any
/// monotonicity or durability violation. This stays OUT of the fingerprinted
/// state, so no path-history ghost can explode the space. That was the Phase-1
/// OOM lesson.
fn assert_transition(parent: &AcquisitionState, child: &AcquisitionState, action: ShareAction) {
    // KFC-1: `promote_deferred` is the only route out of `Deferred`. A leader
    // reload does write the record back as `Available`, but the new leader
    // re-derives the deferral before the state is readable again, so the
    // deferred set is unchanged across that transition too.
    if action != ShareAction::PromoteDeferred {
        for raw in parent.start_offset.0..parent.end_offset.0 {
            let off = Offset(raw);
            if offset_state(parent, off) == Some(RecordState::Deferred) {
                assert!(
                    offset_state(child, off) == Some(RecordState::Deferred),
                    "deferred offset {off} left Deferred on {action:?}"
                );
            }
        }
    }
    assert!(
        child.start_offset >= parent.start_offset,
        "SPSO regressed: {} -> {}",
        parent.start_offset,
        child.start_offset
    );
    assert!(
        child.delivery_complete_count >= parent.delivery_complete_count,
        "delivery_complete_count regressed: {} -> {}",
        parent.delivery_complete_count,
        child.delivery_complete_count
    );
    // Per-offset delivery_count never regresses for offsets live in both.
    for raw in child.start_offset.0..child.end_offset.0 {
        let off = Offset(raw);
        if let (Some(pc), Some(cc)) = (offset_dc(parent, off), offset_dc(child, off)) {
            assert!(
                cc >= pc,
                "delivery_count regressed at offset {off}: {pc} -> {cc}"
            );
        }
    }
    // An Acknowledged offset is terminal: in the child it is still Acknowledged
    // or has dropped below the (non-decreasing) SPSO — never resurrected.
    for raw in parent.start_offset.0..parent.end_offset.0 {
        let off = Offset(raw);
        if offset_state(parent, off) == Some(RecordState::Acknowledged) {
            match offset_state(child, off) {
                None => assert!(
                    off < child.start_offset,
                    "acknowledged offset {off} vanished while still in window"
                ),
                Some(s) => assert!(
                    s == RecordState::Acknowledged,
                    "acknowledged offset {off} reverted to {s:?}"
                ),
            }
        }
    }
}

impl Model for ShareModel {
    type State = ShareState;
    type Action = ShareAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ShareState {
            sm: AcquisitionState::new(Offset(0)),
            clock: 0,
            hwm: Offset(0),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let has_available = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Available);
        let has_acquired = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Acquired);

        if state.hwm < self.max_offset {
            actions.push(ShareAction::Produce);
        }
        // Materialize only when there are produced-but-unmaterialized records and
        // no Available batch remains (the real `materialize` no-ops otherwise).
        if state.sm.end_offset < state.hwm && !has_available {
            actions.push(ShareAction::Materialize);
        }
        if has_available {
            for member in 0..self.members {
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: 1,
                });
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: i32::MAX,
                });
            }
        }
        // Data-dependent: ack/renew only over ranges a member actually holds.
        for member in 0..self.members {
            let name = Self::member_name(member);
            for (first, last) in acquired_runs(&state.sm, &name) {
                for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                    actions.push(ShareAction::Acknowledge {
                        member,
                        first,
                        last,
                        ack,
                    });
                }
                actions.push(ShareAction::Renew {
                    member,
                    first,
                    last,
                });
                // A split (first half) exercises partial-ack / partial-renew.
                if last > first {
                    let mid = first + (last.0 - first.0) / 2;
                    for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                        actions.push(ShareAction::Acknowledge {
                            member,
                            first,
                            last: mid,
                            ack,
                        });
                    }
                    actions.push(ShareAction::Renew {
                        member,
                        first,
                        last: mid,
                    });
                }
            }
        }
        if self.allow_defer {
            // Every sub-range of the window that covers something the schedule
            // could still hold back. Ranges rather than single offsets, so the
            // model exercises the splits `defer_internal` makes at its edges.
            for first in state.sm.start_offset.0..state.sm.end_offset.0 {
                for last in first..state.sm.end_offset.0 {
                    let defers_something = (first..=last).any(|raw| {
                        offset_state(&state.sm, Offset(raw)) == Some(RecordState::Available)
                    });
                    if defers_something {
                        actions.push(ShareAction::Defer {
                            first: Offset(first),
                            last: Offset(last),
                        });
                    }
                }
            }
            if !deferred_offsets(&state.sm).is_empty() {
                actions.push(ShareAction::PromoteDeferred);
            }
        }
        if has_acquired {
            actions.push(ShareAction::ExpireLocks);
        }
        if state.clock < self.max_tick {
            actions.push(ShareAction::Tick);
        }
        if self.allow_reload && state.sm.end_offset > state.sm.start_offset {
            actions.push(ShareAction::Reload);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ShareAction::Produce => {
                if state.hwm >= self.max_offset {
                    return None;
                }
                state.hwm += 1;
            }
            ShareAction::Materialize => {
                let before = state.sm.end_offset;
                state.sm.materialize(state.hwm, self.max_inflight);
                if state.sm.end_offset == before {
                    return None; // no-op: nothing materialized
                }
            }
            ShareAction::Acquire {
                member,
                max_records,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                let deferred = deferred_offsets(&state.sm);
                let handed_out =
                    state
                        .sm
                        .acquire(&name, max_records, i32::MAX, now, LOCK, self.max_attempts);
                for range in &handed_out {
                    for raw in range.first.0..=range.last.0 {
                        assert!(
                            !deferred.contains(&Offset(raw)),
                            "acquire handed out deferred offset {raw}"
                        );
                    }
                }
            }
            ShareAction::Defer { first, last: hi } => {
                state.sm.defer_internal(first, hi);
                if state.sm == last.sm {
                    return None; // nothing in the range was Available
                }
            }
            ShareAction::PromoteDeferred => {
                state.sm.promote_deferred();
                if state.sm == last.sm {
                    return None; // nothing was deferred
                }
            }
            ShareAction::Acknowledge {
                member,
                first,
                last: hi,
                ack,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                if state.sm.acknowledge(&name, first, hi, ack, now).is_err() {
                    return None; // inapplicable ack: no transition
                }
            }
            ShareAction::Renew {
                member,
                first,
                last: hi,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                if state.sm.renew(&name, first, hi, now, LOCK).is_err() {
                    return None; // inapplicable renew: no transition
                }
            }
            ShareAction::ExpireLocks => {
                let now = self.now(state.clock);
                state.sm.expire_locks(now);
            }
            ShareAction::Tick => {
                if state.clock >= self.max_tick {
                    return None;
                }
                state.clock += 1;
            }
            ShareAction::Reload => {
                let deferred = deferred_offsets(&state.sm);
                let window = (state.sm.start_offset, state.sm.end_offset);
                let (start, dcc, batches) = state.sm.to_persist_batches();
                let mut fresh = AcquisitionState::new(start);
                fresh.load_from(
                    start,
                    state.sm.state_epoch,
                    state.sm.leader_epoch,
                    dcc,
                    &batches,
                );
                // KFC-1: `Deferred` persists as `Available`, so the new leader
                // re-derives it from the log and its own clock. The model's
                // clock has not moved, so the same offsets come back deferred,
                // and the round trip must lose none of them.
                for off in &deferred {
                    fresh.defer_internal(*off, *off);
                }
                assert!(
                    (fresh.start_offset, fresh.end_offset) == window,
                    "reload lost part of the window: {window:?} -> {:?}",
                    (fresh.start_offset, fresh.end_offset)
                );
                assert!(
                    deferred_offsets(&fresh) == deferred,
                    "reload lost the deferral: {deferred:?} -> {:?}",
                    deferred_offsets(&fresh)
                );
                state.sm = fresh;
            }
        }
        assert_transition(&last.sm, &state.sm, action);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("window_integrity", |_, s: &ShareState| {
                window_integrity(&s.sm)
            }),
            Property::always("mutual_exclusion", |_, s: &ShareState| {
                mutual_exclusion(&s.sm)
            }),
            Property::always("lock_consistency", |_, s: &ShareState| {
                lock_consistency(&s.sm)
            }),
            Property::always(
                "delivery_count_bounded",
                |m: &ShareModel, s: &ShareState| {
                    s.sm.batches
                        .iter()
                        .all(|b| b.delivery_count <= m.max_attempts)
                },
            ),
            Property::always("spso_in_range", |m: &ShareModel, s: &ShareState| {
                0 <= s.sm.start_offset
                    && s.sm.start_offset <= s.sm.end_offset
                    && s.sm.end_offset <= m.max_offset
            }),
            Property::sometimes("can_advance_spso", |_, s: &ShareState| {
                s.sm.start_offset > 0
            }),
            Property::sometimes("can_acknowledge", |_, s: &ShareState| {
                s.sm.batches
                    .iter()
                    .any(|b| b.state == RecordState::Acknowledged)
            }),
            Property::sometimes("can_archive", |_, s: &ShareState| {
                s.sm.batches
                    .iter()
                    .any(|b| b.state == RecordState::Archived)
            }),
            Property::sometimes("can_redeliver", |_, s: &ShareState| {
                s.sm.batches.iter().any(|b| b.delivery_count >= 2)
            }),
        ];
        if self.allow_defer {
            properties.push(Property::sometimes("can_defer", |_, s: &ShareState| {
                !deferred_offsets(&s.sm).is_empty()
            }));
            // The claim KFC-1 makes for share groups, and the one a classic
            // group cannot have: a record is handed out while a record below it
            // waits for its delivery time.
            properties.push(Property::sometimes(
                "can_deliver_behind_a_deferred_record",
                |_, s: &ShareState| {
                    deferred_offsets(&s.sm).first().is_some_and(|waiting| {
                        s.sm.batches
                            .iter()
                            .any(|b| b.state == RecordState::Acquired && b.first_offset > *waiting)
                    })
                },
            ));
        }
        properties
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound ONLY the design-unbounded dimensions (so the space is finite);
        // do NOT bound delivery_count — its <= max_attempts boundedness is a
        // property we verify, so pruning it would mask a violation. The 12-batch
        // cap is a loose structural safety net (real max over a <=3 window is 3).
        state.clock <= self.max_tick
            && state.hwm <= self.max_offset
            && state.sm.end_offset <= self.max_offset
            && state.sm.batches.len() <= 12
    }
}

/// Run one bounded config to completion. Assert that the run was exhaustive,
/// that is, that no cap truncated it, and that all properties hold.
fn run(model: ShareModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: search is depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: search is truncated, not exhaustive"
    );
    checker.assert_properties();
}

#[test]
fn share_concurrency_inflight_full() {
    // max_inflight large enough to pull the whole window in one materialize.
    run(
        ShareModel::concurrency(3, 3),
        "share_concurrency_inflight_full",
    );
}

#[test]
fn share_concurrency_inflight_one() {
    // max_inflight = 1: exercises drain-then-rematerialize across Produce steps.
    run(
        ShareModel::concurrency(3, 1),
        "share_concurrency_inflight_one",
    );
}

#[test]
fn share_failover() {
    // Adds leader-failover Reload; stresses acknowledged-is-terminal durability.
    run(ShareModel::failover(), "share_failover");
}

#[test]
fn share_deferral() {
    // Adds KFC-1 Defer/PromoteDeferred over the failover window, so the
    // deferral invariants are checked across a leader change as well.
    run(ShareModel::deferral(), "share_deferral");
}

#[test]
fn share_deferral_wide() {
    // Three offsets: a deferral can span a range, and a due record can sit two
    // behind a waiting one.
    run(ShareModel::deferral_wide(), "share_deferral_wide");
}
