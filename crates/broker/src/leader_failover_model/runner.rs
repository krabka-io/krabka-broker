//! The checker bounds and the two entry points the model tests call.
//!
//! stateright's BFS keeps every visited unique state resident, so the depth and
//! state-count fences live next to the assertions that prove a run was
//! exhaustive: a search that hit either cap proves nothing, and the two must
//! be tuned together.

use assert2::assert;
use stateright::{Checker, Model};

use super::{failover_state::FailoverModel, recovery_state::RecoveryModel};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
// The six `min.insync.replicas` 1 counts moved (from 105, 280, 105, 77, 140
// and 77) when the state gained `elr` and `elected_from_elr` and the search
// gained `FailoverAction::ExpandIsr`: a revived follower now rejoins the ISR,
// so ISR shapes past the first election are reachable, and the published ELR
// is non-empty whenever an ISR has emptied outright. The three ELR counts were
// new configurations then.
//
// All nine failover counts moved again (from 2,198 / 2,198 / 2,198 / 826 /
// 826 / 826 / 3,297 / 2,891 / 1,211) when `failover_one` took Kafka's
// `handleBrokerFenced` semantics, and every one shrank because fewer ISR
// shapes are reachable: a failover now removes only the dead broker from the
// ISR, where it used to drop every member that was down at once (that is how
// the reachable set included an empty ISR under a live leader, the
// `leader_in_isr` counterexample); a leader that is down is re-elected by
// whichever of its partition's failovers runs first; and a broker that is only
// a replica leaves the partition alone. The assignment also became `[1, 3, 2]`
// so that ISR order and assignment order can differ.
pub(super) const PINNED_UNIQUE_STATES_FAILOVER_SAFE: usize = 525;
pub(super) const PINNED_UNIQUE_STATES_FAILOVER_UNCLEAN: usize = 525;
pub(super) const PINNED_UNIQUE_STATES_FAILOVER_RECOVER: usize = 525;
pub(super) const PINNED_UNIQUE_STATES_WITNESS_SAFE: usize = 210;
pub(super) const PINNED_UNIQUE_STATES_WITNESS_UNCLEAN: usize = 210;
pub(super) const PINNED_UNIQUE_STATES_WITNESS_RECOVER: usize = 210;
pub(super) const PINNED_UNIQUE_STATES_OFFSET_RECOVERY: usize = 6_859;
pub(super) const PINNED_UNIQUE_STATES_ELR_UNCLEAN: usize = 1_470;
pub(super) const PINNED_UNIQUE_STATES_ELR_RECOVER: usize = 1_246;
pub(super) const PINNED_UNIQUE_STATES_WITNESS_ELR_UNCLEAN: usize = 532;

pub(super) fn run_failover(model: FailoverModel, label: &str, pinned_unique_states: usize) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
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
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    // Pin: a changed count is a changed model, not a retuning knob.
    assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

pub(super) fn run_recovery(model: RecoveryModel, label: &str, pinned_unique_states: usize) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
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
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    // Pin: a changed count is a changed model, not a retuning knob.
    assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
