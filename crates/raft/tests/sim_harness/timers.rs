//! The harness's timer vocabulary: the timer kinds it schedules, the staggered
//! election timeout each node is configured with, and the earliest-deadline
//! comparison the scheduler picks with. Determinism depends on all three, so
//! they stay together.

use krabka_raft::kraft::types::{NodeId, SimInstant};
use krabka_units::prelude::{Time, TimeExt as _};

/// Harness-level timer kinds. This extends the core's `TimerKind`, which is
/// Election and Fetch, with the leader `Heartbeat` that the core does not model
/// on a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SimTimer {
    Election,
    Fetch,
    Heartbeat,
}

/// Leader heartbeat period. It stays well below the election timeout, so a
/// healthy leader's re-announcements always reach the voters before any
/// watchdog escalates.
pub(super) const HEARTBEAT_MS: u64 = 300;

/// The base election timeout, which is also the fetch watchdog period,
/// configured for node `id`. It is staggered by node id, so timer ties break
/// deterministically and the lowest live id tends to win the election race.
/// Elections therefore always converge.
pub(super) fn election_timeout_ms_of(id: NodeId) -> u64 {
    1000 + id.0 * 50
}

/// [`election_timeout_ms_of`] as the quantity [`QuorumStateMachine::new`] takes.
/// The simulation's own clock stays in integer logical milliseconds, because a
/// [`SimInstant`] is a coordinate and not an extent. This conversion therefore
/// happens only at the core's constructor.
pub(super) fn election_timeout_of(id: NodeId) -> Time {
    Time::from_millis(i64::try_from(election_timeout_ms_of(id)).unwrap_or(i64::MAX))
}

/// Updates `best` to the earliest `(deadline, id, kind)` seen so far. An earlier
/// deadline wins. On a tie the smaller node id wins. Callers iterate the ids in
/// ascending order, so this keeps the choice deterministic.
pub(super) fn consider(
    best: &mut Option<(SimInstant, NodeId, SimTimer)>,
    deadline: SimInstant,
    id: NodeId,
    kind: SimTimer,
) {
    match best {
        Some((bd, _, _)) if *bd <= deadline => {}
        _ => *best = Some((deadline, id, kind)),
    }
}
