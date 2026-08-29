//! The assignor's input and output data types, together with the small helpers
//! that read and normalise a working `member -> tasks` role map.
//!
//! [`AssignorMember`] and [`AssignorInput`] are what the coordinator actor
//! hands the assignor, and [`StreamsAssignment`] is what it gets back. The
//! placement steps work on the looser `member -> Vec<Task>` form instead, so
//! [`to_role_maps`] converts that back into the public shape.

use std::collections::{BTreeMap, BTreeSet, HashMap};

/// One group member as the assignor sees it.
///
/// It carries the member's current ownership, which drives stickiness, and its
/// reported per-task changelog lag, which decides warmup catch-up.
#[derive(Debug, Clone)]
pub struct AssignorMember {
    pub member_id: String,
    /// Process the member runs in. Standby placement spreads copies across
    /// separate processes for fault tolerance.
    pub process_id: String,
    pub rack_id: Option<String>,
    /// Active tasks the member currently owns, as
    /// `subtopology_id -> partitions`. The assignor reads them for
    /// stickiness.
    pub current_active: BTreeMap<String, Vec<i32>>,
    /// Standby tasks the member currently owns.
    pub current_standby: BTreeMap<String, Vec<i32>>,
    /// Warmup tasks the member currently owns.
    pub current_warmup: BTreeMap<String, Vec<i32>>,
    /// Reported changelog lag for each task: `(subtopology, partition) -> lag`,
    /// where the lag is `end - position`. An absent entry means the lag is
    /// unknown and the task is not caught up.
    pub task_lag: BTreeMap<(String, i32), i64>,
}

/// Inputs to one assignment computation.
///
/// They hold the task universe, the set of stateful subtopologies, and the
/// placement settings: the standby and warmup counts, the acceptable recovery
/// lag, and the assignor kind.
#[derive(Debug, Clone)]
pub struct AssignorInput {
    /// The full task universe: `subtopology_id -> ALL partitions`.
    pub tasks: BTreeMap<String, Vec<i32>>,
    /// Subtopology ids that have a changelog, that is, the stateful ones.
    pub stateful: BTreeSet<String>,
    /// `num.standby.replicas`: standby copies per stateful task.
    pub num_standby_replicas: i32,
    /// `max.warmup.replicas`: the global cap on the warmup tasks this
    /// assignment creates at one time.
    pub num_warmup_replicas: i32,
    /// `acceptable.recovery.lag`: the maximum changelog lag at which the
    /// assignor treats a warmup target as caught up and allows the active move
    /// immediately.
    pub acceptable_recovery_lag: i64,
    /// Server-side assignor selection.
    pub kind: StreamsAssignorKind,
}

/// The computed target assignment: per-member task maps for each role.
///
/// A member with no tasks in a role has no entry in that role's map.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StreamsAssignment {
    /// `member_id -> active tasks`.
    pub active: HashMap<String, BTreeMap<String, Vec<i32>>>,
    /// `member_id -> standby tasks`.
    pub standby: HashMap<String, BTreeMap<String, Vec<i32>>>,
    /// `member_id -> warmup tasks`.
    pub warmup: HashMap<String, BTreeMap<String, Vec<i32>>>,
}

/// A `(subtopology_id, partition)` task in its canonical ordered form.
pub(super) type Task = (String, i32);

/// Reports whether a role map contains `task`.
pub(super) fn owns(role: &BTreeMap<String, Vec<i32>>, task: &Task) -> bool {
    role.get(&task.0)
        .is_some_and(|parts| parts.contains(&task.1))
}

/// Reports whether `member` holds `task` in the given role map.
pub(super) fn owns_role(role: &HashMap<String, Vec<Task>>, member: &str, task: &Task) -> bool {
    role.get(member).is_some_and(|ts| ts.contains(task))
}

/// Converts a `member -> Vec<Task>` working map into the public
/// `member -> (subtopology -> partitions)` form.
///
/// The function normalises every task map: it sorts and dedups the partitions
/// and drops the empty subtopology entries. It also drops members that have no
/// tasks in the role.
pub(super) fn to_role_maps(
    by_member: &HashMap<String, Vec<Task>>,
) -> HashMap<String, BTreeMap<String, Vec<i32>>> {
    let mut out: HashMap<String, BTreeMap<String, Vec<i32>>> = HashMap::new();
    for (member, ts) in by_member {
        if ts.is_empty() {
            continue;
        }
        let mut role: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for (sub, part) in ts {
            role.entry(sub.clone()).or_default().push(*part);
        }
        for parts in role.values_mut() {
            parts.sort_unstable();
            parts.dedup();
        }
        role.retain(|_, parts| !parts.is_empty());
        if !role.is_empty() {
            out.insert(member.clone(), role);
        }
    }
    out
}
