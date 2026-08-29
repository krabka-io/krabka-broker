//! Step 1 of the assignment: the balanced active target, in which every task
//! gets exactly one owner.
//!
//! Placement is sticky first — a task stays with its current owner while that
//! owner is still a member — and the remaining tasks go to the least-loaded
//! member. A balancing pass then moves tasks off the most-loaded member until
//! the spread is at most one task.

use std::collections::HashMap;

use super::types::{AssignorMember, Task, owns};

/// Returns the member that currently owns `task` as active, if such a member
/// is still present.
pub(super) fn current_active_owner<'a>(
    members: &[&'a AssignorMember],
    task: &Task,
) -> Option<&'a str> {
    members
        .iter()
        .find(|m| owns(&m.current_active, task))
        .map(|m| m.member_id.as_str())
}

/// Step 1: computes the balanced active target, where each task gets exactly
/// one owner.
///
/// The function keeps a task on its current owner when that owner still
/// exists, places the rest on the least-loaded member, and then balances until
/// the difference between the maximum and minimum load is `<= 1`.
pub(super) fn assign_active(
    members: &[&AssignorMember],
    tasks: &[Task],
) -> HashMap<String, Vec<Task>> {
    let mut active: HashMap<String, Vec<Task>> = HashMap::new();
    for m in members {
        active.entry(m.member_id.clone()).or_default();
    }

    // Sticky placement first; collect orphans for least-loaded placement.
    let mut orphans: Vec<Task> = Vec::new();
    for task in tasks {
        if let Some(owner) = current_active_owner(members, task) {
            active
                .get_mut(owner)
                .expect("owner present")
                .push(task.clone());
        } else {
            orphans.push(task.clone());
        }
    }

    // Place orphans on the least-loaded member (tie-break: lexicographic id).
    for task in orphans {
        let target = least_loaded(members, &active);
        active.get_mut(&target).expect("member present").push(task);
    }

    // Balancing pass: while the spread exceeds 1, move one task from the
    // most-loaded member to the least-loaded. We move the lexicographically
    // largest task off the most-loaded member — simple and deterministic, and
    // since orphan placement already balanced perfectly, this only fires when
    // sticky placement skewed the load.
    while let Some((max_id, min_id)) = load_extremes(members, &active) {
        let max_load = active[&max_id].len();
        let min_load = active[&min_id].len();
        if max_load <= min_load + 1 {
            break;
        }
        let moved = {
            let from = active.get_mut(&max_id).expect("member present");
            // Largest task by (subtopology, partition) order.
            let idx = from
                .iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.cmp(b))
                .map(|(i, _)| i)
                .expect("most-loaded member is non-empty");
            from.swap_remove(idx)
        };
        active.get_mut(&min_id).expect("member present").push(moved);
    }

    active
}

/// Returns the member id with the fewest active tasks. A tie breaks on the
/// lexicographic id.
fn least_loaded(members: &[&AssignorMember], active: &HashMap<String, Vec<Task>>) -> String {
    members
        .iter()
        .min_by(|a, b| {
            let la = active[&a.member_id].len();
            let lb = active[&b.member_id].len();
            la.cmp(&lb).then_with(|| a.member_id.cmp(&b.member_id))
        })
        .map(|m| m.member_id.clone())
        .expect("members non-empty")
}

/// Returns `(most_loaded_id, least_loaded_id)` by active load, each with a
/// deterministic tie-break. Returns `None` when there are no members.
fn load_extremes(
    members: &[&AssignorMember],
    active: &HashMap<String, Vec<Task>>,
) -> Option<(String, String)> {
    let max = members
        .iter()
        .max_by(|a, b| {
            let la = active[&a.member_id].len();
            let lb = active[&b.member_id].len();
            // On a load tie, prefer the lexicographically *larger* id as the
            // donor so the pairing with `least_loaded` (which prefers smaller)
            // is stable.
            la.cmp(&lb).then_with(|| a.member_id.cmp(&b.member_id))
        })
        .map(|m| m.member_id.clone())?;
    let min = least_loaded(members, active);
    Some((max, min))
}
