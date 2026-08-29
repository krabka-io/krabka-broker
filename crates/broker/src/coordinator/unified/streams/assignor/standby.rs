//! Step 3 of the assignment: standby placement, which the `HighlyAvailable`
//! assignor runs and the `Sticky` one skips.
//!
//! A standby copy only helps if it survives the failure that takes the active
//! task down, so this step spreads the copies of one task across distinct
//! processes and prefers a rack other than the active owner's.

use std::collections::{BTreeSet, HashMap};

use super::types::{AssignorInput, AssignorMember, Task, owns_role};

/// Step 3: standby placement, for `HighlyAvailable` only.
///
/// For each stateful task, this step places up to `num_standby_replicas`
/// copies on members whose processes differ from the active owner's, from each
/// other's, and from that of any warmup holder of the task. It prefers a
/// different rack first, then the smallest standby load, then the
/// lexicographic id.
pub(super) fn assign_standby(
    members: &[&AssignorMember],
    input: &AssignorInput,
    tasks: &[Task],
    active: &HashMap<String, Vec<Task>>,
    warmup: &HashMap<String, Vec<Task>>,
    standby: &mut HashMap<String, Vec<Task>>,
) {
    if input.num_standby_replicas <= 0 {
        return;
    }

    // Reverse index: which member holds each task as active.
    let active_owner: HashMap<&Task, &str> = active
        .iter()
        .flat_map(|(member, ts)| ts.iter().map(move |t| (t, member.as_str())))
        .collect();

    let by_id: HashMap<&str, &AssignorMember> =
        members.iter().map(|m| (m.member_id.as_str(), *m)).collect();

    for task in tasks {
        if !input.stateful.contains(&task.0) {
            continue; // stateless tasks get no standby.
        }
        let Some(&owner_id) = active_owner.get(task) else {
            continue;
        };
        let Some(owner) = by_id.get(owner_id) else {
            continue;
        };
        let active_rack = owner.rack_id.as_deref();

        // Processes already excluded for this task: the active owner's.
        let mut used_processes: BTreeSet<&str> = BTreeSet::new();
        used_processes.insert(owner.process_id.as_str());

        for _ in 0..input.num_standby_replicas {
            let chosen = members
                .iter()
                .filter(|m| !used_processes.contains(m.process_id.as_str()))
                .filter(|m| !owns_role(warmup, &m.member_id, task))
                .min_by(|a, b| {
                    standby_rank(a, active_rack, standby).cmp(&standby_rank(
                        b,
                        active_rack,
                        standby,
                    ))
                });

            let Some(chosen) = chosen else {
                break; // no more distinct processes available.
            };
            used_processes.insert(chosen.process_id.as_str());
            standby
                .entry(chosen.member_id.clone())
                .or_default()
                .push(task.clone());
        }
    }
}

/// Ranking key for a standby candidate. It prefers a rack *different* from the
/// active owner's, then the fewest standby tasks so far, then the lexicographic
/// id. A smaller key is better.
fn standby_rank(
    m: &AssignorMember,
    active_rack: Option<&str>,
    standby: &HashMap<String, Vec<Task>>,
) -> (u8, usize, String) {
    // 0 = preferred (different rack, or no rack info to compare on); 1 = same
    // rack as the active owner.
    let rack_penalty = match (active_rack, m.rack_id.as_deref()) {
        (Some(a), Some(b)) if a == b => 1,
        _ => 0,
    };
    let load = standby.get(&m.member_id).map_or(0, Vec::len);
    (rack_penalty, load, m.member_id.clone())
}
