//! Step 2 of the assignment: warmup deferral, which the `HighlyAvailable`
//! assignor runs and the `Sticky` one skips.
//!
//! Moving a stateful task to a member that has not caught up on the changelog
//! would stall processing, so this step keeps the task active on its current
//! owner and stages a warmup copy on the intended target instead. The global
//! `max.warmup.replicas` cap bounds how many warmups one assignment creates.

use std::collections::HashMap;

use super::{
    active::current_active_owner,
    types::{AssignorInput, AssignorMember, Task},
};

/// Step 2: warmup deferral, for `HighlyAvailable` only.
///
/// This step looks at each *stateful* task that has a current active owner and
/// whose balanced-target owner differs from that owner. It keeps the move only
/// if the target is caught up. Otherwise it leaves the task active on the
/// current owner and stages a warmup on the intended target. The global warmup
/// cap limits how many warmups it stages.
pub(super) fn defer_warmups(
    members: &[&AssignorMember],
    input: &AssignorInput,
    tasks: &[Task],
    active: &mut HashMap<String, Vec<Task>>,
    warmup: &mut HashMap<String, Vec<Task>>,
) {
    // Snapshot the balanced-target owner for every task before mutating.
    let target_owner: HashMap<Task, String> = active
        .iter()
        .flat_map(|(member, ts)| ts.iter().map(move |t| (t.clone(), member.clone())))
        .collect();

    let mut warmups_created: i32 = 0;

    for task in tasks {
        if !input.stateful.contains(&task.0) {
            continue; // stateless: move applies directly, no warmup.
        }
        let Some(current) = current_active_owner(members, task) else {
            continue; // no prior owner: applied directly.
        };
        let Some(target) = target_owner.get(task) else {
            continue; // not in the active target (shouldn't happen).
        };
        if target == current {
            continue; // not a move.
        }

        // Is the intended target caught up on this task's changelog?
        let caught_up = members
            .iter()
            .find(|m| m.member_id == *target)
            .and_then(|m| m.task_lag.get(&(task.0.clone(), task.1)))
            .is_some_and(|&lag| lag <= input.acceptable_recovery_lag);

        if caught_up {
            continue; // keep the move; active already on `target`.
        }

        // Defer the move: active stays on `current`. Move the task back.
        move_active(active, target, current, task);

        // Stage a warmup on the intended target while under the global cap.
        if warmups_created < input.num_warmup_replicas {
            warmup.entry(target.clone()).or_default().push(task.clone());
            warmups_created += 1;
        }
    }
}

/// Moves `task` from the active set of `from` to the active set of `to`.
fn move_active(active: &mut HashMap<String, Vec<Task>>, from: &str, to: &str, task: &Task) {
    if let Some(list) = active.get_mut(from)
        && let Some(idx) = list.iter().position(|t| t == task)
    {
        list.swap_remove(idx);
    }
    active.entry(to.to_owned()).or_default().push(task.clone());
}
