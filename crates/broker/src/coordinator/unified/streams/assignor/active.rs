//! Active placement: Kafka's `StickyTaskAssignor.assignActive`.
//!
//! A task stays with its current owner while that owner is under the active
//! quota. A task that stays unplaced goes to a member that holds its state as
//! a standby, a warmup, or through reported offsets, again within the quota.
//! The rest go one at a time to the least loaded process.

use super::{
    process::{LocalState, Member},
    types::Task,
};

/// Places every task in `tasks` as an active task.
pub(super) fn assign_active(state: &mut LocalState, tasks: &[Task]) {
    // As in Kafka, the sticky passes go by partition first, because a range
    // assignment pairs the same partition of each subtopology.
    let mut remaining: Vec<Task> = tasks.to_vec();
    remaining.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

    // 1. The previous active owner, within its quota.
    remaining.retain(|task| {
        let Some(prev) = state.active_task_to_prev_member.get(task).cloned() else {
            return true;
        };
        !place_within_active_quota(state, &prev, task)
    });

    // 2. The previous standby owner with the least load, within its quota.
    remaining.retain(|task| {
        let prev =
            state.prev_member_with_least_load(state.standby_task_to_prev_members.get(task), None);
        let Some(prev) = prev else {
            return true;
        };
        !place_within_active_quota(state, &prev, task)
    });

    // 3. The least loaded process, by subtopology first for a range-like
    //    initial assignment.
    remaining.sort();
    for task in &remaining {
        let Some(process_id) = state.processes_by_load().into_iter().next() else {
            return;
        };
        let count = state.processes.get_mut(&process_id).map_or(0, |process| {
            process.add_task_to_least_loaded_member(task, true)
        });
        state.maybe_update_active_tasks_per_member(count);
        state.maybe_update_total_tasks_per_member(count);
    }
}

/// Places `task` on `member` when the member is under the active quota, and
/// returns whether it did.
fn place_within_active_quota(state: &mut LocalState, member: &Member, task: &Task) -> bool {
    if state.member_count(member) >= state.active_tasks_per_member {
        return false;
    }
    let count = state.add_task(member, task, true);
    state.maybe_update_active_tasks_per_member(count);
    state.maybe_update_total_tasks_per_member(count);
    true
}
