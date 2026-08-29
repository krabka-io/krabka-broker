//! KIP-1071 server-side task assignor. It is sticky or highly available, and
//! it places active, standby, and warmup tasks and promotes caught-up ones.
//!
//! This is a *pure* module, with no async, no I/O, and no metadata access. It
//! takes already-resolved inputs, an [`AssignorInput`] and a slice of
//! [`AssignorMember`], and returns a target [`StreamsAssignment`]. The
//! coordinator actor builds the inputs from the member state and the topology,
//! and applies the output as the next target assignment.
//!
//! A *task* is `(subtopology_id, partition)`. Tasks come in three roles.
//! **active** has exactly one instance for each task. **standby** holds
//! replicas of stateful tasks for failover. **warmup** is transient: a member
//! builds up a stateful task's local state there before it can safely take the
//! task over as active.
//!
//! Determinism is mandatory. The assignor processes members in lexicographic
//! `member_id` order and tasks in `(subtopology_id, partition)` order, so the
//! same inputs always give the same assignment.
//!
//! The steps live in their own modules: `active` computes the balanced active
//! target, `warmup` defers the moves whose target is not caught up, `standby`
//! places the fault-tolerant copies, and `types` holds the input and output
//! shapes. This file keeps the entry point that runs them in order.

use std::collections::{BTreeMap, HashMap};

use self::{
    active::assign_active,
    standby::assign_standby,
    types::{Task, to_role_maps},
    warmup::defer_warmups,
};
use super::config::StreamsAssignorKind;

mod active;
mod standby;
mod types;
mod warmup;

#[cfg(test)]
mod tests;

pub use self::types::{AssignorInput, AssignorMember, StreamsAssignment};

/// Computes the target [`StreamsAssignment`] for a streams group.
///
/// See the module docs for the algorithm. With no members, it returns an empty
/// assignment.
#[must_use]
pub fn assign(members: &[AssignorMember], input: &AssignorInput) -> StreamsAssignment {
    if members.is_empty() {
        return StreamsAssignment::default();
    }

    // Stable member ordering for every downstream decision.
    let mut ordered: Vec<&AssignorMember> = members.iter().collect();
    ordered.sort_by(|a, b| a.member_id.cmp(&b.member_id));

    let kind = resolve_kind(input);
    let highly_available = matches!(kind, StreamsAssignorKind::HighlyAvailable);

    // Flatten the task universe in (subtopology, partition) order.
    let tasks = flatten_tasks(&input.tasks);

    // 1. Balanced active target (both modes).
    let mut active = assign_active(&ordered, &tasks);

    // 2. Warmup deferral (HighlyAvailable only). May rewrite active back to the
    //    current owner and stage a warmup on the intended target.
    let mut warmup: HashMap<String, Vec<Task>> = HashMap::new();
    if highly_available {
        defer_warmups(&ordered, input, &tasks, &mut active, &mut warmup);
    }

    // 3. Standby placement (HighlyAvailable only).
    let mut standby: HashMap<String, Vec<Task>> = HashMap::new();
    if highly_available {
        assign_standby(&ordered, input, &tasks, &active, &warmup, &mut standby);
    }

    // 4. Assemble + normalize.
    StreamsAssignment {
        active: to_role_maps(&active),
        standby: to_role_maps(&standby),
        warmup: to_role_maps(&warmup),
    }
}

/// Resolves `Auto` to a concrete kind. It returns `HighlyAvailable` when a
/// stateful subtopology exists, and `Sticky` otherwise. `Sticky` and
/// `HighlyAvailable` pass through unchanged.
fn resolve_kind(input: &AssignorInput) -> StreamsAssignorKind {
    match input.kind {
        StreamsAssignorKind::Auto => {
            if input.stateful.is_empty() {
                StreamsAssignorKind::Sticky
            } else {
                StreamsAssignorKind::HighlyAvailable
            }
        }
        other => other,
    }
}

/// Flattens the `subtopology -> partitions` universe into an ordered, de-duped
/// list of `(subtopology, partition)` tasks.
fn flatten_tasks(tasks: &BTreeMap<String, Vec<i32>>) -> Vec<Task> {
    let mut out: Vec<Task> = Vec::new();
    for (sub, parts) in tasks {
        for &p in parts {
            out.push((sub.clone(), p));
        }
    }
    // BTreeMap already orders by subtopology; sort/dedup guards against
    // unsorted or duplicate partition lists in the input.
    out.sort();
    out.dedup();
    out
}
