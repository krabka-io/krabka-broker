//! The assignor's input and output data types.
//!
//! [`AssignorMember`] and [`AssignorInput`] are what the coordinator actor
//! hands the assignor, and [`StreamsAssignment`] is what it gets back. They
//! mirror Kafka's `MemberAssignmentState`, `AssignmentConfigs` with the
//! `TopologyDescriber`, and `GroupAssignment`.

use std::collections::{BTreeMap, BTreeSet, HashMap};

/// One group member as the assignor sees it: Kafka's
/// `MemberMetadataAndStateImpl`.
///
/// It carries the tasks that the member currently owns, which drive
/// stickiness, and the task offsets that it last reported, which rank the
/// members that hold a task's state.
#[derive(Debug, Clone, Default)]
pub struct AssignorMember {
    pub member_id: String,
    /// The process that the member runs in. Load is balanced across
    /// processes, and a process holds at most one copy of a task.
    pub process_id: String,
    /// Active tasks the member currently owns, as
    /// `subtopology_id -> partitions`.
    pub current_active: BTreeMap<String, Vec<i32>>,
    /// Standby tasks the member currently owns.
    pub current_standby: BTreeMap<String, Vec<i32>>,
    /// Warmup tasks the member currently owns.
    pub current_warmup: BTreeMap<String, Vec<i32>>,
    /// The task offset sums that the member last reported, keyed by
    /// `(subtopology, partition)`. A larger sum is a more caught-up state.
    pub task_offsets: BTreeMap<(String, i32), i64>,
}

/// Inputs to one assignment computation: the task universe, the stateful
/// subtopologies and `num.standby.replicas`.
#[derive(Debug, Clone, Default)]
pub struct AssignorInput {
    /// The full task universe: `subtopology_id -> ALL partitions`.
    pub tasks: BTreeMap<String, Vec<i32>>,
    /// Subtopology ids that have a changelog, that is, the stateful ones.
    pub stateful: BTreeSet<String>,
    /// `num.standby.replicas`: standby copies per stateful task.
    pub num_standby_replicas: i32,
}

/// The computed target assignment: per-member task maps for each role.
///
/// A member with no tasks in a role has no entry in that role's map. The
/// assignor assigns no warmup tasks: Kafka's default assignment refiner,
/// `NoOpAssignmentRefiner`, hands out none, so a task that has to move does
/// so at once.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StreamsAssignment {
    /// `member_id -> active tasks`.
    pub active: HashMap<String, BTreeMap<String, Vec<i32>>>,
    /// `member_id -> standby tasks`.
    pub standby: HashMap<String, BTreeMap<String, Vec<i32>>>,
}

/// A `(subtopology_id, partition)` task in its canonical ordered form.
pub(super) type Task = (String, i32);

/// Converts a `member -> tasks` working map into the public
/// `member -> (subtopology -> partitions)` form.
///
/// The function sorts the partitions and drops the members that have no tasks
/// in the role.
pub(super) fn to_role_maps(
    by_member: &BTreeMap<String, BTreeSet<Task>>,
) -> HashMap<String, BTreeMap<String, Vec<i32>>> {
    by_member
        .iter()
        .filter(|(_, tasks)| !tasks.is_empty())
        .map(|(member, tasks)| {
            let mut role: BTreeMap<String, Vec<i32>> = BTreeMap::new();
            for (sub, part) in tasks {
                role.entry(sub.clone()).or_default().push(*part);
            }
            (member.clone(), role)
        })
        .collect()
}
