//! KIP-1071 server-side task assignor: a port of Kafka's `StickyTaskAssignor`,
//! the only built-in streams assignor.
//!
//! This is a *pure* module, with no async, no I/O, and no metadata access. It
//! takes already-resolved inputs, an [`AssignorInput`] and a slice of
//! [`AssignorMember`], and returns a target [`StreamsAssignment`]. The
//! coordinator actor builds the inputs from the member state and the topology,
//! and applies the output as the next target assignment.
//!
//! A *task* is `(subtopology_id, partition)`. The assignor places one active
//! copy of every task and `num.standby.replicas` standby copies of every
//! stateful task, balancing the load across processes and keeping tasks where
//! their state already is. It assigns no warmup tasks: in Kafka those come
//! from the assignment refiner, whose default hands out none, so a task that
//! has to move does so at once.
//!
//! Determinism is mandatory. Kafka breaks some load ties by hash-map order;
//! this port breaks them by process id and member id, so the same inputs
//! always give the same assignment.
//!
//! The steps live in their own modules: `process` holds the per-process load
//! and the previous owners, `active` places the active tasks, `standby` places
//! the standby copies, and `types` holds the input and output shapes.

use {
    self::{
        active::assign_active,
        process::LocalState,
        standby::assign_standby,
        types::{Task, to_role_maps},
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        convert::AsRef,
        fmt::Debug,
        sync::Arc,
    },
};

mod active;
mod process;
mod standby;
mod types;

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
    let mut ordered: Vec<&AssignorMember> = members.iter().collect();
    ordered.sort_by(|a, b| a.member_id.cmp(&b.member_id));

    let tasks = flatten_tasks(&input.tasks);
    let stateful_tasks: Vec<Task> = tasks
        .iter()
        .filter(|(sub, _)| input.stateful.contains(sub))
        .cloned()
        .collect();
    let num_standby_replicas = i64::from(input.num_standby_replicas.max(0));

    let mut state = LocalState::new(&ordered, &tasks, stateful_tasks.len(), num_standby_replicas);
    assign_active(&mut state, &tasks);
    if num_standby_replicas > 0 {
        assign_standby(&mut state, &stateful_tasks);
    }

    let mut active: BTreeMap<String, BTreeSet<Task>> = BTreeMap::new();
    let mut standby: BTreeMap<String, BTreeSet<Task>> = BTreeMap::new();
    for process in state.processes.values() {
        for (member, tasks) in &process.active {
            active
                .entry(member.clone())
                .or_default()
                .extend(tasks.iter().cloned());
        }
        for (member, tasks) in &process.standby {
            standby
                .entry(member.clone())
                .or_default()
                .extend(tasks.iter().cloned());
        }
    }
    StreamsAssignment {
        active: to_role_maps(&active),
        standby: to_role_maps(&standby),
    }
}

/// Flattens the `subtopology -> partitions` universe into an ordered, de-duped
/// list of `(subtopology, partition)` tasks.
fn flatten_tasks(tasks: &BTreeMap<String, Vec<i32>>) -> Vec<Task> {
    let mut out: Vec<Task> = tasks
        .iter()
        .flat_map(|(sub, parts)| parts.iter().map(move |&p| (sub.clone(), p)))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Native, in-process extension. Implementations must be deterministic,
/// non-blocking and must not panic. This is not a JVM class loader.
pub trait TaskAssignor: Debug + Send + Sync {
    /// Stable, unique registration name.
    fn name(&self) -> &str;
    /// Compute the next target.
    /// # Errors
    /// Returns an implementation-specific failure; the coordinator keeps its old target.
    fn assign(
        &self,
        members: &[AssignorMember],
        input: &AssignorInput,
    ) -> Result<StreamsAssignment, String>;
}
/// Ordered native extensions registered before startup. First entry is default.
#[derive(Debug, Clone, Default)]
pub struct TaskAssignorRegistry(Vec<Arc<dyn TaskAssignor>>);
impl PartialEq for TaskAssignorRegistry {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len() && self.0.iter().zip(&other.0).all(|(a, b)| Arc::ptr_eq(a, b))
    }
}
impl Eq for TaskAssignorRegistry {}
impl TaskAssignorRegistry {
    /// Register a native extension.
    /// # Errors
    /// Rejects blank, padded, duplicate or reserved names.
    pub fn register(&mut self, assignor: Arc<dyn TaskAssignor>) -> Result<(), String> {
        let name = assignor.name();
        if name.is_empty()
            || name.trim() != name
            || ["auto", "sticky", "highly_available", "highly-available"].contains(&name)
            || self.get(name).is_some()
        {
            return Err(format!(
                "invalid or duplicate Streams task assignor name: {name}"
            ));
        }
        self.0.push(assignor);
        Ok(())
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&dyn TaskAssignor> {
        self.0.iter().find(|a| a.name() == name).map(AsRef::as_ref)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> { self.0.iter().map(|a| a.name()) }

    #[must_use]
    pub fn default_name(&self) -> Option<&str> { self.0.first().map(|a| a.name()) }
}

/// Validate extensions before their targets can enter coordinator state.
///
/// # Errors
/// Rejects unknown members/tasks, duplicate or missing active ownership, and
/// standby copies placed with an active/standby copy on the same process.
pub fn validate_assignment(
    assignment: &StreamsAssignment,
    members: &[AssignorMember],
    input: &AssignorInput,
) -> Result<(), String> {
    let universe: BTreeSet<_> = flatten_tasks(&input.tasks).into_iter().collect();
    let mut active = BTreeSet::new();
    let mut placements = BTreeSet::new();
    let mut standby_counts = BTreeMap::<_, i32>::new();
    for (is_active, role) in [(true, &assignment.active), (false, &assignment.standby)] {
        for (member_id, tasks) in role {
            let member = members
                .iter()
                .find(|m| &m.member_id == member_id)
                .ok_or_else(|| format!("assignor returned unknown member {member_id}"))?;
            for (sub, partitions) in tasks {
                for partition in partitions {
                    let task = (sub.clone(), *partition);
                    if !universe.contains(&task) || (!is_active && !input.stateful.contains(sub)) {
                        return Err(format!("assignor returned invalid task {task:?}"));
                    }
                    if !is_active {
                        let count = standby_counts.entry(task.clone()).or_default();
                        *count += 1;
                        if *count > input.num_standby_replicas.max(0) {
                            return Err(format!(
                                "assignor exceeded standby replica count: {task:?}"
                            ));
                        }
                    }
                    if is_active && !active.insert(task.clone()) {
                        return Err(format!("assignor duplicated active task {task:?}"));
                    }
                    if !placements.insert((task.clone(), member.process_id.clone())) {
                        return Err(format!("assignor duplicated task on process: {task:?}"));
                    }
                }
            }
        }
    }
    if active != universe {
        return Err("assignor omitted active tasks".into());
    }
    Ok(())
}
