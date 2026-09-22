//! The reconciliation of one member's current assignment toward its target:
//! a port of Kafka's `org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder`.
//!
//! A member moves to the target epoch only when it has nothing to revoke. A
//! member that must revoke tasks keeps its epoch in `UnrevokedTasks`, gets its
//! assignment without the revoked tasks, and moves on once a heartbeat no
//! longer reports them. A task of the target that another member still owns,
//! or that another member of the same process still runs in another role, is
//! held back, and the member waits in `UnreleasedTasks`.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::{StreamsMemberAssignmentState, StreamsMemberState, task_map::normalize_task_map};

type TaskMap = BTreeMap<String, Vec<i32>>;

/// The tasks of the three roles, in the order active, standby, warmup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleTasks {
    pub active: TaskMap,
    pub standby: TaskMap,
    pub warmup: TaskMap,
}

impl RoleTasks {
    fn is_empty(&self) -> bool {
        self.active.is_empty() && self.standby.is_empty() && self.warmup.is_empty()
    }

    /// Kafka's `TasksTuple.containsAny`: a task of any role is in both.
    fn contains_any(&self, other: &Self) -> bool {
        let overlap = |a: &TaskMap, b: &TaskMap| {
            a.iter().any(|(subtopology, partitions)| {
                b.get(subtopology)
                    .is_some_and(|other| partitions.iter().any(|p| other.contains(p)))
            })
        };
        overlap(&self.active, &other.active)
            || overlap(&self.standby, &other.standby)
            || overlap(&self.warmup, &other.warmup)
    }
}

/// The process ids that currently hold each task, over the assigned tasks and
/// the tasks pending revocation of every member: Kafka's
/// `StreamsGroup.currentActiveTaskProcessId`, `currentStandbyTaskProcessIds`
/// and `currentWarmupTaskProcessIds`.
#[derive(Debug, Default)]
pub struct TaskOwners {
    active: HashMap<(String, i32), String>,
    standby: HashMap<(String, i32), HashSet<String>>,
    warmup: HashMap<(String, i32), HashSet<String>>,
}

impl TaskOwners {
    pub fn of<'a>(members: impl IntoIterator<Item = &'a StreamsMemberState>) -> Self {
        let mut owners = Self::default();
        for member in members {
            for map in [&member.active, &member.active_pending_revocation] {
                for task in tasks(map) {
                    owners.active.insert(task, member.process_id.clone());
                }
            }
            for map in [&member.standby, &member.standby_pending_revocation] {
                for task in tasks(map) {
                    owners
                        .standby
                        .entry(task)
                        .or_default()
                        .insert(member.process_id.clone());
                }
            }
            for map in [&member.warmup, &member.warmup_pending_revocation] {
                for task in tasks(map) {
                    owners
                        .warmup
                        .entry(task)
                        .or_default()
                        .insert(member.process_id.clone());
                }
            }
        }
        owners
    }

    fn runs_elsewhere_in_process(&self, task: &(String, i32), process_id: &str) -> bool {
        self.standby
            .get(task)
            .is_some_and(|processes| processes.contains(process_id))
            || self
                .warmup
                .get(task)
                .is_some_and(|processes| processes.contains(process_id))
    }

    /// Kafka's `isUnreleasedActiveTask`.
    fn active_unreleased(&self, task: &(String, i32), process_id: &str) -> bool {
        self.active.contains_key(task) || self.runs_elsewhere_in_process(task, process_id)
    }

    /// Kafka's `isUnreleasedStandbyTask` and `isUnreleasedWarmupTask`.
    fn standby_or_warmup_unreleased(&self, task: &(String, i32), process_id: &str) -> bool {
        self.active
            .get(task)
            .is_some_and(|owner| owner == process_id)
            || self.runs_elsewhere_in_process(task, process_id)
    }
}

fn tasks(map: &TaskMap) -> impl Iterator<Item = (String, i32)> + '_ {
    map.iter().flat_map(|(subtopology, partitions)| {
        partitions
            .iter()
            .map(move |partition| (subtopology.clone(), *partition))
    })
}

/// Kafka's `CurrentAssignmentBuilder.build`: the member after it reconciles
/// toward `target` at `target_epoch`, or `None` when nothing changes.
///
/// `owned` holds the tasks that the heartbeat reports, when it reports all
/// three roles.
pub fn next_member_state(
    member: &StreamsMemberState,
    target_epoch: i32,
    target: &RoleTasks,
    owners: &TaskOwners,
    owned: Option<&RoleTasks>,
) -> Option<StreamsMemberState> {
    match member.assignment_state {
        StreamsMemberAssignmentState::Stable => (member.member_epoch != target_epoch).then(|| {
            compute_next_assignment(
                member,
                member.member_epoch,
                target_epoch,
                target,
                owners,
                owned,
            )
        }),
        StreamsMemberAssignmentState::UnrevokedTasks => {
            let pending = RoleTasks {
                active: member.active_pending_revocation.clone(),
                standby: member.standby_pending_revocation.clone(),
                warmup: member.warmup_pending_revocation.clone(),
            };
            match owned {
                Some(owned) if !owned.contains_any(&pending) => Some(compute_next_assignment(
                    member,
                    member.member_epoch.saturating_add(1),
                    target_epoch,
                    target,
                    owners,
                    Some(owned),
                )),
                _ => None,
            }
        }
        StreamsMemberAssignmentState::UnreleasedTasks => Some(compute_next_assignment(
            member,
            member.member_epoch,
            target_epoch,
            target,
            owners,
            owned,
        )),
    }
}

/// Kafka's `computeAssignmentDifference` for one role: the assigned tasks
/// (current and target), the tasks to revoke (current and not target), the
/// tasks to assign (target and not assigned, less the unreleased ones), and
/// whether a task was held back.
fn role_difference(
    current: &TaskMap,
    target: &TaskMap,
    unreleased: impl Fn(&(String, i32)) -> bool,
) -> (TaskMap, TaskMap, TaskMap, bool) {
    let mut assigned = TaskMap::new();
    let mut revoke = TaskMap::new();
    let mut assign = TaskMap::new();
    let mut held_back = false;
    for task in tasks(current) {
        let in_target = target
            .get(&task.0)
            .is_some_and(|partitions| partitions.contains(&task.1));
        let side = if in_target {
            &mut assigned
        } else {
            &mut revoke
        };
        side.entry(task.0).or_default().push(task.1);
    }
    for task in tasks(target) {
        let already = assigned
            .get(&task.0)
            .is_some_and(|partitions| partitions.contains(&task.1));
        if already {
            continue;
        }
        if unreleased(&task) {
            held_back = true;
        } else {
            assign.entry(task.0).or_default().push(task.1);
        }
    }
    (
        normalize_task_map(assigned),
        normalize_task_map(revoke),
        normalize_task_map(assign),
        held_back,
    )
}

fn merge(mut a: TaskMap, b: TaskMap) -> TaskMap {
    for (subtopology, partitions) in b {
        a.entry(subtopology).or_default().extend(partitions);
    }
    normalize_task_map(a)
}

/// Kafka's `computeNextAssignment` and `buildNewMember`.
fn compute_next_assignment(
    member: &StreamsMemberState,
    member_epoch: i32,
    target_epoch: i32,
    target: &RoleTasks,
    owners: &TaskOwners,
    owned: Option<&RoleTasks>,
) -> StreamsMemberState {
    let process = member.process_id.as_str();
    let (active, active_revoke, active_assign, active_held) =
        role_difference(&member.active, &target.active, |task| {
            owners.active_unreleased(task, process)
        });
    let (standby, standby_revoke, standby_assign, standby_held) =
        role_difference(&member.standby, &target.standby, |task| {
            owners.standby_or_warmup_unreleased(task, process)
        });
    let (warmup, warmup_revoke, warmup_assign, warmup_held) =
        role_difference(&member.warmup, &target.warmup, |task| {
            owners.standby_or_warmup_unreleased(task, process)
        });
    let revoke = RoleTasks {
        active: active_revoke,
        standby: standby_revoke,
        warmup: warmup_revoke,
    };
    let assign = RoleTasks {
        active: active_assign,
        standby: standby_assign,
        warmup: warmup_assign,
    };
    let held_back = active_held || standby_held || warmup_held;

    let mut next = member.clone();
    next.previous_member_epoch = member.member_epoch;
    let has_tasks_to_revoke =
        !revoke.is_empty() && owned.is_none_or(|owned| owned.contains_any(&revoke));
    if has_tasks_to_revoke {
        next.assignment_state = StreamsMemberAssignmentState::UnrevokedTasks;
        next.member_epoch = member_epoch;
        next.active = active;
        next.standby = standby;
        next.warmup = warmup;
        next.active_pending_revocation = revoke.active;
        next.standby_pending_revocation = revoke.standby;
        next.warmup_pending_revocation = revoke.warmup;
        return next;
    }
    next.assignment_state = if held_back {
        StreamsMemberAssignmentState::UnreleasedTasks
    } else {
        StreamsMemberAssignmentState::Stable
    };
    next.member_epoch = target_epoch;
    next.active = merge(active, assign.active);
    next.standby = merge(standby, assign.standby);
    next.warmup = merge(warmup, assign.warmup);
    next.active_pending_revocation = TaskMap::new();
    next.standby_pending_revocation = TaskMap::new();
    next.warmup_pending_revocation = TaskMap::new();
    next
}
