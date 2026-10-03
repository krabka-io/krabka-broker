use super::*;

pub(super) fn topology(epoch: i32) -> StreamsGroupTopologyValue {
    StreamsGroupTopologyValue {
        epoch,
        subtopologies: vec![StoredSubtopology {
            subtopology_id: SUBTOPOLOGY.to_string(),
            source_topics: Vec::new(),
            source_topic_regex: Vec::new(),
            repartition_sink_topics: Vec::new(),
            state_changelog_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            copartition_groups: Vec::new(),
        }],
    }
}

fn config() -> StreamsGroupConfig {
    StreamsGroupConfig {
        assignor: StreamsAssignorKind::Sticky,
        num_standby_replicas: 0,
        num_warmup_replicas: 0,
        ..StreamsGroupConfig::default()
    }
}

fn task_counts(partitions: i32) -> BTreeMap<String, i32> {
    [(SUBTOPOLOGY.to_string(), partitions)].into()
}

pub(super) fn reconcile(state: &mut State) -> bool {
    if state.actor.state.group_epoch >= MAX_GROUP_EPOCH {
        return false;
    }
    let topology = state
        .actor
        .topology
        .clone()
        .expect("model always has a topology");
    compute_and_install_target(
        &mut state.actor,
        &config(),
        &topology,
        &task_counts(state.partitions),
    );
    true
}

pub(super) fn at(state: &State) -> Instant {
    state
        .origin
        .checked_add(Duration::from_secs(u64::from(state.clock)))
        .expect("bounded model clock fits Instant")
}

fn task_map_projection(map: &BTreeMap<String, Vec<i32>>) -> TaskMapProjection {
    map.iter()
        .map(|(subtopology, partitions)| (subtopology.clone(), partitions.clone()))
        .collect()
}

pub(super) fn target_projection(
    target: &std::collections::HashMap<String, BTreeMap<String, Vec<i32>>>,
) -> TargetProjection {
    let mut projection: TargetProjection = target
        .iter()
        .map(|(member, tasks)| (member.clone(), task_map_projection(tasks)))
        .collect();
    projection.sort();
    projection
}

pub(super) fn member_projection(group: &StreamsGroupState) -> Vec<MemberProjection> {
    let mut projection: Vec<MemberProjection> = group
        .members
        .values()
        .map(|member| {
            (
                member.member_id.clone(),
                member.member_epoch,
                member.previous_member_epoch,
                member.assignment_state.as_i8(),
                task_map_projection(&member.active),
                task_map_projection(&member.active_pending_revocation),
                task_map_projection(&member.standby),
                task_map_projection(&member.warmup),
                member.last_seen,
            )
        })
        .collect();
    projection.sort_by(|left, right| left.0.cmp(&right.0));
    projection
}

fn durable_member_projection(group: &StreamsGroupState) -> Vec<DurableMemberProjection> {
    let mut projection: Vec<DurableMemberProjection> = group
        .members
        .values()
        .map(|member| {
            (
                member.member_id.clone(),
                member.member_epoch,
                member.previous_member_epoch,
                member.assignment_state.as_i8(),
                task_map_projection(&member.active),
                task_map_projection(&member.active_pending_revocation),
                task_map_projection(&member.standby),
                task_map_projection(&member.warmup),
            )
        })
        .collect();
    projection.sort();
    projection
}

pub(super) fn durable_projection(actor: &ActorState) -> DurableProjection {
    (
        actor.state.group_epoch,
        actor.state.assignment_epoch,
        actor.state.topology_epoch,
        actor.state.phase.as_str(),
        durable_member_projection(&actor.state),
        target_projection(&actor.state.target.active),
        target_projection(&actor.state.target.standby),
        target_projection(&actor.state.target.warmup),
    )
}

/// The tasks of all three roles that a heartbeat of `member_id` reports: its
/// assigned tasks, and its tasks pending revocation while it still holds
/// them.
pub(super) fn reported_tasks(state: &State, member_id: &str, kind: ReportKind) -> RoleTasks {
    let member = &state.actor.state.members[member_id];
    let with_pending = |assigned: &BTreeMap<String, Vec<i32>>,
                        pending: &BTreeMap<String, Vec<i32>>| {
        let mut reported = assigned.clone();
        if kind == ReportKind::Holding {
            for (subtopology, partitions) in pending {
                reported
                    .entry(subtopology.clone())
                    .or_default()
                    .extend(partitions.iter().copied());
            }
        }
        for partitions in reported.values_mut() {
            partitions.sort_unstable();
            partitions.dedup();
        }
        reported
    };
    RoleTasks {
        active: with_pending(&member.active, &member.active_pending_revocation),
        standby: with_pending(&member.standby, &member.standby_pending_revocation),
        warmup: with_pending(&member.warmup, &member.warmup_pending_revocation),
    }
}

pub(super) fn active_task_exclusive(group: &StreamsGroupState) -> bool {
    let mut held = HashSet::new();
    group.members.values().all(|member| {
        member
            .active
            .iter()
            .chain(member.active_pending_revocation.iter())
            .all(|(subtopology, partitions)| {
                partitions
                    .iter()
                    .all(|&partition| held.insert((subtopology.clone(), partition)))
            })
    })
}

pub(super) fn target_active_exclusive(group: &StreamsGroupState) -> bool {
    let mut assigned = HashSet::new();
    group.target.active.values().all(|tasks| {
        tasks.iter().all(|(subtopology, partitions)| {
            partitions
                .iter()
                .all(|&partition| assigned.insert((subtopology.clone(), partition)))
        })
    })
}

fn map_in_topology(tasks: &BTreeMap<String, Vec<i32>>, partitions: i32) -> bool {
    tasks.iter().all(|(subtopology, assigned)| {
        subtopology == SUBTOPOLOGY
            && assigned
                .iter()
                .all(|&partition| partition >= 0 && partition < partitions)
    })
}

pub(super) fn assignments_in_topology(state: &State) -> bool {
    let current_valid = state.actor.state.members.values().all(|member| {
        map_in_topology(&member.active, state.partitions)
            && map_in_topology(&member.standby, state.partitions)
            && map_in_topology(&member.warmup, state.partitions)
    });
    let target_valid = [
        &state.actor.state.target.active,
        &state.actor.state.target.standby,
        &state.actor.state.target.warmup,
    ]
    .into_iter()
    .all(|role| {
        role.values()
            .all(|tasks| map_in_topology(tasks, state.partitions))
    });
    current_valid && target_valid
}

pub(super) fn epochs_fenced(group: &StreamsGroupState) -> bool {
    group.group_epoch >= 0
        && group.assignment_epoch == group.target.epoch
        && group.target.epoch >= 0
        && group.target.epoch <= group.group_epoch
        && group.members.values().all(|member| {
            member.previous_member_epoch >= 0
                && member.previous_member_epoch <= member.member_epoch
                && member.member_epoch <= group.target.epoch
        })
}

pub(super) fn phase_coherent(group: &StreamsGroupState) -> bool {
    if group.members.is_empty() {
        return group.phase == StreamsGroupStatePhase::Empty;
    }
    // Kafka's `maybeUpdateGroupState`: a member that is not stable at the
    // assignment epoch keeps the group reconciling.
    let reconciling = group.members.values().any(|member| {
        member.assignment_state != StreamsMemberAssignmentState::Stable
            || member.member_epoch != group.target.epoch
    });
    if reconciling {
        group.phase == StreamsGroupStatePhase::Reconciling
    } else {
        group.phase == StreamsGroupStatePhase::Stable
    }
}
