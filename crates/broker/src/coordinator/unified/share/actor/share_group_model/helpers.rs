use super::*;

/// Marks the share state of exactly the topic's `partitions` initialized, as
/// the lifecycle hook leaves it once the persister answers. The model's
/// assignor only hands out initialized partitions.
pub(super) fn initialize(group: &mut ShareGroupState, partitions: i32) {
    group.initialized = (0..partitions).map(|p| (TOPIC, p)).collect();
    group.topic_names.insert(TOPIC, TOPIC_NAME.to_owned());
}

pub(super) fn metadata(partitions: i32) -> ModelMetadata {
    ModelMetadata { partitions }
}

pub(super) fn at(state: &State) -> Instant {
    state
        .origin
        .checked_add(Duration::from_secs(u64::from(state.clock)))
        .expect("bounded model clock fits Instant")
}

pub(super) fn assignment_coordinates_unique(group: &ShareGroupState) -> bool {
    group.members.values().all(|member| {
        let mut seen = HashSet::new();
        member.assigned_partitions.iter().all(|(topic, parts)| {
            parts
                .iter()
                .all(|partition| seen.insert((*topic, *partition)))
        })
    }) && group.target.per_member.values().all(|assignment| {
        let mut seen = HashSet::new();
        assignment.iter().all(|(topic, parts)| {
            parts
                .iter()
                .all(|partition| seen.insert((*topic, *partition)))
        })
    })
}

pub(super) fn epochs_fenced(group: &ShareGroupState) -> bool {
    group.group_epoch >= 0
        && group.target.epoch >= 0
        && group.target.epoch <= group.group_epoch
        && group
            .members
            .values()
            .all(|member| member.member_epoch >= 0 && member.member_epoch <= group.group_epoch)
}

pub(super) fn assignments_in_metadata(state: &State) -> bool {
    let valid = |assignment: &HashMap<Uuid, Vec<i32>>| {
        assignment.iter().all(|(topic, parts)| {
            *topic == TOPIC
                && parts
                    .iter()
                    .all(|partition| *partition >= 0 && *partition < state.partitions)
        })
    };
    state.group.members.values().all(|member| {
        member.member_epoch < state.group.target.epoch || valid(&member.assigned_partitions)
    }) && state.group.target.per_member.values().all(valid)
}

pub(super) fn durable_projection(group: &ShareGroupState) -> DurableProjection {
    let mut members: Vec<DurableMemberProjection> = group
        .members
        .values()
        .map(|member| {
            let mut subscriptions: Vec<String> =
                member.subscribed_topic_names.iter().cloned().collect();
            subscriptions.sort();
            let mut assigned = member
                .assigned_partitions
                .get(&TOPIC)
                .cloned()
                .unwrap_or_default();
            assigned.sort_unstable();
            (
                member.member_id.clone(),
                member.member_epoch,
                member.previous_member_epoch,
                subscriptions,
                assigned,
            )
        })
        .collect();
    members.sort();
    let mut target: Vec<(String, Vec<i32>)> = group
        .members
        .keys()
        .filter_map(|member_id| {
            group.target.per_member.get(member_id).map(|assignment| {
                let mut partitions = assignment.get(&TOPIC).cloned().unwrap_or_default();
                partitions.sort_unstable();
                (member_id.clone(), partitions)
            })
        })
        .collect();
    target.sort();
    (group.group_epoch, group.target.epoch, members, target)
}
