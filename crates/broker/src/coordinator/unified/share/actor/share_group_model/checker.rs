use super::*;

impl Model for ShareModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut group = ShareGroupState::new("g");
        initialize(&mut group, 1);
        vec![State {
            group,
            origin: Instant::now(),
            clock: 0,
            partitions: 1,
            witnesses: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.group.group_epoch < MAX_EPOCH {
            for member_id in ["a", "b"] {
                if !state.group.members.contains_key(member_id) {
                    actions.push(Action::Join(member_id));
                }
            }
        }
        for member_id in ["a", "b"] {
            if state.group.members.contains_key(member_id) {
                for epoch in [EpochKind::Current, EpochKind::Stale, EpochKind::Forward] {
                    actions.push(Action::Heartbeat(member_id, epoch));
                }
                if state.group.group_epoch < MAX_EPOCH {
                    actions.push(Action::Leave(member_id));
                    for partitions in [1, 2] {
                        if partitions != state.partitions {
                            actions.push(Action::MetadataHeartbeat(member_id, partitions));
                        }
                    }
                }
            }
        }
        if state.clock < MAX_CLOCK && state.group.group_epoch < MAX_EPOCH {
            actions.push(Action::TimeoutTick);
        }
        if state.witnesses & WITNESS_REPLAY == 0 {
            actions.push(Action::Replay);
        }
        if state.witnesses & WITNESS_UNKNOWN == 0 {
            actions.push(Action::UnknownHeartbeat);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Join(member_id) => {
                if state.group.members.contains_key(member_id) {
                    return None;
                }
                let mut member = ShareMemberState::joining(
                    member_id,
                    "client",
                    "host",
                    [TOPIC_NAME.to_owned()].into(),
                );
                member.last_seen = at(&state);
                state.group.add_or_update_member(member);
                if !reconcile(
                    &mut state.group,
                    &metadata(state.partitions),
                    std::time::Duration::ZERO,
                ) {
                    return None;
                }
                state.group.advance_member_epoch(member_id);
            }
            Action::Heartbeat(member_id, kind) => {
                let current = state.group.members.get(member_id)?.member_epoch;
                let requested = match kind {
                    EpochKind::Current => current,
                    EpochKind::Stale => current.saturating_sub(1),
                    EpochKind::Forward => current.saturating_add(1),
                };
                match state.group.validate_member_epoch(member_id, requested) {
                    Ok(_) => {
                        state.group.members.get_mut(member_id)?.last_seen = at(&state);
                        if !reconcile(
                            &mut state.group,
                            &metadata(state.partitions),
                            std::time::Duration::ZERO,
                        ) {
                            return None;
                        }
                        if state.group.target.epoch > current {
                            state.group.advance_member_epoch(member_id);
                        }
                    }
                    Err(error) => match kind {
                        EpochKind::Stale => {
                            assert2::assert!(error == crate::codes::FENCED_MEMBER_EPOCH);
                            state.witnesses |= WITNESS_STALE_FENCED;
                        }
                        EpochKind::Forward => {
                            assert2::assert!(error == crate::codes::FENCED_MEMBER_EPOCH);
                            state.witnesses |= WITNESS_FORWARD_FENCED;
                        }
                        EpochKind::Current => return None,
                    },
                }
            }
            Action::Leave(member_id) => {
                state.group.remove_member(member_id)?;
                if !state.group.bump_epoch() {
                    return None;
                }
            }
            Action::TimeoutTick => {
                state.clock += 1;
                let expired = state
                    .group
                    .evict_expired(at(&state), Duration::from_secs(1));
                if !expired.is_empty() {
                    state.witnesses |= WITNESS_TIMEOUT;
                    if !reconcile(
                        &mut state.group,
                        &metadata(state.partitions),
                        std::time::Duration::ZERO,
                    ) {
                        return None;
                    }
                }
            }
            Action::MetadataHeartbeat(member_id, partitions) => {
                let current = state.group.members.get(member_id)?.member_epoch;
                state.partitions = partitions;
                initialize(&mut state.group, partitions);
                let before = state.group.group_epoch;
                if !reconcile(
                    &mut state.group,
                    &metadata(state.partitions),
                    std::time::Duration::ZERO,
                ) {
                    return None;
                }
                if state.group.target.epoch > current {
                    state.group.advance_member_epoch(member_id);
                }
                if state.group.group_epoch > before {
                    state.witnesses |= WITNESS_METADATA;
                }
                state.group.members.get_mut(member_id)?.last_seen = at(&state);
            }
            Action::UnknownHeartbeat => {
                let error = state
                    .group
                    .validate_member_epoch("unknown", 1)
                    .expect_err("unknown member is rejected");
                assert2::assert!(error == crate::codes::UNKNOWN_MEMBER_ID);
                state.witnesses |= WITNESS_UNKNOWN;
            }
            Action::Replay => {
                let before = durable_projection(&state.group);
                let seed = snapshot_seed(&state.group);
                let mut restored = ShareGroupState::new("g");
                apply_seed(&mut restored, seed);
                for member in restored.members.values_mut() {
                    member.last_seen = at(&state);
                }
                assert2::assert!(durable_projection(&restored) == before);
                state.group = restored;
                state.witnesses |= WITNESS_REPLAY;
            }
        }
        assert2::assert!(assignment_coordinates_unique(&state.group));
        assert2::assert!(epochs_fenced(&state.group));
        assert2::assert!(assignments_in_metadata(&state));
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("ownership_coordinate_uniqueness", |_, state: &State| {
                assignment_coordinates_unique(&state.group)
            }),
            Property::always("member_epoch_fencing", |_, state: &State| {
                epochs_fenced(&state.group)
            }),
            Property::always("assignment_within_metadata", |_, state: &State| {
                assignments_in_metadata(state)
            }),
            Property::sometimes("stale_epoch_rejected", |_, state: &State| {
                state.witnesses & WITNESS_STALE_FENCED != 0
            }),
            Property::sometimes("forward_epoch_rejected", |_, state: &State| {
                state.witnesses & WITNESS_FORWARD_FENCED != 0
            }),
            Property::sometimes("timeout_evicted_member", |_, state: &State| {
                state.witnesses & WITNESS_TIMEOUT != 0
            }),
            Property::sometimes("metadata_changed_assignment", |_, state: &State| {
                state.witnesses & WITNESS_METADATA != 0
            }),
            Property::sometimes("state_replayed", |_, state: &State| {
                state.witnesses & WITNESS_REPLAY != 0
            }),
            Property::sometimes("unknown_member_rejected", |_, state: &State| {
                state.witnesses & WITNESS_UNKNOWN != 0
            }),
            Property::sometimes("two_members_joined", |_, state: &State| {
                state.group.members.len() == 2
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.clock <= MAX_CLOCK
            && state.group.group_epoch <= MAX_EPOCH
            && state.group.members.len() <= 2
    }
}
