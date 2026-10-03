use super::*;

impl Model for StreamsModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut actor = ActorState::new("g".to_string());
        let topology = topology(1);
        actor.state.topology_epoch = topology.epoch;
        actor.state.topology = Some(super::super::super::state::StoredTopologyHandle {
            epoch: topology.epoch,
        });
        actor.topology = Some(topology);
        vec![State {
            actor,
            origin: Instant::now(),
            clock: 0,
            partitions: 1,
            witnesses: 0,
            replayed: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.actor.state.group_epoch < MAX_GROUP_EPOCH {
            for member_id in ["a", "b"] {
                if state.actor.state.members.contains_key(member_id) {
                    if !state.actor.state.members[member_id]
                        .active_pending_revocation
                        .is_empty()
                    {
                        actions.push(Action::CurrentHeartbeat(member_id, ReportKind::Holding));
                    }
                    actions.push(Action::CurrentHeartbeat(member_id, ReportKind::Released));
                    if state.witnesses & WITNESS_STALE_FENCED == 0 {
                        actions.push(Action::StaleHeartbeat(member_id));
                    }
                    if state.witnesses & WITNESS_FORWARD_FENCED == 0 {
                        actions.push(Action::ForwardHeartbeat(member_id));
                    }
                    actions.push(Action::Leave(member_id));
                } else {
                    actions.push(Action::Join(member_id));
                }
            }
            if state.partitions == 1 && state.actor.state.topology_epoch < MAX_TOPOLOGY_EPOCH {
                actions.push(Action::ChangeTopology(2));
            }
            if state.clock < MAX_CLOCK && !state.actor.state.members.is_empty() {
                actions.push(Action::TimeoutTick);
            }
        }
        if state.witnesses & WITNESS_UNKNOWN_FENCED == 0 {
            actions.push(Action::UnknownHeartbeat);
        }
        if !state.replayed {
            actions.push(Action::Replay);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Join(member_id) => {
                if state.actor.state.members.contains_key(member_id) {
                    return None;
                }
                let mut member = StreamsMemberState::joining(member_id, "client", "host");
                member.process_id = member_id.to_string();
                member.topology_epoch = state.actor.state.topology_epoch;
                member.last_seen = at(&state);
                state.actor.state.add_or_update_member(member);
                if !reconcile(&mut state) {
                    return None;
                }
                state
                    .actor
                    .state
                    .reconcile_member(member_id, Some(&RoleTasks::default()));
            }
            Action::CurrentHeartbeat(member_id, report_kind) => {
                let current = state.actor.state.members.get(member_id)?.member_epoch;
                state
                    .actor
                    .state
                    .validate_heartbeat_epoch(member_id, current, OwnedTasks::default())
                    .ok()?;
                let before_pending = !state.actor.state.members[member_id]
                    .active_pending_revocation
                    .is_empty();
                let before_unreleased = state.actor.state.members[member_id].assignment_state
                    == StreamsMemberAssignmentState::UnreleasedTasks;
                let reported = reported_tasks(&state, member_id, report_kind);
                state.actor.state.members.get_mut(member_id)?.last_seen = at(&state);
                state
                    .actor
                    .state
                    .reconcile_member(member_id, Some(&reported));
                let member = &state.actor.state.members[member_id];
                if before_pending && member.active_pending_revocation.is_empty() {
                    state.witnesses |= WITNESS_RELEASED;
                }
                if before_unreleased
                    && member.assignment_state == StreamsMemberAssignmentState::Stable
                {
                    state.witnesses |= WITNESS_RELEASED;
                }
            }
            Action::StaleHeartbeat(member_id) => {
                let current = state.actor.state.members.get(member_id)?.member_epoch;
                let requested = current.saturating_sub(1);
                // Epoch 0 is a rejoin, not a stale epoch.
                if requested == current || requested == 0 {
                    return None;
                }
                // The request reports no owned tasks, so even the previous
                // epoch is fenced (Kafka's
                // `throwIfStreamsGroupMemberEpochIsInvalid`).
                let error = state
                    .actor
                    .state
                    .validate_heartbeat_epoch(member_id, requested, OwnedTasks::default())
                    .expect_err("stale member epoch is rejected");
                assert2::assert!(error == crate::codes::FENCED_MEMBER_EPOCH);
                state.witnesses |= WITNESS_STALE_FENCED;
            }
            Action::ForwardHeartbeat(member_id) => {
                let current = state.actor.state.members.get(member_id)?.member_epoch;
                let requested = current.checked_add(1)?;
                let error = state
                    .actor
                    .state
                    .validate_heartbeat_epoch(member_id, requested, OwnedTasks::default())
                    .expect_err("forward member epoch is rejected");
                assert2::assert!(error == crate::codes::FENCED_MEMBER_EPOCH);
                state.witnesses |= WITNESS_FORWARD_FENCED;
            }
            Action::Leave(member_id) => {
                state.actor.state.remove_member(member_id)?;
                if !reconcile(&mut state) {
                    return None;
                }
            }
            Action::TimeoutTick => {
                state.clock += 1;
                let expired = state
                    .actor
                    .state
                    .evict_expired(at(&state), Duration::from_secs(1));
                if !expired.is_empty() {
                    state.witnesses |= WITNESS_TIMEOUT;
                    if !reconcile(&mut state) {
                        return None;
                    }
                }
            }
            Action::ChangeTopology(partitions) => {
                state.partitions = partitions;
                let topology_epoch = state.actor.state.topology_epoch.checked_add(1)?;
                let topology = topology(topology_epoch);
                state.actor.state.topology_epoch = topology_epoch;
                state.actor.state.topology =
                    Some(super::super::super::state::StoredTopologyHandle {
                        epoch: topology_epoch,
                    });
                state.actor.topology = Some(topology);
                state.actor.state.dirty = true;
                if !reconcile(&mut state) {
                    return None;
                }
                state.witnesses |= WITNESS_TOPOLOGY;
            }
            Action::UnknownHeartbeat => {
                let error = state
                    .actor
                    .state
                    .validate_heartbeat_epoch("unknown", 1, OwnedTasks::default())
                    .expect_err("unknown member is rejected");
                assert2::assert!(error == crate::codes::UNKNOWN_MEMBER_ID);
                state.witnesses |= WITNESS_UNKNOWN_FENCED;
            }
            Action::Replay => {
                let before = durable_projection(&state.actor);
                let seed = snapshot_seed(&state.actor);
                let mut restored = ActorState::new("g".to_string());
                apply_seed(&mut restored, seed);
                for member in restored.state.members.values_mut() {
                    member.last_seen = at(&state);
                }
                assert2::assert!(durable_projection(&restored) == before);
                state.actor = restored;
                state.replayed = true;
                state.witnesses |= WITNESS_REPLAY;
            }
        }

        if state
            .actor
            .state
            .members
            .values()
            .any(|member| member.assignment_state == StreamsMemberAssignmentState::UnreleasedTasks)
        {
            state.witnesses |= WITNESS_WITHHELD;
        }
        assert2::assert!(active_task_exclusive(&state.actor.state));
        assert2::assert!(target_active_exclusive(&state.actor.state));
        assert2::assert!(assignments_in_topology(&state));
        assert2::assert!(epochs_fenced(&state.actor.state));
        assert2::assert!(phase_coherent(&state.actor.state));
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("active_task_exclusivity", |_, state: &State| {
                active_task_exclusive(&state.actor.state)
            }),
            Property::always("target_active_task_exclusivity", |_, state: &State| {
                target_active_exclusive(&state.actor.state)
            }),
            Property::always("assignment_within_topology", |_, state: &State| {
                assignments_in_topology(state)
            }),
            Property::always("member_epoch_fencing", |_, state: &State| {
                epochs_fenced(&state.actor.state)
            }),
            Property::always("reconciliation_phase_coherence", |_, state: &State| {
                phase_coherent(&state.actor.state)
            }),
            Property::sometimes("stale_epoch_rejected", |_, state: &State| {
                state.witnesses & WITNESS_STALE_FENCED != 0
            }),
            Property::sometimes("forward_epoch_rejected", |_, state: &State| {
                state.witnesses & WITNESS_FORWARD_FENCED != 0
            }),
            Property::sometimes("unknown_member_rejected", |_, state: &State| {
                state.witnesses & WITNESS_UNKNOWN_FENCED != 0
            }),
            Property::sometimes("timeout_evicted_member", |_, state: &State| {
                state.witnesses & WITNESS_TIMEOUT != 0
            }),
            Property::sometimes("topology_changed", |_, state: &State| {
                state.witnesses & WITNESS_TOPOLOGY != 0
            }),
            Property::sometimes("state_replayed", |_, state: &State| {
                state.witnesses & WITNESS_REPLAY != 0
            }),
            Property::sometimes("active_task_withheld", |_, state: &State| {
                state.witnesses & WITNESS_WITHHELD != 0
            }),
            Property::sometimes("active_task_released", |_, state: &State| {
                state.witnesses & WITNESS_RELEASED != 0
            }),
            Property::sometimes("two_members_joined", |_, state: &State| {
                state.actor.state.members.len() == 2
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.clock <= MAX_CLOCK
            && state.partitions <= 2
            && state.actor.state.group_epoch <= MAX_GROUP_EPOCH
            && state.actor.state.topology_epoch <= MAX_TOPOLOGY_EPOCH
            && state.actor.state.members.len() <= 2
    }
}
