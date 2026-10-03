use super::*;

impl Model for ReplayModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State::default()]
    }

    fn actions(&self, _state: &Self::State, actions: &mut Vec<Self::Action>) {
        for epoch in [-1, 0, 1, i32::MAX] {
            actions.push(Action::WriteGroup(epoch));
            actions.push(Action::WriteTargetEpoch(epoch));
            for member in 0..2 {
                actions.push(Action::WriteCurrent(member, epoch));
            }
        }
        for member in 0..2 {
            actions.push(Action::WriteMember(member));
            actions.push(Action::WriteTarget(member));
            actions.push(Action::TombstoneMember(member));
            actions.push(Action::TombstoneTarget(member));
            actions.push(Action::TombstoneCurrent(member));
        }
        actions.extend([
            Action::WriteTopology,
            Action::WritePartitionMetadata,
            Action::WriteStatePartitionMetadata,
            Action::TombstoneGroup,
            Action::TombstoneTargetEpoch,
            Action::TombstoneTopology,
            Action::MismatchedValue,
        ]);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::WriteGroup(epoch) => {
                let current = state.group_epoch.unwrap_or(0);
                if replay_epoch_is_admissible(current, epoch) {
                    state.group_epoch = Some(epoch);
                    state.tombstone_dominant = false;
                } else {
                    state.witnesses |= WITNESS_STALE_EPOCH;
                }
            }
            Action::WriteMember(member) => {
                if mutation(&state, ReplayRecordKind::MemberMetadata, member)
                    == ReplayMutation::Apply
                {
                    state.members |= bit(member);
                } else {
                    state.witnesses |= WITNESS_IGNORED_ORPHAN;
                }
            }
            Action::WriteTargetEpoch(epoch) => {
                if mutation(&state, ReplayRecordKind::TargetAssignmentMetadata, 0)
                    == ReplayMutation::Apply
                    && replay_epoch_is_admissible(state.target_epoch, epoch)
                {
                    state.target_epoch = epoch;
                } else if epoch < 0 || epoch < state.target_epoch {
                    state.witnesses |= WITNESS_STALE_EPOCH;
                } else {
                    state.witnesses |= WITNESS_IGNORED_ORPHAN;
                }
            }
            Action::WriteTarget(member) => {
                if mutation(&state, ReplayRecordKind::TargetAssignmentMember, member)
                    == ReplayMutation::Apply
                {
                    state.targets |= bit(member);
                } else {
                    state.witnesses |= WITNESS_IGNORED_ORPHAN;
                }
            }
            Action::WriteCurrent(member, epoch) => {
                if mutation(&state, ReplayRecordKind::CurrentMemberAssignment, member)
                    == ReplayMutation::Apply
                    && replay_epoch_is_admissible(
                        state.currents[usize::from(member)].unwrap_or(0),
                        epoch,
                    )
                {
                    state.currents[usize::from(member)] = Some(epoch);
                } else if epoch < 0
                    || state.currents[usize::from(member)].is_some_and(|old| epoch < old)
                {
                    state.witnesses |= WITNESS_STALE_EPOCH;
                } else {
                    state.witnesses |= WITNESS_IGNORED_ORPHAN;
                }
            }
            Action::WriteTopology => {
                if mutation(&state, ReplayRecordKind::Topology, 0) == ReplayMutation::Apply {
                    state.metadata |= METADATA_TOPOLOGY;
                } else {
                    state.witnesses |= WITNESS_IGNORED_ORPHAN;
                }
            }
            Action::WritePartitionMetadata => {
                if mutation(&state, ReplayRecordKind::PartitionMetadata, 0) == ReplayMutation::Apply
                {
                    state.metadata |= METADATA_PARTITIONS;
                } else {
                    state.witnesses |= WITNESS_IGNORED_ORPHAN;
                }
            }
            Action::WriteStatePartitionMetadata => {
                if mutation(&state, ReplayRecordKind::StatePartitionMetadata, 0)
                    == ReplayMutation::Apply
                {
                    state.metadata |= METADATA_SHARE_STATE;
                } else {
                    state.witnesses |= WITNESS_IGNORED_ORPHAN;
                }
            }
            Action::TombstoneGroup => {
                assert2::assert!(
                    tombstone(&state, ReplayRecordKind::GroupMetadata, 0)
                        == ReplayMutation::RemoveGroup
                );
                state = State {
                    tombstone_dominant: true,
                    witnesses: state.witnesses,
                    ..State::default()
                };
            }
            Action::TombstoneMember(member) => {
                if tombstone(&state, ReplayRecordKind::MemberMetadata, member)
                    == ReplayMutation::RemoveField
                {
                    state.members &= !bit(member);
                    state.targets &= !bit(member);
                    state.currents[usize::from(member)] = None;
                }
            }
            Action::TombstoneTargetEpoch => {
                if tombstone(&state, ReplayRecordKind::TargetAssignmentMetadata, 0)
                    == ReplayMutation::RemoveField
                {
                    state.target_epoch = 0;
                    state.targets = 0;
                }
            }
            Action::TombstoneTarget(member) => {
                if tombstone(&state, ReplayRecordKind::TargetAssignmentMember, member)
                    == ReplayMutation::RemoveField
                {
                    state.targets &= !bit(member);
                }
            }
            Action::TombstoneCurrent(member) => {
                if tombstone(&state, ReplayRecordKind::CurrentMemberAssignment, member)
                    == ReplayMutation::RemoveField
                {
                    state.currents[usize::from(member)] = None;
                }
            }
            Action::TombstoneTopology => {
                if tombstone(&state, ReplayRecordKind::Topology, 0) == ReplayMutation::RemoveField {
                    state.metadata &= !METADATA_TOPOLOGY;
                }
            }
            Action::MismatchedValue => {
                assert2::assert!(
                    replay_mutation(
                        ReplayRecordKind::Topology,
                        Some(ReplayRecordKind::MemberMetadata),
                        state.group_epoch.is_some(),
                        member_exists(&state, 0),
                    ) == ReplayMutation::Reject
                );
                state.witnesses |= WITNESS_REJECTED_BINDING;
            }
        }
        assert2::assert!(coherent(&state), "incoherent after {action:?}: {state:?}");
        (state != *last).then_some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("coherent_parentage", |_, state: &State| coherent(state)),
            Property::always("group_tombstone_dominates", |_, state: &State| {
                !state.tombstone_dominant || state.group_epoch.is_none()
            }),
            Property::sometimes("binding_rejected", |_, state: &State| {
                state.witnesses & WITNESS_REJECTED_BINDING != 0
            }),
            Property::sometimes("orphan_ignored", |_, state: &State| {
                state.witnesses & WITNESS_IGNORED_ORPHAN != 0
            }),
            Property::sometimes("stale_epoch_ignored", |_, state: &State| {
                state.witnesses & WITNESS_STALE_EPOCH != 0
            }),
            Property::sometimes("max_epoch_reached", |_, state: &State| {
                state.group_epoch == Some(i32::MAX)
            }),
            Property::sometimes("member_assignment_replayed", |_, state: &State| {
                state.targets != 0 && state.currents.iter().any(Option::is_some)
            }),
        ]
    }
}
