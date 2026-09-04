//! Bounded Stateright model of coordinator record replay.
//!
//! DRIVEN production code: [`replay_mutation`] binds keys to value decoders,
//! preserves parent-before-child application, and selects tombstone actions;
//! [`replay_epoch_is_admissible`] fences malformed and stale epochs. MODELED:
//! one group, two members, every group/member/assignment/topology record class,
//! exact retries, malformed type bindings, epochs 0, 1, and `i32::MAX`, and
//! every reachable log ordering through depth 16.

use stateright::{Checker, Model, Property};

use super::replay_policy::{
    ReplayMutation, ReplayRecordKind, replay_epoch_is_admissible, replay_mutation,
};

const MAX_DEPTH: usize = 16;
const MAX_STATES: usize = 2_000_000;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 46_672;
const WITNESS_REJECTED_BINDING: u8 = 1 << 0;
const WITNESS_IGNORED_ORPHAN: u8 = 1 << 1;
const WITNESS_STALE_EPOCH: u8 = 1 << 2;
const METADATA_TOPOLOGY: u8 = 1 << 0;
const METADATA_PARTITIONS: u8 = 1 << 1;
const METADATA_SHARE_STATE: u8 = 1 << 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Action {
    WriteGroup(i32),
    WriteMember(u8),
    WriteTargetEpoch(i32),
    WriteTarget(u8),
    WriteCurrent(u8, i32),
    WriteTopology,
    WritePartitionMetadata,
    WriteStatePartitionMetadata,
    TombstoneGroup,
    TombstoneMember(u8),
    TombstoneTargetEpoch,
    TombstoneTarget(u8),
    TombstoneCurrent(u8),
    TombstoneTopology,
    MismatchedValue,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct State {
    group_epoch: Option<i32>,
    target_epoch: i32,
    members: u8,
    targets: u8,
    currents: [Option<i32>; 2],
    metadata: u8,
    tombstone_dominant: bool,
    witnesses: u8,
}

#[derive(Clone, Debug)]
struct ReplayModel;

fn bit(member: u8) -> u8 {
    1 << member
}

fn member_exists(state: &State, member: u8) -> bool {
    state.members & bit(member) != 0
}

fn mutation(state: &State, kind: ReplayRecordKind, member: u8) -> ReplayMutation {
    replay_mutation(
        kind,
        Some(kind),
        state.group_epoch.is_some(),
        member_exists(state, member),
    )
}

fn tombstone(state: &State, kind: ReplayRecordKind, member: u8) -> ReplayMutation {
    replay_mutation(
        kind,
        None,
        state.group_epoch.is_some(),
        member_exists(state, member),
    )
}

fn coherent(state: &State) -> bool {
    if state.group_epoch.is_none() {
        return state.target_epoch == 0
            && state.members == 0
            && state.targets == 0
            && state.currents == [None, None]
            && state.metadata == 0;
    }
    state.targets & !state.members == 0
        && state.currents.iter().enumerate().all(|(member, epoch)| {
            epoch.is_none() || state.members & bit(u8::try_from(member).unwrap()) != 0
        })
        && state.group_epoch.is_some_and(|epoch| epoch >= 0)
        && state.target_epoch >= 0
        && state.currents.iter().flatten().all(|epoch| *epoch >= 0)
}

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

#[test]
fn coordinator_replay_log_orders_are_safe() {
    let checker = ReplayModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "coordinator_replay unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH, "depth cap hit");
    assert2::assert!(checker.state_count() < MAX_STATES, "state cap hit");
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
