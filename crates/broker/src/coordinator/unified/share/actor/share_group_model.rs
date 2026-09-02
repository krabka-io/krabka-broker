//! Bounded stateright model of the KIP-932 share-group membership machine.
//!
//! DRIVEN production code: [`ShareGroupState`] membership and timeout
//! transitions, the real share assignor through [`reconcile`], the shared
//! member-epoch fence, and the real snapshot/apply replay adapters. MODELED:
//! two member identities, one subscribed topic with one or two partitions, a
//! logical clock through four ticks, and group epochs through five. The search
//! explores join, current/stale/forward heartbeat, leave, timeout, metadata
//! resize, and one crash/replay in every reachable ordering.
//!
//! Share assignments intentionally permit the same partition across members
//! when members outnumber partitions. The ownership property therefore proves
//! uniqueness of each `(member, topic, partition)` coordinate, not consumer-
//! group-style cross-member exclusivity.

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};

use krabka_protocol::primitives::uuid::Uuid;
use stateright::{Checker, Model, Property};

use super::{
    assignment::reconcile,
    seed::{apply_seed, snapshot_seed},
};
use crate::coordinator::unified::{
    actor::MetadataProvider,
    reconciler::ReconcileInput,
    share::state::{ShareGroupState, ShareMemberState},
    validate_member_epoch,
};

const TOPIC: Uuid = Uuid([42; 16]);
const TOPIC_NAME: &str = "t";
const MAX_CLOCK: u8 = 4;
const MAX_EPOCH: i32 = 5;
const MAX_STATES: usize = 1_000_000;
const MAX_DEPTH: usize = 64;
const WITNESS_STALE_FENCED: u8 = 1 << 0;
const WITNESS_FORWARD_FENCED: u8 = 1 << 1;
const WITNESS_TIMEOUT: u8 = 1 << 2;
const WITNESS_METADATA: u8 = 1 << 3;
const WITNESS_REPLAY: u8 = 1 << 4;
const WITNESS_UNKNOWN: u8 = 1 << 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EpochKind {
    Current,
    Stale,
    Forward,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Action {
    Join(&'static str),
    Heartbeat(&'static str, EpochKind),
    Leave(&'static str),
    TimeoutTick,
    MetadataHeartbeat(&'static str, i32),
    UnknownHeartbeat,
    Replay,
}

#[derive(Clone, Debug)]
struct State {
    group: ShareGroupState,
    origin: Instant,
    clock: u8,
    partitions: i32,
    witnesses: u8,
}

type MemberProjection = (String, i32, Vec<String>, Vec<i32>, Instant);
type DurableMemberProjection = (String, i32, Vec<String>, Vec<i32>);
type GroupProjection = (
    i32,
    i32,
    bool,
    u8,
    i32,
    Vec<MemberProjection>,
    Vec<(String, Vec<i32>)>,
    u8,
);

impl State {
    fn projection(&self) -> GroupProjection {
        let mut members: Vec<MemberProjection> = self
            .group
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
                    subscriptions,
                    assigned,
                    member.last_seen,
                )
            })
            .collect();
        members.sort();
        let mut target: Vec<(String, Vec<i32>)> = self
            .group
            .target
            .per_member
            .iter()
            .map(|(member_id, assignment)| {
                let mut partitions = assignment.get(&TOPIC).cloned().unwrap_or_default();
                partitions.sort_unstable();
                (member_id.clone(), partitions)
            })
            .collect();
        target.sort();
        (
            self.group.group_epoch,
            self.group.target.epoch,
            self.group.dirty,
            self.clock,
            self.partitions,
            members,
            target,
            self.witnesses,
        )
    }
}

impl PartialEq for State {
    fn eq(&self, other: &Self) -> bool {
        self.projection() == other.projection()
    }
}

impl Eq for State {}

impl Hash for State {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.projection().hash(state);
    }
}

#[derive(Clone, Debug)]
struct ShareModel;

#[derive(Debug)]
struct ModelMetadata {
    partitions: i32,
}

impl MetadataProvider for ModelMetadata {
    fn snapshot(&self) -> ReconcileInput {
        ReconcileInput {
            topic_id_by_name: [(TOPIC_NAME.to_owned(), TOPIC)].into(),
            partitions_per_topic: [(TOPIC, self.partitions)].into(),
            ..Default::default()
        }
    }
}

fn metadata(partitions: i32) -> ModelMetadata {
    ModelMetadata { partitions }
}

fn at(state: &State) -> Instant {
    state
        .origin
        .checked_add(Duration::from_secs(u64::from(state.clock)))
        .expect("bounded model clock fits Instant")
}

fn assignment_coordinates_unique(group: &ShareGroupState) -> bool {
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

fn epochs_fenced(group: &ShareGroupState) -> bool {
    group.group_epoch >= 0
        && group.target.epoch >= 0
        && group.target.epoch <= group.group_epoch
        && group
            .members
            .values()
            .all(|member| member.member_epoch >= 0 && member.member_epoch <= group.group_epoch)
}

fn assignments_in_metadata(state: &State) -> bool {
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

type DurableProjection = (
    i32,
    i32,
    Vec<DurableMemberProjection>,
    Vec<(String, Vec<i32>)>,
);

fn durable_projection(group: &ShareGroupState) -> DurableProjection {
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

impl Model for ShareModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            group: ShareGroupState::new("g"),
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
                if !reconcile(&mut state.group, &metadata(state.partitions)) {
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
                match validate_member_epoch(Some(current), requested) {
                    Ok(_) => {
                        state.group.members.get_mut(member_id)?.last_seen = at(&state);
                        if !reconcile(&mut state.group, &metadata(state.partitions)) {
                            return None;
                        }
                        if state.group.target.epoch > current {
                            state.group.advance_member_epoch(member_id);
                        }
                    }
                    Err(error) => match kind {
                        EpochKind::Stale => {
                            assert2::assert!(error == crate::codes::STALE_MEMBER_EPOCH);
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
                    if !reconcile(&mut state.group, &metadata(state.partitions)) {
                        return None;
                    }
                }
            }
            Action::MetadataHeartbeat(member_id, partitions) => {
                let current = state.group.members.get(member_id)?.member_epoch;
                state.partitions = partitions;
                let before = state.group.group_epoch;
                if !reconcile(&mut state.group, &metadata(state.partitions)) {
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
                let error = validate_member_epoch(None, 0).expect_err("unknown member is rejected");
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

#[test]
fn share_group_membership_model() {
    let checker = ShareModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[share_group_membership] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH, "depth cap hit");
    assert2::assert!(checker.state_count() < MAX_STATES, "state cap hit");
    checker.assert_properties();
}
