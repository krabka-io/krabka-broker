//! Bounded Stateright model of KIP-1071 streams-group reconciliation.
//!
//! DRIVEN production code: [`StreamsGroupState`] membership and timeout
//! transitions, the real streams assignor through
//! [`compute_and_install_target`], the shared member-epoch fence, active-task
//! withholding through [`StreamsGroupState::reconcile_member`], and the real
//! snapshot/apply replay adapters. MODELED: two member identities, one
//! subtopology with one or two tasks, a logical clock through three ticks,
//! topology epochs through two, group epochs through five, and one
//! crash/replay in every reachable ordering.
//!
//! The model counts a task pending revocation as still owned. Its exclusivity
//! property therefore prevents a target member from receiving an active task
//! until the previous owner reports that it released the task.

use std::{
    collections::{BTreeMap, HashSet},
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};

use stateright::{Checker, Model, Property};

use super::{
    ActorState,
    reconciliation::compute_and_install_target,
    records::{apply_seed, snapshot_seed},
};
use crate::coordinator::unified::{
    streams::{
        config::{StreamsAssignorKind, StreamsGroupConfig},
        persistence::{StoredSubtopology, StreamsGroupTopologyValue},
        state::{
            StreamsGroupState, StreamsGroupStatePhase, StreamsMemberAssignmentState,
            StreamsMemberState,
        },
    },
    validate_member_epoch,
};

const SUBTOPOLOGY: &str = "s";
const MAX_CLOCK: u8 = 3;
const MAX_GROUP_EPOCH: i32 = 5;
const MAX_TOPOLOGY_EPOCH: i32 = 2;
const MAX_STATES: usize = 1_000_000;
const MAX_DEPTH: usize = 64;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 43_256;
const WITNESS_STALE_FENCED: u16 = 1 << 0;
const WITNESS_FORWARD_FENCED: u16 = 1 << 1;
const WITNESS_UNKNOWN_FENCED: u16 = 1 << 2;
const WITNESS_TIMEOUT: u16 = 1 << 3;
const WITNESS_TOPOLOGY: u16 = 1 << 4;
const WITNESS_REPLAY: u16 = 1 << 5;
const WITNESS_WITHHELD: u16 = 1 << 6;
const WITNESS_RELEASED: u16 = 1 << 7;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ReportKind {
    Holding,
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Action {
    Join(&'static str),
    CurrentHeartbeat(&'static str, ReportKind),
    StaleHeartbeat(&'static str),
    ForwardHeartbeat(&'static str),
    Leave(&'static str),
    TimeoutTick,
    ChangeTopology(i32),
    UnknownHeartbeat,
    Replay,
}

type TaskMapProjection = Vec<(String, Vec<i32>)>;
type MemberProjection = (
    String,
    i32,
    i32,
    i8,
    TaskMapProjection,
    TaskMapProjection,
    TaskMapProjection,
    TaskMapProjection,
    Instant,
);
type DurableMemberProjection = (
    String,
    i32,
    i32,
    i8,
    TaskMapProjection,
    TaskMapProjection,
    TaskMapProjection,
    TaskMapProjection,
);
type TargetProjection = Vec<(String, TaskMapProjection)>;

#[derive(Debug, PartialEq, Eq, Hash)]
struct Projection {
    group_epoch: i32,
    assignment_epoch: i32,
    topology_epoch: i32,
    phase: &'static str,
    dirty: bool,
    members: Vec<MemberProjection>,
    target_active: TargetProjection,
    target_standby: TargetProjection,
    target_warmup: TargetProjection,
    clock: u8,
    partitions: i32,
    witnesses: u16,
    replayed: bool,
}

#[derive(Clone)]
struct State {
    actor: ActorState,
    origin: Instant,
    clock: u8,
    partitions: i32,
    witnesses: u16,
    replayed: bool,
}

impl std::fmt::Debug for State {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.projection().fmt(formatter)
    }
}

impl State {
    fn projection(&self) -> Projection {
        Projection {
            group_epoch: self.actor.state.group_epoch,
            assignment_epoch: self.actor.state.assignment_epoch,
            topology_epoch: self.actor.state.topology_epoch,
            phase: self.actor.state.phase.as_str(),
            dirty: self.actor.state.dirty,
            members: member_projection(&self.actor.state),
            target_active: target_projection(&self.actor.state.target.active),
            target_standby: target_projection(&self.actor.state.target.standby),
            target_warmup: target_projection(&self.actor.state.target.warmup),
            clock: self.clock,
            partitions: self.partitions,
            witnesses: self.witnesses,
            replayed: self.replayed,
        }
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
struct StreamsModel;

fn topology(epoch: i32) -> StreamsGroupTopologyValue {
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

fn reconcile(state: &mut State) -> bool {
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

fn at(state: &State) -> Instant {
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

fn target_projection(
    target: &std::collections::HashMap<String, BTreeMap<String, Vec<i32>>>,
) -> TargetProjection {
    let mut projection: TargetProjection = target
        .iter()
        .map(|(member, tasks)| (member.clone(), task_map_projection(tasks)))
        .collect();
    projection.sort();
    projection
}

fn member_projection(group: &StreamsGroupState) -> Vec<MemberProjection> {
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

type DurableProjection = (
    i32,
    i32,
    i32,
    &'static str,
    Vec<DurableMemberProjection>,
    TargetProjection,
    TargetProjection,
    TargetProjection,
);

fn durable_projection(actor: &ActorState) -> DurableProjection {
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

fn reported_active(state: &State, member_id: &str, kind: ReportKind) -> BTreeMap<String, Vec<i32>> {
    let member = &state.actor.state.members[member_id];
    let mut reported = member.active.clone();
    if kind == ReportKind::Holding {
        for (subtopology, partitions) in &member.active_pending_revocation {
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
}

fn active_task_exclusive(group: &StreamsGroupState) -> bool {
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

fn target_active_exclusive(group: &StreamsGroupState) -> bool {
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

fn assignments_in_topology(state: &State) -> bool {
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

fn epochs_fenced(group: &StreamsGroupState) -> bool {
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

fn phase_coherent(group: &StreamsGroupState) -> bool {
    if group.members.is_empty() {
        return group.phase == StreamsGroupStatePhase::Empty;
    }
    let reconciling = group
        .members
        .values()
        .any(|member| member.assignment_state != StreamsMemberAssignmentState::Stable);
    if reconciling {
        group.phase == StreamsGroupStatePhase::Reconciling
    } else {
        group.phase == StreamsGroupStatePhase::Stable
    }
}

impl Model for StreamsModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut actor = ActorState::new("g".to_string());
        let topology = topology(1);
        actor.state.topology_epoch = topology.epoch;
        actor.state.topology = Some(super::super::state::StoredTopologyHandle {
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
                state.actor.state.advance_member_epoch(member_id);
                state
                    .actor
                    .state
                    .reconcile_member(member_id, &BTreeMap::new());
            }
            Action::CurrentHeartbeat(member_id, report_kind) => {
                let current = state.actor.state.members.get(member_id)?.member_epoch;
                validate_member_epoch(Some(current), current).ok()?;
                let before_pending = !state.actor.state.members[member_id]
                    .active_pending_revocation
                    .is_empty();
                let before_unreleased = state.actor.state.members[member_id].assignment_state
                    == StreamsMemberAssignmentState::UnreleasedActiveTasks;
                let reported = reported_active(&state, member_id, report_kind);
                state.actor.state.members.get_mut(member_id)?.last_seen = at(&state);
                if state.actor.state.target.epoch > current {
                    state.actor.state.advance_member_epoch(member_id);
                }
                state.actor.state.reconcile_member(member_id, &reported);
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
                if requested == current {
                    return None;
                }
                let error = validate_member_epoch(Some(current), requested)
                    .expect_err("stale member epoch is rejected");
                assert2::assert!(error == crate::codes::STALE_MEMBER_EPOCH);
                state.witnesses |= WITNESS_STALE_FENCED;
            }
            Action::ForwardHeartbeat(member_id) => {
                let current = state.actor.state.members.get(member_id)?.member_epoch;
                let requested = current.checked_add(1)?;
                let error = validate_member_epoch(Some(current), requested)
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
                state.actor.state.topology = Some(super::super::state::StoredTopologyHandle {
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
                let error = validate_member_epoch(None, 0).expect_err("unknown member is rejected");
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

        if state.actor.state.members.values().any(|member| {
            member.assignment_state == StreamsMemberAssignmentState::UnreleasedActiveTasks
        }) {
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

#[test]
fn streams_group_reconciliation_model() {
    let checker = StreamsModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[streams_group_reconciliation] unique={} generated={} depth={}",
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
