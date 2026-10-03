//! Bounded Stateright model of KIP-1071 streams-group reconciliation.
//!
//! DRIVEN production code: [`StreamsGroupState`] membership and timeout
//! transitions, the real streams assignor through
//! [`compute_and_install_target`], the shared member-epoch fence, the port of
//! Kafka's `CurrentAssignmentBuilder` through
//! [`StreamsGroupState::reconcile_member`], and the real
//! snapshot/apply replay adapters. MODELED: two member identities, one
//! subtopology with one or two tasks, a logical clock through three ticks,
//! topology epochs through two, five group epoch bumps past Kafka's initial
//! group epoch 1, and one crash/replay in every reachable ordering.
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
use crate::coordinator::unified::streams::{
    config::{StreamsAssignorKind, StreamsGroupConfig},
    persistence::{StoredSubtopology, StreamsGroupTopologyValue},
    state::{
        INITIAL_EPOCH, OwnedTasks, RoleTasks, StreamsGroupState, StreamsGroupStatePhase,
        StreamsMemberAssignmentState, StreamsMemberState,
    },
};

const SUBTOPOLOGY: &str = "s";

const MAX_CLOCK: u8 = 3;

/// A new group starts at [`INITIAL_EPOCH`], so the bound allows five bumps.
const MAX_GROUP_EPOCH: i32 = INITIAL_EPOCH + 5;

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
// With the stale heartbeat restricted to epochs past `INITIAL_EPOCH` the count
// is the 37_688 of the model that started at group epoch 0: the start at 1 is a
// plain shift. The difference is a stale heartbeat at epoch 1 from a member at
// its first assigned epoch 2, which Kafka 4.3 fences and a start at 0 could
// not send, because its epoch 0 is a rejoin.
const PINNED_UNIQUE_STATES: usize = 37_960;

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

#[path = "streams_group_model/helpers.rs"]
mod helpers;
use helpers::{
    active_task_exclusive, assignments_in_topology, at, durable_projection, epochs_fenced,
    member_projection, phase_coherent, reconcile, reported_tasks, target_active_exclusive,
    target_projection, topology,
};

#[path = "streams_group_model/checker.rs"]
mod checker;

#[cfg(test)]
#[path = "streams_group_model/tests.rs"]
mod tests;
