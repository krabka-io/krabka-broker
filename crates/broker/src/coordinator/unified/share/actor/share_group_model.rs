//! Bounded stateright model of the KIP-932 share-group membership machine.
//!
//! DRIVEN production code: [`ShareGroupState`] membership and timeout
//! transitions, the real share assignor through [`reconcile`], the shared
//! member-epoch fence, and the real snapshot/apply replay adapters. MODELED:
//! two member identities, one subscribed topic with one or two partitions, a
//! logical clock through four ticks, and group epochs through five. The search
//! explores join, current/stale/forward heartbeat, leave, timeout, metadata
//! resize, and one crash/replay in every reachable ordering. A stale heartbeat
//! sends the member epoch minus one: Kafka accepts it when it is the previous
//! member epoch or 0 (a rejoin), and fences it otherwise.
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
};

const TOPIC: Uuid = Uuid([42; 16]);

const TOPIC_NAME: &str = "t";

const MAX_CLOCK: u8 = 4;

const MAX_EPOCH: i32 = 5;

const MAX_STATES: usize = 1_000_000;

const MAX_DEPTH: usize = 64;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 71_656;

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

type MemberProjection = (String, i32, i32, Vec<String>, Vec<i32>, Instant);

type DurableMemberProjection = (String, i32, i32, Vec<String>, Vec<i32>);

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
                    member.previous_member_epoch,
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

type DurableProjection = (
    i32,
    i32,
    Vec<DurableMemberProjection>,
    Vec<(String, Vec<i32>)>,
);

#[path = "share_group_model/helpers.rs"]
mod helpers;
use helpers::{
    assignment_coordinates_unique, assignments_in_metadata, at, durable_projection, epochs_fenced,
    initialize, metadata,
};

#[path = "share_group_model/checker.rs"]
mod checker;

#[cfg(test)]
#[path = "share_group_model/tests.rs"]
mod tests;
