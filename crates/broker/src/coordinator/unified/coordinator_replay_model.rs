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

#[path = "coordinator_replay_model/helpers.rs"]
mod helpers;
use helpers::{bit, coherent, member_exists, mutation, tombstone};

#[path = "coordinator_replay_model/checker.rs"]
mod checker;

#[cfg(test)]
#[path = "coordinator_replay_model/tests.rs"]
mod tests;
