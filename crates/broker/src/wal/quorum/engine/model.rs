//! Bounded crash/failover model for the in-process three-replica WAL harness.
//!
//! Bounds: three voters, two one-record batches, three leader epochs, eight
//! ordered operations, and fewer than 20,000 generated states. Every append, quorum
//! acknowledgement, and crash recovery reconstructs real `Log` instances and
//! drives `Log::append`, `WalShardEngine::replicate_and_sync`, or
//! `WalShardEngine::new(OpenMode::Recover)`. Recovery performs the real tail
//! truncation and repair. Stateright enumerates replica failures and
//! revivals, and clean leader changes, around those operations.
//!
//! Scope: this is the in-process replica harness, the `#[cfg(test)]`
//! `WalShardEngine::new` and the local-replica branch of `replicate_and_sync`.
//! The production diskless path, where remote voters fetch the leader's tail
//! and report durable offsets (`record_follower_ack`, `serve_fetch`), is not
//! driven here; its unit tests in `engine/distributed.rs`, `engine.rs`, and
//! `follower.rs` cover it.
//!
//! DRIVEN: record-batch append, exact-range replica sync with byte
//! verification of each replica's whole retained log, quorum acknowledgement
//! and watermark advancement, and byte-agreement recovery and truncation.
//! Each acknowledgement is checked against its rule: it succeeds exactly when
//! at least two live voters (the leader among them) hold no record that
//! disagrees with the leader's log up to the target. Each recovery must
//! succeed; a recovery error fails the run instead of pruning the edge.
//!
//! MODELED: `KRaft` supplies a monotonically increasing leader epoch and
//! elects a new leader only when the current one is down. The model does
//! not derive leader completeness; it assumes it. `Elect(v)` is enabled only
//! when `v` is live and its log starts with the whole acknowledged prefix
//! (`has_committed_prefix`), which is what KIP-595's vote rule (a voter grants
//! only to a candidate whose log is at least as up to date) and quorum
//! intersection guarantee in production. Appends are the leader's own; a
//! deposed leader's append is fenced outside this engine (the registry
//! checks the leader epoch before it reaches a shard), so the model does not
//! enumerate one. Process crashes preserve each modelled disk exactly.
//! Filesystem calls are assumed atomic at the successful operation boundaries
//! exposed by `krabka-log`.

use std::sync::{Arc, Mutex};

use krabka_ids::Offset;
use krabka_kraft_core::NodeId;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::convert::ByteSizeExt as _;
use stateright::{Checker, Model, Property};

use super::{OpenMode, WalReplica, WalShardEngine, split_batches};

const VOTERS: usize = 3;

const MAX_RECORDS: usize = 2;

const MAX_EPOCH: u8 = 2;

const MAX_STEPS: u8 = 8;

const MAX_DEPTH: usize = 10;

const MAX_STATES: usize = 20_000;

// The exact unique-state count of the exhaustive BFS. A changed count is a
// changed reachable set -- a dropped action, a `next_state` arm that stops
// firing, a state field that stops being hashed -- and fails the run rather
// than silently shrinking the search. The generated count is not pinned: it
// depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 2_996;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WalState {
    steps: u8,
    logs: [Vec<u8>; VOTERS],
    leader: usize,
    leader_epoch: u8,
    live: u8,
    hwm: usize,
    committed: Vec<u8>,
    last_ack_failed: bool,
    recovered: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Action {
    Append,
    Acknowledge,
    Fail(usize),
    Revive(usize),
    Elect(usize),
    CrashRecover,
}

#[derive(Clone, Debug)]
struct WalModel;

#[path = "model/checker.rs"]
mod checker;

#[path = "model/helpers.rs"]
mod helpers;
use helpers::{
    ack_has_a_majority, drive_ack, drive_append, drive_recovery, has_committed_prefix, is_live,
};

#[cfg(test)]
#[path = "model/tests.rs"]
mod tests;
