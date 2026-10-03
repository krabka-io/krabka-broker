//! Exhaustive stateright model of KIP-101/279/320 log reconciliation between a
//! leader and its followers, driven through the production lookup
//! (`epoch_and_offset_for_entries`, Kafka's `LeaderEpochFileCache.endOffsetFor`)
//! and the production checkpoint truncation (`truncate_to`).
//!
//! # What is modeled
//!
//! A small cluster of replicas, each with a log -- one leader epoch per
//! offset, so a record is identified by `(offset, epoch)` -- and a leader
//! epoch checkpoint. Three actions interleave freely:
//!
//! * `Elect(r)` bumps the leader epoch and makes any replica `r` the leader,
//!   clean or unclean. As Kafka's `Partition.makeLeader` does, the new leader
//!   assigns the new epoch at its log end (`maybeAssignEpochStartOffset`), so
//!   an epoch in which a leader writes nothing still leaves a gap in every
//!   other replica's history.
//! * `Write` appends one record at the current epoch on the leader.
//! * `Fetch(f)` runs one follower fetch round: the follower sends its log end
//!   and its latest checkpoint epoch as `lastFetchedEpoch`; the leader answers
//!   as `Partition.readRecords` does (`OFFSET_OUT_OF_RANGE`, a
//!   `diverging_epoch`, or one record); and the follower acts on a
//!   `diverging_epoch` as `AbstractFetcherThread.getOffsetTruncationState`
//!   does, asking its *own* checkpoint `endOffsetFor` and truncating, over
//!   several rounds when it does not know the leader's epoch (KIP-279).
//!
//! # What is checked
//!
//! * `reconciled_follower_is_leader_prefix` (the headline) -- once the leader
//!   accepts a follower's position and serves it records, the follower's log
//!   is a prefix of the leader's: the divergent suffix is gone.
//! * `no_agreed_record_truncated` -- no truncation ever drops a record the
//!   follower shares with the leader (the common prefix survives).
//! * `divergence_makes_progress` -- every `diverging_epoch` changes the
//!   follower, so the protocol cannot livelock on it.
//! * `leader_places_every_follower_epoch` -- the leader never answers
//!   `OFFSET_OUT_OF_RANGE`: with the epoch assigned at election, its latest
//!   epoch bounds every follower's.
//! * `checkpoints_strictly_increasing` -- the lookup's precondition.
//!
//! `sometimes` witnesses show the interesting paths are reached: a divergent
//! suffix really truncated, a gap epoch resolved to the leader's floor epoch,
//! a KIP-279 step-back round, and every replica converged afterwards.
//!
//! # Bounds
//!
//! Two replicas with leader epochs through 5 and at most three records per
//! leader log, and three replicas with epochs through 3 and two records; each
//! search is exhaustive over every interleaving within those bounds.
//!
//! A third configuration -- two replicas, epochs through 4, three records --
//! drops the assign-at-election step and shows that the model is sharp: the
//! leader then cannot place a follower's newer epoch and answers
//! `OFFSET_OUT_OF_RANGE`, while the safety properties still hold.
//!
//! [`fuzz`](super::fuzz) drives the same step function over random schedules
//! at a scale this search cannot reach.

use krabka_ids::{LeaderEpoch, Offset};
use stateright::{Checker, Model, Property};

use super::{EpochEntry, epoch_and_offset_for_entries, truncate_to};

const MAX_STATES: usize = 2_000_000;

const MAX_DEPTH: usize = 64;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_TWO: usize = 35_274;

const PINNED_UNIQUE_STATES_THREE: usize = 20_212;

const PINNED_UNIQUE_STATES_NO_ASSIGN: usize = 10_865;

/// One replica: its log, one leader epoch per offset, and its checkpoint.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub(super) struct Replica {
    pub(super) log: Vec<LeaderEpoch>,
    pub(super) epochs: Vec<EpochEntry>,
}

impl Replica {
    pub(super) fn log_end(&self) -> Offset {
        Offset(i64::try_from(self.log.len()).expect("model log length fits in i64"))
    }

    /// Kafka's `LeaderEpochFileCache.assign`: a no-op for the latest epoch at
    /// or after its recorded start; otherwise drop every trailing entry that
    /// the new one does not strictly follow (`maybeTruncateNonMonotonicEntries`)
    /// and record it.
    pub(super) fn assign(&mut self, epoch: LeaderEpoch, start_offset: Offset) {
        if self
            .epochs
            .last()
            .is_some_and(|latest| latest.epoch == epoch && latest.start_offset <= start_offset)
        {
            return;
        }
        while self
            .epochs
            .last()
            .is_some_and(|last| last.epoch >= epoch || last.start_offset >= start_offset)
        {
            self.epochs.pop();
        }
        self.epochs.push(EpochEntry {
            epoch,
            start_offset,
        });
    }

    /// `UnifiedLog.append`: one record, with its batch epoch assigned.
    pub(super) fn append(&mut self, epoch: LeaderEpoch) {
        let offset = self.log_end();
        self.log.push(epoch);
        self.assign(epoch, offset);
    }

    /// `UnifiedLog.truncateTo`: the log, and the checkpoint through the
    /// production `truncate_to` (`LeaderEpochFileCache.truncateFromEnd`).
    pub(super) fn truncate(&mut self, offset: Offset) {
        self.log
            .truncate(usize::try_from(offset.0).expect("truncation offset is non-negative"));
        truncate_to(&mut self.epochs, offset);
    }

    /// `UnifiedLog.endOffsetForEpoch` over the production lookup.
    fn end_offset_for(&self, epoch: LeaderEpoch) -> (LeaderEpoch, Offset) {
        epoch_and_offset_for_entries(&self.epochs, epoch, self.log_end())
    }
}

/// The leader's answer to one follower fetch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum FetchAnswer {
    OutOfRange,
    Diverging {
        epoch: LeaderEpoch,
        end_offset: Offset,
    },
    Records,
}

/// A follower's truncation for one `diverging_epoch`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Truncation {
    pub(super) offset: Offset,
    /// `false` for a KIP-279 step-back: the follower does not know the
    /// leader's epoch and asks again from a lower one.
    pub(super) complete: bool,
}

/// Safety ghosts: each is set once, when its property is broken, and never
/// cleared.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub(super) struct Violations {
    /// A truncation dropped a record the follower shared with the leader.
    pub(super) over_truncated: bool,
    /// A `diverging_epoch` left the follower unchanged.
    pub(super) stalled: bool,
    /// The leader answered `OFFSET_OUT_OF_RANGE`.
    pub(super) out_of_range: bool,
}

/// Non-vacuity witnesses: each is set once, when its path is taken, and
/// never cleared.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub(super) struct Witnesses {
    pub(super) divergent_truncation: bool,
    pub(super) gap: bool,
    pub(super) step_back: bool,
}

/// The cluster: every replica, the current leader and epoch, and the ghost
/// facts the properties read.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct Cluster {
    pub(super) epoch: LeaderEpoch,
    pub(super) leader: usize,
    pub(super) replicas: Vec<Replica>,
    /// The leader accepted this follower's position since the last election
    /// or truncation.
    pub(super) reconciled: Vec<bool>,
    pub(super) violations: Violations,
    pub(super) witnesses: Witnesses,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum Action {
    Elect(usize),
    Write,
    Fetch(usize),
}

struct ReconcileModel {
    replicas: usize,
    max_epoch: i32,
    max_log: usize,
    assign_on_election: bool,
}

#[path = "leader_epoch_model/helpers.rs"]
mod helpers;
use helpers::{common_prefix, is_strictly_increasing};
pub(super) use helpers::{follower_truncation, leader_answer};

#[path = "leader_epoch_model/transitions.rs"]
mod transitions;

#[path = "leader_epoch_model/checker.rs"]
mod checker;

#[path = "leader_epoch_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "leader_epoch_model/tests.rs"]
mod tests;
