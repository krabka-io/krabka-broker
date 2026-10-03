//! Exhaustive stateright enumeration of the KIP-227 forget+merge composition
//! (`super::apply_incremental`).
//!
//! The session-cache partition map is **stateful** across incremental fetches,
//! so a real state machine fits. The stateless quota lookup, by contrast, uses
//! exhaustive enumeration and proptest only. The model drives the REAL
//! forget+merge over sequences of incremental fetches. The topic references in
//! those fetches carry varying identity halves: name-only (Fetch v ≤ 12),
//! id-only (v ≥ 13), or both. The topic/id/partition universe is tiny, and the
//! model starts from a fully-resolved session. It asserts:
//!
//! - `no_shadow` (headline): no two cached keys ever refer to one logical
//!   partition, that is the same partition AND a shared non-trivial identity
//!   half. A shadow would silently corrupt subsequent reads. The merge-only
//!   shadow is already fixed and tested. This model targets the *composed*
//!   forget-then-merge path.
//! - **subscription fidelity** (per-transition): the cache reflects a
//!   subscribed partition with the requested `max_bytes`.
//! - `no_orphan_default`: no key carries default state (`max_bytes == 0`). A
//!   merge-created entry always takes the request's value.
//!
//! Note on determinism: the real merge resolves a double-match through
//! `HashMap` iteration order. A double-match is a request whose name matches
//! one cached key and whose id matches another. So `next_state` is not a pure
//! function of `(state, action)`. That is faithful to production and sound
//! here. Every possible resolution satisfies the asserted invariants, which are
//! choice-independent. The sorted projection also gives identical fingerprints
//! for identical resulting content, so the explored graph is well-defined.

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

use krabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
    primitives::uuid::Uuid as WireUuid,
};
use stateright::{Checker, Model, Property};

use super::{CachedPartitionState, FetchSessionKey, apply_incremental};

// Exhaustiveness is bounded on UNIQUE states (memory-proportional). The combined
// forget×sub action cross-product makes the *generated* (visited-edge) count
// ~100× the unique-state count — path convergence, not real growth — so
// `target_state_count` is only a truncation backstop set far above the natural
// generated total, and the real bound is `MAX_UNIQUE_STATES`.
const TARGET_STATE_COUNT: usize = 30_000_000;

const MAX_UNIQUE_STATES: usize = 500_000;

const MAX_DEPTH: usize = 30;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_BASIC: usize = 18;

const PINNED_UNIQUE_STATES_WIDE: usize = 148;

// Tiny symbolic universe. Names {A,B}, ids {U,V} (a rename = same id, new name).
const NAME_A: &str = "A";

const NAME_B: &str = "B";

/// A client topic reference and which identity halves it carries. `tag` 1 = id
/// U and `tag` 2 = id V. The name is one of {A, B}. `Both(B, 1)` models a
/// rename of topic A (id U) to name B. `IdOnly(1)` and `NameOnly(A)` model the
/// half-identity wire forms.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Ref {
    Both(&'static str, u8),
    NameOnly(&'static str),
    IdOnly(u8),
}

#[derive(Clone, Debug)]
struct CacheState {
    partitions: HashMap<FetchSessionKey, CachedPartitionState>,
}

impl CacheState {
    /// Canonical, hashable projection: (name, id-bytes, partition, `max_bytes`),
    /// sorted. `max_bytes` distinguishes a real entry from a default-state
    /// shadow. The other `CachedPartitionState` fields are irrelevant here.
    fn proj(&self) -> Vec<(String, [u8; 16], i32, i32)> {
        let mut v: Vec<_> = self
            .partitions
            .iter()
            .map(|(k, s)| (k.topic_name.clone(), k.topic_id.0, k.partition, s.max_bytes))
            .collect();
        v.sort();
        v
    }
}

impl PartialEq for CacheState {
    fn eq(&self, o: &Self) -> bool {
        self.proj() == o.proj()
    }
}

impl Eq for CacheState {}

impl Hash for CacheState {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.proj().hash(h);
    }
}

/// One incremental fetch: optionally forget one (ref, partition), optionally
/// subscribe one (ref, partition, `max_bytes`). A real fetch can carry both.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Fetch {
    forget: Option<(Ref, i32)>,
    sub: Option<(Ref, i32, i32)>,
}

struct FsModel {
    refs: Vec<Ref>,
    partitions: Vec<i32>,
}

#[path = "fetch_session_model/helpers.rs"]
mod helpers;
use helpers::{fetch_topic, forgotten_topic, id_of, no_shadow, ref_matches};

#[path = "fetch_session_model/checker.rs"]
mod checker;

#[path = "fetch_session_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "fetch_session_model/tests.rs"]
mod tests;
