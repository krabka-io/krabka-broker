//! Unified `Group` container for KIP-848 migration.
//!
//! One in-memory model for a consumer group, whichever protocol its members
//! speak.
//!
//! A `Group` is a discriminated container over the two existing, tested state
//! machines: the classic 5-state machine ([`ClassicState`]) and the next-gen
//! epoch machine ([`ConsumerState`]). The unified coordinator and the
//! persistence path can therefore hold either one behind a single type.
//!
//! The actor can change the live kind during KIP-848 upgrade and downgrade.
//! The container keeps both protocol-specific state machines behind one surface
//! so the coordinator and persistence code always use the live state.

// The state machines are reused verbatim, relocated under `unified/`. These
// aliases give the unified surface its types without renaming the moved code
// (the classic file keeps its internal `Group`/`GroupState` names).
use std::collections::{HashMap, HashSet};

pub(crate) use crate::coordinator::unified::{
    classic_state::{ClassicGroup as ClassicState, OffsetEntry},
    consumer_state::GroupState as ConsumerState,
};

/// Which protocol the members of a [`Group`] speak. The variant carries that
/// protocol's full state machine.
#[derive(Debug)]
pub enum GroupKind {
    /// Classic `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` group.
    Classic(ClassicState),
    /// KIP-848 `ConsumerGroupHeartbeat` group.
    Consumer(ConsumerState),
}

/// A consumer group in the unified coordinator.
#[derive(Debug)]
pub struct CoordinatorGroup {
    pub group_id: String,
    pub kind: GroupKind,
    /// Committed offsets, from `__consumer_offsets` k0 and k1.
    ///
    /// They do not depend on the protocol. A group's offsets key by
    /// `(topic, partition)` whichever protocol its members speak, so they live
    /// on the container instead of inside either state machine. A later type
    /// change, in slices C to E, can therefore carry the committed offsets
    /// through a conversion untouched.
    pub committed_offsets: HashMap<(String, i32), OffsetEntry>,
    /// KIP-447: the `(topic, partition)` keys a transaction has written an
    /// offset commit for and that no commit or abort marker has resolved yet,
    /// grouped by the producer whose transaction wrote them.
    ///
    /// The offsets themselves are not here: they live in the log below the
    /// partition's LSO until the marker lands, and `WriteTxnMarkers`
    /// materializes them into `committed_offsets` only for a commit. These
    /// keys are what makes an `OffsetFetch` with `require_stable = true`
    /// answer `UNSTABLE_OFFSET_COMMIT` instead of the older stable offset.
    ///
    /// Grouping by producer is what lets one producer's marker resolve its own
    /// keys while another producer's in-flight transaction keeps its own.
    pending_txn_offsets: HashMap<i64, HashSet<(String, i32)>>,
}

/// A group's offset state as `OffsetFetch` needs to see it: the stable
/// committed offsets, plus the keys an unresolved transaction has written.
///
/// The two travel together because a `require_stable` fetch has to decide per
/// partition between them. Reading them in two actor turns would let a
/// transaction marker land in between and produce a row that is neither the
/// pre-transaction offset nor the post-transaction one.
#[derive(Debug, Default, Clone)]
pub struct GroupOffsets {
    pub committed: HashMap<(String, i32), OffsetEntry>,
    pub pending_txn: HashSet<(String, i32)>,
}

impl CoordinatorGroup {
    /// A fresh, empty classic group.
    pub fn new_classic(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        Self {
            kind: GroupKind::Classic(ClassicState::new(group_id.clone())),
            group_id,
            committed_offsets: HashMap::new(),
            pending_txn_offsets: HashMap::new(),
        }
    }

    /// A fresh, empty next-gen group, on the consumer protocol.
    pub fn new_consumer(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        Self {
            kind: GroupKind::Consumer(ConsumerState::new(group_id.clone())),
            group_id,
            committed_offsets: HashMap::new(),
            pending_txn_offsets: HashMap::new(),
        }
    }

    /// A group rebuilt from state that already exists: a replayed or
    /// hand-built state machine together with the committed offsets that go
    /// with it. No transaction is open on a group built this way; a replay
    /// that recovered one re-registers it with `add_pending_txn_offsets`.
    pub fn seeded(
        group_id: impl Into<String>,
        kind: GroupKind,
        committed_offsets: HashMap<(String, i32), OffsetEntry>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            kind,
            committed_offsets,
            pending_txn_offsets: HashMap::new(),
        }
    }

    /// Returns `true` if this group speaks the classic protocol.
    pub fn is_classic(&self) -> bool {
        matches!(self.kind, GroupKind::Classic(_))
    }

    /// Returns `true` if this group speaks the next-gen protocol.
    pub fn is_consumer(&self) -> bool {
        matches!(self.kind, GroupKind::Consumer(_))
    }

    pub fn as_classic(&self) -> Option<&ClassicState> {
        match &self.kind {
            GroupKind::Classic(s) => Some(s),
            GroupKind::Consumer(_) => None,
        }
    }

    pub fn as_classic_mut(&mut self) -> Option<&mut ClassicState> {
        match &mut self.kind {
            GroupKind::Classic(s) => Some(s),
            GroupKind::Consumer(_) => None,
        }
    }

    pub fn as_consumer(&self) -> Option<&ConsumerState> {
        match &self.kind {
            GroupKind::Consumer(s) => Some(s),
            GroupKind::Classic(_) => None,
        }
    }

    pub fn as_consumer_mut(&mut self) -> Option<&mut ConsumerState> {
        match &mut self.kind {
            GroupKind::Consumer(s) => Some(s),
            GroupKind::Classic(_) => None,
        }
    }

    /// Gives mutable access to the discriminated `kind`, so that a
    /// live-migration trigger can replace `Classic(..)` with `Consumer(..)` in
    /// place. This is the KIP-848 upgrade.
    pub fn kind_mut(&mut self) -> &mut GroupKind {
        &mut self.kind
    }

    /// Marks `keys` as written by `producer_id`'s open transaction.
    ///
    /// The caller must have made the offset-commit records durable first, so
    /// that the transaction's marker can always find the same keys again and
    /// clear them.
    pub fn add_pending_txn_offsets(
        &mut self,
        producer_id: i64,
        keys: impl IntoIterator<Item = (String, i32)>,
    ) {
        self.pending_txn_offsets
            .entry(producer_id)
            .or_default()
            .extend(keys);
    }

    /// Drops every pending mark `producer_id`'s transaction holds, whether its
    /// marker committed or aborted. Publishing the committed offsets is a
    /// separate step, because an abort publishes nothing.
    pub fn clear_pending_txn_offsets(&mut self, producer_id: i64) {
        self.pending_txn_offsets.remove(&producer_id);
    }

    /// The group's offset state for `OffsetFetch`, with every open
    /// transaction's pending keys flattened into one set.
    pub fn offsets(&self) -> GroupOffsets {
        GroupOffsets {
            committed: self.committed_offsets.clone(),
            pending_txn: self
                .pending_txn_offsets
                .values()
                .flatten()
                .cloned()
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn classic_container_exposes_classic_state_only() {
        let mut g = CoordinatorGroup::new_classic("g");
        check!(g.is_classic());
        check!(!g.is_consumer());
        check!(g.as_classic().is_some());
        check!(g.as_consumer().is_none());
        check!(g.as_classic_mut().is_some());
        check!(g.group_id == "g");
    }

    #[test]
    fn consumer_container_exposes_consumer_state_only() {
        let mut g = CoordinatorGroup::new_consumer("g");
        check!(g.is_consumer());
        check!(!g.is_classic());
        check!(g.as_consumer().is_some());
        check!(g.as_classic().is_none());
        check!(g.as_consumer_mut().is_some());
        check!(g.group_id == "g");
    }

    #[test]
    fn pending_txn_offsets_flatten_across_producers_and_clear_per_producer() {
        let mut g = CoordinatorGroup::new_classic("g");
        check!(g.offsets().pending_txn.is_empty());

        g.add_pending_txn_offsets(7, [("orders".to_string(), 0), ("orders".to_string(), 1)]);
        g.add_pending_txn_offsets(9, [("payments".to_string(), 3)]);
        check!(
            g.offsets().pending_txn
                == HashSet::from([
                    ("orders".to_string(), 0),
                    ("orders".to_string(), 1),
                    ("payments".to_string(), 3),
                ])
        );

        g.clear_pending_txn_offsets(7);
        check!(g.offsets().pending_txn == HashSet::from([("payments".to_string(), 3)]));

        g.clear_pending_txn_offsets(9);
        check!(g.offsets().pending_txn.is_empty());
    }
}
