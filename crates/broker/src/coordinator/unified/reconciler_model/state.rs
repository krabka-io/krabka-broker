//! The model state the checker enumerates, the actions it explores, and the
//! small accessors that read one member or one advertisement out of that state.
//!
//! Every field is a sorted `Vec` rather than a map, because stateright hashes
//! and compares each state, so the representation has to be canonical.

use std::collections::{BTreeMap, BTreeSet};

use crate::coordinator::unified::persistence_next_gen::MemberAssignmentState;

/// Per-member coordinator-side projection, which maps a single topic to a
/// `Vec<i32>`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct MemberProj {
    pub(super) id: String,
    pub(super) member_epoch: i32,
    pub(super) assignment_state: MemberAssignmentState,
    pub(super) assigned: Vec<i32>, // coordinator's authoritative current assignment
    pub(super) pending_revocation: Vec<i32>, // sorted
    pub(super) target: Vec<i32>,   // group.target.per_member (sorted)
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct ReconState {
    pub(super) group_epoch: i32,
    pub(super) dirty: bool,
    pub(super) target_epoch: i32,
    pub(super) members: Vec<MemberProj>, // sorted by id
    /// Ground-truth ledger: what each member consumes. It is sorted by id, and
    /// the partitions are sorted. The main invariant checks this observable.
    pub(super) client_owned: Vec<(String, Vec<i32>)>,
    /// The assignment that the coordinator last advertised to each member, in
    /// that member's last heartbeat response. The faithful client adds and
    /// revokes against THIS, not against the raw target, because a member
    /// learns its new assignment only when it heartbeats. It is sorted by id,
    /// and the partitions are sorted.
    pub(super) advertised: Vec<(String, Vec<i32>)>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum ReconAction {
    Join(String),
    Leave(String),
    Heartbeat(String),
    ClientAdd(String, i32),
    ClientRevoke(String, i32),
}

pub(super) fn owned_map(s: &ReconState) -> BTreeMap<String, BTreeSet<i32>> {
    s.client_owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect()
}

pub(super) fn advertised_map(s: &ReconState) -> BTreeMap<String, Vec<i32>> {
    s.advertised
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

pub(super) fn owned_to_vec(owned: &BTreeMap<String, BTreeSet<i32>>) -> Vec<(String, Vec<i32>)> {
    owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect()
}

pub(super) fn member<'a>(s: &'a ReconState, id: &str) -> Option<&'a MemberProj> {
    s.members.iter().find(|m| m.id == id)
}

pub(super) fn advertised_for(s: &ReconState, id: &str) -> Vec<i32> {
    s.advertised
        .iter()
        .find(|(k, _)| k == id)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}
