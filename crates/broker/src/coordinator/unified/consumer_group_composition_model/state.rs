//! The model state the checker enumerates, the actions it explores, and the
//! small accessors that read one member, one advertisement, or one committed
//! offset out of that state.
//!
//! Every field is a sorted `Vec` rather than a map, because stateright hashes
//! and compares each state, so the representation has to be canonical.

use std::collections::{BTreeMap, BTreeSet};

use krabka_log::Offset;

use crate::coordinator::unified::persistence_next_gen::MemberAssignmentState;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct MemberProj {
    pub(super) id: String,
    pub(super) member_epoch: i32,
    pub(super) assignment_state: MemberAssignmentState,
    pub(super) assigned: Vec<i32>,
    pub(super) pending_revocation: Vec<i32>,
    pub(super) target: Vec<i32>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct CgcState {
    pub(super) group_epoch: i32,
    pub(super) dirty: bool,
    pub(super) target_epoch: i32,
    pub(super) members: Vec<MemberProj>, // sorted by id
    pub(super) client_owned: Vec<(String, Vec<i32>)>, // ground-truth ownership ledger
    pub(super) advertised: Vec<(String, Vec<i32>)>, // last advertised to each member
    pub(super) committed: Vec<(i32, Offset)>, // MODELED per-partition committed offset, sorted
}

/// Which epoch a member presents on an `OffsetCommit`: its current epoch (the
/// legitimate owner), one behind (a zombie from before the last rebalance), or
/// one ahead (an impossible/forward epoch). The real fence must accept only
/// `Current`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum EpochKind {
    Current,
    Stale,
    Forward,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum CgcAction {
    Join(String),
    Leave(String),
    Heartbeat(String),
    ClientAdd(String, i32),
    ClientRevoke(String, i32),
    Commit(String, i32, EpochKind), // (member, partition, presented-epoch) — fenced commit
}

pub(super) fn owned_map(s: &CgcState) -> BTreeMap<String, BTreeSet<i32>> {
    s.client_owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect()
}
pub(super) fn advertised_map(s: &CgcState) -> BTreeMap<String, Vec<i32>> {
    s.advertised
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
pub(super) fn committed_map(s: &CgcState) -> BTreeMap<i32, Offset> {
    s.committed.iter().copied().collect()
}
pub(super) fn owned_to_vec(owned: &BTreeMap<String, BTreeSet<i32>>) -> Vec<(String, Vec<i32>)> {
    owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect()
}
pub(super) fn member<'a>(s: &'a CgcState, id: &str) -> Option<&'a MemberProj> {
    s.members.iter().find(|m| m.id == id)
}
pub(super) fn advertised_for(s: &CgcState, id: &str) -> Vec<i32> {
    s.advertised
        .iter()
        .find(|(k, _)| k == id)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}
pub(super) fn committed_of(s: &CgcState, part: i32) -> Offset {
    s.committed
        .iter()
        .find(|(p, _)| *p == part)
        .map_or(Offset(0), |(_, o)| *o)
}
