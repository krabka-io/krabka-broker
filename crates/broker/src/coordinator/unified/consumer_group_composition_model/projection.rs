//! The projection between the model state and the real
//! [`GroupState`](crate::coordinator::unified::consumer_state::GroupState),
//! which mirrors `reconciler_model`.
//!
//! Every transition rebuilds a real group from the enumerated state, drives the
//! real code over it, and projects the result back. Keeping both directions in
//! one file is what makes it easy to see that they are inverses.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::{Duration, Instant},
};

use krabka_log::Offset;
use krabka_protocol::primitives::uuid::Uuid;

use super::{
    TOPIC, TOPIC_NAME,
    state::{CgcState, MemberProj},
};
use crate::coordinator::unified::consumer_state::{GroupState, MemberState};

fn parts_of(map: Option<&HashMap<Uuid, Vec<i32>>>) -> Vec<i32> {
    let mut v: Vec<i32> = map.and_then(|m| m.get(&TOPIC)).cloned().unwrap_or_default();
    v.sort_unstable();
    v
}

fn to_map(parts: &[i32]) -> HashMap<Uuid, Vec<i32>> {
    if parts.is_empty() {
        HashMap::new()
    } else {
        [(TOPIC, parts.to_vec())].into()
    }
}

fn epochs_of(map: &HashMap<Uuid, HashMap<i32, i32>>) -> Vec<(i32, i32)> {
    let mut v: Vec<(i32, i32)> = map
        .get(&TOPIC)
        .map(|epochs| epochs.iter().map(|(&p, &e)| (p, e)).collect())
        .unwrap_or_default();
    v.sort_unstable();
    v
}

fn epochs_to_map(epochs: &[(i32, i32)]) -> HashMap<Uuid, HashMap<i32, i32>> {
    if epochs.is_empty() {
        HashMap::new()
    } else {
        [(TOPIC, epochs.iter().copied().collect())].into()
    }
}

pub(super) fn rebuild_group(s: &CgcState) -> GroupState {
    let mut g = GroupState::new("g");
    g.group_epoch = s.group_epoch;
    g.dirty = s.dirty;
    g.target.epoch = s.target_epoch;
    let now = Instant::now();
    for m in &s.members {
        let mut subs = HashSet::new();
        subs.insert(TOPIC_NAME.to_string());
        let ms = MemberState {
            member_id: m.id.clone(),
            instance_id: None,
            rack_id: None,
            client_id: String::new(),
            client_host: String::new(),
            subscribed_topic_names: subs,
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout: Duration::from_mins(1),
            member_epoch: m.member_epoch,
            previous_member_epoch: 0,
            assignment_state: m.assignment_state,
            assigned_partitions: to_map(&m.assigned),
            partitions_pending_revocation: to_map(&m.pending_revocation),
            assignment_epochs: epochs_to_map(&m.assignment_epochs),
            last_seen: now,
            classic: None,
        };
        g.members.insert(m.id.clone(), ms);
        if !m.target.is_empty() {
            g.target.per_member.insert(m.id.clone(), to_map(&m.target));
        }
    }
    g
}

pub(super) fn project(
    g: &GroupState,
    owned: &BTreeMap<String, BTreeSet<i32>>,
    advertised: &BTreeMap<String, Vec<i32>>,
    committed: &BTreeMap<i32, Offset>,
) -> CgcState {
    let mut members: Vec<MemberProj> = g
        .members
        .values()
        .map(|m| MemberProj {
            id: m.member_id.clone(),
            member_epoch: m.member_epoch,
            assignment_state: m.assignment_state,
            assigned: parts_of(Some(&m.assigned_partitions)),
            pending_revocation: parts_of(Some(&m.partitions_pending_revocation)),
            assignment_epochs: epochs_of(&m.assignment_epochs),
            target: parts_of(g.target.per_member.get(&m.member_id)),
        })
        .collect();
    members.sort_by(|a, b| a.id.cmp(&b.id));
    CgcState {
        group_epoch: g.group_epoch,
        dirty: g.dirty,
        target_epoch: g.target.epoch,
        members,
        client_owned: owned
            .iter()
            .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
            .collect(),
        advertised: advertised
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        committed: committed.iter().map(|(&k, &v)| (k, v)).collect(),
    }
}

pub(super) fn assert_epoch_monotonic(pre: &CgcState, post: &GroupState) {
    for pm in &pre.members {
        if let Some(m) = post.members.get(&pm.id) {
            assert2::assert!(
                m.member_epoch >= pm.member_epoch,
                "member_epoch regressed for {}: {} -> {}",
                pm.id,
                pm.member_epoch,
                m.member_epoch
            );
        }
    }
}
