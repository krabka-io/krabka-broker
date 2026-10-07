//! The projection between the model state and the real
//! [`GroupState`](crate::coordinator::unified::consumer_state::GroupState).
//!
//! Every transition rebuilds a real group from the enumerated state, drives the
//! real code over it, and projects the result back. Keeping both directions in
//! one file is what makes it easy to see that they are inverses. The
//! epoch-monotonicity check that every transition runs against the rebuilt
//! group lives here too, because it reads the same two representations.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use super::state::{MemberProj, ReconState};
use crate::coordinator::unified::{
    actor::reconciliation_model_support::{insert_modeled_member, modeled_member, parts_of},
    consumer_state::GroupState,
};

/// Rebuilds a real `GroupState` from the projection, so that the next real
/// call behaves exactly as it does in a live run.
///
/// The fields that the projection does not hold get faithful constants. The
/// subscription is fixed to the one topic, `last_seen` is constant, and
/// `previous_member_epoch` affects no decision.
pub(super) fn rebuild_group(s: &ReconState) -> GroupState {
    let mut g = GroupState::new("g");
    g.group_epoch = s.group_epoch;
    g.dirty = s.dirty;
    g.target.epoch = s.target_epoch;
    let now = Instant::now();
    for m in &s.members {
        let ms = modeled_member(
            &m.id,
            m.member_epoch,
            m.assignment_state,
            &m.assigned,
            &m.pending_revocation,
            now,
        );
        insert_modeled_member(&mut g, ms, &m.target);
    }
    g
}

/// Projects a real `GroupState`, the client ledger, and the advertised map
/// back into the hashable state.
pub(super) fn project(
    g: &GroupState,
    owned: &BTreeMap<String, BTreeSet<i32>>,
    advertised: &BTreeMap<String, Vec<i32>>,
) -> ReconState {
    let mut members: Vec<MemberProj> = g
        .members
        .values()
        .map(|m| MemberProj {
            id: m.member_id.clone(),
            member_epoch: m.member_epoch,
            assignment_state: m.assignment_state,
            assigned: parts_of(Some(&m.assigned_partitions)),
            pending_revocation: parts_of(Some(&m.partitions_pending_revocation)),
            target: parts_of(g.target.per_member.get(&m.member_id)),
        })
        .collect();
    members.sort_by(|a, b| a.id.cmp(&b.id));
    let client_owned: Vec<(String, Vec<i32>)> = owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect();
    let advertised: Vec<(String, Vec<i32>)> = advertised
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    ReconState {
        group_epoch: g.group_epoch,
        dirty: g.dirty,
        target_epoch: g.target.epoch,
        members,
        client_owned,
        advertised,
    }
}

/// Per-member epoch must never regress across a real step.
pub(super) fn assert_epoch_monotonic(pre: &ReconState, post: &GroupState) {
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
