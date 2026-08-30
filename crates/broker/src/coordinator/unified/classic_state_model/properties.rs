//! The stateright [`Model`] implementation: the initial state, the enabled
//! actions, the transition function that drives the real `ClassicGroup`, the
//! search boundary, and the properties the checker proves.
//!
//! Each transition mirrors one handler guard and then calls the real
//! membership transition, so the guards and the code they protect read
//! together. The invariants the transitions assert live in the sibling
//! `invariants` module, which the properties below restate.

use std::time::Duration;

use bytes::Bytes;
use stateright::{Model, Property};

use super::{
    config::ClassicModel,
    fixtures::{at, mk_member},
    invariants::{index_coherent, single_owner},
    state::{Act, GrpState},
};
use crate::coordinator::unified::classic_state::{ClassicGroup, GroupState, Member};

impl Model for ClassicModel {
    type State = GrpState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![GrpState {
            g: ClassicGroup::new("g"),
            clock: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        for &mid in &self.members {
            actions.push(Act::JoinDynamic(mid));
            actions.push(Act::Heartbeat(mid));
            actions.push(Act::Leave(mid));
            for &iid in &self.instances {
                actions.push(Act::JoinStatic(iid, mid));
            }
        }
        if matches!(s.g.state, GroupState::PreparingRebalance) && !s.g.members.is_empty() {
            actions.push(Act::CompleteRebalance);
        }
        if matches!(s.g.state, GroupState::CompletingRebalance) {
            actions.push(Act::Sync);
        }
        if s.clock < self.max_clock {
            actions.push(Act::ExpireTick);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Act::JoinDynamic(mid) => {
                // Handler guard (classic_ops step 2b): a known member_id with a
                // different instance nature is fenced — here, a dynamic rejoin of
                // a member currently pinned to an instance.
                if s.g
                    .members
                    .get(mid)
                    .is_some_and(|m| m.group_instance_id.is_some())
                {
                    return None;
                }
                s.g.add_member(mk_member(mid, None, s.clock));
            }
            Act::JoinStatic(iid, mid) => {
                // Handler step 2b: a known member_id must keep a consistent
                // instance id (else the overwrite orphans the static index).
                if s.g
                    .members
                    .get(mid)
                    .is_some_and(|m| m.group_instance_id.as_deref() != Some(iid))
                {
                    return None;
                }
                // Handler step 3: instance id pinned to a different live member.
                if let Some(pinned) = s.g.current_member_id_for_instance(iid)
                    && pinned != mid
                {
                    return None;
                }
                s.g.add_member(mk_member(mid, Some(iid), s.clock));
            }
            Act::Heartbeat(mid) => {
                let m = s.g.members.get_mut(mid)?;
                m.last_heartbeat = at(s.clock);
            }
            Act::Leave(mid) => {
                if !s.g.members.contains_key(mid) {
                    return None;
                }
                s.g.remove_member(mid);
                // Mirror handle_leave: a member leaving a live (Stable) group
                // triggers a membership-change rebalance. (leader_id is NOT reset
                // here — it is best-effort, overwritten by the next
                // complete_rebalance; a stale leader is recovered via the
                // rebalance timeout, so it is not a safety invariant.)
                if !s.g.members.is_empty() && matches!(s.g.state, GroupState::Stable) {
                    s.g.state = GroupState::PreparingRebalance;
                    s.g.rebalance_from_empty = false;
                }
            }
            Act::CompleteRebalance => {
                if !matches!(s.g.state, GroupState::PreparingRebalance) || s.g.members.is_empty() {
                    return None;
                }
                s.g.complete_rebalance("range");
            }
            Act::Sync => {
                if !matches!(s.g.state, GroupState::CompletingRebalance) {
                    return None;
                }
                let assignments =
                    s.g.members
                        .keys()
                        .map(|id| (id.clone(), Bytes::from_static(b"a")))
                        .collect();
                s.g.install_assignments(assignments);
            }
            Act::ExpireTick => {
                s.clock += 1;
                let dropped = s.g.expire_dead_members(at(s.clock), Duration::from_secs(3));
                for id in &dropped {
                    assert2::assert!(
                        !last.g.members.get(id).is_some_and(Member::is_static),
                        "static member {id} was expired"
                    );
                }
            }
        }
        assert2::assert!(index_coherent(&s.g), "index coherence violated: {:?}", s.g);
        assert2::assert!(single_owner(&s.g), "single-owner violated: {:?}", s.g);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("index_coherence", |_, s: &GrpState| index_coherent(&s.g)),
            Property::always("single_owner_per_instance", |_, s: &GrpState| {
                single_owner(&s.g)
            }),
            Property::always("joined_subset", |_, s: &GrpState| {
                s.g.joined_this_round
                    .iter()
                    .all(|id| s.g.members.contains_key(id))
            }),
            Property::always("empty_iff_empty_state", |_, s: &GrpState| {
                s.g.members.is_empty() == matches!(s.g.state, GroupState::Empty)
            }),
            Property::sometimes("reached_stable", |_, s: &GrpState| {
                matches!(s.g.state, GroupState::Stable)
            }),
            Property::sometimes("instance_pinned", |_, s: &GrpState| {
                !s.g.static_members.is_empty()
            }),
            Property::sometimes("two_members", |_, s: &GrpState| s.g.members.len() >= 2),
            Property::sometimes("generation_bumped", |_, s: &GrpState| {
                s.g.generation_id >= 1
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.clock <= self.max_clock && s.g.members.len() <= self.members.len()
    }
}
