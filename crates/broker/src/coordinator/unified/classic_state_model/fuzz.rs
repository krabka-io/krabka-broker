//! Randomized companion to the exhaustive search: long proptest-generated op
//! sequences over a real `ClassicGroup`.
//!
//! The exhaustive model bounds the clock and the member pool so that the search
//! terminates. This module trades exhaustiveness for depth and runs sequences
//! far longer than the bounded search reaches, over the same handler guards and
//! the same invariants.

use std::time::Duration;

use bytes::Bytes;
use proptest::prelude::*;

use super::{
    fixtures::{at, mk_member},
    invariants::{index_coherent, single_owner},
};
use crate::coordinator::unified::classic_state::{ClassicGroup, GroupState};

#[derive(Clone, Debug)]
enum Op {
    JoinDynamic(u8),
    JoinStatic(u8, u8),
    Heartbeat(u8),
    Leave(u8),
    Complete,
    Sync,
    Expire,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u8..3).prop_map(Op::JoinDynamic),
        (0u8..2, 0u8..3).prop_map(|(i, m)| Op::JoinStatic(i, m)),
        (0u8..3).prop_map(Op::Heartbeat),
        (0u8..3).prop_map(Op::Leave),
        Just(Op::Complete),
        Just(Op::Sync),
        Just(Op::Expire),
    ]
}

proptest! {
    /// Large-N random op sequences over a real `ClassicGroup`, which mirror
    /// the handler fence guards and the leave retrigger. The membership
    /// invariants hold after every step.
    #[test]
    fn classic_invariants_hold(ops in proptest::collection::vec(op_strategy(), 0..300)) {
        let mids = ["a", "b", "c"];
        let iids = ["x", "y"];
        let mut g = ClassicGroup::new("g");
        let mut clock: i64 = 0;
        for op in ops {
            match op {
                Op::JoinDynamic(m) => {
                    let mid = mids[m as usize];
                    // Handler step 2b: a known member_id pinned to an instance
                    // can't rejoin as dynamic.
                    if g
                        .members
                        .get(mid)
                        .is_none_or(|mm| mm.group_instance_id.is_none())
                    {
                        g.add_member(mk_member(mid, None, clock));
                    }
                }
                Op::JoinStatic(i, m) => {
                    let iid = iids[i as usize];
                    let mid = mids[m as usize];
                    let member_mismatch = g
                        .members
                        .get(mid)
                        .is_some_and(|mm| mm.group_instance_id.as_deref() != Some(iid));
                    let fenced = g.current_member_id_for_instance(iid).is_some_and(|p| p != mid);
                    if !(member_mismatch || fenced) {
                        g.add_member(mk_member(mid, Some(iid), clock));
                    }
                }
                Op::Heartbeat(m) => {
                    if let Some(mm) = g.members.get_mut(mids[m as usize]) {
                        mm.last_heartbeat = at(clock);
                    }
                }
                Op::Leave(m) => {
                    g.remove_member(mids[m as usize]);
                    if !g.members.is_empty() && matches!(g.state, GroupState::Stable) {
                        g.state = GroupState::PreparingRebalance;
                        g.rebalance_from_empty = false;
                    }
                }
                Op::Complete => {
                    if matches!(g.state, GroupState::PreparingRebalance) && !g.members.is_empty()
                    {
                        g.complete_rebalance("range");
                    }
                }
                Op::Sync => {
                    if matches!(g.state, GroupState::CompletingRebalance) {
                        let a = g
                            .members
                            .keys()
                            .map(|k| (k.clone(), Bytes::from_static(b"a")))
                            .collect();
                        g.install_assignments(a);
                    }
                }
                Op::Expire => {
                    clock += 1;
                    let static_before: std::collections::HashSet<String> = g
                        .members
                        .iter()
                        .filter(|(_, m)| m.is_static())
                        .map(|(id, _)| id.clone())
                        .collect();
                    let dropped =
                        g.expire_dead_members(at(clock), Duration::from_secs(3));
                    for id in &dropped {
                        prop_assert!(!static_before.contains(id), "static member was expired");
                    }
                }
            }
            prop_assert!(index_coherent(&g), "index coherence");
            prop_assert!(single_owner(&g), "single owner");
            prop_assert!(
                g.joined_this_round.iter().all(|id| g.members.contains_key(id)),
                "joined subset"
            );
            prop_assert_eq!(
                g.members.is_empty(),
                matches!(g.state, GroupState::Empty),
                "empty iff Empty"
            );
        }
    }
}
