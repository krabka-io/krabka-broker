//! The stateright [`Model`] implementation: the initial state, the enabled
//! actions, the transition function, the search boundary, and the properties
//! the checker proves.
//!
//! The transitions themselves live in the sibling modules. This file only
//! dispatches to them and states what must hold.

use std::{
    collections::{BTreeSet, HashSet},
    time::Instant,
};

use stateright::{Model, Property};

use super::{
    MAX_OFFSET,
    commit::do_commit,
    config::{CgcModel, config},
    heartbeat::{advertised_of, hb_request},
    projection::{assert_epoch_monotonic, project, rebuild_group},
    state::{
        CgcAction, CgcState, EpochKind, advertised_for, advertised_map, committed_map,
        committed_of, member, owned_map, owned_to_vec,
    },
};
use crate::coordinator::unified::{ClientIdentity, actor::step_heartbeat};

impl Model for CgcModel {
    type State = CgcState;
    type Action = CgcAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CgcState {
            group_epoch: 0,
            dirty: false,
            target_epoch: 0,
            members: vec![],
            client_owned: vec![],
            advertised: vec![],
            committed: vec![],
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let under_cap = state.group_epoch < self.max_epoch;
        if under_cap {
            for &id in &self.pool {
                if member(state, id).is_none() {
                    actions.push(CgcAction::Join(id.to_string()));
                }
            }
        }
        for m in &state.members {
            if under_cap {
                actions.push(CgcAction::Leave(m.id.clone()));
                actions.push(CgcAction::Heartbeat(m.id.clone()));
            }
            let advertised = advertised_for(state, &m.id);
            let owned: BTreeSet<i32> = state
                .client_owned
                .iter()
                .find(|(k, _)| k == &m.id)
                .map(|(_, v)| v.iter().copied().collect())
                .unwrap_or_default();
            for &tp in &advertised {
                if !owned.contains(&tp) {
                    actions.push(CgcAction::ClientAdd(m.id.clone(), tp));
                }
            }
            for &tp in &owned {
                if !advertised.contains(&tp) {
                    actions.push(CgcAction::ClientRevoke(m.id.clone(), tp));
                }
            }
            // Offset commit: offered with EACH epoch kind (current / stale /
            // forward) so the real epoch fence — not a precondition — is what's
            // exercised; the committed counter is bounded by MAX_OFFSET.
            for part in 0..self.partitions {
                if committed_of(state, part) < MAX_OFFSET {
                    for kind in [EpochKind::Current, EpochKind::Stale, EpochKind::Forward] {
                        actions.push(CgcAction::Commit(m.id.clone(), part, kind));
                    }
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut owned = owned_map(last);
        let mut adv = advertised_map(last);
        let committed = committed_map(last);
        match action {
            CgcAction::ClientAdd(id, tp) => {
                let advertised_has = advertised_for(last, &id).contains(&tp);
                let entry = owned.entry(id).or_default();
                if !advertised_has || entry.contains(&tp) {
                    return None;
                }
                entry.insert(tp);
                let mut next = last.clone();
                next.client_owned = owned_to_vec(&owned);
                Some(next)
            }
            CgcAction::ClientRevoke(id, tp) => {
                let advertised_has = advertised_for(last, &id).contains(&tp);
                let entry = owned.entry(id).or_default();
                if advertised_has || !entry.contains(&tp) {
                    return None;
                }
                entry.remove(&tp);
                let mut next = last.clone();
                next.client_owned = owned_to_vec(&owned);
                Some(next)
            }
            CgcAction::Join(id) => {
                if member(last, &id).is_some() {
                    return None;
                }
                let mut g = rebuild_group(last);
                let req = hb_request(&id, 0, &BTreeSet::new());
                let step = step_heartbeat(
                    &mut g,
                    &config(),
                    &self.metadata(),
                    &req,
                    ClientIdentity { id: "", host: "" },
                    Instant::now(),
                );
                assert_epoch_monotonic(last, &g);
                owned.entry(id.clone()).or_default();
                adv.insert(id, advertised_of(&step));
                Some(project(&g, &owned, &adv, &committed))
            }
            CgcAction::Leave(id) => {
                member(last, &id)?;
                let mut g = rebuild_group(last);
                let req = hb_request(&id, -1, &BTreeSet::new());
                let _ = step_heartbeat(
                    &mut g,
                    &config(),
                    &self.metadata(),
                    &req,
                    ClientIdentity { id: "", host: "" },
                    Instant::now(),
                );
                assert_epoch_monotonic(last, &g);
                owned.remove(&id);
                adv.remove(&id);
                Some(project(&g, &owned, &adv, &committed))
            }
            CgcAction::Heartbeat(id) => {
                let epoch = member(last, &id)?.member_epoch;
                let cur_owned: BTreeSet<i32> = owned.get(&id).cloned().unwrap_or_default();
                let mut g = rebuild_group(last);
                let req = hb_request(&id, epoch, &cur_owned);
                let step = step_heartbeat(
                    &mut g,
                    &config(),
                    &self.metadata(),
                    &req,
                    ClientIdentity { id: "", host: "" },
                    Instant::now(),
                );
                assert_epoch_monotonic(last, &g);
                adv.insert(id, advertised_of(&step));
                Some(project(&g, &owned, &adv, &committed))
            }
            CgcAction::Commit(id, part, kind) => do_commit(last, &id, part, kind),
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: no two members ever simultaneously own the same partition
            // (the real reconciliation's withholding — re-verified in the composed
            // context, with offset traffic interleaved).
            Property::always("exclusive_ownership", |_, s: &CgcState| {
                let mut seen: HashSet<i32> = HashSet::new();
                for (_, parts) in &s.client_owned {
                    for &p in parts {
                        if !seen.insert(p) {
                            return false;
                        }
                    }
                }
                true
            }),
            // A member is never advertised a partition another member currently
            // owns — the coordinator-side withholding invariant.
            Property::always(
                "advertised_disjoint_from_others_owned",
                |_, s: &CgcState| {
                    for (mid, adv) in &s.advertised {
                        for &p in adv {
                            if s.client_owned
                                .iter()
                                .any(|(k, v)| k != mid && v.contains(&p))
                            {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // The real OffsetCommit epoch fence agrees with the independent oracle
            // is enforced as a per-transition equality assertion in the Commit arm (a
            // divergence is a real `validate_commit_decision` regression). The
            // value here is the COMPOSITION: the epochs that fence drives are set
            // by the real reconciliation, so a zombie from before a rebalance is
            // rejected — see the `member_epoch_advanced` witness.

            // ----- non-vacuity witnesses -----
            // A current-epoch commit was accepted (the fence's accept path fires).
            Property::sometimes("offset_advanced", |_, s: &CgcState| {
                s.committed.iter().any(|&(_, o)| o > 0)
            }),
            // The reconciliation actually advanced a member's epoch past its first
            // generation — so `Stale`/`Forward` commits are genuinely distinct from
            // `Current` and the fence is exercised over non-trivial epochs (a real
            // zombie scenario: a stale commit after a rebalance bumped the epoch).
            Property::sometimes("member_epoch_advanced", |_, s: &CgcState| {
                s.members.iter().any(|m| m.member_epoch >= 2)
            }),
            // A handoff state: a partition is in one member's target while another
            // member currently owns it (the baton is mid-pass).
            Property::sometimes("handoff_witness", |_, s: &CgcState| {
                for m in &s.members {
                    for &tp in &m.target {
                        if s.client_owned
                            .iter()
                            .any(|(k, v)| k != &m.id && v.contains(&tp))
                        {
                            return true;
                        }
                    }
                }
                false
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.group_epoch <= self.max_epoch && state.committed.iter().all(|&(_, o)| o <= MAX_OFFSET)
    }
}
