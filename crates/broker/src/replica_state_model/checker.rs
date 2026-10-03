use super::*;

impl Model for IsrModel {
    type State = IsrState;
    type Action = IsrAction;

    fn init_states(&self) -> Vec<Self::State> {
        // Fresh leader: full replica set in the ISR, followers seeded at 0.
        let mut rs = ReplicaState::new();
        rs.install_isr(&self.replicas, &self.replicas, self.leader(), self.t0);
        vec![IsrState {
            rs,
            leader_leo: Offset(0),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let leader = self.leader();

        if state.leader_leo < self.max_offset {
            actions.push(IsrAction::LeaderAppend);
        }

        // Follower fetches: advance by one or jump to the leader's LEO. Targets
        // are monotonic (never below the follower's current LEO) — a real
        // follower's reported LEO never regresses, which is what keeps HW
        // monotone. `test_overshoot` additionally probes the defensive clamp.
        for f in self.followers() {
            let cur = state.rs.per_follower.get(&f).map_or(Offset(0), |s| s.leo);
            let mut targets: Vec<Offset> = Vec::new();
            if cur < state.leader_leo {
                targets.push(cur + 1);
                targets.push(state.leader_leo);
            }
            if self.test_overshoot {
                targets.push(state.leader_leo + 1);
            }
            targets.sort_unstable();
            targets.dedup();
            for leo in targets {
                actions.push(IsrAction::FollowerFetch { follower: f, leo });
            }
        }

        // ISR changes: every subset of replicas that contains the leader and
        // differs from the current ISR. Expansion only admits followers whose
        // log end reached the HW (per_follower.leo >= hw) — the high-watermark
        // half of the leader's rule, Kafka's `Partition.isFollowerInSync`;
        // without it the model would report a false data-loss violation.
        let cur_isr: HashSet<NodeId> = state.rs.isr.clone();
        let follower_vec: Vec<NodeId> = self.followers().collect();
        for mask in 0u32..(1u32 << follower_vec.len()) {
            let mut isr: Vec<NodeId> = vec![leader];
            for (i, &f) in follower_vec.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    isr.push(f);
                }
            }
            let isr_set: HashSet<NodeId> = isr.iter().copied().collect();
            if isr_set == cur_isr {
                continue;
            }
            let expansion_ok = isr
                .iter()
                .filter(|&&n| n != leader && !cur_isr.contains(&n))
                .all(|f| state.rs.per_follower.get(f).map_or(Offset(0), |s| s.leo) >= state.rs.hw);
            if !expansion_ok {
                continue;
            }
            isr.sort_unstable();
            actions.push(IsrAction::InstallIsr { isr });
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            IsrAction::LeaderAppend => {
                if state.leader_leo >= self.max_offset {
                    return None;
                }
                state.leader_leo += 1;
                state.rs.recompute_hw_for_leader_append(state.leader_leo);
            }
            IsrAction::FollowerFetch { follower, leo } => {
                state
                    .rs
                    .update_follower_leo(follower, leo, state.leader_leo, self.t0);
            }
            IsrAction::InstallIsr { isr } => {
                state
                    .rs
                    .install_isr(&isr, &self.replicas, self.leader(), self.t0);
            }
        }
        // Transition invariant (kept out of the fingerprinted state): the
        // high-watermark never regresses.
        assert2::assert!(
            state.rs.hw >= last.rs.hw,
            "HWM regressed: {} -> {}",
            last.rs.hw,
            state.rs.hw
        );
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("hw_within_leader", |_, s: &IsrState| {
                s.rs.hw <= s.leader_leo
            }),
            // No-committed-data-loss: every ISR member holds every committed
            // record. A missing per_follower entry for an ISR member counts as a
            // violation, although compute_hw reads one as log end -1 and holds
            // the HW for it.
            Property::always("no_data_loss", |m: &IsrModel, s: &IsrState| {
                let leader = m.leader();
                s.rs.isr
                    .iter()
                    .filter(|&&f| f != leader)
                    .all(|f| s.rs.per_follower.get(f).is_some_and(|st| st.leo >= s.rs.hw))
            }),
            Property::always("leo_clamped", |_, s: &IsrState| {
                s.rs.per_follower.values().all(|st| st.leo <= s.leader_leo)
            }),
            Property::always("hw_nonneg", |_, s: &IsrState| s.rs.hw >= 0),
            Property::always("leader_in_isr", |m: &IsrModel, s: &IsrState| {
                s.rs.isr.contains(&m.leader())
            }),
            Property::sometimes("can_advance_hw", |_, s: &IsrState| s.rs.hw > 0),
            Property::sometimes("can_reach_leader_leo", |_, s: &IsrState| {
                s.leader_leo > 0 && s.rs.hw == s.leader_leo
            }),
            Property::sometimes("can_pin_below_leader", |_, s: &IsrState| {
                s.rs.hw > 0 && s.rs.hw < s.leader_leo
            }),
            Property::sometimes("can_shrink_isr", |m: &IsrModel, s: &IsrState| {
                let leader = m.leader();
                m.replicas.iter().any(|&r| {
                    r != leader && !s.rs.isr.contains(&r) && s.rs.per_follower.contains_key(&r)
                })
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_leo <= self.max_offset
            && state.rs.hw <= self.max_offset
            && state
                .rs
                .per_follower
                .values()
                .all(|s| s.leo <= self.max_offset)
    }
}
