use super::*;

impl Model for ReassignModel {
    type State = ReassignState;
    type Action = ReassignAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ReassignState {
            replicas: self.replicas.clone(),
            isr: self.initial_isr.clone(),
            adding: self.adding.clone(),
            removing: self.removing.clone(),
            leader: self.leader,
            leader_epoch: 0,
            alive: self.replicas.iter().copied().collect(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // AdmitToIsr: any replica not yet in ISR (models a catch-up + admit).
        for &r in &state.replicas {
            if !state.isr.contains(&r) {
                actions.push(ReassignAction::AdmitToIsr(r));
            }
        }
        // Die / Revive over the replica set (keep >= 1 alive).
        if state.alive.len() > 1 {
            for &r in &state.replicas {
                if state.alive.contains(&r) {
                    actions.push(ReassignAction::Die(r));
                }
            }
        }
        for &r in &state.replicas {
            if !state.alive.contains(&r) {
                actions.push(ReassignAction::Revive(r));
            }
        }
        // ReassignStep when in flight, under the epoch cap.
        if in_flight(state) && state.leader_epoch < self.max_epoch {
            actions.push(ReassignAction::ReassignStep);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ReassignAction::AdmitToIsr(n) => {
                if state.isr.contains(&n) || !state.replicas.contains(&n) {
                    return None;
                }
                // Rebuild ISR in canonical replica order (keeps the space small).
                state.isr = state
                    .replicas
                    .iter()
                    .copied()
                    .filter(|r| state.isr.contains(r) || *r == n)
                    .collect();
            }
            ReassignAction::Die(n) => {
                if last.alive.len() <= 1 || !state.alive.remove(&n) {
                    return None;
                }
            }
            ReassignAction::Revive(n) => {
                if !state.alive.insert(n) {
                    return None;
                }
            }
            ReassignAction::ReassignStep => {
                if !in_flight(&state) {
                    return None;
                }
                let pr = pr_of(&state);
                let alive: HashSet<NodeId> = state.alive.iter().copied().collect();
                {
                    let next = reassign_one(&pr, &alive)?;
                    assert_step(last, &next);
                    state.leader = next.leader;
                    state.isr = next.isr;
                    state.adding = next.adding_replicas;
                    state.removing = next.removing_replicas;
                    state.replicas = next.replicas;
                    state.leader_epoch = next.leader_epoch.0;
                }
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("isr_subset_replicas", |_, s: &ReassignState| {
                s.isr.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("leader_in_replicas", |_, s: &ReassignState| {
                s.replicas.contains(&s.leader)
            }),
            Property::always("leader_in_isr", |_, s: &ReassignState| {
                s.isr.contains(&s.leader)
            }),
            Property::always("adding_subset_replicas", |_, s: &ReassignState| {
                s.adding.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("removing_subset_replicas", |_, s: &ReassignState| {
                s.removing.iter().all(|n| s.replicas.contains(n))
            }),
            Property::sometimes("can_complete", |_, s: &ReassignState| {
                s.adding.is_empty() && s.removing.is_empty()
            }),
            // Config-conditional so it is not vacuously unsatisfiable in the
            // basic config (where no handoff happens).
            Property::sometimes("can_handoff", |m: &ReassignModel, s: &ReassignState| {
                !m.removing.contains(&m.leader) || s.leader != m.leader
            }),
            Property::sometimes("can_wait", |_, s: &ReassignState| {
                in_flight(s) && target_of(s).iter().any(|n| !s.isr.contains(n))
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_epoch <= self.max_epoch
    }
}
