use super::*;

impl Model for BreakGlassModel {
    type State = ProposalState;
    type Action = Step;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ProposalState {
            approvals: Vec::new(),
            withdrawn: false,
            consumed: false,
            now_ms: 0,
            consumes: 0,
            under_approved: false,
        }]
    }

    fn actions(&self, _state: &Self::State, actions: &mut Vec<Self::Action>) {
        for principal in &self.principals {
            actions.push(Step::Approve(principal));
            actions.push(Step::Withdraw(principal));
        }
        actions.push(Step::Expire);
        actions.push(Step::Consume);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Step::Approve(principal) => self.settle(&mut state, principal, false),
            Step::Withdraw(principal) => self.settle(&mut state, principal, true),
            Step::Expire => state.now_ms = (state.now_ms + 1).min(EXPIRES_AT),
            Step::Consume => self.consume(&mut state),
        }
        // Headline safety, per transition. It fires the moment an interleaving
        // spends one approval twice, rather than at the end of the run.
        assert2::assert!(
            state.consumes <= 1,
            "a proposal was consumed twice after {action:?}: {state:?}"
        );
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_double_spend", |_, state: &ProposalState| {
                state.consumes <= 1
            }),
            Property::always("no_under_approved", |_, state: &ProposalState| {
                !state.under_approved
            }),
            Property::always(
                "a_withdrawn_proposal_is_never_consumed",
                |_, state: &ProposalState| !(state.withdrawn && state.consumes > 0),
            ),
            // Non-vacuity witnesses. Without them a model that refuses every
            // action would pass every safety property.
            Property::sometimes("consumed", |_, state: &ProposalState| state.consumes == 1),
            Property::sometimes("withdrawn", |_, state: &ProposalState| state.withdrawn),
            Property::sometimes("expired", |_, state: &ProposalState| {
                state.now_ms >= EXPIRES_AT
            }),
            Property::sometimes(
                "fully_approved",
                |model: &BreakGlassModel, state: &ProposalState| {
                    distinct(&state.approvals) >= model.config.required_approvals
                },
            ),
            Property::sometimes(
                "expired_before_it_was_spent",
                |model: &BreakGlassModel, state: &ProposalState| {
                    state.now_ms >= EXPIRES_AT
                        && distinct(&state.approvals) >= model.config.required_approvals
                        && state.consumes == 0
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.approvals.len() <= self.principals.len() && state.now_ms <= EXPIRES_AT
    }
}
