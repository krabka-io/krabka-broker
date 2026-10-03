use super::*;

impl Model for ReconcileModel {
    type State = Cluster;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![Cluster::new(self.replicas, self.assign_on_election)]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        if s.epoch.0 < self.max_epoch {
            actions.extend((0..self.replicas).map(Action::Elect));
        }
        if s.replicas[s.leader].log.len() < self.max_log {
            actions.push(Action::Write);
        }
        actions.extend(
            (0..self.replicas)
                .filter(|&r| r != s.leader)
                .map(Action::Fetch),
        );
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = last.clone();
        next.step(action, self.assign_on_election).then_some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("reconciled_follower_is_leader_prefix", |_, s: &Cluster| {
                s.follower_prefix_holds()
            }),
            Property::always("no_agreed_record_truncated", |_, s: &Cluster| {
                !s.violations.over_truncated
            }),
            Property::always("divergence_makes_progress", |_, s: &Cluster| {
                !s.violations.stalled
            }),
            Property::always("leader_places_every_follower_epoch", |_, s: &Cluster| {
                !s.violations.out_of_range
            }),
            Property::always("checkpoints_strictly_increasing", |_, s: &Cluster| {
                s.checkpoints_hold()
            }),
            Property::sometimes("divergent_suffix_truncated", |_, s: &Cluster| {
                s.witnesses.divergent_truncation
            }),
            Property::sometimes("gap_epoch_resolved_to_floor", |_, s: &Cluster| {
                s.witnesses.gap
            }),
            Property::sometimes("step_back_truncation", |_, s: &Cluster| {
                s.witnesses.step_back
            }),
            Property::sometimes("converged_after_divergence", |_, s: &Cluster| {
                s.witnesses.divergent_truncation && s.converged()
            }),
        ]
    }
}
