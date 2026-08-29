//! The `acks=all` write of the model. It mirrors the two gates that
//! `handlers::produce` applies, and it sets the sticky flag that
//! `minority_never_commits` reads.

use super::{
    config::StretchModel,
    state::{StretchState, WriteOutcome},
};

impl StretchModel {
    /// The outcome of one `acks=all` produce against the current leader. This
    /// mirrors the two gates of `handlers::produce`.
    pub fn produce_outcome(&self, state: &StretchState) -> WriteOutcome {
        let leader_site = self.site_of(state.leader);
        if state.down.contains(&leader_site) {
            // No leader runs, so the client gets NOT_LEADER_OR_FOLLOWER.
            return WriteOutcome::Rejected;
        }
        let isr_size = i32::try_from(state.isr.len()).expect("ISR size fits in i32");
        if isr_size < self.min_insync {
            // `validate_partition_gate`: NOT_ENOUGH_REPLICAS (19).
            return WriteOutcome::Rejected;
        }
        // The high watermark covers the append only after every in-sync
        // replica takes the record. A replica outside the leader's network
        // component never takes it, and the produce times out with
        // NOT_ENOUGH_REPLICAS_AFTER_APPEND (20).
        let component = self.component_of(state, leader_site);
        if state
            .isr
            .iter()
            .all(|&node| component.contains(&self.site_of(node)))
        {
            WriteOutcome::Committed
        } else {
            WriteOutcome::Rejected
        }
    }

    pub fn apply_produce(&self, state: &mut StretchState) {
        let outcome = self.produce_outcome(state);
        state.last_write = Some(outcome);
        if outcome == WriteOutcome::Committed && !self.leader_holds_majority(state) {
            state.commit_in_minority = true;
        }
    }
}
