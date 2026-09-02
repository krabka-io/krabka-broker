//! The controller decisions of the model: the failover that the real
//! `failover_one` shape takes for an unreachable broker, and the KIP-460
//! preferred election that the real `select_new_leader_for_partition` takes.
//!
//! Both transitions call production code and then record its record into the
//! model state, so they share one file with the convergence test that the
//! availability property depends on.

use krabka_raft::NodeId;

use super::{
    block_on,
    config::{StretchModel, TOPIC},
    state::{StretchState, check_epoch},
};
use crate::{
    config_keys::RecoveryStrategy,
    leader_election::{ElectionType, FailoverDecision, select_new_leader_for_partition},
};

impl StretchModel {
    /// `true` when the controller has no failover work left. Every broker it
    /// cannot reach gives a decision that changes nothing.
    pub fn converged(&self, state: &StretchState) -> bool {
        let alive = self.alive(state);
        let record = self.record(state);
        self.replicas.iter().all(|&replica| {
            alive.contains(&replica)
                || matches!(
                    (self.elect)(
                        &record,
                        replica,
                        &alive,
                        &self.witnesses,
                        &[],
                        RecoveryStrategy::None,
                        false,
                    ),
                    FailoverDecision::NoChange | FailoverDecision::Unavailable
                )
        })
    }

    /// Run the controller failover decision for `dead` and apply it.
    pub fn apply_failover(&self, last: &StretchState, dead: NodeId) -> Option<StretchState> {
        let alive = self.alive(last);
        if alive.contains(&dead) {
            return None;
        }
        let decision = (self.elect)(
            &self.record(last),
            dead,
            &alive,
            &self.witnesses,
            &[],
            RecoveryStrategy::None,
            false,
        );
        let mut state = last.clone();
        match decision {
            FailoverDecision::Elect { leader, isr, .. } => {
                if last.leader_epoch >= self.max_epoch {
                    return None;
                }
                state.leader = leader;
                state.isr = isr;
                state.leader_epoch += 1;
            }
            FailoverDecision::ShrinkIsr { isr } => state.isr = isr,
            FailoverDecision::Recover(_)
            | FailoverDecision::Unavailable
            | FailoverDecision::NoChange => return None,
        }
        self.normalize_isr(&mut state.isr);
        check_epoch(last, &mut state);
        Some(state)
    }

    /// `true` when the preferred site holds a replica that can take
    /// leadership: alive, in the in-sync replica set, and not a witness.
    ///
    /// The model gives each site one broker, so that replica is `replicas[0]`,
    /// which is the one replica a Kafka preferred election considers. A site
    /// with a second broker would need this test to name `replicas[0]`
    /// directly.
    fn preferred_site_is_electable(&self, state: &StretchState) -> bool {
        let alive = self.alive(state);
        self.replicas.iter().any(|&replica| {
            self.site_of(replica) == self.preferred_site
                && !self.witnesses.contains(&replica)
                && alive.contains(&replica)
                && state.isr.contains(&replica)
        })
    }

    /// Run the real KIP-460 preferred election and apply its record.
    pub fn apply_preferred(&self, last: &StretchState) -> Option<StretchState> {
        let image = self.image(last);
        let liveness = self.liveness(last);
        let elected = block_on(select_new_leader_for_partition(
            &image,
            &liveness,
            &self.witnesses,
            TOPIC,
            0,
            ElectionType::Preferred,
        ));
        let mut state = last.clone();
        if let Ok(record) = elected {
            if last.leader_epoch >= self.max_epoch {
                return None;
            }
            state.leader = record.leader;
            state.isr = record.isr;
            state.leader_epoch = record.leader_epoch.0;
            self.normalize_isr(&mut state.isr);
        }
        // Leadership pinning: the preferred site keeps the leader whenever it
        // holds a replica that can take leadership.
        if self.preferred_site_is_electable(last)
            && self.site_of(state.leader) != self.preferred_site
        {
            state.preferred_pinning_broken = true;
        }
        check_epoch(last, &mut state);
        if state == *last {
            return None;
        }
        Some(state)
    }
}
