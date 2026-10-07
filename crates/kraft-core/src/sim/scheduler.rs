//! The simulated timer wheel and the loop that settles the cluster.
//!
//! This module picks the earliest-due election, fetch, or heartbeat deadline
//! across every node, advances the logical clock to it, and fires it. It also
//! holds `run_until_stable`, which drains the bus and fires timers until the
//! cluster fingerprint stops changing, and the role reconciliation that decides
//! which of a node's timers stay armed.

use super::{
    Sim,
    node::{HEARTBEAT, deadline_millis, election_timeout_of},
    trace::TraceAction,
};
use crate::{
    action::Action,
    event::Event,
    simulation_support::SimulationTimer as SimTimer,
    types::{Epoch, NodeId},
};

impl Sim {
    // ---- fingerprint / stability ---------------------------------------------

    fn fingerprint(&self) -> Vec<(NodeId, &'static str, Epoch, usize, i64)> {
        self.nodes
            .values()
            .map(|n| {
                crate::simulation_support::fingerprint(
                    n.id,
                    &n.machine,
                    n.log.record_count(),
                    n.high_watermark,
                )
            })
            .collect()
    }

    /// Run the scheduler until the cluster fingerprint stops changing or until
    /// `max_ticks` is reached.
    ///
    /// Both the curated scenarios and the playground's "settle" button call
    /// this method.
    pub fn run_until_stable(&mut self, max_ticks: usize) {
        let mut last_fingerprint = self.fingerprint();
        let mut stable_rounds = 0u32;
        for _ in 0..max_ticks {
            if let Some(msg) = self.queue.pop_front() {
                self.deliver(&msg);
                continue;
            }
            let fired = self.fire_next_timer();
            let fp = self.fingerprint();
            if fp == last_fingerprint {
                stable_rounds += 1;
                if stable_rounds >= 2 {
                    return;
                }
            } else {
                stable_rounds = 0;
                last_fingerprint = fp;
            }
            if !fired && self.queue.is_empty() {
                return;
            }
        }
    }

    pub(super) fn fire_next_timer(&mut self) -> bool {
        let best = crate::simulation_support::earliest_timer(self.nodes.values().map(|node| {
            (
                node.id,
                [
                    node.election_deadline,
                    node.fetch_deadline,
                    node.heartbeat_deadline,
                    node.check_quorum_deadline,
                ],
            )
        }));
        let Some((deadline, id, kind)) = best else {
            return false;
        };
        if deadline > self.now {
            self.now = deadline;
        }
        {
            let node = self.nodes.get_mut(&id).unwrap();
            crate::simulation_support::clear_timer(
                kind,
                &mut node.election_deadline,
                &mut node.fetch_deadline,
                &mut node.heartbeat_deadline,
                &mut node.check_quorum_deadline,
            );
        }
        match kind {
            SimTimer::Heartbeat => {
                self.fire_leader_heartbeat(id);
                true
            }
            SimTimer::Fetch => {
                if let Some(leader_id) = crate::simulation_support::reachable_leader(
                    self.nodes[&id].machine.role(),
                    id,
                    &self.partitioned,
                    |leader| {
                        self.nodes
                            .get(&leader)
                            .is_some_and(|node| node.machine.role().is_leader())
                    },
                ) {
                    let deadline = self
                        .now
                        .saturating_add_ms(deadline_millis(election_timeout_of(id)));
                    self.nodes.get_mut(&id).unwrap().fetch_deadline = Some(deadline);
                    self.apply_action(id, Action::SendFetch { leader_id });
                    return true;
                }
                self.record(
                    TraceAction::Timeout {
                        node: id.0,
                        kind: "fetch".to_string(),
                    },
                    format!("N{id} lost contact with its leader and starts an election"),
                );
                self.step(id, Event::FetchTimeout);
                true
            }
            SimTimer::Election => {
                self.record(
                    TraceAction::Timeout {
                        node: id.0,
                        kind: "election".to_string(),
                    },
                    format!("N{id}'s election timer fires"),
                );
                self.step(id, Event::ElectionTimeout);
                true
            }
            SimTimer::CheckQuorum => {
                self.record(
                    TraceAction::Timeout {
                        node: id.0,
                        kind: "check-quorum".to_string(),
                    },
                    format!("N{id} has not heard from a majority of voters and resigns"),
                );
                self.step(id, Event::CheckQuorumTimeout);
                true
            }
        }
    }

    fn fire_leader_heartbeat(&mut self, id: NodeId) {
        if !self.nodes[&id].machine.role().is_leader() {
            return;
        }
        let epoch = self.nodes[&id].machine.quorum_state().leader_epoch;
        self.apply_action(id, Action::SendBeginQuorumEpoch { epoch });
        let deadline = self.now.saturating_add_ms(deadline_millis(HEARTBEAT));
        self.nodes.get_mut(&id).unwrap().heartbeat_deadline = Some(deadline);
    }

    pub(super) fn reconcile_timers_for_role(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        crate::simulation_support::reconcile_timers(
            node.machine.role(),
            &mut node.election_deadline,
            &mut node.fetch_deadline,
            &mut node.heartbeat_deadline,
            &mut node.check_quorum_deadline,
            self.now.saturating_add_ms(deadline_millis(HEARTBEAT)),
        );
    }
}
