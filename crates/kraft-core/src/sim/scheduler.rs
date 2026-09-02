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
    role::Role,
    types::{Epoch, NodeId, SimInstant},
};

/// Harness-level timer kinds.
///
/// This enum extends the core's `TimerKind`, which holds `Election`, `Fetch`
/// and `CheckQuorum`, with the leader `Heartbeat`. The core does not model the
/// heartbeat on a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimTimer {
    Election,
    Fetch,
    Heartbeat,
    CheckQuorum,
}

fn consider(
    best: &mut Option<(SimInstant, NodeId, SimTimer)>,
    deadline: SimInstant,
    id: NodeId,
    kind: SimTimer,
) {
    match best {
        Some((bd, _, _)) if *bd <= deadline => {}
        _ => *best = Some((deadline, id, kind)),
    }
}

impl Sim {
    // ---- fingerprint / stability ---------------------------------------------

    fn fingerprint(&self) -> Vec<(NodeId, &'static str, Epoch, usize, i64)> {
        self.nodes
            .values()
            .map(|n| {
                let hwm = match n.machine.role() {
                    Role::Leader { high_watermark, .. } => *high_watermark,
                    _ => n.high_watermark,
                };
                (
                    n.id,
                    n.machine.role().name(),
                    n.machine.quorum_state().leader_epoch,
                    n.log.record_count(),
                    hwm,
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
        let mut best: Option<(SimInstant, NodeId, SimTimer)> = None;
        for node in self.nodes.values() {
            if let Some(d) = node.election_deadline {
                consider(&mut best, d, node.id, SimTimer::Election);
            }
            if let Some(d) = node.fetch_deadline {
                consider(&mut best, d, node.id, SimTimer::Fetch);
            }
            if let Some(d) = node.heartbeat_deadline {
                consider(&mut best, d, node.id, SimTimer::Heartbeat);
            }
            if let Some(d) = node.check_quorum_deadline {
                consider(&mut best, d, node.id, SimTimer::CheckQuorum);
            }
        }
        let Some((deadline, id, kind)) = best else {
            return false;
        };
        if deadline > self.now {
            self.now = deadline;
        }
        {
            let node = self.nodes.get_mut(&id).unwrap();
            match kind {
                SimTimer::Election => node.election_deadline = None,
                SimTimer::Fetch => node.fetch_deadline = None,
                SimTimer::Heartbeat => node.heartbeat_deadline = None,
                SimTimer::CheckQuorum => node.check_quorum_deadline = None,
            }
        }
        match kind {
            SimTimer::Heartbeat => {
                self.fire_leader_heartbeat(id);
                true
            }
            SimTimer::Fetch => {
                if let Role::Follower { leader_id, .. }
                | Role::Observer {
                    leader_id: Some(leader_id),
                    ..
                } = *self.nodes[&id].machine.role()
                {
                    let leader_alive = !self.partitioned.contains(&id)
                        && !self.partitioned.contains(&leader_id)
                        && self
                            .nodes
                            .get(&leader_id)
                            .is_some_and(|n| n.machine.role().is_leader());
                    if leader_alive {
                        let deadline = self
                            .now
                            .saturating_add_ms(deadline_millis(election_timeout_of(id)));
                        self.nodes.get_mut(&id).unwrap().fetch_deadline = Some(deadline);
                        self.apply_action(id, Action::SendFetch { leader_id });
                        return true;
                    }
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
        match node.machine.role() {
            Role::Leader { .. } => {
                node.election_deadline = None;
                node.fetch_deadline = None;
                if node.heartbeat_deadline.is_none() {
                    node.heartbeat_deadline =
                        Some(self.now.saturating_add_ms(deadline_millis(HEARTBEAT)));
                }
            }
            Role::Follower { .. } | Role::Observer { .. } => {
                node.election_deadline = None;
                node.heartbeat_deadline = None;
                node.check_quorum_deadline = None;
            }
            Role::Unattached { .. }
            | Role::Voted { .. }
            | Role::Prospective { .. }
            | Role::Candidate { .. }
            | Role::Resigned => {
                node.fetch_deadline = None;
                node.heartbeat_deadline = None;
                node.check_quorum_deadline = None;
            }
        }
    }
}
