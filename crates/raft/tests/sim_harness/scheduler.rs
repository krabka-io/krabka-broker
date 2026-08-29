//! The event loop: which timer fires next, how a queued message is delivered,
//! and how one event is fed to a node's consensus machine. This is the part of
//! the harness that makes the simulation deterministic.

use krabka_raft::kraft::{
    action::Action,
    event::Event,
    role::Role,
    types::{NodeId, SimInstant},
};

use super::{
    cluster::Sim,
    node::Message,
    node_log::SimNodeLog,
    timers::{HEARTBEAT_MS, SimTimer, consider, election_timeout_ms_of},
};

impl<L: SimNodeLog> Sim<L> {
    /// Finds the earliest armed timer across all nodes, advances the clock to
    /// it, and fires it. A partitioned node still ticks internally and still
    /// counts here. Returns `false` if no timer is armed.
    pub(super) fn fire_next_timer(&mut self) -> bool {
        // Pick the node with the earliest deadline; ties break by node id
        // (BTreeMap iteration is ascending by id, so the first minimum wins).
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
        }
        let Some((deadline, id, kind)) = best else {
            return false;
        };
        if deadline > self.now {
            self.now = deadline;
        }
        // Clear the fired timer; the handler re-arms below / via ResetTimer.
        {
            let node = self.nodes.get_mut(&id).unwrap();
            match kind {
                SimTimer::Election => node.election_deadline = None,
                SimTimer::Fetch => node.fetch_deadline = None,
                SimTimer::Heartbeat => node.heartbeat_deadline = None,
            }
        }
        match kind {
            SimTimer::Heartbeat => {
                self.fire_leader_heartbeat(id);
                true
            }
            SimTimer::Fetch => {
                // A fetch watchdog firing while the follower's leader is still
                // reachable is a routine long-poll expiry: re-poll the leader
                // rather than escalate to an election. Only when the leader is
                // gone (unreachable / unknown) does the watchdog become a real
                // `FetchTimeout` that elects. This mirrors `KRaft`, where continuous
                // polling resets the timer and only sustained silence elects.
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
                        let deadline = self.now.saturating_add_ms(election_timeout_ms_of(id));
                        self.nodes.get_mut(&id).unwrap().fetch_deadline = Some(deadline);
                        self.apply_action(id, Action::SendFetch { leader_id });
                        return true;
                    }
                }
                self.step(id, Event::FetchTimeout);
                true
            }
            SimTimer::Election => {
                self.step(id, Event::ElectionTimeout);
                true
            }
        }
    }

    /// A leader's periodic heartbeat. It re-broadcasts `BeginQuorumEpoch` to
    /// every peer, faithful to the `KRaft` resend to non-fetching voters, and
    /// re-arms the heartbeat. This is how a stale leader that rejoins after a
    /// partition learns of the newer epoch from the current leader and steps
    /// down to follower.
    fn fire_leader_heartbeat(&mut self, id: NodeId) {
        if !self.nodes[&id].machine.role().is_leader() {
            return;
        }
        let epoch = self.nodes[&id].machine.quorum_state().leader_epoch;
        self.apply_action(id, Action::SendBeginQuorumEpoch { epoch });
        let deadline = self.now.saturating_add_ms(HEARTBEAT_MS);
        self.nodes.get_mut(&id).unwrap().heartbeat_deadline = Some(deadline);
    }

    /// Delivers a queued message, and drops it if either endpoint is
    /// partitioned.
    pub(super) fn deliver(&mut self, msg: Message) {
        if self.partitioned.contains(&msg.src) || self.partitioned.contains(&msg.dst) {
            return;
        }
        if !self.nodes.contains_key(&msg.dst) {
            return;
        }
        self.step(msg.dst, msg.event);
    }

    /// Feeds one event to a node and translates the resulting actions into new
    /// messages, timer arming, and log and HWM bookkeeping.
    fn step(&mut self, id: NodeId, event: Event) {
        let now = self.now;
        // A `ReceiveFetch` is a leader-side request; remember who asked and the
        // leader epoch so we can synthesize the matching fetch *response* back to
        // the follower (the core only emits HWM/Truncate, not a response message).
        let fetch_from = if let Event::ReceiveFetch { from, .. } = &event {
            Some(*from)
        } else {
            None
        };
        // Run the machine. We must not hold a mutable borrow of `nodes` while we
        // re-borrow other nodes during action translation, so collect first.
        let actions = {
            let node = self.nodes.get_mut(&id).unwrap();
            node.machine.on_event(event, &node.log, now)
        };
        // If this was a fetch the leader served, reply to the follower (so it can
        // re-arm its fetch watchdog and truncate on divergence) — but only when
        // there is something to report: new data to replicate, or a divergence
        // hint. When the follower is already fully caught up, the leader's
        // long-poll *parks* with no immediate answer; the follower's watchdog
        // (re-armed below in `apply_action`) becomes the next event, and a
        // watchdog firing while the leader is still reachable is modelled as a
        // re-poll rather than an election (see `fire_next_timer`). This bounds the
        // steady-state fetch loop deterministically.
        if let Some(follower) = fetch_from {
            let diverging = actions.iter().find_map(|a| match a {
                Action::TruncateTo(point) => Some(*point),
                _ => None,
            });
            let leader_epoch = self.nodes[&id].machine.quorum_state().leader_epoch;
            if self.nodes[&id].machine.role().is_leader() {
                let leader_end = self.nodes[&id].log.end_offset();
                let follower_end = self.nodes[&follower].log.end_offset();
                let has_new_data = follower_end < leader_end;
                if diverging.is_some() || has_new_data {
                    self.send(
                        id,
                        follower,
                        Event::ReceiveFetchResponse {
                            leader_id: id,
                            leader_epoch,
                            diverging,
                        },
                    );
                }
            }
        }
        for action in actions {
            // A leader-side `TruncateTo` emitted while serving a fetch is a hint
            // *for the follower* (carried in the fetch response's `diverging`),
            // not an instruction to truncate the leader's own log — skip it.
            if fetch_from.is_some() && matches!(action, Action::TruncateTo(_)) {
                continue;
            }
            self.apply_action(id, action);
        }
        self.reconcile_timers_for_role(id);
    }

    /// Enforces per-role timer ownership, which the core does not fully manage
    /// through `ResetTimer` actions alone:
    ///
    /// - A leader runs neither an election timer nor a fetch timer. Its liveness
    ///   is a separate check-quorum mechanism, out of scope for slice 3a.
    /// - A follower or an observer runs only the fetch watchdog, and never an
    ///   election timer. But `handle_begin_quorum_epoch` emits only
    ///   `ResetTimer{Fetch}`, which leaves a previously-armed election timer
    ///   live. Without a clear of that timer, a healthy follower's stale
    ///   election timer fires, the follower goes `Prospective`, and the cluster
    ///   never stabilises.
    /// - An electing role, which is Unattached, Voted, Prospective, or
    ///   Candidate, runs only the election timer, and never a fetch watchdog.
    ///
    /// The core does arm the correct timer on each transition. This method only
    /// clears the stale opposite timer, so the harness scheduler matches the
    /// per-role timer model of `KRaft`.
    fn reconcile_timers_for_role(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        match node.machine.role() {
            Role::Leader { .. } => {
                node.election_deadline = None;
                node.fetch_deadline = None;
                // Arm the leader heartbeat if not already running.
                if node.heartbeat_deadline.is_none() {
                    node.heartbeat_deadline = Some(self.now.saturating_add_ms(HEARTBEAT_MS));
                }
            }
            Role::Follower { .. } | Role::Observer { .. } => {
                node.election_deadline = None;
                node.heartbeat_deadline = None;
            }
            Role::Unattached { .. }
            | Role::Voted { .. }
            | Role::Prospective { .. }
            | Role::Candidate { .. }
            | Role::Resigned => {
                node.fetch_deadline = None;
                node.heartbeat_deadline = None;
            }
        }
    }
}
