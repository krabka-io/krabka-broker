use super::*;

impl ConsensusModel {
    pub(super) fn model_next_state(
        &self,
        last: &<Self as Model>::State,
        action: <Self as Model>::Action,
    ) -> Option<<Self as Model>::State> {
        let mut state = last.clone();
        state.step_witness = None;
        match action {
            ModelAction::Deliver(env) => {
                if !state.network.remove(&env) {
                    return None;
                }
                // A crashed destination is unreachable: the message is consumed
                // (removed) but produces no transition.
                if !state.crashed.contains(&env.dst) {
                    self.step(&mut state, env.dst, env.event);
                }
            }
            ModelAction::DropMsg(env) => {
                // Network loss: remove without delivering. No-op if already gone.
                if !state.network.remove(&env) {
                    return None;
                }
            }
            ModelAction::DuplicateDeliver(env) => {
                // Network duplication: deliver a copy, leave the original queued.
                if !state.network.contains(&env) {
                    return None;
                }
                if !state.crashed.contains(&env.dst) {
                    self.step(&mut state, env.dst, env.event);
                }
            }
            ModelAction::Crash(id) => {
                if !state.crashed.insert(id) {
                    return None;
                }
                // Omission model: drop all messages to/from the crashed node.
                state.network.retain(|e| e.src != id && e.dst != id);
            }
            ModelAction::Recover(id) => {
                if !state.crashed.remove(&id) {
                    return None;
                }
            }
            ModelAction::Timeout(id, kind) => {
                let event = match kind {
                    TimerKind::Election => Event::ElectionTimeout,
                    TimerKind::Fetch => Event::FetchTimeout,
                    TimerKind::CheckQuorum => Event::CheckQuorumTimeout,
                };
                self.step(&mut state, id, event);
                if kind == TimerKind::CheckQuorum && self.voter_ids.len() > 1 {
                    // The core re-arms this timer only when a majority has
                    // fetched, so an expiry that leaves the node still leading
                    // means an isolated leader kept its epoch. That is the
                    // whole defect check-quorum exists to close.
                    state.check_quorum_violation |= is_leader(&state.nodes[&id]);
                }
                state.leader_resigned |= state
                    .nodes
                    .values()
                    .any(|n| matches!(n.machine.role(), Role::Resigned));
            }
            ModelAction::ClientAppend(client, value) => {
                let leader = state
                    .nodes
                    .iter()
                    .find(|(id, n)| is_leader(n) && !state.crashed.contains(*id))
                    .map(|(&id, _)| id)?;
                let epoch = state.nodes[&leader].machine.quorum_state().leader_epoch;
                let offset = state.nodes[&leader].log.end_offset();
                // Record the invocation, append at the leader, track until committed.
                let _ = state
                    .linz
                    .on_invoke(client, LogOp::Append(value))
                    .expect("fresh client id has no in-flight op");
                state
                    .nodes
                    .get_mut(&leader)
                    .expect("leader exists")
                    .log
                    .append_in_epoch(epoch, 1);
                state
                    .pending
                    .insert(offset, (client, value, CommitPoint::KRaftHighWatermark));
                state.appends_issued += 1;
            }
            ModelAction::AppendVia(appender, client, value) => {
                let leader = live_authority(&state)?;
                let epoch = state.nodes[&leader].machine.quorum_state().leader_epoch;
                let offset = state.nodes[&leader].log.end_offset();
                let _ = state
                    .linz
                    .on_invoke(client, LogOp::Append(value))
                    .expect("fresh client id has no in-flight op");
                state
                    .nodes
                    .get_mut(&leader)
                    .expect("leader exists")
                    .log
                    .append_in_epoch(epoch, 1);
                state.appenders_seen.insert(appender);
                state
                    .pending
                    .insert(offset, (client, value, CommitPoint::WalQuorumDurable));
                state.appends_issued += 1;
            }
            ModelAction::WalFsync(node, end_offset) => {
                if state.crashed.contains(&node) {
                    return None;
                }
                state
                    .wal_frontiers
                    .entry(node)
                    .and_modify(|frontier| *frontier = (*frontier).max(end_offset));
            }
        }
        // After any transition, return the contiguous prefix that crossed its
        // configured durability boundary.
        settle_committed(&mut state);
        Some(state)
    }
}
