use super::*;

impl ConsensusModel {
    pub(super) fn model_init_states(&self) -> Vec<<Self as Model>::State> {
        let voters = self.voter_set();
        let mut nodes = BTreeMap::new();
        for &id in &self.voter_ids {
            let machine = QuorumStateMachine::new(
                id,
                QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone()),
                Self::election_timeout_of(id),
            );
            nodes.insert(
                id,
                NodeModel {
                    machine,
                    log: ModelLog::default(),
                    high_watermark: 0,
                },
            );
        }
        vec![ModelState {
            nodes,
            network: BTreeSet::new(),
            linz: LinearizabilityTester::new(KraftLogSpec::default()),
            pending: BTreeMap::new(),
            wal_frontiers: self.voter_ids.iter().map(|&id| (id, 0)).collect(),
            appenders_seen: BTreeSet::new(),
            committed: Vec::new(),
            committed_epochs: Vec::new(),
            appends_issued: 0,
            crashed: BTreeSet::new(),
            check_quorum_violation: false,
            leader_resigned: false,
            step_witness: None,
        }]
    }
}

impl ConsensusModel {
    pub(super) fn model_actions(
        &self,
        state: &<Self as Model>::State,
        actions: &mut Vec<<Self as Model>::Action>,
    ) {
        // Every in-flight message is independently deliverable (unordered net).
        // Loss/duplication, when enabled, offer a drop and a duplicate-deliver
        // for each in-flight message.
        for env in &state.network {
            actions.push(ModelAction::Deliver(env.clone()));
            if self.enable_loss_dup {
                actions.push(ModelAction::DropMsg(env.clone()));
                actions.push(ModelAction::DuplicateDeliver(env.clone()));
            }
        }
        // Any voter that is not currently leader may suffer an election timeout;
        // any follower/observer may suffer a fetch timeout. The core ignores
        // inapplicable ones, so over-offering is sound (it only adds interleavings).
        // Crashed nodes are unreachable: offered no timeouts.
        for (&id, node) in &state.nodes {
            if state.crashed.contains(&id) {
                continue;
            }
            match node.machine.role() {
                // A leader runs neither watchdog; its one timer is the
                // check-quorum window that ends the epoch it has lost.
                Role::Leader { .. } => {
                    if self.enable_check_quorum {
                        actions.push(ModelAction::Timeout(id, TimerKind::CheckQuorum));
                    }
                }
                Role::Follower { .. } | Role::Observer { .. } => {
                    actions.push(ModelAction::Timeout(id, TimerKind::Fetch));
                    actions.push(ModelAction::Timeout(id, TimerKind::Election));
                }
                _ => actions.push(ModelAction::Timeout(id, TimerKind::Election)),
            }
        }
        // Crash/recover, capped at `max_crashes` concurrently crashed.
        if state.crashed.len() < self.max_crashes {
            for &id in &self.voter_ids {
                if !state.crashed.contains(&id) {
                    actions.push(ModelAction::Crash(id));
                }
            }
        }
        for &id in &state.crashed {
            actions.push(ModelAction::Recover(id));
        }
        if self.enable_append_via
            && let Some((&offset, _)) = state
                .pending
                .iter()
                .find(|(_, (_, _, point))| *point == CommitPoint::WalQuorumDurable)
        {
            let end_offset = offset + 1;
            // WAL members are exchangeable here: this focused config has no
            // WAL-node crash action, and only the majority-th frontier is
            // observed. Advance the first eligible label as a symmetry normal
            // form instead of exploring every permutation of identical fsyncs.
            if let Some(&id) = self.voter_ids.iter().find(|id| {
                !state.crashed.contains(id)
                    && state.wal_frontiers.get(id).copied().unwrap_or(0) < end_offset
            }) {
                actions.push(ModelAction::WalFsync(id, end_offset));
            }
        }
        // A client appends to the single current (live) leader (only when the
        // target is unambiguous and the append budget remains). A fresh client id
        // per append keeps every linearizability "thread" single-op.
        let leaders: Vec<NodeId> = state
            .nodes
            .iter()
            .filter(|(id, n)| is_leader(n) && !state.crashed.contains(*id))
            .map(|(&id, _)| id)
            .collect();
        if state.appends_issued < self.max_appends {
            let client = ClientId::from(state.appends_issued) + 1;
            let value = u64::from(state.appends_issued) + 1;
            if self.enable_append_via {
                // Stateless appenders can enter through any live broker. Do
                // not reintroduce the old `leaders.len() == 1` emission gate:
                // routing resolves the highest-epoch live authority when the
                // action executes, including during authority handoff.
                if live_authority(state).is_some() {
                    // Each append uses the next canonical appender label. This
                    // represents every permutation of two exchangeable labels
                    // while retaining the target concurrent-appender path.
                    let appender = AppenderId::try_from(state.appends_issued)
                        .expect("append bound fits the appender domain");
                    assert2::assert!(appender < APPENDER_COUNT);
                    actions.push(ModelAction::AppendVia(appender, client, value));
                }
            } else if leaders.len() == 1 {
                actions.push(ModelAction::ClientAppend(client, value));
            }
        }
    }
}
