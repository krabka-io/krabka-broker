use super::*;

impl Model for WalModel {
    type Action = Action;
    type State = WalState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![WalState {
            steps: 0,
            logs: std::array::from_fn(|_| Vec::new()),
            leader: 0,
            leader_epoch: 0,
            live: 0b111,
            hwm: 0,
            committed: Vec::new(),
            last_ack_failed: false,
            recovered: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.steps == MAX_STEPS {
            return;
        }
        if is_live(state, state.leader) {
            if state.logs[state.leader].len() < MAX_RECORDS {
                actions.push(Action::Append);
            }
            if state.logs[state.leader].len() > state.hwm {
                actions.push(Action::Acknowledge);
            }
        }
        for voter in 0..VOTERS {
            if is_live(state, voter) {
                actions.push(Action::Fail(voter));
                if !is_live(state, state.leader)
                    && voter != state.leader
                    && state.leader_epoch < MAX_EPOCH
                    && has_committed_prefix(state, voter)
                {
                    actions.push(Action::Elect(voter));
                }
            } else {
                actions.push(Action::Revive(voter));
            }
        }
        if state.live == 0b111 && state.logs.iter().any(|log| log != &state.logs[0]) {
            actions.push(Action::CrashRecover);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        state.steps += 1;
        match action {
            Action::Append => {
                state.logs = drive_append(&state);
                state.last_ack_failed = false;
            }
            Action::Acknowledge => {
                let expected_success = ack_has_a_majority(&state);
                let (logs, result) = drive_ack(&state);
                assert2::assert!(
                    result.is_ok() == expected_success,
                    "an acknowledgement succeeds exactly when a majority can hold the leader's log"
                );
                state.logs = logs;
                match result {
                    Ok(hwm) => {
                        assert2::assert!(hwm >= state.committed.len());
                        assert2::assert!(
                            state.logs[state.leader][..state.committed.len()]
                                == state.committed[..]
                        );
                        state.hwm = hwm;
                        state.committed = state.logs[state.leader][..hwm].to_vec();
                        state.last_ack_failed = false;
                    }
                    Err(()) => {
                        state.last_ack_failed = true;
                    }
                }
            }
            Action::Fail(voter) => {
                state.live &= !(1 << voter);
            }
            Action::Revive(voter) => {
                state.live |= 1 << voter;
            }
            Action::Elect(voter) => {
                state.leader = voter;
                state.leader_epoch += 1;
                state.last_ack_failed = false;
            }
            Action::CrashRecover => {
                let (logs, hwm) = drive_recovery(&state);
                state.logs = logs;
                assert2::assert!(hwm >= state.committed.len());
                assert2::assert!(
                    state.logs[state.leader][..state.committed.len()] == state.committed[..]
                );
                state.hwm = hwm;
                state.committed = state.logs[state.leader][..hwm].to_vec();
                state.last_ack_failed = false;
                state.recovered = true;
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("hwm_is_inside_leader_log", |_, state: &WalState| {
                state.hwm <= state.logs[state.leader].len()
            }),
            Property::always("committed_frontier_matches_hwm", |_, state: &WalState| {
                state.committed.len() == state.hwm
            }),
            Property::always("committed_prefix_has_a_quorum", |_, state: &WalState| {
                state
                    .logs
                    .iter()
                    .filter(|log| log.len() >= state.hwm && log[..state.hwm] == state.committed[..])
                    .count()
                    >= 2
            }),
            Property::sometimes("quorum_acknowledges", |_, state: &WalState| state.hwm > 0),
            Property::sometimes("minority_ack_fails", |_, state: &WalState| {
                state.last_ack_failed && state.live.count_ones() < 2
            }),
            Property::sometimes(
                "divergent_ack_fails_with_a_live_majority",
                |_, state: &WalState| state.last_ack_failed && state.live.count_ones() >= 2,
            ),
            Property::sometimes("recovery_runs", |_, state: &WalState| state.recovered),
            Property::sometimes("leader_changes", |_, state: &WalState| state.leader != 0),
            Property::sometimes("replicas_diverge", |_, state: &WalState| {
                state.logs.iter().any(|log| log != &state.logs[0])
            }),
        ]
    }
}
