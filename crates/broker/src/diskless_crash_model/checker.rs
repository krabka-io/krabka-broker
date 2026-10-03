use super::*;

impl Model for CrashModel {
    type Action = Act;
    type State = CrashState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CrashState {
            kraft_next: 0,
            log_end: 0,
            wal_nodes: [0; WAL_NODES],
            wal_lost: [false; WAL_NODES],
            wal_acked: 0,
            handoff_wal_acked: 0,
            advertised_hwm: 0,
            sequencer_epoch: 0,
            object_frontier: 0,
            index_frontier: 0,
            trimmed: 0,
            producer_committed: 0,
            reservations: Vec::new(),
            appenders_seen: 0,
            witnesses: 0,
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        if s.kraft_next < MAX_OFFSET {
            for node in 0..APPENDERS {
                if !s.wal_lost[node] {
                    acts.push(Act::ReserveVia(node));
                }
            }
        }
        if s.log_end < s.kraft_next {
            acts.push(Act::FsyncAppend);
            acts.push(Act::CrashBeforeFsync);
            acts.push(Act::CrashMidFsync);
        }
        if s.object_frontier < s.wal_acked {
            acts.push(Act::PutObject);
        }
        if s.index_frontier < s.object_frontier {
            acts.push(Act::CommitIndex);
        }
        if s.trimmed < s.index_frontier {
            acts.push(Act::Trim);
        }
        if s.wal_lost.iter().filter(|lost| **lost).count() == 0 && s.wal_acked > 0 {
            for node in 0..WAL_NODES {
                acts.push(Act::LoseWalNode(node));
            }
        }
        if s.wal_acked > 0 && s.sequencer_epoch < 2 {
            acts.push(Act::SequencerHandoff);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Act::ReserveVia(node) => {
                let (base, next) = reserve_via_controller(&s);
                s.kraft_next = next;
                s.reservations.push((base, next));
                s.appenders_seen |= 1 << node;
                if s.appenders_seen.count_ones() >= 2 {
                    s.witnesses |= WITNESS_STATELESS_APPEND;
                }
            }
            Act::FsyncAppend => {
                s.log_end += 1;
                fsync_quorum(&mut s);
                s.producer_committed = s.log_end;
            }
            Act::CrashBeforeFsync => {
                if s.kraft_next > s.log_end {
                    s.witnesses |= WITNESS_KRAFT_FSYNC_GAP;
                }
            }
            Act::CrashMidFsync => {
                s.witnesses |= WITNESS_MID_FSYNC;
                s.kraft_next = s.kraft_next.max(s.log_end);
            }
            Act::PutObject => {
                s.object_frontier = s.wal_acked;
                if s.object_frontier > s.index_frontier {
                    s.witnesses |= WITNESS_PUT_BEFORE_INDEX;
                }
            }
            Act::CommitIndex => {
                s.index_frontier = s.object_frontier;
            }
            Act::Trim => {
                let decision = krabka_verified::diskless::diskless_trim_decision(
                    s.index_frontier,
                    s.wal_acked,
                    0,
                    s.trimmed,
                );
                if decision.should_trim {
                    s.trimmed = decision.target;
                }
                if s.trimmed > 0 && s.trimmed == s.index_frontier {
                    s.witnesses |= WITNESS_TRIM_AT_INDEX;
                }
            }
            Act::LoseWalNode(node) => {
                s.wal_lost[node] = true;
                s.witnesses |= WITNESS_MINORITY_WAL_LOSS;
            }
            Act::SequencerHandoff => {
                s.sequencer_epoch += 1;
                s.handoff_wal_acked = s.handoff_wal_acked.max(s.wal_acked);
                // KIP-207 permits the newly advertised frontier to move back
                // while authority changes. Durability does not move back: the
                // new sequencer can re-derive `wal_acked` from the surviving
                // quorum, independently of this conservative visible value.
                s.advertised_hwm = s.index_frontier.min(s.wal_acked);
                s.witnesses |= WITNESS_SEQUENCER_HANDOFF;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("wal_acked_durable", |_, s: &CrashState| {
                s.wal_acked <= surviving_wal_frontier(s)
            }),
            Property::always("committed_durable", |_, s: &CrashState| {
                s.producer_committed <= surviving_wal_frontier(s)
            }),
            Property::always(
                "sequencer_handoff_never_regresses_wal_acked",
                |_, s: &CrashState| s.wal_acked >= s.handoff_wal_acked,
            ),
            Property::always("trim_at_committed_index_frontier", |_, s: &CrashState| {
                s.trimmed <= s.index_frontier && s.trimmed <= s.wal_acked
            }),
            Property::always("reservations_gap_free_and_unique", |_, s: &CrashState| {
                let mut next = 0;
                for &(base, end) in &s.reservations {
                    if base != next || end <= base {
                        return false;
                    }
                    next = end;
                }
                next == s.kraft_next
            }),
            Property::sometimes("crash_in_kraft_fsync_gap", |_, s: &CrashState| {
                s.witnesses & WITNESS_KRAFT_FSYNC_GAP != 0
            }),
            Property::sometimes("crash_between_put_and_index", |_, s: &CrashState| {
                s.witnesses & WITNESS_PUT_BEFORE_INDEX != 0
            }),
            Property::sometimes("crash_mid_fsync", |_, s: &CrashState| {
                s.witnesses & WITNESS_MID_FSYNC != 0
            }),
            Property::sometimes("trim_at_index_frontier", |_, s: &CrashState| {
                s.witnesses & WITNESS_TRIM_AT_INDEX != 0
            }),
            Property::sometimes(
                "acked_unflushed_survives_minority_wal_loss",
                |_, s: &CrashState| {
                    s.witnesses & WITNESS_MINORITY_WAL_LOSS != 0 && s.wal_acked > s.trimmed
                },
            ),
            Property::sometimes("two_appenders_race_gap_free", |_, s: &CrashState| {
                s.witnesses & WITNESS_STATELESS_APPEND != 0
            }),
            Property::sometimes(
                "sequencer_handoff_regresses_only_advertised_hwm",
                |_, s: &CrashState| {
                    s.witnesses & WITNESS_SEQUENCER_HANDOFF != 0 && s.advertised_hwm < s.wal_acked
                },
            ),
        ]
    }
}
