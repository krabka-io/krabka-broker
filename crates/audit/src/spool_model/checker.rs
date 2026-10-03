use super::*;

impl Model for SpoolModel {
    type State = SpoolState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![SpoolState {
            runtime: Runtime::Open,
            next_record: 0,
            durable_history: 0,
            volatile: 0,
            durable: 0,
            deliveries: [0; MAX_RECORDS as usize],
            cursor: 0,
            unsynced: 0,
            replay: ReplayPhase::Idle,
            crashes: 0,
            loss_events: 0,
            memory: Losses::default(),
            sidecar: Losses::default(),
            unpersisted: 0,
            forgotten_at_crash: 0,
            flow: MarkerFlow::Idle,
            markers: [None; MAX_RECORDS as usize],
            accounted: 0,
            witnesses: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.runtime == Runtime::Stopped {
            return;
        }
        if state.runtime == Runtime::Closed {
            actions.push(Action::Reopen);
            return;
        }
        // A loss from `AuditHandle::emit` and a crash can land anywhere.
        if state.loss_events < MAX_LOSSES {
            actions.push(Action::Lose);
        }
        if state.crashes < 2 {
            actions.push(Action::Crash);
        }
        // The writer is serial: while `persist_with` runs, its next step is
        // the only writer action.
        let next_step = match state.flow {
            MarkerFlow::Idle => None,
            MarkerFlow::Snapshotted { .. } => Some(Action::PersistSnapshot),
            MarkerFlow::Persisted { .. } => Some(Action::AppendLossMarker),
            MarkerFlow::Appended { .. } => Some(Action::SyncLossMarker),
            MarkerFlow::Synced { .. } => Some(Action::CommitLossMarker),
            MarkerFlow::Committed { .. } => Some(Action::PersistCommit),
        };
        if let Some(step) = next_step {
            actions.push(step);
            return;
        }
        if state.next_record < MAX_RECORDS {
            actions.push(Action::Append);
            actions.push(Action::TearAppend);
        }
        if state.volatile != 0 {
            actions.push(Action::Sync);
        }
        match state.replay {
            ReplayPhase::Idle if state.durable != 0 => actions.push(Action::BeginReplay),
            ReplayPhase::Poisoned { .. } => {
                actions.push(Action::Deliver);
                actions.push(Action::DefiniteFailure);
            }
            ReplayPhase::Delivered { .. } => actions.push(Action::CommitCursor),
            ReplayPhase::CursorCommitted { .. } => actions.push(Action::ClearPoison),
            ReplayPhase::Idle => {}
        }
        if state.unpersisted > 0 {
            actions.push(Action::PersistLosses);
        }
        if state.memory.count > 0 && state.next_record < MAX_RECORDS {
            actions.push(Action::SnapshotLosses);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Append => {
                spool_append(&mut state)?;
            }
            Action::Sync => sync(&mut state),
            Action::TearAppend => {
                state.next_record += 1;
                state.witnesses |= SAW_TORN_APPEND;
            }
            Action::BeginReplay => {
                let record = u8::try_from(state.durable.trailing_zeros()).unwrap_or(u8::MAX);
                state.replay = ReplayPhase::Poisoned {
                    record,
                    offset: state.cursor,
                };
            }
            Action::Deliver => {
                let ReplayPhase::Poisoned { record, offset } = state.replay else {
                    return None;
                };
                state.deliveries[usize::from(record)] += 1;
                state.replay = ReplayPhase::Delivered { record, offset };
            }
            Action::DefiniteFailure => {
                if !matches!(state.replay, ReplayPhase::Poisoned { .. }) {
                    return None;
                }
                state.replay = ReplayPhase::Idle;
                state.witnesses |= SAW_RETRY;
            }
            Action::CommitCursor => {
                let ReplayPhase::Delivered { record, offset } = state.replay else {
                    return None;
                };
                state.durable &= !bit(record);
                state.cursor += 1;
                state.replay = ReplayPhase::CursorCommitted { offset };
            }
            Action::ClearPoison => state.replay = ReplayPhase::Idle,
            Action::Crash => {
                state.volatile = 0;
                state.unsynced = 0;
                state.crashes += 1;
                state.runtime = Runtime::Closed;
                // Memory is gone: the flow with it, and a marker frame that
                // never reached disk.
                state.forgotten_at_crash += state.unpersisted;
                state.unpersisted = 0;
                state.memory = state.sidecar;
                state.flow = MarkerFlow::Idle;
                for (record, marker) in state.markers.iter_mut().enumerate() {
                    if state.durable_history & (1 << record) == 0 {
                        *marker = None;
                    }
                }
            }
            Action::Reopen => {
                state.runtime = Runtime::Open;
                return self.reopen(state);
            }
            Action::Lose => {
                (state.memory.generation, state.memory.count) =
                    add_loss_state(state.memory.generation, state.memory.count, 1);
                state.loss_events += 1;
                state.unpersisted += 1;
            }
            Action::PersistLosses => persist_sidecar(&mut state),
            Action::SnapshotLosses => {
                state.flow = MarkerFlow::Snapshotted {
                    batch: state.memory,
                };
            }
            Action::PersistSnapshot => {
                let MarkerFlow::Snapshotted { batch } = state.flow else {
                    return None;
                };
                persist_sidecar(&mut state);
                state.flow = MarkerFlow::Persisted { batch };
            }
            Action::AppendLossMarker => {
                let MarkerFlow::Persisted { batch } = state.flow else {
                    return None;
                };
                if let Some(record) = spool_append(&mut state) {
                    state.markers[usize::from(record)] = Some(batch);
                    state.flow = MarkerFlow::Appended { batch };
                } else {
                    // `append_loss_marker` fails with "spool is full"; the
                    // losses stay pending for the next attempt.
                    state.flow = MarkerFlow::Idle;
                    state.witnesses |= SAW_MARKER_REFUSED;
                }
            }
            Action::SyncLossMarker => {
                let MarkerFlow::Appended { batch } = state.flow else {
                    return None;
                };
                sync(&mut state);
                state.flow = MarkerFlow::Synced { batch };
            }
            Action::CommitLossMarker => {
                let MarkerFlow::Synced { batch } = state.flow else {
                    return None;
                };
                state.memory = (self.commit)(state.memory, batch);
                state.flow = MarkerFlow::Committed { batch };
            }
            Action::PersistCommit => {
                let MarkerFlow::Committed { batch } = state.flow else {
                    return None;
                };
                persist_sidecar(&mut state);
                state.accounted = state.accounted.saturating_add(batch.count);
                if state.sidecar.count > 0 {
                    state.witnesses |= SAW_REMAINDER_CARRIED;
                }
                state.flow = MarkerFlow::Idle;
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("delivered_or_durably_pending", |_, state: &SpoolState| {
                let delivered = state
                    .deliveries
                    .iter()
                    .enumerate()
                    .fold(0_u8, |mask, (record, count)| {
                        mask | u8::from(*count > 0) << record
                    });
                state.durable_history & !(delivered | state.durable) == 0
            }),
            Property::always(
                "automatic_delivery_at_most_once",
                |_, state: &SpoolState| state.deliveries.iter().all(|count| *count <= 1),
            ),
            // No marker in the spool names a generation the sidecar has not
            // reached, and no two name the same one: reconciliation finds a
            // generation's marker by its generation alone.
            Property::always("marker_generations_unique", |_, state: &SpoolState| {
                let generations: Vec<u64> = state.durable_markers().map(|m| m.generation).collect();
                generations.iter().all(|g| *g <= state.sidecar.generation)
                    && generations
                        .iter()
                        .enumerate()
                        .all(|(i, g)| !generations[..i].contains(g))
            }),
            // Every loss is settled against a durable marker, still pending in
            // the sidecar, added since the last sidecar write, or was held
            // only in memory at a crash, which is where the queued events a
            // crash drops are too. Exactly one of these.
            Property::always("losses_accounted", |_, state: &SpoolState| {
                u64::from(state.loss_events)
                    == state.accounted
                        + state.sidecar.count
                        + u64::from(state.unpersisted)
                        + u64::from(state.forgotten_at_crash)
            }),
            // A settlement is credited only against losses a durable marker
            // reports, and, once the writer is idle on an open spool, every
            // durable marker's losses are settled: none is reported twice or
            // left pending beside its marker.
            Property::always("markers_settled_once", |_, state: &SpoolState| {
                state.accounted <= state.reported()
                    && (state.runtime != Runtime::Open
                        || state.flow != MarkerFlow::Idle
                        || state.accounted == state.reported())
            }),
            Property::sometimes("torn_append_recovered", |_, state: &SpoolState| {
                state.witnesses & SAW_TORN_APPEND != 0 && state.crashes > 0
            }),
            Property::sometimes("definite_failure_can_retry", |_, state: &SpoolState| {
                state.witnesses & SAW_RETRY != 0
            }),
            Property::sometimes("uncertain_delivery_stops", |_, state: &SpoolState| {
                state.witnesses & SAW_UNCERTAIN_POISON != 0 && state.runtime == Runtime::Stopped
            }),
            Property::sometimes("committed_poison_clears", |_, state: &SpoolState| {
                state.witnesses & SAW_COMMITTED_POISON != 0
                    && matches!(state.replay, ReplayPhase::Idle)
            }),
            Property::sometimes("full_spool_refuses_marker", |_, state: &SpoolState| {
                state.witnesses & SAW_MARKER_REFUSED != 0
            }),
            Property::sometimes("durable_loss_marker_reconciles", |_, state: &SpoolState| {
                state.witnesses & SAW_LOSS_RECONCILE != 0
            }),
            Property::sometimes(
                "concurrent_loss_carried_forward",
                |_, state: &SpoolState| state.witnesses & SAW_REMAINDER_CARRIED != 0,
            ),
        ]
    }
}
