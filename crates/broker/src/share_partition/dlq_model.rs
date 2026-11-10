//! Bounded DLQ orchestration over the real acquisition machine.
//!
//! One rejected offset, two produce attempts and one crash/leader reload. Persistence and
//! transport scheduling are modeled; acquisition, rejection, persistence
//! encoding/reload and final archival execute production code. This does not
//! model a multi-node persister, network protocol, metrics or byte encoding.
//! Terminal authorization refusal is an abstract transport outcome, not an ACL
//! evaluator. Phase-2 success/failure prunes only after the persistence attempt,
//! matching the production manager; failed persistence may replay a DLQ write.
use {
    super::{
        AckType, AcquisitionState, RecordState,
        test_support::{DeadLetterQueue, acquire_window},
    },
    crate::model_check::{
        KnownFailure, assert_pinned_count, check_model_with_known_failures, run_bfs,
    },
    assert2::assert,
    krabka_log::Offset,
    stateright::{Checker as _, Expectation, Model, Property},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    live: AcquisitionState,
    durable: AcquisitionState,
    persisted: bool,
    in_flight: bool,
    settled: bool,
    attempts: u8,
    appended: u8,
    crashes: u8,
    denied: bool,
    persistence_failed: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Persist,
    Send,
    Retry,
    Success,
    Oversized,
    AuthorizationDenied,
    PersistFailure,
    Finish,
    Crash,
    Confirm,
}
struct DlqModel {
    lose_recovery: bool,
}
fn has_archiving(sm: &AcquisitionState) -> bool {
    sm.record_states()
        .iter()
        .any(|(_, state)| *state == RecordState::Archiving)
}
fn restored(sm: &AcquisitionState) -> AcquisitionState {
    let (start, _, batches) = sm.to_persist_batches();
    let mut fresh = AcquisitionState::new(start);
    fresh.load_from(start, sm.state_epoch, sm.leader_epoch, &batches);
    fresh.set_dlq_enabled(true);
    fresh
}
impl Model for DlqModel {
    type Action = Action;
    type State = State;

    fn init_states(&self) -> Vec<State> {
        let mut live = AcquisitionState::new(Offset(0));
        acquire_window()
            .state(&mut live)
            .end(1)
            .dead_letter_queue(DeadLetterQueue::Enabled)
            .call();
        live.acknowledge("m1", Offset(0), Offset(0), AckType::Reject, 5)
            .unwrap();
        vec![State {
            durable: restored(&live),
            live,
            persisted: false,
            in_flight: false,
            settled: false,
            attempts: 0,
            appended: 0,
            crashes: 0,
            denied: false,
            persistence_failed: false,
        }]
    }

    fn actions(&self, _: &State, actions: &mut Vec<Action>) {
        actions.extend([
            Action::Persist,
            Action::Send,
            Action::Retry,
            Action::Success,
            Action::Oversized,
            Action::AuthorizationDenied,
            Action::PersistFailure,
            Action::Finish,
            Action::Crash,
            Action::Confirm,
        ]);
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut s = last.clone();
        match action {
            Action::Persist if !s.persisted => {
                s.durable = restored(&s.live);
                s.persisted = true;
            }
            Action::Send if s.persisted && !s.in_flight && !s.settled && s.attempts < 2 => {
                if s.live.take_pending_dlq().is_empty() {
                    return None;
                }
                s.in_flight = true;
                s.attempts += 1;
            }
            // A leader error retries the same in-flight operation; the second
            // attempt must settle, bounding schedules without assuming fairness.
            Action::Retry if s.in_flight && s.attempts < 2 => {
                s.attempts += 1;
            }
            Action::Success if s.in_flight => {
                s.appended += 1;
                s.in_flight = false;
                s.settled = true;
            }
            Action::Oversized if s.in_flight => {
                s.in_flight = false;
                s.settled = true;
            }
            Action::AuthorizationDenied if s.in_flight => {
                s.in_flight = false;
                s.settled = true;
                s.denied = true;
            }
            Action::Finish if s.settled && s.live.start_offset == Offset(0) => {
                s.live.finish_archiving(Offset(0), Offset(0));
            }
            // Production prunes after either persistence outcome. A failed
            // phase-2 write leaves durable ARCHIVING: a crash can enqueue again.
            Action::PersistFailure
                if s.settled && !has_archiving(&s.live) && s.live.start_offset == Offset(0) =>
            {
                s.live.prune_terminal_prefix();
                s.persistence_failed = true;
            }
            Action::Confirm
                if s.settled && !has_archiving(&s.live) && s.durable.start_offset == Offset(0) =>
            {
                s.durable = restored(&s.live);
                s.live.prune_terminal_prefix();
            }
            Action::Crash if s.persisted && s.crashes < 1 => {
                s.live = restored(&s.durable);
                s.in_flight = false;
                s.settled = false;
                s.attempts = 0;
                s.crashes += 1;
                if self.lose_recovery && s.live.start_offset == Offset(0) {
                    let _ = s.live.take_pending_dlq();
                }
            }
            _ => return None,
        }
        (s != *last).then_some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("no_stranded_archiving", |m: &Self, s: &State| {
                s.live.start_offset == Offset(1)
                    || [
                        Action::Persist,
                        Action::Send,
                        Action::Retry,
                        Action::Success,
                        Action::Oversized,
                        Action::AuthorizationDenied,
                        Action::PersistFailure,
                        Action::Finish,
                        Action::Confirm,
                    ]
                    .into_iter()
                    .any(|a| m.next_state(s, a).is_some())
            }),
            Property::always(
                "no_write_before_archiving_persisted",
                |_: &Self, s: &State| s.attempts == 0 || s.persisted,
            ),
            Property::always("archiving_blocks_source_progress", |_: &Self, s: &State| {
                !s.live
                    .record_states()
                    .iter()
                    .any(|(_, r)| *r == RecordState::Archiving)
                    || s.live.start_offset == Offset(0)
            }),
            Property::always("recovered_archiving_has_work", |_: &Self, s: &State| {
                s.live.start_offset == Offset(1)
                    || s.in_flight
                    || s.settled
                    || !s.live.pending_dlq.is_empty()
            }),
            Property::always(
                "prune_only_after_archived_persistence_attempt",
                |_: &Self, s: &State| {
                    s.live.start_offset == Offset(0)
                        || s.durable.start_offset == Offset(1)
                        || s.persistence_failed
                },
            ),
            Property::always("terminal_failure_can_finish", |_: &Self, s: &State| {
                if s.settled {
                    let mut sm = s.live.clone();
                    sm.finish_archiving(Offset(0), Offset(0));
                    sm.prune_terminal_prefix();
                    sm.start_offset == Offset(1)
                } else {
                    true
                }
            }),
            Property::sometimes(
                "authorization_after_retry_archives_without_append",
                |_: &Self, s: &State| {
                    s.denied
                        && s.attempts == 2
                        && s.live.start_offset == Offset(1)
                        && s.appended == 0
                },
            ),
            Property::sometimes(
                "failed_archived_persist_reloads_work",
                |_: &Self, s: &State| {
                    s.persistence_failed
                        && s.crashes == 1
                        && has_archiving(&s.live)
                        && s.appended > 0
                },
            ),
            Property::sometimes(
                "failed_archived_persist_can_duplicate",
                |_: &Self, s: &State| {
                    s.persistence_failed
                        && s.crashes == 1
                        && s.appended == 2
                        && s.live.start_offset == Offset(1)
                },
            ),
            Property::sometimes("oversize_archives_without_append", |_: &Self, s: &State| {
                !s.denied && s.live.start_offset == Offset(1) && s.appended == 0
            }),
            Property::sometimes(
                "append_before_confirmation_can_duplicate",
                |_: &Self, s: &State| s.appended == 2,
            ),
            Property::sometimes(
                "archived_confirmation_survives_reload",
                |_: &Self, s: &State| {
                    s.crashes == 1
                        && s.durable.start_offset == Offset(1)
                        && s.live.start_offset == Offset(1)
                },
            ),
        ];
        if self.lose_recovery {
            properties.retain(|p| p.expectation != Expectation::Sometimes);
        }
        properties
    }
}
#[test]
fn dlq_archiving_fault_schedules() {
    let checker = run_bfs(
        DlqModel {
            lose_recovery: false,
        },
        "dlq_archiving",
        32,
        10_000,
    );
    assert_pinned_count(checker.unique_state_count(), 128, "dlq_archiving");
    checker.assert_properties();
}
#[test]
fn dlq_model_detects_lost_recovery_mutant() {
    check_model_with_known_failures(
        DlqModel {
            lose_recovery: true,
        },
        "dlq_archiving_mutant",
        (32, 10_000, 1000),
        46,
        &[
            KnownFailure {
                property: "recovered_archiving_has_work",
                depth: 2,
            },
            KnownFailure {
                property: "no_stranded_archiving",
                depth: 2,
            },
        ],
    );
}

/// Pin the phase-2 persistence cut separately from the exhaustive graph: local
/// completion is not a durable acknowledgement, including after terminal ACL
/// denial. A restart must retry the old ARCHIVING row in either case.
#[test]
fn failed_phase_two_persist_requeues_after_success_or_denial() {
    let model = DlqModel {
        lose_recovery: false,
    };
    for outcome in [Action::Success, Action::AuthorizationDenied] {
        let mut state = model.init_states().remove(0);
        for action in [
            Action::Persist,
            Action::Send,
            Action::Retry,
            outcome,
            Action::Finish,
        ] {
            state = model.next_state(&state, action).expect("enabled schedule");
        }
        let (start, completed, _) = state.live.to_persist_batches();
        assert!((start, completed, has_archiving(&state.durable)) == (Offset(0), 1, true));
        state = model.next_state(&state, Action::PersistFailure).unwrap();
        assert!(state.live.start_offset == Offset(1));
        state = model.next_state(&state, Action::Crash).unwrap();
        assert!(state.live.start_offset == Offset(0));
        assert!(state.live.take_pending_dlq().len() == 1);
    }
}

/// A confirmed ARCHIVED write, unlike a live-cache-only completion, prevents
/// recovery from enqueueing another write. Exercise both destination outcomes.
#[test]
fn confirmed_phase_two_persist_does_not_requeue() {
    let model = DlqModel {
        lose_recovery: false,
    };
    for outcome in [Action::Success, Action::AuthorizationDenied] {
        let mut state = model.init_states().remove(0);
        let schedule = [
            Action::Persist,
            Action::Send,
            outcome,
            Action::Finish,
            Action::Confirm,
            Action::Crash,
        ];
        state = schedule.into_iter().fold(state, |current, action| {
            model
                .next_state(&current, action)
                .expect("confirmed schedule")
        });
        assert!(
            (state.live.start_offset, state.live.take_pending_dlq()) == (Offset(1), Vec::new())
        );
    }
}
