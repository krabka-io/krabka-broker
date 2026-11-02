//! Bounded DLQ orchestration over the real acquisition machine. One rejected
//! offset, two produce attempts and one crash/leader reload. Persistence and
//! transport scheduling are modeled; acquisition, rejection, persistence
//! encoding/reload and final archival execute production code. This does not
//! model a multi-node persister, network protocol, metrics or byte encoding.
use {
    super::{
        AckType, AcquisitionState, RecordState,
        test_support::{DeadLetterQueue, acquire_window},
    },
    crate::model_check::run_bfs,
    assert2::assert,
    krabka_log::Offset,
    stateright::{Checker as _, Model, Property},
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
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Persist,
    Send,
    Retry,
    Success,
    Oversized,
    Finish,
    Crash,
    Confirm,
}
struct DlqModel {
    lose_recovery: bool,
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
        }]
    }

    fn actions(&self, _: &State, actions: &mut Vec<Action>) {
        actions.extend([
            Action::Persist,
            Action::Send,
            Action::Retry,
            Action::Success,
            Action::Oversized,
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
            Action::Finish if s.settled && s.live.start_offset == Offset(0) => {
                s.live.finish_archiving(Offset(0), Offset(0));
            }
            Action::Confirm
                if s.live.start_offset == Offset(1) && s.durable.start_offset == Offset(0) =>
            {
                s.durable = restored(&s.live);
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
        vec![
            Property::always("no_stranded_archiving", |m: &Self, s: &State| {
                s.live.start_offset == Offset(1)
                    || [
                        Action::Persist,
                        Action::Send,
                        Action::Retry,
                        Action::Success,
                        Action::Oversized,
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
            Property::always("terminal_failure_can_finish", |_: &Self, s: &State| {
                if s.settled {
                    let mut sm = s.live.clone();
                    sm.finish_archiving(Offset(0), Offset(0));
                    sm.start_offset == Offset(1)
                } else {
                    true
                }
            }),
            Property::sometimes("oversize_archives_without_append", |_: &Self, s: &State| {
                s.live.start_offset == Offset(1) && s.appended == 0
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
        ]
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
    eprintln!("DLQ unique states: {}", checker.unique_state_count());
    checker.assert_properties();
}
#[test]
fn dlq_model_detects_lost_recovery_mutant() {
    let checker = run_bfs(
        DlqModel {
            lose_recovery: true,
        },
        "dlq_archiving_mutant",
        32,
        10_000,
    );
    assert!(checker.discovery("recovered_archiving_has_work").is_some());
}
