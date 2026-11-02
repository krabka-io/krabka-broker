//! Bounded scheduling model driving the production delayed-upload predicate.
//!
//! Enumerates effective limits -1/0/2, time 0..=4 around a timestamp of 2,
//! and newer active/sealed byte counts 0..=3/0..=2. Configuration changes,
//! clock progress and byte growth interleave. This is the eligibility kernel,
//! not a remote-write, metadata-commit, leadership or restart model.

use {
    super::CopyDelay,
    crate::model_check::check_model,
    stateright::{Model, Property},
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct State {
    ms: i64,
    bytes: i64,
    now: i64,
    active: u64,
    sealed: u64,
}

impl State {
    fn eligible(&self) -> bool {
        CopyDelay {
            lag_ms: self.ms,
            lag_bytes: self.bytes,
            now_ms: self.now,
            active_bytes: self.active,
        }
        .allows(2, self.sealed)
    }

    fn age_trigger(&self) -> bool { self.ms > 0 && (self.now < 2 || self.now - 2 >= self.ms) }

    fn byte_trigger(&self) -> bool {
        u64::try_from(self.bytes).is_ok_and(|limit| limit > 0 && self.active + self.sealed >= limit)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    Clock,
    ActiveBytes,
    SealedBytes,
    TimeLimit(i64),
    ByteLimit(i64),
}

struct DelayModel;

impl Model for DelayModel {
    type Action = Action;
    type State = State;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            ms: -1,
            bytes: -1,
            now: 0,
            active: 0,
            sealed: 0,
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.now < 4 {
            actions.push(Action::Clock);
        }
        if state.active < 3 {
            actions.push(Action::ActiveBytes);
        }
        if state.sealed < 2 {
            actions.push(Action::SealedBytes);
        }
        for limit in [-1, 0, 2] {
            if state.ms != limit {
                actions.push(Action::TimeLimit(limit));
            }
            if state.bytes != limit {
                actions.push(Action::ByteLimit(limit));
            }
        }
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Clock => next.now += 1,
            Action::ActiveBytes => next.active += 1,
            Action::SealedBytes => next.sealed += 1,
            Action::TimeLimit(limit) => next.ms = limit,
            Action::ByteLimit(limit) => next.bytes = limit,
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_early_upload", |_, s: &State| {
                !s.eligible() || s.ms == 0 || s.bytes == 0 || s.age_trigger() || s.byte_trigger()
            }),
            Property::always("eligible_when_triggered", |_, s: &State| {
                !(s.ms == 0 || s.bytes == 0 || s.age_trigger() || s.byte_trigger()) || s.eligible()
            }),
            Property::sometimes("delayed_despite_positive_limits", |_, s: &State| {
                s.ms == 2
                    && s.bytes == 2
                    && s.now == 2
                    && s.active == 0
                    && s.sealed == 0
                    && !s.eligible()
            }),
            Property::sometimes("age_threshold_is_inclusive", |_, s: &State| {
                s.ms == 2 && s.bytes == -1 && s.now == 4 && s.active == 0 && s.eligible()
            }),
            Property::sometimes("future_timestamp_bypasses_delay", |_, s: &State| {
                s.ms == 2 && s.bytes == -1 && s.now < 2 && s.eligible()
            }),
            Property::sometimes("bytes_release_before_age", |_, s: &State| {
                s.ms == 2
                    && s.bytes == 2
                    && s.now == 2
                    && s.active == 1
                    && s.sealed == 1
                    && s.eligible()
            }),
            Property::sometimes("unlimited_both_remains_delayed", |_, s: &State| {
                s.ms == -1 && s.bytes == -1 && s.now == 3 && s.active == 3 && !s.eligible()
            }),
        ]
    }
}

#[test]
fn delayed_copy_interleaves_limits_clock_and_byte_growth() {
    // 3 time limits * 3 byte limits * 5 clock values * 4 active * 3 sealed.
    check_model(DelayModel, "copy_delay", (16, 20_000, 1_000), 540);
}
