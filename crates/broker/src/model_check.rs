//! Shared runner for the crate's exhaustive Stateright models.
//!
//! Every model test checks the same way: a breadth-first search under a depth
//! cap and a state-count target, then a check that neither cap truncated it.
//! [`run_bfs`] holds that sequence. Each caller supplies its own unique-state
//! pin to [`assert_pinned_count`] and calls `assert_properties`.
//!
//! Two further pieces serve models of asynchronous orchestration:
//!
//! - [`check_model_with_known_failures`] runs a model whose property reaches a
//!   counterexample on the current code, because the bug it reproduces is not
//!   fixed yet. The test pins that counterexample's depth, so it fails when the
//!   model stops reaching the bug as well as when the fix lands.
//! - [`Quiescence`] states liveness as a safety property. A model marks its
//!   fault actions, and a property asks that the system has converged in every
//!   state where no other action is enabled. `docs/verification.md` describes
//!   the pattern.

use {
    stateright::{Checker as _, Expectation, HasDiscoveries, Model},
    std::{collections::BTreeSet, fmt::Debug, hash::Hash},
};

krabka_macros::bounded_bfs!(run_bfs);

/// A property name that no model declares. A search told to finish once this
/// name has a discovery never finishes early, so it explores every state.
const EXHAUSTIVE: &str = "<exhaustive>";

/// Reject changes to the reachable state set using each model's independent pin.
pub fn assert_pinned_count(actual: usize, expected: usize, label: &str) {
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        actual == expected,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
}

/// Run an exhaustive model with its independent unique-state bound and pin.
pub fn check_model<M>(model: M, label: &str, limits: (usize, usize, usize), pinned: usize)
where
    M: Model + Send + Sync + 'static,
    M::State: Debug + Hash + Send + Sync + Clone + PartialEq + 'static,
    M::Action: Debug + Clone + PartialEq,
{
    let (depth, states, unique) = limits;
    let checker = run_bfs(model, label, depth, states);
    assert2::assert!(
        checker.unique_state_count() < unique,
        "[{label}] unique-state bound exceeded"
    );
    assert_pinned_count(checker.unique_state_count(), pinned, label);
    checker.assert_properties();
}

/// A property that a model reaches a counterexample for on the current code.
///
/// The bug that the property names is open. The counterexample is the evidence
/// that the model reaches it, and its depth is pinned the way a model's
/// unique-state count is: a shorter or longer path is a changed model. When the
/// fix merges, the fixing change deletes the entry, and the property is then
/// checked as every other property is.
#[derive(Clone, Copy, Debug)]
pub struct KnownFailure {
    /// The property's name.
    pub property: &'static str,
    /// The number of actions in the shortest counterexample.
    pub depth: usize,
}

/// [`check_model`] for a model with properties that fail on the current code.
///
/// The search runs single-threaded and to exhaustion, so the unique-state
/// count is deterministic and every counterexample is a shortest one. Each
/// property in `known` must reach a counterexample of exactly the pinned depth.
/// Every other property must hold, as [`check_model`] requires.
pub fn check_model_with_known_failures<M>(
    model: M,
    label: &str,
    limits: (usize, usize, usize),
    pinned: usize,
    known: &[KnownFailure],
) where
    M: Model + Send + Sync + 'static,
    M::State: Debug + Hash + Send + Sync + Clone + PartialEq + 'static,
    M::Action: Debug + Clone + PartialEq,
{
    let (max_depth, max_states, unique) = limits;
    let properties = model.properties();
    for failure in known {
        assert2::assert!(
            properties
                .iter()
                .any(|property| property.name == failure.property
                    && property.expectation != Expectation::Sometimes),
            "[{label}] known failure {} names no always or eventually property",
            failure.property
        );
    }
    let checker = model
        .checker()
        .target_max_depth(max_depth)
        .target_state_count(max_states)
        .finish_when(HasDiscoveries::AllOf(BTreeSet::from([EXHAUSTIVE])))
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(
        checker.max_depth() < max_depth,
        "[{label}] hit depth cap {max_depth}: depth-truncated, not exhaustive"
    );
    assert2::assert!(
        checker.state_count() < max_states,
        "[{label}] hit state cap {max_states}: truncated, not exhaustive"
    );
    assert2::assert!(
        checker.unique_state_count() < unique,
        "[{label}] unique-state bound exceeded"
    );
    assert_pinned_count(checker.unique_state_count(), pinned, label);
    for property in &properties {
        let discovery = checker.discovery(property.name);
        if let Some(failure) = known
            .iter()
            .find(|failure| failure.property == property.name)
        {
            let Some(path) = discovery else {
                panic!(
                    "[{label}] {} no longer fails: if its fix merged, remove it from the known \
                     failures",
                    property.name
                );
            };
            let actions = path.into_actions();
            assert2::assert!(
                actions.len() == failure.depth,
                "[{label}] {} fails at a changed depth: {actions:?}",
                property.name
            );
        } else if property.expectation == Expectation::Sometimes {
            assert2::assert!(
                discovery.is_some(),
                "[{label}] {} is never witnessed",
                property.name
            );
        } else if let Some(path) = discovery {
            panic!(
                "[{label}] {} fails: {:?}",
                property.name,
                path.into_actions()
            );
        }
    }
}

/// Liveness checked as safety.
///
/// A model of asynchronous orchestration splits the environment's actions into
/// faults (a crash, a partition, a pause, a lost message) and steps the system
/// takes on its own (a delivery, a retry, a reconcile pass). A state is
/// quiescent when no step is enabled: only a fault can move it. A property
/// that the system has converged in every quiescent state is then a safety
/// property, and a counterexample is a reachable state where the system has
/// stopped short of its goal.
///
/// A step that leaves the state unchanged counts as disabled. A retry loop that
/// makes no progress, such as a replica fetcher that holds a partition back on
/// every round, is then quiescent and not converged, which is the livelock the
/// property exists to find.
///
/// Two obligations keep the property honest:
///
/// - Bound the faults, not the steps. A step that `within_boundary` prunes is
///   still enabled here, so a state at the boundary never counts as
///   quiescent. A fault budget that runs out ends every fault sequence, and the
///   state after it must converge through steps alone.
/// - Keep the steps acyclic: each step moves some measure in one direction,
///   such as a counter that only rises to its bound. Steps that cycle among
///   states that are not quiescent are a livelock that quiescence cannot see.
///   A model that cannot promise this counts the steps since the last fault
///   and checks that count against a bound instead.
pub trait Quiescence: Model
where
    Self::State: PartialEq,
{
    /// Whether `action` is a fault the environment injects, rather than a
    /// step the system takes on its own.
    fn is_fault(&self, action: &Self::Action) -> bool;

    /// Whether every enabled action of `state` is a fault.
    fn quiescent(&self, state: &Self::State) -> bool {
        let mut actions = Vec::new();
        self.actions(state, &mut actions);
        actions
            .into_iter()
            .filter(|action| !self.is_fault(action))
            .all(|action| {
                self.next_state(state, action)
                    .is_none_or(|next| next == *state)
            })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{KnownFailure, Quiescence, check_model_with_known_failures},
        stateright::{Model, Property},
    };

    /// A counter that climbs to a target unless a fault jams it first. A step
    /// on a jammed counter changes nothing, so a jam is a livelock.
    struct Climb {
        target: u8,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct Counter {
        value: u8,
        jammed: bool,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Step {
        Climb,
        Jam,
    }

    impl Model for Climb {
        type Action = Step;
        type State = Counter;

        fn init_states(&self) -> Vec<Counter> {
            vec![Counter {
                value: 0,
                jammed: false,
            }]
        }

        fn actions(&self, state: &Counter, actions: &mut Vec<Step>) {
            if state.value < self.target {
                actions.push(Step::Climb);
            }
            if !state.jammed {
                actions.push(Step::Jam);
            }
        }

        fn next_state(&self, state: &Counter, action: Step) -> Option<Counter> {
            Some(match action {
                Step::Climb if state.jammed => *state,
                Step::Climb => Counter {
                    value: state.value + 1,
                    ..*state
                },
                Step::Jam => Counter {
                    jammed: true,
                    ..*state
                },
            })
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![
                Property::always(
                    "reaches_target_when_quiescent",
                    |model: &Self, s: &Counter| !model.quiescent(s) || s.value == model.target,
                ),
                Property::sometimes("reaches_target", |model: &Self, s: &Counter| {
                    s.value == model.target
                }),
            ]
        }
    }

    impl Quiescence for Climb {
        fn is_fault(&self, action: &Step) -> bool { *action == Step::Jam }
    }

    #[test]
    fn quiescence_ignores_faults_and_unchanged_steps() {
        let model = Climb { target: 2 };
        let free = Counter {
            value: 1,
            jammed: false,
        };
        let jammed = Counter {
            value: 1,
            jammed: true,
        };
        let done = Counter {
            value: 2,
            jammed: false,
        };
        assert2::assert!(!model.quiescent(&free));
        assert2::assert!(model.quiescent(&jammed));
        assert2::assert!(model.quiescent(&done));
    }

    #[test]
    fn a_jam_is_a_known_failure_one_step_deep() {
        // Unique states: three values, each free or jammed.
        check_model_with_known_failures(Climb { target: 2 }, "climb", (10, 100, 100), 6, &[
            KnownFailure {
                property: "reaches_target_when_quiescent",
                depth: 1,
            },
        ]);
    }
}
