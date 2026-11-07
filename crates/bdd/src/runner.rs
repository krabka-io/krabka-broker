//! Runs one scenario: World and hook setup, the steps, and teardown that
//! always runs.

use {
    crate::{
        failure::{Failure, Location},
        fixtures::Fixtures,
        hooks::{self, Hook, HookPhase},
        spec::ScenarioSpec,
        step_check::{Definition, Resolution, registered_definitions, resolve},
    },
    futures_util::FutureExt,
    rstest_bdd::{
        Step, StepContext, StepExecutionMode, StepKeyword, StepPattern,
        execution::{StepExecutionRequest, execute_step, execute_step_async},
        iter, panic_message,
    },
    std::{
        any::Any,
        future::Future,
        panic::{self, AssertUnwindSafe},
        ptr,
    },
    tokio::runtime::{Builder, Runtime},
};

/// The Tokio runtime a scenario runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    /// One thread: a task a step spawns moves only while a step awaits.
    CurrentThread,
    /// Worker threads, so a task a step spawns keeps running between steps.
    MultiThread {
        /// The number of worker threads; `None` takes Tokio's default.
        workers: Option<usize>,
    },
}

impl Flavor {
    /// A runtime of this flavor, with I/O and time enabled.
    ///
    /// # Panics
    ///
    /// Panics when Tokio cannot build the runtime.
    #[must_use]
    pub fn build(self) -> Runtime {
        let mut builder = match self {
            Self::CurrentThread => Builder::new_current_thread(),
            Self::MultiThread { workers } => {
                let mut builder = Builder::new_multi_thread();
                if let Some(workers) = workers {
                    builder.worker_threads(workers);
                }
                builder
            }
        };
        builder
            .enable_all()
            .build()
            .unwrap_or_else(|error| panic!("cannot build the scenario's runtime: {error}"))
    }
}

/// How a scenario ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Every step passed.
    Passed,
    /// A step, hook or World failed.
    Failed(Failure),
    /// A step skipped the rest of the scenario, with its reason.
    Skipped(Option<String>),
}

/// Runs `scenario` with `fixtures` and fails the test when it should.
///
/// # Panics
///
/// Panics with the failure diagnostic when the scenario fails; with
/// `skipped without @allow_skipped` when a step skips and the scenario lacks
/// `@allow_skipped`; and, for an `@inprogress` scenario, with
/// `now passes: remove @inprogress` when it passes. An `@inprogress`
/// scenario that fails prints its diagnostic to stderr and returns.
pub fn run(scenario: &'static ScenarioSpec, fixtures: Fixtures, flavor: Flavor) {
    let outcome = run_outcome(scenario, fixtures, flavor);
    match verdict(scenario, &outcome) {
        Verdict::Pass => {}
        Verdict::ExpectedFailure(report) => eprintln!("{report}"),
        Verdict::Fail(report) => panic!("{report}"),
    }
}

/// Runs `scenario` with `fixtures` and returns how it ended, without
/// panicking.
///
/// The order is: the Worlds' `before`, the `before` hooks, the steps, the
/// `after` hooks, the Worlds' `after`. The `after` hooks and the Worlds'
/// `after` always run, whether the scenario passed, failed, skipped or
/// panicked.
///
/// The Worlds, the hooks and each `async fn` step run in `block_on` on one
/// runtime of `flavor`. A sync step runs outside `block_on`, with the
/// runtime's context entered, as rstest-bdd runs it: it can `tokio::spawn`
/// onto the scenario's runtime, and it can `block_on` a runtime of its own.
/// The fixtures are dropped before the runtime.
pub fn run_outcome(scenario: &'static ScenarioSpec, fixtures: Fixtures, flavor: Flavor) -> Outcome {
    let runtime = flavor.build();
    let outcome = run_with(
        scenario,
        &fixtures,
        &hooks::registered(),
        &registered_definitions(),
        &runtime,
    );
    let context = runtime.enter();
    drop(fixtures);
    drop(context);
    outcome
}

/// [`run_outcome`] over the given hooks and step definitions.
fn run_with(
    scenario: &'static ScenarioSpec,
    fixtures: &Fixtures,
    hooks: &[&Hook],
    definitions: &[Definition<'_>],
    runtime: &Runtime,
) -> Outcome {
    let mut ctx = StepContext::default();
    fixtures.insert_into(&mut ctx);
    let worlds = fixtures.worlds();

    let mut outcome = Outcome::Passed;
    for world in &worlds {
        if let Err(message) = runtime.block_on(guarded(world.before(scenario))) {
            outcome = fail(
                outcome,
                scenario,
                Location::WorldBefore(world.name()),
                message,
            );
            break;
        }
    }
    if outcome == Outcome::Passed {
        outcome = runtime.block_on(run_hooks(
            scenario,
            hooks,
            HookPhase::BeforeScenario,
            outcome,
        ));
    }
    if outcome == Outcome::Passed {
        outcome = run_steps(scenario, &mut ctx, definitions, runtime);
    }
    outcome = runtime.block_on(run_hooks(
        scenario,
        hooks,
        HookPhase::AfterScenario,
        outcome,
    ));
    for world in worlds.iter().rev() {
        if let Err(message) = runtime.block_on(guarded(world.after(scenario, &outcome))) {
            outcome = fail(
                outcome,
                scenario,
                Location::WorldAfter(world.name()),
                message,
            );
        }
    }
    outcome
}

/// Runs the hooks of `phase`, given the outcome so far.
///
/// A failing `before` hook stops the `before` hooks after it; every `after`
/// hook runs.
async fn run_hooks(
    scenario: &'static ScenarioSpec,
    hooks: &[&Hook],
    phase: HookPhase,
    mut outcome: Outcome,
) -> Outcome {
    let location = |hook: Option<&Hook>| {
        let (file, line) = hook.map_or(("", 0), |hook| (hook.file, hook.line));
        match phase {
            HookPhase::BeforeScenario => Location::BeforeHook { file, line },
            HookPhase::AfterScenario => Location::AfterHook { file, line },
        }
    };
    let selected = match hooks::select(hooks, phase, scenario.tags) {
        Ok(selected) => selected,
        Err(message) => return fail(outcome, scenario, location(None), message),
    };
    for hook in selected {
        let result = match phase {
            HookPhase::BeforeScenario => guarded((hook.run)(scenario, None)).await,
            HookPhase::AfterScenario => guarded((hook.run)(scenario, Some(&outcome))).await,
        };
        if let Err(message) = result {
            outcome = fail(outcome, scenario, location(Some(hook)), message);
            if phase == HookPhase::BeforeScenario {
                break;
            }
        }
    }
    outcome
}

/// Runs every step in order until one fails or skips.
fn run_steps(
    scenario: &'static ScenarioSpec,
    ctx: &mut StepContext<'_>,
    definitions: &[Definition<'_>],
    runtime: &Runtime,
) -> Outcome {
    for (index, step) in scenario.steps.iter().enumerate() {
        let at = || Location::Step { index, step: *step };
        let resolution = resolve(definitions, step.keyword, step.star, step.text);
        let (keyword, pattern) = match &resolution {
            Resolution::Found(definition) => (definition.keyword, definition.pattern),
            Resolution::Undefined | Resolution::Ambiguous(_) => {
                let message = resolution
                    .problem(step.keyword, step.star, step.text)
                    .unwrap_or_default();
                return Outcome::Failed(Failure::new(scenario, at(), message));
            }
        };
        let request = StepExecutionRequest {
            index,
            keyword,
            text: step.text,
            docstring: step.docstring,
            table: step.table,
            feature_path: scenario.feature_path,
            scenario_name: scenario.scenario,
        };
        let result = if is_async(keyword, pattern) {
            runtime.block_on(guarded(execute_step_async(&request, ctx)))
        } else {
            let _context = runtime.enter();
            panic::catch_unwind(AssertUnwindSafe(|| execute_step(&request, ctx)))
                .map_err(|payload| panicked(payload.as_ref()))
        };
        match result {
            Err(message) => return Outcome::Failed(Failure::new(scenario, at(), message)),
            Ok(Err(error)) if error.is_skip() => {
                return Outcome::Skipped(error.skip_message().map(ToOwned::to_owned));
            }
            Ok(Err(error)) => {
                return Outcome::Failed(Failure::new(scenario, at(), error.to_string()));
            }
            Ok(Ok(Some(value))) => {
                let _ = ctx.insert_value(value);
            }
            Ok(Ok(None)) => {}
        }
    }
    Outcome::Passed
}

/// Whether the registered definition of `keyword` and `pattern` is an
/// `async fn`.
fn is_async(keyword: StepKeyword, pattern: &StepPattern) -> bool {
    iter::<Step>.into_iter().any(|step| {
        step.keyword == keyword
            && ptr::eq(step.pattern, pattern)
            && matches!(step.execution_mode, StepExecutionMode::Async)
    })
}

/// Awaits `future`, turning a panic into its message.
async fn guarded<F: Future>(future: F) -> Result<F::Output, String> {
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .map_err(|payload| panicked(payload.as_ref()))
}

/// The failure message of a panic with `payload`.
fn panicked(payload: &(dyn Any + Send)) -> String {
    format!("panicked: {}", panic_message(payload))
}

/// Records a problem: the first one makes the outcome Failed, later ones are
/// added to the failure.
fn fail(
    outcome: Outcome,
    scenario: &'static ScenarioSpec,
    location: Location,
    message: String,
) -> Outcome {
    match outcome {
        Outcome::Failed(mut failure) => {
            failure.push(location, message);
            Outcome::Failed(failure)
        }
        Outcome::Passed | Outcome::Skipped(_) => {
            Outcome::Failed(Failure::new(scenario, location, message))
        }
    }
}

/// What the test does with an outcome.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    /// The test passes.
    Pass,
    /// The test passes and prints this report: an `@inprogress` scenario
    /// failed, as expected.
    ExpectedFailure(String),
    /// The test fails with this report.
    Fail(String),
}

fn verdict(scenario: &ScenarioSpec, outcome: &Outcome) -> Verdict {
    let place = format!(
        "{}:{}: Scenario: {}",
        scenario.feature_path, scenario.line, scenario.scenario
    );
    match outcome {
        Outcome::Skipped(_) if scenario.tags.contains(&"allow_skipped") => Verdict::Pass,
        Outcome::Skipped(reason) => Verdict::Fail(format!(
            "{place} skipped without @allow_skipped: {}",
            reason.as_deref().unwrap_or("no reason given")
        )),
        Outcome::Passed if scenario.expect_failure => {
            Verdict::Fail(format!("{place} now passes: remove @inprogress"))
        }
        Outcome::Passed => Verdict::Pass,
        Outcome::Failed(failure) if scenario.expect_failure => Verdict::ExpectedFailure(format!(
            "@inprogress scenario failed, as expected:\n{failure}"
        )),
        Outcome::Failed(failure) => Verdict::Fail(failure.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::spec::StepSpec,
        assert2::assert,
        rstest::rstest,
        rstest_bdd::{StepKeyword, StepPattern},
    };

    const fn scenario(tags: &'static [&'static str], expect_failure: bool) -> ScenarioSpec {
        ScenarioSpec {
            feature_path: "tests/features/a.feature",
            feature: "A",
            rule: None,
            scenario: "S",
            line: 4,
            row: None,
            tags,
            steps: &[],
            expect_failure,
        }
    }

    static PLAIN: ScenarioSpec = scenario(&[], false);
    static ALLOW_SKIPPED: ScenarioSpec = scenario(&["allow_skipped"], false);
    static IN_PROGRESS: ScenarioSpec = scenario(&[], true);

    fn failed() -> Outcome {
        Outcome::Failed(Failure::new(
            &PLAIN,
            Location::WorldBefore("world"),
            "boom".to_owned(),
        ))
    }

    const DIAGNOSTIC: &str = "scenario failed at tests/features/a.feature:4\n  Scenario: S\n  World `world` before\n  boom";

    #[rstest]
    #[case::passed(&PLAIN, Outcome::Passed, Verdict::Pass)]
    #[case::failed(&PLAIN, failed(), Verdict::Fail(DIAGNOSTIC.to_owned()))]
    #[case::skipped_without_allow(
        &PLAIN,
        Outcome::Skipped(Some("no broker".to_owned())),
        Verdict::Fail("tests/features/a.feature:4: Scenario: S skipped without @allow_skipped: no broker".to_owned()),
    )]
    #[case::skipped_without_reason(
        &PLAIN,
        Outcome::Skipped(None),
        Verdict::Fail("tests/features/a.feature:4: Scenario: S skipped without @allow_skipped: no reason given".to_owned()),
    )]
    #[case::skipped_with_allow(&ALLOW_SKIPPED, Outcome::Skipped(None), Verdict::Pass)]
    #[case::in_progress_passes(
        &IN_PROGRESS,
        Outcome::Passed,
        Verdict::Fail("tests/features/a.feature:4: Scenario: S now passes: remove @inprogress".to_owned()),
    )]
    #[case::in_progress_fails(
        &IN_PROGRESS,
        failed(),
        Verdict::ExpectedFailure(format!("@inprogress scenario failed, as expected:\n{DIAGNOSTIC}")),
    )]
    fn verdict_applies_the_outcome(
        #[case] scenario: &ScenarioSpec,
        #[case] outcome: Outcome,
        #[case] expected: Verdict,
    ) {
        assert!(verdict(scenario, &outcome) == expected);
    }

    #[rstest]
    #[case::first_problem_from_passed(Outcome::Passed)]
    #[case::first_problem_from_skipped(Outcome::Skipped(None))]
    fn fail_starts_a_failure(#[case] outcome: Outcome) {
        let expected = Failure::new(&PLAIN, Location::WorldAfter("w"), "x".to_owned());
        assert!(
            fail(outcome, &PLAIN, Location::WorldAfter("w"), "x".to_owned())
                == Outcome::Failed(expected)
        );
    }

    #[test]
    fn fail_adds_to_an_existing_failure() {
        let mut expected = Failure::new(&PLAIN, Location::WorldBefore("world"), "boom".to_owned());
        expected.push(Location::WorldAfter("w"), "x".to_owned());
        assert!(
            fail(failed(), &PLAIN, Location::WorldAfter("w"), "x".to_owned())
                == Outcome::Failed(expected)
        );
    }

    static NO_STEP: [StepSpec; 1] = [StepSpec {
        line: 5,
        keyword: StepKeyword::Given,
        star: false,
        written: "Given",
        text: "nothing defines this",
        docstring: None,
        table: None,
    }];
    static UNDEFINED: ScenarioSpec = ScenarioSpec {
        steps: &NO_STEP,
        ..scenario(&[], false)
    };
    static OTHER: StepPattern = StepPattern::new("something else");

    #[test]
    fn run_with_fails_an_undefined_step_without_calling_the_registry() {
        let definitions = [Definition {
            keyword: StepKeyword::Given,
            pattern: &OTHER,
            file: "s.rs",
            line: 1,
        }];
        let runtime = Flavor::CurrentThread.build();
        let outcome = run_with(&UNDEFINED, &Fixtures::new(), &[], &definitions, &runtime);
        let expected = Failure::new(
            &UNDEFINED,
            Location::Step {
                index: 0,
                step: NO_STEP[0],
            },
            "undefined: no Given step definition matches `nothing defines this`".to_owned(),
        );
        assert!(outcome == Outcome::Failed(expected));
    }
}
