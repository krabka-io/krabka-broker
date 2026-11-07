#![cfg(test)]
//! The runner executes hand-built [`ScenarioSpec`]s against real step
//! definitions: passing and failing steps, async steps, `*` steps, returned
//! [`Attempt`]s, strict resolution, skips and `@inprogress`.
//!
//! The specs mirror `tests/features/runner_scenarios/runner_scenarios.feature`
//! as `scenarios!` would generate them.

use {
    assert2::assert,
    krabka_bdd::{
        __private::{Flavor, StepKeyword, run, run_outcome},
        Attempt, Fixtures, Outcome, RowSpec, ScenarioSpec, StepSpec,
    },
    rstest_bdd::{Step, iter, panic_message, skip},
    rstest_bdd_macros::{given, then, when},
    std::{
        panic::{self, AssertUnwindSafe},
        time::Duration,
    },
    tokio::time::sleep,
};

const FEATURE: &str = "tests/features/runner_scenarios/runner_scenarios.feature";

#[derive(Debug, Default)]
struct Counter(i64);

type ParseAttempt = Attempt<u32, String>;

#[given("the runner counter starts at {start}")]
fn counter_starts(counter: &mut Counter, start: i64) { counter.0 = start; }

#[when("the runner counter is doubled after a pause")]
async fn counter_doubled(counter: &mut Counter) {
    sleep(Duration::from_millis(20)).await;
    counter.0 *= 2;
}

#[then("the runner counter is {expected}")]
fn counter_is(counter: &Counter, expected: i64) -> Result<(), String> {
    if counter.0 == expected {
        Ok(())
    } else {
        Err(format!("the counter is {}, not {expected}", counter.0))
    }
}

#[when("the runner attempts to parse {text}")]
fn attempt_parse(text: String) -> ParseAttempt {
    Attempt::of(|| text.parse::<u32>().map_err(|error| error.to_string()))
}

#[then("the runner attempt failed with {reason}")]
fn attempt_failed(attempt: &ParseAttempt, reason: String) {
    attempt.assert_failed_with(|error| error.contains(&reason));
}

#[given("the runner flips {side} coins")]
fn flips_side(side: String) { let _ = side; }

#[given("the runner flips heads {kind}")]
fn flips_kind(kind: String) { let _ = kind; }

#[given("the runner skips because no broker is running")]
fn skips() {
    skip!("no broker is running");
}

#[then("the runner fails if it is reached")]
fn fails_if_reached() {
    panic!("a step after a skip ran");
}

const fn scenario(name: &'static str, line: u32, steps: &'static [StepSpec]) -> ScenarioSpec {
    ScenarioSpec {
        feature_path: FEATURE,
        feature: "The runner executes scenarios built by hand",
        rule: None,
        scenario: name,
        line,
        row: None,
        tags: &[],
        steps,
        expect_failure: false,
    }
}

static PASSING_STEPS: [StepSpec; 3] = [
    StepSpec::plain(4, StepKeyword::Given, "the runner counter starts at 2"),
    StepSpec {
        star: true,
        written: "*",
        ..StepSpec::plain(
            5,
            StepKeyword::Given,
            "the runner counter is doubled after a pause",
        )
    },
    StepSpec::plain(6, StepKeyword::Then, "the runner counter is 4"),
];
static PASSING: ScenarioSpec = scenario("Every step passes", 3, &PASSING_STEPS);

static FAILING_STEPS: [StepSpec; 2] = [
    StepSpec::plain(11, StepKeyword::Given, "the runner counter starts at 1"),
    StepSpec::plain(12, StepKeyword::Then, "the runner counter is 5"),
];
static FAILING_ROW: ScenarioSpec = ScenarioSpec {
    rule: Some("A failing step names where it failed"),
    row: Some(RowSpec {
        line: 16,
        case: "mismatch",
        values: &[("case", "mismatch"), ("start", "1"), ("expected", "5")],
    }),
    ..scenario("A step fails", 10, &FAILING_STEPS)
};

static ATTEMPT_STEPS: [StepSpec; 2] = [
    StepSpec::plain(19, StepKeyword::When, "the runner attempts to parse x"),
    StepSpec::plain(
        20,
        StepKeyword::Then,
        "the runner attempt failed with invalid digit",
    ),
];
static ATTEMPT: ScenarioSpec = scenario("An attempt reaches a later Then", 18, &ATTEMPT_STEPS);

static AMBIGUOUS_STEPS: [StepSpec; 1] = [StepSpec::plain(
    23,
    StepKeyword::Given,
    "the runner flips heads coins",
)];
static AMBIGUOUS: ScenarioSpec = scenario(
    "A step matches two definitions equally well",
    22,
    &AMBIGUOUS_STEPS,
);

static UNDEFINED_STEPS: [StepSpec; 1] = [StepSpec::plain(
    26,
    StepKeyword::Given,
    "no definition matches this runner step",
)];
static UNDEFINED: ScenarioSpec = scenario("A step matches no definition", 25, &UNDEFINED_STEPS);

static SKIP_STEPS: [StepSpec; 2] = [
    StepSpec::plain(
        30,
        StepKeyword::Given,
        "the runner skips because no broker is running",
    ),
    StepSpec::plain(31, StepKeyword::Then, "the runner fails if it is reached"),
];
static SKIP_ALLOWED: ScenarioSpec = ScenarioSpec {
    tags: &["allow_skipped"],
    ..scenario("A step skips the rest of the scenario", 29, &SKIP_STEPS)
};
/// The same scenario without `@allow_skipped`.
static SKIP_NOT_ALLOWED: ScenarioSpec =
    scenario("A step skips the rest of the scenario", 29, &SKIP_STEPS);

static IN_PROGRESS_STEPS: [StepSpec; 2] = [
    StepSpec::plain(35, StepKeyword::Given, "the runner counter starts at 1"),
    StepSpec::plain(36, StepKeyword::Then, "the runner counter is 4"),
];
static IN_PROGRESS_FAILING: ScenarioSpec = ScenarioSpec {
    tags: &["inprogress"],
    expect_failure: true,
    ..scenario("A scenario in progress", 34, &IN_PROGRESS_STEPS)
};
/// An `@inprogress` scenario whose steps all pass.
static IN_PROGRESS_PASSING: ScenarioSpec = ScenarioSpec {
    tags: &["inprogress"],
    expect_failure: true,
    ..scenario("A scenario in progress", 34, &PASSING_STEPS)
};

fn counter() -> Fixtures { Fixtures::new().with("counter", Counter::default()) }

/// The panic message of `run`, which must panic.
fn panic_of(run: impl FnOnce()) -> String {
    let payload = panic::catch_unwind(AssertUnwindSafe(run)).expect_err("the run panics");
    panic_message(payload.as_ref())
}

fn failure_of(outcome: Outcome) -> String {
    match outcome {
        Outcome::Failed(failure) => failure.to_string(),
        Outcome::Passed | Outcome::Skipped(_) => panic!("expected a failure, got {outcome:?}"),
    }
}

/// `file:line` of the registered definition with `pattern`.
fn defined_at(pattern: &str) -> String {
    let step = iter::<Step>
        .into_iter()
        .find(|step| step.pattern.as_str() == pattern)
        .expect("the step is registered");
    format!("{}:{} `{pattern}`", step.file, step.line)
}

#[test]
fn a_passing_scenario_runs_every_step_including_async_and_star_steps() {
    assert!(run_outcome(&PASSING, counter(), Flavor::CurrentThread) == Outcome::Passed);
    run(&PASSING, counter(), Flavor::CurrentThread);
}

#[test]
fn a_failing_step_reports_feature_rule_example_and_step() {
    let diagnostic = failure_of(run_outcome(&FAILING_ROW, counter(), Flavor::CurrentThread));
    let expected_head = format!(
        "scenario failed at {FEATURE}:10\n  \
         Rule: A failing step names where it failed\n  \
         Scenario: A step fails\n  \
         Example: line 16: case = mismatch, start = 1, expected = 5\n  \
         Step 2 at line 12: Then the runner counter is 5\n  "
    );
    assert!(diagnostic.starts_with(&expected_head), "{diagnostic}");
    assert!(
        diagnostic.contains("the counter is 1, not 5"),
        "{diagnostic}"
    );
    assert!(panic_of(|| run(&FAILING_ROW, counter(), Flavor::CurrentThread)) == diagnostic);
}

#[test]
fn a_returned_attempt_reaches_a_later_then() {
    let fixtures = Fixtures::new().with("attempt", ParseAttempt::default());
    assert!(run_outcome(&ATTEMPT, fixtures, Flavor::CurrentThread) == Outcome::Passed);
}

#[test]
fn ambiguous_steps_fail_listing_every_definition() {
    let diagnostic = failure_of(run_outcome(
        &AMBIGUOUS,
        Fixtures::new(),
        Flavor::CurrentThread,
    ));
    let expected = format!(
        "scenario failed at {FEATURE}:22\n  \
         Scenario: A step matches two definitions equally well\n  \
         Step 1 at line 23: Given the runner flips heads coins\n  \
         ambiguous: 2 step definitions match `the runner flips heads coins` equally well:\n      \
         {}\n      \
         {}",
        defined_at("the runner flips {side} coins"),
        defined_at("the runner flips heads {kind}"),
    );
    assert!(diagnostic == expected);
}

#[test]
fn an_undefined_step_fails_naming_it() {
    let diagnostic = failure_of(run_outcome(
        &UNDEFINED,
        Fixtures::new(),
        Flavor::CurrentThread,
    ));
    assert!(diagnostic.ends_with(
        "Step 1 at line 26: Given no definition matches this runner step\n  \
         undefined: no Given step definition matches `no definition matches this runner step`"
    ));
}

#[test]
fn a_skip_stops_the_scenario() {
    let outcome = run_outcome(&SKIP_ALLOWED, Fixtures::new(), Flavor::CurrentThread);
    assert!(outcome == Outcome::Skipped(Some("no broker is running".to_owned())));
    run(&SKIP_ALLOWED, Fixtures::new(), Flavor::CurrentThread);
}

#[test]
fn a_skip_without_allow_skipped_fails() {
    let message = panic_of(|| run(&SKIP_NOT_ALLOWED, Fixtures::new(), Flavor::CurrentThread));
    assert!(
        message
            == format!(
                "{FEATURE}:29: Scenario: A step skips the rest of the scenario skipped without \
                 @allow_skipped: no broker is running"
            )
    );
}

#[test]
fn an_in_progress_scenario_that_fails_passes_the_test() {
    assert!(matches!(
        run_outcome(&IN_PROGRESS_FAILING, counter(), Flavor::CurrentThread),
        Outcome::Failed(_)
    ));
    run(&IN_PROGRESS_FAILING, counter(), Flavor::CurrentThread);
}

#[test]
fn an_in_progress_scenario_that_passes_fails_the_test() {
    let message = panic_of(|| run(&IN_PROGRESS_PASSING, counter(), Flavor::CurrentThread));
    assert!(
        message
            == format!(
                "{FEATURE}:34: Scenario: A scenario in progress now passes: remove @inprogress"
            )
    );
}
