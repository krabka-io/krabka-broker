#![cfg(test)]
//! The `after` hooks and the World's `after` run however a scenario ends:
//! pass, failure, panic and skip.
//!
//! The specs mirror `tests/features/runner_teardown/runner_teardown.feature`.
//! The World records into a log the test keeps an `Arc` of; the hooks record
//! into a static log, keyed by scenario name because tests run concurrently.

use {
    assert2::assert,
    krabka_bdd::{
        Fixtures, Outcome, ScenarioSpec, StepSpec, World,
        __private::{Hook, HookPhase, StepKeyword, inventory, run_outcome},
    },
    rstest::rstest,
    rstest_bdd::skip,
    rstest_bdd_macros::{given, when},
    std::{
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
    },
};

const FEATURE: &str = "tests/features/runner_teardown/runner_teardown.feature";

fn kind(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Passed => "passed",
        Outcome::Failed(_) => "failed",
        Outcome::Skipped(_) => "skipped",
    }
}

#[derive(Default)]
struct TeardownWorld {
    log: Arc<Mutex<Vec<String>>>,
}

impl TeardownWorld {
    fn record(&self, entry: String) { self.log.lock().unwrap().push(entry); }
}

impl World for TeardownWorld {
    async fn before(&mut self, _scenario: &ScenarioSpec) { self.record("world before".to_owned()); }

    async fn after(&mut self, _scenario: &ScenarioSpec, outcome: &Outcome) {
        self.record(format!("world after: {}", kind(outcome)));
    }
}

static HOOK_LOG: Mutex<Vec<(&str, String)>> = Mutex::new(Vec::new());

fn hook_entries(scenario: &str) -> Vec<String> {
    HOOK_LOG
        .lock()
        .unwrap()
        .iter()
        .filter(|(name, _)| *name == scenario)
        .map(|(_, entry)| entry.clone())
        .collect()
}

fn record_hook(scenario: &ScenarioSpec, entry: String) {
    HOOK_LOG.lock().unwrap().push((scenario.scenario, entry));
}

fn before_hook<'a>(
    scenario: &'a ScenarioSpec,
    _outcome: Option<&'a Outcome>,
) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(async move { record_hook(scenario, "before hook".to_owned()) })
}

fn late_after_hook<'a>(
    scenario: &'a ScenarioSpec,
    outcome: Option<&'a Outcome>,
) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(async move {
        let entry = format!("after hook 5: {}", outcome.map_or("none", kind));
        record_hook(scenario, entry);
    })
}

fn early_after_hook<'a>(
    scenario: &'a ScenarioSpec,
    outcome: Option<&'a Outcome>,
) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(async move {
        let entry = format!("after hook -5: {}", outcome.map_or("none", kind));
        record_hook(scenario, entry);
    })
}

inventory::submit! {
    Hook { phase: HookPhase::BeforeScenario, tags: Some("@teardown"), order: 0, file: file!(), line: line!(), run: before_hook }
}
inventory::submit! {
    Hook { phase: HookPhase::AfterScenario, tags: Some("@teardown"), order: 5, file: file!(), line: line!(), run: late_after_hook }
}
inventory::submit! {
    Hook { phase: HookPhase::AfterScenario, tags: Some("@teardown and not @untracked"), order: -5, file: file!(), line: line!(), run: early_after_hook }
}

#[given("the teardown world is ready")]
fn world_ready(world: &TeardownWorld) { world.record("step: ready".to_owned()); }

#[when("the teardown step fails")]
fn step_fails() -> Result<(), String> { Err("the step failed on purpose".to_owned()) }

#[when("the teardown step panics")]
fn step_panics() { panic!("kaboom"); }

#[when("the teardown step skips")]
fn step_skips() { skip!("nothing to tear down"); }

const fn step(line: u32, keyword: StepKeyword, text: &'static str) -> StepSpec {
    StepSpec {
        line,
        keyword,
        star: false,
        written: keyword.as_str(),
        text,
        docstring: None,
        table: None,
    }
}

const READY: StepSpec = step(5, StepKeyword::Given, "the teardown world is ready");

const fn scenario(
    name: &'static str,
    line: u32,
    tags: &'static [&'static str],
    steps: &'static [StepSpec],
) -> ScenarioSpec {
    ScenarioSpec {
        feature_path: FEATURE,
        feature: "Teardown runs however a scenario ends",
        rule: None,
        scenario: name,
        line,
        row: None,
        tags,
        steps,
        expect_failure: false,
    }
}

static PASSES: ScenarioSpec = scenario("The scenario passes", 4, &["teardown"], &[READY]);
static FAILS: ScenarioSpec = scenario(
    "A step fails",
    8,
    &["teardown"],
    &[READY, step(10, StepKeyword::When, "the teardown step fails")],
);
static PANICS: ScenarioSpec = scenario(
    "A step panics",
    13,
    &["teardown"],
    &[READY, step(15, StepKeyword::When, "the teardown step panics")],
);
static SKIPS: ScenarioSpec = scenario(
    "A step skips",
    18,
    &["teardown", "allow_skipped"],
    &[READY, step(20, StepKeyword::When, "the teardown step skips")],
);
/// Untagged: no hook applies.
static UNHOOKED: ScenarioSpec = scenario("Untagged", 4, &[], &[READY]);

#[rstest]
#[case::passes(&PASSES, "passed")]
#[case::fails(&FAILS, "failed")]
#[case::panics(&PANICS, "failed")]
#[case::skips(&SKIPS, "skipped")]
#[tokio::test]
async fn after_hooks_and_world_after_always_run(
    #[case] scenario: &'static ScenarioSpec,
    #[case] expected: &str,
) {
    let world = TeardownWorld::default();
    let log = Arc::clone(&world.log);
    let outcome = run_outcome(scenario, Fixtures::new().with_world("world", world)).await;

    assert!(kind(&outcome) == expected);
    assert!(
        *log.lock().unwrap()
            == [
                "world before".to_owned(),
                "step: ready".to_owned(),
                format!("world after: {expected}"),
            ]
    );
    assert!(
        hook_entries(scenario.scenario)
            == [
                "before hook".to_owned(),
                format!("after hook -5: {expected}"),
                format!("after hook 5: {expected}"),
            ]
    );
}

#[tokio::test]
async fn a_panicking_step_reports_the_panic_message() {
    let outcome = run_outcome(
        &PANICS,
        Fixtures::new().with_world("world", TeardownWorld::default()),
    )
    .await;
    let Outcome::Failed(failure) = outcome else {
        panic!("expected a failure, got {outcome:?}");
    };
    assert!(failure.message().contains("kaboom"), "{failure}");
}

#[tokio::test]
async fn hooks_apply_only_to_matching_tags() {
    let world = TeardownWorld::default();
    let log = Arc::clone(&world.log);
    let outcome = run_outcome(&UNHOOKED, Fixtures::new().with_world("world", world)).await;
    assert!(outcome == Outcome::Passed);
    assert!(log.lock().unwrap().len() == 3);
    assert!(hook_entries("Untagged").is_empty());
}
