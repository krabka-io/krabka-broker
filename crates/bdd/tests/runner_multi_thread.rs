#![cfg(test)]
//! A task that a Given spawns keeps running in the When on a multi-thread
//! test runtime.
//!
//! The When blocks the test thread on a plain channel that only the spawned
//! task can fill, so the task must run on a worker thread while the step
//! blocks. The spec mirrors
//! `tests/features/runner_multi_thread/runner_multi_thread.feature`.

use {
    assert2::assert,
    krabka_bdd::{
        __private::{StepKeyword, run, run_outcome},
        Fixtures, Outcome, ScenarioSpec, StepSpec,
    },
    rstest_bdd_macros::{given, then, when},
    std::{
        sync::mpsc::{self, Receiver},
        time::Duration,
    },
    tokio::{
        sync::oneshot::{self, Sender},
        task::JoinHandle,
    },
};

/// Long enough for any loaded runner, short enough to fail rather than hang.
const REPORT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Background {
    task: Option<JoinHandle<()>>,
    running: Option<Receiver<()>>,
    release: Option<Sender<()>>,
}

#[given("the runner has spawned a task that waits for a release signal")]
fn spawn_task(background: &mut Background) {
    let (running_tx, running_rx) = mpsc::channel();
    let (release_tx, release_rx) = oneshot::channel();
    background.task = Some(tokio::spawn(async move {
        running_tx.send(()).unwrap();
        release_rx.await.unwrap();
    }));
    background.running = Some(running_rx);
    background.release = Some(release_tx);
}

#[when("the runner waits for the task to report that it runs")]
fn wait_for_report(background: &Background) {
    let running = background.running.as_ref().unwrap();
    assert!(running.recv_timeout(REPORT_TIMEOUT) == Ok(()));
}

#[then("the runner task is still running until it is released")]
async fn task_still_running(background: &mut Background) {
    let task = background.task.take().unwrap();
    assert!(!task.is_finished());
    background.release.take().unwrap().send(()).unwrap();
    assert!(task.await.is_ok());
}

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

static STEPS: [StepSpec; 3] = [
    step(
        4,
        StepKeyword::Given,
        "the runner has spawned a task that waits for a release signal",
    ),
    step(
        5,
        StepKeyword::When,
        "the runner waits for the task to report that it runs",
    ),
    step(
        6,
        StepKeyword::Then,
        "the runner task is still running until it is released",
    ),
];

static SPAWNED_TASK: ScenarioSpec = ScenarioSpec {
    feature_path: "tests/features/runner_multi_thread/runner_multi_thread.feature",
    feature: "A runner scenario on a multi-thread runtime",
    rule: None,
    scenario: "A task spawned in a Given is alive in the When",
    line: 3,
    row: None,
    tags: &[],
    steps: &STEPS,
    expect_failure: false,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_spawned_in_a_given_runs_in_the_when() {
    let outcome = run_outcome(
        &SPAWNED_TASK,
        Fixtures::new().with("background", Background::default()),
    )
    .await;
    assert!(outcome == Outcome::Passed);
    run(
        &SPAWNED_TASK,
        Fixtures::new().with("background", Background::default()),
    )
    .await;
}
