#![cfg(test)]
//! A scenario on `runtime = "tokio-multi-thread"` keeps the tasks its steps
//! spawn running between steps.
//!
//! The When blocks the test thread on a plain channel that only the spawned
//! task can fill. On a current-thread runtime nothing polls the task while the
//! test thread blocks, so the When would time out.

use {
    assert2::assert,
    krabka_bdd::{Given, TokioMultiThreadContext},
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

struct World {
    task: Given<JoinHandle<()>>,
    running: Given<Receiver<()>>,
    release: Given<Sender<()>>,
}

#[rstest::fixture]
fn world() -> World {
    let step = "Given a background task has been spawned that waits for a release signal";
    World {
        task: Given::new("task", step),
        running: Given::new("running", step),
        release: Given::new("release", step),
    }
}

#[given("a background task has been spawned that waits for a release signal")]
fn spawn_task(
    world: &mut World,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    let (running_tx, running_rx) = mpsc::channel();
    let (release_tx, release_rx) = oneshot::channel();
    world.task.set(runtime.spawn(async move {
        running_tx.send(()).unwrap();
        release_rx.await.unwrap();
    }));
    world.running.set(running_rx);
    world.release.set(release_tx);
}

#[when("the test thread waits for the task to report that it runs")]
fn wait_for_report(world: &World) {
    assert!(world.running.get().recv_timeout(REPORT_TIMEOUT) == Ok(()));
}

#[then("the background task is still running")]
fn task_still_running(
    world: &mut World,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    assert!(!world.task.get().is_finished());
    world.release.take().send(()).unwrap();
    assert!(runtime.block_on(world.task.take()).is_ok());
}

krabka_bdd::scenarios!(
    "tests/features/multi_thread",
    fixtures = [world: World],
    runtime = "tokio-multi-thread",
    workers = 2,
);
