//! A scenario harness that runs each scenario inside a multi-thread Tokio
//! runtime.

use {
    rstest_bdd_harness::{
        AttributePolicy, HarnessAdapter, HarnessError, HarnessResult, ScenarioRunRequest,
        TestAttribute,
    },
    std::future::Future,
    tokio::{
        runtime::{Builder, Handle},
        task::JoinHandle,
    },
};

/// Runs a scenario with a multi-thread Tokio runtime that lives for the whole
/// scenario.
///
/// rstest-bdd's own Tokio harness uses a current-thread runtime, so a task
/// spawned in one step makes progress only while a later step awaits. This
/// harness builds a multi-thread runtime instead: a task that a Given step
/// spawns runs on a worker thread and keeps running between steps.
///
/// The scenario's steps run synchronously on the test thread, inside the
/// runtime's context but not inside `block_on`. A step can therefore call
/// [`tokio::spawn`] directly, and drive a future to completion with
/// [`TokioMultiThreadContext::block_on`]. Steps are synchronous functions; an
/// `async fn` step is not supported.
///
/// `WORKERS` sets the number of worker threads. The default, 0, keeps
/// Tokio's default of one worker per core.
///
/// The runtime is dropped when the scenario returns. Tasks that are still
/// running are cancelled at their next yield point.
///
/// # Examples
///
/// A step reaches the runtime through the harness context:
///
/// ```ignore
/// use krabka_bdd::TokioMultiThreadContext;
/// use rstest_bdd_macros::given;
///
/// #[given("a background writer is running")]
/// fn background_writer(#[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext) {
///     runtime.spawn(async { /* ... */ });
/// }
///
/// #[scenario(
///     path = "tests/features/writer.feature",
///     harness = krabka_bdd::TokioMultiThreadHarness,
///     attributes = krabka_bdd::MultiThreadAttributePolicy,
/// )]
/// fn writer() {}
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioMultiThreadHarness<const WORKERS: usize = 0>;

impl<const WORKERS: usize> HarnessAdapter for TokioMultiThreadHarness<WORKERS> {
    type Context = TokioMultiThreadContext;

    fn run<T>(&self, request: ScenarioRunRequest<'_, Self::Context, T>) -> HarnessResult<T> {
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        if WORKERS > 0 {
            builder.worker_threads(WORKERS);
        }
        let runtime = builder.build().map_err(HarnessError::RuntimeBuildFailed)?;
        let context = TokioMultiThreadContext {
            handle: runtime.handle().clone(),
        };
        let output = {
            let _guard = runtime.enter();
            request.run(context)
        };
        drop(runtime);
        Ok(output)
    }
}

/// The harness context that [`TokioMultiThreadHarness`] hands to each step.
///
/// A step takes it as
/// `#[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext`.
#[derive(Debug, Clone)]
pub struct TokioMultiThreadContext {
    handle: Handle,
}

impl TokioMultiThreadContext {
    /// Returns the handle of the scenario's runtime.
    #[must_use]
    pub const fn handle(&self) -> &Handle { &self.handle }

    /// Spawns `future` on the scenario's runtime.
    ///
    /// The task runs on a worker thread and keeps running between steps until
    /// it finishes or the scenario ends.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Drives `future` to completion from a synchronous step and returns its
    /// output.
    ///
    /// # Panics
    ///
    /// Panics if called from inside an async context, such as a task spawned
    /// on the runtime, as [`Handle::block_on`] does.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output { self.handle.block_on(future) }
}

/// The test attributes for a scenario that runs on [`TokioMultiThreadHarness`].
///
/// It renders `#[rstest::rstest]` only. The scenario function stays
/// synchronous and the harness owns the runtime, so no `#[tokio::test]`
/// attribute is wanted. rstest-bdd resolves a policy it does not know by name
/// to `#[rstest::rstest]` too, so this policy states what the macro emits.
#[derive(Debug, Clone, Copy, Default)]
pub struct MultiThreadAttributePolicy;

const MULTI_THREAD_TEST_ATTRIBUTES: [TestAttribute; 1] = [TestAttribute::new("rstest::rstest")];

impl AttributePolicy for MultiThreadAttributePolicy {
    fn test_attributes() -> &'static [TestAttribute] { &MULTI_THREAD_TEST_ATTRIBUTES }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        rstest_bdd_harness::{ScenarioMetadata, ScenarioRunner},
        std::{sync::mpsc, time::Duration},
        tokio::{runtime::RuntimeFlavor, sync::oneshot, time::sleep},
    };

    const WAIT: Duration = Duration::from_secs(5);

    fn run_scenario<const WORKERS: usize, T>(
        scenario: impl FnOnce(TokioMultiThreadContext) -> T,
    ) -> T {
        TokioMultiThreadHarness::<WORKERS>
            .run(ScenarioRunRequest::new(
                ScenarioMetadata::default(),
                ScenarioRunner::new(scenario),
            ))
            .expect("the runtime builds")
    }

    fn spawned_task_runs_while_the_scenario_blocks<const WORKERS: usize>() {
        let finished = run_scenario::<WORKERS, _>(|runtime| {
            let (started_tx, started_rx) = mpsc::channel();
            let (_release_tx, release_rx) = oneshot::channel::<()>();
            let task = runtime.spawn(async move {
                started_tx.send(()).expect("the scenario is listening");
                let _ = release_rx.await;
            });
            started_rx
                .recv_timeout(WAIT)
                .expect("the task runs without the scenario awaiting");
            task.is_finished()
        });
        assert!(!finished);
    }

    #[test]
    fn spawned_task_runs_while_the_scenario_blocks_on_default_workers() {
        spawned_task_runs_while_the_scenario_blocks::<0>();
    }

    #[test]
    fn spawned_task_runs_while_the_scenario_blocks_on_one_worker() {
        spawned_task_runs_while_the_scenario_blocks::<1>();
    }

    #[test]
    fn tokio_spawn_works_inside_a_step() {
        let value = run_scenario::<0, _>(|runtime| {
            let task = tokio::spawn(async { 7 });
            runtime.block_on(task).expect("the task completes")
        });
        assert!(value == 7);
    }

    #[test]
    fn block_on_returns_the_future_output() {
        let value = run_scenario::<1, _>(|runtime| {
            runtime.block_on(async {
                sleep(Duration::from_millis(1)).await;
                42
            })
        });
        assert!(value == 42);
    }

    #[test]
    fn handle_is_a_multi_thread_runtime() {
        let flavor = run_scenario::<1, _>(|runtime| runtime.handle().runtime_flavor());
        assert!(flavor == RuntimeFlavor::MultiThread);
    }

    #[test]
    fn attribute_policy_renders_rstest_only() {
        let rendered: Vec<String> = MultiThreadAttributePolicy::test_attributes()
            .iter()
            .map(|attribute| attribute.render())
            .collect();
        assert!(rendered == ["#[rstest::rstest]"]);
    }
}
