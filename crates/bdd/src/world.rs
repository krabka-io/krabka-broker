//! Scenario state that sets itself up and tears itself down.

use {
    crate::{runner::Outcome, spec::ScenarioSpec},
    std::future::Future,
};

/// A scenario fixture with setup and teardown of its own.
///
/// A fixture passed as `world = name` gets [`before`](World::before) before
/// the first step and [`after`](World::after) once the scenario has ended,
/// whether it passed, failed, skipped or panicked. Both default to doing
/// nothing. An implementation may write them as `async fn`.
///
/// # Examples
///
/// ```
/// use krabka_bdd::{Outcome, ScenarioSpec, World};
///
/// #[derive(Default)]
/// struct LogWorld {
///     directory: Option<String>,
/// }
///
/// impl World for LogWorld {
///     async fn before(&mut self, scenario: &ScenarioSpec) {
///         self.directory = Some(format!("/tmp/{}", scenario.scenario));
///     }
///
///     async fn after(&mut self, _scenario: &ScenarioSpec, _outcome: &Outcome) {
///         self.directory = None;
///     }
/// }
/// ```
pub trait World {
    /// Runs before the scenario's first step and its `before` hooks.
    fn before(&mut self, scenario: &ScenarioSpec) -> impl Future<Output = ()> {
        let _ = scenario;
        async {}
    }

    /// Runs after the scenario's `after` hooks, with the scenario's outcome.
    fn after(&mut self, scenario: &ScenarioSpec, outcome: &Outcome) -> impl Future<Output = ()> {
        let _ = (scenario, outcome);
        async {}
    }
}
