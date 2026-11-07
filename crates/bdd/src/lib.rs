//! Krabka's BDD layer over rstest-bdd.
//!
//! rstest-bdd keeps the step registry and the step definitions. `krabka-bdd`
//! reads the features, holds them to the [Gherkin style guide], generates one
//! test per scenario and per Examples row, and runs the steps. The guide's
//! krabka-bdd section is the user manual; this page lists the entry points.
//!
//! # Running features
//!
//! ```ignore
//! krabka_bdd::scenarios!(
//!     "tests/features/log_policy",
//!     fixtures = [log_policy: LogPolicyScenario],
//! );
//! ```
//!
//! [`scenarios!`] lints every `.feature` file under the directory, relative
//! to the crate's manifest; each house-style error is a `compile_error!` with
//! `file:line:col`, the rule id and the fix. A clean directory becomes one
//! `#[tokio::test]` per scenario and per row, plus a registry check. It takes:
//!
//! - the directory, positional or as `dir = ".."` / `path = ".."`;
//! - `fixtures = [name: Type, ..]`, the rstest fixtures each test hands its
//!   steps;
//! - `world = name`, the fixture whose [`World`] hooks run;
//! - `tags = ".."`, a tag expression that selects the scenarios to generate;
//! - `runtime = "tokio-current-thread"` (the default) or
//!   `runtime = "tokio-multi-thread"` with an optional `workers = N`.
//!
//! # Steps and hooks
//!
//! - [`given`], [`when`], [`then`] and [`step`] wrap rstest-bdd's step
//!   attributes with Cucumber expressions and expansion-time checks.
//! - [`before`] and [`after`] register [`Hook`]s scoped by a tag expression.
//! - [`Attempt`] holds the result of an "attempts to" step.
//! - [`Given`] is a scenario-state slot whose unset read names the missing step.
//! - [`StepParam`] derives `FromStr` and `Display` over an enum's step words;
//!   an unknown word fails with a [`StepParamError`].
//!
//! # Running one scenario
//!
//! The generated tests call the runner with a [`ScenarioSpec`] and
//! [`Fixtures`]. A failure panics with a [`Failure`], whose `Display` names
//! the feature line, rule, row and step.
//!
//! [Gherkin style guide]: https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/gherkin_style_guide.md

mod attempt;
mod failure;
mod fixtures;
mod given;
mod hooks;
mod runner;
mod spec;
mod step_check;
mod step_param;
mod world;

pub use {
    self::{
        attempt::Attempt,
        failure::{Failure, Location},
        fixtures::Fixtures,
        given::Given,
        hooks::{Hook, HookFn, HookPhase},
        runner::Outcome,
        spec::{RowSpec, ScenarioSpec, StepSpec},
        step_param::StepParamError,
        world::World,
    },
    krabka_bdd_macros::{StepParam, after, before, given, scenarios, step, then, when},
};

/// What `scenarios!` expansions name. Not part of the API.
#[doc(hidden)]
pub mod __private {
    pub use {
        crate::{
            fixtures::Fixtures,
            hooks::{Hook, HookFn, HookPhase},
            runner::{run, run_outcome},
            spec::{RowSpec, ScenarioSpec, StepSpec},
            step_check::{RegisteredScenario, StepUse, check_registry, check_steps},
        },
        inventory,
        rstest_bdd::StepKeyword,
        tokio,
    };
}
