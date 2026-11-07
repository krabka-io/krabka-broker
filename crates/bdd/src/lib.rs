//! Krabka's BDD layer over rstest-bdd.
//!
//! `krabka-bdd` runs Gherkin features through
//! [rstest-bdd](https://docs.rs/rstest-bdd) and holds them to the
//! [Gherkin style guide]. Step definitions stay `rstest_bdd_macros::given`,
//! `when` and `then`; this crate replaces the `scenarios!` call that binds a
//! feature directory to them, and adds the helpers those steps share.
//!
//! # `scenarios!`
//!
//! ```ignore
//! krabka_bdd::scenarios!("tests/features/log_policy", fixtures = [log_policy: LogPolicyScenario]);
//! ```
//!
//! The macro reads every `.feature` file under the directory, relative to the
//! crate's manifest, and lints it. A finding is a `compile_error!` that names
//! `file:line:col`, the rule id and the fix; the style guide lists the rule
//! ids. A clean directory expands to `rstest_bdd_macros::scenarios!` with the
//! same arguments, plus a module `<dir>_steps` holding one `#[test]`,
//! `every_step_matches_one_definition`. That test resolves every step of
//! every scenario the call runs, outline rows substituted, against the step
//! registry of the test binary, and fails listing each step that matches no
//! definition and each step that matches two or more, with the `file:line` of
//! every match. rstest-bdd itself picks the most specific match and breaks
//! ties arbitrarily.
//!
//! Arguments:
//!
//! - The directory, positional or as `dir = "..."` / `path = "..."`.
//! - `fixtures = [name: Type, ..]` and `tags = "..."`, which rstest-bdd takes
//!   as they are. `tags` also narrows the step check.
//! - `runtime = "tokio-multi-thread"`, optionally with `workers = N`, runs each
//!   scenario on [`TokioMultiThreadHarness`]. The calling crate must list
//!   `rstest-bdd-harness` as a dev-dependency, because rstest-bdd names the
//!   harness traits through it.
//! - `ignore_tags = "..."` replaces the scenarios the step check leaves out,
//!   by default `@formulated or @inprogress`.
//! - Every other argument is passed to rstest-bdd unchanged.
//!
//! # Step helpers
//!
//! - [`Given`] — a scenario-state slot whose unset read names the missing step.
//! - [`StepParam`] — `#[derive(StepParam)]` on a unit-variant enum generates
//!   `FromStr` and `Display` over its step words, so a placeholder parses into
//!   the domain type. A variant's word is its name in kebab-case, or the
//!   string literals of `#[step_param("word", "alias", ..)]`, the first of
//!   which `Display` writes. An unknown word fails with a [`StepParamError`]
//!   that lists every accepted word.
//! - [`TokioMultiThreadHarness`] and [`TokioMultiThreadContext`] — the
//!   multi-thread runtime behind `runtime = "tokio-multi-thread"`.
//!
//! [Gherkin style guide]: https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/gherkin_style_guide.md

mod attempt;
mod failure;
mod fixtures;
mod given;
mod harness;
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
        harness::{MultiThreadAttributePolicy, TokioMultiThreadContext, TokioMultiThreadHarness},
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
