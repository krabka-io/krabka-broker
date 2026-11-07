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
//! # Data tables
//!
//! - [`TableRow`] derives a struct that one row of a step's data table fills;
//!   [`table::rows`] reads a horizontal table and [`table::vertical`] a
//!   `| field | value |` one. A table that does not fit fails with a
//!   [`TableError`] naming the row, the column and the value.
//! - [`assert_table!`] compares rows against an expected table on its
//!   columns, and reports every differing, missing and extra row at once.
//! - [`Overrides`] derives the Validation Tester's `apply(field, value)`;
//!   [`overrides::apply_table`] applies a table of overrides to a valid
//!   default instance.
//!
//! # Scenario state and layering
//!
//! - [`ScenarioContext`] derives `the_<field>()`, `the_<field>_mut()`,
//!   `set_<field>()` and `Default` for a context struct of [`Given`] slots,
//!   each named by `#[introduced_by("Given ...")]`. Every scenario gets a
//!   fresh one; scenario state never lives in a static. See [`context`].
//! - [`Ref`] is Introduce and Refer: "a topic" introduces an entity, and "the
//!   topic", "the topic orders" and "the oldest topic" find it again. A
//!   lookup that finds nothing fails with a [`RefError`] that names the fix.
//!   See [`refs`].
//! - [`Driver`] and [`Action`] are the Test Driver layer between a step and
//!   the system. [`act`] and [`attempt`] run an action in a traced
//!   [`ACTION_SPAN`] span. The [`driver`] module docs describe the three
//!   layers and the defaults convention: one `defaults` module per domain
//!   area, with a bon builder Default Instance such as `a_topic()` and named
//!   Object Mothers such as `a_compacted_topic()`.
//! - [`StubClock`] is a Stub Dependency for time over `qubit-clock`'s manual
//!   clocks, paired with a periodic real run under `@docker`. See [`clock`].
//!
//! # Running one scenario
//!
//! The generated tests call the runner with a [`ScenarioSpec`] and
//! [`Fixtures`]. A failure panics with a [`Failure`], whose `Display` names
//! the feature line, rule, row and step.
//!
//! [Gherkin style guide]: https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/gherkin_style_guide.md

mod attempt;
pub mod clock;
pub mod context;
pub mod driver;
mod failure;
mod fixtures;
mod given;
mod hooks;
pub mod overrides;
pub mod refs;
mod runner;
mod spec;
mod step_check;
mod step_param;
pub mod table;
mod world;

pub use {
    self::{
        attempt::Attempt,
        clock::StubClock,
        driver::{ACTION_SPAN, Action, Driver, act, attempt},
        failure::{Failure, Location},
        fixtures::Fixtures,
        given::Given,
        hooks::{Hook, HookFn, HookPhase},
        overrides::Overrides,
        refs::{Ref, RefError, Selector},
        runner::Outcome,
        spec::{RowSpec, ScenarioSpec, StepSpec},
        step_param::StepParamError,
        table::{TableError, TableRow},
        world::World,
    },
    krabka_bdd_macros::{
        Overrides, ScenarioContext, StepParam, TableRow, after, before, given, scenarios, step,
        then, when,
    },
};

/// What `scenarios!` expansions name. Not part of the API.
#[doc(hidden)]
pub mod __private {
    pub use {
        crate::{
            context::ContextSlot,
            fixtures::Fixtures,
            hooks::{Hook, HookFn, HookPhase},
            overrides::{field, optional_field, unknown_field},
            runner::{Flavor, run, run_outcome},
            spec::{RowSpec, ScenarioSpec, StepSpec},
            step_check::{RegisteredScenario, StepUse, check_registry, check_steps},
            table::{RowCells, assert_table, cell, optional_cell},
        },
        inventory,
        rstest_bdd::StepKeyword,
        tokio,
    };
}
