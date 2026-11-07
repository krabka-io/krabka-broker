//! Proc macros behind [`krabka-bdd`](../krabka_bdd/index.html).
//!
//! Use them through `krabka-bdd`, which re-exports them next to the runtime
//! types their expansions name. The crate has two macros:
//!
//! - `scenarios!` lints every `.feature` file under a directory against the
//!   [Gherkin style guide], then expands to `rstest_bdd_macros::scenarios!`
//!   with Krabka's defaults and a step-registry check.
//! - `#[derive(StepParam)]` gives a unit-variant enum the `FromStr` and
//!   `Display` that a step placeholder parses with.
//!
//! The lint and the feature model are plain functions in private modules, so
//! their unit tests call them without a compiler in the loop.
//!
//! [Gherkin style guide]: https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/gherkin_style_guide.md

mod context;
mod hook_attr;
mod overrides;
mod scenarios;
mod step_attr;
mod step_param;
mod table_row;

use {
    hook_attr::HookPhase,
    moxy::{
        ast::{ItemEnum, ParseError},
        token::TokenStream,
    },
    proc_macro::TokenStream as NativeTokenStream,
    step_attr::StepAttribute,
};

/// Lints the features under a directory, then runs them with rstest-bdd.
///
/// `krabka-bdd` documents the arguments.
#[moxy::function(name = "scenarios")]
pub fn scenarios_macro(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    scenarios::expand(tokens)
}

/// Derives `FromStr` and `Display` over a step's words for a unit-variant enum.
///
/// `krabka-bdd` documents the `#[step_param(...)]` attribute.
#[moxy::derive(StepParam, attributes(step_param))]
pub fn step_param_derive(item: ItemEnum) -> Result<TokenStream, ParseError> {
    step_param::expand(item)
}

/// Defines a `Given` step from one or more Cucumber expressions.
///
/// `krabka-bdd` documents the expressions and the checks.
#[moxy::attribute(name = "given")]
pub fn given_attribute(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    step_attr::expand_native(StepAttribute::Given, &meta.into(), item)
}

/// Defines a `When` step from one or more Cucumber expressions.
///
/// `krabka-bdd` documents the expressions and the checks.
#[moxy::attribute(name = "when")]
pub fn when_attribute(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    step_attr::expand_native(StepAttribute::When, &meta.into(), item)
}

/// Defines a `Then` step from one or more Cucumber expressions.
///
/// `krabka-bdd` documents the expressions and the checks.
#[moxy::attribute(name = "then")]
pub fn then_attribute(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    step_attr::expand_native(StepAttribute::Then, &meta.into(), item)
}

/// Defines a step under all three keywords from one or more Cucumber
/// expressions.
///
/// `krabka-bdd` documents the expressions and the checks.
#[moxy::attribute(name = "step")]
pub fn step_attribute(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    step_attr::expand_native(StepAttribute::Step, &meta.into(), item)
}

/// Registers an `async fn(&ScenarioSpec)` to run before each scenario.
///
/// `krabka-bdd` documents `tags` and `order`.
#[moxy::attribute(name = "before")]
pub fn before_attribute(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    hook_attr::expand_native(HookPhase::Before, &meta.into(), item)
}

/// Registers an `async fn(&ScenarioSpec, &Outcome)` to run after each
/// scenario, whatever its outcome.
///
/// `krabka-bdd` documents `tags` and `order`.
#[moxy::attribute(name = "after")]
pub fn after_attribute(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    hook_attr::expand_native(HookPhase::After, &meta.into(), item)
}
