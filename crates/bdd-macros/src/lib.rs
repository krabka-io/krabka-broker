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

mod hook_attr;
mod scenarios;
mod step_attr;
mod step_param;

use moxy::{
    ast::{ItemEnum, ParseError},
    token::TokenStream,
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
