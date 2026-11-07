//! What a generated scenario test hands the runner.
//!
//! `krabka_bdd::scenarios!` emits one `static` [`ScenarioSpec`] per scenario
//! and per outline row, so every field is `&'static` data the macro can write
//! as a literal.

use rstest_bdd::StepKeyword;

/// One scenario, or one row of an outline, as the runner executes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScenarioSpec {
    /// The feature file, relative to the crate's manifest, with `/`.
    pub feature_path: &'static str,
    /// The `Feature:` name.
    pub feature: &'static str,
    /// The `Rule:` name, when the scenario sits under one.
    pub rule: Option<&'static str>,
    /// The `Scenario:` or `Scenario Outline:` name.
    pub scenario: &'static str,
    /// The line of the `Scenario:` keyword.
    pub line: u32,
    /// The Examples row, for an outline.
    pub row: Option<RowSpec>,
    /// Feature, rule, scenario and Examples tags, without `@`.
    pub tags: &'static [&'static str],
    /// Feature Background, then rule Background, then the scenario's steps,
    /// with outline placeholders substituted.
    pub steps: &'static [StepSpec],
    /// `@inprogress`: the test passes while the scenario fails, and fails
    /// once the scenario passes.
    pub expect_failure: bool,
}

/// One Examples row of an outline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowSpec {
    /// The row's line.
    pub line: u32,
    /// The row's `case` cell.
    pub case: &'static str,
    /// Every `(header, cell)` of the row, `case` included.
    pub values: &'static [(&'static str, &'static str)],
}

/// One step of a scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepSpec {
    /// The step's line.
    pub line: u32,
    /// The keyword the step resolves to: an `And` or `But` takes the keyword
    /// before it. A `*` step resolves to whichever keyword's definitions match.
    pub keyword: StepKeyword,
    /// The step is a `*` step.
    pub star: bool,
    /// The keyword as written, such as `And`.
    pub written: &'static str,
    /// The step text, placeholders substituted.
    pub text: &'static str,
    /// The doc string, if any.
    pub docstring: Option<&'static str>,
    /// The data table, if any, header row first.
    pub table: Option<&'static [&'static [&'static str]]>,
}
