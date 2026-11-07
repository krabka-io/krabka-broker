//! The tokens `scenarios!` emits for a set of planned features.

use {
    super::Arguments,
    krabka_bdd_model::{
        model::Keyword,
        plan::{Plan, PlannedRow, PlannedScenario, PlannedStep},
    },
    moxy::{
        ast::ParseError,
        token::{Ident, LitStr, Span, TokenStream},
    },
    std::collections::BTreeMap,
};

/// The `runtime = ".."` value that selects a current-thread runtime, the
/// default.
const CURRENT_THREAD: &str = "tokio-current-thread";
/// The `runtime = ".."` value that selects a multi-thread runtime.
const MULTI_THREAD: &str = "tokio-multi-thread";

/// The Tokio runtime each generated test runs on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Runtime {
    /// One thread: spawned tasks move only while a step awaits.
    #[default]
    CurrentThread,
    /// Worker threads, Tokio's default count unless `workers` is given:
    /// spawned tasks keep running between steps.
    MultiThread {
        /// `workers = N`.
        workers: Option<u128>,
    },
}

impl Runtime {
    /// The runtime that `runtime` and `workers` select.
    pub fn from_arguments(
        runtime: Option<&LitStr>,
        workers: Option<u128>,
    ) -> Result<Self, ParseError> {
        match runtime.map(LitStr::value) {
            None | Some(CURRENT_THREAD) if workers.is_none() => Ok(Self::CurrentThread),
            None | Some(CURRENT_THREAD) => Err(ParseError::new(
                runtime.map_or_else(Span::call_site, LitStr::span),
                format!("`workers` needs `runtime = \"{MULTI_THREAD}\"`"),
            )),
            Some(MULTI_THREAD) => Ok(Self::MultiThread { workers }),
            Some(other) => Err(ParseError::new(
                runtime.map_or_else(Span::call_site, LitStr::span),
                format!(
                    "unknown runtime `{other}`: expected `{CURRENT_THREAD}` or `{MULTI_THREAD}`"
                ),
            )),
        }
    }

    /// The `#[tokio::test(..)]` arguments after `crate = ..`.
    fn test_arguments(self) -> TokenStream {
        match self {
            Self::CurrentThread => moxy::template! { flavor = "current_thread" },
            Self::MultiThread { workers: None } => moxy::template! { flavor = "multi_thread" },
            Self::MultiThread {
                workers: Some(workers),
            } => {
                let workers = number(&workers);
                moxy::template! { flavor = "multi_thread", worker_threads = {{ workers }} }
            }
        }
    }
}

/// The whole expansion: a module named after the directory, holding one
/// module per feature file and the registry check.
pub fn expansion(
    arguments: &Arguments,
    base: &str,
    plans: &[Plan],
    tracked: &[String],
) -> TokenStream {
    let base = Ident::new(format!("{base}_scenarios"));
    let tracked: Vec<LitStr> = tracked.iter().map(|path| lit(path)).collect();
    let features: Vec<TokenStream> = plans
        .iter()
        .filter(|plan| !plan.scenarios.is_empty())
        .map(|plan| feature_module(arguments, plan))
        .collect();
    moxy::template! {
        @for path in &tracked {
            const _: &[u8] = ::core::include_bytes!({{ path }});
        }

        /// Scenarios generated from these feature files by `krabka_bdd::scenarios!`.
        mod {{ base }} {
            use super::*;

            @for feature in &features { {{ feature }} }

            /// Every step of every scenario in this test binary matches exactly
            /// one step definition.
            #[test]
            fn every_step_matches_one_definition() {
                ::krabka_bdd::__private::check_registry();
            }
        }
    }
}

/// The module of one feature file, with a module per rule and per outline.
fn feature_module(arguments: &Arguments, plan: &Plan) -> TokenStream {
    // Group by rule, then by outline, keeping file order.
    let mut rules: BTreeMap<usize, (Option<&str>, Vec<&PlannedScenario>)> = BTreeMap::new();
    let mut rule_order: Vec<Option<&str>> = Vec::new();
    for scenario in &plan.scenarios {
        let key = scenario.rule_module.as_deref();
        let index = rule_order
            .iter()
            .position(|existing| *existing == key)
            .unwrap_or_else(|| {
                rule_order.push(key);
                rule_order.len() - 1
            });
        rules
            .entry(index)
            .or_insert_with(|| (key, Vec::new()))
            .1
            .push(scenario);
    }
    let mut body = Vec::new();
    for (rule_module, scenarios) in rules.into_values() {
        let content = outline_modules(arguments, plan, &scenarios);
        body.push(match rule_module {
            Some(name) => wrap(&format!("rule_{name}"), &content),
            None => content,
        });
    }
    // The suffix keeps a feature named after a fixture from shadowing the
    // fixture's `name::default()` that rstest calls.
    wrap(
        &format!("{}_feature", plan.module),
        &moxy::template! { @for part in &body { {{ part }} } },
    )
}

/// The tests of one rule's scenarios, outline rows grouped in a module per
/// outline.
fn outline_modules(
    arguments: &Arguments,
    plan: &Plan,
    scenarios: &[&PlannedScenario],
) -> TokenStream {
    let mut parts = Vec::new();
    let mut index = 0;
    while let Some(scenario) = scenarios.get(index) {
        match &scenario.scenario_module {
            None => {
                parts.push(test_fn(arguments, plan, scenario));
                index += 1;
            }
            Some(module) => {
                let rows: Vec<TokenStream> = scenarios
                    .iter()
                    .skip(index)
                    .take_while(|row| row.scenario_module.as_ref() == Some(module))
                    .map(|row| test_fn(arguments, plan, row))
                    .collect();
                index += rows.len();
                parts.push(wrap(
                    module,
                    &moxy::template! { @for row in &rows { {{ row }} } },
                ));
            }
        }
    }
    moxy::template! { @for part in &parts { {{ part }} } }
}

fn wrap(name: &str, content: &TokenStream) -> TokenStream {
    let name = Ident::new(name);
    moxy::template! {
        mod {{ name }} {
            use super::*;
            {{ content }}
        }
    }
}

/// One generated test with its static spec.
fn test_fn(arguments: &Arguments, plan: &Plan, scenario: &PlannedScenario) -> TokenStream {
    let name = Ident::new(&scenario.test_name);
    let spec_name = Ident::new(format!(
        "__KRABKA_BDD_SPEC_{}",
        scenario.test_name.trim_start_matches('_').to_uppercase()
    ));
    let spec = spec(plan, scenario);
    let runtime = arguments.runtime.test_arguments();
    let ignore = ignore_reason(&scenario.tags).map(lit);
    let params: Vec<TokenStream> = arguments
        .fixtures
        .iter()
        .map(|fixture| {
            let name = Ident::new(&fixture.name);
            let ty = &fixture.ty;
            moxy::template! { {{ name }}: {{ ty }} }
        })
        .collect();
    let inserts: Vec<TokenStream> = arguments
        .fixtures
        .iter()
        .map(|fixture| {
            let ident = Ident::new(&fixture.name);
            let name = lit(&fixture.name);
            if arguments.world.as_deref() == Some(fixture.name.as_str()) {
                moxy::template! { .with_world({{ name }}, {{ ident }}) }
            } else {
                moxy::template! { .with({{ name }}, {{ ident }}) }
            }
        })
        .collect();
    let rstest = (!params.is_empty()).then(|| moxy::template! { #[::rstest::rstest] });
    moxy::template! {
        static {{ spec_name }}: ::krabka_bdd::__private::ScenarioSpec = {{ spec }};

        ::krabka_bdd::__private::inventory::submit! {
            ::krabka_bdd::__private::RegisteredScenario(&{{ spec_name }})
        }

        @if let Some(rstest) = &rstest { {{ rstest }} }
        #[::krabka_bdd::__private::tokio::test(crate = "::krabka_bdd::__private::tokio", {{ runtime }})]
        @if let Some(reason) = &ignore { #[ignore = {{ reason }}] }
        async fn {{ name }}(@for param in &params { {{ param }}, }) {
            ::krabka_bdd::__private::run(
                &{{ spec_name }},
                ::krabka_bdd::__private::Fixtures::new() @for insert in &inserts { {{ insert }} },
            )
            .await;
        }
    }
}

/// Why a scenario's test is `#[ignore]`d, if it is.
pub fn ignore_reason(tags: &[String]) -> Option<&'static str> {
    if tags.iter().any(|tag| tag == "formulated") {
        Some("formulated: automation not started")
    } else if tags.iter().any(|tag| tag == "manual") {
        Some("manual")
    } else {
        None
    }
}

/// The `ScenarioSpec` literal of one scenario.
fn spec(plan: &Plan, scenario: &PlannedScenario) -> TokenStream {
    let feature_path = lit(&plan.feature_path);
    let feature = lit(&plan.feature);
    let rule = optional(scenario.rule.as_deref());
    let name = lit(&scenario.scenario);
    let line = number(&scenario.line);
    let row = scenario.row.as_ref().map_or_else(
        || moxy::template! { ::core::option::Option::None },
        |row| {
            let row = row_spec(row);
            moxy::template! { ::core::option::Option::Some({{ row }}) }
        },
    );
    let tags: Vec<LitStr> = scenario.tags.iter().map(|tag| lit(tag)).collect();
    let steps: Vec<TokenStream> = scenario.steps.iter().map(step_spec).collect();
    let expect_failure = if scenario.tags.iter().any(|tag| tag == "inprogress") {
        moxy::template! { true }
    } else {
        moxy::template! { false }
    };
    moxy::template! {
        ::krabka_bdd::__private::ScenarioSpec {
            feature_path: {{ feature_path }},
            feature: {{ feature }},
            rule: {{ rule }},
            scenario: {{ name }},
            line: {{ line }},
            row: {{ row }},
            tags: &[@for tag in &tags { {{ tag }}, }],
            steps: &[@for step in &steps { {{ step }}, }],
            expect_failure: {{ expect_failure }},
        }
    }
}

fn row_spec(row: &PlannedRow) -> TokenStream {
    let line = number(&row.line);
    let case = lit(&row.case);
    let values: Vec<(LitStr, LitStr)> = row
        .values
        .iter()
        .map(|(header, cell)| (lit(header), lit(cell)))
        .collect();
    moxy::template! {
        ::krabka_bdd::__private::RowSpec {
            line: {{ line }},
            case: {{ case }},
            values: &[@for (header, cell) in &values { ({{ header }}, {{ cell }}), }],
        }
    }
}

fn step_spec(step: &PlannedStep) -> TokenStream {
    let line = number(&step.line);
    let keyword = Ident::new(match step.keyword {
        Keyword::Given => "Given",
        Keyword::When => "When",
        Keyword::Then => "Then",
    });
    let star = if step.star {
        moxy::template! { true }
    } else {
        moxy::template! { false }
    };
    let written = lit(&step.written);
    let text = lit(&step.text);
    let docstring = optional(step.docstring.as_deref());
    let table = step.table.as_ref().map_or_else(
        || moxy::template! { ::core::option::Option::None },
        |rows| {
            let rows: Vec<Vec<LitStr>> = rows
                .iter()
                .map(|row| row.iter().map(|cell| lit(cell)).collect())
                .collect();
            moxy::template! {
                ::core::option::Option::Some(&[
                    @for row in &rows { &[@for cell in row { {{ cell }}, }], }
                ])
            }
        },
    );
    moxy::template! {
        ::krabka_bdd::__private::StepSpec {
            line: {{ line }},
            keyword: ::krabka_bdd::__private::StepKeyword::{{ keyword }},
            star: {{ star }},
            written: {{ written }},
            text: {{ text }},
            docstring: {{ docstring }},
            table: {{ table }},
        }
    }
}

fn lit(text: &str) -> LitStr { LitStr::new(text, Span::call_site()) }

fn optional(text: Option<&str>) -> TokenStream {
    text.map_or_else(
        || moxy::template! { ::core::option::Option::None },
        |text| {
            let text = lit(text);
            moxy::template! { ::core::option::Option::Some({{ text }}) }
        },
    )
}

/// An unsuffixed integer literal.
fn number(value: &impl ToString) -> TokenStream { value.to_string().parse().unwrap_or_default() }

#[cfg(test)]
mod tests {
    use {
        super::{Runtime, ignore_reason},
        assert2::assert,
        moxy::token::{LitStr, Span},
        rstest::rstest,
    };

    #[rstest]
    #[case(&["docker"], None)]
    #[case(&["formulated"], Some("formulated: automation not started"))]
    #[case(&["manual", "docker"], Some("manual"))]
    #[case(&["inprogress"], None)]
    fn lifecycle_tags_choose_the_ignore_reason(
        #[case] tags: &[&str],
        #[case] expected: Option<&str>,
    ) {
        let tags: Vec<String> = tags.iter().map(|tag| (*tag).to_owned()).collect();
        assert!(ignore_reason(&tags) == expected);
    }

    #[rstest]
    #[case(None, None, Some(Runtime::CurrentThread))]
    #[case(Some("tokio-current-thread"), None, Some(Runtime::CurrentThread))]
    #[case(Some("tokio-multi-thread"), None, Some(Runtime::MultiThread { workers: None }))]
    #[case(Some("tokio-multi-thread"), Some(4), Some(Runtime::MultiThread { workers: Some(4) }))]
    #[case(None, Some(4), None)]
    #[case(Some("async-std"), None, None)]
    fn runtime_arguments_select_the_runtime(
        #[case] runtime: Option<&str>,
        #[case] workers: Option<u128>,
        #[case] expected: Option<Runtime>,
    ) {
        let runtime = runtime.map(|text| LitStr::new(text, Span::call_site()));
        assert!(Runtime::from_arguments(runtime.as_ref(), workers).ok() == expected);
    }
}
