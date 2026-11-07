//! Step resolution, and the checks that every feature step matches exactly one
//! step definition.
//!
//! rstest-bdd picks the most specific of several matching definitions and
//! breaks ties arbitrarily. The runner instead resolves each step strictly with
//! [`resolve`]: a step that no definition matches is undefined, and a step that
//! two or more definitions match at the top specificity is ambiguous.
//!
//! [`check_registry`] applies the same resolution to every scenario the test
//! binary registers, so every undefined or ambiguous step fails one test. It
//! also checks `@journey` scenarios and prints an advisory listing of unused
//! step definitions. [`check_steps`] is the older check over a list of step
//! uses.

use {
    crate::spec::ScenarioSpec,
    rstest_bdd::{Step, StepKeyword, StepPattern, extract_placeholders, iter},
    std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Write,
    },
};

/// One step as a feature file uses it.
///
/// `scenarios!` builds these as struct literals, so the field names and types
/// are part of the macro's contract.
#[derive(Debug, Clone, Copy)]
pub struct StepUse {
    /// Where the step is written, as `path:line`, such as
    /// `tests/features/cleanup.feature:12`.
    pub location: &'static str,
    /// The step's keyword, with `And` and `But` already resolved to the
    /// keyword they continue.
    pub keyword: StepKeyword,
    /// The step text, with outline placeholders already substituted.
    pub text: &'static str,
}

/// One step definition, as the checks and the runner see it.
#[derive(Debug, Clone, Copy)]
pub struct Definition<'a> {
    /// The keyword the definition is registered under.
    pub keyword: StepKeyword,
    /// The definition's pattern.
    pub pattern: &'a StepPattern,
    /// The source file of the definition.
    pub file: &'a str,
    /// The line of the definition in `file`.
    pub line: u32,
}

impl Definition<'_> {
    /// The definition's identity in sets: keyword, pattern and location.
    fn key(&self) -> (&str, u32, &'static str, &str) {
        (
            self.file,
            self.line,
            self.keyword.as_str(),
            self.pattern.as_str(),
        )
    }
}

/// Every step definition registered in the test binary.
#[must_use]
pub fn registered_definitions() -> Vec<Definition<'static>> {
    iter::<Step>
        .into_iter()
        .map(|step| Definition {
            keyword: step.keyword,
            pattern: step.pattern,
            file: step.file,
            line: step.line,
        })
        .collect()
}

/// The keywords a `*` step tries.
const STAR_KEYWORDS: [StepKeyword; 3] = [StepKeyword::Given, StepKeyword::When, StepKeyword::Then];

/// How one step resolves against the step definitions.
#[derive(Debug)]
pub enum Resolution<'d, 'a> {
    /// Exactly one definition matches at the top specificity.
    Found(&'d Definition<'a>),
    /// No definition matches.
    Undefined,
    /// Two or more definitions match at the top specificity, sorted by
    /// `file:line`.
    Ambiguous(Vec<&'d Definition<'a>>),
}

impl Resolution<'_, '_> {
    /// Describes an undefined or ambiguous resolution; `None` when found.
    ///
    /// `keyword` and `star` are the step's, as in [`resolve`].
    #[must_use]
    pub fn problem(&self, keyword: StepKeyword, star: bool, text: &str) -> Option<String> {
        match self {
            Self::Found(_) => None,
            Self::Undefined if star => Some(format!(
                "undefined: no Given, When or Then step definition matches `{text}`"
            )),
            Self::Undefined => Some(format!(
                "undefined: no {} step definition matches `{text}`",
                keyword.as_str()
            )),
            Self::Ambiguous(matches) => {
                let mut message = format!(
                    "ambiguous: {} step definitions match `{text}` equally well:",
                    matches.len()
                );
                write_definitions(&mut message, matches);
                Some(message)
            }
        }
    }
}

fn write_definitions(out: &mut String, definitions: &[&Definition<'_>]) {
    for definition in definitions {
        let _ = write!(
            out,
            "\n    {}:{} `{}`",
            definition.file,
            definition.line,
            definition.pattern.as_str()
        );
    }
}

/// Resolves one step strictly against `definitions`.
///
/// A candidate has the step's keyword, or any of Given, When and Then for a
/// `*` step, and a pattern that matches `text`. Of the candidates, those with
/// the highest pattern specificity win; more than one winner is ambiguous.
#[must_use]
pub fn resolve<'d, 'a>(
    definitions: &'d [Definition<'a>],
    keyword: StepKeyword,
    star: bool,
    text: &str,
) -> Resolution<'d, 'a> {
    let candidates: Vec<&Definition<'a>> = definitions
        .iter()
        .filter(|definition| {
            let keyword_fits = if star {
                STAR_KEYWORDS.contains(&definition.keyword)
            } else {
                definition.keyword == keyword
            };
            keyword_fits && extract_placeholders(definition.pattern, text.into()).is_ok()
        })
        .collect();
    let Some(top) = candidates
        .iter()
        .map(|definition| definition.pattern.specificity().ok())
        .max()
    else {
        return Resolution::Undefined;
    };
    let mut winners: Vec<&Definition<'a>> = candidates
        .into_iter()
        .filter(|definition| definition.pattern.specificity().ok() == top)
        .collect();
    if let [only] = winners.as_slice() {
        return Resolution::Found(only);
    }
    winners.sort_by(|left, right| (left.file, left.line).cmp(&(right.file, right.line)));
    Resolution::Ambiguous(winners)
}

/// Checks that every step in `uses` matches exactly one registered step
/// definition.
///
/// Unlike the runner, this check counts every matching definition, not only
/// those at the top specificity.
///
/// # Panics
///
/// Panics with a report listing every step that no definition matches and
/// every step that more than one definition matches.
#[track_caller]
pub fn check_steps(uses: &[StepUse]) {
    if let Some(report) = report(uses, &registered_definitions()) {
        panic!("{report}");
    }
}

/// Builds the report of undefined and ambiguous steps, or `None` when every
/// step matches exactly one definition.
fn report(uses: &[StepUse], definitions: &[Definition<'_>]) -> Option<String> {
    let mut undefined = Vec::new();
    let mut ambiguous = Vec::new();
    for step in uses {
        let mut matches: Vec<&Definition<'_>> = definitions
            .iter()
            .filter(|definition| matches(definition, step))
            .collect();
        match matches.len() {
            0 => undefined.push(step),
            1 => {}
            _ => {
                matches
                    .sort_by(|left, right| (left.file, left.line).cmp(&(right.file, right.line)));
                ambiguous.push((step, matches));
            }
        }
    }
    if undefined.is_empty() && ambiguous.is_empty() {
        return None;
    }
    let mut report = format!(
        "{} undefined and {} ambiguous step(s); every step needs exactly one definition:",
        undefined.len(),
        ambiguous.len()
    );
    for step in undefined {
        let _ = write!(
            report,
            "\nundefined: {} {} ({})",
            step.keyword.as_str(),
            step.text,
            step.location
        );
    }
    for (step, matches) in ambiguous {
        let _ = write!(
            report,
            "\nambiguous: {} {} ({}) matches {} definitions:",
            step.keyword.as_str(),
            step.text,
            step.location,
            matches.len()
        );
        write_definitions(&mut report, &matches);
    }
    Some(report)
}

fn matches(definition: &Definition<'_>, step: &StepUse) -> bool {
    definition.keyword == step.keyword
        && extract_placeholders(definition.pattern, step.text.into()).is_ok()
}

/// A scenario that generated code registers for the whole-binary checks.
///
/// `scenarios!` submits one per scenario and outline row with
/// `inventory::submit!`.
#[derive(Debug, Clone, Copy)]
pub struct RegisteredScenario(pub &'static ScenarioSpec);

inventory::collect!(RegisteredScenario);

/// Checks every scenario the test binary registers.
///
/// It fails on:
///
/// - an undefined step, in a scenario that runs and is not `@inprogress`;
/// - an ambiguous step, in a scenario that runs;
/// - a step of a `@journey` scenario whose definition no scenario without
///   `@journey` uses.
///
/// A scenario runs unless it is tagged `@formulated` or `@manual`. It also
/// prints, on stderr, the step definitions that no scenario uses and the
/// step-definition files each feature uses.
///
/// # Panics
///
/// Panics with a report listing every problem.
#[track_caller]
pub fn check_registry() {
    let scenarios: Vec<&ScenarioSpec> = iter::<RegisteredScenario>
        .into_iter()
        .map(|registered| registered.0)
        .collect();
    let report = registry_report(&scenarios, &registered_definitions());
    if !report.advisory.is_empty() {
        eprintln!("{}", report.advisory);
    }
    if let Some(errors) = report.errors {
        panic!("{errors}");
    }
}

/// What [`check_registry`] reports.
#[derive(Debug, PartialEq, Eq)]
struct RegistryReport {
    /// The failing problems, or `None`.
    errors: Option<String>,
    /// The advisory listing; empty when there is nothing to list.
    advisory: String,
}

/// Whether a scenario runs at all.
fn runs(scenario: &ScenarioSpec) -> bool {
    !scenario
        .tags
        .iter()
        .any(|tag| *tag == "formulated" || *tag == "manual")
}

fn is_journey(scenario: &ScenarioSpec) -> bool { scenario.tags.contains(&"journey") }

/// The definition identity used in sets.
type DefinitionKey<'a> = (&'a str, u32, &'static str, &'a str);

/// One step of one scenario, resolved.
struct ResolvedStep<'s, 'd, 'a> {
    scenario: &'s ScenarioSpec,
    line: u32,
    keyword: StepKeyword,
    text: &'s str,
    resolution: Resolution<'d, 'a>,
}

fn registry_report(scenarios: &[&ScenarioSpec], definitions: &[Definition<'_>]) -> RegistryReport {
    let mut ordered: Vec<&ScenarioSpec> = scenarios.iter().copied().filter(|s| runs(s)).collect();
    ordered.sort_by_key(|scenario| {
        (
            scenario.feature_path,
            scenario.line,
            scenario.row.map(|row| row.line),
        )
    });
    let resolved: Vec<ResolvedStep<'_, '_, '_>> = ordered
        .iter()
        .flat_map(|scenario| {
            scenario.steps.iter().map(|step| ResolvedStep {
                scenario,
                line: step.line,
                keyword: step.keyword,
                text: step.text,
                resolution: resolve(definitions, step.keyword, step.star, step.text),
            })
        })
        .collect();

    let mut used: BTreeSet<DefinitionKey<'_>> = BTreeSet::new();
    let mut used_outside_journeys: BTreeSet<DefinitionKey<'_>> = BTreeSet::new();
    let mut files_by_feature: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for step in &resolved {
        if let Resolution::Found(definition) = step.resolution {
            used.insert(definition.key());
            if !is_journey(step.scenario) {
                used_outside_journeys.insert(definition.key());
            }
            files_by_feature
                .entry(step.scenario.feature_path)
                .or_default()
                .insert(definition.file);
        }
    }

    let mut seen = BTreeSet::new();
    let mut undefined = Vec::new();
    let mut ambiguous = Vec::new();
    let mut journey_only = Vec::new();
    for step in &resolved {
        let place = (step.scenario.feature_path, step.line, step.text);
        if !seen.insert(place) {
            continue;
        }
        let location = format!(
            "{} {} ({}:{})",
            step.keyword.as_str(),
            step.text,
            step.scenario.feature_path,
            step.line
        );
        match &step.resolution {
            Resolution::Undefined if !step.scenario.expect_failure => {
                undefined.push(format!("\nundefined: {location}"));
            }
            Resolution::Undefined => {}
            Resolution::Ambiguous(matches) => {
                let mut entry = format!(
                    "\nambiguous: {location} matches {} definitions equally well:",
                    matches.len()
                );
                write_definitions(&mut entry, matches);
                ambiguous.push(entry);
            }
            Resolution::Found(definition) => {
                if is_journey(step.scenario) && !used_outside_journeys.contains(&definition.key()) {
                    journey_only.push(format!(
                        "\njourney-only: {location} in @journey Scenario: {} uses {}:{} `{}`, \
                         which no scenario without @journey uses",
                        step.scenario.scenario,
                        definition.file,
                        definition.line,
                        definition.pattern.as_str()
                    ));
                }
            }
        }
    }

    let errors =
        (!(undefined.is_empty() && ambiguous.is_empty() && journey_only.is_empty())).then(|| {
            let mut errors = format!(
                "{} undefined, {} ambiguous and {} journey-only step(s) across {} scenario(s):",
                undefined.len(),
                ambiguous.len(),
                journey_only.len(),
                ordered.len()
            );
            for entry in undefined.iter().chain(&ambiguous).chain(&journey_only) {
                errors.push_str(entry);
            }
            errors
        });

    RegistryReport {
        errors,
        advisory: advisory(definitions, &used, &files_by_feature),
    }
}

fn advisory(
    definitions: &[Definition<'_>],
    used: &BTreeSet<DefinitionKey<'_>>,
    files_by_feature: &BTreeMap<&str, BTreeSet<&str>>,
) -> String {
    let mut unused: Vec<&Definition<'_>> = definitions
        .iter()
        .filter(|definition| !used.contains(&definition.key()))
        .collect();
    unused.sort_by_key(|definition| definition.key());
    let mut out = String::new();
    if !unused.is_empty() {
        let _ = write!(
            out,
            "advisory: {} step definition(s) that no scenario uses:",
            unused.len()
        );
        for definition in unused {
            let _ = write!(
                out,
                "\n    {}:{} {} `{}`",
                definition.file,
                definition.line,
                definition.keyword.as_str(),
                definition.pattern.as_str()
            );
        }
    }
    if !files_by_feature.is_empty() {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("advisory: the step-definition files each feature uses:");
        for (feature, files) in files_by_feature {
            let files: Vec<&str> = files.iter().copied().collect();
            let _ = write!(out, "\n    {feature}: {}", files.join(", "));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use {
        super::*, assert2::assert, rstest::rstest, rstest_bdd_macros::given,
        std::panic::catch_unwind,
    };

    static SEGMENTS_ANY: StepPattern = StepPattern::new("a log has {n} segments");
    static SEGMENTS_THREE: StepPattern = StepPattern::new("a log has 3 segments");
    static POLICY: StepPattern = StepPattern::new("a log has cleanup policy {policy}");
    static WRITE: StepPattern = StepPattern::new("the cleaner runs");

    fn definitions() -> Vec<Definition<'static>> {
        vec![
            Definition {
                keyword: StepKeyword::Given,
                pattern: &SEGMENTS_THREE,
                file: "src/other.rs",
                line: 44,
            },
            Definition {
                keyword: StepKeyword::Given,
                pattern: &SEGMENTS_ANY,
                file: "src/config.rs",
                line: 120,
            },
            Definition {
                keyword: StepKeyword::Given,
                pattern: &POLICY,
                file: "src/config.rs",
                line: 10,
            },
            Definition {
                keyword: StepKeyword::When,
                pattern: &WRITE,
                file: "src/cleaner.rs",
                line: 5,
            },
        ]
    }

    const fn given(location: &'static str, text: &'static str) -> StepUse {
        StepUse {
            location,
            keyword: StepKeyword::Given,
            text,
        }
    }

    const fn then(location: &'static str, text: &'static str) -> StepUse {
        StepUse {
            location,
            keyword: StepKeyword::Then,
            text,
        }
    }

    #[rstest]
    #[case::no_steps(&[], None)]
    #[case::every_step_defined_once(
        &[given("f.feature:3", "a log has cleanup policy delete"), given("f.feature:4", "a log has 7 segments")],
        None,
    )]
    #[case::undefined(
        &[given("f.feature:7", "a log is empty")],
        Some("1 undefined and 0 ambiguous step(s); every step needs exactly one definition:\n\
              undefined: Given a log is empty (f.feature:7)"),
    )]
    #[case::keyword_mismatch_is_undefined(
        &[then("f.feature:9", "the cleaner runs")],
        Some("1 undefined and 0 ambiguous step(s); every step needs exactly one definition:\n\
              undefined: Then the cleaner runs (f.feature:9)"),
    )]
    #[case::ambiguous(
        &[given("f.feature:2", "a log has 3 segments")],
        Some("0 undefined and 1 ambiguous step(s); every step needs exactly one definition:\n\
              ambiguous: Given a log has 3 segments (f.feature:2) matches 2 definitions:\n    \
              src/config.rs:120 `a log has {n} segments`\n    \
              src/other.rs:44 `a log has 3 segments`"),
    )]
    #[case::undefined_listed_before_ambiguous_in_input_order(
        &[
            given("f.feature:2", "a log has 3 segments"),
            then("f.feature:5", "the log is empty"),
            given("f.feature:6", "a log has cleanup policy compact"),
            given("f.feature:8", "a topic exists"),
        ],
        Some("2 undefined and 1 ambiguous step(s); every step needs exactly one definition:\n\
              undefined: Then the log is empty (f.feature:5)\n\
              undefined: Given a topic exists (f.feature:8)\n\
              ambiguous: Given a log has 3 segments (f.feature:2) matches 2 definitions:\n    \
              src/config.rs:120 `a log has {n} segments`\n    \
              src/other.rs:44 `a log has 3 segments`"),
    )]
    fn report_lists_every_problem(#[case] uses: &[StepUse], #[case] expected: Option<&str>) {
        assert!(report(uses, &definitions()).as_deref() == expected);
    }

    #[given("a step-check log has {count} replicas")]
    fn step_check_any_replicas(count: u32) { let _ = count; }

    #[given("a step-check log has 2 replicas")]
    fn step_check_two_replicas() {}

    #[given("a step-check log is unique")]
    fn step_check_unique() {}

    fn registered(pattern: &str) -> String {
        let step = iter::<Step>
            .into_iter()
            .find(|step| step.pattern.as_str() == pattern)
            .expect("the step is registered");
        assert!(step.file == file!());
        format!("{}:{} `{pattern}`", step.file, step.line)
    }

    #[test]
    fn check_steps_passes_a_uniquely_matched_registered_step() {
        check_steps(&[given("f.feature:1", "a step-check log is unique")]);
    }

    #[test]
    fn check_steps_reports_overlapping_registered_steps() {
        let payload = catch_unwind(|| {
            check_steps(&[given("f.feature:1", "a step-check log has 2 replicas")]);
        })
        .expect_err("overlapping definitions fail the check");
        let message = payload
            .downcast::<String>()
            .expect("a formatted panic message");
        let any = registered("a step-check log has {count} replicas");
        let two = registered("a step-check log has 2 replicas");
        assert!(message.contains("ambiguous: Given a step-check log has 2 replicas (f.feature:1)"));
        assert!(message.contains(&any));
        assert!(message.contains(&two));
    }
}

#[cfg(test)]
mod resolution_tests {
    use {
        super::*,
        crate::spec::{RowSpec, StepSpec},
        assert2::assert,
        rstest::rstest,
    };

    static SEGMENTS_ANY: StepPattern = StepPattern::new("a log has {n} segments");
    static SEGMENTS_THREE: StepPattern = StepPattern::new("a log has 3 segments");
    static FLIPS_SIDE: StepPattern = StepPattern::new("the runner flips {side} coins");
    static FLIPS_KIND: StepPattern = StepPattern::new("the runner flips heads {kind}");
    static CLEANER: StepPattern = StepPattern::new("the cleaner runs");
    static EMPTY: StepPattern = StepPattern::new("the log is empty");

    const fn definition(
        keyword: StepKeyword,
        pattern: &'static StepPattern,
        file: &'static str,
        line: u32,
    ) -> Definition<'static> {
        Definition {
            keyword,
            pattern,
            file,
            line,
        }
    }

    static DEFINITIONS: [Definition<'static>; 7] = [
        definition(StepKeyword::Given, &SEGMENTS_ANY, "config.rs", 120),
        definition(StepKeyword::Given, &SEGMENTS_THREE, "other.rs", 44),
        definition(StepKeyword::Given, &FLIPS_KIND, "b.rs", 2),
        definition(StepKeyword::Given, &FLIPS_SIDE, "a.rs", 1),
        definition(StepKeyword::When, &CLEANER, "cleaner.rs", 5),
        definition(StepKeyword::Then, &EMPTY, "t.rs", 3),
        definition(StepKeyword::When, &EMPTY, "w.rs", 9),
    ];

    fn summary(resolution: &Resolution<'_, '_>) -> String {
        let locations = |definitions: &[&Definition<'_>]| {
            definitions
                .iter()
                .map(|definition| format!("{}:{}", definition.file, definition.line))
                .collect::<Vec<_>>()
                .join(", ")
        };
        match resolution {
            Resolution::Found(definition) => format!("found {}", locations(&[definition])),
            Resolution::Undefined => "undefined".to_owned(),
            Resolution::Ambiguous(matches) => format!("ambiguous {}", locations(matches)),
        }
    }

    #[rstest]
    #[case::most_specific_wins(
        StepKeyword::Given,
        false,
        "a log has 3 segments",
        "found other.rs:44"
    )]
    #[case::only_generic_matches(
        StepKeyword::Given,
        false,
        "a log has 7 segments",
        "found config.rs:120"
    )]
    #[case::tie_at_top_specificity(
        StepKeyword::Given,
        false,
        "the runner flips heads coins",
        "ambiguous a.rs:1, b.rs:2"
    )]
    #[case::undefined(StepKeyword::Given, false, "a topic exists", "undefined")]
    #[case::keyword_mismatch(StepKeyword::Then, false, "the cleaner runs", "undefined")]
    #[case::star_finds_a_when(StepKeyword::Given, true, "the cleaner runs", "found cleaner.rs:5")]
    #[case::star_tries_every_keyword(
        StepKeyword::Given,
        true,
        "the log is empty",
        "ambiguous t.rs:3, w.rs:9"
    )]
    #[case::keyword_narrows_without_star(
        StepKeyword::Then,
        false,
        "the log is empty",
        "found t.rs:3"
    )]
    fn resolve_is_strict(
        #[case] keyword: StepKeyword,
        #[case] star: bool,
        #[case] text: &str,
        #[case] expected: &str,
    ) {
        assert!(summary(&resolve(&DEFINITIONS, keyword, star, text)) == expected);
    }

    #[rstest]
    #[case::found(StepKeyword::When, false, "the cleaner runs", None)]
    #[case::undefined(
        StepKeyword::When,
        false,
        "nothing",
        Some("undefined: no When step definition matches `nothing`")
    )]
    #[case::undefined_star(
        StepKeyword::When,
        true,
        "nothing",
        Some("undefined: no Given, When or Then step definition matches `nothing`")
    )]
    #[case::ambiguous(
        StepKeyword::Given,
        false,
        "the runner flips heads coins",
        Some(
            "ambiguous: 2 step definitions match `the runner flips heads coins` equally well:\n    \
             a.rs:1 `the runner flips {side} coins`\n    \
             b.rs:2 `the runner flips heads {kind}`"
        )
    )]
    fn problem_describes_the_resolution(
        #[case] keyword: StepKeyword,
        #[case] star: bool,
        #[case] text: &str,
        #[case] expected: Option<&str>,
    ) {
        let problem = resolve(&DEFINITIONS, keyword, star, text).problem(keyword, star, text);
        assert!(problem.as_deref() == expected);
    }

    const fn step(line: u32, keyword: StepKeyword, text: &'static str) -> StepSpec {
        StepSpec {
            line,
            keyword,
            star: false,
            written: keyword.as_str(),
            text,
            docstring: None,
            table: None,
        }
    }

    const fn scenario(
        feature_path: &'static str,
        scenario: &'static str,
        line: u32,
        row: Option<u32>,
        tags: &'static [&'static str],
        steps: &'static [StepSpec],
    ) -> ScenarioSpec {
        ScenarioSpec {
            feature_path,
            feature: "F",
            rule: None,
            scenario,
            line,
            row: match row {
                Some(line) => Some(RowSpec {
                    line,
                    case: "c",
                    values: &[],
                }),
                None => None,
            },
            tags,
            steps,
            expect_failure: false,
        }
    }

    static PLAIN_STEPS: [StepSpec; 2] = [
        step(4, StepKeyword::Given, "a log has 3 segments"),
        step(5, StepKeyword::When, "the cleaner runs"),
    ];
    static JOURNEY_STEPS: [StepSpec; 2] = [
        step(11, StepKeyword::Given, "a log has 7 segments"),
        step(12, StepKeyword::When, "the cleaner runs"),
    ];
    static BROKEN_STEPS: [StepSpec; 2] = [
        step(3, StepKeyword::Given, "a topic exists"),
        step(4, StepKeyword::Given, "the runner flips heads coins"),
    ];
    static UNWRITTEN_STEPS: [StepSpec; 1] = [step(21, StepKeyword::Given, "an unwritten step")];

    static PLAIN: ScenarioSpec = scenario("f.feature", "Plain", 3, None, &[], &PLAIN_STEPS);
    static JOURNEY: ScenarioSpec = scenario(
        "f.feature",
        "Journey",
        10,
        None,
        &["journey"],
        &JOURNEY_STEPS,
    );
    static IN_PROGRESS: ScenarioSpec = ScenarioSpec {
        expect_failure: true,
        ..scenario("f.feature", "In progress", 20, None, &[], &UNWRITTEN_STEPS)
    };
    static FORMULATED: ScenarioSpec = scenario(
        "f.feature",
        "Formulated",
        30,
        None,
        &["formulated"],
        &UNWRITTEN_STEPS,
    );
    static ROW_ONE: ScenarioSpec = scenario("g.feature", "Outline", 2, Some(8), &[], &BROKEN_STEPS);
    static ROW_TWO: ScenarioSpec = scenario("g.feature", "Outline", 2, Some(9), &[], &BROKEN_STEPS);

    #[rstest]
    #[case::clean(
        &[&PLAIN],
        RegistryReport {
            errors: None,
            advisory: "advisory: 5 step definition(s) that no scenario uses:\n    \
                       a.rs:1 Given `the runner flips {side} coins`\n    \
                       b.rs:2 Given `the runner flips heads {kind}`\n    \
                       config.rs:120 Given `a log has {n} segments`\n    \
                       t.rs:3 Then `the log is empty`\n    \
                       w.rs:9 When `the log is empty`\n\
                       advisory: the step-definition files each feature uses:\n    \
                       f.feature: cleaner.rs, other.rs"
                .to_owned(),
        },
    )]
    #[case::every_problem_once(
        &[&ROW_TWO, &JOURNEY, &FORMULATED, &PLAIN, &ROW_ONE, &IN_PROGRESS],
        RegistryReport {
            errors: Some(
                "1 undefined, 1 ambiguous and 1 journey-only step(s) across 5 scenario(s):\n\
                 undefined: Given a topic exists (g.feature:3)\n\
                 ambiguous: Given the runner flips heads coins (g.feature:4) matches 2 definitions equally well:\n    \
                 a.rs:1 `the runner flips {side} coins`\n    \
                 b.rs:2 `the runner flips heads {kind}`\n\
                 journey-only: Given a log has 7 segments (f.feature:11) in @journey Scenario: Journey \
                 uses config.rs:120 `a log has {n} segments`, which no scenario without @journey uses"
                    .to_owned(),
            ),
            advisory: "advisory: 4 step definition(s) that no scenario uses:\n    \
                       a.rs:1 Given `the runner flips {side} coins`\n    \
                       b.rs:2 Given `the runner flips heads {kind}`\n    \
                       t.rs:3 Then `the log is empty`\n    \
                       w.rs:9 When `the log is empty`\n\
                       advisory: the step-definition files each feature uses:\n    \
                       f.feature: cleaner.rs, config.rs, other.rs"
                .to_owned(),
        },
    )]
    fn registry_report_checks_every_scenario(
        #[case] scenarios: &[&ScenarioSpec],
        #[case] expected: RegistryReport,
    ) {
        assert!(registry_report(scenarios, &DEFINITIONS) == expected);
    }
}
