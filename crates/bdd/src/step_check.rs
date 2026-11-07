//! The check that every feature step matches exactly one step definition.
//!
//! rstest-bdd reports a step without a definition only when its scenario runs,
//! and when several definitions match a step it picks the most specific one
//! and breaks ties arbitrarily. Each `krabka_bdd::scenarios!` call emits a test
//! that calls [`check_steps`] with every step of the features it binds, so
//! both problems fail one test that lists every offending step.

use {
    rstest_bdd::{Step, StepKeyword, StepPattern, extract_placeholders, iter},
    std::fmt::Write,
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

/// One step definition, as the check sees it.
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

/// Checks that every step in `uses` matches exactly one registered step
/// definition.
///
/// # Panics
///
/// Panics with a report listing every step that no definition matches and
/// every step that more than one definition matches.
#[track_caller]
pub fn check_steps(uses: &[StepUse]) {
    let definitions: Vec<Definition<'_>> = iter::<Step>
        .into_iter()
        .map(|step| Definition {
            keyword: step.keyword,
            pattern: step.pattern,
            file: step.file,
            line: step.line,
        })
        .collect();
    if let Some(report) = report(uses, &definitions) {
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
        for definition in matches {
            let _ = write!(
                report,
                "\n    {}:{} `{}`",
                definition.file,
                definition.line,
                definition.pattern.as_str()
            );
        }
    }
    Some(report)
}

fn matches(definition: &Definition<'_>, step: &StepUse) -> bool {
    definition.keyword == step.keyword
        && extract_placeholders(definition.pattern, step.text.into()).is_ok()
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
