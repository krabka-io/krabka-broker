//! Why a scenario failed, rendered as the diagnostic a failing test prints.

use {
    crate::spec::{ScenarioSpec, StepSpec},
    std::fmt::{self, Display, Formatter},
};

/// Where in a scenario's run a problem happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Location {
    /// The World fixture's `before`.
    WorldBefore(&'static str),
    /// A `before` hook, or selecting the hooks.
    BeforeHook {
        /// The hook's source file, empty when selection failed.
        file: &'static str,
        /// The hook's line.
        line: u32,
    },
    /// A step; `index` is zero-based.
    Step {
        /// The step's index in [`ScenarioSpec::steps`].
        index: usize,
        /// The step.
        step: StepSpec,
    },
    /// An `after` hook, or selecting the hooks.
    AfterHook {
        /// The hook's source file, empty when selection failed.
        file: &'static str,
        /// The hook's line.
        line: u32,
    },
    /// The World fixture's `after`.
    WorldAfter(&'static str),
}

impl Display for Location {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorldBefore(name) => write!(f, "World `{name}` before"),
            Self::BeforeHook { file: "", .. } => f.write_str("Selecting the before hooks"),
            Self::BeforeHook { file, line } => write!(f, "Before hook at {file}:{line}"),
            Self::Step { index, step } => write!(
                f,
                "Step {} at line {}: {} {}",
                index + 1,
                step.line,
                step.written,
                step.text
            ),
            Self::AfterHook { file: "", .. } => f.write_str("Selecting the after hooks"),
            Self::AfterHook { file, line } => write!(f, "After hook at {file}:{line}"),
            Self::WorldAfter(name) => write!(f, "World `{name}` after"),
        }
    }
}

/// One problem: where it happened and what went wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Problem {
    location: Location,
    message: String,
}

/// Why a scenario failed.
///
/// Its [`Display`] is the diagnostic a failing scenario test panics with:
///
/// ```text
/// scenario failed at tests/features/log_policy/log_policy.feature:12
///   Rule: A cleanup policy is a membership list
///   Scenario: Each policy reports its members
///   Example: line 24: case = compact and delete, policy = compact,delete
///   Step 2 at line 14: When the cleanup policy is read back
///   <what went wrong>
/// ```
///
/// A problem in an `after` hook or a World's `after` on top of the first one
/// follows as an `also:` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    scenario: &'static ScenarioSpec,
    first: Problem,
    also: Vec<Problem>,
}

impl Failure {
    /// A failure at `location` with `message`.
    #[must_use]
    pub fn new(scenario: &'static ScenarioSpec, location: Location, message: String) -> Self {
        Self {
            scenario,
            first: Problem {
                location,
                message: readable(message),
            },
            also: Vec::new(),
        }
    }

    /// Adds a later problem, such as a panicking `after` hook.
    pub fn push(&mut self, location: Location, message: String) {
        self.also.push(Problem {
            location,
            message: readable(message),
        });
    }

    /// The scenario that failed.
    #[must_use]
    pub const fn scenario(&self) -> &'static ScenarioSpec { self.scenario }

    /// Where the first problem happened.
    #[must_use]
    pub const fn location(&self) -> &Location { &self.first.location }

    /// What went wrong first: the step error, the panic message, or the
    /// undefined or ambiguous step listing.
    #[must_use]
    pub fn message(&self) -> &str { &self.first.message }
}

/// `message` without the Unicode isolation marks (U+2068, U+2069) that
/// rstest-bdd's Fluent localization puts around every interpolated value.
fn readable(mut message: String) -> String {
    message.retain(|character| !matches!(character, '\u{2068}' | '\u{2069}'));
    message
}

fn write_message(f: &mut Formatter<'_>, message: &str) -> fmt::Result {
    for line in message.lines() {
        write!(f, "\n  {line}")?;
    }
    Ok(())
}

impl Display for Failure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let scenario = self.scenario;
        write!(
            f,
            "scenario failed at {}:{}",
            scenario.feature_path, scenario.line
        )?;
        if let Some(rule) = scenario.rule {
            write!(f, "\n  Rule: {rule}")?;
        }
        write!(f, "\n  Scenario: {}", scenario.scenario)?;
        if let Some(row) = scenario.row {
            write!(f, "\n  Example: line {}: ", row.line)?;
            for (index, (header, cell)) in row.values.iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{header} = {cell}")?;
            }
        }
        write!(f, "\n  {}", self.first.location)?;
        write_message(f, &self.first.message)?;
        for problem in &self.also {
            write!(f, "\n  also: {}", problem.location)?;
            write_message(f, &problem.message)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::spec::RowSpec, assert2::assert, rstest::rstest, rstest_bdd::StepKeyword,
    };

    static STEPS: [StepSpec; 2] = [
        StepSpec {
            line: 13,
            keyword: StepKeyword::Given,
            star: false,
            written: "Given",
            text: "a log has cleanup policy compact,delete",
            docstring: None,
            table: None,
        },
        StepSpec {
            line: 14,
            keyword: StepKeyword::When,
            star: false,
            written: "When",
            text: "the cleanup policy is read back",
            docstring: None,
            table: None,
        },
    ];

    static OUTLINE_ROW: ScenarioSpec = ScenarioSpec {
        feature_path: "tests/features/log_policy/log_policy.feature",
        feature: "Log policy",
        rule: Some("A cleanup policy is a membership list"),
        scenario: "Each policy reports its members",
        line: 12,
        row: Some(RowSpec {
            line: 24,
            case: "compact and delete",
            values: &[
                ("case", "compact and delete"),
                ("policy", "compact,delete"),
                ("compact", "true"),
            ],
        }),
        tags: &[],
        steps: &STEPS,
        expect_failure: false,
    };

    static PLAIN: ScenarioSpec = ScenarioSpec {
        feature_path: "tests/features/a.feature",
        feature: "A",
        rule: None,
        scenario: "Plain",
        line: 3,
        row: None,
        tags: &[],
        steps: &STEPS,
        expect_failure: false,
    };

    fn step_failure(scenario: &'static ScenarioSpec) -> Failure {
        Failure::new(
            scenario,
            Location::Step {
                index: 1,
                step: STEPS[1],
            },
            "the step failed\nwith detail".to_owned(),
        )
    }

    #[rstest]
    #[case::outline_row_under_a_rule(
        step_failure(&OUTLINE_ROW),
        "scenario failed at tests/features/log_policy/log_policy.feature:12\n  \
         Rule: A cleanup policy is a membership list\n  \
         Scenario: Each policy reports its members\n  \
         Example: line 24: case = compact and delete, policy = compact,delete, compact = true\n  \
         Step 2 at line 14: When the cleanup policy is read back\n  \
         the step failed\n  \
         with detail",
    )]
    #[case::plain_scenario_with_later_problems(
        {
            let mut failure = step_failure(&PLAIN);
            failure.push(Location::AfterHook { file: "h.rs", line: 7 }, "hook panicked".to_owned());
            failure.push(Location::WorldAfter("world"), "teardown failed".to_owned());
            failure
        },
        "scenario failed at tests/features/a.feature:3\n  \
         Scenario: Plain\n  \
         Step 2 at line 14: When the cleanup policy is read back\n  \
         the step failed\n  \
         with detail\n  \
         also: After hook at h.rs:7\n  \
         hook panicked\n  \
         also: World `world` after\n  \
         teardown failed",
    )]
    #[case::world_before(
        Failure::new(&PLAIN, Location::WorldBefore("world"), "boom".to_owned()),
        "scenario failed at tests/features/a.feature:3\n  Scenario: Plain\n  World `world` before\n  boom",
    )]
    #[case::before_hook(
        Failure::new(&PLAIN, Location::BeforeHook { file: "h.rs", line: 2 }, "boom".to_owned()),
        "scenario failed at tests/features/a.feature:3\n  Scenario: Plain\n  Before hook at h.rs:2\n  boom",
    )]
    #[case::hook_selection(
        Failure::new(&PLAIN, Location::BeforeHook { file: "", line: 0 }, "bad expression".to_owned()),
        "scenario failed at tests/features/a.feature:3\n  Scenario: Plain\n  Selecting the before hooks\n  bad expression",
    )]
    fn display_renders_the_diagnostic(#[case] failure: Failure, #[case] expected: &str) {
        assert!(failure.to_string() == expected);
    }

    #[test]
    fn messages_drop_the_localization_isolation_marks() {
        assert!(
            readable("Step failed at index \u{2068}1\u{2069}: \u{2068}Then\u{2069}".to_owned())
                == "Step failed at index 1: Then"
        );
    }
}
