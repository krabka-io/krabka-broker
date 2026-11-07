//! The tests a feature file generates, one per scenario or outline row.
//!
//! [`plan`] flattens `Rule:` blocks, prepends the feature and rule
//! backgrounds, expands each outline into its Examples rows, and names every
//! test and module. `scenarios!` turns a [`Plan`] into code; it decides
//! nothing about the feature itself.

use {
    crate::model::{Keyword, substitute},
    gherkin::{Background, Examples, Feature, Scenario, Step},
    std::{
        collections::{HashMap, HashSet},
        path::Path,
    },
};

/// Every test one feature file generates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The manifest-relative path of the feature file.
    pub feature_path: String,
    /// The feature's name.
    pub feature: String,
    /// The module the feature's tests live in: the file stem as an identifier.
    pub module: String,
    /// The tests in file order.
    pub scenarios: Vec<PlannedScenario>,
}

/// One generated test: a plain scenario, or one row of an outline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedScenario {
    /// The name of the `Rule:` the scenario sits under.
    pub rule: Option<String>,
    /// The module of the rule, inside the feature module.
    pub rule_module: Option<String>,
    pub scenario: String,
    /// The module of an outline, which holds one test per row. `None` for a
    /// plain scenario.
    pub scenario_module: Option<String>,
    /// The test function's name, unique within its module.
    pub test_name: String,
    /// The 1-based line of the scenario.
    pub line: usize,
    /// The tags of the feature, the rule, the scenario and, for a row, its
    /// Examples block, in that order and without repeats.
    pub tags: Vec<String>,
    pub row: Option<PlannedRow>,
    /// The background steps of the feature, then of the rule, then the
    /// scenario's own steps, with the row's placeholders substituted.
    pub steps: Vec<PlannedStep>,
}

/// The Examples row an outline test runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRow {
    /// The 1-based line of the row.
    ///
    /// It counts from the table's header, so a comment or blank line inside
    /// the table shifts it.
    pub line: usize,
    /// The row's `case` cell, or empty when the table has no `case` column.
    pub case: String,
    /// Every cell of the row, `case` included, as `(column, value)`.
    pub values: Vec<(String, String)>,
}

/// One step a test runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedStep {
    /// The 1-based line of the step.
    pub line: usize,
    /// The resolved keyword: `And`, `But` and `*` take the one before them.
    pub keyword: Keyword,
    /// Whether the step is written with `*`.
    pub star: bool,
    /// The keyword as written, such as `And`.
    pub written: String,
    pub text: String,
    /// The doc string as gherkin reads it.
    pub docstring: Option<String>,
    /// The data table's rows. The empty marker `-` stays as written.
    pub table: Option<Vec<Vec<String>>>,
}

/// The tests `feature`, read from `path`, generates.
#[must_use]
pub fn plan(path: &str, feature: &Feature) -> Plan {
    let mut planner = Planner {
        feature,
        feature_names: Names::default(),
        rule_names: HashMap::new(),
        scenarios: Vec::new(),
    };
    for scenario in &feature.scenarios {
        planner.scenario(None, scenario);
    }
    for rule in &feature.rules {
        let module = planner.feature_names.claim(&rule.name);
        let context = RuleContext {
            name: &rule.name,
            module,
            tags: &rule.tags,
            background: rule.background.as_ref(),
        };
        for scenario in &rule.scenarios {
            planner.scenario(Some(&context), scenario);
        }
    }
    let stem = Path::new(path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(path);
    Plan {
        feature_path: path.to_owned(),
        feature: feature.name.clone(),
        module: ident(stem),
        scenarios: planner.scenarios,
    }
}

/// `text` as a snake-case Rust identifier.
///
/// Every run of characters that is not an ASCII letter or digit becomes one
/// `_`, and a lowercase letter or digit followed by an uppercase letter gets
/// one between them. An identifier that would start with a digit gets a
/// leading `_`, and a Rust keyword gets an `r_` prefix. Text with no letter
/// or digit becomes `unnamed`.
#[must_use]
pub fn ident(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut gap = false;
    let mut previous_lower = false;
    for character in text.chars() {
        if !character.is_ascii_alphanumeric() {
            gap = true;
            previous_lower = false;
            continue;
        }
        let upper = character.is_ascii_uppercase();
        if !out.is_empty() && (gap || (upper && previous_lower)) {
            out.push('_');
        }
        gap = false;
        previous_lower = !upper;
        out.push(character.to_ascii_lowercase());
    }
    if out.is_empty() {
        return "unnamed".to_owned();
    }
    if out.starts_with(|character: char| character.is_ascii_digit()) {
        out.insert(0, '_');
    }
    if RUST_KEYWORDS.contains(&out.as_str()) {
        out.insert_str(0, "r_");
    }
    out
}

/// The strict and reserved keywords of Rust 2024 that can be snake case.
const RUST_KEYWORDS: [&str; 52] = [
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "static", "struct", "super", "trait", "true", "try", "type", "typeof",
    "union", "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

/// The names taken in one module.
#[derive(Default)]
struct Names {
    taken: HashSet<String>,
}

impl Names {
    /// The identifier of `text`, with `_2`, `_3` and so on appended until it
    /// is free in this module.
    fn claim(&mut self, text: &str) -> String {
        let base = ident(text);
        let mut name = base.clone();
        let mut suffix = 2;
        while self.taken.contains(&name) {
            name = format!("{base}_{suffix}");
            suffix += 1;
        }
        self.taken.insert(name.clone());
        name
    }
}

/// The rule a scenario sits under.
struct RuleContext<'f> {
    name: &'f str,
    module: String,
    tags: &'f [String],
    background: Option<&'f Background>,
}

struct Planner<'f> {
    feature: &'f Feature,
    feature_names: Names,
    /// The names taken in each rule module.
    rule_names: HashMap<String, Names>,
    scenarios: Vec<PlannedScenario>,
}

impl Planner<'_> {
    fn scenario(&mut self, rule: Option<&RuleContext<'_>>, scenario: &Scenario) {
        let mut tags = Vec::new();
        for tag in self
            .feature
            .tags
            .iter()
            .chain(rule.into_iter().flat_map(|rule| rule.tags))
            .chain(&scenario.tags)
        {
            push_unique(&mut tags, tag);
        }
        let background: Vec<&Step> = self
            .feature
            .background
            .iter()
            .chain(rule.and_then(|rule| rule.background))
            .flat_map(|background| &background.steps)
            .collect();
        let names = match rule {
            Some(rule) => self.rule_names.entry(rule.module.clone()).or_default(),
            None => &mut self.feature_names,
        };
        let name = names.claim(&scenario.name);
        let base = PlannedScenario {
            rule: rule.map(|rule| rule.name.to_owned()),
            rule_module: rule.map(|rule| rule.module.clone()),
            scenario: scenario.name.clone(),
            scenario_module: None,
            test_name: name.clone(),
            line: scenario.position.line,
            tags,
            row: None,
            steps: Vec::new(),
        };
        if scenario.examples.is_empty() {
            self.scenarios.push(PlannedScenario {
                steps: planned_steps(&background, &scenario.steps, &HashMap::new()),
                ..base
            });
            return;
        }
        let mut row_names = Names::default();
        let mut number = 0;
        for examples in &scenario.examples {
            for (line, header, row) in rows(examples) {
                number += 1;
                let values: HashMap<&str, &str> = header
                    .iter()
                    .map(String::as_str)
                    .zip(row.iter().map(String::as_str))
                    .collect();
                let case = values.get("case").map(|case| (*case).to_owned());
                let test_name = match &case {
                    Some(case) => row_names.claim(case),
                    None => row_names.claim(&format!("case_{number}")),
                };
                let mut row_tags = base.tags.clone();
                for tag in &examples.tags {
                    push_unique(&mut row_tags, tag);
                }
                let steps = planned_steps(&background, &scenario.steps, &values);
                self.scenarios.push(PlannedScenario {
                    scenario_module: Some(name.clone()),
                    test_name,
                    tags: row_tags,
                    row: Some(PlannedRow {
                        line,
                        case: case.unwrap_or_default(),
                        values: header.iter().cloned().zip(row.iter().cloned()).collect(),
                    }),
                    steps,
                    ..base.clone()
                });
            }
        }
    }
}

/// Each data row of an Examples table with its line and the header.
fn rows(examples: &Examples) -> Vec<(usize, &[String], &[String])> {
    let Some(table) = &examples.table else {
        return Vec::new();
    };
    let Some((header, data)) = table.rows.split_first() else {
        return Vec::new();
    };
    data.iter()
        .enumerate()
        .map(|(index, row)| {
            (
                table.position.line + index + 1,
                header.as_slice(),
                row.as_slice(),
            )
        })
        .collect()
}

fn push_unique(tags: &mut Vec<String>, tag: &str) {
    if !tags.iter().any(|existing| existing == tag) {
        tags.push(tag.to_owned());
    }
}

/// The background steps, then the scenario's own steps with `values`
/// substituted. A `*` step takes the keyword of the step before it, or
/// `Given` when it comes first.
fn planned_steps(
    background: &[&Step],
    steps: &[Step],
    values: &HashMap<&str, &str>,
) -> Vec<PlannedStep> {
    let unsubstituted = HashMap::new();
    let mut previous = Keyword::Given;
    background
        .iter()
        .map(|step| (*step, &unsubstituted))
        .chain(steps.iter().map(|step| (step, values)))
        .map(|(step, values)| {
            let planned = planned_step(step, values, previous);
            previous = planned.keyword;
            planned
        })
        .collect()
}

fn planned_step(step: &Step, values: &HashMap<&str, &str>, previous: Keyword) -> PlannedStep {
    let written = step.keyword.trim();
    let star = written == "*";
    PlannedStep {
        line: step.position.line,
        keyword: if star {
            previous
        } else {
            Keyword::from(step.ty)
        },
        star,
        written: written.to_owned(),
        text: substitute(&step.value, values),
        docstring: step
            .docstring
            .as_ref()
            .map(|docstring| substitute(docstring, values)),
        table: step.table.as_ref().map(|table| {
            table
                .rows
                .iter()
                .map(|row| row.iter().map(|cell| substitute(cell, values)).collect())
                .collect()
        }),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, gherkin::GherkinEnv, rstest::rstest};

    const PATH: &str = "tests/features/log_policy/log_policy.feature";

    fn parse(source: &str) -> Feature {
        Feature::parse(source, GherkinEnv::default()).expect("the test feature parses")
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn step(line: usize, keyword: Keyword, written: &str, text: &str) -> PlannedStep {
        PlannedStep {
            line,
            keyword,
            star: written == "*",
            written: written.to_owned(),
            text: text.to_owned(),
            docstring: None,
            table: None,
        }
    }

    fn pairs(values: &[(&str, &str)]) -> Vec<(String, String)> {
        values
            .iter()
            .map(|(column, value)| ((*column).to_owned(), (*value).to_owned()))
            .collect()
    }

    const FEATURE: &str = "\
@docker
Feature: Log cleanup policy

  Background:
    Given a broker

  Scenario: A topic without a policy deletes
    When the cleanup policy is read back
    Then it is delete

  @formulated
  Rule: A cleanup policy is a membership list

    Background:
      Given a topic

    Scenario Outline: Each policy reports its members
      Given a log has cleanup policy <policy>
      When the cleanup policy is read back
      * it reports compact as <compact>
        | policy   | compact   |
        | <policy> | <compact> |
        | -        | -         |
      Then it says
        \"\"\"
        <policy>
        \"\"\"

      Examples:
        | case     | policy  | compact |
        | delete only | delete | false |

      @inprogress
      Examples:
        | case                | policy         | compact |
        | compact and delete  | compact,delete | true    |
";

    #[test]
    fn plan_flattens_rules_backgrounds_and_outline_rows() {
        let actual = plan(PATH, &parse(FEATURE));
        let broker = step(5, Keyword::Given, "Given", "a broker");
        let topic = step(15, Keyword::Given, "Given", "a topic");
        let outline_steps = |policy: &str, compact: &str| {
            vec![
                broker.clone(),
                topic.clone(),
                step(
                    18,
                    Keyword::Given,
                    "Given",
                    &format!("a log has cleanup policy {policy}"),
                ),
                step(19, Keyword::When, "When", "the cleanup policy is read back"),
                PlannedStep {
                    table: Some(vec![
                        strings(&["policy", "compact"]),
                        strings(&[policy, compact]),
                        strings(&["-", "-"]),
                    ]),
                    ..step(
                        20,
                        Keyword::When,
                        "*",
                        &format!("it reports compact as {compact}"),
                    )
                },
                PlannedStep {
                    docstring: Some(format!("\n{policy}\n")),
                    ..step(24, Keyword::Then, "Then", "it says")
                },
            ]
        };
        let row = |name: &str| PlannedScenario {
            rule: Some("A cleanup policy is a membership list".to_owned()),
            rule_module: Some("a_cleanup_policy_is_a_membership_list".to_owned()),
            scenario: "Each policy reports its members".to_owned(),
            scenario_module: Some("each_policy_reports_its_members".to_owned()),
            test_name: name.to_owned(),
            line: 17,
            tags: strings(&["docker", "formulated"]),
            row: None,
            steps: Vec::new(),
        };
        let expected = Plan {
            feature_path: PATH.to_owned(),
            feature: "Log cleanup policy".to_owned(),
            module: "log_policy".to_owned(),
            scenarios: vec![
                PlannedScenario {
                    rule: None,
                    rule_module: None,
                    scenario: "A topic without a policy deletes".to_owned(),
                    scenario_module: None,
                    test_name: "a_topic_without_a_policy_deletes".to_owned(),
                    line: 7,
                    tags: strings(&["docker"]),
                    row: None,
                    steps: vec![
                        broker.clone(),
                        step(8, Keyword::When, "When", "the cleanup policy is read back"),
                        step(9, Keyword::Then, "Then", "it is delete"),
                    ],
                },
                PlannedScenario {
                    row: Some(PlannedRow {
                        line: 31,
                        case: "delete only".to_owned(),
                        values: pairs(&[
                            ("case", "delete only"),
                            ("policy", "delete"),
                            ("compact", "false"),
                        ]),
                    }),
                    steps: outline_steps("delete", "false"),
                    ..row("delete_only")
                },
                PlannedScenario {
                    tags: strings(&["docker", "formulated", "inprogress"]),
                    row: Some(PlannedRow {
                        line: 36,
                        case: "compact and delete".to_owned(),
                        values: pairs(&[
                            ("case", "compact and delete"),
                            ("policy", "compact,delete"),
                            ("compact", "true"),
                        ]),
                    }),
                    steps: outline_steps("compact,delete", "true"),
                    ..row("compact_and_delete")
                },
            ],
        };
        assert!(actual == expected);
    }

    const COLLISIONS: &str = "\
Feature: Names collide

  Scenario: A log rolls!
    When a log rolls

  Scenario: A log rolls?
    When a log rolls

  Scenario Outline: Rows collide
    When a log rolls <n>

    Examples:
      | case | n |
      | same | 1 |
      | Same | 2 |

  Scenario Outline: No case column
    When a log rolls <n>

    Examples:
      | n |
      | 1 |
      | 2 |

  Rule: Same rule
    Scenario: A log rolls
      When a log rolls

  Rule: Same rule!
    Scenario: A log rolls
      When a log rolls

";

    #[test]
    fn names_are_unique_within_their_module() {
        let actual: Vec<(Option<String>, Option<String>, String)> = plan(PATH, &parse(COLLISIONS))
            .scenarios
            .into_iter()
            .map(|scenario| {
                (
                    scenario.rule_module,
                    scenario.scenario_module,
                    scenario.test_name,
                )
            })
            .collect();
        let name = |rule: Option<&str>, outline: Option<&str>, test: &str| {
            (
                rule.map(ToOwned::to_owned),
                outline.map(ToOwned::to_owned),
                test.to_owned(),
            )
        };
        let expected = vec![
            name(None, None, "a_log_rolls"),
            name(None, None, "a_log_rolls_2"),
            name(None, Some("rows_collide"), "same"),
            name(None, Some("rows_collide"), "same_2"),
            name(None, Some("no_case_column"), "case_1"),
            name(None, Some("no_case_column"), "case_2"),
            name(Some("same_rule"), None, "a_log_rolls"),
            name(Some("same_rule_2"), None, "a_log_rolls"),
        ];
        assert!(actual == expected);
    }

    #[test]
    fn row_without_case_column_has_an_empty_case() {
        let cases: Vec<Option<String>> = plan(PATH, &parse(COLLISIONS))
            .scenarios
            .into_iter()
            .filter(|scenario| scenario.scenario == "No case column")
            .map(|scenario| scenario.row.map(|row| row.case))
            .collect();
        assert!(cases == vec![Some(String::new()), Some(String::new())]);
    }

    #[rstest]
    #[case::spaces("A log rolls a new segment", "a_log_rolls_a_new_segment")]
    #[case::punctuation("compact, delete & more!", "compact_delete_more")]
    #[case::leading_digit("3 replicas", "_3_replicas")]
    #[case::keyword("type", "r_type")]
    #[case::keyword_after_casing("Self", "r_self")]
    #[case::camel_case("LeaderEpoch bump", "leader_epoch_bump")]
    #[case::acronym("ISR shrinks", "isr_shrinks")]
    #[case::non_ascii("café über", "caf_ber")]
    #[case::empty("!!!", "unnamed")]
    #[case::trailing_separators("  rolls  ", "rolls")]
    fn ident_makes_snake_case_identifiers(#[case] text: &str, #[case] expected: &str) {
        assert!(ident(text) == expected);
    }
}
