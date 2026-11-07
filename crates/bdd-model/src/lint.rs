//! The house Gherkin style, checked over one feature file.
//!
//! Each rule here is one line of the [Gherkin style guide]. `scenarios!` runs
//! [`lint`] over every feature file it reads and turns each [`Finding`] into a
//! compile error, so a feature that breaks the style does not build.
//!
//! [Gherkin style guide]: https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/gherkin_style_guide.md

use {
    gherkin::{Background, Examples, Feature, GherkinEnv, ParseError, Scenario, Step, StepType},
    std::{
        collections::{HashMap, HashSet},
        fmt,
    },
};

/// The tags the tag table in the Gherkin style guide defines, without their `@`.
pub const ALLOWED_TAGS: [&str; 4] = ["docker", "journey", "formulated", "inprogress"];

/// The tag that marks an end-to-end journey, which most scenario rules exempt.
const JOURNEY_TAG: &str = "journey";

/// Words that name how code works rather than what the domain does.
const MECHANISM_WORDS: [&str; 8] = [
    "call",
    "calls",
    "invoke",
    "invokes",
    "instantiate",
    "construct",
    "mock",
    "struct",
];

/// The most steps an illustrative scenario may have.
const MAX_STEPS: usize = 5;

/// One house-style rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LintRule {
    Parse,
    NoBackground,
    OneWhen,
    MaxSteps,
    StarStep,
    JourneyLocation,
    FirstPerson,
    OrInStep,
    QuoteInStep,
    MechanismWord,
    UnknownTag,
    UserStoryFeature,
    PlaceholderName,
    CaseColumn,
    DuplicateExampleRow,
    RaggedTable,
    DuplicateScenarioName,
    ScenarioOutsideRule,
}

impl LintRule {
    /// The kebab-case id a finding names the rule by.
    pub const fn id(self) -> &'static str {
        match self {
            Self::Parse => "parse",
            Self::NoBackground => "no-background",
            Self::OneWhen => "one-when",
            Self::MaxSteps => "max-steps",
            Self::StarStep => "star-step",
            Self::JourneyLocation => "journey-location",
            Self::FirstPerson => "first-person",
            Self::OrInStep => "or-in-step",
            Self::QuoteInStep => "quote-in-step",
            Self::MechanismWord => "mechanism-word",
            Self::UnknownTag => "unknown-tag",
            Self::UserStoryFeature => "user-story-feature",
            Self::PlaceholderName => "placeholder-name",
            Self::CaseColumn => "case-column",
            Self::DuplicateExampleRow => "duplicate-example-row",
            Self::RaggedTable => "ragged-table",
            Self::DuplicateScenarioName => "duplicate-scenario-name",
            Self::ScenarioOutsideRule => "scenario-outside-rule",
        }
    }

    /// A short imperative fix for a violation of the rule.
    pub const fn hint(self) -> &'static str {
        match self {
            Self::Parse => "correct the Gherkin syntax at this position",
            Self::NoBackground => {
                "move each essential step into the scenarios that need it, and delete the rest"
            }
            Self::OneWhen => {
                "move the extra actions into Given context, or split the scenario so each has \
                 exactly one When"
            }
            Self::MaxSteps => {
                "delete the steps that do not illustrate the rule, or split the scenario"
            }
            Self::StarStep => "use Given, When or Then; `*` steps belong to @journey scenarios",
            Self::JourneyLocation => "move the journey into a feature file under `journeys/`",
            Self::FirstPerson => "name the actor by role, such as a consumer or an operator",
            Self::OrInStep => "write one scenario per alternative",
            Self::QuoteInStep => {
                "drop the quotation marks; a step definition parses an unquoted value"
            }
            Self::MechanismWord => {
                "describe the request, the record or the state instead of the code mechanism"
            }
            Self::UnknownTag => {
                "use a tag from the tag table in docs/style_guides/gherkin_style_guide.md, or \
                 remove the tag"
            }
            Self::UserStoryFeature => "name the feature after the capability it describes",
            Self::PlaceholderName => {
                "name the scenario so it completes `The one where ...` in domain terms"
            }
            Self::CaseColumn => "make the first column `case`, saying why each row exists",
            Self::DuplicateExampleRow => {
                "delete the repeated row; every row must teach something different"
            }
            Self::RaggedTable => "give every row of the table the same number of cells",
            Self::DuplicateScenarioName => {
                "rename one scenario so each name says what it illustrates"
            }
            Self::ScenarioOutsideRule => "move the scenario under the `Rule:` it illustrates",
        }
    }
}

/// One house-style violation at a place in a feature file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub rule: LintRule,
    /// The manifest-relative path of the feature file.
    pub path: String,
    /// The 1-based line.
    pub line: usize,
    /// The 1-based column.
    pub col: usize,
    pub message: String,
}

impl fmt::Display for Finding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}:{}: {}: {}. Fix: {}",
            self.path,
            self.line,
            self.col,
            self.rule.id(),
            self.message,
            self.rule.hint()
        )
    }
}

/// Every house-style violation in one feature file, in file order.
///
/// `path` is the manifest-relative path with `/` separators. A file that does
/// not parse yields its ragged tables, which gherkin refuses to parse, or else
/// one [`LintRule::Parse`] finding.
pub fn lint(path: &str, source: &str) -> Vec<Finding> {
    let mut linter = Linter {
        path,
        lines: source.lines().collect(),
        findings: Vec::new(),
    };
    linter.ragged_tables();
    match Feature::parse(source, GherkinEnv::default()) {
        Ok(feature) => linter.feature(&feature),
        Err(error) => {
            if linter.findings.is_empty() {
                linter.parse_error(&error);
            }
        }
    }
    let mut findings = linter.findings;
    findings.sort_by_key(|finding| (finding.line, finding.col));
    findings
}

/// One row of a table as the raw source has it.
struct RawRow {
    line: usize,
    col: usize,
    cells: usize,
}

struct Linter<'s> {
    path: &'s str,
    lines: Vec<&'s str>,
    findings: Vec<Finding>,
}

impl Linter<'_> {
    fn report(&mut self, rule: LintRule, line: usize, col: usize, message: String) {
        self.findings.push(Finding {
            rule,
            path: self.path.to_owned(),
            line,
            col,
            message,
        });
    }

    fn parse_error(&mut self, error: &ParseError) {
        let text = error.to_string();
        let (line, col, expected) = parse_error_location(&text).unwrap_or((1, 1, text.as_str()));
        // gherkin appends a final newline before it parses, so an error at the
        // end of the input lands on a line past the last one.
        let last_line = self.lines.len().max(1);
        let (line, col) = if line > last_line {
            (last_line, 1)
        } else {
            (line, col)
        };
        let message = format!("gherkin cannot parse the file here, expected one of {expected}");
        self.report(LintRule::Parse, line, col, message);
    }

    fn feature(&mut self, feature: &Feature) {
        let line = feature.position.line;
        if is_user_story(&feature.name) {
            let message = format!("feature `{}` is named after a user story", feature.name);
            self.report(
                LintRule::UserStoryFeature,
                line,
                feature.position.col,
                message,
            );
        }
        self.tags(&feature.tags, line);
        self.background(feature.background.as_ref(), "the feature");
        let has_rules = !feature.rules.is_empty();
        for scenario in &feature.scenarios {
            let journey = self.scenario(scenario, &feature.tags);
            if has_rules && !journey {
                let message = format!(
                    "scenario `{}` sits directly under the feature, which has `Rule:` blocks",
                    scenario.name
                );
                let position = scenario.position;
                self.report(
                    LintRule::ScenarioOutsideRule,
                    position.line,
                    position.col,
                    message,
                );
            }
        }
        for rule in &feature.rules {
            self.tags(&rule.tags, rule.position.line);
            self.background(rule.background.as_ref(), &format!("rule `{}`", rule.name));
            let tags: Vec<String> = feature.tags.iter().chain(&rule.tags).cloned().collect();
            for scenario in &rule.scenarios {
                self.scenario(scenario, &tags);
            }
        }
        self.duplicate_names(feature);
    }

    fn background(&mut self, background: Option<&Background>, owner: &str) {
        if let Some(background) = background {
            let message = format!("{owner} has a `Background`");
            let position = background.position;
            self.report(LintRule::NoBackground, position.line, position.col, message);
        }
    }

    /// Lints one scenario and reports whether it is a journey.
    fn scenario(&mut self, scenario: &Scenario, inherited_tags: &[String]) -> bool {
        let journey = inherited_tags
            .iter()
            .chain(&scenario.tags)
            .any(|tag| tag == JOURNEY_TAG);
        self.tags(&scenario.tags, scenario.position.line);
        self.scenario_name(scenario);
        if journey {
            self.journey_location(scenario);
        } else {
            self.scenario_shape(scenario);
        }
        for step in &scenario.steps {
            self.step(step, journey);
        }
        self.examples(scenario);
        journey
    }

    fn scenario_name(&mut self, scenario: &Scenario) {
        let position = scenario.position;
        if is_placeholder_name(&scenario.name) {
            let message = format!("scenario name `{}` is a placeholder", scenario.name);
            self.report(
                LintRule::PlaceholderName,
                position.line,
                position.col,
                message,
            );
        }
        if let Some(word) = first_person_word(&scenario.name) {
            let message = format!(
                "scenario name `{}` uses the first-person word `{word}`",
                scenario.name
            );
            self.report(LintRule::FirstPerson, position.line, position.col, message);
        }
    }

    fn journey_location(&mut self, scenario: &Scenario) {
        if !self
            .path
            .split('/')
            .any(|component| component == "journeys")
        {
            let message = format!(
                "journey `{}` is in `{}`, outside a `journeys` directory",
                scenario.name, self.path
            );
            let position = scenario.position;
            self.report(
                LintRule::JourneyLocation,
                position.line,
                position.col,
                message,
            );
        }
    }

    fn scenario_shape(&mut self, scenario: &Scenario) {
        let position = scenario.position;
        let whens = scenario
            .steps
            .iter()
            .filter(|step| step.ty == StepType::When)
            .count();
        if whens != 1 {
            let message = format!(
                "scenario `{}` has {whens} `When` steps, and exactly one is allowed",
                scenario.name
            );
            self.report(LintRule::OneWhen, position.line, position.col, message);
        }
        let steps = scenario.steps.len();
        if steps > MAX_STEPS {
            let message = format!(
                "scenario `{}` has {steps} steps, and at most {MAX_STEPS} are allowed",
                scenario.name
            );
            self.report(LintRule::MaxSteps, position.line, position.col, message);
        }
    }

    fn step(&mut self, step: &Step, journey: bool) {
        let (line, col) = (step.position.line, step.position.col);
        let text = step.value.as_str();
        if !journey && step.keyword.trim() == "*" {
            let message = format!("step `* {text}` uses `*` outside a @journey scenario");
            self.report(LintRule::StarStep, line, col, message);
        }
        if let Some(word) = first_person_word(text) {
            let message = format!("step `{text}` uses the first-person word `{word}`");
            self.report(LintRule::FirstPerson, line, col, message);
        }
        if let Some(word) = words(text).find(|word| word.eq_ignore_ascii_case("or")) {
            let message = format!("step `{text}` offers alternatives with `{word}`");
            self.report(LintRule::OrInStep, line, col, message);
        }
        if text.contains('"') {
            let message = format!("step `{text}` quotes a value");
            self.report(LintRule::QuoteInStep, line, col, message);
        }
        if let Some(word) = words(text).find(|word| {
            MECHANISM_WORDS
                .iter()
                .any(|mechanism| word.eq_ignore_ascii_case(mechanism))
        }) {
            let message = format!("step `{text}` uses the mechanism word `{word}`");
            self.report(LintRule::MechanismWord, line, col, message);
        }
    }

    fn examples(&mut self, scenario: &Scenario) {
        let mut seen: HashSet<(&[String], &[String])> = HashSet::new();
        for examples in &scenario.examples {
            self.tags(&examples.tags, examples.position.line);
            let Some(table) = &examples.table else {
                continue;
            };
            let Some((header, rows)) = table.rows.split_first() else {
                continue;
            };
            let first = header.first().map_or("", |cell| cell.trim());
            if first != "case" {
                let message =
                    format!("the Examples table starts with column `{first}`, not `case`");
                let position = table.position;
                self.report(LintRule::CaseColumn, position.line, position.col, message);
            }
            for (index, row) in rows.iter().enumerate() {
                if !seen.insert((header.as_slice(), row.as_slice())) {
                    self.duplicate_row(examples, index, row);
                }
            }
        }
    }

    fn duplicate_row(&mut self, examples: &Examples, index: usize, row: &[String]) {
        let start = examples
            .table
            .as_ref()
            .map_or(examples.position, |table| table.position);
        let raw = self.raw_rows(start.line);
        let (line, col) = raw
            .get(index + 1)
            .map_or((start.line, start.col), |row| (row.line, row.col));
        let message = format!(
            "example row `| {} |` repeats an earlier row",
            row.join(" | ")
        );
        self.report(LintRule::DuplicateExampleRow, line, col, message);
    }

    fn duplicate_names(&mut self, feature: &Feature) {
        let mut scenarios: Vec<&Scenario> = feature
            .scenarios
            .iter()
            .chain(feature.rules.iter().flat_map(|rule| &rule.scenarios))
            .collect();
        scenarios.sort_by_key(|scenario| scenario.position.line);
        let mut first_lines: HashMap<&str, usize> = HashMap::new();
        for scenario in scenarios {
            let position = scenario.position;
            if let Some(first) = first_lines.get(scenario.name.as_str()) {
                let message = format!(
                    "scenario name `{}` is already used on line {first}",
                    scenario.name
                );
                self.report(
                    LintRule::DuplicateScenarioName,
                    position.line,
                    position.col,
                    message,
                );
            } else {
                first_lines.insert(scenario.name.as_str(), position.line);
            }
        }
    }

    fn tags(&mut self, tags: &[String], node_line: usize) {
        for tag in tags {
            if ALLOWED_TAGS.contains(&tag.as_str()) {
                continue;
            }
            let message = if tag == "done" {
                "tag `@done` is not used, because the absence of a lifecycle tag means done"
                    .to_owned()
            } else {
                format!(
                    "tag `@{tag}` is not in the tag table; adding a tag means adding a row to \
                     the tag table in docs/style_guides/gherkin_style_guide.md"
                )
            };
            let (line, col) = self.tag_position(tag, node_line);
            self.report(LintRule::UnknownTag, line, col, message);
        }
    }

    /// Finds `@tag` on the tag lines above the node that carries it.
    fn tag_position(&self, tag: &str, node_line: usize) -> (usize, usize) {
        let needle = format!("@{tag}");
        let above = self
            .lines
            .iter()
            .enumerate()
            .take(node_line.saturating_sub(1));
        for (index, line) in above.rev() {
            let trimmed = line.trim_start();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if !trimmed.starts_with('@') {
                break;
            }
            if let Some(col) = token_column(line, &needle) {
                return (index + 1, col);
            }
        }
        (node_line, 1)
    }

    /// Reports each table, outside doc strings, whose rows differ in width.
    fn ragged_tables(&mut self) {
        let mut docstring: Option<&str> = None;
        let mut next_line = 1;
        let mut tables = Vec::new();
        for (index, line) in self.lines.iter().enumerate() {
            let line_number = index + 1;
            if line_number < next_line {
                continue;
            }
            let trimmed = line.trim_start();
            if let Some(fence) = docstring {
                if trimmed.starts_with(fence) {
                    docstring = None;
                }
            } else if let Some(fence) = ["\"\"\"", "```"]
                .into_iter()
                .find(|fence| trimmed.starts_with(fence))
            {
                docstring = Some(fence);
            } else if trimmed.starts_with('|') {
                let rows = self.raw_rows(line_number);
                next_line = rows.last().map_or(line_number, |row| row.line) + 1;
                tables.push(rows);
            }
        }
        for rows in tables {
            self.ragged_table(&rows);
        }
    }

    fn ragged_table(&mut self, rows: &[RawRow]) {
        let Some((header, rest)) = rows.split_first() else {
            return;
        };
        if let Some(row) = rest.iter().find(|row| row.cells != header.cells) {
            let message = format!(
                "the table row has {} cells, and the header on line {} has {}",
                row.cells, header.line, header.cells
            );
            self.report(LintRule::RaggedTable, row.line, row.col, message);
        }
    }

    /// The rows of the table whose first row is on `start_line`. Blank lines
    /// and comments inside the table are skipped.
    fn raw_rows(&self, start_line: usize) -> Vec<RawRow> {
        let mut rows = Vec::new();
        for (index, line) in self
            .lines
            .iter()
            .enumerate()
            .skip(start_line.saturating_sub(1))
        {
            let trimmed = line.trim_start();
            if trimmed.starts_with('|') {
                rows.push(RawRow {
                    line: index + 1,
                    col: line.chars().count() - trimmed.chars().count() + 1,
                    cells: separators(trimmed).saturating_sub(1),
                });
            } else if !trimmed.is_empty() && !trimmed.starts_with('#') {
                break;
            }
        }
        rows
    }
}

/// Splits `Error at LINE:COL: EXPECTED` into its parts.
fn parse_error_location(text: &str) -> Option<(usize, usize, &str)> {
    let rest = text.strip_prefix("Error at ")?;
    let (line, rest) = rest.split_once(':')?;
    let (col, expected) = rest.split_once(':')?;
    Some((line.parse().ok()?, col.parse().ok()?, expected.trim()))
}

/// Counts the unescaped `|` in a table row.
fn separators(row: &str) -> usize {
    let mut count = 0;
    let mut escaped = false;
    for character in row.chars() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '|' {
            count += 1;
        }
    }
    count
}

/// The 1-based column of `needle` in `line` as a whole whitespace-separated
/// token.
fn token_column(line: &str, needle: &str) -> Option<usize> {
    line.match_indices(needle).find_map(|(start, _)| {
        let before = line.get(..start)?;
        let after = line.get(start + needle.len()..)?;
        let bounded = before.chars().next_back().is_none_or(char::is_whitespace)
            && after.chars().next().is_none_or(char::is_whitespace);
        bounded.then(|| before.chars().count() + 1)
    })
}

/// The words of `text`, split at every character that is not alphanumeric.
fn words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|word| !word.is_empty())
}

fn first_person_word(text: &str) -> Option<&str> {
    words(text).find(|word| {
        *word == "I" || word.eq_ignore_ascii_case("me") || word.eq_ignore_ascii_case("my")
    })
}

fn is_user_story(name: &str) -> bool {
    let lower = name.trim_start().to_lowercase();
    lower.starts_with("as a ") || lower.starts_with("as an ")
}

fn is_placeholder_name(name: &str) -> bool {
    let lower = name.trim().to_lowercase();
    lower == "happy path"
        || ["test", "scenario", "tc"].into_iter().any(|prefix| {
            lower.strip_prefix(prefix).is_some_and(|rest| {
                let number = rest.strip_prefix([' ', '_', '-']).unwrap_or(rest);
                !number.is_empty() && number.chars().all(|character| character.is_ascii_digit())
            })
        })
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, rstest::rstest};

    const PATH: &str = "tests/features/log/log.feature";
    const JOURNEY_PATH: &str = "tests/features/journeys/rebalance.feature";

    fn ids(path: &str, source: &str) -> Vec<(&'static str, usize)> {
        lint(path, source)
            .into_iter()
            .map(|finding| (finding.rule.id(), finding.line))
            .collect()
    }

    #[rstest]
    #[case::no_background("\
Feature: Logs roll
  Background:
    Given a log
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("no-background", 2)])]
    #[case::one_when_two("\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends a batch
    And a producer appends another batch
    Then the log rolls a new segment
", vec![("one-when", 2)])]
    #[case::one_when_none("\
Feature: Logs roll
  Scenario: A full segment rolls
    Given a log
    Then the log rolls a new segment
", vec![("one-when", 2)])]
    #[case::max_steps("\
Feature: Logs roll
  Scenario: A full segment rolls
    Given a log
    And a segment
    And a producer
    And a consumer
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("max-steps", 2)])]
    #[case::star_step("\
Feature: Logs roll
  Scenario: A full segment rolls
    * a log
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("star-step", 3)])]
    #[case::journey_location("\
Feature: Logs roll
  @journey
  Scenario: A log lives a long life
    * a producer appends
", vec![("journey-location", 3)])]
    #[case::first_person_step("\
Feature: Logs roll
  Scenario: A full segment rolls
    When I'm appending past the segment size
    Then the log rolls a new segment
", vec![("first-person", 3)])]
    #[case::first_person_name("\
Feature: Logs roll
  Scenario: My segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("first-person", 2)])]
    #[case::or_in_step("\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends past the size OR the age
    Then the log rolls a new segment
", vec![("or-in-step", 3)])]
    #[case::quote_in_step("\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends to \"orders\"
    Then the log rolls a new segment
", vec![("quote-in-step", 3)])]
    #[case::mechanism_word("\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer calls append
    Then the log rolls a new segment
", vec![("mechanism-word", 3)])]
    #[case::unknown_tag("\
@done
Feature: Logs roll
  @use_mock_store
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("unknown-tag", 1), ("unknown-tag", 3)])]
    #[case::user_story_feature("\
Feature: As an operator I want logs to roll
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("user-story-feature", 1)])]
    #[case::placeholder_name("\
Feature: Logs roll
  Scenario: TC_12
    When a producer appends past the segment size
    Then the log rolls a new segment
  Scenario: Happy path
    When a producer appends past the segment age
    Then the log rolls a new segment
", vec![("placeholder-name", 2), ("placeholder-name", 5)])]
    #[case::case_column("\
Feature: Logs roll
  Scenario Outline: A full segment rolls
    When a producer appends <bytes> bytes
    Then the log rolls a new segment

    Examples:
      | bytes |
      | 1025  |
", vec![("case-column", 7)])]
    #[case::duplicate_example_row("\
Feature: Logs roll
  Scenario Outline: A full segment rolls
    When a producer appends <bytes> bytes
    Then the log rolls a new segment

    Examples:
      | case     | bytes |
      | over     | 1025  |

    Examples:
      | case     | bytes |
      # a comment inside the table
      | far over | 4096  |
      | over     | 1025  |
", vec![("duplicate-example-row", 14)])]
    #[case::ragged_long_row("\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends these batches
      | size | key \\| part |
      | 1025 | a    | b    |
    Then the log rolls a new segment
", vec![("ragged-table", 5)])]
    #[case::ragged_short_row("\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends these batches
      | size | key |
      | 1025 |
    Then the log rolls a new segment
", vec![("ragged-table", 5)])]
    #[case::duplicate_scenario_name("\
Feature: Logs roll
  Rule: Segments roll on size
    Scenario: A segment rolls
      When a producer appends past the segment size
      Then the log rolls a new segment
  Rule: Segments roll on age
    Scenario: A segment rolls
      When the segment passes its maximum age
      Then the log rolls a new segment
", vec![("duplicate-scenario-name", 7)])]
    #[case::scenario_outside_rule("\
Feature: Logs roll
  Scenario: An empty log does not roll
    When the segment passes its maximum age
    Then the log keeps its segment
  Rule: Segments roll on size
    Scenario: A full segment rolls
      When a producer appends past the segment size
      Then the log rolls a new segment
", vec![("scenario-outside-rule", 2)])]
    #[case::parse("\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends past the segment size
  Nonsense here
", vec![("parse", 4)])]
    fn each_rule_flags_its_violation(
        #[case] source: &str,
        #[case] expected: Vec<(&'static str, usize)>,
    ) {
        assert!(ids(PATH, source) == expected);
    }

    #[test]
    fn house_style_feature_is_clean() {
        let source = "\
@docker
Feature: Log segments roll on size and age

  Rule: A segment rolls when it is full

    Scenario: Append past the segment size rolls a new segment
      Given a topic with a segment size of 1024 bytes
      When a producer appends these batches
        | case      | size |
        | over size | 1025 |
      Then the log has two segments

    Scenario Outline: Append at the size boundary
      When a producer appends a batch of <size> bytes
      Then the log has <segments> segments

      Examples:
        | case        | size | segments |
        | under limit | 1023 | 1        |
        | at limit    | 1024 | 1        |

  Rule: A segment rolls when it is old

    @inprogress
    Scenario: An old segment rolls on the next append
      Given a segment older than log.roll.ms
      When a producer appends a batch
      Then the log has two segments
";
        assert!(lint(PATH, source) == Vec::new());
    }

    #[test]
    fn journey_feature_is_clean() {
        let source = "\
@journey
Feature: Consumer groups rebalance

  Scenario: A group resumes from committed offsets after a member leaves
    * a consumer group with two members
    * a member commits offset 40
    * the member leaves the group
    When the group rebalances
    * the remaining member takes over the partition
    When the remaining member fetches
    Then it resumes from offset 40
";
        assert!(lint(JOURNEY_PATH, source) == Vec::new());
    }

    #[test]
    fn finding_display_names_place_rule_and_fix() {
        let source = "\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer calls append
    Then the log rolls a new segment
";
        let rendered: Vec<String> = lint(PATH, source).iter().map(ToString::to_string).collect();
        let expected = vec![format!(
            "{PATH}:3:5: mechanism-word: step `a producer calls append` uses the mechanism word \
             `calls`. Fix: {}",
            LintRule::MechanismWord.hint()
        )];
        assert!(rendered == expected);
    }

    #[test]
    fn unknown_tag_points_at_the_tag() {
        let source = "\
Feature: Logs roll
  @inprogress @done
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
";
        let places: Vec<(usize, usize, String)> = lint(PATH, source)
            .into_iter()
            .map(|finding| (finding.line, finding.col, finding.message))
            .collect();
        let expected = vec![(
            2,
            15,
            "tag `@done` is not used, because the absence of a lifecycle tag means done".to_owned(),
        )];
        assert!(places == expected);
    }

    #[rstest]
    #[case("Test 3", true)]
    #[case("TC-12", true)]
    #[case("tc12", true)]
    #[case("Scenario_2", true)]
    #[case("HAPPY PATH", true)]
    #[case("Test", false)]
    #[case("Tests 3", false)]
    #[case("Test 3 rolls a segment", false)]
    fn placeholder_names(#[case] name: &str, #[case] expected: bool) {
        assert!(is_placeholder_name(name) == expected);
    }
}
