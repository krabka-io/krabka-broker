//! The house Gherkin style, checked over one feature file.
//!
//! Each rule here is one line of the [Gherkin style guide]. `scenarios!` runs
//! [`lint`] over every feature file it reads and turns each error
//! [`Finding`] into a compile error, so a feature that breaks the style does
//! not build. An advisory finding names a smell that a reader judges.
//!
//! [Gherkin style guide]: https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/gherkin_style_guide.md

use {
    crate::glossary::Glossary,
    gherkin::{
        Background, Examples, Feature, GherkinEnv, ParseError, Rule, Scenario, Step, StepType,
    },
    std::{
        collections::{HashMap, HashSet},
        fmt, iter,
    },
};

/// The tag that marks an end-to-end journey, which most scenario rules exempt.
const JOURNEY_TAG: &str = "journey";

/// The tag that marks a scenario as agreed but not automated.
const FORMULATED_TAG: &str = "formulated";

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

/// Imperative verbs that make a `Given` narrate setup instead of a state.
const IMPERATIVE_VERBS: [&str; 18] = [
    "create",
    "add",
    "set",
    "start",
    "send",
    "make",
    "open",
    "configure",
    "append",
    "produce",
    "write",
    "put",
    "delete",
    "register",
    "enable",
    "disable",
    "run",
    "call",
];

/// The description line that records an open question with its owner.
const QUESTION_PREFIX: &str = "Question:";

/// The description line that names a feature this one depends on.
const DEPENDS_ON_PREFIX: &str = "Depends on:";

/// The most steps an illustrative scenario may have.
const MAX_STEPS: usize = 5;

/// How much a finding blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Severity {
    /// A violation that fails the build.
    Error,
    /// A smell for the reader to judge; it does not fail the build.
    Advisory,
}

/// One house-style rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    RuleWithoutScenario,
    QuestionShape,
    OpenQuestionAutomated,
    AndInStep,
    ImperativeGiven,
    ShouldInThen,
    CaseCell,
    EquivalentExampleRow,
    UnknownPlaceholder,
    DescriptionSmell,
    TagCaseDuplicate,
    TicketTag,
    DependsOn,
}

impl LintRule {
    /// Every rule, in the order the style guide lists them.
    pub const ALL: [Self; 31] = [
        Self::Parse,
        Self::NoBackground,
        Self::OneWhen,
        Self::MaxSteps,
        Self::StarStep,
        Self::JourneyLocation,
        Self::FirstPerson,
        Self::OrInStep,
        Self::QuoteInStep,
        Self::MechanismWord,
        Self::UnknownTag,
        Self::UserStoryFeature,
        Self::PlaceholderName,
        Self::CaseColumn,
        Self::DuplicateExampleRow,
        Self::RaggedTable,
        Self::DuplicateScenarioName,
        Self::ScenarioOutsideRule,
        Self::RuleWithoutScenario,
        Self::QuestionShape,
        Self::OpenQuestionAutomated,
        Self::AndInStep,
        Self::ImperativeGiven,
        Self::ShouldInThen,
        Self::CaseCell,
        Self::EquivalentExampleRow,
        Self::UnknownPlaceholder,
        Self::DescriptionSmell,
        Self::TagCaseDuplicate,
        Self::TicketTag,
        Self::DependsOn,
    ];

    /// The kebab-case id a finding names the rule by.
    #[must_use]
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
            Self::RuleWithoutScenario => "rule-without-scenario",
            Self::QuestionShape => "question-shape",
            Self::OpenQuestionAutomated => "open-question-automated",
            Self::AndInStep => "and-in-step",
            Self::ImperativeGiven => "imperative-given",
            Self::ShouldInThen => "should-in-then",
            Self::CaseCell => "case-cell",
            Self::EquivalentExampleRow => "equivalent-example-row",
            Self::UnknownPlaceholder => "unknown-placeholder",
            Self::DescriptionSmell => "description-smell",
            Self::TagCaseDuplicate => "tag-case-duplicate",
            Self::TicketTag => "ticket-tag",
            Self::DependsOn => "depends-on",
        }
    }

    /// The rule whose [`id`](Self::id) is `id`.
    #[must_use]
    pub fn from_id(id: &str) -> Option<Self> { Self::ALL.into_iter().find(|rule| rule.id() == id) }

    /// Whether a violation of the rule fails the build.
    #[must_use]
    pub const fn severity(self) -> Severity {
        match self {
            Self::AndInStep | Self::DescriptionSmell => Severity::Advisory,
            _ => Severity::Error,
        }
    }

    /// A short imperative fix for a violation of the rule.
    #[must_use]
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
                "use a tag from crates/bdd-model/glossary.toml, or add it there with its meaning"
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
            Self::RuleWithoutScenario => {
                "add a scenario that illustrates the rule, or delete the rule"
            }
            Self::QuestionShape => "write the question as `Question: <text> (owner: <owner>)`",
            Self::OpenQuestionAutomated => {
                "tag the scenario @formulated until the question is answered, or delete the \
                 answered question"
            }
            Self::AndInStep => {
                "check that each part is essential; move context into its own Given, or name the \
                 combined outcome"
            }
            Self::ImperativeGiven => {
                "describe the state the action left, in the past tense or the passive, such as \
                 `a topic has been created`"
            }
            Self::ShouldInThen => {
                "state the outcome as a present-tense fact, such as `the leader returns ...`"
            }
            Self::CaseCell => {
                "say in each case cell, in words and once per outline, why the row exists"
            }
            Self::EquivalentExampleRow => {
                "delete the row, or change a value so it teaches something different"
            }
            Self::UnknownPlaceholder => {
                "add the column to every Examples table, or correct the placeholder name"
            }
            Self::DescriptionSmell => {
                "state the rule in a `Rule:` name or a scenario instead; a description holds only \
                 `Question:` and `Depends on:` lines"
            }
            Self::TagCaseDuplicate => "spell the tag exactly as the glossary does",
            Self::TicketTag => "remove the ticket id; link the ticket from the pull request",
            Self::DependsOn => {
                "write `Depends on: <name> - <relative path>` with a path, relative to this \
                 feature file, that exists"
            }
        }
    }
}

/// One house-style violation at a place in a feature file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub rule: LintRule,
    /// The rule's severity.
    pub severity: Severity,
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
        let advisory = match self.severity {
            Severity::Error => "",
            Severity::Advisory => " (advisory)",
        };
        write!(
            formatter,
            "{}:{}:{}: {}{advisory}: {}. Fix: {}",
            self.path,
            self.line,
            self.col,
            self.rule.id(),
            self.message,
            self.rule.hint()
        )
    }
}

/// What the lint needs beyond the feature file itself.
pub struct LintContext<'c> {
    /// The tags a feature may use.
    pub glossary: &'c Glossary,
    /// Reports whether a manifest-relative path exists. The lint resolves a
    /// `Depends on:` path against the feature file's directory, `.` and `..`
    /// included, before it asks.
    pub file_exists: &'c dyn Fn(&str) -> bool,
}

/// Every house-style violation in one feature file, in file order.
///
/// `path` is the manifest-relative path with `/` separators. A file that does
/// not parse yields its ragged tables, which gherkin refuses to parse, or else
/// one [`LintRule::Parse`] finding.
#[must_use]
pub fn lint(path: &str, source: &str, context: &LintContext<'_>) -> Vec<Finding> {
    let mut linter = Linter {
        path,
        context,
        lines: source.lines().collect(),
        tag_spellings: HashMap::new(),
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

/// One non-empty line of a feature or rule description.
struct DescriptionLine {
    line: usize,
    col: usize,
    text: String,
}

/// Where a tag was first written, and how.
#[derive(Clone)]
struct TagSpelling {
    tag: String,
    line: usize,
}

struct Linter<'s> {
    path: &'s str,
    context: &'s LintContext<'s>,
    lines: Vec<&'s str>,
    /// The first spelling of each tag in the file, by its lowercase form.
    tag_spellings: HashMap<String, TagSpelling>,
    findings: Vec<Finding>,
}

impl Linter<'_> {
    fn report(&mut self, rule: LintRule, line: usize, col: usize, message: String) {
        self.findings.push(Finding {
            rule,
            severity: rule.severity(),
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
        let feature_question = self.description(feature.description.as_deref(), line);
        self.background(feature.background.as_ref(), "the feature");
        let has_rules = !feature.rules.is_empty();
        for scenario in &feature.scenarios {
            let open = feature_question.then_some("its feature");
            let journey = self.scenario(scenario, &feature.tags, open);
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
            self.rule(rule, &feature.tags, feature_question);
        }
        self.duplicate_names(feature);
    }

    fn rule(&mut self, rule: &Rule, feature_tags: &[String], feature_question: bool) {
        let position = rule.position;
        self.tags(&rule.tags, position.line);
        let rule_question = self.description(rule.description.as_deref(), position.line);
        self.background(rule.background.as_ref(), &format!("rule `{}`", rule.name));
        if rule.scenarios.is_empty() {
            let message = format!("rule `{}` has no scenario", rule.name);
            self.report(
                LintRule::RuleWithoutScenario,
                position.line,
                position.col,
                message,
            );
        }
        let open = if rule_question {
            Some("its rule")
        } else {
            feature_question.then_some("its feature")
        };
        let tags: Vec<String> = feature_tags.iter().chain(&rule.tags).cloned().collect();
        for scenario in &rule.scenarios {
            self.scenario(scenario, &tags, open);
        }
    }

    /// Lints the description of the feature or rule on `node_line` and reports
    /// whether it holds a question.
    fn description(&mut self, description: Option<&str>, node_line: usize) -> bool {
        let mut question = false;
        for line in self.description_lines(description, node_line) {
            if let Some(rest) = line.text.strip_prefix(QUESTION_PREFIX) {
                question = true;
                if !is_question(rest) {
                    let message = format!(
                        "question `{}` does not name its owner as `(owner: <owner>)`",
                        line.text
                    );
                    self.report(LintRule::QuestionShape, line.line, line.col, message);
                }
            } else if let Some(rest) = line.text.strip_prefix(DEPENDS_ON_PREFIX) {
                self.depends_on(&line, rest);
            } else {
                let message = format!(
                    "description line `{}` is neither a question nor a dependency",
                    line.text
                );
                self.report(LintRule::DescriptionSmell, line.line, line.col, message);
            }
        }
        question
    }

    fn depends_on(&mut self, line: &DescriptionLine, rest: &str) {
        let parts = rest
            .trim()
            .rsplit_once(" - ")
            .map(|(name, path)| (name.trim(), path.trim()))
            .filter(|(name, path)| !name.is_empty() && !path.is_empty());
        let message = match parts {
            None => format!(
                "dependency `{}` is not written as `Depends on: <name> - <relative path>`",
                line.text
            ),
            Some((name, relative)) => {
                let resolved = resolve(self.path, relative);
                if (self.context.file_exists)(&resolved) {
                    return;
                }
                format!("dependency `{name}` at `{relative}` does not exist, as `{resolved}`")
            }
        };
        self.report(LintRule::DependsOn, line.line, line.col, message);
    }

    /// The non-empty lines of a description with their places in the source.
    fn description_lines(
        &self,
        description: Option<&str>,
        node_line: usize,
    ) -> Vec<DescriptionLine> {
        let mut found = Vec::new();
        // Line `node_line + 1` is at index `node_line`.
        let mut next = node_line;
        for text in description
            .into_iter()
            .flat_map(str::lines)
            .map(str::trim)
            .filter(|text| !text.is_empty())
        {
            let place = self
                .lines
                .iter()
                .enumerate()
                .skip(next)
                .find(|(_, line)| line.trim() == text);
            let (line, col) = match place {
                Some((index, source)) => {
                    next = index + 1;
                    (index + 1, indent(source) + 1)
                }
                None => (node_line, 1),
            };
            found.push(DescriptionLine {
                line,
                col,
                text: text.to_owned(),
            });
        }
        found
    }

    fn background(&mut self, background: Option<&Background>, owner: &str) {
        if let Some(background) = background {
            let message = format!("{owner} has a `Background`");
            let position = background.position;
            self.report(LintRule::NoBackground, position.line, position.col, message);
        }
    }

    /// Lints one scenario and reports whether it is a journey. `open_question`
    /// names the feature or rule that holds an open question, if one does.
    fn scenario(
        &mut self,
        scenario: &Scenario,
        inherited_tags: &[String],
        open_question: Option<&str>,
    ) -> bool {
        let has_tag = |name: &str| {
            inherited_tags
                .iter()
                .chain(&scenario.tags)
                .any(|tag| tag == name)
        };
        let journey = has_tag(JOURNEY_TAG);
        let formulated = has_tag(FORMULATED_TAG)
            || (!scenario.examples.is_empty()
                && scenario
                    .examples
                    .iter()
                    .all(|examples| examples.tags.iter().any(|tag| tag == FORMULATED_TAG)));
        self.tags(&scenario.tags, scenario.position.line);
        self.scenario_name(scenario);
        if let Some(owner) = open_question.filter(|_| !formulated) {
            let message = format!(
                "scenario `{}` is automated while {owner} has an open question",
                scenario.name
            );
            let position = scenario.position;
            self.report(
                LintRule::OpenQuestionAutomated,
                position.line,
                position.col,
                message,
            );
        }
        if journey {
            self.journey_location(scenario);
        } else {
            self.scenario_shape(scenario);
        }
        for step in &scenario.steps {
            self.step(step, journey);
        }
        self.examples(scenario);
        self.placeholders(scenario);
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
        let star = step.keyword.trim() == "*";
        if !journey && star {
            let message = format!("step `* {text}` uses `*` outside a @journey scenario");
            self.report(LintRule::StarStep, line, col, message);
        }
        if let Some(word) = first_person_word(text) {
            let message = format!("step `{text}` uses the first-person word `{word}`");
            self.report(LintRule::FirstPerson, line, col, message);
        }
        if let Some(word) = find_word(text, &["or"]) {
            let message = format!("step `{text}` offers alternatives with `{word}`");
            self.report(LintRule::OrInStep, line, col, message);
        }
        if let Some(word) = find_word(text, &["and"]) {
            let message = format!("step `{text}` joins two parts with `{word}`");
            self.report(LintRule::AndInStep, line, col, message);
        }
        if text.contains('"') {
            let message = format!("step `{text}` quotes a value");
            self.report(LintRule::QuoteInStep, line, col, message);
        }
        if let Some(word) = find_word(text, &MECHANISM_WORDS) {
            let message = format!("step `{text}` uses the mechanism word `{word}`");
            self.report(LintRule::MechanismWord, line, col, message);
        }
        if !star && step.ty == StepType::Given {
            let first = words(text).next();
            if let Some(verb) = first.filter(|word| contains_word(&IMPERATIVE_VERBS, word)) {
                let message = format!("given `{text}` starts with the imperative verb `{verb}`");
                self.report(LintRule::ImperativeGiven, line, col, message);
            }
        }
        if !star
            && step.ty == StepType::Then
            && let Some(word) = find_word(text, &["should"])
        {
            let message = format!("then `{text}` hedges the outcome with `{word}`");
            self.report(LintRule::ShouldInThen, line, col, message);
        }
    }

    fn examples(&mut self, scenario: &Scenario) {
        let mut exact: HashSet<(&[String], &[String])> = HashSet::new();
        let mut equivalent: HashSet<(Vec<&str>, Vec<&str>)> = HashSet::new();
        let mut cases: HashSet<&str> = HashSet::new();
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
            let case_index = header.iter().position(|cell| cell == "case");
            for (index, row) in rows.iter().enumerate() {
                let duplicate = !exact.insert((header.as_slice(), row.as_slice()));
                let equivalent_row = case_index.is_some_and(|case| {
                    !equivalent.insert((without(header, case), without(row, case)))
                });
                let (line, col) = self.row_position(examples, index);
                let shown = row.join(" | ");
                if duplicate {
                    let message = format!("example row `| {shown} |` repeats an earlier row");
                    self.report(LintRule::DuplicateExampleRow, line, col, message);
                } else if equivalent_row {
                    let message = format!(
                        "example row `| {shown} |` equals an earlier row apart from its case"
                    );
                    self.report(LintRule::EquivalentExampleRow, line, col, message);
                }
                let Some(case) = case_index.and_then(|case| row.get(case)) else {
                    continue;
                };
                let message = if case.is_empty() {
                    format!("example row `| {shown} |` has an empty case")
                } else if case.chars().all(|character| character.is_ascii_digit()) {
                    format!("case `{case}` is a number, not a reason for the row")
                } else if !cases.insert(case) && !duplicate {
                    format!("case `{case}` names an earlier row of the outline too")
                } else {
                    continue;
                };
                self.report(LintRule::CaseCell, line, col, message);
            }
        }
    }

    /// The place of data row `index` of an Examples table.
    fn row_position(&self, examples: &Examples, index: usize) -> (usize, usize) {
        let start = examples
            .table
            .as_ref()
            .map_or(examples.position, |table| table.position);
        let raw = self.raw_rows(start.line);
        raw.get(index + 1)
            .map_or((start.line, start.col), |row| (row.line, row.col))
    }

    /// Reports each `<name>` in an outline step that some Examples table lacks.
    fn placeholders(&mut self, scenario: &Scenario) {
        let headers: Vec<(usize, &[String])> = scenario
            .examples
            .iter()
            .filter_map(|examples| {
                let table = examples.table.as_ref()?;
                let header = table.rows.first()?;
                Some((table.position.line, header.as_slice()))
            })
            .collect();
        if headers.is_empty() {
            return;
        }
        for step in &scenario.steps {
            let mut names: Vec<&str> = Vec::new();
            let table_cells = step
                .table
                .iter()
                .flat_map(|table| table.rows.iter().flatten());
            for text in iter::once(&step.value)
                .chain(&step.docstring)
                .chain(table_cells)
            {
                for name in placeholder_names(text) {
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
            }
            for name in names {
                let missing = headers
                    .iter()
                    .find(|(_, header)| !header.iter().any(|column| column == name));
                if let Some((table_line, _)) = missing {
                    let message = format!(
                        "step `{}` uses `<{name}>`, which the Examples table on line \
                         {table_line} has no column for",
                        step.value
                    );
                    let position = step.position;
                    self.report(
                        LintRule::UnknownPlaceholder,
                        position.line,
                        position.col,
                        message,
                    );
                }
            }
        }
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
            let (line, col) = self.tag_position(tag, node_line);
            let earlier = self.earlier_spelling(tag, line);
            let glossary = self.context.glossary;
            if glossary.has_tag(tag) {
                continue;
            }
            let defined = glossary
                .tags
                .keys()
                .find(|defined| defined.eq_ignore_ascii_case(tag));
            let (rule, message) = if tag == "done" {
                (
                    LintRule::UnknownTag,
                    "tag `@done` is not used, because the absence of a lifecycle tag means done"
                        .to_owned(),
                )
            } else if let Some(defined) = defined {
                (
                    LintRule::TagCaseDuplicate,
                    format!("tag `@{tag}` differs only in case from the glossary tag `@{defined}`"),
                )
            } else if is_ticket(tag) {
                (
                    LintRule::TicketTag,
                    format!("tag `@{tag}` looks like a ticket id"),
                )
            } else {
                (
                    LintRule::UnknownTag,
                    format!(
                        "tag `@{tag}` is not in the glossary; adding a tag means adding it, with \
                         its meaning, to crates/bdd-model/glossary.toml"
                    ),
                )
            };
            self.report(rule, line, col, message);
            if rule == LintRule::UnknownTag
                && let Some(earlier) = earlier
            {
                let message = format!(
                    "tag `@{tag}` differs only in case from `@{}` on line {}",
                    earlier.tag, earlier.line
                );
                self.report(LintRule::TagCaseDuplicate, line, col, message);
            }
        }
    }

    /// The first spelling of `tag` in the file, when it differs from `tag`
    /// only in case. Records `tag` when it is the first.
    fn earlier_spelling(&mut self, tag: &str, line: usize) -> Option<TagSpelling> {
        let first = self
            .tag_spellings
            .entry(tag.to_lowercase())
            .or_insert_with(|| TagSpelling {
                tag: tag.to_owned(),
                line,
            });
        (first.tag != tag).then(|| first.clone())
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
                    col: indent(line) + 1,
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

/// The number of characters before the first non-whitespace one.
fn indent(line: &str) -> usize { line.chars().count() - line.trim_start().chars().count() }

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

/// Reports whether `word` is one of `list`, ignoring ASCII case.
fn contains_word(list: &[&str], word: &str) -> bool {
    list.iter().any(|listed| word.eq_ignore_ascii_case(listed))
}

/// The first word of `text` that is one of `list`, ignoring ASCII case.
fn find_word<'t>(text: &'t str, list: &[&str]) -> Option<&'t str> {
    words(text).find(|word| contains_word(list, word))
}

/// `cells` without the one at `skip`.
fn without(cells: &[String], skip: usize) -> Vec<&str> {
    cells
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != skip)
        .map(|(_, cell)| cell.as_str())
        .collect()
}

/// Every `<name>` in `text`: a name that is not empty, holds no `<`, and
/// neither starts nor ends with whitespace.
fn placeholder_names(text: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut rest = text;
    while let Some((_, after)) = rest.split_once('<') {
        rest = after;
        let Some((name, tail)) = after.split_once('>') else {
            break;
        };
        let bounded = name
            .chars()
            .next()
            .is_some_and(|first| !first.is_whitespace())
            && name
                .chars()
                .next_back()
                .is_some_and(|last| !last.is_whitespace());
        if bounded && !name.contains('<') {
            names.push(name);
            rest = tail;
        }
    }
    names
}

/// Reports whether the text after `Question:` reads `<text> (owner: <owner>)`.
fn is_question(rest: &str) -> bool {
    rest.trim()
        .strip_suffix(')')
        .and_then(|body| body.rsplit_once("(owner:"))
        .is_some_and(|(text, owner)| !text.trim().is_empty() && !owner.trim().is_empty())
}

/// Reports whether `tag` looks like a ticket id: letters, then an optional
/// `-` or `_`, then digits.
fn is_ticket(tag: &str) -> bool {
    let digits = tag.trim_start_matches(|character: char| character.is_ascii_alphabetic());
    if digits.len() == tag.len() {
        return false;
    }
    let digits = digits.strip_prefix(['-', '_']).unwrap_or(digits);
    !digits.is_empty() && digits.chars().all(|character| character.is_ascii_digit())
}

/// `relative`, resolved against the directory of the feature at `path`.
fn resolve(path: &str, relative: &str) -> String {
    let mut parts: Vec<&str> = path.split('/').collect();
    parts.pop();
    for part in relative.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|last| *last != "..") {
                    parts.pop();
                } else {
                    parts.push("..");
                }
            }
            other => parts.push(other),
        }
    }
    parts.join("/")
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

    const EXISTING: &str = "tests/features/retention/retention.feature";

    fn exists(path: &str) -> bool { path == EXISTING }

    fn run(path: &str, source: &str) -> Vec<Finding> {
        let glossary = Glossary::embedded().unwrap();
        let context = LintContext {
            glossary,
            file_exists: &exists,
        };
        lint(path, source, &context)
    }

    fn ids(path: &str, source: &str) -> Vec<(&'static str, usize)> {
        run(path, source)
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
    #[case::rule_without_scenario("\
Feature: Logs roll
  Rule: Segments roll on size
", vec![("rule-without-scenario", 2)])]
    #[case::question_shape("\
Feature: Logs roll
  Question: Does an empty append roll a segment?

  @formulated
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("question-shape", 2)])]
    #[case::open_question_feature("\
Feature: Logs roll
  Question: Does an empty append roll a segment? (owner: the log team)

  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("open-question-automated", 4)])]
    #[case::open_question_rule("\
Feature: Logs roll
  Rule: Segments roll on size
    Question: Does an empty append roll a segment? (owner: the log team)

    Scenario Outline: A full segment rolls
      When a producer appends <bytes> bytes
      Then the log rolls a new segment

      @formulated
      Examples:
        | case | bytes |
        | over | 1025  |

      Examples:
        | case     | bytes |
        | far over | 4096  |
", vec![("open-question-automated", 5)])]
    #[case::and_in_step("\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends a batch and a marker
    Then the log rolls a new segment
", vec![("and-in-step", 3)])]
    #[case::imperative_given("\
Feature: Logs roll
  Scenario: A full segment rolls
    Given add a full segment to the log
    When a producer appends a batch
    Then the log rolls a new segment
", vec![("imperative-given", 3)])]
    #[case::should_in_then("\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log should roll a new segment
", vec![("should-in-then", 4)])]
    #[case::case_cell("\
Feature: Logs roll
  Scenario Outline: A full segment rolls
    When a producer appends <bytes> bytes
    Then the log rolls a new segment

    Examples:
      | case | bytes |
      | 1    | 1025  |
      |      | 2048  |
      | over | 4096  |
      | over | 8192  |
", vec![("case-cell", 8), ("case-cell", 9), ("case-cell", 11)])]
    #[case::equivalent_example_row("\
Feature: Logs roll
  Scenario Outline: A full segment rolls
    When a producer appends <bytes> bytes
    Then the log rolls a new segment

    Examples:
      | case      | bytes |
      | over      | 1025  |
      | just over | 1025  |
", vec![("equivalent-example-row", 9)])]
    #[case::unknown_placeholder("\
Feature: Logs roll
  Scenario Outline: A full segment rolls
    When a producer appends <bytes> bytes
    Then the log has <segment> segments

    Examples:
      | case | bytes |
      | over | 1025  |
", vec![("unknown-placeholder", 4)])]
    #[case::unknown_placeholder_in_table_and_docstring("\
Feature: Logs roll
  Scenario Outline: A full segment rolls
    When a producer appends these batches
      | size    |
      | <bytes> |
    Then the log reports
      \"\"\"
      <segment> segments
      \"\"\"

    Examples:
      | case | bytes |
      | over | 1025  |
", vec![("unknown-placeholder", 6)])]
    #[case::description_smell("\
Feature: Logs roll
  Segments roll so that retention can delete whole files.

  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("description-smell", 2)])]
    #[case::tag_case_glossary("\
@Docker
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("tag-case-duplicate", 1)])]
    #[case::tag_case_in_file("\
Feature: Logs roll
  @Slow
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
  @slow
  Scenario: An old segment rolls
    When the segment passes its maximum age
    Then the log rolls a new segment
", vec![
        ("unknown-tag", 2),
        ("unknown-tag", 6),
        ("tag-case-duplicate", 6),
    ])]
    #[case::ticket_tag("\
@JIRA-12
Feature: Logs roll
  @gh123 @ABC_3
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("ticket-tag", 1), ("ticket-tag", 3), ("ticket-tag", 3)])]
    #[case::depends_on("\
Feature: Logs roll
  Depends on: Log retention - ../retention/retention.feature
  Depends on: Log compaction - ../compaction/compaction.feature
  Depends on: Log compaction

  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![("depends-on", 3), ("depends-on", 4)])]
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
  Depends on: Log retention - ../retention/retention.feature

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

  Rule: An empty append does not roll
    Question: Does an empty batch count as an append? (owner: the log team)

    @formulated
    Scenario: An empty batch leaves a full segment in place
      Given a full segment
      When a producer appends an empty batch
      Then the log has one segment
";
        assert!(run(PATH, source) == Vec::new());
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
        assert!(run(JOURNEY_PATH, source) == Vec::new());
    }

    #[test]
    fn finding_display_names_place_rule_and_fix() {
        let source = "\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer calls append
    Then the log rolls a new segment
";
        let rendered: Vec<String> = run(PATH, source).iter().map(ToString::to_string).collect();
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
        let places: Vec<(usize, usize, String)> = run(PATH, source)
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

    #[test]
    fn findings_carry_their_rule_severity() {
        let source = "\
Feature: Logs roll
  Segments roll so that retention can delete whole files.

  Scenario: A full segment rolls
    When a producer appends a batch and a marker
    Then the log should roll a new segment
";
        let actual: Vec<(&str, Severity)> = run(PATH, source)
            .into_iter()
            .map(|finding| (finding.rule.id(), finding.severity))
            .collect();
        let expected = vec![
            ("description-smell", Severity::Advisory),
            ("and-in-step", Severity::Advisory),
            ("should-in-then", Severity::Error),
        ];
        assert!(actual == expected);
    }

    #[test]
    fn only_the_advisory_rules_are_advisory() {
        let advisory: Vec<&str> = LintRule::ALL
            .into_iter()
            .filter(|rule| rule.severity() == Severity::Advisory)
            .map(LintRule::id)
            .collect();
        assert!(advisory == vec!["and-in-step", "description-smell"]);
    }

    #[test]
    fn every_rule_id_reads_back() {
        for rule in LintRule::ALL {
            assert!(LintRule::from_id(rule.id()) == Some(rule));
        }
        assert!(LintRule::from_id("no-such-rule").is_none());
    }

    #[test]
    fn advisory_display_marks_the_rule() {
        let source = "\
Feature: Logs roll
  Scenario: A full segment rolls
    When a producer appends a batch and a marker
    Then the log rolls a new segment
";
        let rendered: Vec<String> = run(PATH, source).iter().map(ToString::to_string).collect();
        let expected = vec![format!(
            "{PATH}:3:5: and-in-step (advisory): step `a producer appends a batch and a marker` \
             joins two parts with `and`. Fix: {}",
            LintRule::AndInStep.hint()
        )];
        assert!(rendered == expected);
    }

    #[rstest]
    #[case::depends_on_missing("\
Feature: Logs roll
  Depends on: Log compaction - ../compaction/./compaction.feature

  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![(
        2,
        3,
        "dependency `Log compaction` at `../compaction/./compaction.feature` does not exist, as \
         `tests/features/compaction/compaction.feature`",
    )])]
    #[case::tag_case_glossary("\
Feature: Logs roll
  @Docker
  Scenario: A full segment rolls
    When a producer appends past the segment size
    Then the log rolls a new segment
", vec![(2, 3, "tag `@Docker` differs only in case from the glossary tag `@docker`")])]
    #[case::case_cell_number("\
Feature: Logs roll
  Scenario Outline: A full segment rolls
    When a producer appends <bytes> bytes
    Then the log rolls a new segment

    Examples:
      | case | bytes |
      | 1    | 1025  |
", vec![(8, 7, "case `1` is a number, not a reason for the row")])]
    fn findings_name_the_place_and_the_cause(
        #[case] source: &str,
        #[case] expected: Vec<(usize, usize, &str)>,
    ) {
        let actual: Vec<(usize, usize, String)> = run(PATH, source)
            .into_iter()
            .map(|finding| (finding.line, finding.col, finding.message))
            .collect();
        let expected: Vec<(usize, usize, String)> = expected
            .into_iter()
            .map(|(line, col, message)| (line, col, message.to_owned()))
            .collect();
        assert!(actual == expected);
    }

    #[rstest]
    #[case("JIRA-12", true)]
    #[case("ABC_3", true)]
    #[case("gh123", true)]
    #[case("docker", false)]
    #[case("allow_skipped", false)]
    #[case("123", false)]
    #[case("JIRA-", false)]
    #[case("kip-848-group", false)]
    fn ticket_shapes(#[case] tag: &str, #[case] expected: bool) {
        assert!(is_ticket(tag) == expected);
    }

    #[rstest]
    #[case(
        "tests/features/log/log.feature",
        "../retention/r.feature",
        "tests/features/retention/r.feature"
    )]
    #[case(
        "tests/features/log/log.feature",
        "./other.feature",
        "tests/features/log/other.feature"
    )]
    #[case("log.feature", "../up.feature", "../up.feature")]
    fn dependency_paths_resolve_against_the_feature_directory(
        #[case] path: &str,
        #[case] relative: &str,
        #[case] expected: &str,
    ) {
        assert!(resolve(path, relative) == expected);
    }

    #[rstest]
    #[case("Why? (owner: the log team)", true)]
    #[case("Why?", false)]
    #[case("(owner: the log team)", false)]
    #[case("Why? (owner: )", false)]
    fn question_shapes(#[case] rest: &str, #[case] expected: bool) {
        assert!(is_question(rest) == expected);
    }
}
