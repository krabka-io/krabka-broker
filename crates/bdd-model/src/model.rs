//! The steps a run of a feature file executes.
//!
//! `scenarios!` checks every step a run reaches against the step registry, so
//! it needs the concrete step texts: outline placeholders substituted per
//! Examples row, and scenarios left out by the tag filters dropped.

use {
    gherkin::{Background, Examples, Feature, Scenario, Step, StepType, tagexpr::TagOperation},
    std::collections::{HashMap, HashSet},
};

/// The resolved keyword of a step. `And`, `But` and `*` take the keyword
/// before them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keyword {
    Given,
    When,
    Then,
}

impl From<StepType> for Keyword {
    fn from(ty: StepType) -> Self {
        match ty {
            StepType::Given => Self::Given,
            StepType::When => Self::When,
            StepType::Then => Self::Then,
        }
    }
}

/// One step text that a run executes, with the place it first appears.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepUse {
    /// `path:line` of the step in the feature file.
    pub location: String,
    pub keyword: Keyword,
    /// The step text, with outline placeholders substituted.
    pub text: String,
}

/// Parses a Cucumber tag expression such as `@a and not @b`.
///
/// # Errors
///
/// Returns a readable message when the expression does not parse.
pub fn parse_tag_expression(expression: &str) -> Result<TagOperation, String> {
    expression
        .parse::<TagOperation>()
        .map_err(|error| format!("`{expression}` is not a valid tag expression: {error}"))
}

/// Reports whether `tags`, written without their `@`, satisfy `operation`.
pub fn tags_match(operation: &TagOperation, tags: &[String]) -> bool {
    match operation {
        TagOperation::And(left, right) => tags_match(left, tags) && tags_match(right, tags),
        TagOperation::Or(left, right) => tags_match(left, tags) || tags_match(right, tags),
        TagOperation::Not(inner) => !tags_match(inner, tags),
        TagOperation::Tag(tag) => tags.iter().any(|candidate| candidate == tag),
    }
}

/// Every step a run of `feature` executes, outline placeholders substituted per
/// Examples row.
///
/// A scenario, or one Examples block of an outline, runs when `include` is
/// absent or matches its tags and `ignore` is absent or does not match them.
/// Its tags are those of the feature, its rule, itself and, for an outline,
/// the Examples block. Each distinct keyword and text appears once, at its
/// first place in file order.
pub fn step_uses(
    path: &str,
    feature: &Feature,
    include: Option<&TagOperation>,
    ignore: Option<&TagOperation>,
) -> Vec<StepUse> {
    let filter = Filter { include, ignore };
    let mut collector = Collector {
        path,
        seen: HashSet::new(),
        uses: Vec::new(),
    };
    for placed in placed_scenarios(feature) {
        collector.scenario(&placed, &filter);
    }
    collector.uses
}

/// A scenario with the tags and background it inherits from its feature and
/// rule.
struct PlacedScenario<'f> {
    scenario: &'f Scenario,
    inherited_tags: Vec<String>,
    backgrounds: Vec<&'f Background>,
}

fn placed_scenarios(feature: &Feature) -> Vec<PlacedScenario<'_>> {
    let feature_background: Vec<&Background> = feature.background.iter().collect();
    let mut placed: Vec<PlacedScenario<'_>> = feature
        .scenarios
        .iter()
        .map(|scenario| PlacedScenario {
            scenario,
            inherited_tags: feature.tags.clone(),
            backgrounds: feature_background.clone(),
        })
        .collect();
    for rule in &feature.rules {
        let tags: Vec<String> = feature.tags.iter().chain(&rule.tags).cloned().collect();
        let backgrounds: Vec<&Background> = feature_background
            .iter()
            .copied()
            .chain(&rule.background)
            .collect();
        placed.extend(rule.scenarios.iter().map(|scenario| PlacedScenario {
            scenario,
            inherited_tags: tags.clone(),
            backgrounds: backgrounds.clone(),
        }));
    }
    placed.sort_by_key(|placed| placed.scenario.position.line);
    placed
}

struct Filter<'t> {
    include: Option<&'t TagOperation>,
    ignore: Option<&'t TagOperation>,
}

impl Filter<'_> {
    fn keeps(&self, tags: &[String]) -> bool {
        self.include.is_none_or(|include| tags_match(include, tags))
            && self.ignore.is_none_or(|ignore| !tags_match(ignore, tags))
    }
}

struct Collector<'p> {
    path: &'p str,
    seen: HashSet<(Keyword, String)>,
    uses: Vec<StepUse>,
}

impl Collector<'_> {
    fn scenario(&mut self, placed: &PlacedScenario<'_>, filter: &Filter<'_>) {
        let scenario = placed.scenario;
        let tags: Vec<String> = placed
            .inherited_tags
            .iter()
            .chain(&scenario.tags)
            .cloned()
            .collect();
        if scenario.examples.is_empty() {
            if filter.keeps(&tags) {
                self.backgrounds(&placed.backgrounds);
                for step in &scenario.steps {
                    self.push(step, step.value.clone());
                }
            }
            return;
        }
        for examples in &scenario.examples {
            let examples_tags: Vec<String> = tags.iter().chain(&examples.tags).cloned().collect();
            if filter.keeps(&examples_tags) {
                self.backgrounds(&placed.backgrounds);
                self.outline_rows(&scenario.steps, examples);
            }
        }
    }

    fn backgrounds(&mut self, backgrounds: &[&Background]) {
        for step in backgrounds.iter().flat_map(|background| &background.steps) {
            self.push(step, step.value.clone());
        }
    }

    fn outline_rows(&mut self, steps: &[Step], examples: &Examples) {
        let Some((header, rows)) = examples
            .table
            .as_ref()
            .and_then(|table| table.rows.split_first())
        else {
            return;
        };
        for row in rows {
            let values: HashMap<&str, &str> = header
                .iter()
                .map(String::as_str)
                .zip(row.iter().map(String::as_str))
                .collect();
            for step in steps {
                self.push(step, substitute(&step.value, &values));
            }
        }
    }

    fn push(&mut self, step: &Step, text: String) {
        let keyword = Keyword::from(step.ty);
        if self.seen.insert((keyword, text.clone())) {
            self.uses.push(StepUse {
                location: format!("{}:{}", self.path, step.position.line),
                keyword,
                text,
            });
        }
    }
}

/// Replaces each `<name>` in `text` whose name is a column of the row. Other
/// angle-bracketed text stays as it is.
fn substitute(text: &str, values: &HashMap<&str, &str>) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some((before, after)) = rest.split_once('<') {
        out.push_str(before);
        let replacement = after
            .split_once('>')
            .and_then(|(name, tail)| values.get(name).map(|value| (*value, tail)));
        if let Some((value, tail)) = replacement {
            out.push_str(value);
            rest = tail;
        } else {
            out.push('<');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, gherkin::GherkinEnv, rstest::rstest};

    const PATH: &str = "tests/features/log/log.feature";

    fn feature(source: &str) -> Feature {
        Feature::parse(source, GherkinEnv::default()).expect("the test feature parses")
    }

    fn uses(source: &str, include: Option<&str>, ignore: Option<&str>) -> Vec<(Keyword, String)> {
        let include = include.map(|expression| parse_tag_expression(expression).unwrap());
        let ignore = ignore.map(|expression| parse_tag_expression(expression).unwrap());
        step_uses(PATH, &feature(source), include.as_ref(), ignore.as_ref())
            .into_iter()
            .map(|step_use| (step_use.keyword, step_use.text))
            .collect()
    }

    fn texts(expected: &[(Keyword, &str)]) -> Vec<(Keyword, String)> {
        expected
            .iter()
            .map(|(keyword, text)| (*keyword, (*text).to_owned()))
            .collect()
    }

    const OUTLINE: &str = "\
Feature: Fetch offsets

  Scenario Outline: A fetch outside the log is refused
    Given a partition with log end offset <end>
    When a follower fetches from offset <offset>
    Then the leader returns <error>

    Examples:
      | case        | end | offset | error               |
      | past end    | 10  | 11     | OFFSET_OUT_OF_RANGE |
      | before start| 10  | -1     | OFFSET_OUT_OF_RANGE |
";

    #[test]
    fn outline_rows_substitute_placeholders_and_dedupe() {
        let actual = step_uses(PATH, &feature(OUTLINE), None, None);
        let expected = vec![
            StepUse {
                location: format!("{PATH}:4"),
                keyword: Keyword::Given,
                text: "a partition with log end offset 10".to_owned(),
            },
            StepUse {
                location: format!("{PATH}:5"),
                keyword: Keyword::When,
                text: "a follower fetches from offset 11".to_owned(),
            },
            StepUse {
                location: format!("{PATH}:6"),
                keyword: Keyword::Then,
                text: "the leader returns OFFSET_OUT_OF_RANGE".to_owned(),
            },
            StepUse {
                location: format!("{PATH}:5"),
                keyword: Keyword::When,
                text: "a follower fetches from offset -1".to_owned(),
            },
        ];
        assert!(actual == expected);
    }

    const RULES: &str = "\
@docker
Feature: Producers are fenced by epoch

  Scenario: A plain scenario
    Given a producer at epoch 1
    And a broker <unknown>
    When it appends a batch
    Then the append is accepted

  Rule: A producer with an older epoch is fenced

    @inprogress
    Scenario: Append from a stale producer epoch is refused
      Given a producer at epoch 5
      When it appends a batch with epoch 4
      Then the append fails

  @formulated
  Rule: A new epoch resets the sequence

    Scenario: First batch of a new epoch starts at sequence zero
      Given a producer at epoch 1
      When it appends sequence 0 in epoch 3
      Then the append is accepted

    Scenario Outline: Examples tags filter one block
      When it appends <what>

      Examples:
        | case | what  |
        | a    | one   |

      @journey
      Examples:
        | case | what  |
        | b    | two   |
";

    #[rstest]
    #[case::everything(None, None, texts(&[
        (Keyword::Given, "a producer at epoch 1"),
        (Keyword::Given, "a broker <unknown>"),
        (Keyword::When, "it appends a batch"),
        (Keyword::Then, "the append is accepted"),
        (Keyword::Given, "a producer at epoch 5"),
        (Keyword::When, "it appends a batch with epoch 4"),
        (Keyword::Then, "the append fails"),
        (Keyword::When, "it appends sequence 0 in epoch 3"),
        (Keyword::When, "it appends one"),
        (Keyword::When, "it appends two"),
    ]))]
    #[case::ignore_lifecycle(None, Some("@formulated or @inprogress"), texts(&[
        (Keyword::Given, "a producer at epoch 1"),
        (Keyword::Given, "a broker <unknown>"),
        (Keyword::When, "it appends a batch"),
        (Keyword::Then, "the append is accepted"),
    ]))]
    #[case::include_examples_tag(Some("@journey"), None, texts(&[
        (Keyword::When, "it appends two"),
    ]))]
    #[case::include_and_not(Some("@docker and not @formulated"), Some("@inprogress"), texts(&[
        (Keyword::Given, "a producer at epoch 1"),
        (Keyword::Given, "a broker <unknown>"),
        (Keyword::When, "it appends a batch"),
        (Keyword::Then, "the append is accepted"),
    ]))]
    fn rule_scenarios_and_tag_filters(
        #[case] include: Option<&str>,
        #[case] ignore: Option<&str>,
        #[case] expected: Vec<(Keyword, String)>,
    ) {
        assert!(uses(RULES, include, ignore) == expected);
    }

    #[rstest]
    #[case::and_both("@a and @b", &["a", "b"], true)]
    #[case::and_one("@a and @b", &["a"], false)]
    #[case::or_one("@a or @b", &["b"], true)]
    #[case::or_none("@a or @b", &["c"], false)]
    #[case::not_absent("not @a", &["b"], true)]
    #[case::not_present("not @a", &["a"], false)]
    #[case::combined("@a and not (@b or @c)", &["a", "c"], false)]
    fn tags_match_evaluates_expressions(
        #[case] expression: &str,
        #[case] tags: &[&str],
        #[case] expected: bool,
    ) {
        let operation = parse_tag_expression(expression).unwrap();
        let tags: Vec<String> = tags.iter().map(|tag| (*tag).to_owned()).collect();
        assert!(tags_match(&operation, &tags) == expected);
    }

    #[test]
    fn invalid_tag_expression_is_an_error() {
        let error = parse_tag_expression("@a not @b").unwrap_err();
        assert!(error.starts_with("`@a not @b` is not a valid tag expression"));
    }
}
