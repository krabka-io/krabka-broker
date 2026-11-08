//! What the reporting commands read out of a parsed feature: its open
//! questions, the lifecycle of its scenarios and the rows of its outlines.

use {
    crate::{discover::FeatureFile, fs::FileSystem},
    gherkin::{Feature, GherkinEnv, Scenario},
    std::fmt::Write,
};

/// The description line that records an open question with its owner.
const QUESTION_PREFIX: &str = "Question:";

/// The tags whose meaning a report shows beside a scenario.
pub const LIFECYCLE_TAGS: [&str; 5] = ["formulated", "inprogress", "manual", "journey", "docker"];

/// The tag that marks a scenario as agreed but not automated.
pub const FORMULATED: &str = "formulated";

/// The tag that marks a scenario whose automation is incomplete.
pub const INPROGRESS: &str = "inprogress";

/// The tag that marks an end-to-end journey.
pub const JOURNEY: &str = "journey";

/// One open question from discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Question {
    pub text: String,
    /// The owner the line names, or `None` when it names none.
    pub owner: Option<String>,
}

/// A feature file and what parsing it gave.
pub struct Loaded {
    pub file: FeatureFile,
    /// The feature, or gherkin's parse error as text.
    pub feature: Result<Feature, String>,
}

/// Reads and parses every file.
///
/// # Errors
///
/// Returns the message of the first file that cannot be read. A file that
/// does not parse is not an error here; its [`Loaded::feature`] holds why.
pub fn load(filesystem: &dyn FileSystem, files: Vec<FeatureFile>) -> Result<Vec<Loaded>, String> {
    files
        .into_iter()
        .map(|file| {
            let source = filesystem.read(&file.absolute)?;
            Ok(Loaded {
                feature: parse(&source),
                file,
            })
        })
        .collect()
}

/// Parses one feature file's text.
///
/// # Errors
///
/// Returns gherkin's parse error as text.
pub fn parse(source: &str) -> Result<Feature, String> {
    Feature::parse(source, GherkinEnv::default()).map_err(|error| error.to_string())
}

/// The `Question:` lines of a feature or rule description.
#[must_use]
pub fn questions(description: Option<&str>) -> Vec<Question> {
    description
        .into_iter()
        .flat_map(str::lines)
        .filter_map(|line| line.trim().strip_prefix(QUESTION_PREFIX))
        .map(|rest| {
            let rest = rest.trim();
            let parsed = rest
                .strip_suffix(')')
                .and_then(|body| body.rsplit_once("(owner:"))
                .map(|(text, owner)| (text.trim(), owner.trim()))
                .filter(|(text, owner)| !text.is_empty() && !owner.is_empty());
            match parsed {
                Some((text, owner)) => Question {
                    text: text.to_owned(),
                    owner: Some(owner.to_owned()),
                },
                None => Question {
                    text: rest.to_owned(),
                    owner: None,
                },
            }
        })
        .collect()
}

/// The tags that apply to `scenario`: its feature's, its rule's and its own,
/// in that order and without repeats.
#[must_use]
pub fn effective_tags(scenario: &Scenario, inherited: &[&[String]]) -> Vec<String> {
    let mut tags: Vec<String> = Vec::new();
    for tag in inherited.iter().copied().flatten().chain(&scenario.tags) {
        if !tags.contains(tag) {
            tags.push(tag.clone());
        }
    }
    tags
}

/// The label of each Examples row of an outline: its `case` cell when the
/// first column is `case`, or else its cells joined by ` | `.
#[must_use]
pub fn example_labels(scenario: &Scenario) -> Vec<String> {
    let mut labels = Vec::new();
    for examples in &scenario.examples {
        let Some(table) = &examples.table else {
            continue;
        };
        let mut rows = table.rows.iter();
        let has_case = rows
            .next()
            .and_then(|header| header.first())
            .is_some_and(|first| first == "case");
        for row in rows {
            let label = if has_case {
                row.first().cloned().unwrap_or_default()
            } else {
                row.join(" | ")
            };
            labels.push(label);
        }
    }
    labels
}

/// How many examples `scenario` gives: one for a plain scenario, one per
/// Examples row for an outline.
#[must_use]
pub fn example_count(scenario: &Scenario) -> usize {
    if scenario.examples.is_empty() {
        1
    } else {
        example_labels(scenario).len()
    }
}

/// The lifecycle tags among `tags`, written as inline code, each after a
/// space.
#[must_use]
pub fn lifecycle_suffix(tags: &[String]) -> String {
    let mut suffix = String::new();
    for tag in LIFECYCLE_TAGS {
        if tags.iter().any(|have| have == tag) {
            // Writing to a `String` cannot fail.
            let _ = write!(suffix, " `@{tag}`");
        }
    }
    suffix
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, rstest::rstest};

    fn question(text: &str, owner: Option<&str>) -> Question {
        Question {
            text: text.to_owned(),
            owner: owner.map(str::to_owned),
        }
    }

    #[rstest]
    #[case::none(None, vec![])]
    #[case::with_owner(
        Some("  Question: Is a retry fenced? (owner: protocol team)\n"),
        vec![question("Is a retry fenced?", Some("protocol team"))],
    )]
    #[case::without_owner(
        Some("Question: Who decides?\nDepends on: Topics - ../topics/topics.feature"),
        vec![question("Who decides?", None)],
    )]
    #[case::empty_owner(Some("Question: A? (owner: )"), vec![question("A? (owner: )", None)])]
    fn questions_read_the_question_lines(
        #[case] description: Option<&str>,
        #[case] expected: Vec<Question>,
    ) {
        assert!(questions(description) == expected);
    }

    const OUTLINE: &str = "\
Feature: Epochs
  @inprogress
  Scenario Outline: Epoch boundaries
    Given a producer at epoch <epoch>
    When it appends
    Then the append is <outcome>

    Examples:
      | case      | epoch | outcome  |
      | equal     | 5     | accepted |
      | one below | 4     | refused  |

    Examples:
      | epoch | outcome |
      | 9     | fenced  |
";

    #[test]
    fn outline_rows_are_labelled_by_case_cell_or_by_their_cells() {
        let feature = parse(OUTLINE).unwrap();
        let scenario = &feature.scenarios[0];
        let expected = vec![
            "equal".to_owned(),
            "one below".to_owned(),
            "9 | fenced".to_owned(),
        ];
        assert!(example_labels(scenario) == expected);
        assert!(example_count(scenario) == 3);
    }

    #[test]
    fn effective_tags_inherit_without_repeats() {
        let feature = parse(OUTLINE).unwrap();
        let scenario = &feature.scenarios[0];
        let feature_tags = vec!["docker".to_owned(), "inprogress".to_owned()];
        let tags = effective_tags(scenario, &[&feature_tags]);
        assert!(tags == vec!["docker".to_owned(), "inprogress".to_owned()]);
        assert!(lifecycle_suffix(&tags) == " `@inprogress` `@docker`");
    }
}
