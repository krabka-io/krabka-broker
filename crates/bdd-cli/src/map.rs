//! `krabka-bdd map`: each feature as an example map for a business review.
//!
//! The map follows example mapping as *Effective Behavior-Driven
//! Development* runs it: the feature is the yellow card, each rule a blue
//! card, each example a green card and each open question a red card.

use {
    crate::{
        Report,
        feature::{self, FORMULATED, INPROGRESS, Loaded, Question},
    },
    gherkin::{Feature, Scenario},
    std::fmt::Write,
};

/// At this many open questions a feature is not ready to automate.
const MANY_QUESTIONS: usize = 3;

/// Past this many rules a feature is worth splitting.
const MANY_RULES: usize = 6;

/// The example maps of `loaded`, one section per feature.
///
/// A file that does not parse gets a note in place of its map, and fails the
/// report.
#[must_use]
pub fn render(loaded: &[Loaded]) -> Report {
    let mut text = String::from("# Example maps\n");
    let mut failed = false;
    for entry in loaded {
        text.push('\n');
        match &entry.feature {
            Ok(feature) => text.push_str(&example_map(&entry.file.shown, feature)),
            Err(error) => {
                failed = true;
                let _ = writeln!(
                    text,
                    "## ⚠️ `{}`\n\nDoes not parse: {error}",
                    entry.file.shown
                );
            }
        }
    }
    Report { text, failed }
}

/// The example map of one feature, as a Markdown section.
#[must_use]
pub fn example_map(shown: &str, feature: &Feature) -> String {
    let counts = Counts::of(feature);
    let mut text = String::new();
    // Writing to a `String` cannot fail.
    let _ = writeln!(text, "## 🟨 {}\n", feature.name);
    let _ = writeln!(
        text,
        "`{shown}` · {} rules · {} scenarios · {} `@{FORMULATED}` · {} `@{INPROGRESS}`",
        feature.rules.len(),
        counts.scenarios,
        counts.formulated,
        counts.inprogress
    );
    let questions = feature::questions(feature.description.as_deref());
    if !questions.is_empty() {
        text.push('\n');
        push_questions(&mut text, &questions);
    }
    for rule in &feature.rules {
        let _ = writeln!(text, "\n### 🟦 {}\n", rule.name);
        let inherited: [&[String]; 2] = [&feature.tags, &rule.tags];
        for scenario in &rule.scenarios {
            push_scenario(&mut text, scenario, &inherited);
        }
        push_questions(&mut text, &feature::questions(rule.description.as_deref()));
    }
    if !feature.scenarios.is_empty() {
        let heading = if feature.rules.is_empty() {
            "Examples"
        } else {
            "Examples outside a rule"
        };
        let _ = writeln!(text, "\n### {heading}\n");
        let inherited: [&[String]; 1] = [&feature.tags];
        for scenario in &feature.scenarios {
            push_scenario(&mut text, scenario, &inherited);
        }
    }
    let notes = readiness(feature);
    text.push_str("\n**Readiness:**");
    if notes.is_empty() {
        text.push_str(" nothing to note.\n");
    } else {
        text.push_str("\n\n");
        for note in notes {
            let _ = writeln!(text, "- {note}");
        }
    }
    text
}

/// The advisory notes on whether `feature` is ready to automate and well
/// sized.
#[must_use]
pub fn readiness(feature: &Feature) -> Vec<String> {
    let mut notes = Vec::new();
    let questions = feature::questions(feature.description.as_deref()).len()
        + feature
            .rules
            .iter()
            .map(|rule| feature::questions(rule.description.as_deref()).len())
            .sum::<usize>();
    if questions >= MANY_QUESTIONS {
        notes.push(format!(
            "{questions} open questions: not ready to automate."
        ));
    }
    if feature.rules.len() > MANY_RULES {
        notes.push(format!(
            "{} rules: consider splitting the feature.",
            feature.rules.len()
        ));
    }
    for rule in &feature.rules {
        let examples: usize = rule.scenarios.iter().map(feature::example_count).sum();
        if examples == 1 {
            notes.push(format!(
                "Rule `{}` has one example: add a counter-example or a boundary example.",
                rule.name
            ));
        }
    }
    notes
}

/// How many scenarios a feature has, and how many carry each lifecycle tag.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub scenarios: usize,
    pub formulated: usize,
    pub inprogress: usize,
}

impl Counts {
    /// The counts of `feature`, an outline counting once, with feature and
    /// rule tags applied to every scenario under them.
    #[must_use]
    pub fn of(feature: &Feature) -> Self {
        let mut counts = Self::default();
        let mut add = |scenario: &Scenario, inherited: &[&[String]]| {
            let tags = feature::effective_tags(scenario, inherited);
            counts.scenarios += 1;
            counts.formulated += usize::from(tags.iter().any(|tag| tag == FORMULATED));
            counts.inprogress += usize::from(tags.iter().any(|tag| tag == INPROGRESS));
        };
        for scenario in &feature.scenarios {
            add(scenario, &[&feature.tags]);
        }
        for rule in &feature.rules {
            for scenario in &rule.scenarios {
                add(scenario, &[&feature.tags, &rule.tags]);
            }
        }
        counts
    }
}

fn push_scenario(text: &mut String, scenario: &Scenario, inherited: &[&[String]]) {
    let tags = feature::effective_tags(scenario, inherited);
    let _ = writeln!(
        text,
        "- 🟩 {}{}",
        scenario.name,
        feature::lifecycle_suffix(&tags)
    );
    for label in feature::example_labels(scenario) {
        let _ = writeln!(text, "  - 🟩 {label}");
    }
}

fn push_questions(text: &mut String, questions: &[Question]) {
    for question in questions {
        let owner = question.owner.as_deref().unwrap_or("no owner named");
        let _ = writeln!(text, "- 🟥 {} (owner: {owner})", question.text);
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, rstest::rstest};

    const FENCING: &str = "\
@docker
Feature: Producers are fenced by epoch
  Question: Does a fenced producer get a retriable error? (owner: protocol team)

  Rule: A producer with an older epoch is fenced
    Question: Is epoch overflow fenced as well? (owner: broker team)

    Scenario: Append from a stale producer epoch is refused
      Given a transactional producer has been bumped to epoch 5
      When it appends a batch with epoch 4
      Then the append fails with INVALID_PRODUCER_EPOCH

    @formulated
    Scenario Outline: Epochs at the fencing boundary
      Given a transactional producer has been bumped to epoch 5
      When it appends a batch with epoch <epoch>
      Then the append is <outcome>

      Examples:
        | case            | epoch | outcome  |
        | the same epoch  | 5     | accepted |
        | one epoch below | 4     | refused  |

  @inprogress
  Rule: A new epoch resets the sequence

    Scenario: First batch of a new epoch starts at sequence zero
      Given a producer has appended up to sequence 7 in epoch 2
      When it appends sequence 0 in epoch 3
      Then the append is accepted
";

    #[test]
    fn example_map_renders_cards_counts_and_readiness() {
        let feature = feature::parse(FENCING).unwrap();
        let expected = "\
## 🟨 Producers are fenced by epoch

`crates/p/tests/features/fencing/fencing.feature` · 2 rules · 3 scenarios · 1 `@formulated` · 1 `@inprogress`

- 🟥 Does a fenced producer get a retriable error? (owner: protocol team)

### 🟦 A producer with an older epoch is fenced

- 🟩 Append from a stale producer epoch is refused `@docker`
- 🟩 Epochs at the fencing boundary `@formulated` `@docker`
  - 🟩 the same epoch
  - 🟩 one epoch below
- 🟥 Is epoch overflow fenced as well? (owner: broker team)

### 🟦 A new epoch resets the sequence

- 🟩 First batch of a new epoch starts at sequence zero `@inprogress` `@docker`

**Readiness:**

- Rule `A new epoch resets the sequence` has one example: add a counter-example or a boundary example.
";
        let map = example_map("crates/p/tests/features/fencing/fencing.feature", &feature);
        assert!(map == expected);
    }

    #[test]
    fn example_map_of_a_feature_without_rules() {
        let feature = feature::parse(
            "\
Feature: Segments roll on size
  Question: Does an empty segment roll? (owner: storage)
  Question: What is the floor?

  Scenario: A full segment rolls
    Given a segment has reached its size limit
    When a batch is appended
    Then a new segment is opened
",
        )
        .unwrap();
        let expected = "\
## 🟨 Segments roll on size

`roll.feature` · 0 rules · 1 scenarios · 0 `@formulated` · 0 `@inprogress`

- 🟥 Does an empty segment roll? (owner: storage)
- 🟥 What is the floor? (owner: no owner named)

### Examples

- 🟩 A full segment rolls

**Readiness:** nothing to note.
";
        assert!(example_map("roll.feature", &feature) == expected);
    }

    fn feature_with(questions: usize, rules: &[usize]) -> Feature {
        let mut source = String::from("Feature: Sized\n");
        for index in 0..questions {
            let _ = writeln!(source, "  Question: Open {index}? (owner: o)");
        }
        for (rule, examples) in rules.iter().enumerate() {
            let _ = writeln!(source, "\n  Rule: Rule {rule}\n");
            for example in 0..*examples {
                let _ = writeln!(
                    source,
                    "    Scenario: Example {rule} {example}\n      Given a state\n      When \
                     a thing happens\n      Then an outcome holds\n"
                );
            }
        }
        feature::parse(&source).unwrap()
    }

    #[rstest]
    #[case::ready(2, &[2, 3], vec![])]
    #[case::three_questions(3, &[2], vec!["3 open questions: not ready to automate."])]
    #[case::six_rules_is_fine(0, &[2, 2, 2, 2, 2, 2], vec![])]
    #[case::seven_rules(0, &[2, 2, 2, 2, 2, 2, 2], vec!["7 rules: consider splitting the feature."])]
    #[case::one_example(
        0,
        &[1, 2],
        vec!["Rule `Rule 0` has one example: add a counter-example or a boundary example."],
    )]
    fn readiness_notes(
        #[case] questions: usize,
        #[case] rules: &[usize],
        #[case] expected: Vec<&str>,
    ) {
        assert!(readiness(&feature_with(questions, rules)) == expected);
    }

    #[test]
    fn an_outline_with_two_rows_is_two_examples() {
        let feature = feature::parse(FENCING).unwrap();
        let notes = readiness(&feature);
        assert!(notes.len() == 1);
        assert!(
            Counts::of(&feature)
                == Counts {
                    scenarios: 3,
                    formulated: 1,
                    inprogress: 1,
                }
        );
    }
}
