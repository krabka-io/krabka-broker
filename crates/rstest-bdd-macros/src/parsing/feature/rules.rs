//! Flattening of Gherkin `Rule:` blocks into the feature's scenario list.
//!
//! The `gherkin` parser keeps scenarios declared under a `Rule:` in
//! `Feature::rules[*].scenarios`, while every code-generation path reads only
//! `Feature::scenarios`. Rather than teach each consumer about rules, the
//! loader flattens rule scenarios into `Feature::scenarios` once, before the
//! feature is cached:
//!
//! - the rule `Background` steps are prepended to each scenario's own steps (the feature
//!   `Background` is prepended later, during step extraction, so the execution order is feature
//!   background, rule background, scenario steps);
//! - the rule tags are merged ahead of the scenario tags, so tag filters, `@allow_skipped`, and
//!   runtime scenario tags inherit them.
//!
//! The original `Feature::rules` entries are kept so [`rule_name_for`] can
//! recover which rule a flattened scenario came from. Gherkin forbids
//! feature-level scenarios after the first `Rule:`, so appending rule
//! scenarios after the feature-level ones preserves file order and keeps the
//! indexes of scenarios outside any rule unchanged.

use gherkin::{Feature, Rule, Scenario};

use crate::parsing::tags;

/// Append every rule scenario to `feature.scenarios`, applying the rule's
/// background steps and tags.
///
/// # Examples
///
/// ```rust,ignore
/// let mut feature = gherkin::Feature::parse(
///     "Feature: f\n Rule: r\n  Scenario: s\n   Given a step\n",
///     gherkin::GherkinEnv::default(),
/// )?;
/// flatten_rules(&mut feature);
/// assert_eq!(feature.scenarios.len(), 1);
/// assert_eq!(rule_name_for(&feature, &feature.scenarios[0]), Some("r"));
/// ```
pub(crate) fn flatten_rules(feature: &mut Feature) {
    let flattened: Vec<Scenario> = feature.rules.iter().flat_map(flatten_rule).collect();
    feature.scenarios.extend(flattened);
}

/// Produce the scenarios of one rule with its background and tags applied.
fn flatten_rule(rule: &Rule) -> impl Iterator<Item = Scenario> + '_ {
    rule.scenarios.iter().map(move |scenario| {
        let mut flattened = scenario.clone();
        if let Some(background) = &rule.background {
            flattened.steps = background
                .steps
                .iter()
                .chain(scenario.steps.iter())
                .cloned()
                .collect();
        }
        flattened.tags = tags::merge_tag_sets(&rule.tags, &scenario.tags);
        flattened
    })
}

/// Return the name of the rule that declares `scenario`, if any.
///
/// Scenarios are matched by their source span, which is unique within a
/// feature file and survives the cloning performed by [`flatten_rules`].
pub(crate) fn rule_name_for<'a>(feature: &'a Feature, scenario: &Scenario) -> Option<&'a str> {
    feature
        .rules
        .iter()
        .find(|rule| rule.scenarios.iter().any(|s| s.span == scenario.span))
        .map(|rule| rule.name.as_str())
}

#[cfg(test)]
mod tests {
    //! Unit tests for rule flattening.

    use gherkin::{GherkinEnv, StepType};

    use super::*;

    const RULE_FEATURE: &str = concat!(
        "@feature\n",
        "Feature: rules\n",
        "  Background:\n",
        "    Given a feature background\n",
        "  Scenario: outside\n",
        "    Given a top-level step\n",
        "  @rule_tag\n",
        "  Rule: first rule\n",
        "    Background:\n",
        "      Given a rule background\n",
        "    @scenario_tag\n",
        "    Scenario: inside\n",
        "      When a rule step\n",
        "  Rule: second rule\n",
        "    Scenario: plain\n",
        "      Then another step\n",
    );

    fn parsed() -> Feature {
        let mut feature =
            Feature::parse(RULE_FEATURE, GherkinEnv::default()).expect("feature should parse");
        flatten_rules(&mut feature);
        feature
    }

    fn scenario<'a>(feature: &'a Feature, name: &str) -> &'a Scenario {
        feature
            .scenarios
            .iter()
            .find(|s| s.name == name)
            .expect("scenario should be flattened")
    }

    #[test]
    fn appends_rule_scenarios_after_feature_scenarios() {
        let names: Vec<_> = parsed().scenarios.iter().map(|s| s.name.clone()).collect();
        assert_eq!(names, ["outside", "inside", "plain"]);
    }

    #[test]
    fn prepends_rule_background_only() {
        let feature = parsed();
        let steps: Vec<_> = scenario(&feature, "inside")
            .steps
            .iter()
            .map(|s| (s.ty, s.value.clone()))
            .collect();
        assert_eq!(
            steps,
            [
                (StepType::Given, "a rule background".to_owned()),
                (StepType::When, "a rule step".to_owned()),
            ]
        );
    }

    #[test]
    fn merges_rule_tags_ahead_of_scenario_tags() {
        let feature = parsed();
        assert_eq!(
            scenario(&feature, "inside").tags,
            ["@rule_tag", "@scenario_tag"]
        );
        assert_eq!(scenario(&feature, "plain").tags, Vec::<String>::new());
    }

    #[test]
    fn recovers_rule_names() {
        let feature = parsed();
        assert_eq!(
            rule_name_for(&feature, scenario(&feature, "inside")),
            Some("first rule")
        );
        assert_eq!(
            rule_name_for(&feature, scenario(&feature, "plain")),
            Some("second rule")
        );
        assert_eq!(rule_name_for(&feature, scenario(&feature, "outside")), None);
    }
}
