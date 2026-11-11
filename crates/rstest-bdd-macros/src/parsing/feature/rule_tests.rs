//! Regression tests for discovery and inherited context of rule scenarios.

use std::path::Path;

use crate::parsing::{
    feature::{ScenarioData, extract_scenario_steps, parse_and_load_feature},
    tags::TagExpression,
};

/// Load the shared behavioural fixture through the same entry point as macros.
fn rule_feature() -> gherkin::Feature {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../rstest-bdd/tests/features/rules/rules.feature");
    parse_and_load_feature(&path).expect("load rule scenarios")
}

/// Extract one fixture scenario with its inherited steps and tags.
fn rule_scenario(index: usize) -> ScenarioData {
    extract_scenario_steps(&rule_feature(), Some(index)).expect("extract rule scenario")
}

#[test]
fn discovers_features_containing_only_rule_scenarios() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../rstest-bdd/tests/features/rules/only_rules.feature");
    let feature = parse_and_load_feature(&path).expect("load rule-only feature");
    assert_eq!(feature.scenarios.len(), 1);
    let scenario = extract_scenario_steps(&feature, Some(0)).expect("extract rule-only scenario");
    assert_eq!(scenario.name, "No top-level scenario is required");
}

#[test]
fn discovers_top_level_and_rule_scenarios_in_source_order() {
    let feature = rule_feature();
    let names: Vec<_> = feature
        .scenarios
        .iter()
        .map(|scenario| scenario.name.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "Outside a rule",
            "Inside the first rule",
            "Outline inside the first rule",
            "Inside the second rule",
        ]
    );
    let lines: Vec<_> = (0..feature.scenarios.len())
        .map(|index| rule_scenario(index).line)
        .collect();
    assert_eq!(lines, [6, 16, 21, 40]);
}

#[test]
fn rule_backgrounds_follow_feature_background_without_leaking() {
    let expected = [
        vec![
            "a count contribution of 1",
            "the count increases by 2",
            "the count is 3",
        ],
        vec![
            "a count contribution of 1",
            "a count contribution of 10",
            "the count increases by 2",
            "the count is 13",
        ],
        vec![
            "a count contribution of 1",
            "a count contribution of 20",
            "the count increases by 2",
            "the count is 23",
        ],
    ];
    for (index, expected_steps) in [0, 1, 3].into_iter().zip(expected) {
        let scenario = rule_scenario(index);
        let steps: Vec<_> = scenario
            .steps
            .iter()
            .map(|step| step.text.as_str())
            .collect();
        assert_eq!(steps, expected_steps);
    }
}

#[test]
fn rule_tags_reach_scenarios_and_example_filters_without_leaking() {
    assert_eq!(rule_scenario(0).tags, ["@feature"]);
    assert_eq!(rule_scenario(1).tags, ["@feature", "@first", "@plain"]);
    assert_eq!(rule_scenario(3).tags, ["@feature", "@second"]);
    let mut outline = rule_scenario(2);
    let filter = TagExpression::parse("@feature and @first and @outline and @fast and not @slow")
        .expect("valid tag expression");
    assert!(outline.filter_by_tags(&filter));
    let examples = outline.examples.expect("outline examples");
    assert_eq!(examples.rows, [vec!["1", "12"]]);
    assert_eq!(
        examples.row_tags,
        [vec!["@feature", "@first", "@outline", "@fast"]]
    );
}
