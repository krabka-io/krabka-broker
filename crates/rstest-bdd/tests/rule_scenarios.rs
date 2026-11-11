//! Rule scenarios retain their backgrounds, tags, and outline examples.

use rstest::fixture;
use rstest_bdd_macros::{given, scenario, scenarios, then, when};

#[derive(Default)]
struct Counter {
    value: i32,
}

#[rstest_bdd_test_macros::allow_fixture_expansion_lints]
#[fixture]
fn counter() -> Counter { Counter::default() }

#[given("a count contribution of {amount}")]
#[when("the count increases by {amount}")]
fn increase_count(counter: &mut Counter, amount: i32) { counter.value += amount; }

#[then("the count is {expected}")]
fn count_is(counter: &Counter, expected: i32) {
    assert_eq!(counter.value, expected);
}

scenarios!("tests/features/rules", fixtures = [counter: Counter]);

#[scenario(
    path = "tests/features/rules/rules.feature",
    name = "Inside the first rule",
    tags = "@feature and @first and @plain"
)]
fn selects_rule_scenario_by_name(counter: Counter) {
    assert_eq!(counter.value, 13);
}

#[scenario(
    path = "tests/features/rules/rules.feature",
    index = 3,
    tags = "@second and not @first"
)]
fn selects_rule_scenario_by_index(counter: Counter) {
    assert_eq!(counter.value, 23);
}

#[scenario(
    path = "tests/features/rules/rules.feature",
    name = "Outline inside the first rule",
    tags = "@first and @outline and @fast and not @slow"
)]
fn filters_rule_outline_examples(counter: Counter, amount: String, expected: String) {
    assert_eq!(
        (amount.as_str(), expected.as_str(), counter.value),
        ("1", "12", 12)
    );
}
