#![cfg(test)]
//! `#[derive(ScenarioContext)]`: the generated accessors and `Default`, the
//! panic that names a missing step, and fresh state for every scenario.
//!
//! The two scenarios in `tests/features/context_scenarios` append different
//! record counts. If they shared state, whichever ran second would count the
//! other's records too.

mod panics;

use {
    assert2::assert,
    krabka_bdd::{Given, Ref, ScenarioContext},
    panics::panic_of,
    rstest::rstest,
};

#[derive(ScenarioContext)]
struct TopicScenario {
    #[introduced_by("Given a topic with {n} partitions")]
    partitions: Given<u32>,
    #[introduced_by("When a producer appends {n} records")]
    appended: Given<u64>,
    /// Plain state: `Default` builds it, and it gets no accessors.
    producers: Ref<String>,
}

#[rstest::fixture]
fn topic_scenario() -> TopicScenario {
    TopicScenario {
        ..TopicScenario::default()
    }
}

#[krabka_bdd::given("a topic with {partitions} partition(s)")]
fn a_topic(topic_scenario: &mut TopicScenario, partitions: u32) {
    topic_scenario.set_partitions(partitions);
}

#[krabka_bdd::when("a producer appends {records} records")]
fn a_producer_appends(topic_scenario: &mut TopicScenario, records: u64) {
    topic_scenario
        .producers
        .introduce("producer", "producer-1".to_owned());
    if !topic_scenario.appended.is_set() {
        topic_scenario.set_appended(0);
    }
    *topic_scenario.the_appended_mut() += records;
}

#[krabka_bdd::then("the topic holds {records} records across {partitions} partition(s)")]
fn the_topic_holds(topic_scenario: &TopicScenario, records: u64, partitions: u32) {
    assert!(
        (
            *topic_scenario.the_appended(),
            *topic_scenario.the_partitions()
        ) == (records, partitions)
    );
    assert!(topic_scenario.producers.all("producer") == ["producer-1"]);
}

krabka_bdd::scenarios!(
    "tests/features/context_scenarios",
    fixtures = [topic_scenario: TopicScenario],
);

/// One scenario's steps, run against the context it is handed.
fn run_scenario(scenario: &mut TopicScenario, partitions: u32, records: u64) {
    a_topic(scenario, partitions);
    a_producer_appends(scenario, records);
    the_topic_holds(scenario, records, partitions);
}

#[rstest]
#[case::three_partitions_first(&[(3, 2), (1, 5)])]
#[case::one_partition_first(&[(1, 5), (3, 2)])]
fn each_scenario_starts_from_a_fresh_context_in_either_order(#[case] scenarios: &[(u32, u64)]) {
    for &(partitions, records) in scenarios {
        let mut scenario = topic_scenario::default();
        run_scenario(&mut scenario, partitions, records);
    }
}

#[test]
fn default_leaves_every_slot_unset() {
    let scenario = TopicScenario::default();
    assert!(!scenario.partitions.is_set());
    assert!(!scenario.appended.is_set());
    assert!(scenario.producers.all("producer").is_empty());
}

#[test]
fn set_then_the_returns_the_value() {
    let mut scenario = TopicScenario::default();
    scenario.set_partitions(3);
    scenario.set_appended(2);
    *scenario.the_appended_mut() += 1;
    assert!((*scenario.the_partitions(), *scenario.the_appended()) == (3, 3));
}

#[rstest]
#[case::the_partitions(
    || { let _ = TopicScenario::default().the_partitions(); },
    "scenario state `partitions` is not set: the scenario needs the step `Given a topic with \
     {n} partitions` before this one",
)]
#[case::the_partitions_mut(
    || { let _ = TopicScenario::default().the_partitions_mut(); },
    "scenario state `partitions` is not set: the scenario needs the step `Given a topic with \
     {n} partitions` before this one",
)]
#[case::the_appended(
    || { let _ = TopicScenario::default().the_appended(); },
    "scenario state `appended` is not set: the scenario needs the step `When a producer \
     appends {n} records` before this one",
)]
#[case::a_then_without_its_given(
    || the_topic_holds(&TopicScenario::default(), 2, 3),
    "scenario state `appended` is not set: the scenario needs the step `When a producer \
     appends {n} records` before this one",
)]
fn an_unset_slot_names_the_field_and_its_step(#[case] read: fn(), #[case] expected: &str) {
    assert!(panic_of(read) == expected);
}

/// A generic context: the accessors keep the struct's parameters.
#[derive(ScenarioContext)]
struct Holder<T> {
    #[introduced_by("Given a value has been stored")]
    value: Given<T>,
}

#[test]
fn a_generic_context_reads_its_typed_slot() {
    let mut holder = Holder::<Vec<u8>>::default();
    holder.set_value(vec![1, 2]);
    holder.the_value_mut().push(3);
    assert!(*holder.the_value() == [1, 2, 3]);
}
