#![cfg(test)]
//! [`Ref`] as steps use it: "a topic" introduces, "the topic" refers to the
//! one introduced last, "the topic orders" finds one by name, and "the oldest
//! segment" applies a registered selector.

use {
    assert2::assert,
    krabka_bdd::{Given, Ref, ScenarioContext},
    rstest::rstest,
    rstest_bdd::panic_message,
    std::panic::{AssertUnwindSafe, catch_unwind},
};

#[derive(ScenarioContext)]
struct ClusterScenario {
    /// Partition counts by topic.
    topics: Ref<u32>,
    /// Base offsets by segment, oldest first.
    segments: Ref<u64>,
    #[introduced_by("When an operator inspects the {selector} segment")]
    inspected: Given<u64>,
}

fn oldest(segments: &[u64]) -> Option<usize> {
    segments
        .iter()
        .enumerate()
        .min_by_key(|(_, base)| **base)
        .map(|(index, _)| index)
}

fn latest(segments: &[u64]) -> Option<usize> {
    segments
        .iter()
        .enumerate()
        .max_by_key(|(_, base)| **base)
        .map(|(index, _)| index)
}

#[rstest::fixture]
fn cluster() -> ClusterScenario {
    let mut cluster = ClusterScenario::default();
    cluster
        .segments
        .register_selector("segment", "oldest", oldest);
    cluster
        .segments
        .register_selector("segment", "latest", latest);
    cluster
}

#[krabka_bdd::given("a topic named {name} with {partitions} partitions")]
fn a_named_topic(cluster: &mut ClusterScenario, name: String, partitions: u32) {
    cluster.topics.introduce_named("topic", &name, partitions);
}

#[krabka_bdd::given("a segment with base offset {base}")]
fn a_segment(cluster: &mut ClusterScenario, base: u64) {
    cluster.segments.introduce("segment", base);
}

#[krabka_bdd::when("an operator adds {added} partitions to the topic")]
fn adds_to_the_topic(cluster: &mut ClusterScenario, added: u32) {
    *cluster.topics.the_mut("topic") += added;
}

#[krabka_bdd::when("an operator adds {added} partitions to the topic {name}")]
fn adds_to_the_named_topic(cluster: &mut ClusterScenario, added: u32, name: String) {
    *cluster.topics.named_mut("topic", &name) += added;
}

#[krabka_bdd::when("an operator inspects the {selector} segment")]
fn inspects(cluster: &mut ClusterScenario, selector: String) {
    let base = *cluster.segments.select("segment", &selector);
    cluster.set_inspected(base);
}

#[krabka_bdd::then("the topic {name} has {partitions} partitions")]
fn the_topic_has(cluster: &ClusterScenario, name: String, partitions: u32) {
    assert!(*cluster.topics.named("topic", &name) == partitions);
}

#[krabka_bdd::then("the inspected segment starts at offset {offset}")]
fn the_inspected_segment_starts_at(cluster: &ClusterScenario, offset: u64) {
    assert!(*cluster.the_inspected() == offset);
}

krabka_bdd::scenarios!(
    "tests/features/refs_scenarios",
    fixtures = [cluster: ClusterScenario],
);

fn panic_of(step: impl FnOnce()) -> String {
    let payload = catch_unwind(AssertUnwindSafe(step)).expect_err("the step panics");
    panic_message(payload.as_ref())
}

#[rstest]
#[case::the_topic_without_a_topic(
    |cluster: &mut ClusterScenario| adds_to_the_topic(cluster, 2),
    "no topic has been introduced: add a step like `Given a topic ...` before this step",
)]
#[case::an_unknown_name(
    |cluster: &mut ClusterScenario| {
        a_named_topic(cluster, "orders".to_owned(), 3);
        adds_to_the_named_topic(cluster, 2, "refunds".to_owned());
    },
    "no topic named refunds has been introduced: the named topic entities are `orders`",
)]
#[case::an_unknown_selector(
    |cluster: &mut ClusterScenario| {
        a_segment(cluster, 0);
        inspects(cluster, "largest".to_owned());
    },
    "no segment selector `largest`: the segment selectors are `latest`, `oldest`",
)]
#[case::a_selector_without_segments(
    |cluster: &mut ClusterScenario| inspects(cluster, "oldest".to_owned()),
    "no segment has been introduced: add a step like `Given a segment ...` before this step",
)]
fn a_step_that_refers_to_nothing_names_the_fix(
    #[case] steps: fn(&mut ClusterScenario),
    #[case] expected: &str,
) {
    let mut scenario = cluster::default();
    assert!(panic_of(|| steps(&mut scenario)) == expected);
}
