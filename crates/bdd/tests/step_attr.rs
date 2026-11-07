//! `#[krabka_bdd::given]`, `when`, `then`, `step`, `before` and `after` as a
//! test binary uses them: what lands in the rstest-bdd step registry and in
//! the hook registry, and which step texts the expanded patterns match.

#![cfg(test)]

use {
    assert2::assert,
    krabka_bdd::{Attempt, Hook, HookPhase, Outcome, ScenarioSpec},
    rstest::rstest,
    rstest_bdd::{Step, StepKeyword, StepText, extract_placeholders},
};

#[derive(Debug, Default)]
struct Cluster {
    topics: Vec<String>,
    partitions: u32,
}

#[krabka_bdd::given("a/an topic named {string}")]
fn topic(cluster: &mut Cluster, name: String) { cluster.topics.push(name); }

#[krabka_bdd::when(
    "the client creates {int} partition(s)",
    "the client adds {int} partition(s)"
)]
fn partitions(cluster: &mut Cluster, count: u32) { cluster.partitions += count; }

#[krabka_bdd::then("the topic has {int} replica(s) of {word}")]
fn replicas(cluster: &Cluster, count: u16, name: String) -> Result<(), String> {
    if cluster.topics.contains(&name) && count > 0 {
        Ok(())
    } else {
        Err(format!("no replicas of {name}"))
    }
}

#[krabka_bdd::step("the broker is up")]
fn broker_is_up() {}

#[krabka_bdd::when("the client attempts to delete {word}")]
fn delete(cluster: &Cluster, name: String) -> Attempt<(), String> {
    Attempt::from_result(if cluster.topics.contains(&name) {
        Ok(())
    } else {
        Err(name)
    })
}

#[krabka_bdd::when("the client waits {int} ms")]
async fn wait(milliseconds: u64) {
    tokio::time::sleep(tokio::time::Duration::from_millis(milliseconds)).await;
}

#[krabka_bdd::before(tags = "@docker", order = 5)]
async fn start_docker(scenario: &ScenarioSpec) { drop(scenario); }

#[krabka_bdd::after]
async fn report(scenario: &ScenarioSpec, outcome: &Outcome) { drop((scenario, outcome)); }

/// Every step this file defines, as `(keyword, pattern)`, sorted.
fn registered_here() -> Vec<(String, &'static str)> {
    let mut steps: Vec<(String, &'static str)> = rstest_bdd::iter::<Step>
        .into_iter()
        .filter(|step| step.file == file!())
        .map(|step| (format!("{:?}", step.keyword), step.pattern.as_str()))
        .collect();
    steps.sort_unstable();
    steps
}

/// The step this file registers under `keyword` with `pattern`.
fn step(keyword: StepKeyword, pattern: &str) -> &'static Step {
    rstest_bdd::iter::<Step>
        .into_iter()
        .find(|step| {
            step.file == file!() && step.keyword == keyword && step.pattern.as_str() == pattern
        })
        .unwrap_or_else(|| panic!("no {keyword:?} step `{pattern}` in {}", file!()))
}

/// `text` with `‹` and `›` for `{` and `}`, which keeps Clippy from reading
/// the patterns as format strings.
fn braced(text: &str) -> String { text.replace('‹', "{").replace('›', "}") }

#[test]
fn every_expansion_and_keyword_is_registered() {
    let mut expected: Vec<(String, String)> = [
        (StepKeyword::Given, "a topic named ‹name:string›"),
        (StepKeyword::Given, "an topic named ‹name:string›"),
        (
            StepKeyword::When,
            "the client creates ‹count:u32› partition",
        ),
        (
            StepKeyword::When,
            "the client creates ‹count:u32› partitions",
        ),
        (StepKeyword::When, "the client adds ‹count:u32› partition"),
        (StepKeyword::When, "the client adds ‹count:u32› partitions"),
        (
            StepKeyword::Then,
            "the topic has ‹count:u16› replica of ‹name›",
        ),
        (
            StepKeyword::Then,
            "the topic has ‹count:u16› replicas of ‹name›",
        ),
        (StepKeyword::Given, "the broker is up"),
        (StepKeyword::When, "the broker is up"),
        (StepKeyword::Then, "the broker is up"),
        (StepKeyword::When, "the client attempts to delete ‹name›"),
        (StepKeyword::When, "the client waits ‹milliseconds:u64› ms"),
    ]
    .iter()
    .map(|(keyword, pattern)| (format!("{keyword:?}"), braced(pattern)))
    .collect();
    expected.sort_unstable();
    let registered: Vec<(String, String)> = registered_here()
        .into_iter()
        .map(|(keyword, pattern)| (keyword, pattern.to_owned()))
        .collect();
    assert!(registered == expected);
}

#[rstest]
#[case::article(StepKeyword::Given, "a topic named ‹name:string›", "a topic named \"orders\"", &["orders"])]
#[case::other_article(StepKeyword::Given, "an topic named ‹name:string›", "an topic named 'events'", &["events"])]
#[case::singular(StepKeyword::When, "the client creates ‹count:u32› partition", "the client creates 1 partition", &["1"])]
#[case::plural(StepKeyword::When, "the client adds ‹count:u32› partitions", "the client adds 12 partitions", &["12"])]
#[case::word(
    StepKeyword::Then,
    "the topic has ‹count:u16› replicas of ‹name›",
    "the topic has 3 replicas of orders",
    &["3", "orders"]
)]
#[case::attempt(
    StepKeyword::When,
    "the client attempts to delete ‹name›",
    "the client attempts to delete orders",
    &["orders"]
)]
fn expanded_patterns_match_their_texts(
    #[case] keyword: StepKeyword,
    #[case] pattern: &str,
    #[case] text: &str,
    #[case] expected: &[&str],
) {
    let captured = extract_placeholders(
        step(keyword, &braced(pattern)).pattern,
        StepText::from(text),
    );
    let expected: Vec<String> = expected.iter().map(|value| (*value).to_owned()).collect();
    assert!(captured.ok() == Some(expected));
}

#[rstest]
#[case::negative_count(
    StepKeyword::When,
    "the client creates ‹count:u32› partitions",
    "the client creates -1 partitions"
)]
#[case::singular_is_not_plural(
    StepKeyword::When,
    "the client creates ‹count:u32› partition",
    "the client creates 2 partitions"
)]
#[case::unquoted_string(
    StepKeyword::Given,
    "a topic named ‹name:string›",
    "a topic named orders"
)]
fn expanded_patterns_reject_other_texts(
    #[case] keyword: StepKeyword,
    #[case] pattern: &str,
    #[case] text: &str,
) {
    let captured = extract_placeholders(
        step(keyword, &braced(pattern)).pattern,
        StepText::from(text),
    );
    assert!(let Err(_) = captured);
}

/// The hooks this file registers, as `(phase, tags, order)`, sorted by order.
fn hooks_here() -> Vec<(HookPhase, Option<&'static str>, i32)> {
    let mut hooks: Vec<&Hook> = inventory::iter::<Hook>
        .into_iter()
        .filter(|hook| hook.file == file!())
        .collect();
    hooks.sort_by_key(|hook| hook.order);
    hooks
        .iter()
        .map(|hook| (hook.phase, hook.tags, hook.order))
        .collect()
}

#[test]
fn hooks_are_registered_with_their_tags_and_order() {
    assert!(
        hooks_here()
            == [
                (HookPhase::AfterScenario, None, 0),
                (HookPhase::BeforeScenario, Some("@docker"), 5),
            ]
    );
}
