#![cfg(test)]
//! Operators define barrier groups: a definition creates a group or replaces
//! a live one, and the broker refuses one it cannot honour.
//!
//! The scenarios are `tests/features/barrier_groups/barrier_groups.feature`.

mod steps;
mod support;

use {
    assert2::{assert, check},
    rstest_bdd_macros::{given, then, when},
    steps::{BarrierScenario, barrier},
    support::{GROUP, GroupDefinition, Transcript},
};

/// The topic a scenario names when the topic does not matter.
const TOPIC: &str = "orders";

/// A topic no step creates.
const MISSING_TOPIC: &str = "not-created-yet";

/// `INVALID_CONFIG`, which KFC-4 gives for a setting out of range.
const INVALID_CONFIG: i16 = 40;

/// `INVALID_REQUEST`, which KFC-4 gives for a malformed topic list.
const INVALID_REQUEST: i16 = 42;

/// A definition of the scenario's group over the default topic.
fn over_orders() -> GroupDefinition {
    GroupDefinition::builder()
        .topics(vec![TOPIC.to_owned()])
        .build()
}

/// Run a definition and keep what the tool printed, whatever it was.
async fn attempt(barrier: &mut BarrierScenario, definition: GroupDefinition) {
    let cluster = barrier.cluster.get().await;
    let transcript = cluster.operator().define(definition).await;
    barrier.transcript.set(transcript);
}

/// What the broker describes the scenario's group as, beside `label`.
fn described(transcript: &Transcript, label: &str) -> Option<String> {
    assert!(transcript.exit == 0, "{transcript:?}");
    transcript.described(label)
}

/// Whether a stderr line reports `code`, with or without the group before it.
fn reports_code(line: &str, code: i16) -> bool {
    let error = line.split_once('\t').map_or(line, |(_, rest)| rest);
    let Some(rest) = error.strip_prefix(&format!("error {code}")) else {
        return false;
    };
    rest.is_empty() || rest.starts_with(':') || rest.starts_with(' ')
}

/// Require the broker to have refused the scenario's request with `code`.
fn refused_with(barrier: &BarrierScenario, code: i16) {
    let transcript = barrier.transcript.get();
    check!(!transcript.command_line_refused, "{transcript:?}");
    check!(transcript.exit == 1, "{transcript:?}");
    check!(transcript.stdout.is_empty(), "{transcript:?}");
    check!(
        transcript
            .stderr
            .lines()
            .any(|line| reports_code(line, code)),
        "{transcript:?}"
    );
}

#[given("an operator has defined a barrier group without a cadence")]
async fn defined_without_cadence(barrier: &mut BarrierScenario) {
    barrier.define(over_orders()).await;
}

#[given("an operator has defined a barrier group with a cadence of {cadence}")]
async fn defined_with_cadence(barrier: &mut BarrierScenario, cadence: String) {
    barrier
        .define(
            GroupDefinition::builder()
                .topics(vec![TOPIC.to_owned()])
                .cadence(cadence)
                .build(),
        )
        .await;
}

#[given("an operator has defined a barrier group without a retention")]
async fn defined_without_retention(barrier: &mut BarrierScenario) {
    barrier.define(over_orders()).await;
}

#[given("the operator has deleted the group")]
async fn deleted(barrier: &mut BarrierScenario) {
    let cluster = barrier.cluster.get().await;
    let deleted = cluster.operator().delete(GROUP).await;
    assert!(deleted.exit == 0, "{deleted:?}");
}

#[when("the operator defines the group again over the {first} and {second} topics")]
async fn redefine_over_two_topics(barrier: &mut BarrierScenario, first: String, second: String) {
    attempt(
        barrier,
        GroupDefinition::builder()
            .topics(vec![first, second])
            .build(),
    )
    .await;
}

#[when("an operator defines a barrier group over a topic that does not exist")]
async fn define_over_missing_topic(barrier: &mut BarrierScenario) {
    attempt(
        barrier,
        GroupDefinition::builder()
            .topics(vec![MISSING_TOPIC.to_owned()])
            .build(),
    )
    .await;
}

#[when("an operator tries to define a barrier group with a cadence of {cadence}")]
async fn try_define_with_cadence(barrier: &mut BarrierScenario, cadence: String) {
    attempt(
        barrier,
        GroupDefinition::builder()
            .topics(vec![TOPIC.to_owned()])
            .cadence(cadence)
            .build(),
    )
    .await;
}

#[when("an operator defines a barrier group that retains {retained} cut")]
async fn define_retaining(barrier: &mut BarrierScenario, retained: i32) {
    define_with_retention(barrier, retained).await;
}

#[when("an operator tries to define a barrier group that retains {retained} cuts")]
async fn try_define_retaining(barrier: &mut BarrierScenario, retained: i32) {
    define_with_retention(barrier, retained).await;
}

/// Define the group over the orders topic with `retained` cuts, keeping the
/// transcript whether or not the broker accepts it.
async fn define_with_retention(barrier: &mut BarrierScenario, retained: i32) {
    attempt(
        barrier,
        GroupDefinition::builder()
            .topics(vec![TOPIC.to_owned()])
            .retained_cuts(retained)
            .build(),
    )
    .await;
}

#[when("an operator tries to define a barrier group over the {topic} topic twice")]
async fn try_define_naming_twice(barrier: &mut BarrierScenario, topic: String) {
    attempt(
        barrier,
        GroupDefinition::builder()
            .topics(vec![topic.clone(), topic])
            .build(),
    )
    .await;
}

#[then("the group cuts across {first} and {second}")]
async fn cuts_across(barrier: &mut BarrierScenario, first: String, second: String) {
    let redefined = barrier.transcript.get();
    assert!(redefined.exit == 0, "{redefined:?}");
    let cluster = barrier.cluster.get().await;
    let description = cluster.operator().describe(GROUP).await;
    check!(described(&description, "topics") == Some(format!("{first}, {second}")));
}

#[then("the tool reports the group as defined")]
fn reported_defined(barrier: &BarrierScenario) {
    let transcript = barrier.transcript.get();
    check!(
        *transcript
            == Transcript {
                exit: 0,
                stdout: format!("{GROUP}\tok\n"),
                stderr: String::new(),
                command_line_refused: false,
            }
    );
}

#[then("the tool refuses the command line")]
fn command_line_refused(barrier: &BarrierScenario) {
    let transcript = barrier.transcript.get();
    check!(transcript.command_line_refused, "{transcript:?}");
    check!(transcript.exit != 0, "{transcript:?}");
    check!(transcript.stdout.is_empty(), "{transcript:?}");
}

#[then("the group is described as cut on demand only")]
fn described_on_demand(barrier: &BarrierScenario) {
    let transcript = barrier.transcript.get();
    check!(described(transcript, "interval") == Some("on demand only".to_owned()));
}

#[then("the group is described as cut every {interval}")]
fn described_every(barrier: &BarrierScenario, interval: String) {
    let transcript = barrier.transcript.get();
    check!(described(transcript, "interval") == Some(interval));
}

#[then("the group is described as retaining {retained} cuts")]
fn described_retaining(barrier: &BarrierScenario, retained: String) {
    let transcript = barrier.transcript.get();
    check!(described(transcript, "retained cuts") == Some(retained));
}

#[then("the group is described with no last epoch yet")]
fn described_no_epoch(barrier: &BarrierScenario) {
    let transcript = barrier.transcript.get();
    check!(described(transcript, "last epoch") == Some("none yet".to_owned()));
}

#[then("the group is described with last epoch {epoch}")]
fn described_epoch(barrier: &BarrierScenario, epoch: String) {
    let transcript = barrier.transcript.get();
    check!(described(transcript, "last epoch") == Some(epoch));
}

#[then("the broker refuses the definition as an out-of-range setting")]
fn refused_out_of_range(barrier: &BarrierScenario) { refused_with(barrier, INVALID_CONFIG); }

#[then("the broker refuses the definition as a malformed request")]
fn refused_malformed(barrier: &BarrierScenario) { refused_with(barrier, INVALID_REQUEST); }

krabka_bdd::scenarios!(
    "tests/features/barrier_groups",
    fixtures = [barrier: BarrierScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
