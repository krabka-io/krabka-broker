#![cfg(test)]
//! Operators define barrier groups: a definition creates a group or replaces
//! a live one, and the broker refuses one it cannot honour.
//!
//! The scenarios are `tests/features/barrier_groups/barrier_groups.feature`.

mod steps;
mod support;

use {
    assert2::{assert, check},
    krabka_bdd::TokioMultiThreadContext,
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
fn attempt(
    barrier: &mut BarrierScenario,
    runtime: &TokioMultiThreadContext,
    definition: GroupDefinition,
) {
    let cluster = barrier.cluster.get(runtime);
    let transcript = runtime.block_on(cluster.operator().define(definition));
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
fn defined_without_cadence(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    barrier.define(runtime, over_orders());
}

#[given("an operator has defined a barrier group with a cadence of {cadence}")]
fn defined_with_cadence(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    cadence: String,
) {
    barrier.define(
        runtime,
        GroupDefinition::builder()
            .topics(vec![TOPIC.to_owned()])
            .cadence(cadence)
            .build(),
    );
}

#[given("an operator has defined a barrier group without a retention")]
fn defined_without_retention(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    barrier.define(runtime, over_orders());
}

#[given("the operator has deleted the group")]
fn deleted(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    let cluster = barrier.cluster.get(runtime);
    let deleted = runtime.block_on(cluster.operator().delete(GROUP));
    assert!(deleted.exit == 0, "{deleted:?}");
}

#[when("the operator defines the group again over the {first} and {second} topics")]
fn redefine_over_two_topics(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    first: String,
    second: String,
) {
    attempt(
        barrier,
        runtime,
        GroupDefinition::builder()
            .topics(vec![first, second])
            .build(),
    );
}

#[when("an operator defines a barrier group over a topic that does not exist")]
fn define_over_missing_topic(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
) {
    attempt(
        barrier,
        runtime,
        GroupDefinition::builder()
            .topics(vec![MISSING_TOPIC.to_owned()])
            .build(),
    );
}

#[when("an operator tries to define a barrier group with a cadence of {cadence}")]
fn try_define_with_cadence(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    cadence: String,
) {
    attempt(
        barrier,
        runtime,
        GroupDefinition::builder()
            .topics(vec![TOPIC.to_owned()])
            .cadence(cadence)
            .build(),
    );
}

#[when("an operator defines a barrier group that retains {retained} cut")]
fn define_retaining(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    retained: i32,
) {
    define_with_retention(barrier, runtime, retained);
}

#[when("an operator tries to define a barrier group that retains {retained} cuts")]
fn try_define_retaining(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    retained: i32,
) {
    define_with_retention(barrier, runtime, retained);
}

/// Define the group over the orders topic with `retained` cuts, keeping the
/// transcript whether or not the broker accepts it.
fn define_with_retention(
    barrier: &mut BarrierScenario,
    runtime: &TokioMultiThreadContext,
    retained: i32,
) {
    attempt(
        barrier,
        runtime,
        GroupDefinition::builder()
            .topics(vec![TOPIC.to_owned()])
            .retained_cuts(retained)
            .build(),
    );
}

#[when("an operator tries to define a barrier group over the {topic} topic twice")]
fn try_define_naming_twice(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    topic: String,
) {
    attempt(
        barrier,
        runtime,
        GroupDefinition::builder()
            .topics(vec![topic.clone(), topic])
            .build(),
    );
}

#[then("the group cuts across {first} and {second}")]
fn cuts_across(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    first: String,
    second: String,
) {
    let redefined = barrier.transcript.get();
    assert!(redefined.exit == 0, "{redefined:?}");
    let cluster = barrier.cluster.get(runtime);
    let description = runtime.block_on(cluster.operator().describe(GROUP));
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
