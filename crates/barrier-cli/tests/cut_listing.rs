#![cfg(test)]
//! Operators read back the cuts a group retains, oldest first, from an
//! epoch, and within the group's retention.

mod steps;
mod support;

use {
    assert2::{assert, check},
    krabka_bdd::TokioMultiThreadContext,
    rstest_bdd_macros::{given, then, when},
    steps::{BarrierScenario, barrier},
    support::CutQuery,
};

/// Parse an English list of epochs such as `1, 2 and 3` or `2 and 3`.
fn english_epochs(list: &str) -> Vec<i64> {
    list.split(", ")
        .flat_map(|part| part.split(" and "))
        .map(|epoch| {
            epoch
                .trim()
                .parse()
                .unwrap_or_else(|error| panic!("{epoch:?} in {list:?} is not an epoch: {error}"))
        })
        .collect()
}

#[given(
    "a barrier group over the {topic} topic that retains {retained} cuts has been cut \
     {times} times"
)]
fn cut_with_retention(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    topic: String,
    retained: i32,
    times: usize,
) {
    barrier.cut_group(runtime, &topic, Some(retained), times);
}

#[when("the operator lists {how_many} cut of the group")]
fn list_how_many(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    how_many: i32,
) {
    let cluster = barrier.cluster.get(runtime);
    let transcript = runtime.block_on(
        cluster
            .operator()
            .list(CutQuery::builder().how_many(how_many).build()),
    );
    barrier.transcript.set(transcript);
}

#[when("the operator lists the cuts of the group from epoch {from}")]
fn list_from_epoch(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    from: i64,
) {
    let cluster = barrier.cluster.get(runtime);
    let transcript = runtime.block_on(
        cluster
            .operator()
            .list(CutQuery::builder().from_epoch(from).build()),
    );
    barrier.transcript.set(transcript);
}

#[then("the listed epochs are {epochs}")]
fn listed_epochs_are(barrier: &BarrierScenario, epochs: String) {
    let transcript = barrier.transcript.get();
    assert!(transcript.exit == 0, "{transcript:?}");
    check!(
        transcript.listed_epochs() == english_epochs(&epochs),
        "{transcript:?}"
    );
}

#[then("the only listed epoch is {epoch}")]
fn only_listed_epoch(barrier: &BarrierScenario, epoch: i64) {
    let transcript = barrier.transcript.get();
    assert!(transcript.exit == 0, "{transcript:?}");
    check!(transcript.listed_epochs() == vec![epoch], "{transcript:?}");
}

krabka_bdd::scenarios!(
    "tests/features/cut_listing",
    fixtures = [barrier: BarrierScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
