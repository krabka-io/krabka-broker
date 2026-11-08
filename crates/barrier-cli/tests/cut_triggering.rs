#![cfg(test)]
//! Scenarios for triggering a cut on demand: which partitions a cut names,
//! the epoch it takes, and when a group's topics are resolved.

mod steps;
mod support;

use {
    rstest_bdd_macros::{given, then, when},
    std::collections::BTreeSet,
    steps::{BarrierScenario, barrier},
};

/// A group name no step defines.
const UNDEFINED_GROUP: &str = "never-defined";

#[given("the {topic} topic was created afterwards with {partitions} partitions")]
async fn topic_created_since(barrier: &mut BarrierScenario, topic: String, partitions: i32) {
    barrier.create_topic(&topic, partitions).await;
}

#[when("an operator tries to trigger a cut of a group nobody defined")]
async fn trigger_undefined(barrier: &mut BarrierScenario) {
    let cluster = barrier.cluster.get().await;
    let transcript = cluster.operator().trigger(UNDEFINED_GROUP).await;
    barrier.transcript.set(transcript);
}

/// The table's header row is skipped; each remaining row is a topic and a
/// partition.
#[then("the cut names an offset for exactly these partitions:")]
fn cut_names_partitions(barrier: &BarrierScenario, datatable: Vec<Vec<String>>) {
    let expected: BTreeSet<(String, i32)> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [topic, partition] = <[String; 2]>::try_from(row).expect("topic | partition");
            (topic, partition.parse().expect("partition number"))
        })
        .collect();
    let transcript = barrier.transcript.get();
    assert2::check!(transcript.exit == 0, "{transcript:?}");
    // A partition the cut names as missing is named too, and the tool writes
    // those to stderr, so an empty stderr is part of "exactly".
    assert2::check!(transcript.stderr.is_empty(), "{transcript:?}");
    let named: BTreeSet<(String, i32)> = transcript
        .cut_rows()
        .into_iter()
        .map(|(topic, partition, _offset)| (topic, partition))
        .collect();
    assert2::assert!(named == expected, "{transcript:?}");
}

#[then("the cut is epoch {epoch}")]
fn cut_is_epoch(barrier: &BarrierScenario, epoch: i64) {
    let transcript = barrier.transcript.get();
    assert2::check!(transcript.exit == 0, "{transcript:?}");
    assert2::assert!(transcript.epoch() == epoch, "{transcript:?}");
}

krabka_bdd::scenarios!(
    "tests/features/cut_triggering",
    fixtures = [barrier: BarrierScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
