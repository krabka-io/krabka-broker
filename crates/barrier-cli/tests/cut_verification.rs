#![cfg(test)]
//! Verifying a cut against the log, as KFC-4 specifies it: a cut is worth
//! what the markers behind it are.

mod steps;
mod support;

use {
    assert2::check,
    rstest_bdd_macros::{given, then, when},
    steps::{BarrierScenario, barrier},
    support::GROUP,
};

/// A group name no step defines.
const UNDEFINED_GROUP: &str = "never-defined";

/// The offset the first cut's marker took in `partition` of `topic`.
fn marker_offset(barrier: &BarrierScenario, topic: &str, partition: i32) -> i64 {
    let cut = barrier.cuts.first().expect("a Given took a cut");
    cut.cut_rows()
        .into_iter()
        .find(|(row_topic, row_partition, _)| row_topic == topic && *row_partition == partition)
        .map_or_else(
            || panic!("the cut names no offset for {topic}-{partition}: {cut:?}"),
            |(_, _, offset)| offset,
        )
}

/// Delete every record of `partition` up to and including its marker.
async fn delete_through_marker(barrier: &mut BarrierScenario, topic: &str, partition: i32) {
    let offset = marker_offset(barrier, topic, partition);
    let cluster = barrier.cluster.get().await;
    cluster
        .delete_records_below(topic, partition, offset + 1)
        .await;
}

#[given("the records of {topic} partition {partition} up to the marker have been deleted")]
async fn deleted_in_one_partition(barrier: &mut BarrierScenario, topic: String, partition: i32) {
    delete_through_marker(barrier, &topic, partition).await;
}

#[given(
    "the records of {topic} partitions {first} and {second} up to the marker have been \
     deleted"
)]
async fn deleted_in_two_partitions(
    barrier: &mut BarrierScenario,
    topic: String,
    first: i32,
    second: i32,
) {
    delete_through_marker(barrier, &topic, first).await;
    delete_through_marker(barrier, &topic, second).await;
}

#[given("the {topic} topic has been created again and cut since")]
async fn recreated_and_cut(barrier: &mut BarrierScenario, topic: String) {
    let cluster = barrier.cluster.get().await;
    cluster.delete_topic(&topic).await;
    cluster.create_topic(&topic, 1).await;
    let cut = cluster.operator().trigger(GROUP).await;
    assert2::assert!(cut.exit == 0, "{cut:?}");
    assert2::assert!(cut.epoch() == 2, "{cut:?}");
    let first = marker_offset(barrier, &topic, 0);
    assert2::assert!(
        cut.cut_rows() == vec![(topic, 0, first)],
        "the second marker must land where the first did: {cut:?}"
    );
    barrier.cuts.push(cut);
}

#[when("the operator verifies cut {epoch} of the group")]
async fn verify(barrier: &mut BarrierScenario, epoch: i64) {
    let cluster = barrier.cluster.get().await;
    let transcript = cluster.operator().verify(GROUP, epoch).await;
    barrier.transcript.set(transcript);
}

#[when("the operator tries to verify cut {epoch} of the group")]
async fn try_verify(barrier: &mut BarrierScenario, epoch: i64) { verify(barrier, epoch).await; }

#[when("an operator tries to verify cut {epoch} of a group nobody defined")]
async fn try_verify_undefined(barrier: &mut BarrierScenario, epoch: i64) {
    let cluster = barrier.cluster.get().await;
    let transcript = cluster.operator().verify(UNDEFINED_GROUP, epoch).await;
    barrier.transcript.set(transcript);
}

#[then("the cut verifies with {markers} markers in the log")]
fn verifies(barrier: &BarrierScenario, markers: usize) {
    let transcript = barrier.transcript.get();
    let epoch = barrier.cuts.last().expect("a Given took a cut").epoch();
    check!(transcript.exit == 0, "{transcript:?}");
    check!(
        transcript
            .stdout
            .lines()
            .any(|line| line == format!("cut {epoch} verified: {markers} markers are in the log")),
        "{transcript:?}"
    );
    check!(
        transcript
            .stdout
            .lines()
            .filter(|line| line.ends_with("\tok"))
            .count()
            == markers,
        "{transcript:?}"
    );
}

#[then(
    "the cut does not match the log because {topic} partition {partition} no longer holds the marker"
)]
fn marker_gone(barrier: &BarrierScenario, topic: String, partition: i32) {
    let transcript = barrier.transcript.get();
    check!(transcript.exit == 3, "{transcript:?}");
    let offset = marker_offset(barrier, &topic, partition);
    check!(
        transcript.mismatch_rows()
            == vec![(
                topic,
                partition,
                offset,
                format!("the log no longer holds offset {offset}, so the marker has left the log"),
            )],
        "{transcript:?}"
    );
}

#[then("the tool reports {wrong} of {total} offsets as wrong")]
fn reports_wrong(barrier: &BarrierScenario, wrong: usize, total: usize) {
    let transcript = barrier.transcript.get();
    check!(transcript.exit == 3, "{transcript:?}");
    check!(
        transcript.stderr.lines().any(|line| {
            line.starts_with("cut ")
                && line.ends_with(&format!(
                    "does not match the log: {wrong} of {total} offsets are wrong"
                ))
        }),
        "{transcript:?}"
    );
    check!(transcript.mismatch_rows().len() == wrong, "{transcript:?}");
}

#[then("the cut does not match the log because the marker carries epoch {carried}")]
fn mismatch_by_epoch(barrier: &BarrierScenario, carried: i64) {
    let transcript = barrier.transcript.get();
    let verified = barrier.cuts.first().expect("a Given took a cut").epoch();
    check!(transcript.exit == 3, "{transcript:?}");
    check!(
        transcript.mismatch_rows().iter().any(|(.., reason)| {
            reason.contains(&format!("carries epoch {carried}, not {verified}"))
        }),
        "{transcript:?}"
    );
}

#[then("the tool refuses because the group has no retained cut at epoch {epoch}")]
fn refuses_missing_epoch(barrier: &BarrierScenario, epoch: i64) {
    let transcript = barrier.transcript.get();
    check!(transcript.exit == 1, "{transcript:?}");
    check!(transcript.exit != 3, "{transcript:?}");
    check!(
        transcript
            .stderr
            .contains(&format!("{GROUP} has no retained cut at epoch {epoch}")),
        "{transcript:?}"
    );
}

/// `RESOURCE_NOT_FOUND` (91) is what KFC-4 gives for a group that is not live,
/// and verifying one is a refusal, not a cut the log contradicts.
#[then("the tool refuses because the broker knows no such group")]
fn refuses_unknown_group(barrier: &BarrierScenario) {
    let transcript = barrier.transcript.get();
    check!(transcript.exit == 1, "{transcript:?}");
    check!(transcript.stdout.is_empty(), "{transcript:?}");
    check!(
        transcript.stderr.starts_with(&format!(
            "cannot read the cuts of {UNDEFINED_GROUP}: error 91"
        )),
        "{transcript:?}"
    );
}

krabka_bdd::scenarios!(
    "tests/features/cut_verification",
    fixtures = [barrier: BarrierScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
