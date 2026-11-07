#![cfg(test)]
//! Verifying a cut against the log, as KFC-4 specifies it: a cut is worth
//! what the markers behind it are.

mod steps;
mod support;

use {
    assert2::check,
    krabka_bdd::TokioMultiThreadContext,
    rstest_bdd_macros::{given, then, when},
    steps::{BarrierScenario, barrier},
    support::{GROUP, Transcript},
};

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
fn delete_through_marker(
    barrier: &mut BarrierScenario,
    runtime: &TokioMultiThreadContext,
    topic: &str,
    partition: i32,
) {
    let offset = marker_offset(barrier, topic, partition);
    let cluster = barrier.cluster.get(runtime);
    runtime.block_on(cluster.delete_records_below(topic, partition, offset + 1));
}

/// The mismatch rows a verify wrote: topic, partition, offset and reason.
fn mismatch_rows(transcript: &Transcript) -> Vec<(String, i32, i64, String)> {
    transcript
        .stderr
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(4, '\t');
            let (Some(topic), Some(partition), Some(offset), Some(reason)) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                return None;
            };
            Some((
                topic.to_owned(),
                partition.parse().ok()?,
                offset.parse().ok()?,
                reason.to_owned(),
            ))
        })
        .collect()
}

#[given("the records of {topic} partition {partition} up to the marker have been deleted")]
fn deleted_in_one_partition(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    topic: String,
    partition: i32,
) {
    delete_through_marker(barrier, runtime, &topic, partition);
}

#[given(
    "the records of {topic} partitions {first} and {second} up to the marker have been \
     deleted"
)]
fn deleted_in_two_partitions(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    topic: String,
    first: i32,
    second: i32,
) {
    delete_through_marker(barrier, runtime, &topic, first);
    delete_through_marker(barrier, runtime, &topic, second);
}

#[given("the {topic} topic has been created again and cut since")]
fn recreated_and_cut(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    topic: String,
) {
    let cluster = barrier.cluster.get(runtime);
    runtime.block_on(cluster.delete_topic(&topic));
    runtime.block_on(cluster.create_topic(&topic, 1));
    let cut = runtime.block_on(cluster.operator().trigger(GROUP));
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
fn verify(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    epoch: i64,
) {
    let cluster = barrier.cluster.get(runtime);
    let transcript = runtime.block_on(cluster.operator().verify(GROUP, epoch));
    barrier.transcript.set(transcript);
}

#[when("the operator tries to verify cut {epoch} of the group")]
fn try_verify(
    barrier: &mut BarrierScenario,
    #[from(rstest_bdd_harness_context)] runtime: &TokioMultiThreadContext,
    epoch: i64,
) {
    verify(barrier, runtime, epoch);
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
        mismatch_rows(transcript)
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
    check!(mismatch_rows(transcript).len() == wrong, "{transcript:?}");
}

#[then("the cut does not match the log because the marker carries epoch {carried}")]
fn mismatch_by_epoch(barrier: &BarrierScenario, carried: i64) {
    let transcript = barrier.transcript.get();
    let verified = barrier.cuts.first().expect("a Given took a cut").epoch();
    check!(transcript.exit == 3, "{transcript:?}");
    check!(
        mismatch_rows(transcript).iter().any(|(.., reason)| {
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

krabka_bdd::scenarios!(
    "tests/features/cut_verification",
    fixtures = [barrier: BarrierScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
