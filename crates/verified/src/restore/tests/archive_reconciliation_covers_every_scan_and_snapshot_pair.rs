use super::*;
#[cfg(test)]
use std::vec;

#[yare::parameterized(
    scanned_live = { true, RestoreSnapshotState::Live, RestoreReconcileDecision::Keep },
    scanned_delete_started = {
        true,
        RestoreSnapshotState::DeleteStarted,
        RestoreReconcileDecision::Exclude,
    },
    scanned_missing = { true, RestoreSnapshotState::Missing, RestoreReconcileDecision::Disagree },
    scanned_delete_finished = {
        true,
        RestoreSnapshotState::DeleteFinished,
        RestoreReconcileDecision::Disagree,
    },
    unscanned_live = { false, RestoreSnapshotState::Live, RestoreReconcileDecision::Disagree },
    unscanned_delete_started = {
        false,
        RestoreSnapshotState::DeleteStarted,
        RestoreReconcileDecision::Exclude,
    },
    unscanned_delete_finished = {
        false,
        RestoreSnapshotState::DeleteFinished,
        RestoreReconcileDecision::Exclude,
    },
    unscanned_missing = { false, RestoreSnapshotState::Missing, RestoreReconcileDecision::Exclude },
)]
fn archive_reconciliation_covers_every_scan_and_snapshot_pair(
    scanned: bool,
    state: RestoreSnapshotState,
    expected: RestoreReconcileDecision,
) {
    check!(restore_archive_reconcile(scanned, state) == expected);
}

#[test]
fn batch_step_is_contiguous_and_overflow_safe() {
    check!(restore_batch_step(10, 10, 2) == Some(13));
    check!(restore_batch_step(10, 11, 0) == Some(12));
    check!(restore_batch_step(10, 9, 0) == None);
    check!(restore_batch_step(10, 10, -1) == None);
    check!(restore_batch_step(i64::MAX, i64::MAX, 0) == None);
    check!(restore_batch_step(i64::MAX - 1, i64::MAX - 1, 0) == Some(i64::MAX));
}

use {
    rstest::fixture,
    rstest_bdd::{Slot, datatable::Rows},
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
};

struct RestoredBatch {
    frame: Option<RestoreBatchFrame>,
    coordinates: Slot<Option<(i64, i64)>>,
    layout: Option<RestoreLayout>,
    producer: Option<RestoreProducer>,
    frontier: Slot<Option<i64>>,
}

#[fixture]
fn batch() -> RestoredBatch {
    // Start this scenario with fresh inputs and observations.

    RestoredBatch {
        frame: None,
        coordinates: Slot::default(),
        layout: None,
        producer: None,
        frontier: Slot::default(),
    }
}

#[derive(DataTableRow)]
struct BatchFrameRow {
    base_offset: i64,
    last_delta: i32,
    timestamp_type: String,
    base_timestamp: i64,
    max_timestamp: i64,
}

#[given("an archived record batch has this coordinate frame:")]
fn batch_frame(batch: &mut RestoredBatch, #[datatable] rows: Rows<BatchFrameRow>) {
    let mut rows = rows.into_iter();
    let row = rows.next().expect("one archived batch frame");
    assert2::assert!(rows.next().is_none());
    let timestamp_type = match row.timestamp_type.as_str() {
        "create-time" => RestoreTimestampType::CreateTime,
        "log-append-time" => RestoreTimestampType::LogAppendTime,
        kind => panic!("unknown restore timestamp type: {kind}"),
    };
    let mut facts = frame(timestamp_type, row.base_timestamp, row.max_timestamp);
    facts.base_offset = row.base_offset;
    facts.last_offset_delta = row.last_delta;
    batch.frame = Some(facts);
}

#[when(
    "a record with offset delta {offset_delta} and timestamp delta {timestamp_delta} is restored"
)]
fn restore_coordinates(batch: &mut RestoredBatch, offset_delta: i32, timestamp_delta: i64) {
    batch.coordinates.set(restore_record_coordinates(
        batch.frame.expect("an archived batch frame was given"),
        deltas(offset_delta, timestamp_delta),
    ));
}

#[then("the restored record has offset {offset} and timestamp {timestamp}")]
fn record_coordinates(batch: &RestoredBatch, offset: i64, timestamp: i64) {
    check!(batch.coordinates.get() == Some(Some((offset, timestamp))));
}

#[then("the restored record coordinates are rejected")]
fn rejected_coordinates(batch: &RestoredBatch) {
    check!(batch.coordinates.get() == Some(None));
}

#[derive(DataTableRow)]
struct RewrittenHeaderRow {
    base_offset: i64,
    last_delta: i32,
    record_count: i32,
    control: bool,
    transactional: bool,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

fn producer(
    control: bool,
    transactional: bool,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> RestoreProducer {
    RestoreProducer {
        control,
        transactional,
        producer_id,
        producer_epoch,
        base_sequence,
    }
}

#[given("a rewritten batch has this header:")]
fn rewritten_header(batch: &mut RestoredBatch, #[datatable] rows: Rows<RewrittenHeaderRow>) {
    let mut rows = rows.into_iter();
    let row = rows.next().expect("one rewritten batch header");
    assert2::assert!(rows.next().is_none());
    batch.layout = Some(layout(row.base_offset, row.last_delta, row.record_count));
    batch.producer = Some(producer(
        row.control,
        row.transactional,
        row.producer_id,
        row.producer_epoch,
        row.base_sequence,
    ));
}

#[when("the restore writer validates the rewritten batch header")]
fn validate_header(batch: &mut RestoredBatch) {
    batch.frontier.set(restore_rewritten_batch_header(
        batch.layout.expect("a rewritten layout was given"),
        batch.producer.expect("rewritten producer facts were given"),
    ));
}

#[then("the rewritten batch advances the restore frontier to {frontier}")]
fn accepted_header(batch: &RestoredBatch, frontier: i64) {
    check!(batch.frontier.get() == Some(Some(frontier)));
}

#[then("the rewritten batch header is rejected")]
fn rejected_header(batch: &RestoredBatch) {
    check!(batch.frontier.get() == Some(None));
}

scenarios!("tests/features/migrated/src/restore/tests/archive_reconciliation_covers_every_scan_and_snapshot_pair.feature", fixtures = [batch: RestoredBatch]);
