#[cfg(test)]
use std::vec;
use {super::*, crate::log_index::IndexEntry};

#[yare::parameterized(
    ordinary_range = { 100, 104, Some(4) },
    zero_base = { 0, 0, Some(0) },
    largest_relative_span = { 0, i64::from(u32::MAX), Some(i64::from(u32::MAX)) },
    first_span_over_limit = { 0, i64::from(u32::MAX) + 1, None },
    signed_overflow = { 0, i64::MAX, None },
    negative_base = { -1, 104, None },
    end_before_base = { 104, 100, None },
)]
fn index_frontier_is_the_u32_relative_span(base: i64, end: i64, expected: Option<i64>) {
    check!(restore_index_frontier(base, end) == expected);
}

#[test]
fn offset_and_time_index_entries_are_strict_and_bounded() {
    check!(restore_offset_index_entry_valid(None, 0, 0, 4, 100));
    check!(restore_offset_index_entry_valid(
        Some(IndexEntry::new(0, 0)),
        3,
        50,
        4,
        100
    ));
    check!(!restore_offset_index_entry_valid(
        Some(IndexEntry::new(3, 50)),
        3,
        60,
        4,
        100
    ));
    check!(!restore_offset_index_entry_valid(
        Some(IndexEntry::new(0, 50)),
        3,
        50,
        4,
        100
    ));
    check!(!restore_offset_index_entry_valid(None, 5, 0, 4, 100));
    check!(!restore_offset_index_entry_valid(None, 0, 100, 4, 100));
    check!(restore_offset_index_entry_valid(
        None,
        u32::MAX,
        0,
        i64::from(u32::MAX),
        1
    ));

    check!(restore_time_index_entry_valid(None, 10, 0, 4));
    // krabka's writer repeats the running maximum timestamp.
    check!(restore_time_index_entry_valid(
        Some(IndexEntry::new(10, 0)),
        10,
        3,
        4
    ));
    check!(!restore_time_index_entry_valid(
        Some(IndexEntry::new(10, 3)),
        11,
        3,
        4
    ));
    check!(!restore_time_index_entry_valid(
        Some(IndexEntry::new(10, 0)),
        9,
        3,
        4
    ));
    check!(!restore_time_index_entry_valid(None, 10, 5, 4));
}

use {
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
};

struct RestoredTransactions {
    segment: RestoreSegmentExtent,
    entries: Vec<RestoreAbortedTxn>,
    valid: Option<bool>,
}

#[fixture]
fn index() -> RestoredTransactions {
    // Start this scenario with fresh inputs and observations.

    RestoredTransactions {
        segment: SEGMENT,
        entries: Vec::new(),
        valid: None,
    }
}

#[given("the restored segment spans offsets {base} through {last}")]
fn restored_segment(index: &mut RestoredTransactions, base: i64, last: i64) {
    index.segment = RestoreSegmentExtent {
        base_offset: base,
        last_offset: last,
    };
}

#[derive(DataTableRow)]
struct AbortedTransactionRow {
    producer_id: i64,
    transaction_start: i64,
    abort_marker: i64,
}

#[given("the archived transaction index contains these entries in order:")]
fn archived_transactions(
    index: &mut RestoredTransactions,
    #[datatable] rows: Rows<AbortedTransactionRow>,
) {
    index.entries = rows
        .into_iter()
        .map(|row| txn(row.producer_id, row.transaction_start, row.abort_marker))
        .collect();
}

#[when("the restored transaction index is validated in marker order")]
fn validate_transaction_index(index: &mut RestoredTransactions) {
    index.valid = Some(txn_index_valid(&index.entries, index.segment));
}

#[then("the restored transaction index is valid: {valid}")]
fn transaction_index_validity(index: &RestoredTransactions, valid: bool) {
    check!(index.valid == Some(valid));
}

scenarios!("tests/features/migrated/src/restore_sidecar/tests/index_frontier_is_the_u32_relative_span.feature", fixtures = [index: RestoredTransactions]);

#[test]
fn leader_epoch_entries_are_strict_and_bounded() {
    check!(restore_leader_epoch_entry_valid(None, 0, 100, 100, 104));
    check!(restore_leader_epoch_entry_valid(
        Some((0, 100)),
        1,
        103,
        100,
        104
    ));
    check!(!restore_leader_epoch_entry_valid(
        Some((1, 100)),
        1,
        103,
        100,
        104
    ));
    check!(!restore_leader_epoch_entry_valid(
        Some((0, 103)),
        1,
        103,
        100,
        104
    ));
    check!(!restore_leader_epoch_entry_valid(None, -1, 100, 100, 104));
    check!(!restore_leader_epoch_entry_valid(None, 0, 105, 100, 104));
}

#[test]
fn producer_ids_are_canonical_and_unique() {
    check!(restore_producer_ids_strict(&[]));
    check!(restore_producer_ids_strict(&[7]));
    check!(restore_producer_ids_strict(&[7, 8, 9]));
    check!(!restore_producer_ids_strict(&[7, 9, 8]));
    check!(!restore_producer_ids_strict(&[7, 9, 9]));
}
