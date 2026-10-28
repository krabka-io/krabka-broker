use super::*;
#[cfg(test)]
use std::vec;

#[cfg(test)]
#[rstest_bdd_macros::then(
    "producer snapshot tests reload log start follows kafka log loader: reload log start follows kafka log loader for {example}"
)]
fn bdd_verify_reload_log_start_follows_kafka_log_loader(example: String) {
    type Example = (bool, Option<i64>, i64, i64);
    let (remote, established, local, expected): Example = match example.as_str() {
        "remote checkpoint below local" => (true, Some(3), 8, 3),
        "remote without checkpoint" => (true, None, 8, 0),
        "local uses segment base over checkpoint" => (false, Some(3), 8, 8),
        "local without checkpoint" => (false, None, 8, 8),
        _ => panic!("unknown BDD example: {example}"),
    };

    assert2::check!(producer_snapshot_reload_log_start(remote, established, local) == expected);
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/producer_snapshot/tests/reload_log_start_follows_kafka_log_loader.feature",
    name = "reload log start follows kafka log loader"
)]
fn reload_log_start_follows_kafka_log_loader(example: String) { let _ = example; }

#[cfg(test)]
#[rstest_bdd_macros::then(
    "producer snapshot tests reload log start follows kafka log loader: reload keeps exactly the half open range above the log start for {example}"
)]
fn bdd_verify_reload_keeps_exactly_the_half_open_range_above_the_log_start(example: String) {
    type Example = (i64, bool);
    let (offset, kept): Example = match example.as_str() {
        "below log start" => (4, false),
        "at log start" => (5, false),
        "inside log range" => (6, true),
        "at log end" => (10, true),
        "above log end" => (11, false),
        "negative offset" => (-1, false),
        _ => panic!("unknown BDD example: {example}"),
    };

    assert2::check!(producer_snapshot_reload_keeps(offset, RANGE) == kept);
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/producer_snapshot/tests/reload_log_start_follows_kafka_log_loader.feature",
    name = "reload keeps exactly the half open range above the log start"
)]
fn reload_keeps_exactly_the_half_open_range_above_the_log_start(example: String) {
    let _ = example;
}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "producer snapshot tests reload log start follows kafka log loader: stray snapshots follow kafka remove stray snapshots for {example}"
)]
fn bdd_verify_stray_snapshots_follow_kafka_remove_stray_snapshots(example: String) {
    type Example<'a> = (&'a [i64], &'a [i64], &'a [bool]);
    let (snapshots, bases, removed): Example<'_> = match example.as_str() {
        "all match segment bases" => (
            &[0_i64, 4, 8][..],
            &[0_i64, 4, 8][..],
            &[false, false, false][..],
        ),
        "older strays removed" => (
            &[2_i64, 4, 6][..],
            &[0_i64, 4, 8][..],
            &[true, false, true][..],
        ),
        "clean shutdown snapshot preserved" => (
            &[4_i64, 8, 11][..],
            &[0_i64, 4, 8][..],
            &[false, false, false][..],
        ),
        "only newest above bases preserved" => {
            (&[9_i64, 11][..], &[0_i64, 4, 8][..], &[true, false][..])
        }
        "newest snapshot without segments" => (&[3_i64, 7][..], &[][..], &[true, false][..]),
        "input order is irrelevant" => (
            &[11_i64, 2, 4][..],
            &[8_i64, 0, 4][..],
            &[false, true, false][..],
        ),
        _ => panic!("unknown BDD example: {example}"),
    };

    let actual: Vec<_> = (0..removed.len())
        .map(|index| producer_snapshot_stray(snapshots, index, bases))
        .collect();
    assert2::check!(actual == removed);
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/producer_snapshot/tests/reload_log_start_follows_kafka_log_loader.feature",
    name = "stray snapshots follow kafka remove stray snapshots"
)]
fn stray_snapshots_follow_kafka_remove_stray_snapshots(example: String) { let _ = example; }

#[cfg(test)]
#[rstest_bdd_macros::then(
    "producer snapshot tests reload log start follows kafka log loader: latest snapshot is the newest one the reload keeps for {example}"
)]
fn bdd_verify_latest_snapshot_is_the_newest_one_the_reload_keeps(example: String) {
    type Example<'a> = (&'a [i64], ProducerReloadRange, Option<usize>);
    let (offsets, range, expected): Example<'_> = match example.as_str() {
        "no snapshots" => (&[][..], RANGE, None),
        "all offsets out of range" => (&[3, 5, 11, -1][..], RANGE, None),
        "newest kept in unsorted list" => (&[6, 10, 7, 12][..], RANGE, Some(1)),
        "start boundary is excluded" => (&[5, 6][..], RANGE, Some(1)),
        "duplicate latest keeps first" => (&[10, 10][..], RANGE, Some(0)),
        "maximum offset is valid" => (
            &[i64::MAX][..],
            ProducerReloadRange {
                log_start: i64::MAX - 1,
                local_start: i64::MAX - 1,
                log_end: i64::MAX,
            },
            Some(0),
        ),
        _ => panic!("unknown BDD example: {example}"),
    };

    assert2::check!(producer_snapshot_latest_index(offsets, range) == expected);
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/producer_snapshot/tests/reload_log_start_follows_kafka_log_loader.feature",
    name = "latest snapshot is the newest one the reload keeps"
)]
fn latest_snapshot_is_the_newest_one_the_reload_keeps(example: String) { let _ = example; }

/// One row per validity rule over a snapshot at offset 10, each a change
/// to one valid entry: producer 7 at epoch 2, whose last batch is
/// sequences ..=4 at offsets 6..=9 (delta 3), with a transaction open
/// since offset 5 under coordinator epoch 0.
const VALID: ProducerSnapshotEntryFacts = ProducerSnapshotEntryFacts {
    producer_id: 7,
    producer_epoch: 2,
    last_sequence: 4,
    last_offset: 9,
    offset_delta: 3,
    coordinator_epoch: 0,
    current_txn_first_offset: 5,
};
const MARKER_ONLY: ProducerSnapshotEntryFacts = ProducerSnapshotEntryFacts {
    producer_id: 0,
    producer_epoch: 0,
    last_sequence: -1,
    last_offset: -1,
    offset_delta: 0,
    coordinator_epoch: -1,
    current_txn_first_offset: -1,
};

#[cfg(test)]
#[rstest_bdd_macros::then(
    "producer snapshot tests reload log start follows kafka log loader: entry validation is exact and snapshot bounded for {example}"
)]
fn bdd_verify_entry_validation_is_exact_and_snapshot_bounded(example: String) {
    type Example = (i64, ProducerSnapshotEntryFacts, bool);
    let (snapshot_offset, entry, expected): Example = match example.as_str() {
        "real producer" => (10, VALID, true),
        "marker only producer" => (10, MARKER_ONLY, true),
        "integer extremes" => (
            i64::MAX,
            ProducerSnapshotEntryFacts {
                producer_id: i64::MAX,
                producer_epoch: i16::MAX,
                last_sequence: i32::MAX,
                last_offset: i64::MAX - 1,
                offset_delta: i32::MAX,
                coordinator_epoch: i32::MAX,
                current_txn_first_offset: i64::MAX - 2,
            },
            true,
        ),
        "negative snapshot offset" => (-1, MARKER_ONLY, false),
        "negative producer id" => (
            10,
            ProducerSnapshotEntryFacts {
                producer_id: -1,
                ..VALID
            },
            false,
        ),
        "negative producer epoch" => (
            10,
            ProducerSnapshotEntryFacts {
                producer_epoch: -1,
                ..VALID
            },
            false,
        ),
        "coordinator epoch below sentinel" => (
            10,
            ProducerSnapshotEntryFacts {
                coordinator_epoch: -2,
                ..VALID
            },
            false,
        ),
        "last record at boundary" => (
            10,
            ProducerSnapshotEntryFacts {
                last_offset: 10,
                ..VALID
            },
            false,
        ),
        "delta below zero" => (
            10,
            ProducerSnapshotEntryFacts {
                last_offset: 0,
                offset_delta: 1,
                current_txn_first_offset: -1,
                ..VALID
            },
            false,
        ),
        "sentinel with delta" => (
            10,
            ProducerSnapshotEntryFacts {
                offset_delta: 1,
                ..MARKER_ONLY
            },
            false,
        ),
        "transaction at boundary" => (
            10,
            ProducerSnapshotEntryFacts {
                current_txn_first_offset: 10,
                ..VALID
            },
            false,
        ),
        "transaction after last record" => (
            10,
            ProducerSnapshotEntryFacts {
                last_offset: 4,
                offset_delta: 0,
                ..VALID
            },
            false,
        ),
        "transaction below sentinel" => (
            10,
            ProducerSnapshotEntryFacts {
                current_txn_first_offset: -2,
                ..VALID
            },
            false,
        ),
        _ => panic!("unknown BDD example: {example}"),
    };

    assert2::check!(producer_snapshot_entry_valid(snapshot_offset, entry) == expected);
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/producer_snapshot/tests/reload_log_start_follows_kafka_log_loader.feature",
    name = "entry validation is exact and snapshot bounded"
)]
fn entry_validation_is_exact_and_snapshot_bounded(example: String) { let _ = example; }
