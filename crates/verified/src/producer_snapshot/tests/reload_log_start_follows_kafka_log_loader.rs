use {
    super::*,
    rstest::fixture,
    rstest_bdd::{Slot, datatable::Rows},
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    std::vec,
};

#[derive(Default)]
struct SnapshotReload {
    remote: bool,
    established: Option<i64>,
    local_start: i64,
    range: Option<ProducerReloadRange>,
    offsets: Vec<i64>,
    bases: Vec<i64>,
    snapshot_offset: i64,
    entry: Option<ProducerSnapshotEntryFacts>,
    start: Option<i64>,
    kept: Option<bool>,
    removed: Option<Vec<bool>>,
    latest: Slot<Option<usize>>,
    valid: Option<bool>,
}

#[fixture]
fn reload() -> SnapshotReload {
    // Start this scenario with fresh inputs and observations.

    SnapshotReload {
        range: None,
        ..SnapshotReload::default()
    }
}

fn offsets(csv: &str) -> Vec<i64> {
    if csv == "empty" {
        Vec::new()
    } else {
        csv.split(',')
            .map(|value| value.parse().expect("snapshot offset is an integer"))
            .collect()
    }
}

#[given(
    "remote storage is {remote} with checkpoint {checkpoint} and local segment start {local_start}"
)]
fn log_starts(reload: &mut SnapshotReload, remote: bool, checkpoint: String, local_start: i64) {
    reload.remote = remote;
    reload.established = if checkpoint == "none" {
        None
    } else {
        Some(checkpoint.parse().expect("checkpoint is an offset"))
    };
    reload.local_start = local_start;
}

#[when("the producer-state loader chooses its log start")]
fn choose_reload_start(reload: &mut SnapshotReload) {
    reload.start = Some(producer_snapshot_reload_log_start(
        reload.remote,
        reload.established,
        reload.local_start,
    ));
}

#[then("producer-state reload starts at offset {start}")]
fn reload_start(reload: &SnapshotReload, start: i64) {
    assert2::check!(reload.start == Some(start));
}

#[given("a producer reload range starting at {start} and ending at {end}")]
fn reload_range(reload: &mut SnapshotReload, start: i64, end: i64) {
    reload.range = Some(ProducerReloadRange {
        log_start: start,
        local_start: start,
        log_end: end,
    });
}

#[when("the loader considers snapshot offset {offset}")]
fn consider_snapshot(reload: &mut SnapshotReload, offset: i64) {
    reload.kept = Some(producer_snapshot_reload_keeps(
        offset,
        reload.range.expect("a reload range was given"),
    ));
}

#[then("the snapshot is retained for reload: {kept}")]
fn snapshot_kept(reload: &SnapshotReload, kept: bool) {
    assert2::check!(reload.kept == Some(kept));
}

#[given("snapshot offsets {snapshots} and segment bases {bases}")]
fn snapshot_inventory(reload: &mut SnapshotReload, snapshots: String, bases: String) {
    reload.offsets = offsets(&snapshots);
    reload.bases = offsets(&bases);
}

#[when("the loader classifies every snapshot as stray or retained")]
fn classify_snapshots(reload: &mut SnapshotReload) {
    reload.removed = Some(
        (0..reload.offsets.len())
            .map(|index| producer_snapshot_stray(&reload.offsets, index, &reload.bases))
            .collect(),
    );
}

#[then("the snapshot removal decisions are {decisions}")]
fn stray_decisions(reload: &SnapshotReload, decisions: String) {
    let expected: Vec<_> = decisions
        .split(',')
        .map(|decision| match decision {
            "remove" => true,
            "keep" => false,
            _ => panic!("unknown snapshot removal decision: {decision}"),
        })
        .collect();
    assert2::check!(reload.removed.as_ref() == Some(&expected));
}

#[given("available producer snapshot offsets {snapshots}")]
fn available_snapshots(reload: &mut SnapshotReload, snapshots: String) {
    reload.offsets = offsets(&snapshots);
}

#[when("the loader selects the latest usable snapshot")]
fn latest_snapshot(reload: &mut SnapshotReload) {
    reload.latest.set(producer_snapshot_latest_index(
        &reload.offsets,
        reload.range.expect("a reload range was given"),
    ));
}

#[then("the selected snapshot index is {selected}")]
fn selected_snapshot(reload: &SnapshotReload, selected: String) {
    let expected = if selected == "none" {
        None
    } else {
        Some(selected.parse().expect("snapshot index is an integer"))
    };
    assert2::check!(reload.latest.get() == Some(expected));
}

#[derive(DataTableRow)]
struct SnapshotEntryRow {
    snapshot_offset: i64,
    producer_id: i64,
    producer_epoch: i16,
    last_sequence: i32,
    last_offset: i64,
    offset_delta: i32,
    coordinator_epoch: i32,
    transaction_start: i64,
}

#[given("a producer snapshot contains this entry:")]
fn snapshot_entry(reload: &mut SnapshotReload, #[datatable] rows: Rows<SnapshotEntryRow>) {
    let mut rows = rows.into_iter();
    let row = rows.next().expect("one producer snapshot entry");
    assert2::assert!(rows.next().is_none());
    reload.snapshot_offset = row.snapshot_offset;
    reload.entry = Some(ProducerSnapshotEntryFacts {
        producer_id: row.producer_id,
        producer_epoch: row.producer_epoch,
        last_sequence: row.last_sequence,
        last_offset: row.last_offset,
        offset_delta: row.offset_delta,
        coordinator_epoch: row.coordinator_epoch,
        current_txn_first_offset: row.transaction_start,
    });
}

#[when("the producer snapshot entry is validated against its snapshot boundary")]
fn validate_entry(reload: &mut SnapshotReload) {
    reload.valid = Some(producer_snapshot_entry_valid(
        reload.snapshot_offset,
        reload.entry.expect("a producer snapshot entry was given"),
    ));
}

#[then("the snapshot entry is valid: {valid}")]
fn valid_entry(reload: &SnapshotReload, valid: bool) {
    assert2::check!(reload.valid == Some(valid));
}

scenarios!("tests/features/migrated/src/producer_snapshot/tests/reload_log_start_follows_kafka_log_loader.feature", fixtures = [reload: SnapshotReload]);
