//! Scenarios for `tests/features/damaged_segments`: what
//! `--continue-on-corrupt` skips, and what stops the restore instead.
#![cfg(test)]

#[path = "support/archived_log.rs"]
mod archived_log;

use {
    archived_log::{archive_segment, restore_args},
    krabka_bdd::Given,
    krabka_object_store::fault::{
        FaultKind, FaultPolicy, OpFault, StoreOp, build_faulty_object_store,
    },
    krabka_restore::{
        ArchiveStore, EXIT_ARCHIVE_UNREADABLE, RestoreError, RestoreReport, object_store_config,
        restore_with,
    },
    tempfile::TempDir,
    tokio::runtime::{Builder, Runtime},
    uuid::Uuid,
};

const RESTORED: &str = "When an operator restores the archive, continuing past corrupt segments";

/// The S3 answer every read from the archive gets.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Fault {
    #[step_param("NoSuchKey")]
    NoSuchKey,
    #[step_param("SlowDown")]
    SlowDown,
    #[step_param("AccessDenied")]
    AccessDenied,
}

impl Fault {
    const fn kind(self) -> FaultKind {
        match self {
            Self::NoSuchKey => FaultKind::NotFound,
            Self::SlowDown => FaultKind::Throttled,
            Self::AccessDenied => FaultKind::AccessDenied,
        }
    }
}

/// An archive on local disk, a target beside it, and the faults the archive
/// answers reads with.
struct DamagedSegmentScenario {
    runtime: Runtime,
    archive: TempDir,
    target: TempDir,
    reads: FaultPolicy,
    segment_id: Given<Uuid>,
    outcome: Given<Result<RestoreReport, RestoreError>>,
}

impl DamagedSegmentScenario {
    /// Restore the archive with `--continue-on-corrupt`, through a store that
    /// answers reads by the scenario's faults.
    fn restore(&self) -> Result<RestoreReport, RestoreError> {
        let args = restore_args(
            self.archive.path(),
            &self.target.path().join("restored"),
            &["--continue-on-corrupt"],
        );
        let reads = self.reads.clone();
        self.runtime.block_on(restore_with(&args, move |args| {
            let store = build_faulty_object_store(&object_store_config(args)?, reads)?;
            Ok(ArchiveStore::with_store(store, None))
        }))
    }
}

#[rstest::fixture]
fn damaged_segment() -> DamagedSegmentScenario {
    DamagedSegmentScenario {
        runtime: Builder::new_multi_thread().enable_all().build().unwrap(),
        archive: tempfile::tempdir().unwrap(),
        target: tempfile::tempdir().unwrap(),
        reads: FaultPolicy::none(),
        segment_id: Given::new("segment_id", "Given a partition has an archived segment"),
        outcome: Given::new("outcome", RESTORED),
    }
}

#[rstest_bdd_macros::given("a partition has an archived segment")]
fn archived_segment(damaged_segment: &mut DamagedSegmentScenario) {
    let segment_id = Uuid::from_u128(0x5E6);
    archive_segment(
        damaged_segment.archive.path(),
        "orders",
        0,
        Uuid::from_u128(0x0D),
        segment_id,
    );
    damaged_segment.segment_id.set(segment_id);
}

#[rstest_bdd_macros::given("the archive answers every read with {fault}")]
fn every_read_fails(damaged_segment: &mut DamagedSegmentScenario, fault: Fault) {
    damaged_segment.reads = FaultPolicy::none().with(StoreOp::Get, OpFault::always(fault.kind()));
}

#[rstest_bdd_macros::when("an operator restores the archive, continuing past corrupt segments")]
fn restores(damaged_segment: &mut DamagedSegmentScenario) {
    let outcome = damaged_segment.restore();
    damaged_segment.outcome.set(outcome);
}

#[rstest_bdd_macros::when(
    "an operator attempts to restore the archive, continuing past corrupt segments"
)]
fn attempts_to_restore(damaged_segment: &mut DamagedSegmentScenario) { restores(damaged_segment); }

#[rstest_bdd_macros::then("the report names the segment as skipped")]
fn segment_skipped(damaged_segment: &DamagedSegmentScenario) {
    assert2::assert!(let Ok(report) = damaged_segment.outcome.get());
    let skipped: Vec<(&str, i32, Uuid)> = report
        .skipped
        .iter()
        .map(|segment| {
            (
                segment.topic.as_str(),
                segment.partition,
                segment.segment_id,
            )
        })
        .collect();
    let segment_id = *damaged_segment.segment_id.get();
    assert2::assert!(skipped == [("orders", 0, segment_id)]);
}

#[rstest_bdd_macros::then("the restore stops because the archive cannot be read")]
fn archive_unreadable(damaged_segment: &DamagedSegmentScenario) {
    assert2::assert!(let Err(error) = damaged_segment.outcome.get());
    assert2::assert!(error.exit_code() == EXIT_ARCHIVE_UNREADABLE, "{error}");
}

krabka_bdd::scenarios!(
    "tests/features/damaged_segments",
    fixtures = [damaged_segment: DamagedSegmentScenario]
);
