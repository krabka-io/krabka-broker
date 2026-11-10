//! Steps for `tests/features/storage/local_retention_sweep.feature`: one
//! retention sweep trims a hosted replica whether or not this broker leads it.

use {
    super::*,
    crate::{
        cleaner::test_support::ReplicaRole,
        log_retention::test_support::{expired_partition, segment_files},
        test_support::CommittedPartitionSetup,
    },
    assert2::assert,
    krabka_bdd::{Given, given, then, when},
    krabka_ids::PartitionIndex,
    tempfile::TempDir,
};

/// The topic every scenario's replica belongs to.
const TOPIC: &str = "orders";

struct RetentionSweepScenario {
    dir: TempDir,
    registry: PartitionRegistry,
    /// The segment files on disk before the sweep.
    before: Given<Vec<String>>,
}

#[rstest::fixture]
fn retention_sweep() -> RetentionSweepScenario {
    RetentionSweepScenario {
        dir: tempfile::tempdir().expect("log root"),
        registry: PartitionRegistry::new(),
        before: Given::new(
            "before",
            "Given a <role> replica holds sealed segments past retention.ms",
        ),
    }
}

#[given("a {role} replica holds sealed segments past retention.ms")]
async fn expired_replica(retention_sweep: &mut RetentionSweepScenario, role: ReplicaRole) {
    let partition = expired_partition(&retention_sweep.dir, CommittedPartitionSetup {
        topic: TOPIC,
        leader: role.leader(),
        ..Default::default()
    })
    .await;
    retention_sweep
        .registry
        .insert(TOPIC.into(), PartitionIndex(0), partition);
    retention_sweep
        .before
        .set(segment_files(&retention_sweep.dir, TOPIC));
}

#[when("the local retention sweep runs")]
async fn retention_sweep_runs(retention_sweep: &mut RetentionSweepScenario) {
    tick_all(&retention_sweep.registry, None, &BrokerMetrics::new()).await;
}

#[then("the expired segments leave the disk")]
fn expired_segments_leave(retention_sweep: &RetentionSweepScenario) {
    let before = retention_sweep.before.get();
    let after = segment_files(&retention_sweep.dir, TOPIC);
    assert!(after.len() < before.len(), "{before:?} -> {after:?}");
}

krabka_bdd::scenarios!(
    "tests/features/storage/local_retention_sweep.feature",
    fixtures = [retention_sweep: RetentionSweepScenario],
);
