//! Steps for `tests/features/storage/log_compaction_sweep.feature`: which
//! hosted replicas one cleaner sweep compacts, by cleanup policy, by
//! leadership and by Kafka's cleanable test.

use {
    super::*,
    crate::{
        cleaner::test_support::{
            CompactionSetup, ReplicaRole, compactable_partition, record_count,
        },
        metrics::PartitionLabel,
    },
    assert2::assert,
    krabka_bdd::{Given, given, then, when},
    krabka_ids::PartitionIndex,
    krabka_log::{CleanupPolicy, LogConfig},
    krabka_metadata::NodeId,
    tempfile::TempDir,
};

/// The topic every scenario's replica belongs to.
const TOPIC: &str = "orders";

/// A topic's `cleanup.policy`, as Kafka writes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum Policy {
    #[step_param("compact")]
    Compact,
    #[step_param("delete")]
    Delete,
    #[step_param("compact,delete")]
    CompactAndDelete,
}

impl Policy {
    const fn cleanup_policy(self) -> CleanupPolicy {
        match self {
            Self::Compact => CleanupPolicy::Compact,
            Self::Delete => CleanupPolicy::Delete,
            Self::CompactAndDelete => CleanupPolicy::CompactAndDelete,
        }
    }
}

/// The replica a scenario sweeps, and its record count before the sweep.
struct HostedReplica {
    partition: Arc<Partition>,
    before: usize,
}

struct CompactionSweepScenario {
    dir: TempDir,
    registry: PartitionRegistry,
    metrics: BrokerMetrics,
    replica: Given<HostedReplica>,
}

#[rstest::fixture]
fn compaction_sweep() -> CompactionSweepScenario {
    CompactionSweepScenario {
        dir: tempfile::tempdir().expect("log root"),
        registry: PartitionRegistry::new(),
        metrics: BrokerMetrics::new(),
        replica: Given::new(
            "replica",
            "Given a <role> replica of a topic with cleanup.policy <policy>",
        ),
    }
}

impl CompactionSweepScenario {
    /// Host one replica that holds superseded values of one key.
    async fn host(&mut self, leader: NodeId, cfg: LogConfig) {
        let partition = compactable_partition()
            .root(&self.dir)
            .topic(TOPIC)
            .leader(leader)
            .cfg(cfg)
            .call()
            .await;
        let before = record_count(&partition);
        self.registry
            .insert(TOPIC.into(), PartitionIndex(0), Arc::clone(&partition));
        self.replica.set(HostedReplica { partition, before });
    }

    /// The replica's record count before the sweep and now.
    fn counts(&self) -> (usize, usize) {
        let HostedReplica { partition, before } = self.replica.get();
        (*before, record_count(partition))
    }
}

#[given("a {role} replica of a topic with cleanup.policy {policy}")]
async fn replica_with_policy(
    compaction_sweep: &mut CompactionSweepScenario,
    role: ReplicaRole,
    policy: Policy,
) {
    let cfg = LogConfig {
        cleanup_policy: policy.cleanup_policy(),
        ..CompactionSetup::default().cfg
    };
    compaction_sweep.host(role.leader(), cfg).await;
}

#[given("a compacted replica with min.cleanable.dirty.ratio {ratio}")]
async fn replica_with_dirty_ratio(compaction_sweep: &mut CompactionSweepScenario, ratio: f64) {
    let cfg = LogConfig {
        min_cleanable_dirty_ratio: krabka_units::fraction(ratio),
        ..CompactionSetup::default().cfg
    };
    compaction_sweep
        .host(ReplicaRole::Leader.leader(), cfg)
        .await;
}

#[given("its records are older than a max.compaction.lag.ms of {lag} millisecond(s)")]
fn records_older_than_max_lag(compaction_sweep: &mut CompactionSweepScenario, lag: u32) {
    let log = compaction_sweep
        .replica
        .get()
        .partition
        .lock_log()
        .expect("partition log lock");
    let cfg = LogConfig {
        max_compaction_lag: Some(krabka_units::millis(lag)),
        ..log.config_snapshot()
    };
    log.set_config(cfg);
}

#[when("the log cleaner sweeps the broker")]
async fn cleaner_sweeps(compaction_sweep: &mut CompactionSweepScenario) {
    tick_all(
        &compaction_sweep.registry,
        None,
        &compaction_sweep.metrics,
        &mut UncleanablePartitions::default(),
    )
    .await;
}

#[then("the superseded values leave the replica")]
fn superseded_values_leave(compaction_sweep: &CompactionSweepScenario) {
    let (before, after) = compaction_sweep.counts();
    assert!(after < before);
}

#[then("the replica keeps every record")]
fn replica_keeps_every_record(compaction_sweep: &CompactionSweepScenario) {
    let (before, after) = compaction_sweep.counts();
    assert!(after == before);
}

/// What the cleaner's metrics say about one sweep: passes it counted, and
/// compactions of the scenario's partition.
#[derive(Debug, PartialEq, Eq)]
struct SweepAccount {
    runs: u64,
    compactions: u64,
}

#[then("the cleaner counts one sweep and no compaction")]
fn one_sweep_no_compaction(compaction_sweep: &CompactionSweepScenario) {
    let metrics = &compaction_sweep.metrics;
    let compactions = metrics
        .log_compactions_total
        .get_or_create(&PartitionLabel {
            topic: TOPIC.into(),
            partition: 0,
        })
        .get();
    let account = SweepAccount {
        runs: metrics.log_cleaner_runs_total.get(),
        compactions,
    };
    assert!(
        account
            == SweepAccount {
                runs: 1,
                compactions: 0,
            }
    );
}

krabka_bdd::scenarios!(
    "tests/features/storage/log_compaction_sweep.feature",
    fixtures = [compaction_sweep: CompactionSweepScenario],
);
