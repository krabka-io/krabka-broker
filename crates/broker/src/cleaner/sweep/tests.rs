//! Tests for one sweep: which partitions it compacts, which it skips for
//! cleanup policy or for a dirty region too small to be worth a pass, and how
//! a KFC-9 topic write freeze and the later thaw move a partition out of and
//! back into eligibility.

use std::sync::atomic::Ordering;

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_metadata::{MetadataRecord, NodeId, PatternType, TopicFreezeRecord};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::{
    cleaner::test_support::{
        block_compaction_swap, compactable_partition, compactable_partition_in_registry,
        compactable_partition_with_config, record_count,
    },
    metrics::{CleanerFailureLabel, CleanerFailureReason},
};

/// The cleanup policy is the only thing that makes a partition the cleaner's
/// work. Leadership is not: Kafka's `LogCleanerManager` walks every log the
/// broker holds, so the replica this broker follows (leader 8) is compacted
/// beside the one it leads. Skipping it left a compacted topic's followers
/// with no bound on their segment count at all, because `cleanup.policy=
/// compact` sets no `retention.ms` for the local-retention sweep to act on.
#[tokio::test]
async fn tick_all_compacts_every_hosted_compact_topic_whoever_leads_it() {
    let dir = tempfile::tempdir().expect("log root");
    let registry = PartitionRegistry::new();
    // (topic, leader, cleanup_policy, expect_compacted): every partition with
    // the Compact policy should shrink, led here (7) or not (8).
    let specs = [
        ("local-compact", 7, krabka_log::CleanupPolicy::Compact, true),
        (
            "follower-compact",
            8,
            krabka_log::CleanupPolicy::Compact,
            true,
        ),
        ("local-delete", 7, krabka_log::CleanupPolicy::Delete, false),
        (
            "follower-delete",
            8,
            krabka_log::CleanupPolicy::Delete,
            false,
        ),
        // Kafka derives `compact` from the policy list by membership, so a
        // `compact,delete` topic is the cleaner's work exactly as a `compact`
        // one is. Kafka Streams writes that value on every windowed-store
        // changelog topic.
        (
            "local-compact-and-delete",
            7,
            krabka_log::CleanupPolicy::CompactAndDelete,
            true,
        ),
    ];
    let mut cases = Vec::new();
    for (topic, leader, policy, expect_compacted) in specs {
        let partition = compactable_partition(&dir, topic, 0, NodeId(leader), policy).await;
        let before = record_count(&partition);
        registry.insert(topic.into(), PartitionIndex(0), Arc::clone(&partition));
        cases.push((topic, partition, before, expect_compacted));
    }

    let metrics = BrokerMetrics::new();
    tick_all(
        &registry,
        None,
        &metrics,
        &mut UncleanablePartitions::default(),
    )
    .await;

    // A single `tick_all` is exactly one cleaner sweep, so the run counter
    // must advance by one. This pins `record_cleaner_run` against a no-op
    // mutation (nothing else asserts on `log_cleaner_runs_total`).
    assert2::assert!((metrics.log_cleaner_runs_total.get()) == (1));

    for (topic, partition, before, expect_compacted) in cases {
        let after = record_count(&partition);
        let count_ok = if expect_compacted {
            after < before
        } else {
            after == before
        };
        assert!(
            count_ok,
            "case: {topic} (before={before}, after={after}, expect_compacted={expect_compacted})"
        );
    }
}

/// Kafka's cleanable test, from the sweep's side: a partition whose dirty
/// region is too small a share of the log earns no pass, and the same
/// partition earns one once `max.compaction.lag.ms` has elapsed over it.
#[tokio::test]
async fn tick_all_skips_a_partition_below_its_dirty_ratio_until_the_max_lag() {
    let dir = tempfile::tempdir().expect("log root");
    let registry = PartitionRegistry::new();
    let base = krabka_log::LogConfig {
        cleanup_policy: krabka_log::CleanupPolicy::Compact,
        segment_size: krabka_units::bytes(256),
        // No share of the log short of all of it is worth a pass.
        min_cleanable_dirty_ratio: krabka_units::fraction(1.0),
        ..Default::default()
    };
    let partition =
        compactable_partition_with_config(&dir, "too-clean", 0, NodeId(7), base.clone()).await;
    let before = record_count(&partition);
    registry.insert(
        "too-clean".into(),
        PartitionIndex(0),
        Arc::clone(&partition),
    );

    let metrics = BrokerMetrics::new();
    tick_all(
        &registry,
        None,
        &metrics,
        &mut UncleanablePartitions::default(),
    )
    .await;
    assert!(
        record_count(&partition) == before,
        "ratio must hold it back"
    );

    // The partition's records are older than a one-millisecond max lag, so
    // the next sweep owes it a pass however clean the ratio says it is.
    partition
        .log
        .lock()
        .expect("partition log lock")
        .set_config(krabka_log::LogConfig {
            max_compaction_lag: Some(krabka_units::millis(1)),
            ..base
        });
    tick_all(
        &registry,
        None,
        &metrics,
        &mut UncleanablePartitions::default(),
    )
    .await;
    assert!(
        record_count(&partition) < before,
        "the max lag must force a pass"
    );
}

// ── KFC-9 topic write freeze ─────────────────────────────────────

/// An image holding one live freeze entry per `(scope, pattern_type)`.
fn image_with_freezes(scopes: &[(&str, PatternType)]) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::from_u128(0x5150));
    for &(scope, pattern_type) in scopes {
        image.apply(&MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
            scope: scope.to_owned(),
            pattern_type,
            frozen: true,
            reason: "DR cutover".to_owned(),
            set_by: "User:alice".to_owned(),
            set_at_ms: 1_770_000_000_000,
            proposal_id: Uuid::nil(),
            key_id: String::new(),
            signature: Vec::new(),
        }));
    }
    image
}

/// The thaw record for `scope`: the same entry with `frozen` cleared,
/// which is what removes it from the registry.
fn thaw(image: &mut MetadataImage, scope: &str, pattern_type: PatternType) {
    image.apply(&MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
        scope: scope.to_owned(),
        pattern_type,
        frozen: false,
        reason: String::new(),
        set_by: "User:bob".to_owned(),
        set_at_ms: 1_770_000_100_000,
        proposal_id: Uuid::from_u128(7),
        key_id: String::new(),
        signature: Vec::new(),
    }));
}

/// Register one compactable, locally-led partition per topic and report
/// each one's pre-sweep record count beside it.
async fn compactable_topics(
    dir: &TempDir,
    registry: &PartitionRegistry,
    topics: &[&'static str],
) -> Vec<(&'static str, Arc<Partition>, usize)> {
    let mut built = Vec::new();
    for &topic in topics {
        let partition =
            compactable_partition(dir, topic, 0, NodeId(7), krabka_log::CleanupPolicy::Compact)
                .await;
        let before = record_count(&partition);
        registry.insert(topic.into(), PartitionIndex(0), Arc::clone(&partition));
        built.push((topic, partition, before));
    }
    built
}

#[tokio::test]
async fn tick_all_skips_a_frozen_partition_and_compacts_an_unfrozen_control() {
    let dir = tempfile::tempdir().expect("log root");
    let registry = PartitionRegistry::new();
    let cases = compactable_topics(
        &dir,
        &registry,
        &["frozen-literal", "tenant-a.orders", "unfrozen"],
    )
    .await;
    // One image, one sweep. The control partition is compactable in
    // exactly the same way as the two frozen ones, so a sweep that
    // compacted nothing at all could not pass this test.
    let image = image_with_freezes(&[
        ("frozen-literal", PatternType::Literal),
        ("tenant-a.", PatternType::Prefixed),
    ]);

    tick_all(
        &registry,
        Some(&image),
        &BrokerMetrics::new(),
        &mut UncleanablePartitions::default(),
    )
    .await;

    // (topic, whether the sweep should have compacted it)
    let expected = [
        ("frozen-literal", false),
        ("tenant-a.orders", false),
        ("unfrozen", true),
    ];
    for ((topic, partition, before), (label, expect_compacted)) in cases.iter().zip(expected) {
        check!(*topic == label);
        check!(
            (record_count(partition) < *before) == expect_compacted,
            "{label}"
        );
    }
}

#[tokio::test]
async fn tick_all_compacts_again_once_the_thaw_leaves_the_image() {
    let dir = tempfile::tempdir().expect("log root");
    let registry = PartitionRegistry::new();
    let cases = compactable_topics(&dir, &registry, &["orders"]).await;
    let (_, partition, before) = &cases[0];
    let mut image = image_with_freezes(&[("orders", PatternType::Literal)]);
    let metrics = BrokerMetrics::new();

    tick_all(
        &registry,
        Some(&image),
        &metrics,
        &mut UncleanablePartitions::default(),
    )
    .await;
    check!(
        record_count(partition) == *before,
        "the frozen sweep removes no record"
    );

    thaw(&mut image, "orders", PatternType::Literal);
    tick_all(
        &registry,
        Some(&image),
        &metrics,
        &mut UncleanablePartitions::default(),
    )
    .await;

    check!(
        record_count(partition) < *before,
        "the sweep after the thaw compacts with no operator action"
    );
}

#[test]
fn freeze_stops_compaction_reads_the_registry_the_produce_path_reads() {
    let image = image_with_freezes(&[
        ("orders", PatternType::Literal),
        ("tenant-a.", PatternType::Prefixed),
    ]);

    for (label, topic, want) in [
        (
            "a literal freeze covers the one topic it names",
            "orders",
            true,
        ),
        (
            "a prefix freeze covers every topic under it",
            "tenant-a.billing",
            true,
        ),
        (
            "an unfrozen topic keeps its cleaner eligibility",
            "events",
            false,
        ),
        (
            "an internal topic is never frozen, so it stays compactable",
            "__consumer_offsets",
            false,
        ),
    ] {
        check!(
            freeze_stops_compaction(Some(&image), topic) == want,
            "{label}"
        );
        check!(
            !freeze_stops_compaction(None, topic),
            "{label}: a sweep with no metadata authority resolves no freeze"
        );
    }
}

// ── A cleaner that cannot clean ──────────────────────────────────

/// How many times the cleaner failed `topic-0` for `reason`.
fn failures(metrics: &BrokerMetrics, topic: &str, reason: CleanerFailureReason) -> u64 {
    metrics
        .log_cleaner_failures
        .get_or_create(&CleanerFailureLabel {
            topic: topic.into(),
            partition: 0,
            reason,
        })
        .get()
}

/// A compaction that fails against the disk is the failure mode with no other
/// signal: the topic is compacted, so local retention deletes nothing either,
/// and the dir fills while `kafka-log-dirs --describe` still calls it online.
/// The sweep has to account it, refuse to count itself as a pass, and leave
/// the partition marked uncleanable until a pass succeeds.
#[tokio::test]
async fn tick_all_accounts_a_failed_compaction_and_takes_the_log_dir_offline() {
    let dir = tempfile::tempdir().expect("log root");
    let status = crate::log_dir_status::LogDirRegistry::probe(&[dir.path().to_path_buf()]);
    let registry = PartitionRegistry::new();
    let partition =
        compactable_partition_in_registry(&dir, "orders", NodeId(7), status.clone()).await;
    let before = record_count(&partition);
    registry.insert("orders".into(), PartitionIndex(0), Arc::clone(&partition));
    // A directory where the rewrite must create its `.swap` file: the open
    // fails with EISDIR, which is a storage error the filesystem raises.
    let blocked = block_compaction_swap(&dir, "orders");

    let metrics = BrokerMetrics::new();
    let mut uncleanable = UncleanablePartitions::default();
    tick_all(&registry, None, &metrics, &mut uncleanable).await;

    check!(record_count(&partition) == before, "nothing was compacted");
    check!(failures(&metrics, "orders", CleanerFailureReason::Io) == 1);
    check!(
        metrics.log_cleaner_runs_total.get() == 0,
        "a sweep that failed every partition it swept is not a pass"
    );
    check!(metrics.log_cleaner_uncleanable_partitions.get() == 1);
    check!(
        status.is_offline(dir.path()),
        "a background write failure takes the dir offline, as a produce would"
    );

    // The same partition, once the disk takes the write again: the sweep
    // compacts it, drops the uncleanable mark, and counts itself.
    for swap in blocked {
        std::fs::remove_dir(&swap).expect("unblock the swap path");
    }
    tick_all(&registry, None, &metrics, &mut uncleanable).await;

    check!(record_count(&partition) < before, "the retry compacted");
    check!(failures(&metrics, "orders", CleanerFailureReason::Io) == 1);
    check!(metrics.log_cleaner_runs_total.get() == 1);
    check!(metrics.log_cleaner_uncleanable_partitions.get() == 0);
}

/// A partition this broker stops hosting is not the cleaner's to clean, so it
/// leaves the uncleanable set rather than holding the gauge above zero for as
/// long as the process lives.
///
/// Losing the leadership is no longer one of those exits: the sweep cleans
/// every hosted replica, so a partition this broker follows is still its
/// cleaner's problem and stays counted until a pass succeeds on it.
#[tokio::test]
async fn a_partition_this_broker_stops_hosting_leaves_the_uncleanable_set() {
    let dir = tempfile::tempdir().expect("log root");
    let status = crate::log_dir_status::LogDirRegistry::probe(&[dir.path().to_path_buf()]);
    let registry = PartitionRegistry::new();
    let partition = compactable_partition_in_registry(&dir, "orders", NodeId(7), status).await;
    registry.insert("orders".into(), PartitionIndex(0), Arc::clone(&partition));
    let blocked = block_compaction_swap(&dir, "orders");

    let metrics = BrokerMetrics::new();
    let mut uncleanable = UncleanablePartitions::default();
    tick_all(&registry, None, &metrics, &mut uncleanable).await;
    check!(metrics.log_cleaner_uncleanable_partitions.get() == 1);

    // Another broker takes the leadership. The replica stays here, so the
    // cleaner still owes it a pass and the gauge still counts it.
    partition.current_leader.store(8, Ordering::Relaxed);
    tick_all(&registry, None, &metrics, &mut uncleanable).await;
    check!(
        metrics.log_cleaner_uncleanable_partitions.get() == 1,
        "a follower replica is this cleaner's work too"
    );
    check!(
        metrics.log_cleaner_runs_total.get() == 0,
        "a sweep that failed the one partition it swept is not a pass"
    );

    // The replica is reassigned away from this broker, which is what takes
    // the partition out of the registry the sweep walks.
    registry.remove("orders", PartitionIndex(0));
    tick_all(&registry, None, &metrics, &mut uncleanable).await;

    check!(
        metrics.log_cleaner_uncleanable_partitions.get() == 0,
        "a partition this broker no longer hosts is nobody's here to clean"
    );
    check!(
        metrics.log_cleaner_runs_total.get() == 1,
        "the sweep that swept nothing is a pass"
    );
    drop(blocked);
}

/// The reason label is the error class an operator acts on, so the mapping is
/// pinned rather than left to whichever error the sweep happened to see.
#[test]
fn failure_reason_separates_a_disk_from_a_dead_writer() {
    let cases = [
        (
            "an io error from the log layer is the disk",
            BrokerError::Log(krabka_log::LogError::Io(std::io::Error::other("EIO"))),
            CleanerFailureReason::Io,
        ),
        (
            "a dead writer actor reached no disk at all",
            BrokerError::Replication("partition writer dead".into()),
            CleanerFailureReason::Writer,
        ),
        (
            "anything else the log refused",
            BrokerError::Log(krabka_log::LogError::Corrupt("bad crc".into())),
            CleanerFailureReason::Other,
        ),
    ];
    for (label, error, want) in cases {
        check!(failure_reason(&error) == want, "{label}");
    }
}
