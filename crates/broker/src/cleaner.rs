//! Per-broker log-compaction ticker.
//!
//! Every `interval`, it walks the partitions registry and dispatches
//! [`Partition::compact_log`] for every partition where all of these hold:
//!
//!   - the topic's `cleanup.policy` is `compact`,
//!   - this broker is currently the leader, and
//!   - no KFC-9 write freeze covers the topic.
//!
//! The compaction itself runs on the partition's writer actor, so appends and
//! compaction run in sequence.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use krabka_metadata::{MetadataImage, NodeId};
use krabka_units::{Time, convert::TimeExt as _};
use qubit_clock::sleep::{AsyncSleeper, SystemSleeper};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    freeze::resolve::resolve_topic_freeze, metrics::BrokerMetrics, partition::Partition,
    partition_registry::PartitionRegistry,
};

/// Tunables for [`run`].
#[derive(Clone)]
pub(crate) struct CleanerConfig {
    pub interval: Time,
    /// Relative sleeper that drives the compaction-sweep cadence. Production
    /// uses [`qubit_clock::sleep::SystemSleeper`], which is real time. Tests
    /// inject a [`qubit_clock::sleep::MockSleeper`], so the sweep interval
    /// fires on a controlled mock timeline instead of wall-clock time.
    pub sleeper: Arc<dyn AsyncSleeper>,
    /// The metadata authority the sweep reads the KFC-9 write-freeze registry
    /// from, re-read once per sweep so a freeze and a thaw both take effect on
    /// the next tick.
    ///
    /// `None` is a sweep with no metadata authority to ask. It resolves no
    /// freeze and leaves every partition eligible, which is the answer an
    /// empty registry gives anyway.
    pub metadata: Option<Arc<dyn crate::metadata_source::MetadataSource>>,
}

impl CleanerConfig {
    pub(crate) fn system(interval: Time) -> Self {
        Self {
            interval,
            sleeper: Arc::new(SystemSleeper::new()),
            metadata: None,
        }
    }
}

/// Spawned task entry point.
pub(crate) async fn run(
    partitions: Arc<PartitionRegistry>,
    node_id: NodeId,
    cfg: CleanerConfig,
    shutdown: CancellationToken,
    metrics: BrokerMetrics,
) {
    // Drive the sweep cadence through the injected `AsyncSleeper` (production:
    // real time; tests: a controlled mock timeline). A zero-duration first sleep
    // reproduces `tokio::time::interval`'s immediate t=0 tick, so the first sweep
    // fires at startup; each subsequent sleep is re-armed to `cfg.interval` only
    // after the sweep completes (`MissedTickBehavior::Delay` semantics — a slow
    // sweep never triggers a catch-up burst). The sleeper is cloned into a local
    // so the tick future borrows it rather than `cfg`, leaving `cfg` free.
    let sleeper = cfg.sleeper.clone();
    let mut tick = sleeper.sleep_for_async(Duration::ZERO);
    loop {
        tokio::select! {
            () = &mut tick => {
                // One image read per sweep. The registry the sweep gates on is
                // whatever the metadata authority holds when the tick starts,
                // so a freeze applied mid-sweep takes effect on the next one.
                let image = cfg.metadata.as_ref().map(|source| source.current_image());
                tick_all(&partitions, image.as_deref(), node_id, &metrics).await;
                tick = sleeper.sleep_for_async(cfg.interval.to_std());
            }
            () = shutdown.cancelled() => {
                debug!("cleaner task shutting down");
                return;
            }
        }
    }
}

/// Whether a KFC-9 write freeze stops this sweep from compacting `topic`.
///
/// Compaction removes records, and the KFC's rule refuses every operation that
/// removes data from a frozen topic's log. A disaster-recovery promotion needs
/// the frozen prefix byte-identical between the two sites, and one cleaner run
/// on one side leaves the same offsets holding different bytes.
///
/// `image` is `None` for a sweep with no metadata authority to ask, which
/// resolves no freeze.
fn freeze_stops_compaction(image: Option<&MetadataImage>, topic: &str) -> bool {
    image.is_some_and(|image| resolve_topic_freeze(image, topic).is_some())
}

pub(crate) async fn tick_all(
    partitions: &PartitionRegistry,
    image: Option<&MetadataImage>,
    node_id: NodeId,
    metrics: &BrokerMetrics,
) {
    // Snapshot first to avoid holding any registry guard across await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    for partition in snapshot {
        let leader = partition.current_leader.load(Ordering::Relaxed);
        if leader != node_id {
            continue;
        }
        let policy = {
            // Recover the guard if the mutex was poisoned by a panic
            // elsewhere rather than killing the (discarded-JoinHandle)
            // cleaner task. The config snapshot stays readable.
            let log = partition
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            log.config_snapshot().cleanup_policy
        };
        if policy != krabka_log::CleanupPolicy::Compact {
            continue;
        }
        // KFC-9, beside the policy test and skipping in exactly the same way.
        // A skip is the right shape here rather than an error: the cleaner is
        // a background loop with no caller to refuse, so a frozen topic simply
        // has no work for it, and the partition becomes eligible again on the
        // first sweep after the thaw leaves the image, with no operator step.
        if freeze_stops_compaction(image, &partition.topic) {
            continue;
        }
        match partition.compact_log().await {
            Ok(()) => {
                metrics.record_compaction(&partition.topic, partition.index.get());
            }
            Err(e) => {
                warn!(
                    topic = %partition.topic,
                    partition_id = partition.index.get(),
                    error = %e,
                    "compaction failed for partition",
                );
            }
        }
    }
    // One increment per completed sweep, whether or not any partition was
    // eligible, so a test that seals a segment can poll this counter to
    // confirm a full pass ran after the seal (see `wait_for_metrics`).
    metrics.record_cleaner_run();
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use assert2::{assert, check};
    use bytes::Bytes;
    use krabka_ids::PartitionIndex;
    use krabka_metadata::{MetadataRecord, PatternType, TopicFreezeRecord};
    use krabka_protocol::records::{Record, RecordBatch};
    use krabka_units::secs;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;

    fn keyed_batch(base: i64, key: &[u8], value: &[u8]) -> RecordBatch {
        RecordBatch {
            base_offset: base,
            records: vec![Record {
                offset_delta: 0,
                key: Some(Bytes::copy_from_slice(key)),
                value: Some(Bytes::copy_from_slice(value)),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn compactable_partition(
        root: &TempDir,
        topic: &str,
        partition_id: i32,
        leader: NodeId,
        cleanup_policy: krabka_log::CleanupPolicy,
    ) -> Arc<Partition> {
        let part_dir = crate::log_dir::partition_dir(root.path(), topic, partition_id);
        std::fs::create_dir_all(&part_dir).expect("create partition dir");
        let cfg = krabka_log::LogConfig {
            cleanup_policy,
            segment_size: krabka_units::bytes(256),
            ..Default::default()
        };
        let mut log = krabka_log::Log::open(&part_dir, cfg).expect("open compactable log");
        for idx in 0..12 {
            let mut batch = keyed_batch(idx, b"duplicate-key", format!("v{idx}").as_bytes());
            log.append(&mut batch).expect("append duplicate-key batch");
        }
        let mut active = keyed_batch(12, b"active-key", b"active");
        log.append(&mut active).expect("append active batch");

        let part = crate::broker::spawn_partition(
            topic.to_string(),
            PartitionIndex(partition_id),
            root.path().to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        part.current_leader.store(leader.0, Ordering::Relaxed);
        part
    }

    fn record_count(partition: &Partition) -> usize {
        let read = partition
            .log
            .lock()
            .expect("partition log lock")
            .read(krabka_log::Offset(0), krabka_units::mebibytes(1))
            .expect("read partition log");
        read.batches.iter().map(|batch| batch.records.len()).sum()
    }

    #[tokio::test]
    async fn tick_all_compacts_only_local_leader_compact_topics() {
        let dir = tempfile::tempdir().expect("log root");
        let registry = PartitionRegistry::new();
        // (topic, leader, cleanup_policy, expect_compacted): only partitions
        // led locally (leader 7) with the Compact policy should shrink.
        let specs = [
            ("local-compact", 7, krabka_log::CleanupPolicy::Compact, true),
            (
                "follower-compact",
                8,
                krabka_log::CleanupPolicy::Compact,
                false,
            ),
            ("local-delete", 7, krabka_log::CleanupPolicy::Delete, false),
        ];
        let cases: Vec<_> = specs
            .into_iter()
            .map(|(topic, leader, policy, expect_compacted)| {
                let partition = compactable_partition(&dir, topic, 0, NodeId(leader), policy);
                let before = record_count(&partition);
                registry.insert(topic.to_string(), PartitionIndex(0), Arc::clone(&partition));
                (topic, partition, before, expect_compacted)
            })
            .collect();

        let metrics = BrokerMetrics::new();
        tick_all(&registry, None, NodeId(7), &metrics).await;

        // A single `tick_all` is exactly one cleaner sweep, so the run counter
        // must advance by one. This pins `record_cleaner_run` against a no-op
        // mutation (nothing else asserts on `log_cleaner_runs_total`).
        assert_eq!(metrics.log_cleaner_runs_total.get(), 1);

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

    #[tokio::test]
    async fn run_ticks_until_shutdown() {
        use qubit_clock::{MockWaiterKind, sleep::MockSleeper};

        let dir = tempfile::tempdir().expect("log root");
        let registry = Arc::new(PartitionRegistry::new());
        let partition = compactable_partition(
            &dir,
            "run-compact",
            0,
            NodeId(7),
            krabka_log::CleanupPolicy::Compact,
        );
        let before = record_count(&partition);
        registry.insert(
            "run-compact".to_string(),
            PartitionIndex(0),
            Arc::clone(&partition),
        );

        // Drive the sweep cadence on a mock timeline instead of wall-clock time.
        let interval = secs(30);
        let sleeper = MockSleeper::new();
        let timeline = sleeper.timeline();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            Arc::clone(&registry),
            NodeId(7),
            CleanerConfig {
                interval,
                sleeper: Arc::new(sleeper),
                metadata: None,
            },
            shutdown.clone(),
            BrokerMetrics::new(),
        ));

        // The immediate t=0 tick runs a compaction sweep, then the loop re-arms
        // on `sleep_for_async(interval)`. Block (bounded real time, hang-guard
        // only) until that interval-sleep waiter is parked — it registers
        // strictly after the first sweep's `tick_all` returns, so the compaction
        // is fully applied by then. `wait_for_blocked_waiters` runs on a blocking
        // thread so it never stalls the current-thread runtime that must drive
        // the cleaner task and the partition writer actor to completion.
        let tl = timeline.clone();
        let parked = tokio::task::spawn_blocking(move || {
            tl.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(
            parked,
            "cleaner should park on the interval sleep after the first sweep"
        );
        assert!(
            record_count(&partition) < before,
            "immediate first sweep should compact the eligible partition"
        );

        // Advance one interval to fire a second sweep, then confirm the loop
        // re-parks — proving it keeps ticking on the injected cadence with no
        // wall-clock time (the second sweep is idempotent, so the log stays
        // compacted rather than shrinking further).
        timeline.advance(interval.to_std());
        let tl = timeline.clone();
        let parked_again = tokio::task::spawn_blocking(move || {
            tl.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(
            parked_again,
            "cleaner should re-park on the interval sleep after the second sweep"
        );
        assert!(record_count(&partition) < before, "log stays compacted");

        shutdown.cancel();
        task.await.expect("cleaner task exits");
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
    fn compactable_topics(
        dir: &TempDir,
        registry: &PartitionRegistry,
        topics: &[&'static str],
    ) -> Vec<(&'static str, Arc<Partition>, usize)> {
        topics
            .iter()
            .map(|&topic| {
                let partition = compactable_partition(
                    dir,
                    topic,
                    0,
                    NodeId(7),
                    krabka_log::CleanupPolicy::Compact,
                );
                let before = record_count(&partition);
                registry.insert(topic.to_string(), PartitionIndex(0), Arc::clone(&partition));
                (topic, partition, before)
            })
            .collect()
    }

    #[tokio::test]
    async fn tick_all_skips_a_frozen_partition_and_compacts_an_unfrozen_control() {
        let dir = tempfile::tempdir().expect("log root");
        let registry = PartitionRegistry::new();
        let cases = compactable_topics(
            &dir,
            &registry,
            &["frozen-literal", "tenant-a.orders", "unfrozen"],
        );
        // One image, one sweep. The control partition is compactable in
        // exactly the same way as the two frozen ones, so a sweep that
        // compacted nothing at all could not pass this test.
        let image = image_with_freezes(&[
            ("frozen-literal", PatternType::Literal),
            ("tenant-a.", PatternType::Prefixed),
        ]);

        tick_all(&registry, Some(&image), NodeId(7), &BrokerMetrics::new()).await;

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
        let cases = compactable_topics(&dir, &registry, &["orders"]);
        let (_, partition, before) = &cases[0];
        let mut image = image_with_freezes(&[("orders", PatternType::Literal)]);
        let metrics = BrokerMetrics::new();

        tick_all(&registry, Some(&image), NodeId(7), &metrics).await;
        check!(
            record_count(partition) == *before,
            "the frozen sweep removes no record"
        );

        thaw(&mut image, "orders", PatternType::Literal);
        tick_all(&registry, Some(&image), NodeId(7), &metrics).await;

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
}
