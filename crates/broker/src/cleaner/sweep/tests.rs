//! Tests for one sweep: which partitions it compacts, which it skips for
//! leadership or cleanup policy, and how a KFC-9 topic write freeze and the
//! later thaw move a partition out of and back into eligibility.

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_metadata::{MetadataRecord, PatternType, TopicFreezeRecord};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::cleaner::test_support::{compactable_partition, record_count};

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
            let partition =
                compactable_partition(dir, topic, 0, NodeId(7), krabka_log::CleanupPolicy::Compact);
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
