//! The KIP-966 state machine: what one partition's ELR becomes when a change
//! applies, and the `V1TopicConfig` records the publisher appends for it.

use assert2::assert;
use krabka_metadata::{
    LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord,
    TopicRecord,
};

use super::{ElrPublisher, next_partition_elr};
use crate::{
    config_keys::{ELIGIBLE_LEADER_REPLICAS, MIN_INSYNC_REPLICAS, RETENTION_MS},
    elr::state::PartitionElr,
};

const TOPIC: &str = "orders";

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn partition(leader: u64, replicas: &[u64], isr: &[u64]) -> PartitionRecord {
    PartitionRecord {
        topic: TOPIC.into(),
        partition: 0,
        leader: NodeId(leader),
        replicas: nodes(replicas),
        isr: nodes(isr),
        leader_epoch: LeaderEpoch(7),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 4,
    }
}

fn elr(eligible: &[i32], last_known: &[i32]) -> PartitionElr {
    PartitionElr {
        eligible_leader_replicas: eligible.to_vec(),
        last_known_elr: last_known.to_vec(),
    }
}

/// An image holding topic `orders` with the given overrides and the given
/// partition state. `min_isr` is published as an ordinary topic override, the
/// way `kafka-configs --alter` sets it.
fn image(
    min_isr: Option<&str>,
    published: Option<&str>,
    current: &PartitionRecord,
) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.into(),
        topic_id: uuid::Uuid::from_u128(1),
        partitions: 1,
        replication_factor: i16::try_from(current.replicas.len()).expect("rf fits i16"),
    }));
    image.apply(&MetadataRecord::V1Partition(current.clone()));
    let overrides: std::collections::BTreeMap<String, String> = [
        min_isr.map(|value| (MIN_INSYNC_REPLICAS.to_string(), value.to_string())),
        published.map(|value| (ELIGIBLE_LEADER_REPLICAS.to_string(), value.to_string())),
    ]
    .into_iter()
    .flatten()
    .collect();
    if !overrides.is_empty() {
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.into(),
            overrides,
        }));
    }
    image
}

/// The rules of `PartitionChangeBuilder.maybePopulateTargetElr`, one row per
/// transition, each starting from the published state the row names.
#[test]
fn the_elr_follows_the_isr_across_min_insync_replicas() {
    for (label, min_isr, published, before, after, want) in [
        (
            "an ISR at min ISR keeps the ELR empty",
            Some("2"),
            elr(&[], &[]),
            partition(1, &[1, 2, 3], &[1, 2, 3]),
            partition(1, &[1, 2, 3], &[1, 2]),
            elr(&[], &[]),
        ),
        (
            "a shrink below min ISR makes the replicas it dropped eligible",
            Some("2"),
            elr(&[], &[]),
            partition(1, &[1, 2, 3], &[1, 2, 3]),
            partition(1, &[1, 2, 3], &[1]),
            elr(&[2, 3], &[]),
        ),
        (
            "a further shrink adds to the ELR rather than replacing it",
            Some("3"),
            elr(&[3], &[]),
            partition(1, &[1, 2, 3], &[1, 2]),
            partition(1, &[1, 2, 3], &[1]),
            elr(&[2, 3], &[]),
        ),
        (
            "a replica that rejoins the ISR leaves the ELR",
            Some("3"),
            elr(&[2, 3], &[]),
            partition(1, &[1, 2, 3], &[1]),
            partition(1, &[1, 2, 3], &[1, 2]),
            elr(&[3], &[]),
        ),
        (
            "an expand to min ISR clears both sets",
            Some("2"),
            elr(&[2, 3], &[]),
            partition(1, &[1, 2, 3], &[1]),
            partition(1, &[1, 2, 3], &[1, 2]),
            elr(&[], &[]),
        ),
        (
            "an ELR replica dropped from the replica set becomes last-known",
            Some("3"),
            elr(&[2, 3], &[]),
            partition(1, &[1, 2, 3], &[1]),
            partition(1, &[1, 2], &[1]),
            elr(&[2], &[3]),
        ),
        (
            "a last-known replica stays last-known while the ISR is short",
            Some("3"),
            elr(&[2], &[3]),
            partition(1, &[1, 2], &[1]),
            partition(1, &[1, 2], &[1]),
            elr(&[2], &[3]),
        ),
        (
            "an unclean election clears both sets",
            Some("3"),
            elr(&[2], &[3]),
            partition(1, &[1, 2, 4], &[1]),
            partition(4, &[1, 2, 4], &[4]),
            elr(&[], &[]),
        ),
        (
            "electing an ELR replica is clean, so the rest stays eligible",
            Some("3"),
            elr(&[2, 3], &[]),
            partition(1, &[1, 2, 3], &[1]),
            partition(2, &[1, 2, 3], &[2]),
            elr(&[1, 3], &[]),
        ),
        (
            "Kafka's default min ISR of 1 can never leave a replica eligible",
            None,
            elr(&[], &[]),
            partition(1, &[1, 2, 3], &[1, 2, 3]),
            partition(1, &[1, 2, 3], &[1]),
            elr(&[], &[]),
        ),
        (
            "a min ISR above the replication factor is capped by it",
            Some("5"),
            elr(&[], &[]),
            partition(1, &[1, 2, 3], &[1, 2, 3]),
            partition(1, &[1, 2, 3], &[1, 2, 3]),
            elr(&[], &[]),
        ),
    ] {
        let image = image(min_isr, None, &before);
        let got = next_partition_elr(
            &image,
            Some(&before),
            &after,
            &published,
            &std::collections::BTreeSet::new(),
        );
        assert!(got == want, "{label}");
    }
}

/// A partition the batch creates has no history to remember, so it starts
/// with no ELR whatever its ISR looks like.
#[test]
fn a_new_partition_starts_with_no_elr() {
    let created = partition(1, &[1, 2, 3], &[1]);
    let image = image(Some("3"), None, &created);

    let got = next_partition_elr(
        &image,
        None,
        &created,
        &elr(&[], &[]),
        &std::collections::BTreeSet::new(),
    );

    assert!(got == elr(&[], &[]));
}

/// Kafka's `uncleanShutdownReplicas`, which the batch that reacts to a
/// returning broker names it with.
///
/// The ISR removal and the recompute are the same batch, so without the
/// exclusion the recompute reads the broker straight back out of the ISR the
/// removal is leaving -- `old_isr ∪ eligible_before` -- and publishes it as
/// eligible. The two rows are the same change; only the exclusion differs,
/// and the second is the one the withdrawal survives.
///
/// The excluded id still lands in the last-known set, because
/// `PartitionChangeBuilder.maybePopulateTargetElr` subtracts
/// `uncleanShutdownReplicas` from `targetElr` and from nothing else: broker 3
/// *was* the last replica known to hold every committed record, whatever the
/// process holding that node id now has on disk.
#[test]
fn an_unclean_shutdown_replica_is_not_re_derived_from_the_isr_it_is_leaving() {
    let before = partition(1, &[1, 2, 3], &[1, 2, 3]);
    let image = image(Some("3"), None, &before);
    let shrink = MetadataRecord::V1Partition(partition(1, &[1, 2, 3], &[1, 2]));

    let published = |value: &str| {
        MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.into(),
            overrides: [
                (MIN_INSYNC_REPLICAS.to_string(), "3".to_string()),
                (ELIGIBLE_LEADER_REPLICAS.to_string(), value.to_string()),
            ]
            .into_iter()
            .collect(),
        })
    };

    let mut plain = vec![shrink.clone()];
    ElrPublisher::new(&image).extend(&mut plain);
    assert!(plain == vec![shrink.clone(), published("0:3:")]);

    let mut excluded = vec![shrink.clone()];
    ElrPublisher::after_unclean_shutdown(&image, NodeId(3)).extend(&mut excluded);
    assert!(excluded == vec![shrink, published("0::3")]);
}

/// The published record replaces a topic's whole override map, so it has to
/// carry the topic's other overrides forward alongside the ELR value.
#[test]
fn the_published_record_keeps_the_topics_other_overrides() {
    let before = partition(1, &[1, 2, 3], &[1, 2, 3]);
    let image = image(Some("2"), None, &before);
    let mut changes = vec![MetadataRecord::V1Partition(partition(1, &[1, 2, 3], &[1]))];

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(
        changes
            == vec![
                MetadataRecord::V1Partition(partition(1, &[1, 2, 3], &[1])),
                MetadataRecord::V1TopicConfig(TopicConfigRecord {
                    topic: TOPIC.into(),
                    overrides: [
                        (MIN_INSYNC_REPLICAS.to_string(), "2".to_string()),
                        (ELIGIBLE_LEADER_REPLICAS.to_string(), "0:2,3:".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                }),
            ]
    );
}

/// A topic that recovers drops the key rather than publishing an entry that
/// says "no ELR", so `DescribeConfigs` stops reporting it at all.
#[test]
fn a_recovered_topic_drops_the_key_and_keeps_the_rest() {
    let before = partition(1, &[1, 2, 3], &[1]);
    let image = image(Some("2"), Some("0:2,3:"), &before);
    let mut changes = vec![MetadataRecord::V1Partition(partition(
        1,
        &[1, 2, 3],
        &[1, 2],
    ))];

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(
        changes[1..]
            == [MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.into(),
                overrides: [(MIN_INSYNC_REPLICAS.to_string(), "2".to_string())]
                    .into_iter()
                    .collect(),
            })]
    );
}

/// Nothing is appended when the state does not move. This is what keeps the
/// publisher off every ISR change in a cluster that never set
/// `min.insync.replicas`, and what makes a re-published batch idempotent.
#[test]
fn an_unchanged_state_publishes_nothing() {
    for (label, min_isr, published, before, after) in [
        (
            "a healthy topic that stays healthy",
            Some("2"),
            None,
            partition(1, &[1, 2, 3], &[1, 2, 3]),
            partition(1, &[1, 2, 3], &[1, 2]),
        ),
        (
            "a topic whose ELR is recomputed to what it already holds",
            Some("3"),
            Some("0:2,3:"),
            partition(1, &[1, 2, 3], &[1]),
            partition(1, &[1, 2, 3], &[1]),
        ),
        (
            "a topic with no min ISR override at all",
            None,
            None,
            partition(1, &[1, 2, 3], &[1, 2, 3]),
            partition(1, &[1, 2, 3], &[1]),
        ),
    ] {
        let image = image(min_isr, published, &before);
        let mut changes = vec![MetadataRecord::V1Partition(after)];
        ElrPublisher::new(&image).extend(&mut changes);
        assert!(changes.len() == 1, "{label}");
    }
}

/// A `V1TopicConfig` already in the batch replaces the topic's whole map when
/// it applies, so the appended record has to be built on that map and not on
/// the one the image still holds. Here the batch drops the topic's retention
/// override; the ELR record must not put it back.
#[test]
fn the_appended_record_builds_on_a_topic_config_the_batch_already_carries() {
    let before = partition(1, &[1, 2, 3], &[1, 2, 3]);
    let mut image = image(Some("2"), None, &before);
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: TOPIC.into(),
        overrides: [
            (MIN_INSYNC_REPLICAS.to_string(), "2".to_string()),
            (RETENTION_MS.to_string(), "60000".to_string()),
        ]
        .into_iter()
        .collect(),
    }));
    let mut changes = vec![
        MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.into(),
            overrides: [(MIN_INSYNC_REPLICAS.to_string(), "2".to_string())]
                .into_iter()
                .collect(),
        }),
        MetadataRecord::V1Partition(partition(1, &[1, 2, 3], &[1])),
    ];

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(
        changes[2..]
            == [MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.into(),
                overrides: [
                    (MIN_INSYNC_REPLICAS.to_string(), "2".to_string()),
                    (ELIGIBLE_LEADER_REPLICAS.to_string(), "0:2,3:".to_string()),
                ]
                .into_iter()
                .collect(),
            })]
    );
}

/// A batch that deletes the topic gets no ELR record: the delete removes the
/// topic's config map, and a record after it would resurrect one.
#[test]
fn a_deleted_topic_publishes_nothing() {
    let before = partition(1, &[1, 2, 3], &[1, 2, 3]);
    let image = image(Some("2"), None, &before);
    let mut changes = vec![
        MetadataRecord::V1Partition(partition(1, &[1, 2, 3], &[1])),
        MetadataRecord::V1DeleteTopic(krabka_metadata::DeleteTopicRecord { name: TOPIC.into() }),
    ];

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(changes.len() == 2);
}

/// Two partitions of one topic share a single published value, so a batch
/// that moves both appends one record carrying both entries.
#[test]
fn one_record_carries_every_partition_the_batch_moved() {
    let before = partition(1, &[1, 2, 3], &[1, 2, 3]);
    let mut image = image(Some("2"), None, &before);
    let mut sibling = before.clone();
    sibling.partition = 1;
    image.apply(&MetadataRecord::V1Partition(sibling));

    let shrunk_zero = partition(1, &[1, 2, 3], &[1]);
    let mut shrunk_one = partition(2, &[1, 2, 3], &[2]);
    shrunk_one.partition = 1;
    let mut changes = vec![
        MetadataRecord::V1Partition(shrunk_zero),
        MetadataRecord::V1Partition(shrunk_one),
    ];

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(
        changes[2..]
            == [MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.into(),
                overrides: [
                    (MIN_INSYNC_REPLICAS.to_string(), "2".to_string()),
                    (
                        ELIGIBLE_LEADER_REPLICAS.to_string(),
                        "0:2,3:;1:1,3:".to_string()
                    ),
                ]
                .into_iter()
                .collect(),
            })]
    );
}

/// KIP-966 against a broker that comes back as a new incarnation: it stops
/// being eligible everywhere it was named, the topic's other overrides ride
/// the record that says so, and a broker no ELR names writes nothing at all.
#[test]
fn a_restarted_broker_is_demoted_out_of_every_published_elr() {
    let image = image(Some("2"), Some("0:2,3:"), &partition(1, &[1, 2, 3], &[1]));

    let records = crate::elr::records_for_restarted_broker(&image, NodeId(3));

    assert!(
        records
            == vec![MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.into(),
                overrides: [
                    (MIN_INSYNC_REPLICAS.to_string(), "2".to_string()),
                    (ELIGIBLE_LEADER_REPLICAS.to_string(), "0:2:3".to_string()),
                ]
                .into_iter()
                .collect(),
            })]
    );
    assert!(crate::elr::records_for_restarted_broker(&image, NodeId(9)).is_empty());
}

/// A topic whose last eligible member is demoted keeps the key, because the
/// replica is still the last one known to have been complete. A topic with no
/// ELR state at all writes nothing.
#[test]
fn a_demotion_keeps_the_last_known_half_of_the_state() {
    let clean = image(Some("2"), None, &partition(1, &[1, 2, 3], &[1, 2, 3]));
    let published = image(Some("2"), Some("0:3:"), &partition(1, &[1, 2, 3], &[1]));

    let records = crate::elr::records_for_restarted_broker(&published, NodeId(3));

    assert!(
        records
            == vec![MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.into(),
                overrides: [
                    (MIN_INSYNC_REPLICAS.to_string(), "2".to_string()),
                    (ELIGIBLE_LEADER_REPLICAS.to_string(), "0::3".to_string()),
                ]
                .into_iter()
                .collect(),
            })]
    );
    assert!(crate::elr::records_for_restarted_broker(&clean, NodeId(3)).is_empty());
}
