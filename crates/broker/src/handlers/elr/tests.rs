//! Parsing and projection of the KIP-966 ELR state.

use assert2::assert;
use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};

use super::{PartitionElr, TopicElr};
use crate::config_keys::ELIGIBLE_LEADER_REPLICAS;

fn elr(eligible: &[i32], last_known: &[i32]) -> PartitionElr {
    PartitionElr {
        eligible_leader_replicas: eligible.to_vec(),
        last_known_elr: last_known.to_vec(),
    }
}

/// The grammar, one row per shape the controller can publish, plus the
/// malformed shapes that must degrade to "no ELR" rather than fail a request.
#[test]
fn parse_projects_each_partition_of_the_config_value() {
    for (value, partition, want) in [
        // Nothing published at all.
        ("", 0, elr(&[], &[])),
        // Both sets on one partition.
        ("0:2,3:4,5", 0, elr(&[2, 3], &[4, 5])),
        // ELR only, and last-known only.
        ("0:2,3:", 0, elr(&[2, 3], &[])),
        ("0::5", 0, elr(&[], &[5])),
        // A partition the value does not name projects as no ELR.
        ("0:2:3", 1, elr(&[], &[])),
        // Several partitions in one value, in either order.
        ("4::5;0:2,3:", 0, elr(&[2, 3], &[])),
        ("4::5;0:2,3:", 4, elr(&[], &[5])),
        // A trailing separator is not an entry.
        ("0:2:;", 0, elr(&[2], &[])),
        // Malformed entries drop, and drop only themselves.
        ("nope:1:2;0:7:", 0, elr(&[7], &[])),
        ("nope:1:2", 0, elr(&[], &[])),
        ("0:1", 0, elr(&[], &[])),
        ("0:1:2:3", 0, elr(&[], &[])),
        ("0:x:2", 0, elr(&[], &[])),
        ("0:1,:2", 0, elr(&[], &[])),
    ] {
        assert!(
            TopicElr::parse(value).partition(partition) == want,
            "value {value:?} partition {partition}"
        );
    }
}

/// The projection reads the topic config out of the image, so a topic the
/// controller has never published ELR for answers with empty lists.
#[test]
fn of_topic_reads_the_published_config_and_defaults_to_no_elr() {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides: [(ELIGIBLE_LEADER_REPLICAS.to_string(), "0:2,3:4".to_string())]
            .into_iter()
            .collect(),
    }));

    assert!(TopicElr::of_topic(&image, "orders").partition(0) == elr(&[2, 3], &[4]));
    assert!(TopicElr::of_topic(&image, "orders").partition(1) == elr(&[], &[]));
    assert!(TopicElr::of_topic(&image, "payments").partition(0) == elr(&[], &[]));
}
