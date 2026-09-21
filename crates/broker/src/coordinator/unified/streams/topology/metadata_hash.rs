//! The metadata hash of a streams group: one number that changes when any
//! topic that the topology reads or keeps changes.
//!
//! Kafka stores this hash in `StreamsGroupMetadataValue.MetadataHash`. On a
//! heartbeat it computes the hash again from the current metadata image, and
//! when the value differs it configures the topology again and bumps the group
//! epoch. A created, deleted or grown source topic, and an internal topic that
//! the controller created, therefore reach the assignment on the next
//! heartbeat.
//!
//! The byte layout is the one of Kafka's `Utils.computeTopicHash` and
//! `Utils.computeGroupHash`, which feed hash4j's streaming XXH3 with seed 0.
//! hash4j writes each integer as little-endian bytes and each `String` as its
//! UTF-16 code units in little-endian order followed by its length as an
//! `int`.

use std::collections::BTreeSet;

use krabka_metadata::MetadataImage;
use twox_hash::XxHash3_64;

use crate::coordinator::unified::streams::persistence::StreamsGroupTopologyValue;

/// `Utils.TOPIC_HASH_MAGIC_BYTE`: the version of the topic hash layout.
const TOPIC_HASH_MAGIC_BYTE: u8 = 0x00;

/// The names of the topics that `StreamsTopology.requiredTopics` returns: the
/// source topics, the repartition source topics and the changelog topics of
/// every subtopology.
#[must_use]
pub fn required_topics(topology: &StreamsGroupTopologyValue) -> BTreeSet<&str> {
    topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| {
            subtopology
                .source_topics
                .iter()
                .map(String::as_str)
                .chain(
                    subtopology
                        .repartition_source_topics
                        .iter()
                        .map(|topic| topic.name.as_str()),
                )
                .chain(
                    subtopology
                        .state_changelog_topics
                        .iter()
                        .map(|topic| topic.name.as_str()),
                )
        })
        .collect()
}

/// Kafka's `StreamsGroup.computeMetadataHash`: the group hash over the topic
/// hash of every required topic that exists in `image`.
#[must_use]
pub fn metadata_hash(topology: &StreamsGroupTopologyValue, image: &MetadataImage) -> i64 {
    // `required_topics` is sorted by name, which is the order that
    // `computeGroupHash` sorts the entries into.
    let topic_hashes: Vec<i64> = required_topics(topology)
        .into_iter()
        .filter_map(|topic| topic_hash(topic, image))
        .collect();
    if topic_hashes.is_empty() {
        return 0;
    }
    let mut stream = Vec::with_capacity(topic_hashes.len() * 8);
    for hash in topic_hashes {
        stream.extend_from_slice(&hash.to_le_bytes());
    }
    xxh3(&stream)
}

/// Kafka's `Utils.computeTopicHash`, or `None` for a topic that `image` does
/// not hold.
fn topic_hash(topic: &str, image: &MetadataImage) -> Option<i64> {
    let record = image.topic(topic)?;
    let partition_count = image.topic_partition_count(topic);
    let mut stream = vec![TOPIC_HASH_MAGIC_BYTE];
    let (most, least) = record.topic_id.as_u64_pair();
    stream.extend_from_slice(&most.to_le_bytes());
    stream.extend_from_slice(&least.to_le_bytes());
    put_string(&mut stream, &record.name);
    stream.extend_from_slice(&partition_count.to_le_bytes());
    for partition in 0..partition_count {
        stream.extend_from_slice(&partition.to_le_bytes());
        let mut racks: Vec<&str> = image
            .partition(topic, partition)
            .into_iter()
            .flat_map(|record| record.replicas.iter())
            .filter_map(|replica| image.broker(*replica))
            .filter_map(|broker| broker.rack.as_deref())
            .collect();
        // Java sorts the racks by UTF-16 code units. Rust sorts `str` by
        // bytes. The two orders agree for every rack in the Basic Multilingual
        // Plane, and Kafka's rack ids are ASCII in practice.
        racks.sort_unstable();
        for rack in racks {
            stream.extend_from_slice(&utf16_len(rack).to_le_bytes());
            put_string(&mut stream, rack);
        }
    }
    Some(xxh3(&stream))
}

/// hash4j's `putString`: the UTF-16 code units, then their count as an `int`.
fn put_string(stream: &mut Vec<u8>, value: &str) {
    for unit in value.encode_utf16() {
        stream.extend_from_slice(&unit.to_le_bytes());
    }
    stream.extend_from_slice(&utf16_len(value).to_le_bytes());
}

/// Java's `String.length()`: the number of UTF-16 code units.
fn utf16_len(value: &str) -> i32 {
    i32::try_from(value.encode_utf16().count()).unwrap_or(i32::MAX)
}

fn xxh3(stream: &[u8]) -> i64 {
    i64::from_le_bytes(XxHash3_64::oneshot(stream).to_le_bytes())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    use super::*;
    use crate::coordinator::unified::streams::{
        persistence::StoredTopicInfo,
        topology::test_support::{image_with, sub},
    };

    fn topology() -> StreamsGroupTopologyValue {
        let mut s0 = sub("0");
        s0.source_topics = vec!["in".into()];
        s0.repartition_sink_topics = vec!["sink-only".into()];
        s0.state_changelog_topics = vec![StoredTopicInfo {
            name: "store-changelog".into(),
            partitions: 0,
            replication_factor: 0,
            topic_configs: vec![],
        }];
        StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        }
    }

    #[test]
    fn required_topics_leave_out_the_repartition_sink_topics() {
        check!(required_topics(&topology()) == BTreeSet::from(["in", "store-changelog"]),);
    }

    /// The hash changes exactly when a required topic appears, grows, goes
    /// away or moves to another rack, and ignores a topic that the topology
    /// does not read.
    #[test]
    fn metadata_hash_follows_the_required_topics() {
        let with_racks = |racks: &[Option<&str>]| {
            let mut image = image_with(&[("in", 1, 2), ("other", 2, 1)]);
            for (index, rack) in racks.iter().enumerate() {
                image.apply(&MetadataRecord::V1BrokerRegistration(
                    BrokerRegistrationRecord {
                        node_id: krabka_audit::NodeId(1 + u64::try_from(index).unwrap()),
                        broker_epoch: 0,
                        incarnation_id: uuid::Uuid::nil(),
                        host: "127.0.0.1".into(),
                        port: 9092,
                        rack: rack.map(str::to_owned),
                        endpoints: vec![],
                        log_dirs: vec![],
                        features: std::collections::BTreeMap::new(),
                    },
                ));
            }
            image
        };
        let base = metadata_hash(&topology(), &image_with(&[("in", 1, 2)]));
        // (image, hash equals the base hash)
        let rows = [
            ("no required topic", image_with(&[("other", 2, 1)]), false),
            ("same image", image_with(&[("in", 1, 2)]), true),
            (
                "unrelated topic added",
                image_with(&[("in", 1, 2), ("other", 2, 1)]),
                true,
            ),
            ("partitions added", image_with(&[("in", 1, 3)]), false),
            ("topic id changed", image_with(&[("in", 9, 2)]), false),
            (
                "changelog created",
                image_with(&[("in", 1, 2), ("store-changelog", 3, 2)]),
                false,
            ),
            ("rack of a replica", with_racks(&[Some("r1")]), false),
            ("replica without a rack", with_racks(&[None]), true),
        ];
        check!(metadata_hash(&topology(), &image_with(&[])) == 0);
        for (name, image, same) in rows {
            check!(
                (metadata_hash(&topology(), &image) == base) == same,
                "{name}"
            );
        }
    }
}
