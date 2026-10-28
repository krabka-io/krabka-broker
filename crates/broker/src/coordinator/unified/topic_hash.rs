//! Kafka's group metadata hash: one number over the metadata of the topics
//! that a consumer, share or streams group reads.
//!
//! Kafka 4.3.1 writes the number as the `MetadataHash` of
//! `ConsumerGroupMetadataValue` (tagged field 0), `ShareGroupMetadataValue`
//! and `StreamsGroupMetadataValue` (both plain fields). A heartbeat computes
//! it again when the group's metadata expired, and a new value bumps the group
//! epoch, so the next target assignment sees the change.
//!
//! The byte layout is the one of `Utils.computeTopicHash` and
//! `Utils.computeGroupHash`, which stream into hash4j's XXH3-64 with seed 0
//! (`Hashing.xxh3_64().hashStream()`). The streamed bytes of one topic are:
//!
//! 1. the magic byte `TOPIC_HASH_MAGIC_BYTE`, `0x00`;
//! 2. the most and then the least significant 64 bits of the topic id;
//! 3. the topic name;
//! 4. the partition count, an `int`;
//! 5. for each partition from 0, its index, an `int`, then for each replica
//!    rack in sorted order the rack's length, an `int`, and the rack.
//!
//! hash4j writes every `long` and `int` as little-endian bytes, and a `String`
//! as its UTF-16 code units, each little-endian, followed by its length as an
//! `int`. So a rack is its length twice: once from `computeTopicHash` and once
//! from `putString`. The group hash streams the topic hashes as `long`s, in
//! topic name order, and is 0 for no topic.
//!
//! Java orders `String`s by UTF-16 code unit, which sorts a character above
//! the Basic Multilingual Plane before `U+E000`..`U+FFFF`, where UTF-8 byte
//! order puts it after. Both sorts here follow Java.

use {
    krabka_metadata::MetadataImage,
    std::{cmp::Ordering, collections::BTreeSet},
    twox_hash::XxHash3_64,
};

/// `Utils.TOPIC_HASH_MAGIC_BYTE`: the version of the topic hash layout.
const TOPIC_HASH_MAGIC_BYTE: u8 = 0x00;

/// Java's `String.compareTo`: the order of the UTF-16 code units.
fn java_string_order(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

/// Java's `String.length()`: the number of UTF-16 code units.
fn utf16_len(value: &str) -> i32 { i32::try_from(value.encode_utf16().count()).unwrap_or(i32::MAX) }

/// hash4j's `putString`: the UTF-16 code units, then their count as an `int`.
fn put_string(stream: &mut Vec<u8>, value: &str) {
    for unit in value.encode_utf16() {
        stream.extend_from_slice(&unit.to_le_bytes());
    }
    stream.extend_from_slice(&utf16_len(value).to_le_bytes());
}

fn xxh3(stream: &[u8]) -> i64 { i64::from_le_bytes(XxHash3_64::oneshot(stream).to_le_bytes()) }

/// Kafka's `Utils.computeTopicHash` for a topic that exists.
///
/// `partition_racks` holds one entry per partition, from partition 0, so its
/// length is the partition count. Each entry names the rack of every replica
/// of the partition whose broker has one, in any order and with repeats, as
/// `CoordinatorMetadataImage.TopicMetadata.partitionRacks` returns them.
#[must_use]
pub fn topic_hash<'a>(
    topic_id: [u8; 16],
    name: &str,
    partition_racks: impl ExactSizeIterator<Item = Vec<&'a str>>,
) -> i64 {
    let [
        m0,
        m1,
        m2,
        m3,
        m4,
        m5,
        m6,
        m7,
        l0,
        l1,
        l2,
        l3,
        l4,
        l5,
        l6,
        l7,
    ] = topic_id;
    let mut stream = vec![TOPIC_HASH_MAGIC_BYTE];
    // A Java `long` from `Uuid.getMostSignificantBits` is the big-endian
    // reading of the id's first eight bytes; hash4j writes it little-endian.
    for half in [[m0, m1, m2, m3, m4, m5, m6, m7], [
        l0, l1, l2, l3, l4, l5, l6, l7,
    ]] {
        stream.extend_from_slice(&u64::from_be_bytes(half).to_le_bytes());
    }
    put_string(&mut stream, name);
    let partition_count = i32::try_from(partition_racks.len()).unwrap_or(i32::MAX);
    stream.extend_from_slice(&partition_count.to_le_bytes());
    for (partition, mut racks) in (0..partition_count).zip(partition_racks) {
        stream.extend_from_slice(&partition.to_le_bytes());
        racks.sort_by(|left, right| java_string_order(left, right));
        for rack in racks {
            stream.extend_from_slice(&utf16_len(rack).to_le_bytes());
            put_string(&mut stream, rack);
        }
    }
    xxh3(&stream)
}

/// The hash of one topic, with the name that [`group_hash`] orders it by.
#[derive(Debug, Clone, Copy)]
pub struct NamedTopicHash<'a> {
    pub name: &'a str,
    pub hash: i64,
}

/// Kafka's `Utils.computeGroupHash`: the topic hashes in topic name order, or
/// 0 for no topic.
#[must_use]
pub fn group_hash<'a>(topic_hashes: impl IntoIterator<Item = NamedTopicHash<'a>>) -> i64 {
    let mut sorted: Vec<NamedTopicHash<'_>> = topic_hashes.into_iter().collect();
    if sorted.is_empty() {
        return 0;
    }
    sorted.sort_by(|left, right| java_string_order(left.name, right.name));
    let mut stream = Vec::with_capacity(sorted.len() * 8);
    for topic in sorted {
        stream.extend_from_slice(&topic.hash.to_le_bytes());
    }
    xxh3(&stream)
}

/// Kafka's `Utils.computeTopicHash` against a metadata image, or `None` for a
/// topic that the image does not hold.
///
/// The partition count is the number of partitions that the image holds, as
/// `KRaftCoordinatorMetadataImage` counts `topicImage.partitions()`, and the
/// racks of a partition are the racks of its replicas' registered brokers.
#[must_use]
pub fn image_topic_hash(topic: &str, image: &MetadataImage) -> Option<i64> {
    let record = image.topic(topic)?;
    let partition_count = image.topic_partition_count(topic);
    let racks = (0..partition_count).map(|partition| {
        image
            .partition(topic, partition)
            .into_iter()
            .flat_map(|record| record.replicas.iter())
            .filter_map(|replica| image.broker(*replica))
            .filter_map(|broker| broker.rack.as_deref())
            .collect::<Vec<&str>>()
    });
    Some(topic_hash(*record.topic_id.as_bytes(), &record.name, racks))
}

/// Kafka's `ModernGroup.computeMetadataHash` and
/// `StreamsGroup.computeMetadataHash`: the group hash over the topic hash of
/// every topic in `topics` that `image` holds. A name that repeats counts
/// once.
#[must_use]
pub fn image_metadata_hash<'a>(
    topics: impl IntoIterator<Item = &'a str>,
    image: &MetadataImage,
) -> i64 {
    let topics: BTreeSet<&str> = topics.into_iter().collect();
    group_hash(topics.into_iter().filter_map(|topic| {
        image_topic_hash(topic, image).map(|hash| NamedTopicHash { name: topic, hash })
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::{NamedTopicHash, group_hash, topic_hash},
        assert2::check,
        uuid::Uuid,
    };

    #[derive(Default)]
    struct MetadataHashScenario {
        topic_id: [u8; 16],
        name: String,
        racks: Vec<Vec<String>>,
        topics: Vec<(String, i64)>,
        topic_hash: Option<i64>,
        group_hash: Option<i64>,
    }

    #[rstest::fixture]
    fn metadata_hash() -> MetadataHashScenario {
        // Keep metadata hash inputs and observations fresh for each scenario.
        MetadataHashScenario::default()
    }

    #[rstest_bdd_macros::given("topic {name} with UUID {id} has replica racks {racks}")]
    fn topic_metadata(
        metadata_hash: &mut MetadataHashScenario,
        name: String,
        id: Uuid,
        racks: String,
    ) {
        metadata_hash.topic_id = *id.as_bytes();
        metadata_hash.name = name;
        metadata_hash.racks =
            serde_json::from_str(&racks).expect("rack lists are arrays of strings");
    }

    #[rstest_bdd_macros::given("the group topic hashes are {topics}")]
    fn group_topic_hashes(metadata_hash: &mut MetadataHashScenario, topics: String) {
        metadata_hash.topics =
            serde_json::from_str(&topics).expect("topic hashes are name/hash pairs");
    }

    #[rstest_bdd_macros::when("the coordinator computes the topic metadata hash")]
    fn compute_topic_metadata_hash(metadata_hash: &mut MetadataHashScenario) {
        let racks = metadata_hash
            .racks
            .iter()
            .map(|partition| partition.iter().map(String::as_str).collect());
        metadata_hash.topic_hash = Some(topic_hash(
            metadata_hash.topic_id,
            &metadata_hash.name,
            racks,
        ));
    }

    #[rstest_bdd_macros::when("the coordinator computes the group metadata hash")]
    fn compute_group_metadata_hash(metadata_hash: &mut MetadataHashScenario) {
        let topics = metadata_hash
            .topics
            .iter()
            .map(|(name, hash)| NamedTopicHash { name, hash: *hash });
        metadata_hash.group_hash = Some(group_hash(topics));
    }

    #[rstest_bdd_macros::then("the topic hash equals Kafka's reference value {hash}")]
    fn kafka_topic_hash(metadata_hash: &MetadataHashScenario, hash: i64) {
        check!(metadata_hash.topic_hash == Some(hash));
    }

    #[rstest_bdd_macros::then("the group hash equals Kafka's reference value {hash}")]
    fn kafka_group_hash(metadata_hash: &MetadataHashScenario, hash: i64) {
        check!(metadata_hash.group_hash == Some(hash));
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/src/coordinator/unified/topic_hash.feature", fixtures = [metadata_hash: MetadataHashScenario]);
}
