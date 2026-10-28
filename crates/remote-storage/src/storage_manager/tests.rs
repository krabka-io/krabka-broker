//! Tests for the Kafka-compatible remote-tier naming: the index suffixes, the
//! partition-directory and segment-file names that [`super::partition_dir_name`]
//! and [`super::segment_file_name`] render, and the parsers that read them
//! back, including the property tests that round-trip both names.
//!
//! They sit in their own file because the pinned Kafka names, the malformed
//! inputs each parser must reject, and the two round-trip properties run to
//! nearly as many lines as the module they check.

use {
    super::*,
    crate::metadata::{
        RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentState, TopicIdPartition,
    },
    assert2::check,
    krabka_ids::{LeaderEpoch, Offset, PartitionIndex},
    proptest::prelude::*,
};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct StorageMetadataSetup<'a> {
    #[default("orders")]
    topic: &'a str,
    #[default(PartitionIndex(0))]
    partition: PartitionIndex,
    #[default(Uuid::from_u128(1))]
    topic_id: Uuid,
    #[default(Uuid::from_u128(30))]
    segment_id: Uuid,
    #[default(Offset(0))]
    base_offset: Offset,
}

#[bon::builder]
fn metadata(
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(into, default = PartitionIndex (0))] partition: PartitionIndex,
    #[builder(default = Uuid :: from_u128 (1))] topic_id: Uuid,
    #[builder(default = Uuid :: from_u128 (30))] segment_id: Uuid,
    #[builder(into, default = Offset (0))] base_offset: Offset,
) -> RemoteLogSegmentMetadata {
    let setup = StorageMetadataSetup {
        topic,
        partition,
        topic_id,
        segment_id,
        base_offset,
    };
    let StorageMetadataSetup {
        topic,
        partition,
        topic_id,
        segment_id,
        base_offset,
    } = setup;
    let base_offset = base_offset.0;
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(topic_id, topic, partition.0),
            segment_id,
        ),
        base_offset,
        base_offset,
        0,
        1,
        0,
        RemoteLogSegmentDetails::new(
            1,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => base_offset},
        ),
    )
    .unwrap()
}

#[derive(Debug, PartialEq, Eq)]
enum KafkaUuidObservation {
    Decoded(Uuid),
    Malformed,
}

struct RemoteNameScenario {
    metadata: Option<RemoteLogSegmentMetadata>,
    kind: Option<IndexType>,
    suffix: String,
    rendered_suffix: String,
    parsed_kind: Option<IndexType>,
    partition_name: String,
    segment_name: String,
    parsed_partition: Option<PartitionDirName>,
    parsed_segment: Option<(i64, Uuid, String)>,
    uuid_input: String,
    decoded_uuid: Option<KafkaUuidObservation>,
}

#[rstest::fixture]
fn remote_names() -> RemoteNameScenario {
    // Start this scenario with fresh inputs and observations.

    RemoteNameScenario {
        metadata: None,
        kind: None,
        suffix: String::new(),
        rendered_suffix: String::new(),
        parsed_kind: None,
        partition_name: String::new(),
        segment_name: String::new(),
        parsed_partition: None,
        parsed_segment: None,
        uuid_input: String::new(),
        decoded_uuid: None,
    }
}

#[rstest_bdd_macros::given("remote index kind {kind} with Kafka suffix {suffix}")]
fn remote_index(remote_names: &mut RemoteNameScenario, kind: String, suffix: String) {
    remote_names.kind = Some(match kind.as_str() {
        "offset" => IndexType::Offset,
        "timestamp" => IndexType::Timestamp,
        "producer-snapshot" => IndexType::ProducerSnapshot,
        "leader-epoch" => IndexType::LeaderEpoch,
        "transaction" => IndexType::Transaction,
        _ => panic!("unknown remote index {kind}"),
    });
    remote_names.suffix = suffix;
}

#[rstest_bdd_macros::when("the remote index suffix is rendered and parsed")]
fn index_suffix(remote_names: &mut RemoteNameScenario) {
    remote_names.rendered_suffix = remote_names.kind.expect("index kind given").suffix().into();
    remote_names.parsed_kind = IndexType::from_suffix(&remote_names.suffix);
}

#[rstest_bdd_macros::then("the remote index suffix and kind match Kafka in both directions")]
fn compatible_index(remote_names: &RemoteNameScenario) {
    check!(remote_names.rendered_suffix == remote_names.suffix);
    check!(remote_names.parsed_kind == remote_names.kind);
}

#[rstest_bdd_macros::given(
    "orders partition {partition} starts at offset {offset} with topic ID 1 and segment ID {segment}"
)]
fn remote_segment_metadata(
    remote_names: &mut RemoteNameScenario,
    partition: i32,
    offset: i64,
    segment: u128,
) {
    remote_names.metadata = Some(
        metadata()
            .partition(partition)
            .base_offset(offset)
            .segment_id(Uuid::from_u128(segment))
            .call(),
    );
}

#[rstest_bdd_macros::when("remote tier names are rendered for suffix {suffix}")]
fn remote_layout(remote_names: &mut RemoteNameScenario, suffix: String) {
    let metadata = remote_names
        .metadata
        .as_ref()
        .expect("segment metadata given");
    remote_names.partition_name = partition_dir_name(metadata);
    remote_names.segment_name = segment_file_name(metadata, &suffix);
}

#[rstest_bdd_macros::then("the remote partition directory is {name}")]
fn partition_layout(remote_names: &RemoteNameScenario, name: String) {
    check!(remote_names.partition_name == name);
}

#[rstest_bdd_macros::then("the remote segment file is {name}")]
fn segment_layout(remote_names: &RemoteNameScenario, name: String) {
    check!(remote_names.segment_name == name);
}

#[rstest_bdd_macros::given(
    "Kafka remote partition directory {partition} and segment file {segment}"
)]
fn kafka_remote_names(remote_names: &mut RemoteNameScenario, partition: String, segment: String) {
    remote_names.partition_name = partition;
    remote_names.segment_name = segment;
}

#[rstest_bdd_macros::when("the Kafka remote tier names are parsed")]
fn parse_remote_names(remote_names: &mut RemoteNameScenario) {
    remote_names.parsed_partition = parse_partition_dir_name(&remote_names.partition_name);
    remote_names.parsed_segment =
        parse_segment_file_name(&remote_names.segment_name).map(|parsed| {
            (
                parsed.base_offset,
                parsed.segment_id,
                parsed.suffix.to_owned(),
            )
        });
}

#[rstest_bdd_macros::then("the names identify orders partition 7 with topic ID 1")]
fn parsed_remote_partition(remote_names: &RemoteNameScenario) {
    check!(
        remote_names.parsed_partition
            == Some(PartitionDirName {
                topic: "orders".to_owned(),
                partition: 7,
                topic_id: Uuid::from_u128(1)
            })
    );
}

#[rstest_bdd_macros::then("the segment starts at {offset} with ID {segment} and suffix {suffix}")]
fn parsed_remote_segment(
    remote_names: &RemoteNameScenario,
    offset: i64,
    segment: u128,
    suffix: String,
) {
    let parsed = remote_names
        .parsed_segment
        .as_ref()
        .expect("Kafka segment name parsed");
    check!(parsed.0 == offset);
    check!(parsed.1 == Uuid::from_u128(segment));
    check!(parsed.2 == suffix);
}

#[rstest_bdd_macros::given("Kafka UUID text {encoded}")]
fn remote_uuid_input(remote_names: &mut RemoteNameScenario, encoded: String) {
    remote_names.uuid_input = if encoded == "empty" {
        String::new()
    } else {
        encoded
    };
}

#[rstest_bdd_macros::when("the remote Kafka UUID is decoded")]
fn remote_uuid_decode(remote_names: &mut RemoteNameScenario) {
    remote_names.decoded_uuid = Some(decode_kafka_uuid(&remote_names.uuid_input).map_or(
        KafkaUuidObservation::Malformed,
        KafkaUuidObservation::Decoded,
    ));
}

#[rstest_bdd_macros::then("the malformed Kafka UUID is rejected")]
fn remote_uuid_rejected(remote_names: &RemoteNameScenario) {
    check!(
        remote_names.decoded_uuid == Some(KafkaUuidObservation::Malformed),
        "{:?}",
        remote_names.uuid_input
    );
}

rstest_bdd_macros::scenarios!("tests/features/remote_tier_names", fixtures = [remote_names: RemoteNameScenario]);

#[yare::parameterized(
    log = { LOG_FILE_SUFFIX },
    empty = { "" },
    uppercase = { ".INDEX" },
    missing_dot = { "index" },
    extended = { ".timeindex2" },
)]
fn from_suffix_rejects_non_index_suffixes(suffix: &str) {
    check!(IndexType::from_suffix(suffix) == None, "{suffix:?}");
}

#[test]
fn parse_partition_dir_name_splits_a_hyphenated_topic_from_the_right() {
    // "orders-7" is a topic name, not a topic and a partition index.
    check!(
        parse_partition_dir_name("orders-7-3-AAAAAAAAAAAAAAAAAAAAAQ")
            == Some(PartitionDirName {
                topic: "orders-7".to_owned(),
                partition: 3,
                topic_id: Uuid::from_u128(1),
            })
    );
    // A partition index is never negative, so the leading `-` of "-1"
    // belongs to the topic name and the index is 1.
    check!(
        parse_partition_dir_name("orders--1-AAAAAAAAAAAAAAAAAAAAAQ")
            == Some(PartitionDirName {
                topic: "orders-".to_owned(),
                partition: 1,
                topic_id: Uuid::from_u128(1),
            })
    );
}

#[yare::parameterized(
    short_topic_id = { "orders-7-AAAAAAAAAAAAAAAAAAAAA" },
    long_topic_id = { "orders-7-AAAAAAAAAAAAAAAAAAAAAQAA" },
    non_numeric_partition = { "orders-x-AAAAAAAAAAAAAAAAAAAAAQ" },
    empty_topic = { "-7-AAAAAAAAAAAAAAAAAAAAAQ" },
    path_separator = { "or/ders-7-AAAAAAAAAAAAAAAAAAAAAQ" },
    missing_partition = { "orders-AAAAAAAAAAAAAAAAAAAAAQ" },
    empty = { "" },
)]
fn parse_partition_dir_name_rejects_malformed_names(name: &str) {
    check!(parse_partition_dir_name(name) == None, "{name:?}");
}

#[yare::parameterized(
    short_offset = { "0000000000000000001-AAAAAAAAAAAAAAAAAAAA_g.snapshot" },
    non_numeric_offset = { "0000000000000000001x-AAAAAAAAAAAAAAAAAAAA_g.snapshot" },
    short_segment_id = { "00000000000000000011-AAAAAAAAAAAAAAAAAAAA_" },
    missing_separator = { "00000000000000000011xAAAAAAAAAAAAAAAAAAAA_g.snapshot" },
    invalid_base64 = { "00000000000000000011-**********************.snapshot" },
    empty = { "" },
)]
fn parse_segment_file_name_rejects_malformed_names(name: &str) {
    check!(parse_segment_file_name(name) == None, "{name:?}");
}

proptest! {
    #[test]
    fn partition_dir_name_round_trips(
        topic in "[a-z0-9]{1,6}(-[a-z0-9]{1,6}){0,3}",
        partition in 0_i32..=i32::MAX,
        topic_id in any::<u128>(),
    ) {
        let topic_id = Uuid::from_u128(topic_id);
        let metadata = metadata().topic(&topic).partition(partition).topic_id(topic_id).segment_id(Uuid::nil()).call();
        let name = partition_dir_name(&metadata);
        prop_assert!(
            parse_partition_dir_name(&name)
                == Some(PartitionDirName { topic, partition, topic_id }),
            "{name}"
        );
    }

    #[test]
    fn segment_file_name_round_trips(
        base_offset in 0_i64..=i64::MAX,
        segment_id in any::<u128>(),
        index_type in prop_oneof![
            Just(None),
            Just(Some(IndexType::Offset)),
            Just(Some(IndexType::Timestamp)),
            Just(Some(IndexType::ProducerSnapshot)),
            Just(Some(IndexType::LeaderEpoch)),
            Just(Some(IndexType::Transaction)),
        ],
    ) {
        let segment_id = Uuid::from_u128(segment_id);
        let suffix = index_type.map_or(LOG_FILE_SUFFIX, IndexType::suffix);
        let metadata = metadata().topic_id(Uuid::nil()).segment_id(segment_id).base_offset(base_offset).call();
        let name = segment_file_name(&metadata, suffix);
        let parsed = parse_segment_file_name(&name);
        prop_assert!(
            parsed == Some(SegmentFileName { base_offset, segment_id, suffix }),
            "{name}"
        );
        prop_assert!(
            parsed.and_then(|p| IndexType::from_suffix(p.suffix)) == index_type,
            "{name}"
        );
    }
}
