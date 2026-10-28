//! Tests for `DescribeShareGroupOffsets`, `api_key` 90.
//!
//! The module holds the Describe request helpers that the Alter and Delete
//! surfaces also read their results through, and the tests that cover the
//! reported SPSO and lag of an initialized partition, an unknown topic, and a
//! broker with share groups disabled.

use {
    crate::{
        harness::{
            NONE, UNSUPPORTED_VERSION, acquired_count, acquired_topic, bootstrap_share_state,
            broker_config, broker_test_permit, connect, create_topic,
        },
        support::{
            configs::feature_update,
            share::{ShareAck, acknowledge_success},
        },
    },
    assert2::{assert, check},
    krabka_broker::Broker,
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            describe_share_group_offsets_request::{
                DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsRequestGroup,
                DescribeShareGroupOffsetsRequestTopic,
            },
            describe_share_group_offsets_response::{
                DescribeShareGroupOffsetsResponseGroup, DescribeShareGroupOffsetsResponsePartition,
                DescribeShareGroupOffsetsResponseTopic,
            },
            update_features_request::UpdateFeaturesRequest,
        },
        primitives::uuid::Uuid,
    },
    std::time::Duration,
    tokio::time,
};

/// `UpdateFeatures` `UpgradeType` 2, `SAFE_DOWNGRADE`.
const SAFE_DOWNGRADE: i8 = 2;

/// Sends `DescribeShareGroupOffsets` for one `(group, topic, partitions)`
/// and returns the group row.
pub async fn describe_offsets(
    client: &Client,
    group: &str,
    topic: &str,
    partitions: Vec<i32>,
) -> DescribeShareGroupOffsetsResponseGroup {
    describe_group(
        client,
        group,
        Some(vec![DescribeShareGroupOffsetsRequestTopic {
            topic_name: topic.into(),
            partitions,
            ..Default::default()
        }]),
    )
    .await
}

/// Sends `DescribeShareGroupOffsets` for every initialized partition of
/// `group`, a request with no topic list, and returns the group row.
pub async fn describe_all_offsets(
    client: &Client,
    group: &str,
) -> DescribeShareGroupOffsetsResponseGroup {
    describe_group(client, group, None).await
}

async fn describe_group(
    client: &Client,
    group: &str,
    topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
) -> DescribeShareGroupOffsetsResponseGroup {
    let resp = client
        .send(DescribeShareGroupOffsetsRequest {
            groups: vec![DescribeShareGroupOffsetsRequestGroup {
                group_id: group.into(),
                topics,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeShareGroupOffsets");
    resp.groups[0].clone()
}

/// Polls Describe until the partition reports the expected SPSO.
///
/// The persister writes the advanced SPSO asynchronously after the Accept ack.
pub async fn describe_until(
    client: &Client,
    group: &str,
    topic: &str,
    partitions: Vec<i32>,
    want_spso: i64,
) -> DescribeShareGroupOffsetsResponseGroup {
    let mut last = describe_offsets(client, group, topic, partitions.clone()).await;
    for _ in 0..40 {
        if let Some(p) = last.topics.first().and_then(|t| t.partitions.first())
            && p.start_offset == want_spso
        {
            return last;
        }
        // intentional: bounded Describe-RPC poll for the async persister write of the
        // SPSO; returns the response for assertions and also serves the deleted (-1)
        // case that no share-SPSO awaiter covers.
        time::sleep(Duration::from_millis(100)).await;
        last = describe_offsets(client, group, topic, partitions.clone()).await;
    }
    last
}

use {
    krabka_broker::BrokerHandle,
    krabka_protocol::owned::{
        share_fetch_response::PartitionData, update_features_response::UpdateFeaturesResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::sync::Arc,
    tempfile::TempDir,
    tokio::sync::OwnedSemaphorePermit,
};

#[derive(Default)]
struct ShareOffsetDescription {
    permit: Option<OwnedSemaphorePermit>,
    directory: Option<TempDir>,
    broker: Option<BrokerHandle>,
    client: Option<Arc<Client>>,
    member: Option<String>,
    topic: Option<uuid::Uuid>,
    acquired: Option<PartitionData>,
    group: Option<DescribeShareGroupOffsetsResponseGroup>,
    downgrade: Option<UpdateFeaturesResponse>,
}

#[fixture]
fn share_offset_description() -> ShareOffsetDescription {
    // Store RPC results independently from the live membership and acquired records.
    ShareOffsetDescription::default()
}

#[given("a fresh share-enabled broker for offset description")]
async fn description_broker(share_offset_description: &mut ShareOffsetDescription) {
    share_offset_description.permit = Some(broker_test_permit().await);
    let directory = TempDir::new().unwrap();
    let broker = Broker::start(broker_config(directory.path().to_path_buf()))
        .await
        .unwrap();
    share_offset_description.client = Some(connect(&broker.listen_addr().to_string()).await);
    share_offset_description.broker = Some(broker);
    share_offset_description.directory = Some(directory);
}

#[given("the description broker's share-state persister is ready for g1")]
async fn description_persister(share_offset_description: &ShareOffsetDescription) {
    bootstrap_share_state(
        share_offset_description
            .broker
            .as_ref()
            .expect("share broker"),
        share_offset_description
            .client
            .as_ref()
            .expect("share client"),
        "g1",
    )
    .await;
}

#[given("the description broker holds topic t")]
async fn description_topic(share_offset_description: &ShareOffsetDescription) {
    create_topic()
        .broker(
            share_offset_description
                .broker
                .as_ref()
                .expect("share broker"),
        )
        .client(
            share_offset_description
                .client
                .as_ref()
                .expect("share client"),
        )
        .call()
        .await;
}

#[given("g1's member has acquired the three records on t partition 0")]
async fn description_acquisition(share_offset_description: &mut ShareOffsetDescription) {
    let (permit, broker, client, directory, topic, member, row) = acquired_topic().await;
    share_offset_description.permit = Some(permit);
    share_offset_description.broker = Some(broker);
    share_offset_description.client = Some(client);
    share_offset_description.directory = Some(directory);
    share_offset_description.topic = Some(topic);
    share_offset_description.member = Some(member);
    share_offset_description.acquired = Some(row);
}

#[then("the described-group fixture acquired all three offsets")]
fn description_acquired(share_offset_description: &ShareOffsetDescription) {
    assert!(
        acquired_count(
            share_offset_description
                .acquired
                .as_ref()
                .expect("records acquired")
        ) == 3
    );
}

#[when("the described-group member accepts offsets 0 through 2")]
async fn accept_described_group(share_offset_description: &ShareOffsetDescription) {
    acknowledge_success(
        share_offset_description
            .client
            .as_ref()
            .expect("share client"),
        ShareAck::prefix_for(
            share_offset_description.member.as_ref().expect("member"),
            share_offset_description.topic.expect("topic identity"),
            krabka_ids::Offset(2),
        ),
    )
    .await;
}

#[when("DescribeShareGroupOffsets waits for g1's start offset to reach 3")]
async fn describe_consumed_group(share_offset_description: &mut ShareOffsetDescription) {
    share_offset_description.group = Some(
        describe_until(
            share_offset_description
                .client
                .as_ref()
                .expect("share client"),
            "g1",
            "t",
            vec![0],
            3,
        )
        .await,
    );
}

#[then("g1 describes topic t successfully with start offset 3 and lag zero")]
fn consumed_group_description(share_offset_description: &ShareOffsetDescription) {
    let group = share_offset_description
        .group
        .as_ref()
        .expect("group described");
    let partition = &group.topics[0].partitions[0];
    check!(group.error_code == NONE);
    check!(group.topics[0].topic_name == "t");
    check!(partition.error_code == NONE);
    check!(partition.start_offset == 3);
    check!(partition.lag == 0);
}

#[when("g1's offsets for topic {topic} partition 0 are described")]
async fn describe_named_group(
    share_offset_description: &mut ShareOffsetDescription,
    topic: String,
) {
    share_offset_description.group = Some(
        describe_offsets(
            share_offset_description
                .client
                .as_ref()
                .expect("share client"),
            "g1",
            &topic,
            vec![0],
        )
        .await,
    );
}

#[then("the unknown topic description succeeds with zero topic ID and missing start and lag")]
fn missing_group_description(share_offset_description: &ShareOffsetDescription) {
    let group = share_offset_description
        .group
        .as_ref()
        .expect("group described");
    assert!(group.error_code == NONE);
    assert!(
        group.topics
            == vec![DescribeShareGroupOffsetsResponseTopic {
                topic_name: "nonexistent".into(),
                topic_id: Uuid::default(),
                partitions: vec![DescribeShareGroupOffsetsResponsePartition {
                    partition_index: 0,
                    start_offset: -1,
                    leader_epoch: 0,
                    lag: -1,
                    error_code: NONE,
                    ..Default::default()
                }],
                ..Default::default()
            }]
    );
}

#[then("share offset description is available")]
fn enabled_group_description(share_offset_description: &ShareOffsetDescription) {
    assert!(
        share_offset_description
            .group
            .as_ref()
            .expect("group described")
            .error_code
            != UNSUPPORTED_VERSION
    );
}

#[when("share.version is safely downgraded to zero")]
async fn disable_share_offsets(share_offset_description: &mut ShareOffsetDescription) {
    share_offset_description.downgrade = Some(
        share_offset_description
            .client
            .as_ref()
            .expect("share client")
            .send(UpdateFeaturesRequest {
                feature_updates: vec![feature_update("share.version", 0, SAFE_DOWNGRADE)],
                ..Default::default()
            })
            .await
            .expect("UpdateFeatures"),
    );
}

#[then("the share-version downgrade succeeds")]
fn disabled_share_version(share_offset_description: &ShareOffsetDescription) {
    let response = share_offset_description
        .downgrade
        .as_ref()
        .expect("downgrade answered");
    assert!(response.error_code == NONE, "{response:?}");
}

#[then("share offset description is refused with UNSUPPORTED_VERSION")]
fn disabled_group_description(share_offset_description: &ShareOffsetDescription) {
    assert!(
        share_offset_description
            .group
            .as_ref()
            .expect("group described")
            .error_code
            == UNSUPPORTED_VERSION
    );
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/describe.feature",
    name = "describe reflects spso after consume"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_reflects_spso_after_consume(share_offset_description: ShareOffsetDescription) {
    share_offset_description
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_offset_description);
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/describe.feature",
    name = "describe unknown topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_unknown_topic(share_offset_description: ShareOffsetDescription) {
    share_offset_description
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_offset_description);
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/describe.feature",
    name = "admin offsets rejected when share disabled"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_offsets_rejected_when_share_disabled(
    share_offset_description: ShareOffsetDescription,
) {
    share_offset_description
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_offset_description);
}

/// The initialized partition indexes for a topic; missing topics have no indexes.
pub fn initialized_partitions(
    group: &DescribeShareGroupOffsetsResponseGroup,
    topic: &str,
) -> Vec<i32> {
    group
        .topics
        .iter()
        .find(|row| row.topic_name == topic)
        .map_or_default(|row| {
            row.partitions
                .iter()
                .map(|partition| partition.partition_index)
                .collect()
        })
}
