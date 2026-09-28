//! Tests for `DescribeShareGroupOffsets`, `api_key` 90.
//!
//! The module holds the Describe request helpers that the Alter and Delete
//! surfaces also read their results through, and the tests that cover the
//! reported SPSO and lag of an initialized partition, an unknown topic, and a
//! broker with share groups disabled.

use std::time::Duration;

use assert2::{assert, check};
use krabka_broker::Broker;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        describe_share_group_offsets_request::{
            DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsRequestGroup,
            DescribeShareGroupOffsetsRequestTopic,
        },
        describe_share_group_offsets_response::{
            DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};

use crate::harness::{
    ACCEPT, NONE, ShareAck, UNSUPPORTED_VERSION, acquired_count, bootstrap_share_state,
    broker_config, broker_test_permit, connect, create_topic, fetch_until_acquired, join,
    produce_n, share_ack, topic_id, wait_for_share_init,
};

/// Sends `DescribeShareGroupOffsets` for one `(group, topic, partitions)`
/// and returns the group row.
pub async fn describe_offsets(
    client: &Client,
    group: &str,
    topic: &str,
    partitions: Vec<i32>,
) -> krabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
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
) -> krabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
    describe_group(client, group, None).await
}

async fn describe_group(
    client: &Client,
    group: &str,
    topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
) -> krabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
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
) -> krabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
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
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = describe_offsets(client, group, topic, partitions.clone()).await;
    }
    last
}

/// Describe reflects the SPSO after a consume that Accepts all records.
///
/// The SPSO advances to 3, and the lag, from the end offset the leader
/// reports to `ListOffsets`, is 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_reflects_spso_after_consume() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, "g1", tid, 0).await;
    produce_n(&client, "t", tid, 0, 3).await;
    let (member, _epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, "g1", tid, 0).await;

    // Acquire 0..2, Accept all → SPSO advances to 3.
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 3, "must acquire all 3 offsets");
    let ack = share_ack(
        &client,
        ShareAck {
            group: "g1",
            member: &member,
            topic_id: tid,
            partition: 0,
            epoch: 1,
            first: 0,
            last: 2,
            ack_type: ACCEPT,
        },
    )
    .await;
    assert!(ack.error_code == NONE, "accept error: {}", ack.error_code);

    // Let the persister land the advanced SPSO durably.
    let group = describe_until(&client, "g1", "t", vec![0], 3).await;
    let part = &group.topics[0].partitions[0];
    check!(
        group.error_code == NONE,
        "group error: {}",
        group.error_code
    );
    check!(group.topics[0].topic_name == "t");
    check!(
        part.error_code == NONE,
        "partition error: {}",
        part.error_code
    );
    check!(
        part.start_offset == 3,
        "SPSO must be 3 after Accept of 0..2, got {}",
        part.start_offset
    );
    // End offset 3 (3 produced), SPSO 3, 0 delivered past it ⇒ lag 0.
    check!(
        part.lag == 0,
        "lag must be 0 (end offset 3 − SPSO 3), got {}",
        part.lag
    );
}

/// Describe of an unknown topic reports no data, not an error: start offset
/// and lag -1, leader epoch 0 and error 0 per partition, the all-zero topic
/// id, as Kafka's `GroupCoordinatorService.describeShareGroupOffsets`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_unknown_topic() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    // The persister must exist for the handler to reach topic resolution; a
    // FindCoordinator(SHARE) bootstrap makes the share coordinator available.
    // The key needs a syntactically valid `group:topicId:partition` shape; the
    // topic id need not refer to a real topic for the bootstrap to succeed.
    let dummy = uuid::Uuid::new_v4();
    bootstrap_share_state(&broker, &client, "g1", dummy, 0).await;

    let group = describe_offsets(&client, "g1", "nonexistent", vec![0]).await;
    assert!(
        group.error_code == NONE,
        "group-level describe must succeed, got {}",
        group.error_code
    );
    let expected = vec![DescribeShareGroupOffsetsResponseTopic {
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
    }];
    assert!(group.topics == expected);
}

/// With `share_group.enable = false`, the admin offset RPCs are unavailable:
/// `DescribeShareGroupOffsets` marks each requested group `UNSUPPORTED_VERSION`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_offsets_rejected_when_share_disabled() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.enable = false;
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;

    let group = describe_offsets(&client, "g1", "t", vec![0]).await;
    assert!(
        group.error_code == UNSUPPORTED_VERSION,
        "share-disabled describe must be UNSUPPORTED_VERSION (35), got {}",
        group.error_code
    );
}
