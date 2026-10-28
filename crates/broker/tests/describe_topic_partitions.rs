//! KIP-966 `DescribeTopicPartitions` (`api_key` 75): the paginated topic
//! listing the JVM admin client uses for `kafka-topics --describe`
//! against Kafka 3.7+ brokers.
//!
//! Covered:
//!   * named-request, fetch-all, and unknown-topic paths
//!   * the `is_internal` flag lives in `tests/internal_topics.rs`, which
//!     pins it across this API and `Metadata` at once
//!   * `topic_authorized_operations` populated (KIP-430 helper) on every
//!     Allow row. This API's v0 schema has no opt-in flag.
//!   * Pagination through `response_partition_limit` and the `cursor` /
//!     `next_cursor` round-trip
//!   * Stable sort order on fetch-all (alphabetical)
#![cfg(test)]

use {
    crate::support::{acl::TOPIC_FULL_MASK, topics::configured_topic_request},
    assert2::{assert, check},
    krabka_protocol::owned::{
        create_topics_request::CreateTopicsRequest,
        describe_topic_partitions_response::DescribeTopicPartitionsResponse,
    },
    std::time::{Duration, Instant},
    tokio::task,
};
mod support;

use krabka_protocol::owned::{
    describe_topic_partitions_request::{
        Cursor as RequestCursor, DescribeTopicPartitionsRequest, TopicRequest,
    },
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

fn describe_topics_request(
    names: &[&str],
    limit: i32,
    cursor: Option<RequestCursor>,
) -> DescribeTopicPartitionsRequest {
    DescribeTopicPartitionsRequest {
        topics: names
            .iter()
            .map(|name| TopicRequest {
                name: (*name).into(),
                ..Default::default()
            })
            .collect(),
        response_partition_limit: limit,
        cursor,
        ..Default::default()
    }
}

async fn create_topic(p: &support::InProcess, request: CreateTopicsRequest) {
    let response = support::client::send_topic_creation(&p.client, request).await;
    assert!(response.topics[0].error_code == 0, "{response:?}");
}
use {
    krabka_protocol::owned::update_features_response::UpdateFeaturesResponse,
    rstest_bdd::datatable::Rows, rstest_bdd_macros::DataTableRow,
};
struct TopicPartitions {
    broker: Option<support::InProcess>,
    response: Option<DescribeTopicPartitionsResponse>,
    resumed: Option<DescribeTopicPartitionsResponse>,
    downgrade: Option<UpdateFeaturesResponse>,
}
#[rstest::fixture]
fn partitions() -> TopicPartitions {
    // Start this scenario with fresh inputs and observations.

    TopicPartitions {
        broker: None,
        response: None,
        resumed: None,
        downgrade: None,
    }
}
#[derive(DataTableRow)]
struct TopicInput {
    name: String,
    partitions: i32,
}
#[rstest_bdd_macros::given("an allow-all broker contains topics created in this order:")]
async fn described_topic_inputs(
    partitions: &mut TopicPartitions,
    #[datatable] rows: Rows<TopicInput>,
) {
    let broker = support::start().await;
    for row in rows {
        create_topic(
            &broker,
            configured_topic_request()
                .topic(&row.name)
                .num_partitions(row.partitions)
                .call(),
        )
        .await;
    }
    partitions.broker = Some(broker);
}
#[rstest_bdd_macros::when("the client describes topics {names} with partition limit {limit}")]
async fn describe_topic_rows(partitions: &mut TopicPartitions, names: String, limit: i32) {
    let names: Vec<_> = if names == "all" {
        Vec::new()
    } else {
        names.split(',').collect()
    };
    partitions.response = Some(
        partitions
            .broker
            .as_ref()
            .expect("topics created")
            .client
            .send(describe_topics_request(&names, limit, None))
            .await
            .expect("DescribeTopicPartitions"),
    );
}
#[rstest_bdd_macros::then(
    "alpha then beta include all partition rows and leaders without a cursor"
)]
fn named_partition_rows(partitions: &TopicPartitions) {
    let resp = partitions.response.as_ref().expect("topics described");
    assert!(resp.topics.len() == 2);
    // Named-request order preserves the request order.
    check!(resp.topics[0].name.as_deref() == Some("alpha"));
    check!(resp.topics[0].error_code == 0);
    check!(resp.topics[0].partitions.len() == 2);
    for (i, part) in resp.topics[0].partitions.iter().enumerate() {
        check!(part.error_code == 0);
        check!(part.partition_index == i32::try_from(i).unwrap());
        check!(part.leader_id == 1);
    }
    check!(resp.topics[1].name.as_deref() == Some("beta"));
    check!(resp.topics[1].partitions.len() == 1);

    // No truncation expected — every partition fits under the default
    // 2000-partition budget.
    check!(
        resp.next_cursor.is_none(),
        "no cursor expected: {:?}",
        resp.next_cursor,
    );
}
#[rstest_bdd_macros::then("all user topics are alphabetically alpha beta gamma")]
fn sorted_partition_rows(partitions: &TopicPartitions) {
    let resp = partitions.response.as_ref().expect("topics described");
    let names: Vec<&str> = resp
        .topics
        .iter()
        .filter_map(|t| t.name.as_deref())
        .collect();
    // Internal topics may appear too — filter to the user-created ones.
    let user_topics: Vec<&str> = names
        .iter()
        .copied()
        .filter(|n| !n.starts_with("__"))
        .collect();
    assert!(user_topics == vec!["alpha", "beta", "gamma"]);
}
#[rstest_bdd_macros::then(
    "ghost has error 3 and no partitions while real-topic is still described"
)]
fn unknown_partition_rows(partitions: &TopicPartitions) {
    let resp = partitions.response.as_ref().expect("topics described");
    assert!(resp.topics.len() == 2);
    // Unknown topic row carries UNKNOWN_TOPIC_OR_PARTITION (3).
    check!(resp.topics[0].name.as_deref() == Some("ghost"));
    check!(resp.topics[0].error_code == 3);
    check!(resp.topics[0].partitions.is_empty());
    // Known sibling still served on the same response.
    check!(resp.topics[1].name.as_deref() == Some("real-topic"));
    check!(resp.topics[1].error_code == 0);
    check!(resp.topics[1].partitions.len() == 1);
}
#[rstest_bdd_macros::then("both ELR lists are present empty lists rather than null")]
fn non_null_elr_rows(partitions: &TopicPartitions) {
    let resp = partitions.response.as_ref().expect("topics described");
    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].partitions.len() == 1);
    let part = &resp.topics[0].partitions[0];
    // MUST be Some(_), not None. Both fields stay as empty vecs so the
    // JVM 3.8 admin client's unconditional `.stream()` call doesn't NPE.
    assert!(
        part.eligible_leader_replicas.as_deref() == Some(&[][..]),
        "eligible_leader_replicas must be empty list, not null"
    );
    assert!(
        part.last_known_elr.as_deref() == Some(&[][..]),
        "last_known_elr must be empty list, not null"
    );
}
#[rstest_bdd_macros::then("the topic row succeeds and carries the full authorized operations mask")]
fn authorized_partition_rows(partitions: &TopicPartitions) {
    let resp = partitions.response.as_ref().expect("topics described");
    assert!(resp.topics.len() == 1);
    let row = &resp.topics[0];
    assert!(row.error_code == 0);
    // `support::start` uses `AllowAllAuthorizer` by default, so every
    // supported topic operation is authorized → full mask.
    assert!(
        row.topic_authorized_operations == TOPIC_FULL_MASK,
        "expected full topic mask, got 0b{:b}",
        row.topic_authorized_operations
    );
}
#[rstest_bdd_macros::then(
    "the first page contains three big partitions and a cursor for big partition 3"
)]
fn partition_first_page(partitions: &TopicPartitions) {
    let resp = partitions.response.as_ref().expect("first page requested");
    assert!(resp.topics.len() == 1);
    check!(resp.topics[0].name.as_deref() == Some("big"));
    check!(resp.topics[0].partitions.len() == 3);
    let cursor = resp.next_cursor.as_ref().expect("next_cursor must be set");
    assert!(cursor.topic_name == "big");
    assert!(cursor.partition_index == 3);
}
#[rstest_bdd_macros::when("the client resumes from the exact returned cursor with limit 2000")]
async fn resume_partition_page(partitions: &mut TopicPartitions) {
    let p = partitions.broker.as_ref().expect("topics created");
    let cursor = partitions
        .response
        .as_ref()
        .expect("first page requested")
        .next_cursor
        .as_ref()
        .expect("next cursor returned");
    let resp2 = p
        .client
        .send(describe_topics_request(
            &["big"],
            2000,
            Some(RequestCursor {
                topic_name: cursor.topic_name.clone(),
                partition_index: cursor.partition_index,
                ..Default::default()
            }),
        ))
        .await
        .expect("DescribeTopicPartitions (resume)");
    partitions.resumed = Some(resp2);
}
#[rstest_bdd_macros::then(
    "the resumed page contains exactly partitions 3 and 4 without another cursor"
)]
fn partition_last_page(partitions: &TopicPartitions) {
    let resp2 = partitions.resumed.as_ref().expect("cursor resumed");
    assert!(resp2.topics.len() == 1);
    let parts: Vec<i32> = resp2.topics[0]
        .partitions
        .iter()
        .map(|p| p.partition_index)
        .collect();
    assert!(parts == vec![3, 4]);
    assert!(
        resp2.next_cursor.is_none(),
        "no more data should remain after the resume: {:?}",
        resp2.next_cursor,
    );
}
#[rstest_bdd_macros::given(
    "ELR version 1 is finalized and t partition 0 has eligible nodes 2 and 3"
)]
async fn published_elr_input(partitions: &mut TopicPartitions) {
    let p = partitions.broker.as_ref().expect("topics created");
    p.broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: "eligible.leader.replicas.version".into(),
                level: 1,
            },
        ))
        .await
        .expect("finalize eligible.leader.replicas.version");
    p.broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1PartitionElr(
            krabka_metadata::PartitionElrRecord {
                topic: "t".into(),
                partition: 0,
                eligible_leader_replicas: vec![
                    krabka_metadata::NodeId(2),
                    krabka_metadata::NodeId(3),
                ],
                last_known_elr: vec![],
            },
        ))
        .await
        .expect("publish an ELR");
}
#[rstest_bdd_macros::then("t partition 0 reports eligible replicas 2 and 3 before downgrade")]
fn published_elr_reported(partitions: &TopicPartitions) {
    let before = partitions.response.as_ref().expect("ELR described");
    assert!(
        before.topics[0].partitions[0]
            .eligible_leader_replicas
            .as_deref()
            == Some(&[2, 3][..]),
        "precondition: the published membership is reported: {:?}",
        before.topics[0].partitions[0]
    );
}
#[rstest_bdd_macros::when("the admin safely downgrades ELR version to 0")]
async fn downgrade_partition_elr(partitions: &mut TopicPartitions) {
    let p = partitions.broker.as_ref().expect("topics created");
    let downgrade = p
        .client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "eligible.leader.replicas.version".into(),
                max_version_level: 0,
                // 2 = SAFE_DOWNGRADE, what `kafka-features downgrade` sends.
                upgrade_type: 2,
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    partitions.downgrade = Some(downgrade);
}
#[rstest_bdd_macros::then("the ELR feature downgrade succeeds")]
fn partition_elr_downgraded(partitions: &TopicPartitions) {
    let downgrade = partitions.downgrade.as_ref().expect("downgrade requested");
    assert!(
        downgrade.error_code == 0,
        "UpdateFeatures rejected the request: {downgrade:?}"
    );
}
#[rstest_bdd_macros::when("the client retries describing t until the clearing batch is applied")]
async fn read_cleared_partition_elr(partitions: &mut TopicPartitions) {
    let p = partitions.broker.as_ref().expect("topics created");
    let deadline = Instant::now() + Duration::from_secs(10);
    let after = loop {
        let resp = p
            .client
            .send(describe_request())
            .await
            .expect("describe after");
        let cleared = resp.topics[0].partitions[0]
            .eligible_leader_replicas
            .as_deref()
            == Some(&[][..]);
        if cleared || Instant::now() > deadline {
            break resp;
        }
        task::yield_now().await;
    };
    partitions.response = Some(after);
}
#[rstest_bdd_macros::then("both ELR lists are empty and the finalized ELR feature is absent")]
fn cleared_partition_elr(partitions: &TopicPartitions) {
    let p = partitions.broker.as_ref().expect("topics created");
    let after = partitions
        .response
        .as_ref()
        .expect("post-downgrade topics described");
    let part = &after.topics[0].partitions[0];
    check!(
        part.eligible_leader_replicas.as_deref() == Some(&[][..]),
        "the downgrade must clear the ELR, got {:?}",
        part.eligible_leader_replicas
    );
    check!(part.last_known_elr.as_deref() == Some(&[][..]));
    // KIP-584 treats level 0 as a delete, so the feature leaves the finalized
    // map rather than sitting at 0; `feature_enabled` reads both as off.
    check!(
        p.broker
            .controller_image_for_test()
            .finalized_feature("eligible.leader.replicas.version")
            == None,
        "the downgrade must remove the finalized feature"
    );
}
fn describe_request() -> DescribeTopicPartitionsRequest {
    describe_topics_request(&["t"], 2000, None)
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_topic_partitions.feature",
    name = "named request returns listed topics with partitions"
)]
#[tokio::test]
async fn named_request_returns_listed_topics_with_partitions(partitions: TopicPartitions) {
    partitions
        .broker
        .expect("topics created")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_topic_partitions.feature",
    name = "fetch all returns topics in alphabetical order"
)]
#[tokio::test]
async fn fetch_all_returns_topics_in_alphabetical_order(partitions: TopicPartitions) {
    partitions
        .broker
        .expect("topics created")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_topic_partitions.feature",
    name = "unknown topic in named request returns error row"
)]
#[tokio::test]
async fn unknown_topic_in_named_request_returns_error_row(partitions: TopicPartitions) {
    partitions
        .broker
        .expect("topics created")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_topic_partitions.feature",
    name = "elr lists are empty not null for jvm 3 8 admin compatibility"
)]
#[tokio::test]
async fn elr_lists_are_empty_not_null_for_jvm_3_8_admin_compatibility(partitions: TopicPartitions) {
    partitions
        .broker
        .expect("topics created")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_topic_partitions.feature",
    name = "a feature downgrade empties the reported elr"
)]
#[tokio::test]
async fn a_feature_downgrade_empties_the_reported_elr(partitions: TopicPartitions) {
    partitions
        .broker
        .expect("topics created")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_topic_partitions.feature",
    name = "topic authorized operations populated for super user"
)]
#[tokio::test]
async fn topic_authorized_operations_populated_for_super_user(partitions: TopicPartitions) {
    partitions
        .broker
        .expect("topics created")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_topic_partitions.feature",
    name = "pagination caps response at partition limit and returns next cursor"
)]
#[tokio::test]
async fn pagination_caps_response_at_partition_limit_and_returns_next_cursor(
    partitions: TopicPartitions,
) {
    partitions
        .broker
        .expect("topics created")
        .broker
        .shutdown()
        .await;
}
