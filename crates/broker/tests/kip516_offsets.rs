//! KIP-516: `OffsetCommit` v10 and `OffsetFetch` v8+ keyed by `topic_id`.
#![cfg(test)]
use {
    crate::support::{
        offsets::{
            offset_commit_topic, offset_fetch_group, offset_fetch_request, offset_fetch_topic,
        },
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};
mod support;

use {
    krabka_protocol::{
        owned::{
            offset_commit_request::{
                OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
            },
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
            offset_fetch_response::{
                OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
                OffsetFetchResponseTopics,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    },
    support::topic_id_for,
};

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

use crate::support::start_ready_group as start;

struct OffsetIdentity {
    broker: Option<InProcess>,
    group: String,
    topic: String,
    topic_id: WireUuid,
    committed: i64,
    unresolved: Vec<WireUuid>,
    fetched: Option<OffsetFetchResponse>,
    refused_fetches: Vec<(WireUuid, OffsetFetchResponse)>,
    partial_commits: Vec<(WireUuid, OffsetCommitResponse)>,
}

#[fixture]
fn offsets() -> OffsetIdentity {
    // Start this scenario with fresh inputs and observations.

    OffsetIdentity {
        broker: None,
        group: String::new(),
        topic: String::new(),
        topic_id: WireUuid::ZERO,
        committed: 0,
        unresolved: Vec::new(),
        fetched: None,
        refused_fetches: Vec::new(),
        partial_commits: Vec::new(),
    }
}

#[given("topic {topic} exists for group {group}")]
async fn topic_exists(offsets: &mut OffsetIdentity, topic: String, group: String) {
    let broker = start().await;
    broker
        .client
        .send(create_topic_request(
            creatable_topic().name(topic.clone()).call(),
        ))
        .await
        .expect("create topic");
    offsets.topic_id = topic_id_for(&broker.client, &topic).await;
    offsets.broker = Some(broker);
    offsets.topic = topic;
    offsets.group = group;
}

#[given("a ready group coordinator and zero and unassigned offset topic ids")]
async fn unknown_fetch_ids(offsets: &mut OffsetIdentity) {
    offsets.broker = Some(start().await);
    offsets.group = "g2".into();
    offsets.unresolved = support::topics::unresolved_topic_ids(0xabad_1dea)
        .into_iter()
        .map(|(_, id)| id)
        .collect();
}

#[given("one known topic beside zero and unassigned offset commit topic ids")]
async fn mixed_commit_ids(offsets: &mut OffsetIdentity) {
    topic_exists(offsets, "oc_known".into(), "gc".into()).await;
    offsets.unresolved = support::topics::unresolved_topic_ids(0x0bad_0bad)
        .into_iter()
        .map(|(_, id)| id)
        .collect();
}

fn commit_row(topic_id: WireUuid, committed_offset: i64) -> OffsetCommitRequestTopic {
    offset_commit_topic(String::new(), topic_id, vec![
        OffsetCommitRequestPartition {
            partition_index: 0,
            committed_offset,
            ..Default::default()
        },
    ])
}

#[when("offset {offset} is committed using the topic id")]
async fn commit_by_id(offsets: &mut OffsetIdentity, offset: i64) {
    let broker = offsets.broker.as_ref().expect("a coordinator exists");
    broker
        .client
        .send(OffsetCommitRequest {
            group_id: offsets.group.clone(),
            topics: vec![commit_row(offsets.topic_id, offset)],
            ..Default::default()
        })
        .await
        .expect("offset commit");
    offsets.committed = offset;
}

#[when("committed offsets are fetched for {selection}")]
async fn fetch_committed(offsets: &mut OffsetIdentity, selection: String) {
    let topics = match selection.as_str() {
        "the listed topic" => Some(vec![offset_fetch_topic(
            String::new(),
            offsets.topic_id,
            vec![0],
        )]),
        "all topics" => None,
        _ => panic!("unknown topic selection: {selection}"),
    };
    let broker = offsets.broker.as_ref().expect("a coordinator exists");
    offsets.fetched = Some(
        broker
            .client
            .send(offset_fetch_request(offset_fetch_group(
                &offsets.group,
                topics,
            )))
            .await
            .expect("offset fetch"),
    );
}

#[then("partition zero returns the committed offset with the same topic identity")]
fn committed_identity(offsets: &OffsetIdentity) {
    let response = offsets
        .fetched
        .as_ref()
        .expect("committed offsets were fetched");
    let group = response
        .groups
        .iter()
        .find(|group| group.group_id == offsets.group)
        .expect("requested group");
    let topic = group
        .topics
        .iter()
        .find(|topic| topic.topic_id == offsets.topic_id)
        .expect("topic by id");
    let partition = topic.partitions.first().expect("partition 0");
    check!(partition.committed_offset == offsets.committed);
    check!(partition.error_code == 0);
    check!(topic.topic_id == offsets.topic_id);
}

#[when("committed offsets are requested for each unresolved topic id")]
async fn fetch_unknown(offsets: &mut OffsetIdentity) {
    let broker = offsets.broker.as_ref().expect("a coordinator exists");
    for &topic_id in &offsets.unresolved {
        let response = broker
            .client
            .send(offset_fetch_request(offset_fetch_group(
                &offsets.group,
                Some(vec![offset_fetch_topic(String::new(), topic_id, vec![0])]),
            )))
            .await
            .expect("offset fetch");
        offsets.refused_fetches.push((topic_id, response));
    }
}

#[then("each unresolved fetch echoes its topic id with UNKNOWN_TOPIC_ID and offset minus one")]
fn refused_fetches(offsets: &OffsetIdentity) {
    assert!(offsets.refused_fetches.len() == offsets.unresolved.len());
    for (topic_id, response) in &offsets.refused_fetches {
        let expected = OffsetFetchResponse {
            groups: vec![OffsetFetchResponseGroup {
                group_id: offsets.group.clone(),
                topics: vec![OffsetFetchResponseTopics {
                    name: String::new(),
                    topic_id: *topic_id,
                    partitions: vec![OffsetFetchResponsePartitions {
                        partition_index: 0,
                        committed_offset: -1,
                        committed_leader_epoch: -1,
                        metadata: Some(String::new()),
                        error_code: UNKNOWN_TOPIC_ID,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(*response == expected);
    }
}

#[when("offsets five and nine are committed for the known and each unresolved topic id")]
async fn commit_mixed(offsets: &mut OffsetIdentity) {
    let broker = offsets.broker.as_ref().expect("a coordinator exists");
    for &unresolved in &offsets.unresolved {
        let response = broker
            .client
            .send(OffsetCommitRequest {
                group_id: offsets.group.clone(),
                topics: vec![commit_row(offsets.topic_id, 5), commit_row(unresolved, 9)],
                ..Default::default()
            })
            .await
            .expect("offset commit");
        offsets.partial_commits.push((unresolved, response));
    }
}

#[then("each commit refuses the unknown id before acknowledging the known topic")]
fn partial_commit_answers(offsets: &OffsetIdentity) {
    let answer = |topic_id, error_code| OffsetCommitResponseTopic {
        name: String::new(),
        topic_id,
        partitions: vec![OffsetCommitResponsePartition {
            partition_index: 0,
            error_code,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(offsets.partial_commits.len() == offsets.unresolved.len());
    for (unresolved, response) in &offsets.partial_commits {
        let expected = OffsetCommitResponse {
            topics: vec![
                answer(*unresolved, UNKNOWN_TOPIC_ID),
                answer(offsets.topic_id, 0),
            ],
            ..Default::default()
        };
        assert!(*response == expected);
    }
}

async fn stop_offset_broker(offsets: OffsetIdentity) {
    offsets
        .broker
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip516_topic_identity.feature",
    name = "An offset committed by identity can be fetched without the topic name"
)]
#[tokio::test]
async fn offset_commit_and_fetch_by_topic_id_round_trip(
    offsets: OffsetIdentity,
    topic: String,
    group: String,
    offset: i64,
    selection: String,
) {
    let _ = (topic, group, offset, selection);
    stop_offset_broker(offsets).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip516_topic_identity.feature",
    name = "Offset fetch echoes unresolved topic identities in its errors"
)]
#[tokio::test]
async fn offset_fetch_unresolved_topic_id_returns_unknown_topic_id(offsets: OffsetIdentity) {
    stop_offset_broker(offsets).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip516_topic_identity.feature",
    name = "One unknown topic does not prevent a known topic offset commit"
)]
#[tokio::test]
async fn offset_commit_unresolved_topic_id_returns_unknown_topic_id(offsets: OffsetIdentity) {
    stop_offset_broker(offsets).await;
}
