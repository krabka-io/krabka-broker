//! KIP-516: Metadata by `topic_id` semantics.
//!
//! The test client negotiates the broker's highest `Metadata` version, which
//! carries a nullable name and a topic id on each request row. Kafka's
//! `KafkaApis.handleTopicMetadataRequest` describes the requested ids when any
//! row has a non-zero id, and ignores the names.
use assert2::assert;
mod support;

use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponseTopic,
    },
    primitives::uuid::Uuid as WireUuid,
};

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

/// The topic rows of a `Metadata` request with one row per `(name, id)`.
async fn topic_rows(
    client: &krabka_client_core::Client,
    rows: &[(Option<&str>, WireUuid)],
) -> Vec<MetadataResponseTopic> {
    client
        .send(MetadataRequest {
            topics: Some(
                rows.iter()
                    .map(|(name, topic_id)| MetadataRequestTopic {
                        name: name.map(Into::into),
                        topic_id: *topic_id,
                        ..Default::default()
                    })
                    .collect(),
            ),
            allow_auto_topic_creation: false,
            ..Default::default()
        })
        .await
        .expect("metadata")
        .topics
}

#[tokio::test]
async fn metadata_unknown_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    let bogus = WireUuid(uuid::Uuid::from_u128(0xfeed_face).into_bytes());

    let rows = topic_rows(&p.client, &[(None, bogus)]).await;

    assert!(
        rows == vec![MetadataResponseTopic {
            error_code: UNKNOWN_TOPIC_ID,
            name: None,
            topic_id: bogus,
            ..Default::default()
        }]
    );
}

#[tokio::test]
async fn metadata_name_and_id_of_different_topics_describes_the_id() {
    let p = support::start().await;
    for n in ["m_a", "m_b"] {
        p.client
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: n.into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .expect("create topic");
    }
    let by_name = topic_rows(&p.client, &[(Some("m_b"), WireUuid::ZERO)]).await;
    let id_b = by_name[0].topic_id;

    // The row names "m_a" but carries m_b's id. Kafka uses the id.
    let by_id = topic_rows(&p.client, &[(Some("m_a"), id_b)]).await;

    assert!(by_id == by_name);
}
