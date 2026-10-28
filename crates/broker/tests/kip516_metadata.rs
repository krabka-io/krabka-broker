//! KIP-516: Metadata by `topic_id` semantics.
//!
//! The test client negotiates the broker's highest `Metadata` version, which
//! carries a nullable name and a topic id on each request row. Kafka's
//! `KafkaApis.handleTopicMetadataRequest` describes the requested ids when any
//! row has a non-zero id, and ignores the names.
#![cfg(test)]
use {
    crate::support::{
        discovery::topic_metadata_request,
        topics::{creatable_topic, create_topic_request, metadata_topic},
    },
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};
mod support;

use krabka_protocol::{
    owned::{metadata_request::MetadataRequest, metadata_response::MetadataResponseTopic},
    primitives::uuid::Uuid as WireUuid,
};

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

/// A topic requested by name, by id, or by both.
struct TopicRef<'a> {
    name: Option<&'a str>,
    topic_id: WireUuid,
}

/// The topic rows of a `Metadata` request with one row per [`TopicRef`].
async fn topic_rows(
    client: &krabka_client_core::Client,
    rows: &[TopicRef<'_>],
) -> Vec<MetadataResponseTopic> {
    client
        .send(MetadataRequest {
            allow_auto_topic_creation: false,
            ..topic_metadata_request(Some(
                rows.iter()
                    .map(|row| metadata_topic(row.name.map(Into::into), row.topic_id))
                    .collect(),
            ))
        })
        .await
        .expect("metadata")
        .topics
}

struct MetadataById {
    broker: Option<InProcess>,
    requested_id: WireUuid,
    actual: Vec<MetadataResponseTopic>,
    named_topic: Vec<MetadataResponseTopic>,
}

#[fixture]
fn metadata() -> MetadataById {
    // Start this scenario with fresh inputs and observations.

    MetadataById {
        broker: None,
        requested_id: WireUuid::ZERO,
        actual: Vec::new(),
        named_topic: Vec::new(),
    }
}

#[given("a broker with an unassigned nonzero metadata topic id")]
async fn unassigned_id(metadata: &mut MetadataById) {
    metadata.broker = Some(support::start().await);
    metadata.requested_id = WireUuid(uuid::Uuid::from_u128(0xfeed_face).into_bytes());
}

#[given("topics m_a and m_b have distinct identities")]
async fn distinct_topics(metadata: &mut MetadataById) {
    let broker = support::start().await;
    for name in ["m_a", "m_b"] {
        broker
            .client
            .send(create_topic_request(
                creatable_topic().name(name.into()).call(),
            ))
            .await
            .expect("create topic");
    }
    metadata.named_topic = topic_rows(&broker.client, &[TopicRef {
        name: Some("m_b"),
        topic_id: WireUuid::ZERO,
    }])
    .await;
    metadata.requested_id = metadata.named_topic[0].topic_id;
    metadata.broker = Some(broker);
}

#[when("metadata is requested by the unassigned id alone")]
async fn request_unknown_id(metadata: &mut MetadataById) {
    let broker = metadata.broker.as_ref().expect("a broker was started");
    metadata.actual = topic_rows(&broker.client, &[TopicRef {
        name: None,
        topic_id: metadata.requested_id,
    }])
    .await;
}

#[when("metadata names m_a but supplies the identity of m_b")]
async fn request_conflicting_name(metadata: &mut MetadataById) {
    let broker = metadata.broker.as_ref().expect("a broker was started");
    metadata.actual = topic_rows(&broker.client, &[TopicRef {
        name: Some("m_a"),
        topic_id: metadata.requested_id,
    }])
    .await;
}

#[then("metadata returns UNKNOWN_TOPIC_ID with the requested identity and no name")]
fn unknown_topic(metadata: &MetadataById) {
    assert!(
        metadata.actual
            == vec![MetadataResponseTopic {
                error_code: UNKNOWN_TOPIC_ID,
                name: None,
                topic_id: metadata.requested_id,
                ..Default::default()
            }]
    );
}

#[then("the metadata describes m_b exactly as a request by its name does")]
fn identity_wins(metadata: &MetadataById) {
    assert!(metadata.actual == metadata.named_topic);
}

async fn stop_metadata_broker(metadata: MetadataById) {
    metadata
        .broker
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip516_topic_identity.feature",
    name = "An unknown id is echoed in a nameless metadata error"
)]
#[tokio::test]
async fn metadata_unknown_topic_id_returns_unknown_topic_id(metadata: MetadataById) {
    stop_metadata_broker(metadata).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip516_topic_identity.feature",
    name = "Topic identity takes precedence over a conflicting name"
)]
#[tokio::test]
async fn metadata_name_and_id_of_different_topics_describes_the_id(metadata: MetadataById) {
    stop_metadata_broker(metadata).await;
}
