//! What a client learns about the cluster before it produces or consumes.
//!
//! `ApiVersions`, `Metadata`, and `FindCoordinator` are the three round trips
//! a client makes on a fresh connection, and each of them reports this single
//! broker back to the caller. The first group lookup finds no
//! `__consumer_offsets`, so it asks for the topic and the client retries.

use assert2::{assert, check};
use krabka_protocol::owned::{
    api_versions_request::ApiVersionsRequest,
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    find_coordinator_request::FindCoordinatorRequest,
    find_coordinator_response::{Coordinator, FindCoordinatorResponse},
    metadata_request::MetadataRequest,
};

use crate::support;

#[tokio::test]
async fn api_versions_round_trip() {
    let p = support::start().await;
    let resp = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "krabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");
    assert!(resp.error_code == 0);
    // Must include ApiVersions itself.
    assert!(resp.api_keys.iter().any(|k| k.api_key == 18));
    p.broker.shutdown().await;
}

#[tokio::test]
async fn metadata_returns_this_broker_and_listed_topics() {
    let p = support::start().await;
    // Create a topic first.
    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "beta".into(),
            num_partitions: 3,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let _ = p.client.send(create).await.unwrap();

    let resp = p
        .client
        // Null topics means every topic; the schema default is an empty list.
        .send(MetadataRequest {
            topics: None,
            ..Default::default()
        })
        .await
        .expect("Metadata");
    assert!(resp.brokers.len() == 1);
    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("beta"))
        .unwrap();
    assert!(topic.partitions.len() == 3);
    for (i, part) in topic.partitions.iter().enumerate() {
        check!(part.error_code == 0);
        check!(part.partition_index == i32::try_from(i).unwrap());
        check!(part.leader_id == 1);
    }
    p.broker.shutdown().await;
}

/// Kafka's `KafkaApis.getCoordinator`: the first group lookup finds no
/// `__consumer_offsets`, asks for it, and answers `COORDINATOR_NOT_AVAILABLE`
/// (15) with `Node.noNode()`. A retried lookup names this broker.
#[tokio::test]
async fn find_coordinator_creates_the_offsets_topic_then_returns_self() {
    let p = support::start().await;
    let req = FindCoordinatorRequest {
        coordinator_keys: vec!["any-group".into()],
        ..Default::default()
    };
    let first = p.client.send(req).await.expect("FindCoordinator");
    check!(
        first
            == FindCoordinatorResponse {
                coordinators: vec![Coordinator {
                    key: "any-group".into(),
                    node_id: -1,
                    host: String::new(),
                    port: -1,
                    error_code: 15,
                    error_message: None,
                    ..Default::default()
                }],
                ..Default::default()
            }
    );

    let retried = support::find_coordinator(&p.client, support::KEY_TYPE_GROUP, "any-group").await;
    let listen = p.broker.listen_addr();
    check!(
        retried
            == Coordinator {
                key: "any-group".into(),
                node_id: 1,
                host: listen.ip().to_string(),
                port: i32::from(listen.port()),
                error_code: 0,
                error_message: None,
                ..Default::default()
            }
    );
    p.broker.shutdown().await;
}
