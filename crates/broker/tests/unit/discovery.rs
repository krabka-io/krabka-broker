//! What a client learns about the cluster before it produces or consumes.
//!
//! `ApiVersions`, `Metadata`, and `FindCoordinator` are the three round trips
//! a client makes on a fresh connection, and each of them reports this single
//! broker back to the caller.

use assert2::{assert, check};
use krabka_protocol::owned::{
    api_versions_request::ApiVersionsRequest,
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    find_coordinator_request::FindCoordinatorRequest,
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
        .send(MetadataRequest::default())
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

#[tokio::test]
async fn find_coordinator_returns_self() {
    let p = support::start().await;
    let req = FindCoordinatorRequest {
        coordinator_keys: vec!["any-group".into()],
        ..Default::default()
    };
    let r = p.client.send(req).await.expect("FindCoordinator");
    for c in &r.coordinators {
        check!(c.error_code == 0);
        check!(c.node_id == 1);
        check!(!c.host.is_empty());
        check!(c.port > 0);
    }
    p.broker.shutdown().await;
}
