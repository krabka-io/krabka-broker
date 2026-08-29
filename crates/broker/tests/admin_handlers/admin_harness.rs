//! The two setup steps every admin test in this suite repeats: building a
//! `krabka-client-core` client against a started broker, and creating the topic
//! whose configuration, partitions, or records the test then drives.

use assert2::assert;
use krabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

pub(crate) async fn build_client(addr: std::net::SocketAddr) -> krabka_client_core::Client {
    krabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", addr.port()))
        .client_id("admin-handlers-test")
        .build()
        .await
        .expect("client build")
}

pub(crate) async fn create_topic_helper(
    client: &krabka_client_core::Client,
    name: &str,
    partitions: i32,
) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.into(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("create_topics");
    let result = &resp.topics[0];
    assert!(
        result.error_code == 0,
        "create_topics failed: {:?}",
        result.error_message
    );
}
