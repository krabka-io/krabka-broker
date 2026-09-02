//! The client and wire fixtures that more than one `ListOffsets` test module
//! needs, so each of them drives the handler through the same request shape.

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsResponse},
};

use super::sentinels::UNKNOWN_EPOCH;
use crate::codes;

pub(super) fn encode_request(req: &ListOffsetsRequest, version: i16) -> Bytes {
    crate::test_support::encode_request(req, version)
}

pub(super) fn decode_response(bytes: &Bytes, version: i16) -> ListOffsetsResponse {
    crate::test_support::decode_response(bytes, version)
}

pub(super) fn test_context<'a>(
    principal: &'a krabka_security::Principal,
    peer: &'a std::net::SocketAddr,
) -> crate::handlers::RequestContext<'a> {
    crate::test_support::request_context(principal, peer, "admin-client")
}

pub(super) async fn client_for(broker: &crate::broker::BrokerHandle) -> krabka_client_core::Client {
    krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("list-offsets-test")
        .build()
        .await
        .expect("client build")
}

pub(super) async fn create_topic(
    client: &krabka_client_core::Client,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions: 1,
                replication_factor: 1,
                configs,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
}

pub(super) async fn list_one(
    client: &krabka_client_core::Client,
    topic: &str,
    timestamp: i64,
) -> ListOffsetsPartitionResponse {
    list_one_at_epoch(client, topic, timestamp, UNKNOWN_EPOCH).await
}

/// [`list_one`] with the KIP-320 `current_leader_epoch` the client asserts.
/// `UNKNOWN_EPOCH` is the sentinel for "assert nothing", which is what a
/// client sends when it holds no epoch for the partition.
pub(super) async fn list_one_at_epoch(
    client: &krabka_client_core::Client,
    topic: &str,
    timestamp: i64,
    current_leader_epoch: i32,
) -> ListOffsetsPartitionResponse {
    client
        .send(ListOffsetsRequest {
            replica_id: -1,
            topics: vec![ListOffsetsTopic {
                name: topic.to_string(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    current_leader_epoch,
                    timestamp,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("ListOffsets")
        .topics
        .remove(0)
        .partitions
        .remove(0)
}
