//! The PLAINTEXT wire helpers that this suite drives `ElectLeaders` through:
//! the client id every request carries, the typed `ElectLeaders` driver, and the `CreateTopics` call that materialises the partition whose
//! leader the tests then elect.
//!
//! The compatibility shim of the authorizer maps an empty `super_users` list
//! and zero ACLs to Allow, so these helpers need no SASL handshake. The
//! `SASL_PLAINTEXT` flavours of the same two drivers live in `sasl`, next to
//! the authorization test that is the only user of a SASL listener here.

use std::net::SocketAddr;

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::CreateTopicsRequest,
        create_topics_response::CreateTopicsResponse,
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::ElectLeadersResponse,
    },
};
use tokio::net::TcpStream;

use crate::kafka_wire;

pub const ELECT_LEADERS_VERSION: i16 = 2;

/// The client id every request header in this suite carries.
pub const CLIENT_ID: &str = "krabka-elect-test";

/// Drives `ElectLeaders` over a fresh PLAINTEXT connection.
///
/// The compat shim of the authorizer maps no `super_users` and no ACLs to
/// Allow, so this request passes without SASL. This function asserts that the
/// top-level `error_code == 0`. It returns the per-partition
/// `(partition_id, error_code)` rows for the topic named `topic`.
/// Sends `ElectLeaders` with a null topic list -- the "every partition in the
/// cluster" shape -- and returns every `(topic, partition, error_code)` row
/// the broker answered with.
pub async fn drive_elect_all_partitions(
    addr: SocketAddr,
    election_type: i8,
) -> Vec<(String, i32, i16)> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let req = ElectLeadersRequest {
        election_type,
        topic_partitions: None,
        timeout_ms: 30_000,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, ELECT_LEADERS_VERSION)
        .expect("encode ElectLeaders");
    let resp_bytes = kafka_wire::round_trip(
        &mut stream,
        43,
        ELECT_LEADERS_VERSION,
        1,
        CLIENT_ID,
        true,
        &body,
    )
    .await
    .expect("ElectLeaders round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ElectLeadersResponse::decode(&mut cur, ELECT_LEADERS_VERSION)
        .expect("decode ElectLeadersResponse");

    assert!(
        resp.error_code == 0,
        "top-level error_code must be 0, got {}",
        resp.error_code
    );

    resp.replica_election_results
        .into_iter()
        .flat_map(|result| {
            let topic = result.topic;
            result
                .partition_result
                .into_iter()
                .map(move |p| (topic.clone(), p.partition_id, p.error_code))
        })
        .collect()
}

pub async fn drive_elect_leaders(
    addr: SocketAddr,
    topic: &str,
    partitions: Vec<i32>,
    election_type: i8,
) -> Vec<(i32, i16)> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let req = ElectLeadersRequest {
        election_type,
        topic_partitions: Some(vec![TopicPartitions {
            topic: topic.to_string(),
            partitions,
            ..Default::default()
        }]),
        timeout_ms: 30_000,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, ELECT_LEADERS_VERSION)
        .expect("encode ElectLeaders");
    let resp_bytes = kafka_wire::round_trip(
        &mut stream,
        43,
        ELECT_LEADERS_VERSION,
        1,
        CLIENT_ID,
        true,
        &body,
    )
    .await
    .expect("ElectLeaders round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ElectLeadersResponse::decode(&mut cur, ELECT_LEADERS_VERSION)
        .expect("decode ElectLeadersResponse");

    assert!(
        resp.error_code == 0,
        "top-level error_code must be 0, got {}",
        resp.error_code
    );

    resp.replica_election_results
        .into_iter()
        .find(|r| r.topic == topic)
        .map(|r| {
            r.partition_result
                .into_iter()
                .map(|p| (p.partition_id, p.error_code))
                .collect()
        })
        .unwrap_or_default()
}

/// Creates a topic on a PLAINTEXT broker.
///
/// The compat shim of the authorizer lets the request through because there
/// are no `super_users` and no ACLs.
///
/// The topic has one partition on `replicas`, in that order, so the tests know
/// which broker leads it and which one they can elect: an automatic placement
/// starts at a random broker.
pub async fn create_topic_plaintext(addr: SocketAddr, name: &str, replicas: &[i32]) {
    let req = CreateTopicsRequest {
        topics: vec![crate::support::topic_on(name, &[replicas])],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, 7).expect("encode CreateTopics");
    let resp_bytes = kafka_wire::round_trip(&mut stream, 19, 7, 1, CLIENT_ID, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, 7).expect("decode CreateTopicsResponse");
    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}
