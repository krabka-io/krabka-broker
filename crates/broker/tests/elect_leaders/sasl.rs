//! The SASL/PLAIN half of the wire layer: the authentication exchange, and the
//! `CreateTopics` and `ElectLeaders` drivers that run over an authenticated
//! stream.
//!
//! Only the authorization test uses these. It needs a named principal for the
//! authorizer to deny, so its listener is `SASL_PLAINTEXT` rather than the
//! PLAINTEXT one the election tests use.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::ElectLeadersResponse,
    },
};
use tokio::net::TcpStream;

use crate::{
    kafka_wire,
    wire::{CLIENT_ID, ELECT_LEADERS_VERSION, round_trip},
};

/// Creates a topic with SASL/PLAIN.
///
/// The auth-deny test uses this helper, because its listener is
/// `SASL_PLAINTEXT` and not PLAINTEXT.
pub async fn create_topic_sasl_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    name: &str,
    partitions: i32,
    replication_factor: i16,
) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_string(),
            num_partitions: partitions,
            replication_factor,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = sasl_plain_authenticate(addr, user, password)
        .await
        .expect("SASL authenticate for CreateTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, 7).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, 7, 1, true, &body)
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

/// Drives `ElectLeaders` over a SASL/PLAIN authenticated connection.
///
/// This function returns the per-partition `(partition_id, error_code)` rows
/// for the given topic. It does **not** assert the top-level `error_code`, so
/// the caller can examine per-row auth failures.
pub async fn drive_elect_leaders_sasl_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
    partitions: Vec<i32>,
    election_type: i8,
) -> Vec<(i32, i16)> {
    let mut stream = sasl_plain_authenticate(addr, user, password)
        .await
        .expect("SASL authenticate for ElectLeaders");
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
    let resp_bytes = round_trip(&mut stream, 43, ELECT_LEADERS_VERSION, 1, true, &body)
        .await
        .expect("ElectLeaders round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ElectLeadersResponse::decode(&mut cur, ELECT_LEADERS_VERSION)
        .expect("decode ElectLeadersResponse");

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

/// Opens a TCP stream to `addr` and authenticates it with SASL/PLAIN; see
/// [`kafka_wire::sasl_plain_authenticate`].
pub async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> io::Result<TcpStream> {
    kafka_wire::sasl_plain_authenticate(addr, CLIENT_ID, user, password).await
}
