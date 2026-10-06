//! PLAINTEXT wire helpers for the partition-reassignment suite: one
//! length-prefixed request and response exchange, and the `CreateTopics` call
//! that provisions a topic before a test drives a reassignment.
//!
//! The authorizer compatibility shim allows every request when there are no
//! `super_users` and no ACLs, so nothing here performs a SASL handshake.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{Decode, Encode};
use tokio::net::TcpStream;

use crate::kafka_wire;

/// The client id every request header in this suite carries.
pub const CLIENT_ID: &str = "krabka-reassign-test";

/// One length-prefixed request and response exchange over a PLAINTEXT
/// connection; see [`kafka_wire::round_trip`].
pub async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> io::Result<Vec<u8>> {
    kafka_wire::round_trip(
        stream,
        api_key,
        api_version,
        corr_id,
        CLIENT_ID,
        flexible,
        body,
    )
    .await
}

/// Creates a topic over PLAINTEXT. The authorizer's compat shim, with no
/// `super_users` and no ACLs, lets the request through. Copied from
/// `elect_leaders.rs`.
pub async fn create_topic_plaintext(
    addr: SocketAddr,
    name: &str,
    partitions: i32,
    replication_factor: i16,
) {
    use krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };

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
    let mut stream = TcpStream::connect(addr).await.expect("connect");
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
