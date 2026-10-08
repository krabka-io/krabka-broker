//! The SASL/PLAIN half of the wire layer: the `CreateTopics` and
//! `ElectLeaders` drivers that run over an authenticated stream.
//!
//! Only the authorization test uses these. It needs a named principal for the
//! authorizer to deny, so its listener is `SASL_PLAINTEXT` rather than the
//! PLAINTEXT one the election tests use.

use std::net::SocketAddr;

use krabka_protocol::owned::{
    elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
    elect_leaders_response::ElectLeadersResponse,
};

use crate::{
    kafka_wire,
    wire::{CLIENT_ID, ELECT_LEADERS_VERSION},
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
    kafka_wire::create_topic_sasl(
        addr,
        CLIENT_ID,
        (user, password),
        kafka_wire::topic(name, partitions, replication_factor),
    )
    .await;
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
    let mut stream = kafka_wire::sasl_plain_authenticate(addr, CLIENT_ID, user, password)
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
    let resp: ElectLeadersResponse = kafka_wire::exchange(
        &mut stream,
        &req,
        43,
        ELECT_LEADERS_VERSION,
        1,
        CLIENT_ID,
        true,
    )
    .await
    .expect("ElectLeaders round-trip");

    crate::support::admin::election_partition_errors(resp, topic)
}
