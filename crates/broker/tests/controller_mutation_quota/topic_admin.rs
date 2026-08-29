//! The two controller mutations the quota is measured against: `CreateTopics`
//! (`api_key=19`) and `DeleteTopics` (`api_key=20`), each driven over an
//! authenticated connection.
//!
//! Both drivers return the pair the assertions care about — the response
//! `throttle_time_ms` and the first per-topic error code — so a test can tell a
//! quota rejection apart from an authorization failure.

use std::net::SocketAddr;

use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        delete_topics_request::DeleteTopicsRequest,
        delete_topics_response::DeleteTopicsResponse,
    },
};

use crate::wire::{round_trip, sasl_plain_authenticate};

/// Drive `CreateTopics` (`api_key=19`) over a SASL/PLAIN connection.
/// Returns `(throttle_time_ms, per-topic error_code)` from the first result.
pub(crate) async fn drive_create_topics_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    topic: &str,
    partitions: i32,
) -> (i32, i16) {
    const VERSION: i16 = 7; // MAX_VERSION; flexible (>= 5)

    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 30_000,
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for CreateTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, VERSION, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        CreateTopicsResponse::decode(&mut cur, VERSION).expect("decode CreateTopicsResponse");

    let err_code = resp.topics.first().map_or(-1, |t| t.error_code);
    (resp.throttle_time_ms, err_code)
}

/// Drive `DeleteTopics` (`api_key=20`) over a SASL/PLAIN connection.
/// Returns `(throttle_time_ms, per-topic error_code)` from the first result.
pub(crate) async fn drive_delete_topics_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    topic: &str,
) -> (i32, i16) {
    // Use version 3 (flexible=4+, topic_names field for versions 0-5).
    // Flexible starts at version 4; use version 4 to get throttle_time_ms (v1+)
    // and flexible encoding, while using topic_names (not the v6+ topics field).
    const VERSION: i16 = 4;

    let req = DeleteTopicsRequest {
        topic_names: vec![topic.to_string()],
        timeout_ms: 30_000,
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for DeleteTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode DeleteTopics");
    let resp_bytes = round_trip(&mut stream, 20, VERSION, 1, true, &body)
        .await
        .expect("DeleteTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        DeleteTopicsResponse::decode(&mut cur, VERSION).expect("decode DeleteTopicsResponse");

    let err_code = resp.responses.first().map_or(-1, |r| r.error_code);
    (resp.throttle_time_ms, err_code)
}
