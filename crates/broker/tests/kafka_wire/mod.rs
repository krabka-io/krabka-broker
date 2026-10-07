//! Raw-socket Kafka request framing and the SASL/PLAIN handshake for broker
//! integration tests that drive the wire by hand.
//!
//! [`round_trip_with`] writes one length-prefixed request with a v1 or v2
//! request header and reads back one response with its header stripped.
//! [`round_trip`] derives the response header from the request flexibility the
//! way Kafka does. [`sasl_plain_authenticate`] and
//! [`sasl_plain_authenticate_on`] run `ApiVersions`, `SaslHandshake` and
//! `SaslAuthenticate` over the same framing.
//!
//! The client id is always a parameter, because the client-quota suites key
//! their quotas on it.
//!
//! The module sits in `tests/kafka_wire/mod.rs` rather than in `support`, so a
//! suite that frames requests by hand can declare `mod kafka_wire;` without
//! compiling every cluster-boot helper. Cargo does not build a `mod.rs` under
//! `tests/` as its own test binary, and `crate_tests` in `//bazel:defs.bzl`
//! treats it as a helper source of every integration test.

// Each suite declares `mod kafka_wire;` and calls part of it, which is the
// same reason `support` carries this allow.
#![allow(dead_code)]

pub mod quotas;

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::ElectLeadersResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    records::{Record, RecordBatch},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

/// The `ApiVersions` API key, whose response header is v0 at every version.
const API_VERSIONS_KEY: i16 = 18;

/// Sends one request and returns the response body.
///
/// The response header is flexible exactly when the request header is, except
/// for `ApiVersions`, whose response header stays v0 so that a client can read
/// it before it knows which versions the broker speaks.
///
/// # Errors
///
/// Returns the I/O error of a failed write or read, and an error when a
/// flexible response header is malformed.
pub async fn round_trip<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    client_id: &str,
    flexible: bool,
    body: &[u8],
) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let flexibility = Flexibility {
        request: flexible,
        response: flexible && api_key != API_VERSIONS_KEY,
    };
    round_trip_with(
        stream,
        api_key,
        api_version,
        corr_id,
        client_id,
        flexibility,
        body,
    )
    .await
}

/// Whether the request header and the response header of one exchange are
/// flexible (v2 request, v1 response) and so carry a tagged-fields byte.
#[derive(Clone, Copy, Debug)]
pub struct Flexibility {
    /// The request header is v2.
    pub request: bool,
    /// The response header is v1.
    pub response: bool,
}

/// Sends one request with the request and response header flexibility chosen
/// independently, and returns the response body with its header stripped.
///
/// A flexible request header ends in an empty tagged-fields byte. A flexible
/// response header must carry that byte as zero.
///
/// # Errors
///
/// Returns the I/O error of a failed write or read, and an error when a
/// flexible response header is missing its tagged-fields byte or carries tagged
/// fields.
pub async fn round_trip_with<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    client_id: &str,
    flexibility: Flexibility,
    body: &[u8],
) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(16 + client_id.len() + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits in i16"));
    frame.put_slice(client_id.as_bytes());
    if flexibility.request {
        frame.put_u8(0); // empty header tagged-fields byte
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    if cur.len() < 4 {
        return Err(io::Error::other("response missing correlation id"));
    }
    let _resp_corr_id = cur.get_i32();
    if flexibility.response {
        if cur.is_empty() {
            return Err(io::Error::other(
                "flexible response missing tagged-fields byte",
            ));
        }
        let tagged = cur.get_u8();
        if tagged != 0 {
            return Err(io::Error::other(format!(
                "response header tagged-fields byte is {tagged}, expected 0"
            )));
        }
    }
    Ok(cur.to_vec())
}

/// Connects to `addr` and authenticates the connection with SASL/PLAIN.
///
/// # Errors
///
/// Returns the error of a failed connect, of a failed round trip, or of a
/// non-zero `SaslHandshake` or `SaslAuthenticate` error code.
pub async fn sasl_plain_authenticate(
    addr: SocketAddr,
    client_id: &str,
    user: &str,
    password: &[u8],
) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    sasl_plain_authenticate_on(&mut stream, client_id, user, password).await?;
    Ok(stream)
}

/// Authenticates an open connection with SASL/PLAIN: `ApiVersions` v0, then
/// `SaslHandshake` v1 for `PLAIN`, then `SaslAuthenticate` v2 carrying
/// `\0user\0password`, on correlation ids 1, 2 and 3.
///
/// # Errors
///
/// Returns the error of a failed round trip, encode or decode, or of a
/// non-zero `SaslHandshake` or `SaslAuthenticate` error code.
pub async fn sasl_plain_authenticate_on<S>(
    stream: &mut S,
    client_id: &str,
    user: &str,
    password: &[u8],
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut av_body = BytesMut::new();
    ApiVersionsRequest::default()
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp = round_trip(stream, API_VERSIONS_KEY, 0, 1, client_id, false, &av_body).await?;
    ApiVersionsResponse::decode(&mut &av_resp[..], 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp = round_trip(stream, 17, 1, 2, client_id, false, &sh_body).await?;
    let sh_resp = SaslHandshakeResponse::decode(&mut &sh_resp[..], 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let payload = crate::kafka_wire::plain_payload(user, password);
    let mut auth_body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: payload,
        ..Default::default()
    }
    .encode(&mut auth_body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let auth_resp = round_trip(stream, 36, 2, 3, client_id, true, &auth_body).await?;
    let auth_resp = SaslAuthenticateResponse::decode(&mut &auth_resp[..], 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={} message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }
    Ok(())
}

/// Encode, exchange and decode a typed request using the caller's exact wire version.
pub async fn exchange<S, Q, R>(
    stream: &mut S,
    request: &Q,
    api_key: i16,
    version: i16,
    corr_id: i32,
    client_id: &str,
    flexible: bool,
) -> io::Result<R>
where
    S: AsyncRead + AsyncWrite + Unpin,
    Q: Encode,
    R: for<'de> Decode<'de>,
{
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(io::Error::other)?;
    let bytes = round_trip(
        stream, api_key, version, corr_id, client_id, flexible, &body,
    )
    .await?;
    R::decode(&mut &bytes[..], version).map_err(io::Error::other)
}

/// PLAIN's empty authorization id followed by the user and password.
pub fn plain_payload(user: &str, password: &[u8]) -> Bytes {
    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0);
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password);
    Bytes::from(payload)
}

/// A topic with automatic placement and the requested partition geometry.
pub fn topic(
    name: &str,
    partitions: i32,
    replication_factor: i16,
) -> krabka_protocol::owned::create_topics_request::CreatableTopic {
    krabka_protocol::owned::create_topics_request::CreatableTopic {
        name: name.to_string(),
        num_partitions: partitions,
        replication_factor,
        ..Default::default()
    }
}

/// Create exactly one topic on an open connection, checking the whole response row's status.
pub async fn create_topic_on(
    stream: &mut TcpStream,
    client_id: &str,
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
) {
    let name = topic.name.clone();
    let request = krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
        topics: vec![topic],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let response: krabka_protocol::owned::create_topics_response::CreateTopicsResponse =
        exchange(stream, &request, 19, 7, 1, client_id, true)
            .await
            .expect("CreateTopics round-trip");
    assert2::assert!(response.topics.len() == 1);
    assert2::assert!(
        response.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        response.topics[0].error_message
    );
}

/// Create a topic on a newly authenticated PLAIN session.
pub async fn create_topic_sasl(
    addr: SocketAddr,
    client_id: &str,
    (user, password): (&str, &[u8]),
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
) {
    let mut stream = sasl_plain_authenticate(addr, client_id, user, password)
        .await
        .expect("SASL authenticate for CreateTopics");
    create_topic_on(&mut stream, client_id, topic).await;
}

pub async fn sasl_scram_authenticate_on(
    stream: &mut TcpStream,
    client_id: &str,
    user: &str,
    password: &str,
    mechanism: krabka_security::SaslMechanism,
) -> io::Result<()> {
    // ── 1. ApiVersions (v0, non-flexible). Same as PLAIN: pre-auth allowlist.
    let _av_resp: ApiVersionsResponse = exchange(
        stream,
        &ApiVersionsRequest::default(),
        18,
        0,
        1,
        client_id,
        false,
    )
    .await?;

    // ── 2. SaslHandshake v1 (non-flexible).
    let sh_resp: SaslHandshakeResponse = exchange(
        stream,
        &SaslHandshakeRequest {
            mechanism: mechanism.wire_name().to_string(),
            ..Default::default()
        },
        17,
        1,
        2,
        client_id,
        false,
    )
    .await?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    // ── 3. SCRAM client-first → server-first.
    let client = krabka_security::ScramClientExchange::new(
        user.to_string(),
        password.as_bytes().to_vec(),
        mechanism,
    );
    let (client_first, client) = client
        .client_first()
        .map_err(|e| io::Error::other(format!("scram client_first: {e:?}")))?;
    let scram_first_response =
        sasl_authenticate_on(stream, client_id, 3, Bytes::from(client_first)).await?;
    if scram_first_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate round 1 failed: error_code={} error_message={:?}",
            scram_first_response.error_code, scram_first_response.error_message
        )));
    }
    let server_first = scram_first_response.auth_bytes.to_vec();

    // ── 4. SCRAM client-final → server-final.
    let (client_final, client) = client
        .step(&server_first)
        .map_err(|e| io::Error::other(format!("scram client step: {e:?}")))?;
    let scram_final_response =
        sasl_authenticate_on(stream, client_id, 4, Bytes::from(client_final)).await?;
    if scram_final_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate round 2 failed: error_code={} error_message={:?}",
            scram_final_response.error_code, scram_final_response.error_message
        )));
    }
    // Client verifies server signature — proves the broker holds the
    // expected `server_key` rather than just any matching `stored_key`.
    client
        .verify_server_final(&scram_final_response.auth_bytes)
        .map_err(|e| io::Error::other(format!("server-final verify: {e:?}")))?;

    Ok(())
}

/// One SASL Authenticate v2 message; callers decide whether an error is expected.
pub async fn sasl_authenticate_on<S>(
    stream: &mut S,
    client_id: &str,
    corr_id: i32,
    auth_bytes: Bytes,
) -> io::Result<SaslAuthenticateResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    exchange(
        stream,
        &SaslAuthenticateRequest {
            auth_bytes,
            ..Default::default()
        },
        36,
        2,
        corr_id,
        client_id,
        true,
    )
    .await
}

pub async fn sasl_plain_reauthenticate_on(
    stream: &mut TcpStream,
    client_id: &str,
    corr: &mut i32,
    user: &str,
    password: &[u8],
) -> Result<SaslAuthenticateResponse, io::Error> {
    *corr += 1;
    let sh_resp: SaslHandshakeResponse = exchange(
        stream,
        &SaslHandshakeRequest {
            mechanism: "PLAIN".to_string(),
            ..Default::default()
        },
        17,
        1,
        *corr,
        client_id,
        false,
    )
    .await?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let payload = plain_payload(user, password);
    *corr += 1;
    sasl_authenticate_on(stream, client_id, *corr, payload).await
}

/// A Produce v11-compatible batch of zero-filled records for quota tests.
pub fn produce_records(
    topic: &str,
    record_bytes: usize,
    count: usize,
) -> krabka_protocol::owned::produce_request::ProduceRequest {
    let value = vec![0u8; record_bytes];
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(bytes::Bytes::copy_from_slice(&value)),
            ..Default::default()
        })
        .collect();

    ProduceRequest {
        acks: 1,
        timeout_ms: 30_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(
                    RecordBatch {
                        last_offset_delta: i32::try_from(count - 1).unwrap(),
                        records,
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Build an all-ISR Produce request with one record.
pub fn single_record_produce_request(topic: &str, partition: i32, value: &[u8]) -> ProduceRequest {
    ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(
                    RecordBatch {
                        last_offset_delta: 0,
                        records: vec![Record {
                            offset_delta: 0,
                            value: Some(bytes::Bytes::copy_from_slice(value)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub async fn elect_leaders(
    stream: &mut TcpStream,
    client_id: &str,
    topic: &str,
    partitions: Vec<i32>,
    election_type: i8,
) -> Vec<(i32, i16)> {
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
    let resp: ElectLeadersResponse = exchange(stream, &req, 43, 2, 1, client_id, true)
        .await
        .expect("ElectLeaders round-trip");

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

pub async fn create_topic_plaintext(
    addr: SocketAddr,
    client_id: &str,
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    create_topic_on(&mut stream, client_id, topic).await;
}
