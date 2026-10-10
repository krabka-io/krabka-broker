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
use bytes::{Buf, Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::ElectLeadersResponse,
        produce_request::ProduceRequest,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    records::{Record, RecordBatch},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
pub use topics::creatable_topic as topic;

use crate::support::{topics, wire as frames};

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
    let frame = frames::request_frame(frames::WireFrameSetup {
        api_key: krabka_ids::ApiKey(api_key),
        version: krabka_ids::ApiVersion(api_version),
        correlation: crate::support::wire::CorrelationId(corr_id),
        header: crate::support::wire::HeaderEncoding::from_wire(flexibility.request),
        client_id,
        body,
        capacity: Some(
            <krabka_units::ByteSize as krabka_units::convert::ByteSizeExt>::from_bytes(
                u64::try_from(16 + client_id.len() + body.len()).expect("frame capacity fits u64"),
            ),
        ),
        length_context: Some("client_id fits in i16"),
    });

    frames::write_frame(stream, &frame, Some("frame fits in u32")).await?;

    let resp = frames::read_frame(stream).await?;

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

/// Send a flexible request on a new PLAIN connection, retaining named codec errors.
///
/// # Errors
/// Returns authentication, framing, or codec errors from the exchange.
pub async fn request_as_plain<Q, R>(
    addr: SocketAddr,
    client_id: &str,
    (user, password): (&str, &[u8]),
    request: &Q,
    (api_key, version): (i16, i16),
    operation: &str,
) -> io::Result<R>
where
    Q: Encode,
    R: for<'de> Decode<'de>,
{
    let mut stream = sasl_plain_authenticate(addr, client_id, user, password).await?;
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| io::Error::other(format!("{operation} encode: {e}")))?;
    let response = round_trip(&mut stream, api_key, version, 4, client_id, true, &body).await?;
    R::decode(&mut &response[..], version)
        .map_err(|e| io::Error::other(format!("{operation} decode: {e}")))
}

/// Create one topic as the admin fixture, checking the entire response's cardinality.
///
/// # Panics
/// Panics if authentication or creation fails, or the response has no unique topic row.
pub async fn create_topic_as_super_user(
    addr: SocketAddr,
    client_id: &str,
    name: &str,
    partitions: i32,
) {
    let request =
        topics::create_topic_request(topic(crate::support::topics::ConfiguredTopicSetup {
            name: name.to_string(),
            partitions: crate::support::topics::TopicPartitionCount(partitions),
            ..Default::default()
        }));
    let response: krabka_protocol::owned::create_topics_response::CreateTopicsResponse =
        request_as_plain(
            addr,
            client_id,
            ("admin", b"admin-secret"),
            &request,
            (19, 7),
            "CreateTopics",
        )
        .await
        .expect("CreateTopics as super-user must round-trip");
    assert!(response.topics.len() == 1, "one topic in response");
    assert!(
        response.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        response.topics[0].error_message
    );
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
    let av_body = encode_named(&ApiVersionsRequest::default(), 0, "ApiVersions")?;
    let av_resp = round_trip(stream, API_VERSIONS_KEY, 0, 1, client_id, false, &av_body).await?;
    decode_named::<ApiVersionsResponse>(&av_resp, 0, "ApiVersions")?;

    let sh_body = encode_named(
        &SaslHandshakeRequest {
            mechanism: "PLAIN".to_string(),
            ..Default::default()
        },
        1,
        "SaslHandshake",
    )?;
    let sh_resp = round_trip(stream, 17, 1, 2, client_id, false, &sh_body).await?;
    let sh_resp = decode_named::<SaslHandshakeResponse>(&sh_resp, 1, "SaslHandshake")?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let payload = crate::kafka_wire::plain_payload(user, password);
    let auth_body = encode_named(
        &SaslAuthenticateRequest {
            auth_bytes: payload,
            ..Default::default()
        },
        2,
        "SaslAuthenticate",
    )?;
    let auth_resp = round_trip(stream, 36, 2, 3, client_id, true, &auth_body).await?;
    let auth_resp = decode_named::<SaslAuthenticateResponse>(&auth_resp, 2, "SaslAuthenticate")?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={} message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }
    Ok(())
}

/// Encode a typed body with the caller's original operation-labelled error.
///
/// # Errors
/// Returns the codec failure with its operation and encoding phase.
pub fn encode_named<Q: Encode>(request: &Q, version: i16, operation: &str) -> io::Result<Bytes> {
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|error| io::Error::other(format!("{operation} encode: {error}")))?;
    Ok(body.freeze())
}

/// Decode a typed body with its original operation-labelled error.
///
/// # Errors
/// Returns the codec failure with its operation and decoding phase.
pub fn decode_named<R: for<'de> Decode<'de>>(
    mut body: &[u8],
    version: i16,
    operation: &str,
) -> io::Result<R> {
    R::decode(&mut body, version)
        .map_err(|error| io::Error::other(format!("{operation} decode: {error}")))
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

/// Send one typed raw request on a fresh connection, retaining its encoded body length.
///
/// # Panics
/// Panics on encoding, connection, framing or response-decoding failure, as the raw fixtures do.
pub async fn request_once<Q, R>(
    addr: SocketAddr,
    request: &Q,
    api: (i16, i16),
    client_id: &str,
    header: (i32, bool),
) -> (usize, R)
where
    Q: Encode,
    R: for<'de> Decode<'de>,
{
    let (api_key, version) = api;
    let (correlation_id, flexible) = header;
    let mut body = BytesMut::new();
    request.encode(&mut body, version).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let response = round_trip(
        &mut stream,
        api_key,
        version,
        correlation_id,
        client_id,
        flexible,
        &body,
    )
    .await
    .unwrap();
    let response = R::decode(&mut &response[..], version).unwrap();
    (body.len(), response)
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

/// Create exactly one topic on an open connection, checking the whole response row's status.
pub async fn create_topic_on(
    stream: &mut TcpStream,
    client_id: &str,
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
) {
    let name = topic.name.clone();
    let request = topics::create_topic_request(topic);
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
    sasl_handshake_on(stream, client_id, 2, mechanism.wire_name()).await?;

    // ── 3. SCRAM client-first → server-first.
    let (client_first, client) = krabka_macros::scram_client_first!(user, password, mechanism)?;
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

/// A successful v1 handshake on an existing SASL session.
pub async fn sasl_handshake_on<S>(
    stream: &mut S,
    client_id: &str,
    corr_id: i32,
    mechanism: &str,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let response: SaslHandshakeResponse = exchange(
        stream,
        &SaslHandshakeRequest {
            mechanism: mechanism.to_string(),
            ..Default::default()
        },
        17,
        1,
        corr_id,
        client_id,
        false,
    )
    .await?;
    if response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            response.error_code
        )));
    }
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
    sasl_handshake_on(stream, client_id, *corr, "PLAIN").await?;

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

    crate::support::produce::batch_request(
        RecordBatch {
            last_offset_delta: i32::try_from(count - 1).unwrap(),
            records,
            ..Default::default()
        },
        crate::support::produce::SinglePartitionProduceSetup {
            topic: topic.to_string(),
            timeout: crate::support::produce::ProduceTimeoutMillis(30_000),
            ..Default::default()
        },
    )
}

/// Build an all-ISR Produce request with one record.
pub fn single_record_produce_request(topic: &str, partition: i32, value: &[u8]) -> ProduceRequest {
    crate::support::produce::batch_request(
        RecordBatch {
            last_offset_delta: 0,
            records: vec![Record {
                offset_delta: 0,
                value: Some(bytes::Bytes::copy_from_slice(value)),
                ..Default::default()
            }],
            ..Default::default()
        },
        crate::support::produce::SinglePartitionProduceSetup {
            topic: topic.to_string(),
            partition: krabka_ids::PartitionIndex(partition),
            ..crate::support::produce::SinglePartitionProduceSetup::replicated()
        },
    )
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

    crate::support::admin::election_partition_errors(resp, topic)
}

pub async fn create_topic_plaintext(
    addr: SocketAddr,
    client_id: &str,
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    create_topic_on(&mut stream, client_id, topic).await;
}

/// Automatic placement and client identity, with a one-partition orders topic by default.
#[derive(krabka_macros::FieldDefaults)]
pub struct AutomaticTopicSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    pub topic: crate::support::topics::ConfiguredTopicSetup,
}

/// SASL/PLAIN topic creation defaults to the admin fixture's credentials.
#[derive(krabka_macros::FieldDefaults)]
pub struct SaslTopicSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    #[default("admin")]
    pub user: &'a str,
    #[default(b"admin-secret")]
    pub password: &'a [u8],
    pub topic: crate::support::topics::ConfiguredTopicSetup,
}

pub async fn create_configured_topic_sasl(addr: SocketAddr, setup: SaslTopicSetup<'_>) {
    create_topic_sasl(
        addr,
        setup.client_id,
        (setup.user, setup.password),
        topic(setup.topic),
    )
    .await;
}

/// Create an automatically placed topic as the native admin fixture principal.
pub async fn create_topic_as_admin(addr: SocketAddr, setup: AutomaticTopicSetup<'_>) {
    create_configured_topic_sasl(
        addr,
        SaslTopicSetup {
            client_id: setup.client_id,
            topic: setup.topic,
            ..Default::default()
        },
    )
    .await;
}

/// Create a topic on a fresh plaintext connection with the original connect diagnostic.
///
/// # Panics
/// Panics if the connection fails or the topic creation is rejected.
pub async fn create_topic_on_fresh_connection(
    addr: SocketAddr,
    client_id: &str,
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    create_topic_on(&mut stream, client_id, topic).await;
}

/// Create an automatically placed topic over plaintext.
pub async fn create_automatic_topic_plaintext(addr: SocketAddr, setup: AutomaticTopicSetup<'_>) {
    create_topic_on_fresh_connection(addr, setup.client_id, topic(setup.topic)).await;
}

/// Create one partition with an explicit ordered assignment over plaintext.
pub async fn create_assigned_topic_plaintext(
    addr: SocketAddr,
    client_id: &str,
    name: &str,
    replicas: &[i32],
) {
    create_topic_on_fresh_connection(addr, client_id, crate::support::topic_on(name, &[replicas]))
        .await;
}

/// Elect named partitions over a new plaintext connection and retain the top-level oracle.
///
/// # Panics
/// Panics if the connection, election round-trip or top-level election check fails.
pub async fn elect_leaders_plaintext(
    addr: SocketAddr,
    client_id: &str,
    topic: &str,
    partitions: Vec<i32>,
    election_type: i8,
) -> Vec<(i32, i16)> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    elect_leaders(&mut stream, client_id, topic, partitions, election_type).await
}

/// Authenticate with one client id and send Produce v11 with the requested wire id.
///
/// # Panics
/// Panics if record construction, SASL authentication, encoding or the wire exchange fails.
pub async fn produce_sasl_with_ids(
    addr: SocketAddr,
    client_ids: (&str, &str),
    credentials: (&str, &[u8]),
    records: (&str, usize, usize),
) -> krabka_protocol::owned::produce_response::ProduceResponse {
    const VERSION: i16 = 11;
    let req = produce_records(records.0, records.1, records.2);
    let mut stream = sasl_plain_authenticate(addr, client_ids.0, credentials.0, credentials.1)
        .await
        .expect("SASL authenticate for Produce");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Produce");
    let resp_bytes = round_trip(&mut stream, 0, VERSION, 1, client_ids.1, true, &body)
        .await
        .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    krabka_protocol::owned::produce_response::ProduceResponse::decode(&mut cur, VERSION)
        .expect("decode ProduceResponse")
}

/// Automatic plaintext creation with the original JBOD fixtures' connect unwrap policy.
pub async fn create_configured_topic_plaintext(addr: SocketAddr, setup: AutomaticTopicSetup<'_>) {
    create_topic_plaintext(addr, setup.client_id, topic(setup.topic)).await;
}
