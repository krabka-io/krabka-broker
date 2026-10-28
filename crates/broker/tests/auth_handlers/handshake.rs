//! The SASL gate on a `SASL_PLAINTEXT` listener.
//!
//! `ApiVersions` is on the pre-auth allowlist and answers before any SASL
//! exchange, `Metadata` is not and the broker closes the connection instead,
//! a `SaslHandshake` for a mechanism the listener does not enable comes back
//! with the enabled list and then closes, a v0 handshake runs the exchange as
//! raw tokens, and anything but `SaslAuthenticate` after a handshake gets
//! its own `ILLEGAL_SASL_STATE` response, when Kafka can parse it, and closes.

use {
    crate::{
        harness::{
            BrokerSocket, alice_password, alice_plain_socket, round_trip, wrong_scram_password,
        },
        kafka_wire::exchange,
        support::{
            sasl::sasl_plaintext_mechanisms,
            wire::{
                CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
                SocketExchangeSetup, request_frame, write_frame,
            },
        },
    },
    assert2::{assert, check},
    bytes::BytesMut,
    krabka_broker::{Broker, BrokerHandle},
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request::ApiVersionsRequest,
            api_versions_response::ApiVersionsResponse,
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
            list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
            list_offsets_response::{
                ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
            },
            metadata_request::{MetadataRequest, MetadataRequestTopic},
            metadata_response::{MetadataResponse, MetadataResponseTopic},
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
            sasl_handshake_request::SaslHandshakeRequest,
            sasl_handshake_response::SaslHandshakeResponse,
        },
    },
    krabka_security::SaslMechanism,
    krabka_units::{ByteSize, convert::ByteSizeExt},
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::{
        fmt::Debug,
        io,
        path::Path,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    },
};

/// `ApiVersions`, `api_key` 18, is on the pre-auth allowlist and must succeed
/// without any SASL exchange.
///
/// The response should decode without an error. The supported-api list should
/// include `api_keys` 17, which is `SaslHandshake`, and 36, which is
/// `SaslAuthenticate`.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/handshake.feature",
    name = "api versions reachable pre auth on sasl listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_versions_reachable_pre_auth_on_sasl_listener(gate: SaslGate) {
    gate.broker.expect("a SASL gate broker").shutdown().await;
}

/// A pre-auth `Metadata` request on a `SASL_PLAINTEXT` listener must not
/// succeed.
///
/// `Metadata`, `api_key` 3, is not on the pre-auth allowlist. T12 closes the
/// TCP connection and does not encode a typed error response. So the read
/// after the `Metadata` request must return an I/O error, either
/// `UnexpectedEof` or a connection reset, and not a well-formed response.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/handshake.feature",
    name = "metadata rejected pre auth on sasl listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_rejected_pre_auth_on_sasl_listener(gate: SaslGate) {
    gate.broker.expect("a SASL gate broker").shutdown().await;
}

/// A `SaslHandshake` with an unsupported mechanism, GSSAPI, must return
/// `error_code = 33`, which is `UNSUPPORTED_SASL_MECHANISM`, with the enabled
/// list, and then close the connection, as Kafka's `SaslServerAuthenticator`
/// does. A new connection may then negotiate the supported mechanism, PLAIN.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/handshake.feature",
    name = "unsupported mechanism answers 33 then closes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_mechanism_answers_33_then_closes(gate: SaslGate) {
    gate.broker.expect("a SASL gate broker").shutdown().await;
}

async fn start_plain_sasl_broker(log_dir: &Path) -> krabka_broker::BrokerHandle {
    let mut cfg = sasl_plaintext_mechanisms(log_dir.to_path_buf(), vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("alice".to_owned(), alice_password());
    krabka_broker::Broker::start(cfg)
        .await
        .expect("broker must start")
}

async fn plain_handshake(stream: &mut TcpStream, version: i16) -> SaslHandshakeResponse {
    let mut body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_owned(),
        ..Default::default()
    }
    .encode(&mut body, version)
    .unwrap();
    let bytes = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(version),
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("SaslHandshake round-trip");
    let mut cur: &[u8] = &bytes;
    SaslHandshakeResponse::decode(&mut cur, version).expect("SaslHandshakeResponse must decode")
}

async fn write_raw_frame(stream: &mut TcpStream, payload: &[u8]) {
    stream
        .write_u32(u32::try_from(payload.len()).unwrap())
        .await
        .unwrap();
    stream.write_all(payload).await.unwrap();
    stream.flush().await.unwrap();
}

/// `SaslHandshake` v0: Kafka's `enableKafkaSaslAuthenticateHeaders` stays
/// off, so the client sends its PLAIN token as a raw size-prefixed frame with
/// no Kafka request header, and the broker answers with a raw (here empty)
/// size-prefixed token. The connection is then authenticated.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/handshake.feature",
    name = "v0 handshake runs the exchange as raw tokens"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v0_handshake_runs_the_exchange_as_raw_tokens(gate: SaslGate) {
    gate.broker.expect("a SASL gate broker").shutdown().await;
}

/// A v0 raw token with a bad credential has no response shape to carry an
/// error, so Kafka closes the connection without one.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/handshake.feature",
    name = "v0 raw token with bad credentials closes without response"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v0_raw_token_with_bad_credentials_closes_without_response(gate: SaslGate) {
    gate.broker.expect("a SASL gate broker").shutdown().await;
}

/// After a handshake chose a mechanism, Kafka accepts only
/// `SaslAuthenticate`: an `ApiVersions` gets its own error response with
/// `ILLEGAL_SASL_STATE` (34), and then the connection closes.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/handshake.feature",
    name = "api versions after the handshake answers 34 then closes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_versions_after_the_handshake_answers_34_then_closes(gate: SaslGate) {
    gate.broker.expect("a SASL gate broker").shutdown().await;
}

/// A PLAIN SASL broker, the log directory that backs it, and an open socket.
struct PlainHandshake {
    log_dir: tempfile::TempDir,
    handle: krabka_broker::BrokerHandle,
    stream: TcpStream,
}

async fn plain_handshake_fixture(version: i16) -> PlainHandshake {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_plain_sasl_broker(log_dir.path()).await;
    let mut stream = TcpStream::connect(handle.listen_addr()).await.unwrap();
    check!(plain_handshake(&mut stream, version).await.error_code == 0);
    PlainHandshake {
        log_dir,
        handle,
        stream,
    }
}

/// The failed-authentication delay, in milliseconds, of the broker that
/// [`a_request_after_the_handshake_answers_illegal_sasl_state_when_kafka_can_parse_it`]
/// runs.
const MID_EXCHANGE_DELAY_MS: u32 = 500;

/// Checks, for the named case, the response bytes the broker wrote.
type ResponseCheck = Box<dyn Fn(&str, &[u8])>;

/// What the broker does with a request that arrives after the handshake.
enum MidExchange {
    /// Writes the decoded response that the check accepts, after the
    /// failed-authentication delay, and closes.
    Answers(ResponseCheck),
    /// Closes at once, with no response.
    Closes,
}

/// A response that decodes at `version` to `expected`, whole.
fn answers<R>(version: i16, expected: R) -> MidExchange
where
    R: for<'de> Decode<'de> + PartialEq + Debug + 'static,
{
    MidExchange::Answers(Box::new(move |name, bytes| {
        let mut cursor = bytes;
        let response = R::decode(&mut cursor, version).expect("the response decodes");
        assert!(response == expected, "{name}");
        check!(cursor.is_empty(), "{name}: no bytes after the response");
    }))
}

/// One request sent after a v1 PLAIN handshake.
struct MidExchangeCase {
    name: &'static str,
    api_key: i16,
    version: i16,
    body: Vec<u8>,
    outcome: MidExchange,
}

/// `request` encoded at `version`.
fn encoded(request: &impl Encode, version: i16) -> Vec<u8> {
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .expect("the request encodes");
    body.to_vec()
}

/// A `Produce` for `orders` partition 0 with `acks`.
fn produce(acks: i16) -> ProduceRequest {
    ProduceRequest {
        acks,
        topic_data: vec![TopicProduceData {
            name: "orders".to_owned(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The message of the `IllegalSaslStateException` Kafka's authenticator
/// throws for a request of `api`.
fn unexpected_request(api: &str) -> String {
    format!("Unexpected Kafka request of type {api} during SASL authentication.")
}

fn mid_exchange_cases() -> Vec<MidExchangeCase> {
    let metadata = MetadataRequest {
        topics: Some(vec![MetadataRequestTopic {
            name: Some("orders".to_owned()),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let list_offsets = ListOffsetsRequest {
        topics: vec![ListOffsetsTopic {
            name: "orders".to_owned(),
            partitions: vec![ListOffsetsPartition {
                partition_index: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let create_topics = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "orders".to_owned(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        ..Default::default()
    };
    vec![
        // v12 has no top-level error code; the topic row carries it.
        MidExchangeCase {
            name: "Metadata v12",
            api_key: 3,
            version: 12,
            body: encoded(&metadata, 12),
            outcome: answers(12, MetadataResponse {
                topics: vec![MetadataResponseTopic {
                    error_code: 34,
                    name: Some("orders".to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        },
        MidExchangeCase {
            name: "Produce v9",
            api_key: 0,
            version: 9,
            body: encoded(&produce(1), 9),
            outcome: answers(9, ProduceResponse {
                responses: vec![TopicProduceResponse {
                    name: "orders".to_owned(),
                    partition_responses: vec![PartitionProduceResponse {
                        index: 0,
                        error_code: 34,
                        base_offset: -1,
                        log_append_time_ms: -1,
                        log_start_offset: -1,
                        error_message: Some(unexpected_request("PRODUCE")),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        },
        MidExchangeCase {
            name: "ListOffsets v7",
            api_key: 2,
            version: 7,
            body: encoded(&list_offsets, 7),
            outcome: answers(7, ListOffsetsResponse {
                topics: vec![ListOffsetsTopicResponse {
                    name: "orders".to_owned(),
                    partitions: vec![ListOffsetsPartitionResponse {
                        partition_index: 0,
                        error_code: 34,
                        timestamp: -1,
                        offset: -1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        },
        MidExchangeCase {
            name: "CreateTopics v7",
            api_key: 19,
            version: 7,
            body: encoded(&create_topics, 7),
            outcome: answers(7, CreateTopicsResponse {
                topics: vec![CreatableTopicResult {
                    name: "orders".to_owned(),
                    error_code: 34,
                    error_message: Some(unexpected_request("CREATE_TOPICS")),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        },
        // `RequestContext.parseRequest` reads an unsupported `ApiVersions`
        // as an empty v0 request.
        MidExchangeCase {
            name: "ApiVersions at an unsupported version",
            api_key: 18,
            version: i16::MAX,
            body: vec![0xff],
            outcome: answers(0, ApiVersionsResponse {
                error_code: 34,
                ..Default::default()
            }),
        },
        // `ProduceRequest.getErrorResponse` returns null for `acks = 0`.
        MidExchangeCase {
            name: "Produce with acks 0",
            api_key: 0,
            version: 9,
            body: encoded(&produce(0), 9),
            outcome: MidExchange::Closes,
        },
        MidExchangeCase {
            name: "Metadata at an unsupported version",
            api_key: 3,
            version: i16::MAX,
            body: encoded(&MetadataRequest::default(), 13),
            outcome: MidExchange::Closes,
        },
        MidExchangeCase {
            name: "Metadata with a truncated body",
            api_key: 3,
            version: 12,
            body: vec![0xff],
            outcome: MidExchange::Closes,
        },
        // `UnregisterController` (94) is past Kafka 4.3.1's last api key.
        MidExchangeCase {
            name: "an api key Kafka 4.3.1 lacks",
            api_key: 94,
            version: 0,
            body: vec![0, 0, 0, 0, 0],
            outcome: MidExchange::Closes,
        },
    ]
}

/// After a handshake chose a mechanism, Kafka's
/// `SaslServerAuthenticator.handleSaslToken` parses any request but
/// `SaslAuthenticate` and answers it with that request's own
/// `getErrorResponse` for `IllegalSaslStateException`: `ILLEGAL_SASL_STATE`
/// (34) in the request's own response shape and version. The answer and the
/// close wait out the failed-authentication delay. A request it cannot parse,
/// or whose error response is null, closes at once with no answer.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/handshake.feature",
    name = "a request after the handshake answers illegal sasl state when kafka can parse it"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_after_the_handshake_answers_illegal_sasl_state_when_kafka_can_parse_it(
    gate: SaslGate,
    request: String,
    expected: String,
) {
    let _ = (request, expected);
    gate.broker.expect("a SASL gate broker").shutdown().await;
}

struct SaslGate {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    stream: Option<TcpStream>,
    versions: Option<ApiVersionsResponse>,
    handshake: Option<io::Result<SaslHandshakeResponse>>,
    read_result: Option<io::Result<u32>>,
    metadata: Option<io::Result<Vec<u8>>>,
    mid_case: Option<MidExchangeCase>,
    mid_response: Option<io::Result<Vec<u8>>>,
    elapsed: Option<Duration>,
}

#[fixture]
fn gate() -> SaslGate {
    // Start this scenario with fresh inputs and observations.

    SaslGate {
        broker: None,
        directory: None,
        stream: None,
        versions: None,
        handshake: None,
        read_result: None,
        metadata: None,
        mid_case: None,
        mid_response: None,
        elapsed: None,
    }
}

#[given("an unauthenticated connection to a PLAIN SASL listener")]
async fn unauthenticated_socket(gate: &mut SaslGate) {
    let BrokerSocket {
        log_dir,
        handle,
        stream,
    } = alice_plain_socket(alice_password()).await;
    gate.broker = Some(handle);
    gate.directory = Some(log_dir);
    gate.stream = Some(stream);
}

#[given("a PLAIN connection after SaslHandshake version {version}")]
async fn selected_plain(gate: &mut SaslGate, version: i16) {
    let PlainHandshake {
        log_dir,
        handle,
        stream,
    } = plain_handshake_fixture(version).await;
    gate.broker = Some(handle);
    gate.directory = Some(log_dir);
    gate.stream = Some(stream);
}

#[given("the PLAIN listener delays failed authentication by 500 milliseconds")]
async fn delayed_failure_listener(gate: &mut SaslGate) {
    let log_dir = tempfile::tempdir().unwrap();
    let mut config =
        sasl_plaintext_mechanisms(log_dir.path().to_path_buf(), vec![SaslMechanism::Plain]);
    config
        .plain_credentials
        .insert("alice".to_owned(), alice_password());
    config.connection_failed_authentication_delay = krabka_units::millis(MID_EXCHANGE_DELAY_MS);
    gate.broker = Some(Broker::start(config).await.expect("broker must start"));
    gate.directory = Some(log_dir);
}

#[given("the client has selected PLAIN with SaslHandshake v1")]
async fn handshake_selected(gate: &mut SaslGate) {
    let address = gate
        .broker
        .as_ref()
        .expect("a delayed SASL listener")
        .listen_addr();
    let mut stream = TcpStream::connect(address).await.unwrap();
    check!(plain_handshake(&mut stream, 1).await.error_code == 0);
    gate.stream = Some(stream);
}

#[when("ApiVersions is requested before authentication")]
async fn preauth_api_versions(gate: &mut SaslGate) {
    gate.versions = Some(
        exchange()
            .stream(gate.stream.as_mut().expect("an unauthenticated socket"))
            .request(&ApiVersionsRequest::default())
            .client_id("krabka-sasl-test")
            .call()
            .await
            .expect("ApiVersions round-trip"),
    );
}

#[then("ApiVersions succeeds and advertises SaslHandshake and SaslAuthenticate")]
fn preauth_api_surface(gate: &SaslGate) {
    let response = gate.versions.as_ref().expect("ApiVersions was requested");
    check!(
        response.error_code == 0,
        "ApiVersions error_code must be 0 on SASL listener pre-auth"
    );
    check!(
        response.api_keys.iter().any(|key| key.api_key == 17),
        "ApiVersionsResponse must list SaslHandshake (17): {:?}",
        response.api_keys
    );
    check!(
        response.api_keys.iter().any(|key| key.api_key == 36),
        "ApiVersionsResponse must list SaslAuthenticate (36): {:?}",
        response.api_keys
    );
}

#[when("Metadata version 12 is sent before authentication")]
async fn preauth_metadata(gate: &mut SaslGate) {
    let mut body = BytesMut::new();
    MetadataRequest::default().encode(&mut body, 12).unwrap();
    let frame = request_frame()
        .api_key(3)
        .version(12)
        .header(RequestHeaderEncoding::Flexible)
        .client_id("krabka-t19-test")
        .body(&body)
        .maybe_capacity(Some(<ByteSize as ByteSizeExt>::from_bytes(
            u64::try_from(32 + body.len()).expect("frame capacity fits u64"),
        )))
        .call();
    write_frame(
        gate.stream.as_mut().expect("an unauthenticated socket"),
        &frame,
        None,
    )
    .await
    .unwrap();
    read_sasl_connection(gate).await;
}

#[when("the SASL client reads the connection")]
async fn read_sasl_connection(gate: &mut SaslGate) {
    gate.read_result = Some(
        gate.stream
            .as_mut()
            .expect("a SASL socket")
            .read_u32()
            .await,
    );
}

#[then("the SASL connection closes without a next response frame")]
fn no_sasl_frame(gate: &SaslGate) {
    let result = gate.read_result.as_ref().expect("the SASL socket was read");
    assert!(
        result.is_err(),
        "expected TCP close without a response frame, got {result:?}"
    );
}

async fn send_handshake(
    gate: &mut SaslGate,
    mechanism: &str,
    correlation: i32,
) -> io::Result<SaslHandshakeResponse> {
    exchange()
        .stream(gate.stream.as_mut().expect("a SASL socket"))
        .request(&SaslHandshakeRequest {
            mechanism: mechanism.to_owned(),
            ..Default::default()
        })
        .api_key(17)
        .version(1)
        .correlation(correlation)
        .client_id("krabka-sasl-test")
        .call()
        .await
}

#[when("the client selects unsupported mechanism GSSAPI")]
async fn unsupported_handshake(gate: &mut SaslGate) {
    gate.handshake = Some(send_handshake(gate, "GSSAPI", 1).await);
}

#[then("SaslHandshake reports UNSUPPORTED_SASL_MECHANISM and lists PLAIN")]
fn unsupported_sasl_mechanism(gate: &SaslGate) {
    let response = gate
        .handshake
        .as_ref()
        .expect("GSSAPI was selected")
        .as_ref()
        .expect("SaslHandshake(GSSAPI) must get a response, not a TCP close");
    assert!(
        response.error_code == 33,
        "GSSAPI handshake must return error_code=33, got {:?}",
        response.error_code
    );
    assert!(
        response
            .mechanisms
            .iter()
            .any(|mechanism| mechanism == "PLAIN"),
        "error response must include enabled mechanisms: {:?}",
        response.mechanisms
    );
}

#[when("the client retries PLAIN on the refused connection")]
async fn retry_refused_handshake(gate: &mut SaslGate) {
    gate.handshake = Some(send_handshake(gate, "PLAIN", 2).await);
}

#[then("the refused connection cannot answer another handshake")]
fn refused_handshake_closed(gate: &SaslGate) {
    let response = gate.handshake.as_ref().expect("PLAIN was retried");
    assert!(
        response.is_err(),
        "the connection must be closed after UNSUPPORTED_SASL_MECHANISM"
    );
}

#[when("the client selects PLAIN on a fresh connection")]
async fn fresh_plain_handshake(gate: &mut SaslGate) {
    let addr = gate
        .broker
        .as_ref()
        .expect("the SASL listener")
        .listen_addr();
    gate.stream = Some(TcpStream::connect(addr).await.unwrap());
    gate.handshake = Some(send_handshake(gate, "PLAIN", 3).await);
}

#[then("the fresh PLAIN handshake succeeds")]
fn accepted_plain_handshake(gate: &SaslGate) {
    let response = gate
        .handshake
        .as_ref()
        .expect("PLAIN was selected on a fresh socket")
        .as_ref()
        .expect("SaslHandshake(PLAIN) on a new connection");
    assert!(response.error_code == 0);
}

#[when("a raw PLAIN token with {credentials} credentials is sent")]
async fn raw_plain_token(gate: &mut SaslGate, credentials: String) {
    let password = match credentials.as_str() {
        "valid" => alice_password(),
        "invalid" => wrong_scram_password(),
        other => panic!("unknown PLAIN token fixture {other}"),
    };
    let mut token = vec![0_u8];
    token.extend_from_slice(b"alice");
    token.push(0);
    token.extend_from_slice(password.as_bytes());
    write_raw_frame(gate.stream.as_mut().expect("a v0 PLAIN handshake"), &token).await;
    read_sasl_connection(gate).await;
}

#[then("the raw PLAIN server token is empty")]
fn empty_server_token(gate: &SaslGate) {
    check!(
        matches!(gate.read_result.as_ref(), Some(Ok(0))),
        "PLAIN's server token is empty"
    );
}

#[when("the raw-token client requests Metadata after authentication")]
async fn raw_token_metadata(gate: &mut SaslGate) {
    let mut body = BytesMut::new();
    MetadataRequest::default().encode(&mut body, 12).unwrap();
    gate.metadata = Some(
        round_trip(
            gate.stream
                .as_mut()
                .expect("a raw-token authenticated socket"),
            SocketExchangeSetup {
                api_key: WireApiKey(3),
                version: WireApiVersion(12),
                correlation: WireCorrelationId(2),
                header: RequestHeaderEncoding::Flexible,
            },
            &body,
        )
        .await,
    );
}

#[then("the raw-token authenticated client receives Metadata")]
fn raw_token_can_use_data_plane(gate: &SaslGate) {
    check!(
        gate.metadata
            .as_ref()
            .expect("Metadata was requested after raw-token authentication")
            .is_ok()
    );
}

#[when("ApiVersions version 3 arrives after the SASL handshake")]
async fn api_versions_mid_exchange(gate: &mut SaslGate) {
    let mut body = BytesMut::new();
    ApiVersionsRequest::default().encode(&mut body, 3).unwrap();
    let response = round_trip(
        gate.stream.as_mut().expect("a v1 PLAIN handshake"),
        SocketExchangeSetup {
            version: WireApiVersion(3),
            correlation: WireCorrelationId(2),
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("ApiVersions gets an error response");
    let mut cursor: &[u8] = &response;
    gate.versions =
        Some(ApiVersionsResponse::decode(&mut cursor, 3).expect("ApiVersionsResponse must decode"));
}

#[then("ApiVersions returns only ILLEGAL_SASL_STATE")]
fn illegal_api_versions(gate: &SaslGate) {
    assert!(
        gate.versions.as_ref()
            == Some(&ApiVersionsResponse {
                error_code: 34,
                ..Default::default()
            })
    );
}

#[when("the client sends {request} instead of SaslAuthenticate")]
async fn non_authentication_request(gate: &mut SaslGate, request: String) {
    let case = mid_exchange_cases()
        .into_iter()
        .find(|case| case.name == request)
        .unwrap_or_else(|| panic!("unknown mid-exchange request {request}"));
    let started = Instant::now();
    gate.mid_response = Some(
        round_trip(
            gate.stream.as_mut().expect("a v1 PLAIN handshake"),
            SocketExchangeSetup {
                api_key: WireApiKey(case.api_key),
                version: WireApiVersion(case.version),
                correlation: WireCorrelationId(2),
                header: RequestHeaderEncoding::Flexible,
            },
            &case.body,
        )
        .await,
    );
    gate.elapsed = Some(started.elapsed());
    gate.mid_case = Some(case);
}

#[then("the SASL gate applies {expected}")]
async fn mid_exchange_outcome(gate: &mut SaslGate, expected: String) {
    let case = gate
        .mid_case
        .as_ref()
        .expect("a non-authentication request was sent");
    let response = gate
        .mid_response
        .as_ref()
        .expect("the request was exchanged");
    let elapsed = gate.elapsed.expect("the response time was observed");
    let delay = Duration::from_millis(u64::from(MID_EXCHANGE_DELAY_MS));
    match (expected.as_str(), &case.outcome) {
        ("delayed-ILLEGAL_SASL_STATE", MidExchange::Answers(check_response)) => {
            let response = response
                .as_ref()
                .unwrap_or_else(|error| panic!("{}: expected a response, got {error}", case.name));
            check_response(case.name, response);
            check!(
                elapsed >= delay,
                "{}: answered after {elapsed:?}, before the delay",
                case.name
            );
            check!(
                gate.stream
                    .as_mut()
                    .expect("the selected SASL socket")
                    .read_u32()
                    .await
                    .is_err(),
                "{}: then the connection closes",
                case.name
            );
        }
        ("immediate-close", MidExchange::Closes) => {
            check!(response.is_err(), "{}: expected no response", case.name);
            check!(
                elapsed < delay,
                "{}: closed after {elapsed:?}, not at once",
                case.name
            );
        }
        _ => panic!(
            "the feature's expected outcome disagrees with the request fixture for {}",
            case.name
        ),
    }
}
