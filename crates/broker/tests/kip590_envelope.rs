//! KIP-590 `Envelope` (`api_key` 58) on the controller listener.
//!
//! A `KRaft` broker that is not the active controller does not answer an admin
//! write itself: it wraps the client's request bytes in an `Envelope` and
//! sends that to the controller, which unwraps it, runs the request under the
//! *client's* principal, and sends the client's own response bytes back inside
//! the envelope. These tests drive that path over a real socket, with frames
//! built by hand, so the request-header, response-header and body shapes are
//! all exercised rather than assumed.
//!
//! Every constant here comes from `mirror.gcr.io/apache/kafka:4.3.1`'s own
//! `kafka-clients-4.3.1.jar`: `ApiKeys.ENVELOPE` is id 58 at versions `0..=0`,
//! flexible from 0, with `messageType.listeners() == [CONTROLLER]`, and
//! `DefaultKafkaPrincipalBuilder.serialize(new KafkaPrincipal("User",
//! "alice"))` is the byte string [`JVM_USER_ALICE`] holds.
#![cfg(test)]

use {
    crate::support::{
        acl::cluster_acl_record,
        controller_listener::*,
        start_with_bound_listeners,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    krabka_broker::api_catalog::UnstableApiVersions,
    krabka_protocol::owned::{
        allocate_producer_ids_request::{self, AllocateProducerIdsRequest},
        allocate_producer_ids_response::{self, AllocateProducerIdsResponse},
        api_versions_response::ApiVersionsResponse,
        create_delegation_token_request::{self, CreateDelegationTokenRequest},
        create_delegation_token_response::{self, CreateDelegationTokenResponse},
        create_topics_request,
        create_topics_response::{self, CreatableTopicResult, CreateTopicsResponse},
        envelope_request::{self, EnvelopeRequest},
        envelope_response::EnvelopeResponse,
        produce_request::{self, ProduceRequest},
        sasl_authenticate_request::{self, SaslAuthenticateRequest},
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::{self, SaslHandshakeRequest},
        sasl_handshake_response::SaslHandshakeResponse,
    },
    krabka_security::{ListenerProtocol, SaslMechanism, SecretBytes},
    std::{collections::HashSet, iter, net::SocketAddr, str, sync::Arc},
    tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpStream,
    },
};

mod support;

/// `DefaultKafkaPrincipalBuilder.serialize(new KafkaPrincipal("User",
/// "alice"))` from the pinned image: int16 schema version 0, compact string
/// `"User"`, compact string `"alice"`, `token_authenticated = false`, empty
/// tagged fields. A forwarding JVM broker puts exactly these bytes in
/// `EnvelopeRequest.request_principal`.
const JVM_USER_ALICE: &[u8] = krabka_macros::jvm_principal_golden!(alice, false);

/// The same serialization for `new KafkaPrincipal("User", "bob")`. It differs
/// from [`JVM_USER_ALICE`] only in the compact string and its length prefix;
/// the `token_authenticated` byte is `0x00` in both.
const JVM_USER_BOB: &[u8] = krabka_macros::jvm_principal_golden!(bob, false);

/// `JVM_USER_ALICE` with its `token_authenticated` byte set, which is what
/// `DefaultKafkaPrincipalBuilder.serialize` writes for a client that
/// authenticated with a delegation token. The two constants differ in that one
/// byte and nothing else, so a test that sends both isolates the flag.
const JVM_USER_ALICE_VIA_TOKEN: &[u8] = krabka_macros::jvm_principal_golden!(alice, true);

/// The address a forwarding broker copies out of its own client's connection
/// into `client_host_address`. `EnvelopeRequest.Builder` is handed
/// `clientAddress.getAddress()`, so a v4 client is these four raw octets and
/// no port.
const LOOPBACK_CLIENT: &[u8] = &[127, 0, 0, 1];

/// The correlation id the "client" put in the embedded request header. The
/// controller must echo this one, not the envelope's own, or the forwarding
/// hop cannot match the relayed bytes to the client that is waiting.
const EMBEDDED_CORRELATION_ID: i32 = 0x5A5A_1234;

/// A running broker and the directory that backs it.
struct EnvelopeBroker {
    broker: BrokerHandle,
    dir: tempfile::TempDir,
}

async fn start_broker() -> EnvelopeBroker { start_broker_with(|_| {}).await }

async fn start_broker_with(customize: impl FnOnce(&mut BrokerConfig)) -> EnvelopeBroker {
    support::init_tracing();
    let (broker, dir) = start_with_bound_listeners(customize).await;
    EnvelopeBroker { broker, dir }
}

/// Let the plaintext forwarding peer pass the outer gate, preserving inner-principal authorization.
async fn start_forwarder() -> EnvelopeBroker {
    start_broker_with(|config| {
        config.authorizer = Arc::new(SimpleAclAuthorizer::new(
            iter::once("ANONYMOUS".to_owned()).collect(),
        ));
    })
    .await
}

krabka_macros::request_frame_fixture!(request_payload);

/// A Kafka request with its length prefix, nullable client ID and supplied tagged bytes.
fn request_frame(setup: RequestFrameSetup<'_>) -> Bytes {
    let frame = request_payload()
        .api_key(setup.api_key.0)
        .api_version(setup.api_version.0)
        .correlation_id(setup.correlation_id.0)
        .maybe_client_id(setup.client_id)
        .maybe_tagged(setup.tagged)
        .body(setup.body)
        .call();
    let mut out = BytesMut::with_capacity(4 + frame.len());
    out.put_i32(i32::try_from(frame.len()).expect("frame length"));
    out.put_slice(&frame);
    out.freeze()
}

fn embedded_request_frame(api_key: i16, api_version: i16, body: &[u8]) -> Bytes {
    request_frame(RequestFrameSetup {
        api_key: krabka_ids::ApiKey(api_key),
        api_version: krabka_ids::ApiVersion(api_version),
        correlation_id: FrameCorrelationId(EMBEDDED_CORRELATION_ID),
        client_id: Some(b"adminclient-1"),
        tagged: Some(&[0]),
        body,
    })
    .slice(4..)
}

fn encode<T: Encode>(message: &T, version: i16) -> Bytes {
    let mut buf = BytesMut::new();
    message.encode(&mut buf, version).expect("encode");
    buf.freeze()
}

/// Send one request frame and return the response frame with its length
/// prefix stripped, so the caller sees `correlation_id` first.
async fn round_trip(addr: SocketAddr, frame: Bytes) -> Bytes {
    let mut stream = TcpStream::connect(addr).await.expect("connect listener");
    exchange(&mut stream, frame).await
}

/// The same exchange on a connection the caller already holds, for the tests
/// that send more than one request down one authenticated connection.
async fn exchange(stream: &mut TcpStream, frame: Bytes) -> Bytes {
    stream.write_all(&frame).await.expect("write request");
    stream.flush().await.expect("flush request");

    let mut length = [0u8; 4];
    stream
        .read_exact(&mut length)
        .await
        .expect("read response length");
    let length = usize::try_from(i32::from_be_bytes(length)).expect("non-negative length");
    let mut body = vec![0u8; length];
    stream
        .read_exact(&mut body)
        .await
        .expect("read response body");
    Bytes::from(body)
}

/// Send an `Envelope` at v0 to the controller listener on a fresh connection
/// and decode the `EnvelopeResponse` out of the reply.
async fn send_envelope(addr: SocketAddr, envelope: &EnvelopeRequest) -> EnvelopeResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect listener");
    send_envelope_on(&mut stream, envelope).await
}

/// The same exchange on a connection the caller already opened and, where the
/// listener demands it, already authenticated.
///
/// The response header is v1 because the `EnvelopeResponse` body is flexible,
/// so four bytes of correlation id are followed by one tagged-fields byte
/// before the body starts. Both are checked here rather than skipped, since a
/// missing or extra byte would shift the whole body.
async fn send_envelope_on(stream: &mut TcpStream, envelope: &EnvelopeRequest) -> EnvelopeResponse {
    const ENVELOPE_CORRELATION_ID: i32 = 99;

    let frame = request_frame(RequestFrameSetup {
        api_key: krabka_ids::ApiKey(envelope_request::API_KEY),
        correlation_id: FrameCorrelationId(ENVELOPE_CORRELATION_ID),
        client_id: Some(b"forwarding-broker"),
        tagged: Some(&[0]),
        body: &encode(envelope, 0),
        ..Default::default()
    });
    let response = exchange(stream, frame).await;

    let mut cur = response.as_ref();
    assert!(let Some((header, rest)) = cur.split_at_checked(5));
    check!(
        header == [0, 0, 0, 99, 0],
        "response header v1 for Envelope"
    );
    cur = rest;
    let decoded = EnvelopeResponse::decode(&mut cur, 0).expect("decode EnvelopeResponse");
    check!(cur.is_empty(), "EnvelopeResponse consumed the whole body");
    decoded
}

/// Build the `request_data` a forwarding broker would copy out of the client's
/// connection: the client's own request header followed by its body.
fn embedded_create_topics(topic: &str) -> Bytes {
    let version = create_topics_request::MAX_VERSION;
    let body = encode(
        &create_topic_request(creatable_topic().name(topic.into()).call()),
        version,
    );
    // The length prefix belongs to the outer connection, not to `request_data`
    // — KIP-590 wraps the header and body only.
    let setup = RequestFrameSetup {
        body: &body,
        client_id: Some(b"adminclient-1"),
        api_key: krabka_ids::ApiKey(create_topics_request::API_KEY),
        api_version: krabka_ids::ApiVersion(version),
        correlation_id: FrameCorrelationId(EMBEDDED_CORRELATION_ID),
        tagged: Some(&[0]),
    };
    let framed = request_frame(setup);
    framed.slice(4..)
}

fn envelope_from(
    request_data: Bytes,
    principal: Option<&'static [u8]>,
    client_host: &[u8],
) -> EnvelopeRequest {
    EnvelopeRequest {
        request_data,
        request_principal: principal.map(Bytes::from_static),
        client_host_address: Bytes::copy_from_slice(client_host),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

// The expected bytes and whole refusal tuples remain explicit at each scenario.
fn served_embedded_body<'a>(
    response: &'a EnvelopeResponse,
    expected: &[u8],
    context: &str,
) -> &'a [u8] {
    check!(response.error_code == 0, "{context}");
    assert!(let Some(response_data) = &response.response_data);
    assert!(let Some((header, body)) = response_data.split_at_checked(5));
    check!(header == expected, "{context}");
    body
}

fn check_refused(broker: &BrokerHandle, topic: &str, response: &EnvelopeResponse, code: i16) {
    check!(response.error_code == code && response.response_data.is_none());
    check!(
        broker.controller_image_for_test().topic(topic).is_none(),
        "the embedded request must not have run"
    );
}

async fn advertised_api_versions(addr: SocketAddr) -> ApiVersionsResponse {
    let frame = request_frame(RequestFrameSetup {
        client_id: Some(b"probe"),
        ..Default::default()
    });
    let response = round_trip(addr, frame).await;

    let mut cur = response.as_ref();
    assert!(let Some((header, rest)) = cur.split_at_checked(4));
    check!(header == [0, 0, 0, 1], "response header v0 for ApiVersions");
    cur = rest;
    ApiVersionsResponse::decode(&mut cur, 0).expect("decode ApiVersionsResponse")
}

/// Decode the embedded `CreateTopicsResponse` out of a served `Envelope`.
///
/// `CreateTopics` is flexible at its maximum version, so the embedded
/// response header is v1: four bytes of the client's correlation id and one
/// empty tagged-fields byte before the body.
fn embedded_create_topics_response(response: &EnvelopeResponse) -> CreateTopicsResponse {
    decode_embedded(
        response,
        create_topics_response::MAX_VERSION,
        "decode the embedded CreateTopicsResponse",
    )
}

/// A topic and the error code its `CreateTopics` row carried.
#[derive(Debug, PartialEq)]
struct TopicOutcome {
    topic: String,
    error_code: i16,
}

/// The `request_data` for an embedded `CreateDelegationToken` that names no
/// owner, so KIP-48 owner resolution falls back to whoever the envelope says
/// is calling.
fn embedded_create_delegation_token() -> Bytes {
    let version = create_delegation_token_request::MAX_VERSION;
    let body = encode(
        &CreateDelegationTokenRequest {
            owner_principal_type: None,
            owner_principal_name: None,
            renewers: Vec::new(),
            // The `defer to the broker ceiling` sentinel.
            max_lifetime_ms: -1,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
        version,
    );
    embedded_request_frame(create_delegation_token_request::API_KEY, version, &body)
}

/// Decode the embedded `CreateDelegationTokenResponse` out of a served
/// `Envelope`. Flexible at its maximum version, so the embedded response
/// header is v1, exactly as `embedded_create_topics_response` reads it.
fn embedded_create_delegation_token_response(
    response: &EnvelopeResponse,
) -> CreateDelegationTokenResponse {
    decode_embedded(
        response,
        create_delegation_token_response::MAX_VERSION,
        "decode the embedded CreateDelegationTokenResponse",
    )
}

fn decode_embedded<R: for<'de> krabka_protocol::Decode<'de>>(
    response: &EnvelopeResponse,
    version: i16,
    context: &str,
) -> R {
    check!(response.error_code == 0, "the Envelope itself was served");
    let data = response
        .response_data
        .as_ref()
        .expect("a served Envelope carries response_data");
    let mut body = data.get(5..).expect("embedded response header v1");
    let decoded = R::decode(&mut body, version).expect(context);
    check!(
        body.is_empty(),
        "the embedded response consumed its own bytes"
    );
    decoded
}

/// Authenticate a controller-listener connection with SASL/PLAIN, the way a
/// forwarding broker's inter-broker client does: `SaslHandshake` v1 to pick
/// the mechanism, then one `SaslAuthenticate` v2 carrying the RFC 4616
/// `authzid\0authcid\0passwd` triple.
///
/// `BrokerRaftHandshake` consumes both before the controller server sees the
/// stream, which is why neither appears in the listener's `ApiVersions`
/// table. v1 is not flexible and v2 is, so their reply headers differ by the
/// one tagged-fields byte checked below.
async fn sasl_plain(stream: &mut TcpStream, user: &str, password: &str) {
    let handshake = exchange(
        stream,
        request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(sasl_handshake_request::API_KEY),
            api_version: krabka_ids::ApiVersion(1),
            client_id: Some(b"forwarding-broker"),
            body: &encode(
                &SaslHandshakeRequest {
                    mechanism: "PLAIN".to_owned(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                1,
            ),
            ..Default::default()
        }),
    )
    .await;
    let mut cur = handshake.as_ref();
    assert!(let Some((header, rest)) = cur.split_at_checked(4));
    check!(header == [0, 0, 0, 1], "SaslHandshake v1 reply header v0");
    cur = rest;
    let decoded = SaslHandshakeResponse::decode(&mut cur, 1).expect("decode SaslHandshakeResponse");
    check!(cur.is_empty(), "SaslHandshakeResponse consumed its body");
    check!(
        decoded
            == SaslHandshakeResponse {
                error_code: 0,
                mechanisms: vec!["PLAIN".to_owned()],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );

    let mut auth_bytes = Vec::new();
    auth_bytes.push(0);
    auth_bytes.extend_from_slice(user.as_bytes());
    auth_bytes.push(0);
    auth_bytes.extend_from_slice(password.as_bytes());
    let authenticate = exchange(
        stream,
        request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(sasl_authenticate_request::API_KEY),
            api_version: krabka_ids::ApiVersion(2),
            correlation_id: FrameCorrelationId(2),
            client_id: Some(b"forwarding-broker"),
            tagged: Some(&[0]),
            body: &encode(
                &SaslAuthenticateRequest {
                    auth_bytes: Bytes::from(auth_bytes),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                2,
            ),
        }),
    )
    .await;
    let mut cur = authenticate.as_ref();
    assert!(let Some((header, rest)) = cur.split_at_checked(5));
    check!(
        header == [0, 0, 0, 2, 0],
        "SaslAuthenticate v2 reply header v1: correlation id then tagged fields"
    );
    cur = rest;
    let decoded =
        SaslAuthenticateResponse::decode(&mut cur, 2).expect("decode SaslAuthenticateResponse");
    check!(cur.is_empty(), "SaslAuthenticateResponse consumed its body");
    check!((decoded.error_code, decoded.error_message) == (0, None));
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};
struct Forwarding {
    server: Option<EnvelopeBroker>,
    stream: Option<TcpStream>,
    principal: Option<&'static [u8]>,
    host: Vec<u8>,
    unstable: Option<UnstableApiVersions>,
    responses: Vec<(String, EnvelopeResponse)>,
    versions: Option<(ApiVersionsResponse, ApiVersionsResponse)>,
}
#[fixture]
fn forwarding() -> Forwarding {
    // Start this scenario with fresh inputs and observations.

    Forwarding {
        server: None,
        stream: None,
        principal: None,
        host: LOOPBACK_CLIENT.to_vec(),
        unstable: None,
        responses: Vec::new(),
        versions: None,
    }
}
fn principal_bytes(name: &str, token: bool) -> &'static [u8] {
    match (name, token) {
        ("alice", false) => JVM_USER_ALICE,
        ("bob", false) => JVM_USER_BOB,
        ("alice", true) => JVM_USER_ALICE_VIA_TOKEN,
        _ => panic!("no serialized fixture principal User:{name} token={token}"),
    }
}
fn octets(text: &str) -> Vec<u8> {
    text.split(',')
        .map(|octet| octet.parse().expect("address octet"))
        .collect()
}
fn hex_bytes(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "whole hexadecimal bytes");
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            u8::from_str_radix(str::from_utf8(pair).expect("hex text"), 16).expect("hex byte")
        })
        .collect()
}
#[given("the controller uses the compatibility authorizer on its plaintext listener")]
async fn compatible(forwarding: &mut Forwarding) {
    forwarding.server = Some(if let Some(unstable) = forwarding.unstable {
        start_broker_with(|config| config.features.unstable_api_versions = unstable).await
    } else {
        start_broker().await
    });
}
#[given(
    "the plaintext forwarding peer ANONYMOUS is a superuser while embedded principals use ACLs"
)]
async fn authorized_forwarder(forwarding: &mut Forwarding) {
    forwarding.server = Some(start_forwarder().await);
}
#[given("the controller ACL authorizer has no superusers or forwarding peer grants")]
async fn unauthorized_forwarder(forwarding: &mut Forwarding) {
    forwarding.server = Some(
        start_broker_with(|config| {
            config.super_users = HashSet::new();
            config.authorizer = Arc::new(SimpleAclAuthorizer::new(config.super_users.clone()));
        })
        .await,
    );
}
#[given("unstable API versions are enabled {enabled}")]
fn unstable(forwarding: &mut Forwarding, enabled: bool) {
    forwarding.unstable = Some(if enabled {
        UnstableApiVersions::Enabled
    } else {
        UnstableApiVersions::Disabled
    });
}
#[given("the forwarded User principal is {name} with delegation token flag {token}")]
fn principal(forwarding: &mut Forwarding, name: String, token: bool) {
    forwarding.principal = Some(principal_bytes(&name, token));
}
#[given("the forwarded request has no serialized principal")]
fn no_principal(forwarding: &mut Forwarding) { forwarding.principal = None; }
#[given("the forwarded client address contains octets {address}")]
fn client_host(forwarding: &mut Forwarding, address: String) { forwarding.host = octets(&address); }
#[given("cluster grants authorize these principal host and operation combinations")]
async fn grants(forwarding: &mut Forwarding, datatable: Vec<Vec<String>>) {
    let broker = &forwarding
        .server
        .as_ref()
        .expect("controller started")
        .broker;
    for row in datatable.into_iter().skip(1) {
        let [principal, host, operation]: [String; 3] =
            row.try_into().expect("principal host operation");
        let operation = match operation.as_str() {
            "Create" => AclOperation::Create,
            "ClusterAction" => AclOperation::ClusterAction,
            _ => panic!("unsupported grant operation {operation}"),
        };
        broker
            .submit_metadata_record_for_test(
                cluster_acl_record()
                    .principal(&principal)
                    .host(&host)
                    .operation(operation)
                    .call(),
            )
            .await
            .expect("seed cluster ACL");
    }
}
#[given("one controller TCP connection is reused for every forwarded request")]
async fn persistent_connection(forwarding: &mut Forwarding) {
    forwarding.stream = Some(
        TcpStream::connect(
            forwarding
                .server
                .as_ref()
                .expect("controller started")
                .broker
                .controller_addr(),
        )
        .await
        .expect("controller connection"),
    );
}
#[given(
    "a SASL PLAIN forwarding connection authenticates user {user} password {password} with delegation master key {secret}"
)]
async fn authenticated_connection(
    forwarding: &mut Forwarding,
    user: String,
    password: String,
    secret: String,
) {
    let server = start_broker_with(|config| {
        config.controller_listener_protocol = ListenerProtocol::SaslPlaintext;
        config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        config
            .plain_credentials
            .insert(user.clone(), password.clone());
        config.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
            username: user.clone(),
            password: password.clone(),
        });
        config.delegation_token_secret_key = Some(SecretBytes::new(secret.as_bytes().to_vec()));
    })
    .await;
    let mut stream = TcpStream::connect(server.broker.controller_addr())
        .await
        .expect("SASL controller connection");
    sasl_plain(&mut stream, &user, &password).await;
    forwarding.server = Some(server);
    forwarding.stream = Some(stream);
}
async fn forward(forwarding: &mut Forwarding, label: String, request_data: Bytes) {
    let envelope = envelope_from(request_data, forwarding.principal, &forwarding.host);
    let response = if let Some(stream) = &mut forwarding.stream {
        send_envelope_on(stream, &envelope).await
    } else {
        send_envelope(
            forwarding
                .server
                .as_ref()
                .expect("controller started")
                .broker
                .controller_addr(),
            &envelope,
        )
        .await
    };
    forwarding.responses.push((label, response));
}
fn latest(forwarding: &Forwarding) -> &EnvelopeResponse {
    &forwarding
        .responses
        .last()
        .expect("Envelope request completed")
        .1
}
#[when("CreateTopics for topic {topic} is forwarded in an envelope")]
async fn create(forwarding: &mut Forwarding, topic: String) {
    forward(forwarding, topic.clone(), embedded_create_topics(&topic)).await;
}
#[when("a default Produce request is forwarded despite not being a forwardable API")]
async fn forbidden_produce(forwarding: &mut Forwarding) {
    let data = request_frame(RequestFrameSetup {
        api_key: krabka_ids::ApiKey(produce_request::API_KEY),
        api_version: krabka_ids::ApiVersion(produce_request::MAX_VERSION),
        correlation_id: FrameCorrelationId(7),
        client_id: Some(b"adminclient-1"),
        tagged: Some(&[0]),
        body: &encode(&ProduceRequest::default(), produce_request::MAX_VERSION),
    })
    .slice(4..);
    forward(forwarding, "Produce".into(), data).await;
}
#[when("the embedded raw request header bytes hex {hex} are forwarded")]
async fn raw_header(forwarding: &mut Forwarding, hex: String) {
    forward(
        forwarding,
        "raw header".into(),
        Bytes::from(hex_bytes(&hex)),
    )
    .await;
}
#[when("CreateTopics for topic {topic} advertises one version above the latest supported version")]
async fn unsupported_version(forwarding: &mut Forwarding, topic: String) {
    let data = request_frame(RequestFrameSetup {
        api_key: krabka_ids::ApiKey(create_topics_request::API_KEY),
        api_version: krabka_ids::ApiVersion(create_topics_request::MAX_VERSION + 1),
        correlation_id: FrameCorrelationId(EMBEDDED_CORRELATION_ID),
        client_id: Some(b"adminclient-1"),
        tagged: Some(&[0]),
        body: &encode(
            &create_topic_request(creatable_topic().name(topic.clone()).call()),
            create_topics_request::MAX_VERSION,
        ),
    })
    .slice(4..);
    forward(forwarding, topic, data).await;
}
#[when("the latest CreateTopics header forwards this undecodable UTF8 body")]
async fn malformed_body(forwarding: &mut Forwarding, docstring: String) {
    let body = docstring.trim_matches('\n').as_bytes();
    forward(
        forwarding,
        "undecodable body".into(),
        embedded_request_frame(19, create_topics_request::MAX_VERSION, body),
    )
    .await;
}
#[when("the valid CreateTopics frame for {topic} is forwarded with {bytes} trailing bytes removed")]
async fn truncated_body(forwarding: &mut Forwarding, topic: String, bytes: usize) {
    let frame = embedded_create_topics(&topic);
    let truncated = frame.slice(..frame.len() - bytes);
    forward(forwarding, topic, truncated).await;
}
#[when("embedded API key {key} version {version} with an empty body is forwarded")]
async fn empty_body(forwarding: &mut Forwarding, key: i16, version: i16) {
    forward(
        forwarding,
        format!("API{key}v{version}"),
        embedded_request_frame(key, version, &[]),
    )
    .await;
}
#[when("UnregisterController for controller nine is forwarded at its latest version")]
async fn unregister(forwarding: &mut Forwarding) {
    use krabka_protocol::owned::unregister_controller_request::{
        self, UnregisterControllerRequest,
    };
    let version = unregister_controller_request::MAX_VERSION;
    let body = encode(
        &UnregisterControllerRequest {
            controller_id: 9,
            ..Default::default()
        },
        version,
    );
    forward(
        forwarding,
        "UnregisterController".into(),
        embedded_request_frame(unregister_controller_request::API_KEY, version, &body),
    )
    .await;
}
#[when("API versions are requested from both controller and client listeners")]
async fn versions(forwarding: &mut Forwarding) {
    let broker = &forwarding
        .server
        .as_ref()
        .expect("controller started")
        .broker;
    forwarding.versions = Some((
        advertised_api_versions(broker.controller_addr()).await,
        advertised_api_versions(broker.listen_addr()).await,
    ));
}
#[when("these embedded principals forward CreateTopics using the configured client address")]
async fn host_requests(forwarding: &mut Forwarding, datatable: Vec<Vec<String>>) {
    for row in datatable.into_iter().skip(1) {
        let [principal, topic]: [String; 2] = row.try_into().expect("principal and topic");
        forwarding.principal = Some(principal_bytes(&principal, false));
        forward(forwarding, topic.clone(), embedded_create_topics(&topic)).await;
    }
}
#[when("these embedded principals request a producer ID block for the live broker in order")]
async fn producer_ids(forwarding: &mut Forwarding, datatable: Vec<Vec<String>>) {
    let broker = &forwarding
        .server
        .as_ref()
        .expect("controller started")
        .broker;
    broker.wait_until_brokers_registered(1).await;
    let (id, epoch) = {
        let image = broker.controller_image_for_test();
        let registered = image.brokers().next().expect("broker registered");
        (
            i32::try_from(registered.node_id.0).expect("broker ID"),
            registered.broker_epoch,
        )
    };
    let version = allocate_producer_ids_request::MAX_VERSION;
    let request = request_frame(RequestFrameSetup {
        api_key: krabka_ids::ApiKey(allocate_producer_ids_request::API_KEY),
        api_version: krabka_ids::ApiVersion(version),
        correlation_id: FrameCorrelationId(EMBEDDED_CORRELATION_ID),
        client_id: Some(b"forwarding-broker"),
        tagged: Some(&[0]),
        body: &encode(
            &AllocateProducerIdsRequest {
                broker_id: id,
                broker_epoch: epoch,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            version,
        ),
    })
    .slice(4..);
    for row in datatable.into_iter().skip(1) {
        let name = row.into_iter().next().expect("principal");
        forwarding.principal = Some(principal_bytes(&name, false));
        forward(forwarding, name, request.clone()).await;
    }
}
#[when("the embedded client requests a delegation token without an explicit owner")]
async fn token(forwarding: &mut Forwarding) {
    forward(
        forwarding,
        "CreateDelegationToken".into(),
        embedded_create_delegation_token(),
    )
    .await;
}
#[then("the envelope returns error code {code} with response data {presence}")]
fn envelope_outcome(forwarding: &Forwarding, code: i16, presence: String) {
    let response = latest(forwarding);
    check!(response.error_code == code);
    match presence.as_str() {
        "absent" => {
            check!(response.response_data.is_none());
        }
        "present" => {
            check!(response.response_data.is_some());
        }
        _ => panic!("unknown response data presence {presence}"),
    }
}
#[then("topic {topic} remains absent from the controller metadata after refusal code {code}")]
fn absent_topic(forwarding: &Forwarding, topic: String, code: i16) {
    check_refused(
        &forwarding
            .server
            .as_ref()
            .expect("controller started")
            .broker,
        &topic,
        latest(forwarding),
        code,
    );
}
#[then("controller metadata contains topic {topic} with {partitions} partitions")]
fn created_topic(forwarding: &Forwarding, topic: String, partitions: i32) {
    check!(
        forwarding
            .server
            .as_ref()
            .expect("controller started")
            .broker
            .controller_image_for_test()
            .topic(&topic)
            .is_some_and(|created| created.partitions == partitions)
    );
}
#[then("the embedded header is hex {header} and the fully consumed CreateTopics response contains")]
fn create_response(forwarding: &Forwarding, header: String, datatable: Vec<Vec<String>>) {
    let expected_header = hex_bytes(&header);
    let mut body = served_embedded_body(latest(forwarding), &expected_header, "");
    assert!(let Ok(response)=CreateTopicsResponse::decode(&mut body,create_topics_response::MAX_VERSION));
    check!(body.is_empty());
    let rows: Vec<[String; 4]> = datatable
        .into_iter()
        .skip(1)
        .map(|row| row.try_into().expect("topic code partitions replication"))
        .collect();
    check!(response.topics.len() == rows.len());
    for (actual, [topic, code, partitions, replication]) in response.topics.into_iter().zip(rows) {
        check!(
            (
                actual.name.as_str(),
                actual.error_code,
                actual.num_partitions,
                actual.replication_factor
            ) == (
                topic.as_str(),
                code.parse().expect("error code"),
                partitions.parse().expect("partitions"),
                replication.parse().expect("replication factor")
            )
        );
        check!(actual.error_message.is_none());
        check!(actual != CreatableTopicResult::default());
    }
}
#[then("Envelope is advertised only on the controller at versions zero through zero")]
fn envelope_advertised(forwarding: &Forwarding) {
    let (controller, client) = forwarding
        .versions
        .as_ref()
        .expect("both API tables requested");
    let find = |response: &ApiVersionsResponse| {
        response
            .api_keys
            .iter()
            .find(|api| api.api_key == envelope_request::API_KEY)
            .map(|api| (api.min_version, api.max_version))
    };
    check!(find(controller) == Some((0, 0)));
    check!(find(client).is_none());
}
#[then("the host authorized CreateTopics responses contain exactly")]
fn host_outcomes(forwarding: &Forwarding, datatable: Vec<Vec<String>>) {
    let mut observed = Vec::new();
    for (_, response) in &forwarding.responses {
        let rows: Vec<TopicOutcome> = embedded_create_topics_response(response)
            .topics
            .into_iter()
            .map(|row| TopicOutcome {
                topic: row.name,
                error_code: row.error_code,
            })
            .collect();
        check!(
            rows.len() == 1,
            "one row per single-topic CreateTopics: {rows:?}"
        );
        observed.extend(rows);
    }
    let expected: Vec<TopicOutcome> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [topic, code]: [String; 2] = row.try_into().expect("topic and code");
            TopicOutcome {
                topic,
                error_code: code.parse().expect("error code"),
            }
        })
        .collect();
    check!(observed == expected);
}
#[then("producer ID replies echo header hex {header} and contain these complete response fields")]
fn allocated(forwarding: &Forwarding, header: String, datatable: Vec<Vec<String>>) {
    let expected_header = hex_bytes(&header);
    let rows: Vec<[String; 4]> = datatable
        .into_iter()
        .skip(1)
        .map(|row| row.try_into().expect("principal code start length"))
        .collect();
    check!(forwarding.responses.len() == rows.len());
    for ((name, response), [principal, code, start, length]) in
        forwarding.responses.iter().zip(rows)
    {
        check!(name == &principal);
        let mut body = served_embedded_body(response, &expected_header, name);
        let allocated = AllocateProducerIdsResponse::decode(
            &mut body,
            allocate_producer_ids_response::MAX_VERSION,
        )
        .expect("embedded AllocateProducerIdsResponse");
        check!(body.is_empty());
        check!(
            allocated
                == AllocateProducerIdsResponse {
                    throttle_time_ms: 0,
                    error_code: code.parse().expect("error code"),
                    producer_id_start: start.parse().expect("producer ID start"),
                    producer_id_len: length.parse().expect("producer ID length"),
                    unknown_tagged_fields: UnknownTaggedFields::default()
                }
        );
    }
}
#[then(
    "the whole refused delegation token response has code {code} and identifies User alice as owner and requester with timestamps minus one and empty token fields"
)]
fn refused_token(forwarding: &Forwarding, code: i16) {
    check!(
        embedded_create_delegation_token_response(latest(forwarding))
            == CreateDelegationTokenResponse {
                error_code: code,
                principal_type: "User".into(),
                principal_name: "alice".into(),
                token_requester_principal_type: "User".into(),
                token_requester_principal_name: "alice".into(),
                issue_timestamp_ms: -1,
                expiry_timestamp_ms: -1,
                max_timestamp_ms: -1,
                ..CreateDelegationTokenResponse::default()
            }
    );
}
#[then("the minted delegation token succeeds for User alice and carries a nonempty HMAC")]
fn minted_token(forwarding: &Forwarding) {
    let response = embedded_create_delegation_token_response(latest(forwarding));
    check!(
        (
            response.error_code,
            response.principal_type.as_str(),
            response.principal_name.as_str()
        ) == (0, "User", "alice")
    );
    check!(!response.hmac.is_empty());
}
async fn shutdown(forwarding: Forwarding) {
    let EnvelopeBroker { broker, dir } = forwarding.server.expect("controller started");
    broker.shutdown().await;
    drop(dir);
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "A forwarded admin write returns client framed response bytes and reaches metadata"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_create_topics_is_served_and_answered_in_the_clients_own_bytes(
    forwarding: Forwarding,
) {
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "Outer refusals distinguish absent principals forbidden APIs and truncated headers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_envelope_the_controller_refuses_reports_kafkas_own_error_code(forwarding: Forwarding) {
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "Envelope discovery is restricted to the controller listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn envelope_is_advertised_on_the_controller_listener_and_nowhere_else(
    forwarding: Forwarding,
) {
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "Host ACLs evaluate the embedded client address rather than the forwarding connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_request_authorizes_against_the_embedded_client_host(forwarding: Forwarding) {
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "Malformed client address octets refuse the envelope before dispatch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unparseable_client_host_address_is_an_invalid_request(forwarding: Forwarding) {
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "Producer ID allocation checks the embedded principal without consuming denied blocks"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_allocate_producer_ids_needs_cluster_action_for_the_embedded_principal(
    forwarding: Forwarding,
) {
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "An unauthorized forwarding peer cannot impersonate a client"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_envelope_from_a_peer_without_cluster_action_is_refused(forwarding: Forwarding) {
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "The embedded API version is rechecked before dispatch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_embedded_version_the_broker_does_not_serve_is_refused(forwarding: Forwarding) {
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "Malformed embedded requests return errors while the controller connection remains usable"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_embedded_request_the_controller_cannot_parse_is_answered_in_the_envelope(
    forwarding: Forwarding,
) {
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "Unstable API configuration controls forwarding of a trunk API"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_embedded_trunk_api_key_is_refused_unless_unstable_api_versions_are_enabled(
    forwarding: Forwarding,
    enabled: bool,
    code: i16,
    presence: String,
) {
    let _ = (enabled, code, presence);
    shutdown(forwarding).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip590_envelope.feature",
    name = "Delegation token minting follows the embedded client token authentication flag"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_request_carries_the_clients_own_delegation_token_flag(forwarding: Forwarding) {
    shutdown(forwarding).await;
}
