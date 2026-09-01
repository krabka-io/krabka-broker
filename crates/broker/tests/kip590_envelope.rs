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

use assert2::{assert, check};
use bytes::{BufMut as _, Bytes, BytesMut};
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, NodeId, authorizer::SimpleAclAuthorizer};
use krabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};
use krabka_protocol::{
    Decode as _, Encode, UnknownTaggedFields,
    owned::{
        allocate_producer_ids_request::{self, AllocateProducerIdsRequest},
        allocate_producer_ids_response::AllocateProducerIdsResponse,
        api_versions_response::ApiVersionsResponse,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
        envelope_request::{self, EnvelopeRequest},
        envelope_response::EnvelopeResponse,
        produce_request::ProduceRequest,
    },
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

mod support;

/// `DefaultKafkaPrincipalBuilder.serialize(new KafkaPrincipal("User",
/// "alice"))` from the pinned image: int16 schema version 0, compact string
/// `"User"`, compact string `"alice"`, `token_authenticated = false`, empty
/// tagged fields. A forwarding JVM broker puts exactly these bytes in
/// `EnvelopeRequest.request_principal`.
const JVM_USER_ALICE: &[u8] = &[
    0x00, 0x00, 0x05, b'U', b's', b'e', b'r', 0x06, b'a', b'l', b'i', b'c', b'e', 0x00, 0x00,
];

/// The same serialization for `new KafkaPrincipal("User", "bob")`. It differs
/// from [`JVM_USER_ALICE`] only in the compact string and its length prefix;
/// the `token_authenticated` byte is `0x00` in both.
const JVM_USER_BOB: &[u8] = &[
    0x00, 0x00, 0x05, b'U', b's', b'e', b'r', 0x04, b'b', b'o', b'b', 0x00, 0x00,
];

/// The address a forwarding broker copies out of its own client's connection
/// into `client_host_address`. `EnvelopeRequest.Builder` is handed
/// `clientAddress.getAddress()`, so a v4 client is these four raw octets and
/// no port.
const LOOPBACK_CLIENT: &[u8] = &[127, 0, 0, 1];

/// A client address that is deliberately *not* the loopback address every
/// test connection to the controller listener comes from, so a host ACL
/// keyed on it can only match if the embedded address is what was used.
const REMOTE_CLIENT: &[u8] = &[10, 1, 2, 3];

/// The correlation id the "client" put in the embedded request header. The
/// controller must echo this one, not the envelope's own, or the forwarding
/// hop cannot match the relayed bytes to the client that is waiting.
const EMBEDDED_CORRELATION_ID: i32 = 0x5A5A_1234;

async fn start_broker() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|_| {}).await
}

async fn start_broker_with(
    customize: impl FnOnce(&mut BrokerConfig),
) -> (BrokerHandle, tempfile::TempDir) {
    support::init_tracing();
    let dir = tempfile::TempDir::new().expect("tempdir");
    let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind data listener");
    let controller_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind controller listener");
    let data_addr = data_listener.local_addr().expect("data addr");
    let controller_addr = controller_listener.local_addr().expect("controller addr");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listen_addr = data_addr;
    config.advertised_listener = data_addr.to_string();
    config.controller_listen_addr = controller_addr;
    config.controller_quorum_voters = vec![(NodeId(1), controller_addr.to_string())];
    customize(&mut config);
    let broker =
        Broker::start_with_listeners(config, Some(controller_listener), Some(data_listener))
            .await
            .expect("start broker");
    (broker, dir)
}

/// A Kafka request frame: the length prefix, the request header, and the body.
///
/// The header is v2 — with a trailing tagged-fields byte — when `flexible`,
/// and v1 otherwise. `client_id` is a `NULLABLE_STRING` with an i16 length in
/// both, which is why it is written the same way either way.
fn request_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    flexible: bool,
    body: &[u8],
) -> Bytes {
    let mut frame = BytesMut::new();
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(correlation_id);
    match client_id {
        Some(id) => {
            frame.put_i16(i16::try_from(id.len()).expect("client id length"));
            frame.put_slice(id.as_bytes());
        }
        None => frame.put_i16(-1),
    }
    if flexible {
        frame.put_u8(0);
    }
    frame.put_slice(body);

    let mut out = BytesMut::with_capacity(4 + frame.len());
    out.put_i32(i32::try_from(frame.len()).expect("frame length"));
    out.put_slice(&frame);
    out.freeze()
}

fn encode<T: Encode>(message: &T, version: i16) -> Bytes {
    let mut buf = BytesMut::new();
    message.encode(&mut buf, version).expect("encode");
    buf.freeze()
}

/// Send one request frame and return the response frame with its length
/// prefix stripped, so the caller sees `correlation_id` first.
async fn round_trip(addr: std::net::SocketAddr, frame: Bytes) -> Bytes {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect listener");
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

/// Send an `Envelope` at v0 to the controller listener and decode the
/// `EnvelopeResponse` out of the reply.
///
/// The response header is v1 because the `EnvelopeResponse` body is flexible,
/// so four bytes of correlation id are followed by one tagged-fields byte
/// before the body starts. Both are checked here rather than skipped, since a
/// missing or extra byte would shift the whole body.
async fn send_envelope(addr: std::net::SocketAddr, envelope: &EnvelopeRequest) -> EnvelopeResponse {
    const ENVELOPE_CORRELATION_ID: i32 = 99;

    let frame = request_frame(
        envelope_request::API_KEY,
        0,
        ENVELOPE_CORRELATION_ID,
        Some("forwarding-broker"),
        true,
        &encode(envelope, 0),
    );
    let response = round_trip(addr, frame).await;

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
    let version = krabka_protocol::owned::create_topics_request::MAX_VERSION;
    let body = encode(
        &CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_owned(),
                num_partitions: 1,
                replication_factor: 1,
                ..CreatableTopic::default()
            }],
            timeout_ms: 5_000,
            validate_only: false,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
        version,
    );
    // The length prefix belongs to the outer connection, not to `request_data`
    // — KIP-590 wraps the header and body only.
    let framed = request_frame(
        krabka_protocol::owned::create_topics_request::API_KEY,
        version,
        EMBEDDED_CORRELATION_ID,
        Some("adminclient-1"),
        true,
        &body,
    );
    framed.slice(4..)
}

fn envelope_for(request_data: Bytes, principal: Option<&'static [u8]>) -> EnvelopeRequest {
    envelope_from(request_data, principal, LOOPBACK_CLIENT)
}

fn envelope_from(
    request_data: Bytes,
    principal: Option<&'static [u8]>,
    client_host: &'static [u8],
) -> EnvelopeRequest {
    EnvelopeRequest {
        request_data,
        request_principal: principal.map(Bytes::from_static),
        client_host_address: Bytes::from_static(client_host),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

/// The whole forwarding round trip: an `Envelope` carrying a `CreateTopics`
/// reaches the controller, the topic is created, and the bytes that come back
/// are exactly what the client would have received had it reached the
/// controller itself — the client's own correlation id, the client's own
/// response header shape, and the client's own response body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_create_topics_is_served_and_answered_in_the_clients_own_bytes() {
    const TOPIC: &str = "envelope-forwarded-topic";

    let (broker, _dir) = start_broker().await;

    let response = send_envelope(
        broker.controller_addr(),
        &envelope_for(embedded_create_topics(TOPIC), Some(JVM_USER_ALICE)),
    )
    .await;

    check!(response.error_code == 0);
    assert!(let Some(response_data) = response.response_data);

    // `CreateTopics` is flexible at its maximum version, so the embedded
    // response header is v1: the client's correlation id then one empty
    // tagged-fields byte.
    assert!(let Some((header, mut body)) = response_data.split_at_checked(5));
    check!(header == [0x5A, 0x5A, 0x12, 0x34, 0x00]);

    let version = krabka_protocol::owned::create_topics_response::MAX_VERSION;
    assert!(let Ok(created) = CreateTopicsResponse::decode(&mut body, version));
    check!(
        body.is_empty(),
        "the embedded response consumed its own bytes"
    );
    check!(created.topics.len() == 1);
    check!(
        (
            created.topics[0].name.as_str(),
            created.topics[0].error_code,
            created.topics[0].num_partitions,
            created.topics[0].replication_factor,
        ) == (TOPIC, 0, 1, 1)
    );
    assert!(let Some(topic) = created.topics.into_iter().next());
    check!(topic.error_message.is_none());
    check!(topic != CreatableTopicResult::default());

    // The forward is only real if the write landed: the controller's own image
    // must now hold the topic the envelope asked for.
    check!(
        broker
            .controller_image_for_test()
            .topic(TOPIC)
            .is_some_and(|created| created.partitions == 1),
        "the forwarded CreateTopics did not reach the metadata image"
    );
}

/// Envelopes the controller must refuse, and the `error_code` Kafka's
/// `kafka.server.EnvelopeUtils` assigns to each. A refusal still comes back as
/// a well-formed `EnvelopeResponse` with a null `response_data`, because the
/// forwarding hop has to translate the code for its own client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_envelope_the_controller_refuses_reports_kafkas_own_error_code() {
    let (broker, _dir) = start_broker().await;
    let addr = broker.controller_addr();

    let produce = request_frame(
        krabka_protocol::owned::produce_request::API_KEY,
        krabka_protocol::owned::produce_request::MAX_VERSION,
        7,
        Some("adminclient-1"),
        true,
        &encode(
            &ProduceRequest::default(),
            krabka_protocol::owned::produce_request::MAX_VERSION,
        ),
    )
    .slice(4..);

    let cases = [
        (
            "a null request_principal is a deserialization failure",
            envelope_for(embedded_create_topics("envelope-no-principal"), None),
            97,
        ),
        (
            "an embedded api key outside ApiKeys.forwardable is an invalid request",
            envelope_for(produce, Some(JVM_USER_ALICE)),
            42,
        ),
        (
            "an embedded frame too short to hold a request header",
            envelope_for(Bytes::from_static(&[0, 19, 0]), Some(JVM_USER_ALICE)),
            35,
        ),
    ];

    for (case, envelope, want) in cases {
        let response = send_envelope(addr, &envelope).await;

        check!(
            (response.error_code, response.response_data.clone()) == (want, None),
            "case: {case}"
        );
    }
}

/// Read the `ApiVersions` v0 table a listener advertises.
///
/// v0 is deliberate: its request header is v1 and its response header is v0,
/// so neither carries a tagged-fields byte and the reply starts with the
/// correlation id followed straight by the body.
async fn advertised_api_versions(addr: std::net::SocketAddr) -> ApiVersionsResponse {
    let frame = request_frame(18, 0, 1, Some("probe"), false, &[]);
    let response = round_trip(addr, frame).await;

    let mut cur = response.as_ref();
    assert!(let Some((header, rest)) = cur.split_at_checked(4));
    check!(header == [0, 0, 0, 1], "response header v0 for ApiVersions");
    cur = rest;
    ApiVersionsResponse::decode(&mut cur, 0).expect("decode ApiVersionsResponse")
}

/// `Envelope` is advertised on the controller listener and on no other.
///
/// `ApiKeys.ENVELOPE.messageType.listeners()` in the pinned image is exactly
/// `[CONTROLLER]`, and both `ApiKeys.clientApis()` and `ApiKeys.brokerApis()`
/// exclude it. Advertising it to clients would invite them to forward on their
/// own behalf; not advertising it on the controller listener would leave a
/// JVM broker with no forwarding path at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn envelope_is_advertised_on_the_controller_listener_and_nowhere_else() {
    let (broker, _dir) = start_broker().await;

    let controller = advertised_api_versions(broker.controller_addr()).await;
    let client = advertised_api_versions(broker.listen_addr()).await;

    let envelope = |response: &ApiVersionsResponse| {
        response
            .api_keys
            .iter()
            .find(|api| api.api_key == envelope_request::API_KEY)
            .map(|api| (api.min_version, api.max_version))
    };

    check!(envelope(&controller) == Some((0, 0)), "controller listener");
    check!(envelope(&client).is_none(), "client listener");
}

/// Decode the embedded `CreateTopicsResponse` out of a served `Envelope`.
///
/// `CreateTopics` is flexible at its maximum version, so the embedded
/// response header is v1: four bytes of the client's correlation id and one
/// empty tagged-fields byte before the body.
fn embedded_create_topics_response(response: &EnvelopeResponse) -> CreateTopicsResponse {
    check!(response.error_code == 0, "the Envelope itself was served");
    let data = response
        .response_data
        .as_ref()
        .expect("a served Envelope carries response_data");
    let mut body = data.get(5..).expect("embedded response header v1");
    let decoded = CreateTopicsResponse::decode(
        &mut body,
        krabka_protocol::owned::create_topics_response::MAX_VERSION,
    )
    .expect("decode the embedded CreateTopicsResponse");
    check!(
        body.is_empty(),
        "the embedded response consumed its own bytes"
    );
    decoded
}

/// A host ACL on a forwarded request must be evaluated against the client's
/// own address, not the forwarding broker's.
///
/// Both envelopes below reach the controller listener over loopback, so the
/// connection's peer is `127.0.0.1` for both, while `client_host_address`
/// names `10.1.2.3`. The two seeded ACLs are the two ways the connection's
/// address is the wrong answer:
///
/// - `User:alice` may create only from `10.1.2.3`, the address the envelope
///   names. Reading the connection instead denies a client that the host ACL
///   allows.
/// - `User:bob` may create only from `127.0.0.1`, which is the address the
///   *forwarding hop* happens to connect from. Reading the connection instead
///   authorizes a client that the host ACL denies — the broker's own address
///   launders the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_request_authorizes_against_the_embedded_client_host() {
    let (broker, _dir) = start_broker_with(|config| {
        // `ANONYMOUS` is the identity a plaintext controller connection
        // carries, and `ApiKeys.ENVELOPE.clusterAction` gates the envelope on
        // it. Making it a super-user holds that outer gate open so the inner
        // host check is the only thing this test moves.
        config.super_users = std::iter::once("ANONYMOUS".to_owned()).collect();
        config.authorizer =
            std::sync::Arc::new(SimpleAclAuthorizer::new(config.super_users.clone()));
    })
    .await;

    for (principal, host) in [("User:alice", "10.1.2.3"), ("User:bob", "127.0.0.1")] {
        broker
            .submit_metadata_record_for_test(MetadataRecord::V1AccessControlEntry(AclEntry {
                resource_type: ResourceType::Cluster,
                resource_name: "kafka-cluster".into(),
                pattern_type: PatternType::Literal,
                principal: principal.into(),
                host: host.into(),
                operation: AclOperation::Create,
                permission_type: PermissionType::Allow,
            }))
            .await
            .expect("seed host ACL");
    }

    // Both directions are driven before anything is asserted, so one failing
    // case cannot hide the other: reading the connection's address gets the
    // first wrong *and* the second wrong, and a report of only one of them
    // would read like a one-sided mistake.
    let mut outcome: Vec<(&str, i16)> = Vec::new();
    for (principal, topic) in [
        (JVM_USER_ALICE, "envelope-host-acl-alice"),
        (JVM_USER_BOB, "envelope-host-acl-bob"),
    ] {
        let response = send_envelope(
            broker.controller_addr(),
            &envelope_from(
                embedded_create_topics(topic),
                Some(principal),
                REMOTE_CLIENT,
            ),
        )
        .await;

        let rows: Vec<(String, i16)> = embedded_create_topics_response(&response)
            .topics
            .into_iter()
            .map(|row| (row.name, row.error_code))
            .collect();
        check!(
            rows.len() == 1,
            "one row per single-topic CreateTopics: {rows:?}"
        );
        outcome.push((topic, rows.first().map_or(i16::MIN, |row| row.1)));
    }

    check!(
        outcome
            == vec![
                // Allowed from the address the envelope names.
                ("envelope-host-acl-alice", 0),
                // Allowed only from the forwarding hop's address, so refused.
                ("envelope-host-acl-bob", 31),
            ]
    );
}

/// An `EnvelopeRequest.client_host_address` `InetAddress.getByAddress` would
/// refuse is an invalid request, and the embedded handler never runs.
///
/// `EnvelopeUtils.parseForwardedClientAddress` catches `UnknownHostException`
/// and rethrows it as `InvalidRequestException` (42), and it does so before
/// the embedded request header is parsed at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unparseable_client_host_address_is_an_invalid_request() {
    const TOPIC: &str = "envelope-bad-client-host";

    let (broker, _dir) = start_broker().await;

    let response = send_envelope(
        broker.controller_addr(),
        &envelope_from(
            embedded_create_topics(TOPIC),
            Some(JVM_USER_ALICE),
            &[10, 1, 2],
        ),
    )
    .await;

    check!((response.error_code, response.response_data) == (42, None));
    check!(
        broker.controller_image_for_test().topic(TOPIC).is_none(),
        "the embedded request must not have run"
    );
}

/// A forwarded `AllocateProducerIds` (67) is served and returns a block.
///
/// 67 is in `ApiKeys.forwardable`, and a JVM broker forwards it to the
/// controller to obtain the producer-ID block every producer on it needs
/// before it can initialise. Its handler takes no session at all, so the
/// shared invocation path has to dispatch a plain handler as well as the
/// session-carrying kinds: answering `UnsupportedApi` fails the controller
/// connection and the forwarding broker never gets a block.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_allocate_producer_ids_is_served_a_block() {
    let (broker, _dir) = start_broker().await;
    broker.wait_until_brokers_registered(1).await;

    let image = broker.controller_image_for_test();
    let registered = image
        .brokers()
        .next()
        .expect("the broker self-registers before it serves");
    let broker_id = i32::try_from(registered.node_id.0).expect("broker id");

    let version = allocate_producer_ids_request::MAX_VERSION;
    let request_data = request_frame(
        allocate_producer_ids_request::API_KEY,
        version,
        EMBEDDED_CORRELATION_ID,
        Some("forwarding-broker"),
        true,
        &encode(
            &AllocateProducerIdsRequest {
                broker_id,
                broker_epoch: registered.broker_epoch,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            version,
        ),
    )
    .slice(4..);

    let response = send_envelope(
        broker.controller_addr(),
        &envelope_for(request_data, Some(JVM_USER_ALICE)),
    )
    .await;

    check!(response.error_code == 0);
    assert!(let Some(response_data) = response.response_data);
    assert!(let Some((header, mut body)) = response_data.split_at_checked(5));
    check!(header == [0x5A, 0x5A, 0x12, 0x34, 0x00]);

    let allocated = AllocateProducerIdsResponse::decode(
        &mut body,
        krabka_protocol::owned::allocate_producer_ids_response::MAX_VERSION,
    )
    .expect("decode the embedded AllocateProducerIdsResponse");
    check!(
        body.is_empty(),
        "the embedded response consumed its own bytes"
    );
    check!(
        allocated
            == AllocateProducerIdsResponse {
                throttle_time_ms: 0,
                error_code: 0,
                producer_id_start: 0,
                producer_id_len: 1_000,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
}

/// Only a peer that holds `ClusterAction` may speak for another identity.
///
/// `ApiKeys.ENVELOPE.clusterAction` is true, so `EnvelopeUtils` runs that gate
/// on the *forwarding connection's* principal before it looks at the payload
/// at all. Without it, anything that can reach the controller listener can
/// mint an envelope naming any principal it likes and have the controller run
/// the request as that identity -- the forwarding wrapper becomes an
/// impersonation primitive.
///
/// The broker here runs a real ACL authorizer with no super-users, so the
/// plaintext connection's `ANONYMOUS` holds nothing. The refusal has to arrive
/// as an `EnvelopeResponse` carrying the code and a null `response_data`, and
/// the embedded `CreateTopics` must never have run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_envelope_from_a_peer_without_cluster_action_is_refused() {
    const TOPIC: &str = "envelope-no-cluster-action";

    let (broker, _dir) = start_broker_with(|config| {
        config.super_users = std::collections::HashSet::new();
        config.authorizer =
            std::sync::Arc::new(SimpleAclAuthorizer::new(config.super_users.clone()));
    })
    .await;

    let response = send_envelope(
        broker.controller_addr(),
        &envelope_for(embedded_create_topics(TOPIC), Some(JVM_USER_ALICE)),
    )
    .await;

    check!((response.error_code, response.response_data) == (31, None));
    check!(
        broker.controller_image_for_test().topic(TOPIC).is_none(),
        "the embedded request must not have run"
    );
}

/// An embedded request at a version this broker does not serve is an
/// `UNSUPPORTED_VERSION`, and the handler never sees it.
///
/// The embedded header carries its own `api_version`, and nothing on the
/// controller connection has negotiated it: the forwarding hop negotiated
/// `Envelope`, and the *client* negotiated the inner api against the hop.
/// So the version has to be re-checked against this broker's own registry
/// before dispatch, or a handler is handed a version it never advertised.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_embedded_version_the_broker_does_not_serve_is_refused() {
    const TOPIC: &str = "envelope-unsupported-version";

    let (broker, _dir) = start_broker().await;

    let beyond = krabka_protocol::owned::create_topics_request::MAX_VERSION + 1;
    let request_data = request_frame(
        krabka_protocol::owned::create_topics_request::API_KEY,
        beyond,
        EMBEDDED_CORRELATION_ID,
        Some("adminclient-1"),
        true,
        &encode(
            &CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: TOPIC.to_owned(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..CreatableTopic::default()
                }],
                timeout_ms: 5_000,
                validate_only: false,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            krabka_protocol::owned::create_topics_request::MAX_VERSION,
        ),
    )
    .slice(4..);

    let response = send_envelope(
        broker.controller_addr(),
        &envelope_for(request_data, Some(JVM_USER_ALICE)),
    )
    .await;

    check!((response.error_code, response.response_data) == (35, None));
    check!(
        broker.controller_image_for_test().topic(TOPIC).is_none(),
        "the embedded request must not have run"
    );
}
