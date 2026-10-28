//! Per-request authorization on the controller listener (#684).
//!
//! Kafka's `ControllerApis` authorizes every request against the principal of
//! the connection, with the cluster operation that the api needs. A denial is
//! the error response of that api, and the connection stays open. On
//! `PLAINTEXT` the principal is `ANONYMOUS`. On `SASL_PLAINTEXT` it is the
//! SASL principal, and a principal without `ClusterAction` can still run the
//! Admin apis that its own grants allow.
//!
//! Each case talks to a live one-node cluster over a raw controller-listener
//! socket, with a `SimpleAclAuthorizer` whose only super user is the node's
//! own inter-broker principal.
#![cfg(test)]

mod support;

use {
    crate::support::{
        acl::cluster_acl_record,
        controller_listener::*,
        quorum::describe_quorum_request as support_describe_quorum_request,
        start_with_bound_listeners,
        tls::{PinnedCertVerifier, fixture_signature_schemes},
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    krabka_metadata::{MetadataRecord, TopicRecord},
    krabka_protocol::owned::{
        broker_heartbeat_request::{self, BrokerHeartbeatRequest},
        broker_heartbeat_response::BrokerHeartbeatResponse,
        create_topics_request::{self},
        create_topics_response::CreateTopicsResponse,
        describe_cluster_request::{self, DescribeClusterRequest},
        describe_cluster_response::DescribeClusterResponse,
        describe_quorum_request::{self},
        describe_quorum_response::DescribeQuorumResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
        vote_request::{self, VoteRequest},
        vote_response::VoteResponse,
    },
    krabka_raft::{
        API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE, KrabkaMetadataFetchRequest,
        KrabkaMetadataFetchResponse, KrabkaSubmitChangeRequest, KrabkaSubmitChangeResponse,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    rustls::crypto::aws_lc_rs,
    std::{fs, iter, sync::Arc},
    tokio::{
        io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
        net::TcpStream,
    },
    tokio_rustls::rustls::{
        ClientConfig,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _},
    },
};

const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
const MESSAGE: &str = "Cluster authorization failed.";

/// A PLAIN user and its password.
struct Credential {
    user: &'static str,
    password: &'static str,
}

/// The node's own inter-broker principal, and its only super user.
const NODE: Credential = Credential {
    user: "node",
    password: "node-secret",
};

/// A running broker and the directory that backs it.
struct RunningBroker {
    broker: BrokerHandle,
    dir: tempfile::TempDir,
}

async fn start(protocol: ListenerProtocol, users: &[Credential]) -> RunningBroker {
    start_with(protocol, users, |_| {}).await
}

async fn start_with(
    protocol: ListenerProtocol,
    users: &[Credential],
    adjust: impl FnOnce(&mut BrokerConfig),
) -> RunningBroker {
    let (broker, dir) = start_with_bound_listeners(|config| {
        config.controller_listener_protocol = protocol;
        config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        for Credential { user, password } in iter::once(&NODE).chain(users) {
            config
                .plain_credentials
                .insert((*user).to_owned(), (*password).to_owned());
        }
        config.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
            username: NODE.user.to_owned(),
            password: NODE.password.to_owned(),
        });
        config.super_users = iter::once(NODE.user.to_owned()).collect();
        config.authorizer = Arc::new(SimpleAclAuthorizer::new(config.super_users.clone()));
        adjust(config);
    })
    .await;
    RunningBroker { broker, dir }
}

async fn allow(broker: &BrokerHandle, principal: &str, operation: AclOperation) {
    broker
        .submit_metadata_record_for_test(
            cluster_acl_record()
                .principal(principal)
                .operation(operation)
                .call(),
        )
        .await
        .expect("seed ACL");
}

fn encode<T: Encode>(message: &T, version: i16) -> Bytes {
    let mut body = BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.freeze()
}

/// Sends one request and returns the response body after its header. The
/// request and response headers are flexible when `flexible` is set.
async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    api_key: i16,
    version: i16,
    flexible: bool,
    body: &[u8],
) -> Bytes {
    let mut frame = BytesMut::new();
    frame.put_i16(api_key);
    frame.put_i16(version);
    frame.put_i32(77);
    frame.put_i16(-1);
    if flexible {
        frame.put_u8(0);
    }
    frame.put_slice(body);
    stream
        .write_all(&i32::try_from(frame.len()).expect("length").to_be_bytes())
        .await
        .expect("write length");
    stream.write_all(&frame).await.expect("write request");

    let mut length = [0_u8; 4];
    stream
        .read_exact(&mut length)
        .await
        .expect("a response, and the connection stays open");
    let mut response = vec![0_u8; usize::try_from(i32::from_be_bytes(length)).expect("length")];
    stream.read_exact(&mut response).await.expect("read body");
    let header = if flexible { 5 } else { 4 };
    check!(response[..4] == [0, 0, 0, 77], "correlation id");
    Bytes::copy_from_slice(&response[header..])
}

fn decode<T: Decode<'static>>(bytes: &Bytes, version: i16) -> T {
    let mut cursor: &[u8] = bytes;
    let decoded = T::decode(&mut cursor, version).expect("decode response");
    check!(cursor.is_empty(), "the response consumed its body");
    decoded
}

async fn sasl_plain(stream: &mut TcpStream, user: &str, password: &str) {
    let handshake = exchange(
        stream,
        17,
        1,
        false,
        &encode(
            &SaslHandshakeRequest {
                mechanism: "PLAIN".to_owned(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            1,
        ),
    )
    .await;
    check!(decode::<SaslHandshakeResponse>(&handshake, 1).error_code == 0);
    let mut auth_bytes = vec![0];
    auth_bytes.extend_from_slice(user.as_bytes());
    auth_bytes.push(0);
    auth_bytes.extend_from_slice(password.as_bytes());
    let authenticate = exchange(
        stream,
        36,
        2,
        true,
        &encode(
            &SaslAuthenticateRequest {
                auth_bytes: Bytes::from(auth_bytes),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            2,
        ),
    )
    .await;
    assert!(decode::<SaslAuthenticateResponse>(&authenticate, 2).error_code == 0);
}

fn submit_change(topic: &str) -> Bytes {
    let records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: topic.into(),
        topic_id: uuid::Uuid::new_v4(),
        partitions: 1,
        replication_factor: 1,
    })];
    let records =
        <serde_wincode::SerdeCompat<Vec<MetadataRecord>> as wincode::Serialize>::serialize(
            &records,
        )
        .expect("wincode");
    let mut body = Vec::new();
    KrabkaSubmitChangeRequest {
        records: Bytes::from(records),
    }
    .encode_v0(&mut body)
    .expect("encode SubmitChange");
    Bytes::from(body)
}

fn submit_change_response(bytes: &Bytes) -> KrabkaSubmitChangeResponse {
    let mut cursor: &[u8] = bytes;
    KrabkaSubmitChangeResponse::decode_v0(&mut cursor).expect("decode SubmitChange")
}

fn describe_quorum() -> Bytes {
    encode(
        &support_describe_quorum_request("__cluster_metadata".to_owned(), vec![0]),
        describe_quorum_request::MAX_VERSION,
    )
}

#[derive(Default)]
struct PlainControllerAuthorization {
    node: Option<RunningBroker>,
    stream: Option<TcpStream>,
    vote: Option<VoteResponse>,
    refused: Option<KrabkaSubmitChangeResponse>,
    heartbeat: Option<BrokerHeartbeatResponse>,
    applied: Option<KrabkaSubmitChangeResponse>,
}
#[rstest::fixture]
fn plain_controller_auth() -> PlainControllerAuthorization {
    let node = None;
    PlainControllerAuthorization {
        node,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "an open plaintext controller connection has principal ANONYMOUS and no ClusterAction grant"
)]
async fn anonymous_controller_connection(plain_controller_auth: &mut PlainControllerAuthorization) {
    let node = start(ListenerProtocol::Plaintext, &[]).await;
    plain_controller_auth.stream = Some(
        TcpStream::connect(node.broker.controller_addr())
            .await
            .expect("controller connection"),
    );
    plain_controller_auth.node = Some(node);
}
#[rstest_bdd_macros::when(
    "ANONYMOUS sends Vote, SubmitChange for anonymous-denied and BrokerHeartbeat for broker 1 at epoch -1 on that connection"
)]
async fn anonymous_controller_requests(plain_controller_auth: &mut PlainControllerAuthorization) {
    let stream = plain_controller_auth
        .stream
        .as_mut()
        .expect("controller connection");
    let version = vote_request::MAX_VERSION;
    let bytes = exchange(
        stream,
        vote_request::API_KEY,
        version,
        true,
        &encode(&VoteRequest::default(), version),
    )
    .await;
    plain_controller_auth.vote = Some(decode(&bytes, version));
    let bytes = exchange(
        stream,
        API_KEY_SUBMIT_CHANGE,
        0,
        true,
        &submit_change("anonymous-denied"),
    )
    .await;
    plain_controller_auth.refused = Some(submit_change_response(&bytes));
    let version = broker_heartbeat_request::MAX_VERSION;
    let bytes = exchange(
        stream,
        broker_heartbeat_request::API_KEY,
        version,
        true,
        &encode(
            &BrokerHeartbeatRequest {
                broker_id: 1,
                broker_epoch: -1,
                ..Default::default()
            },
            version,
        ),
    )
    .await;
    plain_controller_auth.heartbeat = Some(decode(&bytes, version));
}
#[rstest_bdd_macros::then(
    "Vote and SubmitChange return their complete authorization failure shapes, the heartbeat returns code 31 and anonymous-denied is absent"
)]
fn anonymous_controller_refusals(plain_controller_auth: &PlainControllerAuthorization) {
    check!(
        plain_controller_auth.vote.as_ref().expect("Vote response")
            == &VoteResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            }
    );
    check!(
        plain_controller_auth
            .refused
            .as_ref()
            .expect("SubmitChange response")
            == &KrabkaSubmitChangeResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                leader_hint: -1,
                result: Bytes::new()
            }
    );
    check!(
        plain_controller_auth
            .heartbeat
            .as_ref()
            .expect("heartbeat response")
            .error_code
            == CLUSTER_AUTHORIZATION_FAILED
    );
    check!(
        plain_controller_auth
            .node
            .as_ref()
            .expect("controller broker")
            .broker
            .controller_image_for_test()
            .topic("anonymous-denied")
            .is_none()
    );
}
#[rstest_bdd_macros::when(
    "User:ANONYMOUS receives ClusterAction and submits anonymous-allowed on the same controller connection"
)]
async fn grant_anonymous_controller_action(
    plain_controller_auth: &mut PlainControllerAuthorization,
) {
    allow(
        &plain_controller_auth
            .node
            .as_ref()
            .expect("controller broker")
            .broker,
        "User:ANONYMOUS",
        AclOperation::ClusterAction,
    )
    .await;
    let bytes = exchange(
        plain_controller_auth
            .stream
            .as_mut()
            .expect("same controller connection"),
        API_KEY_SUBMIT_CHANGE,
        0,
        true,
        &submit_change("anonymous-allowed"),
    )
    .await;
    plain_controller_auth.applied = Some(submit_change_response(&bytes));
}
#[rstest_bdd_macros::then(
    "SubmitChange succeeds and anonymous-allowed is committed without reconnecting"
)]
async fn authorized_anonymous_controller_action(
    plain_controller_auth: &mut PlainControllerAuthorization,
) {
    check!(
        plain_controller_auth
            .applied
            .as_ref()
            .expect("authorized submission")
            .error_code
            == 0
    );
    let node = plain_controller_auth
        .node
        .take()
        .expect("controller broker");
    check!(
        node.broker
            .controller_image_for_test()
            .topic("anonymous-allowed")
            .is_some()
    );
    node.broker.shutdown().await;
}

#[derive(Default)]
struct SaslControllerAuthorization {
    node: Option<RunningBroker>,
    creator: Option<TcpStream>,
    replicator: Option<TcpStream>,
    reader: Option<TcpStream>,
    created: Option<CreateTopicsResponse>,
    fetched: Option<KrabkaMetadataFetchResponse>,
    replicator_quorum: Option<DescribeQuorumResponse>,
    reader_quorum: Option<DescribeQuorumResponse>,
    reader_cluster: Option<DescribeClusterResponse>,
}
#[rstest::fixture]
fn sasl_controller_auth() -> SaslControllerAuthorization {
    let node = None;
    SaslControllerAuthorization {
        node,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "SASL PLAIN controller users creator, replicator and reader hold only cluster Create, ClusterAction and Describe respectively"
)]
async fn named_controller_connections(sasl_controller_auth: &mut SaslControllerAuthorization) {
    let credentials = [
        Credential {
            user: "creator",
            password: "creator-secret",
        },
        Credential {
            user: "replicator",
            password: "replicator-secret",
        },
        Credential {
            user: "reader",
            password: "reader-secret",
        },
    ];
    let node = start(ListenerProtocol::SaslPlaintext, &credentials).await;
    for (credential, operation) in credentials.iter().zip([
        AclOperation::Create,
        AclOperation::ClusterAction,
        AclOperation::Describe,
    ]) {
        allow(
            &node.broker,
            &format!("User:{}", credential.user),
            operation,
        )
        .await;
        let mut stream = TcpStream::connect(node.broker.controller_addr())
            .await
            .expect("controller connection");
        sasl_plain(&mut stream, credential.user, credential.password).await;
        match credential.user {
            "creator" => sasl_controller_auth.creator = Some(stream),
            "replicator" => sasl_controller_auth.replicator = Some(stream),
            "reader" => sasl_controller_auth.reader = Some(stream),
            _ => unreachable!("fixed controller users"),
        }
    }
    sasl_controller_auth.node = Some(node);
}
#[rstest_bdd_macros::when("creator creates topic created-by-creator on the controller connection")]
async fn creator_controller_topic(sasl_controller_auth: &mut SaslControllerAuthorization) {
    let version = create_topics_request::MAX_VERSION;
    let bytes = exchange(
        sasl_controller_auth
            .creator
            .as_mut()
            .expect("creator connection"),
        create_topics_request::API_KEY,
        version,
        true,
        &encode(
            &create_topic_request(
                creatable_topic()
                    .name("created-by-creator".to_owned())
                    .call(),
            ),
            version,
        ),
    )
    .await;
    sasl_controller_auth.created = Some(decode(&bytes, version));
}
#[rstest_bdd_macros::then(
    "the creation response contains exactly created-by-creator with success code 0"
)]
fn creator_controller_topic_response(sasl_controller_auth: &SaslControllerAuthorization) {
    let response = sasl_controller_auth
        .created
        .as_ref()
        .expect("creation response");
    check!(
        response
            .topics
            .iter()
            .map(|topic| (topic.name.as_str(), topic.error_code))
            .collect::<Vec<_>>()
            == [("created-by-creator", 0)]
    );
}
#[rstest_bdd_macros::when(
    "replicator fetches metadata from offset 0 with max bytes 1024 and then describes the metadata quorum"
)]
async fn replicator_controller_requests(sasl_controller_auth: &mut SaslControllerAuthorization) {
    let stream = sasl_controller_auth
        .replicator
        .as_mut()
        .expect("replicator connection");
    let mut fetch = Vec::new();
    KrabkaMetadataFetchRequest {
        fetch_offset: 0,
        max_bytes: 1024,
        replica_id: -1,
        replica_directory_id: uuid::Uuid::nil(),
    }
    .encode_v0(&mut fetch);
    let bytes = exchange(stream, API_KEY_METADATA_FETCH, 0, true, &fetch).await;
    sasl_controller_auth.fetched = Some(
        KrabkaMetadataFetchResponse::decode_v0(&mut &bytes[..]).expect("MetadataFetch response"),
    );
    let version = describe_quorum_request::MAX_VERSION;
    let bytes = exchange(
        stream,
        describe_quorum_request::API_KEY,
        version,
        true,
        &describe_quorum(),
    )
    .await;
    sasl_controller_auth.replicator_quorum = Some(decode(&bytes, version));
}
#[rstest_bdd_macros::then(
    "MetadataFetch succeeds but replicator's complete DescribeQuorum response is an authorization failure"
)]
fn replicator_controller_responses(sasl_controller_auth: &SaslControllerAuthorization) {
    check!(
        sasl_controller_auth
            .fetched
            .as_ref()
            .expect("metadata fetch response")
            .error_code
            == 0
    );
    check!(
        sasl_controller_auth
            .replicator_quorum
            .as_ref()
            .expect("quorum response")
            == &DescribeQuorumResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some(MESSAGE.to_owned()),
                ..Default::default()
            }
    );
}
#[rstest_bdd_macros::when(
    "reader describes the metadata quorum and describes cluster endpoint type 2 on the same controller connection"
)]
async fn reader_controller_requests(sasl_controller_auth: &mut SaslControllerAuthorization) {
    let stream = sasl_controller_auth
        .reader
        .as_mut()
        .expect("reader connection");
    let version = describe_quorum_request::MAX_VERSION;
    let bytes = exchange(
        stream,
        describe_quorum_request::API_KEY,
        version,
        true,
        &describe_quorum(),
    )
    .await;
    sasl_controller_auth.reader_quorum = Some(decode(&bytes, version));
    let version = describe_cluster_request::MAX_VERSION;
    let bytes = exchange(
        stream,
        describe_cluster_request::API_KEY,
        version,
        true,
        &encode(
            &DescribeClusterRequest {
                endpoint_type: 2,
                ..Default::default()
            },
            version,
        ),
    )
    .await;
    sasl_controller_auth.reader_cluster = Some(decode(&bytes, version));
}
#[rstest_bdd_macros::then(
    "reader's DescribeQuorum succeeds but its complete DescribeCluster response is an authorization failure"
)]
async fn reader_controller_responses(sasl_controller_auth: &mut SaslControllerAuthorization) {
    check!(
        sasl_controller_auth
            .reader_quorum
            .as_ref()
            .expect("quorum response")
            .error_code
            == 0
    );
    check!(
        sasl_controller_auth
            .reader_cluster
            .as_ref()
            .expect("cluster response")
            == &DescribeClusterResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some(MESSAGE.to_owned()),
                ..Default::default()
            }
    );
    sasl_controller_auth
        .node
        .take()
        .expect("controller broker")
        .broker
        .shutdown()
        .await;
}
#[derive(Default)]
struct TlsControllerAuthorization {
    node: Option<RunningBroker>,
    pem_directory: Option<tempfile::TempDir>,
    stream: Option<TlsStream<TcpStream>>,
    responses: Vec<DescribeQuorumResponse>,
}
#[rstest::fixture]
fn tls_controller_auth() -> TlsControllerAuthorization {
    let node = None;
    TlsControllerAuthorization {
        node,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a mutually authenticated TLS controller connection uses the fixture client certificate principal without a Describe grant"
)]
async fn certificate_controller_connection(tls_controller_auth: &mut TlsControllerAuthorization) {
    let _ = aws_lc_rs::default_provider().install_default();
    let pem_dir = tempfile::TempDir::new().expect("tempdir");
    let write = |name: &str, contents: &str| {
        let path = pem_dir.path().join(name);
        fs::write(&path, contents).expect("write fixture");
        path
    };
    let tls = krabka_security::TlsConfig {
        cert_chain_path: write("server.pem", DEV_CERT),
        private_key_path: write("server.key", DEV_KEY),
        trust_roots_path: None,
        client_ca_path: Some(write("client_ca.pem", DEV_CLIENT_CA)),
        client_auth: krabka_security::ClientAuthMode::Required,
    };
    let RunningBroker { broker, dir } = start_with(ListenerProtocol::Ssl, &[], |config| {
        config.tls_config = Some(tls);
    })
    .await;

    let server_certificate = CertificateDer::pem_slice_iter(DEV_CERT.as_bytes())
        .next()
        .expect("fixture server certificate")
        .expect("parse server certificate");
    let client_certificates: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(DEV_CLIENT_CERT.as_bytes())
            .collect::<Result<_, _>>()
            .expect("parse client certificate");
    let client_key =
        PrivateKeyDer::from_pem_slice(DEV_CLIENT_KEY.as_bytes()).expect("parse client key");
    let client = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
            pinned: server_certificate,
            schemes: fixture_signature_schemes(),
            mismatch: "not the pinned fixture certificate",
        }))
        .with_client_auth_cert(client_certificates, client_key)
        .expect("client certificate");
    let tcp = TcpStream::connect(broker.controller_addr())
        .await
        .expect("connect controller listener");
    let stream = tokio_rustls::TlsConnector::from(Arc::new(client))
        .connect(
            ServerName::try_from("krabka-dev").expect("server name"),
            tcp,
        )
        .await
        .expect("mTLS handshake");

    tls_controller_auth.node = Some(RunningBroker { broker, dir });
    tls_controller_auth.pem_directory = Some(pem_dir);
    tls_controller_auth.stream = Some(stream);
}
#[rstest_bdd_macros::when(
    "the certificate principal sends DescribeQuorum without a Describe grant"
)]
async fn refused_certificate_quorum(tls_controller_auth: &mut TlsControllerAuthorization) {
    let version = describe_quorum_request::MAX_VERSION;
    let bytes = exchange(
        tls_controller_auth
            .stream
            .as_mut()
            .expect("TLS controller connection"),
        describe_quorum_request::API_KEY,
        version,
        true,
        &describe_quorum(),
    )
    .await;
    tls_controller_auth.responses.push(decode(&bytes, version));
}
#[rstest_bdd_macros::then(
    "the complete certificate-authenticated DescribeQuorum response has authorization error 31 and Cluster authorization failed message"
)]
fn certificate_quorum_denial(tls_controller_auth: &TlsControllerAuthorization) {
    check!(
        tls_controller_auth.responses.as_slice()
            == [DescribeQuorumResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some(MESSAGE.to_owned()),
                ..Default::default()
            }]
    );
}
#[rstest_bdd_macros::when(
    "the certificate principal receives Describe and sends DescribeQuorum again on the same TLS connection"
)]
async fn granted_certificate_quorum(tls_controller_auth: &mut TlsControllerAuthorization) {
    allow(
        &tls_controller_auth
            .node
            .as_ref()
            .expect("TLS controller broker")
            .broker,
        &format!("User:{CLIENT_PRINCIPAL}"),
        AclOperation::Describe,
    )
    .await;
    let version = describe_quorum_request::MAX_VERSION;
    let bytes = exchange(
        tls_controller_auth
            .stream
            .as_mut()
            .expect("same TLS connection"),
        describe_quorum_request::API_KEY,
        version,
        true,
        &describe_quorum(),
    )
    .await;
    tls_controller_auth.responses.push(decode(&bytes, version));
}
#[rstest_bdd_macros::then(
    "the next certificate-authenticated DescribeQuorum response succeeds without reconnecting"
)]
async fn certificate_quorum_success(tls_controller_auth: &mut TlsControllerAuthorization) {
    let [_, response] = tls_controller_auth.responses.as_slice() else {
        panic!("denied and allowed responses required")
    };
    check!(response.error_code == 0);
    tls_controller_auth
        .node
        .take()
        .expect("TLS controller broker")
        .broker
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/listener_authorization.feature",
    name = "Plaintext authorization is checked for each request on an open connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plaintext_controller_listener_authorizes_every_request_for_anonymous(
    plain_controller_auth: PlainControllerAuthorization,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/listener_authorization.feature",
    name = "SASL principals receive only the controller operations they hold"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sasl_controller_listener_authorizes_each_request_for_its_principal(
    sasl_controller_auth: SaslControllerAuthorization,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/listener_authorization.feature",
    name = "TLS certificate identity authorizes each request on the same connection"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ssl_controller_listener_authorizes_each_request_for_the_certificate_principal(
    tls_controller_auth: TlsControllerAuthorization,
) {
}

use {
    crate::support::tls::{
        CLIENT_PRINCIPAL, DEV_CERT, DEV_CLIENT_CA, DEV_CLIENT_CERT, DEV_CLIENT_KEY, DEV_KEY,
    },
    tokio_rustls::client::TlsStream,
};
