//! `CreateDelegationToken` over a connection that never authenticated.
//!
//! Kafka's `KafkaApis.allowTokenRequests` refuses the token RPCs when the
//! security protocol is PLAINTEXT, or SSL without a client certificate (the
//! principal is then `KafkaPrincipal.ANONYMOUS`). Membership in
//! `super.users` does not lift that refusal, which is why
//! `super_users = ["ANONYMOUS"]` is never a way to let an unauthenticated
//! reconcile loop mint tokens — it is only a cluster-wide authorization
//! hole. `BrokerConfig::validate` rejects that value outright, and this file
//! pins the wire behavior that justifies the rejection.

use {
    crate::{
        DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        rpc::send_create_delegation_token,
        support::{
            sasl::delegation_token_defaults,
            tls::{
                CertificateDer, ClientAuthMode, ClientConfig, PemObject as _, PinnedCertVerifier,
                ServerName, TcpStream, TlsConfig, TlsConnector,
            },
        },
    },
    assert2::check,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, SslPrincipalMapper, config::ListenerSpec},
    krabka_protocol::owned::{
        create_delegation_token_request::CreateDelegationTokenRequest,
        create_delegation_token_response::CreateDelegationTokenResponse,
    },
    krabka_security::ListenerProtocol,
    std::{fs, net::SocketAddr, sync::Arc},
    tempfile::TempDir,
    tokio_rustls::{client::TlsStream, rustls::crypto::aws_lc_rs::default_provider},
};

const DEV_CERT: &str = include_str!("../fixtures/security/dev_cert.pem");
const DEV_KEY: &str = include_str!("../fixtures/security/dev_key.pem");

/// A running broker, its log and certificate directories, and its listen
/// address.
struct StartedBroker {
    handle: BrokerHandle,
    log_dir: TempDir,
    pem_dir: TempDir,
    addr: SocketAddr,
}

/// Boots a single-broker cluster whose only listener speaks `protocol`, with
/// the delegation-token master key set — so the handler reaches the admission
/// check instead of answering `DELEGATION_TOKEN_AUTH_DISABLED` — and with
/// `super_users` seeded from the caller.
///
/// The TLS listener sets `client_auth: Disabled`, so a client that presents
/// no certificate completes the handshake and reaches the broker as
/// `ANONYMOUS`: the one-way-SSL case Kafka refuses.
async fn start_broker(protocol: ListenerProtocol, super_users: &[&str]) -> StartedBroker {
    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let cert_path = pem_dir.path().join("cert.pem");
    let key_path = pem_dir.path().join("key.pem");
    fs::write(&cert_path, DEV_CERT).unwrap();
    fs::write(&key_path, DEV_KEY).unwrap();

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "PUBLIC".to_owned(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_owned(),
        protocol,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "PUBLIC".to_owned();
    if protocol == ListenerProtocol::Ssl {
        cfg.tls_config = Some(TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        });
    }
    for user in super_users {
        cfg.super_users.insert((*user).to_owned());
    }
    delegation_token_defaults(&mut cfg, b"anon-master-key");

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    StartedBroker {
        handle,
        log_dir,
        pem_dir,
        addr,
    }
}

/// Opens a one-way TLS session — no client certificate — to `addr`.
async fn tls_connect_without_client_cert(addr: SocketAddr) -> TlsStream<TcpStream> {
    let pinned = CertificateDer::pem_slice_iter(DEV_CERT.as_bytes())
        .next()
        .expect("dev cert holds one PEM block")
        .expect("dev cert parses");
    let client_cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
            pinned,
            schemes: default_provider()
                .signature_verification_algorithms
                .supported_schemes(),
            mismatch: "presented cert does not match pinned dev cert",
        }))
        .with_no_client_auth();
    let tcp = TcpStream::connect(addr).await.expect("TCP connect");
    TlsConnector::from(Arc::new(client_cfg))
        .connect(ServerName::try_from("krabka-dev").unwrap(), tcp)
        .await
        .expect("TLS handshake must succeed without a client certificate")
}

struct AnonymousTokenRequest {
    broker: Option<StartedBroker>,
    protocol: Option<ListenerProtocol>,
    response: Option<CreateDelegationTokenResponse>,
}
#[rstest::fixture]
fn anonymous() -> AnonymousTokenRequest {
    // Start this scenario with fresh inputs and observations.

    AnonymousTokenRequest {
        broker: None,
        protocol: None,
        response: None,
    }
}
#[rstest_bdd_macros::given(
    "an unauthenticated {protocol} token listener has configured super user {super_user}"
)]
async fn anonymous_token_listener(
    anonymous: &mut AnonymousTokenRequest,
    protocol: String,
    super_user: String,
) {
    let _ = default_provider().install_default();
    let protocol = match protocol.as_str() {
        "PLAINTEXT" => ListenerProtocol::Plaintext,
        "SSL" => ListenerProtocol::Ssl,
        other => panic!("unsupported anonymous token listener {other}"),
    };
    let users = if super_user == "none" {
        Vec::new()
    } else {
        vec![super_user.as_str()]
    };
    anonymous.broker = Some(start_broker(protocol, &users).await);
    anonymous.protocol = Some(protocol);
}
#[rstest_bdd_macros::when(
    "a connection with no SASL identity or client certificate creates a token with lifetime -1"
)]
async fn anonymous_token_creation(anonymous: &mut AnonymousTokenRequest) {
    let addr = anonymous
        .broker
        .as_ref()
        .expect("anonymous token broker started")
        .addr;
    let request = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        ..Default::default()
    };
    anonymous.response = Some(
        if anonymous.protocol.expect("listener configured") == ListenerProtocol::Ssl {
            let mut stream = tls_connect_without_client_cert(addr).await;
            send_create_delegation_token(&mut stream, 1, &request)
                .await
                .expect("CreateDelegationToken")
        } else {
            let mut stream = TcpStream::connect(addr).await.expect("TCP connect");
            send_create_delegation_token(&mut stream, 1, &request)
                .await
                .expect("CreateDelegationToken")
        },
    );
}
#[rstest_bdd_macros::then("the anonymous token request is refused with error 64")]
fn anonymous_token_refused(anonymous: &AnonymousTokenRequest) {
    check!(
        anonymous
            .response
            .as_ref()
            .expect("anonymous mint attempted")
            .error_code
            == DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/delegation_tokens/unauthenticated.feature",
    name = "unauthenticated create delegation token is refused"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unauthenticated_create_delegation_token_is_refused(
    anonymous: AnonymousTokenRequest,
    protocol: String,
    super_user: String,
) {
    let _ = (protocol, super_user);
    let StartedBroker {
        handle,
        log_dir: _log,
        pem_dir: _pem,
        ..
    } = anonymous.broker.expect("anonymous token broker started");
    handle.shutdown().await;
}
