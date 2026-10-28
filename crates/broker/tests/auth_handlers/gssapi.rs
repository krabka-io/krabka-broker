//! SASL/GSSAPI.
//!
//! One case proves the mechanism is wired through the handshake
//! advertisement and that the broker does not touch the keytab before the
//! first `SaslAuthenticate` round; it needs no KDC. The other runs a full
//! inter-broker initiate from a keytab against the MIT KDC fixture and is
//! `#[ignore]`d by default.

use {
    crate::{
        harness::{round_trip, start_broker_stream},
        inter_broker::drive_inter_broker_client_then_apiversions,
        support::{
            gssapi::broker_config,
            manifest_dir,
            sasl::sasl_temp_config,
            wire::{CorrelationId as WireCorrelationId, SocketExchangeSetup},
        },
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_broker::{config::InterBrokerCredentials, network::client::InterBrokerClient},
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
            sasl_handshake_request::SaslHandshakeRequest,
            sasl_handshake_response::SaslHandshakeResponse,
        },
    },
    krabka_security::{SaslMechanism, gssapi},
    std::{env, io},
    tokio::net::TcpStream,
};

struct KerberosPeer {
    directory: Option<tempfile::TempDir>,
    broker: Option<krabka_broker::BrokerHandle>,
    stream: Option<TcpStream>,
    credentials: Option<InterBrokerCredentials>,
    handshake: Option<SaslHandshakeResponse>,
    authenticated: Option<io::Result<()>>,
}

#[rstest::fixture]
fn kerberos() -> KerberosPeer {
    // Start this scenario with fresh inputs and observations.

    KerberosPeer {
        directory: None,
        broker: None,
        stream: None,
        credentials: None,
        handshake: None,
        authenticated: None,
    }
}

#[rstest_bdd_macros::given("a GSSAPI listener without a running KDC")]
async fn kerberos_handshake_listener(kerberos: &mut KerberosPeer) {
    let (directory, mut cfg) = sasl_temp_config(vec![SaslMechanism::Gssapi]);
    cfg.gssapi = Some(krabka_broker::GssapiConfig {
        keytab_path: manifest_dir().join("tests/fixtures/security/kdc/kafka.keytab"),
        service_name: "kafka".to_owned(),
        principal_to_local_rules: vec![],
        realm: Some("CRABKA.TEST".to_owned()),
        kdc: None,
        max_time_skew: gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
    });
    let peer = start_broker_stream(cfg).await;
    kerberos.directory = Some(directory);
    kerberos.broker = Some(peer.broker);
    kerberos.stream = Some(peer.stream);
}

#[rstest_bdd_macros::when("an unauthenticated client discovers APIs and selects GSSAPI")]
async fn select_kerberos(kerberos: &mut KerberosPeer) {
    let stream = kerberos.stream.as_mut().expect("peer connection");
    let mut body = BytesMut::new();
    ApiVersionsRequest::default().encode(&mut body, 0).unwrap();
    let response = round_trip(
        stream,
        SocketExchangeSetup {
            correlation: WireCorrelationId(0),
            ..Default::default()
        },
        &body,
    )
    .await
    .unwrap();
    ApiVersionsResponse::decode(&mut &response[..], 0).unwrap();
    let mut body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "GSSAPI".to_owned(),
        ..Default::default()
    }
    .encode(&mut body, 1)
    .unwrap();
    let response = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(1),
            ..Default::default()
        },
        &body,
    )
    .await
    .unwrap();
    kerberos.handshake = Some(SaslHandshakeResponse::decode(&mut &response[..], 1).unwrap());
}

#[rstest_bdd_macros::then(
    "GSSAPI selection succeeds and advertises GSSAPI without contacting a KDC"
)]
fn kerberos_advertised(kerberos: &KerberosPeer) {
    let response = kerberos.handshake.as_ref().expect("mechanism selected");
    assert!(response.error_code == 0);
    assert!(
        response
            .mechanisms
            .iter()
            .any(|mechanism| mechanism == "GSSAPI")
    );
}

#[rstest_bdd_macros::given("a live MIT KDC and broker and alice service keytabs")]
async fn live_kerberos_peer(kerberos: &mut KerberosPeer) {
    let fixtures = manifest_dir().join("tests/fixtures/security/kdc");
    let kdc_url = env::var("SSPI_KDC_URL").unwrap_or_else(|_| "tcp://localhost:88".to_owned());
    let (directory, mut cfg) = sasl_temp_config(vec![SaslMechanism::Gssapi]);
    cfg.gssapi = Some(broker_config(
        fixtures.join("kafka.keytab"),
        kdc_url.clone(),
    ));
    kerberos.broker = Some(
        krabka_broker::Broker::start(cfg)
            .await
            .expect("broker must start"),
    );
    kerberos.directory = Some(directory);
    kerberos.credentials = Some(InterBrokerCredentials::Gssapi {
        keytab_path: fixtures.join("alice.keytab"),
        client_principal: "alice@CRABKA.TEST".to_owned(),
        service_name: "kafka".to_owned(),
        kdc_url,
    });
}

#[rstest_bdd_macros::when(
    "the inter broker client authenticates from alice keytab and requests API versions"
)]
async fn kerberos_keytab_authentication(kerberos: &mut KerberosPeer) {
    let client = InterBrokerClient::new(
        None,
        Some(
            kerberos
                .credentials
                .take()
                .expect("alice keytab credentials"),
        ),
    );
    kerberos.authenticated = Some(
        drive_inter_broker_client_then_apiversions(
            &client,
            kerberos.broker.as_ref().expect("GSSAPI peer").listen_addr(),
        )
        .await,
    );
}

#[rstest_bdd_macros::then("the keytab authenticated connection serves API versions")]
fn kerberos_rpc_succeeded(kerberos: &KerberosPeer) {
    assert!(
        kerberos
            .authenticated
            .as_ref()
            .expect("authentication attempted")
            .is_ok()
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/gssapi.feature",
    name = "gssapi handshake advertised when enabled"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gssapi_handshake_advertised_when_enabled(kerberos: KerberosPeer) {
    kerberos.broker.expect("GSSAPI peer").shutdown().await;
}

#[ignore = "requires the MIT KDC fixture (docker compose up) + exported KRB5_CONFIG/SSPI_KDC_URL"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/gssapi.feature",
    name = "gssapi inter broker client authenticates from keytab"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gssapi_inter_broker_client_authenticates_from_keytab(kerberos: KerberosPeer) {
    kerberos.broker.expect("GSSAPI peer").shutdown().await;
}
