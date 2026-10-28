//! `InterBrokerClient` outbound SASL.
//!
//! The client runs `SaslHandshake` and `SaslAuthenticate` on its own and
//! hands back a stream that still serves ordinary RPCs. The drive helper
//! here proves that with an `ApiVersions` round-trip over the post-auth
//! stream, and `gssapi` reuses it for the Kerberos path.

use {
    crate::{
        harness::admin_plain_password,
        kafka_wire::encode_named,
        support::{
            sasl::sasl_temp_config,
            wire::{decode_response_frame, read_frame, request_frame, write_frame},
        },
    },
    krabka_broker::{
        Broker, BrokerHandle, config::InterBrokerCredentials, network::client::InterBrokerClient,
    },
    krabka_protocol::owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    krabka_units::{ByteSize, convert::ByteSizeExt},
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::{io, net::SocketAddr},
    tempfile::TempDir,
};

/// Drive `InterBrokerClient::connect` and run one `ApiVersions` round-trip
/// over the post-auth stream to prove that the stream survives.
///
/// The helper works with any mechanism. It dials `localhost:<port>` over
/// `SaslPlaintext`, so a GSSAPI SPN resolves to `kafka/localhost`. It then
/// asserts that the post-auth stream works.
pub async fn drive_inter_broker_client_then_apiversions(
    client: &InterBrokerClient,
    addr: SocketAddr,
) -> Result<(), io::Error> {
    let options = krabka_client_core::ConnectionOptions {
        client_id: "krabka-t16-test".to_owned(),
        ..Default::default()
    };
    let mut stream = client
        .connect(
            &addr.ip().to_string(),
            addr.port(),
            ListenerProtocol::SaslPlaintext,
            "localhost",
            &options,
        )
        .await
        .map_err(|e| io::Error::other(format!("InterBrokerClient::connect: {e}")))?;

    // Build an ApiVersions v0 request, frame it, send it through the
    // authenticated stream, decode the response. This proves (a) the
    // client returned a usable stream and (b) the broker treats the
    // stream as fully authenticated.
    let av_req = ApiVersionsRequest::default();
    let av_body = encode_named(&av_req, 0, "ApiVersions")?;

    let frame = request_frame()
        .correlation(99)
        .client_id("krabka-t16-test")
        .body(&av_body)
        .maybe_capacity(Some(<ByteSize as ByteSizeExt>::from_bytes(
            u64::try_from(16 + av_body.len()).expect("frame capacity fits u64"),
        )))
        .call();

    write_frame(&mut stream, &frame, None).await?;

    let resp = read_frame(&mut stream).await?;

    // Non-flexible response: header is v0 (just corr_id).
    let _av_resp: ApiVersionsResponse =
        decode_response_frame(&resp, 0, false, "ApiVersions decode")?;
    Ok(())
}

struct OutboundAuthentication {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<InterBrokerClient>,
    result: Option<io::Result<()>>,
}

#[fixture]
fn outbound() -> OutboundAuthentication {
    // Start this scenario with fresh inputs and observations.

    OutboundAuthentication {
        broker: None,
        directory: None,
        client: None,
        result: None,
    }
}

#[given("a SASL_PLAINTEXT peer accepting the broker PLAIN principal")]
async fn plain_peer(outbound: &mut OutboundAuthentication) {
    let (directory, mut config) = sasl_temp_config(vec![SaslMechanism::Plain]);
    config
        .plain_credentials
        .insert("broker".to_owned(), admin_plain_password());
    outbound.broker = Some(Broker::start(config).await.expect("broker must start"));
    outbound.directory = Some(directory);
}

#[given("InterBrokerClient is configured with that principal's PLAIN credentials")]
fn outbound_plain_credentials(outbound: &mut OutboundAuthentication) {
    outbound.client = Some(InterBrokerClient::new(
        None,
        Some(InterBrokerCredentials::Plain {
            username: "broker".to_owned(),
            password: admin_plain_password(),
        }),
    ));
}

#[when("InterBrokerClient connects and sends ApiVersions on the returned authenticated stream")]
async fn connect_plain_peer(outbound: &mut OutboundAuthentication) {
    let client = outbound.client.as_ref().expect("an outbound PLAIN client");
    let address = outbound
        .broker
        .as_ref()
        .expect("a SASL peer broker")
        .listen_addr();
    outbound.result = Some(drive_inter_broker_client_then_apiversions(client, address).await);
}

#[then("the authenticated inter-broker stream returns a decodable ApiVersions response")]
fn usable_outbound_stream(outbound: &OutboundAuthentication) {
    outbound
        .result
        .as_ref()
        .expect("InterBrokerClient connected and requested ApiVersions")
        .as_ref()
        .expect("InterBrokerClient PLAIN auth + ApiVersions round-trip must succeed");
}

/// Start a broker with a `SASL_PLAINTEXT` listener and one PLAIN credential,
/// then dial it with the public `InterBrokerClient` API.
///
/// The client must run `SaslHandshake` and `SaslAuthenticate` on its own, and
/// it must return a stream that the caller can keep using for normal RPCs.
/// The test proves this: it sends an `ApiVersions` request over the returned
/// stream and decodes the response.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/inter_broker.feature",
    name = "inter broker client authenticates via plain"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inter_broker_client_authenticates_via_plain(outbound: OutboundAuthentication) {
    outbound
        .broker
        .expect("a SASL peer broker")
        .shutdown()
        .await;
}
