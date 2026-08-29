//! `InterBrokerClient` outbound SASL.
//!
//! The client runs `SaslHandshake` and `SaslAuthenticate` on its own and
//! hands back a stream that still serves ordinary RPCs. The drive helper
//! here proves that with an `ApiVersions` round-trip over the post-auth
//! stream, and `gssapi` reuses it for the Kerberos path.

use std::{io, net::SocketAddr};

use bytes::{Buf, BufMut, BytesMut};
use krabka_broker::{Broker, BrokerConfig, config::ListenerSpec};
use krabka_protocol::{
    Decode, Encode,
    owned::{api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse},
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::harness::admin_plain_password;

/// Start a broker with a `SASL_PLAINTEXT` listener and one PLAIN credential,
/// then dial it with the public `InterBrokerClient` API.
///
/// The client must run `SaslHandshake` and `SaslAuthenticate` on its own, and
/// it must return a stream that the caller can keep using for normal RPCs.
/// The test proves this: it sends an `ApiVersions` request over the returned
/// stream and decodes the response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inter_broker_client_authenticates_via_plain() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("broker".to_string(), admin_plain_password());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let client = krabka_broker::network::client::InterBrokerClient::new(
        None,
        Some(krabka_broker::config::InterBrokerCredentials::Plain {
            username: "broker".to_string(),
            password: admin_plain_password(),
        }),
    );

    let result = drive_inter_broker_client_then_apiversions(&client, addr).await;
    handle.shutdown().await;
    result.expect("InterBrokerClient PLAIN auth + ApiVersions round-trip must succeed");
}

/// Drive `InterBrokerClient::connect` and run one `ApiVersions` round-trip
/// over the post-auth stream to prove that the stream survives.
///
/// The helper works with any mechanism. It dials `localhost:<port>` over
/// `SaslPlaintext`, so a GSSAPI SPN resolves to `kafka/localhost`. It then
/// asserts that the post-auth stream works.
pub async fn drive_inter_broker_client_then_apiversions(
    client: &krabka_broker::network::client::InterBrokerClient,
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
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;

    let mut frame = BytesMut::with_capacity(16 + av_body.len());
    frame.put_i16(18); // api_key = ApiVersions
    frame.put_i16(0); // api_version
    frame.put_i32(99); // post-auth correlation id (distinct from auth ones)
    let client_id = "krabka-t16-test";
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    // ApiVersions v0 is non-flexible → no tagged-fields byte.
    frame.put_slice(&av_body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    // Non-flexible response: header is v0 (just corr_id).
    let mut cur = &resp[..];
    let _corr = cur.get_i32();
    let _av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;
    Ok(())
}
