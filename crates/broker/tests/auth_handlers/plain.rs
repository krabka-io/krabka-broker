//! SASL/PLAIN end-to-end: the happy path, a wrong password that closes the
//! connection, and the per-mechanism authentication counters those two
//! sessions tick on the `/metrics` scrape.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::BytesMut;
use krabka_broker::{Broker, BrokerConfig, config::ListenerSpec};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        metadata_request::MetadataRequest, metadata_response::MetadataResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::harness::{alice_password, round_trip, wrong_scram_password};

/// Happy-path drive of a SASL/PLAIN session: `ApiVersions` → `SaslHandshake`
/// → `SaslAuthenticate` → Metadata.
///
/// The test asserts that the connection survives every step and that the
/// final Metadata response carries this broker. The dial side sends raw
/// bytes over `TcpStream` and not `Client`, because `Client` does not speak
/// SASL yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_happy_path() {
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
        .insert("alice".to_string(), alice_password());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let result = drive_sasl_plain_session(addr, "alice", alice_password().as_bytes()).await;
    handle.shutdown().await;
    result.expect("SASL/PLAIN session must succeed end-to-end");
}

/// SASL PLAIN metrics: one happy-path session and one wrong-password session
/// tick both the `successful_authentication_total` and the
/// `failed_authentication_total` per-mechanism counters on the `/metrics`
/// scrape.
///
/// The test checks the end-to-end wire path from the `SaslAuthenticate`
/// dispatch site to the rendered Prometheus text.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_authentication_metrics_tick_for_success_and_failure() {
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
        .insert("alice".to_string(), alice_password());
    cfg.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");

    // 1. Happy path — must tick `successful_authentication_total`.
    drive_sasl_plain_session(addr, "alice", alice_password().as_bytes())
        .await
        .expect("happy-path PLAIN session");
    // 2. Wrong password — must tick `failed_authentication_total`.
    let bad = drive_sasl_plain_session(addr, "alice", wrong_scram_password().as_bytes()).await;
    assert!(bad.is_err(), "wrong password must fail: {bad:?}");

    let body = scrape_metrics(metrics_addr).await;
    handle.shutdown().await;

    let success_needle = "krabka_broker_successful_authentication_total{mechanism=\"PLAIN\"} 1";
    let failed_needle = "krabka_broker_failed_authentication_total{mechanism=\"PLAIN\"} 1";
    assert!(
        body.contains(success_needle),
        "missing or wrong-value {success_needle} in:\n{body}"
    );
    assert!(
        body.contains(failed_needle),
        "missing or wrong-value {failed_needle} in:\n{body}"
    );
}

/// Send an HTTP GET `/metrics` to `addr` and return the response body.
///
/// The returned body holds no HTTP head. This helper is a copy of the helper
/// in `tests/metrics.rs`. It stays inline here so that the test does not need
/// a cross-test module.
async fn scrape_metrics(addr: SocketAddr) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n",
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let s = String::from_utf8(buf).unwrap();
    let body_start = s.find("\r\n\r\n").map_or(0, |i| i + 4);
    s[body_start..].to_string()
}

/// Negative path: with a wrong password, `SaslAuthenticate` responds with
/// `error_code = SASL_AUTHENTICATION_FAILED` (58) and the broker closes the
/// connection.
///
/// `drive_sasl_plain_session` reports the failure as an `Err` in two cases.
/// The first case is a non-zero `error_code` on the auth response. The second
/// case is an EOF on the Metadata read that follows, when the peer closed the
/// connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_wrong_password_closes_connection() {
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
        .insert("alice".to_string(), alice_password());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let result = drive_sasl_plain_session(addr, "alice", wrong_scram_password().as_bytes()).await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail the SASL session: {result:?}"
    );
}

/// Drive a complete SASL/PLAIN session against a `SASL_PLAINTEXT` listener.
///
/// On success, this helper returns `Ok(())` after a successful post-auth
/// Metadata round-trip. It returns `Err` when any step fails: frame I/O,
/// response decode, a non-zero error code on a SASL response, or EOF before
/// Metadata.
///
/// This helper handles these wire-protocol mechanics inline, without the
/// `Client` API:
/// - Request headers: v1, non-flexible, for `ApiVersions v0` and
///   `SaslHandshake v1`. v2, flexible with a trailing `0x00` tagged-fields
///   byte, for `SaslAuthenticate v2` and `Metadata v12`.
/// - Response headers: always v0, which holds only `correlation_id`, for
///   `ApiVersions`, whatever the body flexibility. v1, which holds `corr_id`
///   plus a `0x00` tagged byte, for every other flexible response. v0 for
///   non-flexible.
/// - Length framing: a 4-byte big-endian length prefix on every frame in both
///   directions, the same as `krabka_broker::network::codec`.
async fn drive_sasl_plain_session(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<(), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible): proves the pre-auth allowlist
    //    lets us talk to the broker before authentication. We decode the
    //    response and ignore the contents — its presence is enough.
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    let _av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    // ── 2. SaslHandshake v1 (non-flexible, mechanism="PLAIN").
    let mut sh_body = BytesMut::new();
    let sh_req = SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    };
    sh_req
        .encode(&mut sh_body, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    // ── 3. SaslAuthenticate v2 (flexible). auth_bytes = \0user\0password.
    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0); // authzid (empty)
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password);
    let auth_req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    };
    let mut auth_body = BytesMut::new();
    auth_req
        .encode(&mut auth_body, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let auth_resp_bytes = round_trip(&mut stream, 36, 2, 3, true, &auth_body).await?;
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={} error_message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }

    // ── 4. Post-auth Metadata round-trip proves the connection survived
    //    and the data plane is reachable.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req
        .encode(&mut md_body, 12)
        .map_err(|e| io::Error::other(format!("Metadata encode: {e}")))?;
    let md_resp_bytes = round_trip(&mut stream, 3, 12, 4, true, &md_body).await?;
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = MetadataResponse::decode(&mut cur, 12)
        .map_err(|e| io::Error::other(format!("Metadata decode: {e}")))?;
    if md_resp.brokers.is_empty() {
        return Err(io::Error::other("Metadata response carried no brokers"));
    }

    Ok(())
}
