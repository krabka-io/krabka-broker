//! The SASL gate on a `SASL_PLAINTEXT` listener.
//!
//! `ApiVersions` is on the pre-auth allowlist and answers before any SASL
//! exchange, `Metadata` is not and the broker closes the connection instead,
//! and a `SaslHandshake` for a mechanism the listener does not enable comes
//! back with the enabled list and leaves the connection open for a retry.

use assert2::{assert, check};
use bytes::{BufMut, BytesMut};
use krabka_broker::{Broker, BrokerConfig, config::ListenerSpec};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        metadata_request::MetadataRequest, sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::harness::round_trip;

/// `ApiVersions`, `api_key` 18, is on the pre-auth allowlist and must succeed
/// without any SASL exchange.
///
/// The response should decode without an error. The supported-api list should
/// include `api_keys` 17, which is `SaslHandshake`, and 36, which is
/// `SaslAuthenticate`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_versions_reachable_pre_auth_on_sasl_listener() {
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

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req.encode(&mut av_body, 0).unwrap();
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body)
        .await
        .expect("ApiVersions must succeed pre-auth on SASL listener");

    let mut cur: &[u8] = &av_resp_bytes;
    let av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .expect("ApiVersionsResponse must decode successfully");

    check!(
        av_resp.error_code == 0,
        "ApiVersions error_code must be 0 on SASL listener pre-auth"
    );
    check!(
        av_resp.api_keys.iter().any(|k| k.api_key == 17),
        "ApiVersionsResponse must list SaslHandshake (17): {:?}",
        av_resp.api_keys
    );
    check!(
        av_resp.api_keys.iter().any(|k| k.api_key == 36),
        "ApiVersionsResponse must list SaslAuthenticate (36): {:?}",
        av_resp.api_keys
    );

    handle.shutdown().await;
}

/// A pre-auth `Metadata` request on a `SASL_PLAINTEXT` listener must not
/// succeed.
///
/// `Metadata`, `api_key` 3, is not on the pre-auth allowlist. T12 closes the
/// TCP connection and does not encode a typed error response. So the read
/// after the `Metadata` request must return an I/O error, either
/// `UnexpectedEof` or a connection reset, and not a well-formed response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_rejected_pre_auth_on_sasl_listener() {
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

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send Metadata (api_key=3, v12, flexible) WITHOUT any auth.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req.encode(&mut md_body, 12).unwrap();

    // Build the frame manually: header + body, then length-prefix.
    let mut frame = BytesMut::with_capacity(32 + md_body.len());
    frame.put_i16(3); // api_key = Metadata
    frame.put_i16(12); // api_version
    frame.put_i32(1); // correlation_id
    let client_id = "krabka-t19-test";
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    frame.put_u8(0); // flexible header tagged-fields
    frame.put_slice(&md_body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .unwrap();
    stream.write_all(&frame).await.unwrap();
    stream.flush().await.unwrap();

    // The broker closes the connection instead of responding — any read
    // attempt must return an error (UnexpectedEof / connection reset).
    let read_result = stream.read_u32().await;
    assert!(
        read_result.is_err(),
        "expected TCP close after pre-auth Metadata, but read succeeded: {read_result:?}"
    );

    handle.shutdown().await;
}

/// A `SaslHandshake` with an unsupported mechanism, GSSAPI, must return
/// `error_code = 33`, which is `UNSUPPORTED_SASL_MECHANISM`, with the enabled
/// list AND keep the connection open.
///
/// A `SaslHandshake` that follows with the supported mechanism, PLAIN, must
/// succeed with `error_code = 0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_mechanism_rejected_but_handshake_retryable() {
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

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // ── 1. SaslHandshake with "GSSAPI" (not in enabled list).
    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "GSSAPI".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .unwrap();
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 1, false, &sh_body)
        .await
        .expect("SaslHandshake(GSSAPI) must get a response (not a TCP close)");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp =
        SaslHandshakeResponse::decode(&mut cur, 1).expect("SaslHandshakeResponse must decode");
    assert!(
        sh_resp.error_code == 33, // UNSUPPORTED_SASL_MECHANISM
        "GSSAPI handshake must return error_code=33, got {:?}",
        sh_resp.error_code
    );
    assert!(
        sh_resp.mechanisms.iter().any(|m| m == "PLAIN"),
        "error response must include the enabled mechanisms list: {:?}",
        sh_resp.mechanisms
    );

    // ── 2. Retry on the SAME connection with "PLAIN" — must succeed.
    let mut plain_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut plain_body, 1)
    .unwrap();
    let plain_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &plain_body)
        .await
        .expect("SaslHandshake(PLAIN) retry must succeed on the same connection");
    let mut plain_cur: &[u8] = &plain_resp_bytes;
    let plain_resp = SaslHandshakeResponse::decode(&mut plain_cur, 1)
        .expect("SaslHandshakeResponse retry must decode");
    assert!(
        plain_resp.error_code == 0,
        "PLAIN handshake retry on same connection must return error_code=0"
    );

    handle.shutdown().await;
}
