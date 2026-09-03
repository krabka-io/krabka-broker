//! SASL/SCRAM end-to-end for both SHA-256 and SHA-512: the RFC 5802
//! two-round exchange against a credential provisioned through the
//! controller, and the wrong-password path that closes the connection.

use std::{io, net::SocketAddr};

use assert2::{assert, check};
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
use tokio::{io::AsyncReadExt, net::TcpStream};

use crate::harness::{alice_password, round_trip, wrong_scram_password};

/// Happy-path drive of a SASL/SCRAM-SHA-512 session.
///
/// The test provisions a credential for "alice" with the shared test
/// password. It goes through the controller directly and not through the
/// public `AlterUserScramCredentials` handler. It then runs the two-round
/// RFC 5802 exchange end-to-end and asserts that the post-auth Metadata
/// request succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_happy_path() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];

    let handle = Broker::start(cfg).await.expect("broker must start");

    // Provision alice/wonderland directly via the controller, rather than
    // through the public path (AlterUserScramCredentials, api_key 51).
    let cred = krabka_security::hash_scram_password(
        alice_password().as_bytes(),
        SaslMechanism::ScramSha512,
        4096,
    );
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1ScramCredential(
            krabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
                salt: cred.salt,
                stored_key: cred.stored_key,
                server_key: cred.server_key,
                iterations: cred.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");

    let addr = handle.listen_addr();
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha512)
            .await;
    handle.shutdown().await;
    result.expect("SASL/SCRAM session must succeed end-to-end");
}

/// Negative path: with a wrong password, `SaslAuthenticate` round 2 responds
/// with `error_code = 58`, which is `SASL_AUTHENTICATION_FAILED`, and the
/// broker closes the connection.
///
/// `drive_sasl_scram_session` reports the failure as a non-zero error code on
/// the auth response, or as an EOF when the Metadata read that follows
/// returns no bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_wrong_password_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];

    let handle = Broker::start(cfg).await.expect("broker must start");

    let cred = krabka_security::hash_scram_password(
        alice_password().as_bytes(),
        SaslMechanism::ScramSha512,
        4096,
    );
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1ScramCredential(
            krabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
                salt: cred.salt,
                stored_key: cred.stored_key,
                server_key: cred.server_key,
                iterations: cred.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");

    let addr = handle.listen_addr();
    let result = drive_sasl_scram_session(
        addr,
        "alice",
        &wrong_scram_password(),
        SaslMechanism::ScramSha512,
    )
    .await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail SCRAM session: {result:?}"
    );
}

/// SASL/SCRAM-SHA-256 happy path.
///
/// The test is a copy of the SHA-512 test, but it provisions a SHA-256
/// credential and configures the listener to enable only SHA-256. This proves
/// that the new mechanism is wired end-to-end, and that it does not use the
/// SHA-512 code by accident.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha256_happy_path() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha256];

    let handle = Broker::start(cfg).await.expect("broker must start");

    let cred = krabka_security::hash_scram_password(
        alice_password().as_bytes(),
        SaslMechanism::ScramSha256,
        4096,
    );
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1ScramCredential(
            krabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha256,
                salt: cred.salt,
                stored_key: cred.stored_key,
                server_key: cred.server_key,
                iterations: cred.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");

    let addr = handle.listen_addr();
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha256)
            .await;
    handle.shutdown().await;
    result.expect("SASL/SCRAM-SHA-256 session must succeed end-to-end");
}

/// Negative path for SHA-256: a wrong password must close the connection,
/// the same as in the SHA-512 variant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha256_wrong_password_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha256];

    let handle = Broker::start(cfg).await.expect("broker must start");

    let cred = krabka_security::hash_scram_password(
        alice_password().as_bytes(),
        SaslMechanism::ScramSha256,
        4096,
    );
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1ScramCredential(
            krabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha256,
                salt: cred.salt,
                stored_key: cred.stored_key,
                server_key: cred.server_key,
                iterations: cred.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");

    let addr = handle.listen_addr();
    let result = drive_sasl_scram_session(
        addr,
        "alice",
        &wrong_scram_password(),
        SaslMechanism::ScramSha256,
    )
    .await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail SHA-256 SCRAM session: {result:?}"
    );
}

/// Drive a complete SASL/SCRAM session against a `SASL_PLAINTEXT` listener.
///
/// This helper works for both SHA-256 and SHA-512. It passes the mechanism
/// through to the handshake and to the client state machine.
///
/// On success it returns `Ok(())` after a successful post-auth Metadata
/// round-trip. It returns `Err` when any step fails: a non-zero error code on
/// either of the two `SaslAuthenticate` rounds, a server-final signature
/// mismatch in the client-side proof, or EOF before Metadata returns.
pub async fn drive_sasl_scram_session(
    addr: SocketAddr,
    user: &str,
    password: &str,
    mechanism: krabka_security::SaslMechanism,
) -> Result<(), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible). Same as PLAIN: pre-auth allowlist.
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    let _av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    // ── 2. SaslHandshake v1 (non-flexible).
    let mut sh_body = BytesMut::new();
    let sh_req = SaslHandshakeRequest {
        mechanism: mechanism.wire_name().to_string(),
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

    // ── 3. SCRAM client-first → server-first.
    let client = krabka_security::ScramClientExchange::new(
        user.to_string(),
        password.as_bytes().to_vec(),
        mechanism,
    );
    let (client_first, client) = client
        .client_first()
        .map_err(|e| io::Error::other(format!("scram client_first: {e:?}")))?;
    let scram_req_first = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_first),
        ..Default::default()
    };
    let mut scram_body_first = BytesMut::new();
    scram_req_first
        .encode(&mut scram_body_first, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) encode: {e}")))?;
    let scram_first_response_bytes =
        round_trip(&mut stream, 36, 2, 3, true, &scram_body_first).await?;
    let mut cur: &[u8] = &scram_first_response_bytes;
    let scram_first_response = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) decode: {e}")))?;
    if scram_first_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate round 1 failed: error_code={} error_message={:?}",
            scram_first_response.error_code, scram_first_response.error_message
        )));
    }
    let server_first = scram_first_response.auth_bytes.to_vec();

    // ── 4. SCRAM client-final → server-final.
    let (client_final, client) = client
        .step(&server_first)
        .map_err(|e| io::Error::other(format!("scram client step: {e:?}")))?;
    let scram_req_final = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_final),
        ..Default::default()
    };
    let mut scram_body_final = BytesMut::new();
    scram_req_final
        .encode(&mut scram_body_final, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) encode: {e}")))?;
    let scram_final_response_bytes =
        round_trip(&mut stream, 36, 2, 4, true, &scram_body_final).await?;
    let mut cur: &[u8] = &scram_final_response_bytes;
    let scram_final_response = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) decode: {e}")))?;
    if scram_final_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate round 2 failed: error_code={} error_message={:?}",
            scram_final_response.error_code, scram_final_response.error_message
        )));
    }
    // Client verifies server signature — proves the broker holds the
    // expected `server_key` rather than just any matching `stored_key`.
    client
        .verify_server_final(&scram_final_response.auth_bytes)
        .map_err(|e| io::Error::other(format!("server-final verify: {e:?}")))?;

    // ── 5. Post-auth Metadata round-trip proves the connection survived
    //    and the data plane is reachable.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req
        .encode(&mut md_body, 12)
        .map_err(|e| io::Error::other(format!("Metadata encode: {e}")))?;
    let md_resp_bytes = round_trip(&mut stream, 3, 12, 5, true, &md_body).await?;
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = MetadataResponse::decode(&mut cur, 12)
        .map_err(|e| io::Error::other(format!("Metadata decode: {e}")))?;
    if md_resp.brokers.is_empty() {
        return Err(io::Error::other("Metadata response carried no brokers"));
    }

    Ok(())
}

/// Runs the two RFC 5802 rounds on `stream` and returns the round-2
/// response, whose `session_lifetime_ms` is the KIP-368 window.
async fn scram_authenticate(
    stream: &mut TcpStream,
    corr: &mut i32,
    user: &str,
    password: &str,
    mechanism: SaslMechanism,
) -> Result<SaslAuthenticateResponse, io::Error> {
    let sh_req = SaslHandshakeRequest {
        mechanism: mechanism.wire_name().to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req
        .encode(&mut sh_body, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    *corr += 1;
    let sh_resp_bytes = round_trip(stream, 17, 1, *corr, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let client = krabka_security::ScramClientExchange::new(
        user.to_string(),
        password.as_bytes().to_vec(),
        mechanism,
    );
    let (client_first, client) = client
        .client_first()
        .map_err(|e| io::Error::other(format!("scram client_first: {e:?}")))?;
    let mut body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_first),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) encode: {e}")))?;
    *corr += 1;
    let first_bytes = round_trip(stream, 36, 2, *corr, true, &body).await?;
    let mut cur: &[u8] = &first_bytes;
    let first = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) decode: {e}")))?;
    if first.error_code != 0 {
        return Ok(first);
    }

    let (client_final, _client) = client
        .step(&first.auth_bytes)
        .map_err(|e| io::Error::other(format!("scram client step: {e:?}")))?;
    let mut body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_final),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) encode: {e}")))?;
    *corr += 1;
    let final_bytes = round_trip(stream, 36, 2, *corr, true, &body).await?;
    let mut cur: &[u8] = &final_bytes;
    SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) decode: {e}")))
}

/// A `SASL_PLAINTEXT` broker serving SCRAM-SHA-512 for alice and bob, with the
/// KIP-368 window set to `max_reauth` and the idle window switched off.
async fn start_scram_reauth_broker(
    log_dir: &std::path::Path,
    max_reauth: krabka_units::Time,
) -> krabka_broker::BrokerHandle {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.connections_max_idle = Some(krabka_units::millis(0));
    cfg.connections_max_reauth = Some(max_reauth);
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];
    let handle = Broker::start(cfg).await.expect("broker must start");
    for user in ["alice", "bob"] {
        let cred = krabka_security::hash_scram_password(
            alice_password().as_bytes(),
            SaslMechanism::ScramSha512,
            4096,
        );
        handle
            .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1ScramCredential(
                krabka_metadata::ScramCredentialRecord {
                    user: user.into(),
                    mechanism: SaslMechanism::ScramSha512,
                    salt: cred.salt,
                    stored_key: cred.stored_key,
                    server_key: cred.server_key,
                    iterations: cred.iterations,
                },
            ))
            .await
            .expect("submit V1ScramCredential");
    }
    handle
}

/// KIP-368: a SCRAM session under `connections.max.reauth.ms` reports the
/// window as `session_lifetime_ms`, and the broker closes the connection when
/// it elapses without an in-band re-authentication.
#[tokio::test(flavor = "current_thread")]
async fn scram_session_capped_by_connections_max_reauth_then_closes() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_scram_reauth_broker(log_dir.path(), krabka_units::secs(30)).await;
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut corr = 0;
    let resp = scram_authenticate(
        &mut stream,
        &mut corr,
        "alice",
        &alice_password(),
        SaslMechanism::ScramSha512,
    )
    .await
    .expect("SCRAM authenticate round-trips");
    check!(resp.error_code == 0);
    check!(
        (29_000..=30_000).contains(&resp.session_lifetime_ms),
        "session_lifetime_ms = {}, expected the 30_000 ms cap",
        resp.session_lifetime_ms
    );

    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    tokio::time::resume();

    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error");
    check!(
        n == 0,
        "expected EOF after the re-auth window, got {n} bytes"
    );

    handle.shutdown().await;
}

/// KIP-368: a SCRAM re-auth for the same principal runs both rounds in the
/// `Reauthenticating` state and re-arms the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_in_band_reauth_same_principal_reopens_data_plane() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_scram_reauth_broker(log_dir.path(), krabka_units::secs(30)).await;
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut corr = 0;
    scram_authenticate(
        &mut stream,
        &mut corr,
        "alice",
        &alice_password(),
        SaslMechanism::ScramSha512,
    )
    .await
    .expect("initial SCRAM authenticate");
    let reauth = scram_authenticate(
        &mut stream,
        &mut corr,
        "alice",
        &alice_password(),
        SaslMechanism::ScramSha512,
    )
    .await
    .expect("in-band SCRAM re-auth round-trips");
    check!(reauth.error_code == 0, "in-band re-auth must succeed");
    check!(
        (29_000..=30_000).contains(&reauth.session_lifetime_ms),
        "re-auth must re-arm the window, got {}",
        reauth.session_lifetime_ms
    );

    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req.encode(&mut md_body, 12).unwrap();
    corr += 1;
    let md_resp_bytes = round_trip(&mut stream, 3, 12, corr, true, &md_body)
        .await
        .expect("Metadata RPC after in-band re-auth");
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = MetadataResponse::decode(&mut cur, 12).unwrap();
    check!(!md_resp.brokers.is_empty());

    handle.shutdown().await;
}

/// KIP-368 forbids a principal switch mid-connection: a SCRAM re-auth as a
/// different user answers `SASL_AUTHENTICATION_FAILED` (58) and closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_in_band_reauth_with_different_principal_closes() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_scram_reauth_broker(log_dir.path(), krabka_units::secs(30)).await;
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut corr = 0;
    scram_authenticate(
        &mut stream,
        &mut corr,
        "alice",
        &alice_password(),
        SaslMechanism::ScramSha512,
    )
    .await
    .expect("initial SCRAM authenticate");
    let reauth = scram_authenticate(
        &mut stream,
        &mut corr,
        "bob",
        &alice_password(),
        SaslMechanism::ScramSha512,
    )
    .await
    .expect("re-auth round-trips");
    check!(
        reauth.error_code == 58,
        "expected SASL_AUTHENTICATION_FAILED, got {}",
        reauth.error_code
    );

    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error");
    check!(n == 0, "expected EOF after a refused re-auth");

    handle.shutdown().await;
}
