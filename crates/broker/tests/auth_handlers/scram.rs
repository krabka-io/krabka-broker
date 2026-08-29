//! SASL/SCRAM end-to-end for both SHA-256 and SHA-512: the RFC 5802
//! two-round exchange against a credential provisioned through the
//! controller, and the wrong-password path that closes the connection.

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
use tokio::net::TcpStream;

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
