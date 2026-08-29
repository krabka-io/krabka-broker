//! `AlterUserScramCredentials` (`api_key` 51, KIP-554) provisioning.
//!
//! A super-user upserts a SCRAM-SHA-256 or SCRAM-SHA-512 credential and the
//! named user then authenticates with it, while a non-super-user is refused
//! per row. The module also owns the KIP-554 wire constants, the
//! PLAIN-authenticated request drive, and the PBKDF2 salting that the
//! validation cases in `alter_scram_validation` reuse.

use std::{io, net::SocketAddr};

use assert2::{assert, check};
use bytes::BytesMut;
use krabka_broker::{Broker, BrokerConfig, authorizer::SimpleAclAuthorizer, config::ListenerSpec};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        alter_user_scram_credentials_request::{
            AlterUserScramCredentialsRequest, ScramCredentialUpsertion,
        },
        alter_user_scram_credentials_response::AlterUserScramCredentialsResponse,
        api_versions_request::ApiVersionsRequest,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tokio::net::TcpStream;

use crate::{
    harness::{admin_plain_password, alice_password, round_trip, wrong_scram_password},
    scram::drive_sasl_scram_session,
};

/// SCRAM mechanism byte on the `AlterUserScramCredentials` wire, from
/// KIP-554. `1` is `SCRAM-SHA-256` and `2` is `SCRAM-SHA-512`.
pub const WIRE_MECH_SCRAM_SHA_256: i8 = 1;
pub const WIRE_MECH_SCRAM_SHA_512: i8 = 2;
pub const KAFKA_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub const KAFKA_DUPLICATE_RESOURCE: i16 = 92;
pub const KAFKA_UNACCEPTABLE_CREDENTIAL: i16 = 93;
pub const KAFKA_MAX_SCRAM_ITERATIONS: i32 = 16_384;

/// Happy path: a super-user authenticates over PLAIN, sends an
/// `AlterUserScramCredentials` upsertion for `alice`, and the broker stores
/// the credential.
///
/// The test then authenticates as `alice` over SCRAM-SHA-512. This proves
/// that the upsertion wrote a valid credential to the metadata image.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_super_user_can_provision() {
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
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain, SaslMechanism::ScramSha512];
    cfg.plain_credentials
        .insert("admin".to_string(), admin_plain_password());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let (salt, salted) = pbkdf2_salt_and_salted(alice_password().as_bytes(), 4096);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![ScramCredentialUpsertion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            iterations: 4096,
            salt: bytes::Bytes::from(salt),
            salted_password: bytes::Bytes::from(salted.to_vec()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "admin",
        admin_plain_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR upsertion");
    assert!(resp.results.len() == 1, "one result row per upsertion");
    check!(
        resp.results[0].error_code == 0,
        "expected error_code=0, got {:?}",
        resp.results[0]
    );
    check!(resp.results[0].user == "alice");

    // Round-trip: now log in as `alice` over SCRAM, proving the upserted
    // credential actually reached the metadata image. Wait for the raft
    // commit to land the credential in the committed image, then auth.
    handle
        .wait_for_image(|img| {
            img.scram_credential("alice", SaslMechanism::ScramSha512)
                .is_some()
        })
        .await;
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha512)
            .await;
    handle.shutdown().await;
    result.expect("post-upsertion SCRAM auth must succeed");
}

/// Wire-mapping proof: `AlterUserScramCredentials` accepts `mechanism=1`,
/// which is SCRAM-SHA-256, and stores a credential.
///
/// The broker can later authenticate against that credential over SHA-256.
/// The test is a copy of `alter_scram_creds_super_user_can_provision`, but it
/// uses the SHA-256 wire byte and a 32-byte `salted_password` payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_super_user_can_provision_sha256() {
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
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain, SaslMechanism::ScramSha256];
    cfg.plain_credentials
        .insert("admin".to_string(), admin_plain_password());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let (salt, salted) = pbkdf2_salt_and_salted_sha256(alice_password().as_bytes(), 4096);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![ScramCredentialUpsertion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_256,
            iterations: 4096,
            salt: bytes::Bytes::from(salt),
            salted_password: bytes::Bytes::from(salted.to_vec()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "admin",
        admin_plain_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR upsertion (SHA-256)");
    assert!(resp.results.len() == 1);
    check!(
        resp.results[0].error_code == 0,
        "expected error_code=0, got {:?}",
        resp.results[0]
    );
    check!(resp.results[0].user == "alice");

    // Wait for the upserted credential to reach the committed metadata
    // image, then authenticate as `alice` over SHA-256 SCRAM.
    handle
        .wait_for_image(|img| {
            img.scram_credential("alice", SaslMechanism::ScramSha256)
                .is_some()
        })
        .await;
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha256)
            .await;
    handle.shutdown().await;
    result.expect("post-upsertion SHA-256 SCRAM auth must succeed");
}

/// A non-super-user authenticates and tries to upsert.
///
/// The broker accepts the request, because it is a valid SASL listener API.
/// But every per-user row reports `CLUSTER_AUTHORIZATION_FAILED` (31). The
/// broker makes no metadata change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_non_super_user_rejected() {
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
        .insert("bob".to_string(), wrong_scram_password());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);
    // Install `SimpleAclAuthorizer` so the cluster-Alter gate
    // fires for non-super principals; the default `AllowAllAuthorizer`
    // would let alice through.
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let (salt, salted) = pbkdf2_salt_and_salted(alice_password().as_bytes(), 4096);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![ScramCredentialUpsertion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            iterations: 4096,
            salt: bytes::Bytes::from(salt),
            salted_password: bytes::Bytes::from(salted.to_vec()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "bob",
        wrong_scram_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR (rejected)");
    handle.shutdown().await;
    assert!(resp.results.len() == 1);
    assert!(
        resp.results[0].error_code == 31, // CLUSTER_AUTHORIZATION_FAILED
        "non-super-user must get CLUSTER_AUTHORIZATION_FAILED, got {:?}",
        resp.results[0]
    );
}

/// Authenticate over SASL/PLAIN against `addr` as `user`/`password`, send one
/// `AlterUserScramCredentials v0` request, and decode the response.
///
/// The request uses `api_key` 51 and is flexible. Every T15 test case calls
/// this helper, so the SASL boilerplate stays in one place.
pub async fn drive_alter_user_scram_credentials_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: AlterUserScramCredentialsRequest,
) -> Result<AlterUserScramCredentialsResponse, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible).
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let _ = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;

    // ── 2. SaslHandshake v1.
    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
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
    payload.push(0);
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password);
    let mut auth_body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    }
    .encode(&mut auth_body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let auth_resp_bytes = round_trip(&mut stream, 36, 2, 3, true, &auth_body).await?;
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={}",
            auth_resp.error_code
        )));
    }

    // ── 4. AlterUserScramCredentials v0 (api_key 51, flexible from v0).
    let mut auscr_body = BytesMut::new();
    req.encode(&mut auscr_body, 0)
        .map_err(|e| io::Error::other(format!("AUSCR encode: {e}")))?;
    let auscr_resp_bytes = round_trip(&mut stream, 51, 0, 4, true, &auscr_body).await?;
    let mut cur: &[u8] = &auscr_resp_bytes;
    AlterUserScramCredentialsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("AUSCR decode: {e}")))
}

/// Compute `(salt, salted_password)` for a SCRAM-SHA-512 wire upsertion.
///
/// The salt is a fixed 16-byte vector, which keeps the test deterministic.
/// The salted password is the 64-byte PBKDF2-HMAC-SHA-512 output that the
/// KIP-554 wire request carries.
pub fn pbkdf2_salt_and_salted(password: &[u8], iterations: u32) -> (Vec<u8>, [u8; 64]) {
    let salt: Vec<u8> = (0..16).collect();
    let salted: [u8; 64] =
        pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &salt, iterations);
    (salt, salted)
}

/// SHA-256 analog of [`pbkdf2_salt_and_salted`].
///
/// It produces the 32-byte PBKDF2-HMAC-SHA-256 output for the wire tests.
fn pbkdf2_salt_and_salted_sha256(password: &[u8], iterations: u32) -> (Vec<u8>, [u8; 32]) {
    let salt: Vec<u8> = (0..16).collect();
    let salted: [u8; 32] =
        pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &salt, iterations);
    (salt, salted)
}
