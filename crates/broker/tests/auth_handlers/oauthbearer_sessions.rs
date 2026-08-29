//! SASL/OAUTHBEARER re-authentication (KIP-368) end-to-end.
//!
//! Six scenarios exercise the full session-lifetime and in-band re-auth
//! surface: the response carries `session_lifetime_ms ~ exp - now`; a broker
//! cap clamps it; the dispatch-loop timer closes the connection past the
//! token's `exp`; an in-band re-auth with a fresh token resets the timer; an
//! in-band re-auth with a different principal name returns 58 and closes the
//! connection; an in-band attempt to switch mechanism returns 34; and, as a
//! regression, a PLAIN listener reports `session_lifetime_ms = 0` and arms no
//! timer.
//!
//! `tokio::time::pause()` and `advance()` drive the per-connection deadline
//! deterministically. The tests pause AFTER the broker is started and the
//! handshake completes, rather than with `start_paused = true`, because the
//! broker's own internal timers -- heartbeats, JWKS refresh, disk scans,
//! raft -- must run at real wall-clock rates during startup or
//! `Broker::start` hangs. Post-handshake `pause()` is enough: the dispatch
//! loop's `sleep_until(instant_at_epoch_ms(exp))` was armed against the real
//! tokio clock at the moment the loop re-entered `select!` after the
//! `SaslAuthenticate`; `advance()` then jumps past that Instant.

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
use tokio::{io::AsyncReadExt, net::TcpStream};

use crate::{
    harness::{alice_password, round_trip},
    oauthbearer::{
        drive_inband_reauth, drive_sasl_oauthbearer_session_open, now_unix_secs,
        oauthbearer_zero_skew_validator, start_oauthbearer_broker,
        start_oauthbearer_broker_with_cap, unsecured_jws,
    },
};

/// Test #1: a successful OAUTHBEARER authentication carries
/// `session_lifetime_ms ≈ exp - now`.
///
/// `session_lifetime_ms` is the KIP-368 wire field on
/// `SaslAuthenticateResponse v1+`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_session_lifetime_ms_set_from_token_exp() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), oauthbearer_zero_skew_validator()).await;
    let addr = handle.listen_addr();

    let exp_secs = now_unix_secs() + 600;
    let token = unsecured_jws("alice", exp_secs);

    let (stream, session_lifetime_ms) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");
    drop(stream);

    // ~600_000 ms; allow generous wall-clock slop for CI.
    assert!(
        (590_000..605_000).contains(&session_lifetime_ms),
        "session_lifetime_ms = {session_lifetime_ms}, expected ~600_000"
    );

    handle.shutdown().await;
}

/// The broker clamps `session_lifetime_ms` when the config sets
/// `[oauthbearer].max_session_lifetime_seconds`.
///
/// The response value becomes `min(token_exp_ms - now_ms, cap * 1000)`. The
/// dispatch loop anchors its deadline to the CLAMPED value, so the broker
/// enforces what it told the client.
#[tokio::test(flavor = "current_thread")]
async fn oauthbearer_session_capped_by_broker_max_session_lifetime_seconds() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker_with_cap(
        log_dir.path(),
        oauthbearer_zero_skew_validator(),
        Some(krabka_units::secs(30)), // 30s cap
    )
    .await;
    let addr = handle.listen_addr();

    // Token exp = now + 600s. Cap = 30s. Expected session = 30_000 ms.
    let exp_secs = now_unix_secs() + 600;
    let token = unsecured_jws("alice", exp_secs);

    let (mut stream, session_lifetime_ms) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");

    // Cap should clamp the response.
    assert!(
        (29_000..31_000).contains(&session_lifetime_ms),
        "session_lifetime_ms = {session_lifetime_ms}, expected ~30_000 (capped)"
    );

    // Now pause and advance past cap; broker should close.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    tokio::time::resume();

    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error");
    assert!(
        n == 0,
        "expected EOF after cap-bounded session expiry, got {n} bytes"
    );

    handle.shutdown().await;
}

/// Test #2: once the tokio clock advances past the token's `exp`, the
/// dispatch loop's per-connection `sleep_until` fires and closes the
/// TCP stream. The client observes EOF on the next read.
///
/// `tokio::time::pause()` needs the `current_thread` runtime. The test calls
/// `pause()` after the handshake and does not set `start_paused = true`,
/// because the broker's internal start-up timers need real wall-clock
/// progress. Those timers are the raft heartbeats, the JWKS refresh, and the
/// disk scans. Without that progress, `Broker::start` hangs. The dispatch
/// loop armed its deadline on the real Instant when it re-entered `select!`
/// after the handshake, so `advance(61s)` after the pause jumps tokio past
/// that Instant.
#[tokio::test(flavor = "current_thread")]
async fn oauthbearer_session_expires_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), oauthbearer_zero_skew_validator()).await;
    let addr = handle.listen_addr();

    let exp_secs = now_unix_secs() + 60;
    let token = unsecured_jws("alice", exp_secs);

    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");

    // Freeze tokio's clock and jump past the token's expiry. The deadline
    // armed by the dispatch loop after handshake completion was anchored
    // to the real Instant at the time the loop re-entered `select!`; a
    // 61-second `advance` jumps tokio's clock past that Instant.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(61)).await;
    tokio::time::resume();

    // Broker must have closed the connection. Read should EOF.
    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error");
    assert!(n == 0, "expected EOF after session expiry, got {n} bytes");

    handle.shutdown().await;
}

/// Test #3: an in-band SaslHandshake/SaslAuthenticate pair with a fresh token
/// on an already-authenticated stream resets the per-connection deadline.
///
/// The fresh token has a longer `exp`. After the clock advances past the
/// original token's `exp`, the connection is still open and a Metadata RPC
/// succeeds.
#[tokio::test(flavor = "current_thread")]
async fn oauthbearer_in_band_reauth_with_fresh_token_resets_timer() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), oauthbearer_zero_skew_validator()).await;
    let addr = handle.listen_addr();

    // Token A expires in 60s. Token B expires in 600s.
    let token_a = unsecured_jws("alice", now_unix_secs() + 60);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token_a)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // In-band re-auth with the fresh token BEFORE token A expires (we
    // haven't yet paused / advanced anything).
    let token_b = unsecured_jws("alice", now_unix_secs() + 600);
    drive_inband_reauth(&mut stream, &token_b)
        .await
        .expect("in-band re-auth with fresh token must succeed");

    // Now jump past token A's exp (61s). Token B is good for another
    // ~540s, so the dispatch deadline should be re-armed to token B's
    // expiry and the connection must remain open.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_mins(2)).await;
    tokio::time::resume();

    // Issue a Metadata RPC to prove the connection survived.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req
        .encode(&mut md_body, 12)
        .expect("Metadata encode must succeed");
    let md_resp_bytes = round_trip(&mut stream, 3, 12, 99, true, &md_body)
        .await
        .expect("Metadata RPC must succeed past original token expiry");
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = MetadataResponse::decode(&mut cur, 12).expect("Metadata decode must succeed");
    assert!(
        !md_resp.brokers.is_empty(),
        "Metadata response must carry at least one broker"
    );

    handle.shutdown().await;
}

/// Test #4: the broker rejects an in-band re-auth with a token whose `sub`
/// differs from the original principal name.
///
/// `SaslAuthenticateResponse` carries
/// `error_code = SASL_AUTHENTICATION_FAILED (58)`, and the connection closes.
/// The client then reads EOF. KIP-368 forbids a change of principal across an
/// in-band re-auth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_principal_closes() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), oauthbearer_zero_skew_validator()).await;
    let addr = handle.listen_addr();

    let token_alice = unsecured_jws("alice", now_unix_secs() + 600);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token_alice)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // Attempt re-auth with a token belonging to "bob".
    let token_bob = unsecured_jws("bob", now_unix_secs() + 600);
    let result = drive_inband_reauth(&mut stream, &token_bob).await;
    let err = result.expect_err("re-auth with different principal must fail");
    assert!(
        err.to_string().contains("error_code=58"),
        "expected SASL_AUTHENTICATION_FAILED (58); got {err}"
    );

    // Broker closes after the error response.
    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error");
    assert!(n == 0, "expected EOF after failed re-auth");

    handle.shutdown().await;
}

/// Test #5: the broker rejects an in-band `SaslHandshake` whose `mechanism`
/// differs from the mechanism it first negotiated.
///
/// The response carries `error_code = ILLEGAL_SASL_STATE (34)`. KIP-368 needs
/// the same mechanism across an in-band re-auth, even when the broker would
/// otherwise accept SCRAM on a fresh connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_mechanism_closes() {
    let log_dir = tempfile::tempdir().unwrap();
    // Enable both OAUTHBEARER + SCRAM-SHA-512 on the same listener so a
    // fresh-connection SCRAM handshake WOULD succeed. The reject here
    // must be due to the same-mechanism rule, not "mechanism unknown".
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
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512];
    cfg.oauthbearer_validator = oauthbearer_zero_skew_validator();
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let token = unsecured_jws("alice", now_unix_secs() + 600);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // In-band SaslHandshake with SCRAM-SHA-512 — must come back with
    // ILLEGAL_SASL_STATE (34) on the handshake response itself.
    let sh_req = SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-512".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req
        .encode(&mut sh_body, 1)
        .expect("SaslHandshake encode must succeed");
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 200, false, &sh_body)
        .await
        .expect("handshake round-trip");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp =
        SaslHandshakeResponse::decode(&mut cur, 1).expect("SaslHandshake decode must succeed");
    assert!(
        sh_resp.error_code == 34,
        "expected ILLEGAL_SASL_STATE for mechanism switch"
    );

    handle.shutdown().await;
}

/// Test #6: regression for PLAIN listeners.
///
/// A PLAIN `SaslAuthenticate` response must carry `session_lifetime_ms = 0`.
/// The KIP-368 wire field has a meaning for OAUTHBEARER only. The dispatch
/// loop must NOT arm a per-connection deadline. An advance of the tokio clock
/// by one hour is harmless, and a Metadata RPC still succeeds.
///
/// The test uses the `current_thread` flavor, because `tokio::time::pause()`
/// needs a single-threaded runtime. See the comment on test #2 for why we
/// pause after the handshake and do not set `start_paused = true`.
#[tokio::test(flavor = "current_thread")]
async fn plain_listener_session_lifetime_ms_is_zero_and_no_timer() {
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

    // Inline a full PLAIN handshake (mirrors `drive_sasl_plain_session`)
    // so we can capture the SaslAuthenticateResponse and assert its
    // `session_lifetime_ms` field directly.
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req.encode(&mut av_body, 0).unwrap();
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body)
        .await
        .expect("ApiVersions round-trip");
    let mut cur: &[u8] = &av_resp_bytes;
    let _ = ApiVersionsResponse::decode(&mut cur, 0).unwrap();

    let sh_req = SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req.encode(&mut sh_body, 1).unwrap();
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body)
        .await
        .expect("SaslHandshake round-trip");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1).unwrap();
    assert!(sh_resp.error_code == 0, "PLAIN handshake must succeed");

    let mut payload = Vec::new();
    payload.push(0);
    payload.extend_from_slice(b"alice");
    payload.push(0);
    payload.extend_from_slice(alice_password().as_bytes());
    let auth_req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    };
    let mut auth_body = BytesMut::new();
    auth_req.encode(&mut auth_body, 2).unwrap();
    let auth_resp_bytes = round_trip(&mut stream, 36, 2, 3, true, &auth_body)
        .await
        .expect("SaslAuthenticate round-trip");
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp = SaslAuthenticateResponse::decode(&mut cur, 2).unwrap();
    assert!(auth_resp.error_code == 0, "PLAIN authenticate must succeed");
    assert!(
        auth_resp.session_lifetime_ms == 0,
        "PLAIN listener must report session_lifetime_ms = 0 (no KIP-368 deadline)"
    );

    // Advance the tokio clock by an hour. The dispatch loop must NOT
    // have armed a per-connection timer for this non-OAuth session, so
    // the connection stays alive and serves further requests.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_hours(1)).await;
    tokio::time::resume();

    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req.encode(&mut md_body, 12).unwrap();
    let md_resp_bytes = round_trip(&mut stream, 3, 12, 5, true, &md_body)
        .await
        .expect("Metadata RPC must succeed an hour after PLAIN auth");
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = MetadataResponse::decode(&mut cur, 12).unwrap();
    assert!(
        !md_resp.brokers.is_empty(),
        "Metadata response must carry at least one broker"
    );

    handle.shutdown().await;
}
