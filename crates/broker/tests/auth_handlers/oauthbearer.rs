//! Shared SASL/OAUTHBEARER plumbing (KIP-255 / RFC 7628).
//!
//! The module mints the bearer tokens, starts a broker whose only mechanism
//! is OAUTHBEARER, and drives the handshake, the authenticate round, and the
//! KIP-368 in-band re-authentication pair. The OAUTHBEARER suites in
//! `oauthbearer_tokens` and `oauthbearer_sessions` build on these.

use std::{io, net::SocketAddr};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use bytes::BytesMut;
use krabka_broker::{Broker, BrokerConfig, config::ListenerSpec};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tokio::net::TcpStream;

use crate::harness::round_trip;

/// Build an unsecured JWS (`alg:none`) bearer token with a `sub` principal and
/// an `exp` in Unix seconds.
///
/// The signature segment is empty. This matches what the JVM
/// `OAuthBearerUnsecuredLoginCallbackHandler` produces.
pub fn unsecured_jws(sub: &str, exp_unix_secs: i64) -> String {
    let header = B64.encode(b"{\"alg\":\"none\"}");
    let claims = B64.encode(format!("{{\"sub\":\"{sub}\",\"exp\":{exp_unix_secs}}}").as_bytes());
    format!("{header}.{claims}.")
}

/// RFC 7628 client initial response that carries `token` with an empty
/// authzid.
pub fn oauthbearer_initial(token: &str) -> bytes::Bytes {
    bytes::Bytes::from(format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes())
}

pub fn now_unix_secs() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs(),
    )
    .expect("seconds fit in i64")
}

/// Start a single `SASL_PLAINTEXT` broker that enables only OAUTHBEARER, with
/// the given validator.
pub fn start_oauthbearer_broker(
    log_dir: &std::path::Path,
    validator: krabka_security::OAuthBearerValidator,
) -> impl std::future::Future<Output = krabka_broker::BrokerHandle> {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        // This helper exercises only the client-listener validator. Dedicated
        // multi-broker tests cover outbound OAUTHBEARER on the controller and
        // inter-broker paths.
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
    cfg.oauthbearer_validator = validator;
    Box::pin(async move { Broker::start(cfg).await.expect("broker must start") })
}

/// Same as [`start_oauthbearer_broker`], but with a configurable server-side
/// ceiling on the OAUTHBEARER session lifetime.
///
/// `Some(seconds)` clamps `session_lifetime_ms` to
/// `min(token_exp - now, seconds * 1000)`. It clamps the dispatch-loop
/// re-auth deadline to the same value. `None` reproduces the 49e default,
/// where the session ends at the token exp.
pub fn start_oauthbearer_broker_with_cap(
    log_dir: &std::path::Path,
    validator: krabka_security::OAuthBearerValidator,
    max_session_lifetime: Option<krabka_units::Time>,
) -> impl std::future::Future<Output = krabka_broker::BrokerHandle> {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
    cfg.oauthbearer_validator = validator;
    cfg.oauthbearer_max_session_lifetime = max_session_lifetime;
    Box::pin(async move { Broker::start(cfg).await.expect("broker must start") })
}

/// Run a pre-auth `ApiVersions` and a `SaslHandshake`(OAUTHBEARER).
///
/// The helper asserts that the broker advertises OAUTHBEARER and that the
/// handshake succeeds.
pub async fn oauthbearer_handshake(
    stream: &mut TcpStream,
    corr: &mut i32,
) -> Result<(), io::Error> {
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av = round_trip(stream, 18, 0, *corr, false, &av_body).await?;
    *corr += 1;
    let mut cur: &[u8] = &av;
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    let sh_req = SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req
        .encode(&mut sh_body, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh = round_trip(stream, 17, 1, *corr, false, &sh_body).await?;
    *corr += 1;
    let mut cur: &[u8] = &sh;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }
    if !sh_resp.mechanisms.iter().any(|m| m == "OAUTHBEARER") {
        return Err(io::Error::other("OAUTHBEARER not advertised"));
    }
    Ok(())
}

/// Send one `SaslAuthenticate v2` with `auth_bytes` and return the decoded
/// response.
pub async fn oauthbearer_authenticate(
    stream: &mut TcpStream,
    corr: &mut i32,
    auth_bytes: bytes::Bytes,
) -> Result<SaslAuthenticateResponse, io::Error> {
    let req = SaslAuthenticateRequest {
        auth_bytes,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let resp_bytes = round_trip(stream, 36, 2, *corr, true, &body).await?;
    *corr += 1;
    let mut cur: &[u8] = &resp_bytes;
    SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))
}

/// Build an unsecured-JWS validator with zero clock skew and the default
/// `sub` principal claim.
///
/// Zero clock skew makes `exp` the exact session boundary. This validator is
/// the same as the one in the other OAuth tests, but it is pinned to zero
/// skew so that the assertion windows in the re-auth tests do not drift.
pub fn oauthbearer_zero_skew_validator() -> krabka_security::OAuthBearerValidator {
    krabka_security::OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
        allowable_clock_skew: krabka_units::secs(0),
        ..Default::default()
    })
}

/// Drive a `SASL_PLAINTEXT` OAUTHBEARER handshake to completion on a fresh
/// connection.
///
/// The function returns the still-open `TcpStream` and the
/// `session_lifetime_ms` field from the `SaslAuthenticateResponse`. Callers
/// can then assert on the timer and continue to use the connection. The
/// in-band re-auth scenarios do this.
///
/// `bearer_token` is the JWS string. For unsecured tests it is an `alg:none`
/// JWT with the wanted `sub` and `exp`. The function frames the RFC 7628
/// client-first message that wraps the token.
pub async fn drive_sasl_oauthbearer_session_open(
    addr: SocketAddr,
    bearer_token: &str,
) -> Result<(TcpStream, i64), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut corr = 1;
    oauthbearer_handshake(&mut stream, &mut corr).await?;
    let auth =
        oauthbearer_authenticate(&mut stream, &mut corr, oauthbearer_initial(bearer_token)).await?;
    if auth.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate error_code={} message={:?}",
            auth.error_code, auth.error_message
        )));
    }
    if !auth.auth_bytes.is_empty() {
        return Err(io::Error::other(
            "unexpected challenge — token was rejected",
        ));
    }
    Ok((stream, auth.session_lifetime_ms))
}

/// Drive a `SaslHandshake`(OAUTHBEARER) and `SaslAuthenticate` pair on an
/// already-authenticated stream, with a new bearer token.
///
/// This helper exercises KIP-368 in-band re-authentication. It returns
/// `Ok(())` when the broker accepts the new token. It returns `Err` when
/// either round reports a non-zero error code. The caller then asserts on
/// the rendered error text to tell 34 from 58.
pub async fn drive_inband_reauth(stream: &mut TcpStream, new_token: &str) -> Result<(), io::Error> {
    let handshake_request = SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".to_string(),
        ..Default::default()
    };
    let mut handshake_body = BytesMut::new();
    handshake_request
        .encode(&mut handshake_body, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let handshake_response_bytes = round_trip(stream, 17, 1, 100, false, &handshake_body).await?;
    let mut cur: &[u8] = &handshake_response_bytes;
    let handshake_response = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if handshake_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "in-band SaslHandshake error_code={}",
            handshake_response.error_code
        )));
    }

    let authenticate_request = SaslAuthenticateRequest {
        auth_bytes: oauthbearer_initial(new_token),
        ..Default::default()
    };
    let mut authenticate_body = BytesMut::new();
    authenticate_request
        .encode(&mut authenticate_body, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let authenticate_response_bytes =
        round_trip(stream, 36, 2, 101, true, &authenticate_body).await?;
    let mut cur: &[u8] = &authenticate_response_bytes;
    let authenticate_response = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if authenticate_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "in-band SaslAuthenticate error_code={} message={:?}",
            authenticate_response.error_code, authenticate_response.error_message
        )));
    }
    Ok(())
}
