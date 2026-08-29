//! Inline handling of the two SASL request frames. `SaslHandshake` (17) and
//! `SaslAuthenticate` (36) mutate the per-connection authentication state,
//! which the handler registry cannot reach, so the dispatch loop intercepts
//! them here before it consults the registry.

use bytes::{Bytes, BytesMut};
use krabka_protocol::api_key::ApiKey;
use krabka_units::convert::ByteSizeExt as _;

use super::response::encode_response;
use crate::{broker::Broker, codes, error::BrokerError, handlers::ApiKeyCode};

/// `SaslHandshake` wire `api_key`. The loop handles it inline, before the
/// handler table, because it mutates the per-connection auth state.
const SASL_HANDSHAKE_KEY: ApiKeyCode = ApiKey::SaslHandshake as i16;

/// `SaslAuthenticate` wire `api_key`. The loop handles it inline, before the
/// handler table, because it mutates the per-connection auth state.
const SASL_AUTHENTICATE_KEY: ApiKeyCode = ApiKey::SaslAuthenticate as i16;

/// Outcome of a SASL frame interception: the bytes to write back to the peer,
/// and whether the dispatcher should close the connection after the send
/// completes. `SaslAuthenticate` failures and an illegal state both use the
/// close flag.
pub(super) struct SaslFrameOutcome {
    pub(super) response_bytes: Bytes,
    pub(super) close_after: bool,
}

/// Handles a `SaslHandshake` (17) or `SaslAuthenticate` (36) request inline.
///
/// For those two `api_key` values, the function mutates `auth` and returns a
/// [`SaslFrameOutcome`]. It returns `None` for every other `api_key`, and the
/// caller then falls through to the regular registry dispatch.
///
/// An error here closes the connection. Such errors are protocol violations,
/// for example an undecodable header.
pub(super) async fn try_handle_sasl_frame(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &mut crate::network::auth::ConnectionAuth,
    sasl_mechanisms: &[krabka_security::SaslMechanism],
) -> Option<Result<SaslFrameOutcome, BrokerError>> {
    let api_key = parsed.api_key;
    if api_key != SASL_HANDSHAKE_KEY && api_key != SASL_AUTHENTICATE_KEY {
        return None;
    }
    Some(handle_sasl_frame(broker, parsed, auth, sasl_mechanisms).await)
}

async fn handle_sasl_frame(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &mut crate::network::auth::ConnectionAuth,
    sasl_mechanisms: &[krabka_security::SaslMechanism],
) -> Result<SaslFrameOutcome, BrokerError> {
    use krabka_protocol::{Decode, Encode};

    let (resp_body, close_after) = match parsed.api_key {
        SASL_HANDSHAKE_KEY => (handle_sasl_handshake(parsed, auth, sasl_mechanisms)?, false),
        SASL_AUTHENTICATE_KEY => {
            let mut cur: &[u8] = parsed.body;
            let req =
                krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest::decode(
                    &mut cur,
                    parsed.api_version,
                )?;
            // Must be mid-SASL: either `Negotiating` (initial auth: a
            // SaslHandshake was the previous frame) or `Reauthenticating`
            // (KIP-368 in-band re-auth: a SaslHandshake just ran on an
            // already-authenticated connection). Any other state returns
            // ILLEGAL_SASL_STATE (34) and closes.
            let mech_opt = match auth {
                crate::network::auth::ConnectionAuth::Negotiating { mechanism, .. } => {
                    Some(*mechanism)
                }
                crate::network::auth::ConnectionAuth::Reauthenticating { previous, .. } => {
                    Some(previous.mechanism)
                }
                _ => None,
            };
            let resp = if let Some(mech) = mech_opt {
                match mech {
                    krabka_security::SaslMechanism::Plain => {
                        crate::network::auth::handle_authenticate_plain(
                            &req,
                            auth,
                            &broker.config.plain_credentials,
                        )
                    }
                    krabka_security::SaslMechanism::ScramSha256
                    | krabka_security::SaslMechanism::ScramSha512 => {
                        crate::network::auth::handle_authenticate_scram(
                            &req,
                            auth,
                            &*broker.controller,
                        )
                    }
                    krabka_security::SaslMechanism::OAuthBearer => {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
                        crate::network::auth::handle_authenticate_oauthbearer(
                            &req,
                            auth,
                            &broker.config.oauthbearer_validator,
                            now_ms,
                            broker.config.oauthbearer_max_session_lifetime,
                        )
                        .await
                    }
                    krabka_security::SaslMechanism::Gssapi => {
                        let cfg = broker
                            .config
                            .gssapi
                            .as_ref()
                            .expect("GSSAPI enabled without config");
                        crate::network::auth::handle_authenticate_gssapi(&req, auth, cfg)
                    }
                }
            } else {
                krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse {
                    error_code: codes::ILLEGAL_SASL_STATE,
                    error_message: Some("SaslAuthenticate without prior SaslHandshake".into()),
                    auth_bytes: Bytes::new(),
                    session_lifetime_ms: 0,
                    ..Default::default()
                }
            };
            // Account this SaslAuthenticate frame in the
            // per-mechanism success/failure counters. The mechanism
            // is the one selected by the preceding SaslHandshake;
            // ILLEGAL_SASL_STATE rejects (no prior handshake) land
            // under the `Unknown` sentinel so the metric stays
            // bounded.
            let mech_label = mech_opt.map_or(
                crate::metrics::UNKNOWN_LABEL,
                krabka_security::SaslMechanism::wire_name,
            );
            broker
                .metrics
                .record_authentication(mech_label, resp.error_code == 0);
            let close = resp.error_code != 0;
            let mut buf = BytesMut::with_capacity(resp.encoded_len(parsed.api_version));
            resp.encode(&mut buf, parsed.api_version)?;
            (buf.freeze(), close)
        }
        _ => unreachable!("filtered by caller to 17 / 36 only"),
    };

    let response_bytes = encode_response(
        parsed.api_key,
        parsed.correlation_id,
        parsed.body_flexible,
        &resp_body,
        broker.config.socket_request_max.bytes_usize(),
    )?;
    Ok(SaslFrameOutcome {
        response_bytes,
        close_after,
    })
}

fn handle_sasl_handshake(
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &mut crate::network::auth::ConnectionAuth,
    sasl_mechanisms: &[krabka_security::SaslMechanism],
) -> Result<Bytes, BrokerError> {
    use krabka_protocol::{Decode, Encode};

    let mut body = parsed.body;
    let request = krabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest::decode(
        &mut body,
        parsed.api_version,
    )?;
    let response = crate::network::auth::handle_handshake(&request, auth, sasl_mechanisms);
    let mut encoded = BytesMut::with_capacity(response.encoded_len(parsed.api_version));
    response.encode(&mut encoded, parsed.api_version)?;
    Ok(encoded.freeze())
}
