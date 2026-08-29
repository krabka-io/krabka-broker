//! SASL/OAUTHBEARER (KIP-255, RFC 7628) and the KIP-368 re-auth arm.
//!
//! OAUTHBEARER is the only mechanism whose session carries an expiry, so this
//! module also holds the session-lifetime clamp that KIP-368 re-authentication
//! reads, together with the two-message failure handshake RFC 7628 requires.

use krabka_protocol::owned::{
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_authenticate_response::SaslAuthenticateResponse,
};
use krabka_units::{Time, convert::TimeExt as _};

use super::{
    response::fail_authenticate,
    state::{ConnectionAuth, SaslExchange},
};
use crate::codes::SASL_AUTHENTICATION_FAILED;

/// SASL/OAUTHBEARER `SaslAuthenticate` handler (KIP-255 / RFC 7628).
///
/// Round 1 (client initial response):
///   - `auth_bytes` is `n,,\x01auth=Bearer <token>\x01\x01`. The handler
///     parses the bearer token and validates it with `validator` against
///     `now_ms`. On success, `auth` moves to `Authenticated` and the response
///     carries empty `auth_bytes` with `error_code = 0`. That is a
///     single-round success.
///   - On any parse or validation failure, the handler returns the RFC 7628
///     `{"status":"invalid_token"}` JSON in `auth_bytes` with
///     `error_code = 0`, keeps the connection open, and moves to
///     `OAuthBearerFailed`.
///
/// Round 2 runs only after a failure. The JVM client replies to the error JSON
/// with a single `\x01`. The handler returns `SASL_AUTHENTICATION_FAILED`
/// (58), and the dispatcher closes the connection.
// Single state-machine dispatch: Negotiating-success / Negotiating-failure /
// Reauth-success / Reauth-failure / fall-through. Extracting per-arm helpers
// would obscure the shape and force ferrying `mech` / `prev_mech` / now_ms /
// the cap through a parameter wall.
pub async fn handle_authenticate_oauthbearer(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &krabka_security::OAuthBearerValidator,
    now_ms: i64,
    max_session_lifetime: Option<Time>,
) -> SaslAuthenticateResponse {
    match auth {
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::OAuthBearer,
            mechanism,
            // OAUTHBEARER never carries a delegation-token expiry;
            // this side-channel is only ever populated by the SCRAM round-1
            // token-fallback path. Ignore here.
            pending_token_expiry_ms: _,
        } => {
            let mech = *mechanism;
            match validate_bearer(&req.auth_bytes, validator, now_ms).await {
                Ok(outcome) => {
                    // Clamp `session_lifetime_ms` to the optional
                    // broker cap, then anchor `Authenticated.expires_at_ms`
                    // to the CLAMPED value. The dispatch loop reads
                    // `expires_at_ms` to schedule the re-auth deadline — if
                    // we stored the raw token exp here, the broker would
                    // tolerate the connection past the value reported to
                    // the client.
                    let (session_lifetime_ms, effective_expires_at_ms) =
                        oauth_session_lifetime(outcome.expires_at_ms, now_ms, max_session_lifetime);
                    *auth = ConnectionAuth::Authenticated {
                        principal: outcome.principal,
                        mechanism: mech,
                        expires_at_ms: effective_expires_at_ms,
                        // OAUTHBEARER is a real SASL mechanism,
                        // never a delegation token.
                        authenticated_via_token: false,
                    };
                    successful_authentication(session_lifetime_ms)
                }
                Err(reason) => {
                    tracing::debug!(reason, "OAUTHBEARER token rejected");
                    *auth = ConnectionAuth::Negotiating {
                        mechanism: mech,
                        exchange: SaslExchange::OAuthBearerFailed,
                        // OAUTHBEARER failure path never
                        // involves a delegation token.
                        pending_token_expiry_ms: None,
                    };
                    SaslAuthenticateResponse {
                        error_code: 0,
                        error_message: None,
                        auth_bytes: bytes::Bytes::from(
                            krabka_security::invalid_token_json().into_bytes(),
                        ),
                        session_lifetime_ms: 0,
                        ..Default::default()
                    }
                }
            }
        }
        // The client's `\x01` final message after a rejected token: complete
        // the RFC 7628 failure handshake by closing with code 58.
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::OAuthBearerFailed,
            ..
        } => fail_authenticate("oauthbearer token rejected"),
        // In-band re-authentication. Validate the new token and,
        // on success, require the principal name to match the previous
        // session (KIP-368 forbids principal switches mid-connection).
        ConnectionAuth::Reauthenticating {
            previous,
            exchange: SaslExchange::OAuthBearer,
        } => {
            let prev_mech = previous.mechanism;
            let prev_name = previous.principal.name.clone();
            match validate_bearer(&req.auth_bytes, validator, now_ms).await {
                Ok(outcome) => {
                    if outcome.principal.name != prev_name {
                        tracing::debug!(
                            previous = %prev_name,
                            attempted = %outcome.principal.name,
                            "OAUTHBEARER re-auth principal mismatch"
                        );
                        // Principal switch — reject; dispatch closes the
                        // connection on non-zero error_code.
                        return SaslAuthenticateResponse {
                            error_code: SASL_AUTHENTICATION_FAILED,
                            error_message: Some(
                                "re-authentication may not change the principal".to_string(),
                            ),
                            auth_bytes: bytes::Bytes::new(),
                            session_lifetime_ms: 0,
                            ..Default::default()
                        };
                    }
                    // Same clamp as the Negotiating-success arm
                    // so re-auth respects the broker cap.
                    let (session_lifetime_ms, effective_expires_at_ms) =
                        oauth_session_lifetime(outcome.expires_at_ms, now_ms, max_session_lifetime);
                    *auth = ConnectionAuth::Authenticated {
                        principal: outcome.principal,
                        mechanism: prev_mech,
                        expires_at_ms: effective_expires_at_ms,
                        // OAUTHBEARER re-auth never produces a
                        // token-authed session.
                        authenticated_via_token: false,
                    };
                    successful_authentication(session_lifetime_ms)
                }
                Err(reason) => {
                    tracing::debug!(reason, "OAUTHBEARER re-auth token rejected");
                    SaslAuthenticateResponse {
                        error_code: SASL_AUTHENTICATION_FAILED,
                        error_message: Some("re-authentication failed".to_string()),
                        auth_bytes: bytes::Bytes::new(),
                        session_lifetime_ms: 0,
                        ..Default::default()
                    }
                }
            }
        }
        _ => fail_authenticate("not in oauthbearer negotiation"),
    }
}

fn oauth_session_lifetime(
    expires_at_ms: Option<i64>,
    now_ms: i64,
    max_session_lifetime: Option<Time>,
) -> (i64, Option<i64>) {
    let raw_session_ms = expires_at_ms.map_or(0, |expires| (expires - now_ms).max(0));
    let session_lifetime_ms =
        max_session_lifetime.map_or(raw_session_ms, |cap| raw_session_ms.min(cap.millis_i64()));
    (session_lifetime_ms, Some(now_ms + session_lifetime_ms))
}

fn successful_authentication(session_lifetime_ms: i64) -> SaslAuthenticateResponse {
    SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms,
        ..Default::default()
    }
}

/// Parses and validates an OAUTHBEARER client initial response. The authzid,
/// when present, must equal the token principal, as RFC 7628 and Kafka
/// require.
async fn validate_bearer(
    auth_bytes: &[u8],
    validator: &krabka_security::OAuthBearerValidator,
    now_ms: i64,
) -> Result<krabka_security::AuthOutcome, &'static str> {
    let parsed = krabka_security::parse_client_initial_response(auth_bytes)
        .map_err(|_| "malformed OAUTHBEARER client response")?;
    let outcome = validator
        .validate(&parsed.token, now_ms)
        .await
        .map_err(|_| "token validation failed")?;
    if let Some(authzid) = parsed.authzid
        && authzid != outcome.principal.name
    {
        return Err("authzid does not match token principal");
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests;
