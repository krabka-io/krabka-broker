//! SASL/OAUTHBEARER (KIP-255, RFC 7628) and the KIP-368 re-auth arm.
//!
//! OAUTHBEARER is the only mechanism whose session carries an expiry, so this
//! module also holds the session-lifetime clamp that KIP-368 re-authentication
//! reads, together with the two-message failure handshake RFC 7628 requires.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

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
    handle_authenticate_oauthbearer_inner(req, auth, validator, None, now_ms, max_session_lifetime)
        .await
}

pub async fn handle_authenticate_oauthbearer_with_jwks_cache(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &krabka_security::OAuthBearerValidator,
    cache_generation: &AtomicU64,
    last_successful_fetch_ms: &AtomicI64,
    now_ms: i64,
    max_session_lifetime: Option<Time>,
) -> SaslAuthenticateResponse {
    handle_authenticate_oauthbearer_inner(
        req,
        auth,
        validator,
        Some((cache_generation, last_successful_fetch_ms)),
        now_ms,
        max_session_lifetime,
    )
    .await
}

async fn handle_authenticate_oauthbearer_inner(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &krabka_security::OAuthBearerValidator,
    jwks_cache: Option<(&AtomicU64, &AtomicI64)>,
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
            match validate_bearer(&req.auth_bytes, validator, jwks_cache, now_ms).await {
                Ok(outcome) => {
                    // Clamp `session_lifetime_ms` to the optional
                    // broker cap, then anchor `Authenticated.expires_at_ms`
                    // to the CLAMPED value. The dispatch loop reads
                    // `expires_at_ms` to schedule the re-auth deadline — if
                    // we stored the raw token exp here, the broker would
                    // tolerate the connection past the value reported to
                    // the client.
                    let krabka_verified::OAuthSessionDecision::Admit {
                        session_lifetime_ms,
                        effective_expires_at_ms,
                    } = oauth_session_decision(
                        outcome.expires_at_ms,
                        now_ms,
                        max_session_lifetime,
                        false,
                        true,
                    )
                    else {
                        return reject_initial_oauthbearer(
                            auth,
                            mech,
                            "invalid OAuth session lifetime",
                        );
                    };
                    *auth = ConnectionAuth::Authenticated {
                        principal: outcome.principal,
                        mechanism: mech,
                        expires_at_ms: Some(effective_expires_at_ms),
                        // OAUTHBEARER is a real SASL mechanism,
                        // never a delegation token.
                        authenticated_via_token: false,
                    };
                    successful_authentication(session_lifetime_ms)
                }
                Err(reason) => reject_initial_oauthbearer(auth, mech, reason),
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
            ..
        } => {
            let prev_mech = previous.mechanism;
            let prev_name = previous.principal.name.clone();
            match validate_bearer(&req.auth_bytes, validator, jwks_cache, now_ms).await {
                Ok(outcome) => {
                    let principal_matches = outcome.principal.name == prev_name;
                    let decision = oauth_session_decision(
                        outcome.expires_at_ms,
                        now_ms,
                        max_session_lifetime,
                        true,
                        principal_matches,
                    );
                    let krabka_verified::OAuthSessionDecision::Admit {
                        session_lifetime_ms,
                        effective_expires_at_ms,
                    } = decision
                    else {
                        if !principal_matches {
                            tracing::debug!(
                                previous = %prev_name,
                                attempted = %outcome.principal.name,
                                "OAUTHBEARER re-auth principal mismatch"
                            );
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
                        tracing::debug!("OAUTHBEARER re-auth rejected an invalid session lifetime");
                        return SaslAuthenticateResponse {
                            error_code: SASL_AUTHENTICATION_FAILED,
                            error_message: Some("re-authentication failed".to_string()),
                            auth_bytes: bytes::Bytes::new(),
                            session_lifetime_ms: 0,
                            ..Default::default()
                        };
                    };
                    // Same clamp as the Negotiating-success arm
                    // so re-auth respects the broker cap.
                    *auth = ConnectionAuth::Authenticated {
                        principal: outcome.principal,
                        mechanism: prev_mech,
                        expires_at_ms: Some(effective_expires_at_ms),
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

fn oauth_session_decision(
    expires_at_ms: Option<i64>,
    now_ms: i64,
    max_session_lifetime: Option<Time>,
    reauthentication: bool,
    principal_matches: bool,
) -> krabka_verified::OAuthSessionDecision {
    krabka_verified::oauth_session_admission(krabka_verified::OAuthSessionFacts {
        expiry: if expires_at_ms.is_some() {
            krabka_verified::OAuthExpiryPresence::Present
        } else {
            krabka_verified::OAuthExpiryPresence::Missing
        },
        token_expires_at_ms: expires_at_ms.unwrap_or(0),
        now_ms,
        cap: if max_session_lifetime.is_some() {
            krabka_verified::OAuthSessionCap::Enabled
        } else {
            krabka_verified::OAuthSessionCap::Disabled
        },
        cap_ms: max_session_lifetime.map_or(0, krabka_units::convert::TimeExt::millis_i64),
        authentication: if reauthentication {
            krabka_verified::OAuthAuthenticationKind::Reauthentication
        } else {
            krabka_verified::OAuthAuthenticationKind::Initial
        },
        principal: if principal_matches {
            krabka_verified::OAuthPrincipalMatch::Matches
        } else {
            krabka_verified::OAuthPrincipalMatch::Differs
        },
    })
}

fn reject_initial_oauthbearer(
    auth: &mut ConnectionAuth,
    mechanism: krabka_security::SaslMechanism,
    reason: &'static str,
) -> SaslAuthenticateResponse {
    tracing::debug!(reason, "OAUTHBEARER token rejected");
    *auth = ConnectionAuth::Negotiating {
        mechanism,
        exchange: SaslExchange::OAuthBearerFailed,
        pending_token_expiry_ms: None,
    };
    SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::from(krabka_security::invalid_token_json().into_bytes()),
        session_lifetime_ms: 0,
        ..Default::default()
    }
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
    jwks_cache: Option<(&AtomicU64, &AtomicI64)>,
    now_ms: i64,
) -> Result<krabka_security::AuthOutcome, &'static str> {
    let parsed = krabka_security::parse_client_initial_response(auth_bytes)
        .map_err(|_| "malformed OAUTHBEARER client response")?;
    let cache_guard = match (validator, jwks_cache) {
        (
            krabka_security::OAuthBearerValidator::Signed(signed),
            Some((generation, last_successful)),
        ) => {
            let generation_before = generation.load(Ordering::Acquire);
            let last_successful_fetch_ms = last_successful.load(Ordering::Acquire);
            let generation_after = generation.load(Ordering::Acquire);
            let (expiry_enabled, expiry_ms) = signed
                .cache_expiry
                .map_or((false, 0), |expiry| (true, expiry.millis_i64()));
            let facts = krabka_verified::JwksCacheFacts {
                generation_before,
                generation_after,
                last_successful_fetch_ms,
                now_ms,
                expiry_enabled,
                expiry_ms,
            };
            if krabka_verified::jwks_cache_admission(facts)
                != krabka_verified::JwksCacheDecision::Admit
            {
                return Err("JWKS cache is stale or changing");
            }
            Some((generation, facts))
        }
        _ => None,
    };
    let outcome = validator
        .validate(&parsed.token, now_ms)
        .await
        .map_err(|_| "token validation failed")?;
    if let Some((generation, mut facts)) = cache_guard {
        facts.generation_after = generation.load(Ordering::Acquire);
        if krabka_verified::jwks_cache_admission(facts) != krabka_verified::JwksCacheDecision::Admit
        {
            return Err("JWKS cache changed during validation");
        }
    }
    if let Some(authzid) = parsed.authzid
        && authzid != outcome.principal.name
    {
        return Err("authzid does not match token principal");
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests;
