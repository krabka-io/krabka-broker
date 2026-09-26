//! SASL/SCRAM (RFC 5802) over `SaslAuthenticate`, with KIP-48 delegation-
//! token login.
//!
//! The two RFC 5802 rounds and the synthetic token credential that KIP-48
//! defines are one concern: the credential store is chosen inside SCRAM
//! round 1, from the client-first message's `tokenauth` extension, and cannot
//! be read apart from it.

use krabka_protocol::owned::{
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_authenticate_response::SaslAuthenticateResponse,
};
use krabka_security::{AuthMethod, Principal, SaslMechanism, ScramServerExchange};
use krabka_units::Time;
use krabka_verified::delegation_token::{
    ScramCredentialFacts, ScramCredentialSource, TokenLookup, scram_credential_source,
    token_expiry_not_passed, token_is_active,
};

use super::{
    response::fail_authenticate,
    state::{ConnectionAuth, SaslExchange, begin_reauth, finish_reauth, session_expiry},
};

/// SCRAM-SHA-256 and SCRAM-SHA-512 `SaslAuthenticate` handler. It runs the
/// two RFC 5802 rounds over Kafka's `SaslAuthenticate` (`api_key` 36) wire
/// envelope.
///
/// Round 1 (client-first):
///   - `auth_bytes` is the raw SCRAM client-first message,
///     `n,,n=<user>,r=<client-nonce>[,tokenauth=true]`. The handler parses the
///     username and the `tokenauth` extension, looks up the credential in the
///     store the extension selects (the SCRAM credentials, or the delegation
///     tokens by token id), and builds a
///     [`ScramServerExchange`]. The exchange consumes the same client-first
///     bytes and emits the server-first message (`r=…,s=…,i=…`), which
///     becomes the response `auth_bytes`. `auth` moves from
///     `Negotiating { exchange: ScramPending }` to
///     `Negotiating { exchange: Scram(server) }`, still unauthenticated.
///
/// Round 2 (client-final):
///   - `auth_bytes` is `c=biws,r=<combined-nonce>,p=<proof>`. The exchange
///     verifies the client proof and emits the server-final message
///     (`v=<server-signature>`). On success, `auth` moves to
///     `Authenticated { principal }`. On any error, the response carries
///     `error_code = 58` and the dispatcher closes the connection.
///
/// A SCRAM credential carries no lifetime of its own, so `max_reauth` — the
/// listener's `connections.max.reauth.ms` — is what bounds a regular SCRAM
/// session (KIP-368). A delegation-token session is bounded by the earlier of
/// the token expiry and that cap.
pub fn handle_authenticate_scram(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    controller: &dyn crate::metadata_source::MetadataSource,
    max_reauth: Option<Time>,
) -> SaslAuthenticateResponse {
    // KIP-368 in-band re-auth runs the same two rounds, so drive the exchange
    // through the ordinary path and let `finish_reauth` hold it to the
    // previous session's principal.
    let Some(previous) = begin_reauth(auth) else {
        return authenticate_scram(req, auth, controller, max_reauth);
    };
    let resp = authenticate_scram(req, auth, controller, max_reauth);
    finish_reauth(auth, previous, resp)
}

fn authenticate_scram(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    controller: &dyn crate::metadata_source::MetadataSource,
    max_reauth: Option<Time>,
) -> SaslAuthenticateResponse {
    // Round-1 case: still in `ScramPending` — build the exchange now that
    // we have the client-first bytes (and thus the username).
    if let ConnectionAuth::Negotiating {
        exchange: SaslExchange::ScramPending,
        mechanism,
        pending_token_expiry_ms: _,
    } = auth
    {
        let mech = *mechanism;
        let Some(ClientFirst {
            username,
            token_auth_requested,
        }) = parse_scram_client_first(&req.auth_bytes)
        else {
            return fail_authenticate("malformed SCRAM client-first");
        };

        // KIP-48, as Kafka's `ScramSaslServer` does it: the client's
        // `tokenauth=true` extension alone decides whether the username is a
        // token id checked against the delegation-token cache or a user
        // checked against the SCRAM credential store. A token login works
        // under either SCRAM mechanism; its credential is synthesized from
        // the token's HMAC (see `synthesize_token_scram_credential`), the
        // session's principal is the token owner rather than the token id,
        // and the token's expiry bounds the session (KIP-368).
        let image = controller.current_image();
        let now = crate::time_util::now_ms();
        let regular = image.scram_credential(&username, mech);
        let token = image.delegation_token_by_id(&username);
        let token_lookup = match token {
            None => TokenLookup::Missing,
            Some(token)
                if token_is_active(now, token.expiry_timestamp_ms, token.max_timestamp_ms) =>
            {
                TokenLookup::Live
            }
            Some(_) => TokenLookup::Expired,
        };
        let (cred, principal_override, token_expiry_ms) =
            match scram_credential_source(ScramCredentialFacts {
                token_auth_requested,
                has_regular_credential: regular.is_some(),
                token: token_lookup,
            }) {
                ScramCredentialSource::Regular => (
                    regular.expect("verified regular source exists").clone(),
                    None,
                    None,
                ),
                ScramCredentialSource::DelegationToken => {
                    let token = token.expect("verified token source exists");
                    let synth = synthesize_token_scram_credential(token, mech);
                    let owner = Principal {
                        name: token.owner.name.clone(),
                        auth_method: AuthMethod::from_sasl(mech),
                        groups: vec![],
                    };
                    (synth, Some(owner), Some(token.expiry_timestamp_ms))
                }
                ScramCredentialSource::ExpiredDelegationToken => {
                    return fail_authenticate("delegation token expired");
                }
                ScramCredentialSource::Unknown => {
                    return fail_authenticate("unknown user");
                }
            };

        let server = match principal_override {
            Some(p) => ScramServerExchange::new_with_principal(username, cred, p),
            None => ScramServerExchange::new(username, cred),
        };
        // Feed the same client-first bytes; on success the exchange emits
        // the server-first message and yields the next phase.
        match server.step(&req.auth_bytes) {
            krabka_security::StepResult::Continue(bytes, next) => {
                *auth = ConnectionAuth::Negotiating {
                    mechanism: mech,
                    exchange: SaslExchange::Scram(Box::new(next)),
                    // Side-channel — `Some` here is the
                    // unambiguous "this is a token-authed session"
                    // signal that the round-2 success arm consumes
                    // to set `Authenticated.authenticated_via_token`
                    // + `expires_at_ms`.
                    pending_token_expiry_ms: token_expiry_ms,
                };
                SaslAuthenticateResponse {
                    error_code: 0,
                    error_message: None,
                    auth_bytes: bytes::Bytes::from(bytes),
                    session_lifetime_ms: 0,
                    ..Default::default()
                }
            }
            // Done on the first round would be a server bug — SCRAM is
            // always two round trips. Treat as auth failure.
            krabka_security::StepResult::Done(_, _) => {
                fail_authenticate("SCRAM server completed in one round")
            }
            krabka_security::StepResult::Failed(_) => fail_authenticate("SCRAM step failed"),
        }
    } else if let ConnectionAuth::Negotiating {
        exchange: SaslExchange::Scram(_),
        ..
    } = auth
    {
        // Round 2: exchange already exists. `step` consumes the exchange, so
        // extract it by value (mirroring `handle_handshake`'s re-auth
        // snapshot swap) before stepping it with the client-final bytes; on
        // success extract the principal + server-final bytes and transition
        // to `Authenticated`.
        let ConnectionAuth::Negotiating {
            mechanism,
            exchange: SaslExchange::Scram(server),
            pending_token_expiry_ms,
        } = std::mem::replace(auth, ConnectionAuth::Anonymous)
        else {
            unreachable!("matched Negotiating{{Scram}} above");
        };
        match server.step(&req.auth_bytes) {
            krabka_security::StepResult::Continue(_, _) => {
                // Two-round SCRAM: an extra `Continue` here is a bug.
                fail_authenticate("SCRAM second round expected Done")
            }
            krabka_security::StepResult::Done(principal, bytes) => {
                // When round 1 selected a delegation
                // token, `pending_token_expiry_ms` is `Some(expiry)`
                // — its presence is both the marker for
                // `authenticated_via_token: true` and the value of
                // `expires_at_ms` (the KIP-368 re-auth ceiling).
                // For regular SCRAM, it's `None` and the
                // session has no expiry.
                // Round 1 checked the token's full liveness, its maximum
                // timestamp included; the expiry is what can pass between
                // the two rounds.
                let now = crate::time_util::now_ms();
                if pending_token_expiry_ms.is_some_and(|e| !token_expiry_not_passed(now, e)) {
                    return fail_authenticate("delegation token expired");
                }
                let (expires_at_ms, session_lifetime_ms) =
                    session_expiry(now, pending_token_expiry_ms, max_reauth);
                *auth = ConnectionAuth::Authenticated {
                    principal,
                    mechanism,
                    expires_at_ms,
                    authenticated_via_token: pending_token_expiry_ms.is_some(),
                };
                SaslAuthenticateResponse {
                    error_code: 0,
                    error_message: None,
                    auth_bytes: bytes::Bytes::from(bytes),
                    session_lifetime_ms,
                    ..Default::default()
                }
            }
            krabka_security::StepResult::Failed(_) => fail_authenticate("SCRAM proof failed"),
        }
    } else {
        fail_authenticate("not in SCRAM negotiation")
    }
}

/// KIP-48: SCRAM iteration count for delegation-token credentials, Kafka's
/// `ScramMechanism.minIterations` for both SCRAM mechanisms, which
/// `DelegationTokenManager.prepareScramCredentials` uses.
const TOKEN_SCRAM_ITERS: u32 = 4096;

/// Kafka's `ScramLoginModule.TOKEN_AUTH_CONFIG`, the SCRAM extension that marks
/// a delegation-token login.
const TOKEN_AUTH_EXTENSION: &str = "tokenauth";

/// KIP-48: builds the synthetic SCRAM credential that authenticates a
/// delegation-token login under `mechanism`, as Kafka's
/// `DelegationTokenManager.prepareScramCredentials` does for every SCRAM
/// mechanism:
///   - "password" = base64-encoded token HMAC bytes. This is the same value
///     that `CreateDelegationToken` returns to the client and that clients
///     present as the SCRAM password.
///   - salt = UTF-8 bytes of `token_id`. Kafka draws a random salt; the
///     client reads the salt from the server-first message either way, and
///     the token UUID is already uniformly random.
///   - iters = [`TOKEN_SCRAM_ITERS`]
///
/// The broker computes it on every auth attempt instead of storing it for
/// each token in the metadata image.
fn synthesize_token_scram_credential(
    token: &krabka_metadata::DelegationToken,
    mechanism: SaslMechanism,
) -> krabka_security::ScramCredential {
    use base64::Engine;
    let password = base64::engine::general_purpose::STANDARD.encode(&token.hmac);
    let salt = token.token_id.as_bytes().to_vec();
    krabka_security::scram::hash_scram_password_with_salt(
        password.as_bytes(),
        mechanism,
        TOKEN_SCRAM_ITERS,
        salt,
    )
}

/// What round 1 reads from a SCRAM client-first message.
#[derive(Debug, PartialEq, Eq)]
struct ClientFirst {
    username: String,
    /// Kafka's `ScramExtensions.tokenAuthenticated`: the `tokenauth`
    /// extension is present and `Boolean.parseBoolean` reads it as true.
    token_auth_requested: bool,
}

/// Parses a SCRAM client-first message, `n,,n=<user>,r=<nonce>[,<ext>...]`.
///
/// The leading `n,,` is the GS2 header, with no channel binding and no
/// authzid. As in Kafka's `ScramMessages.ClientFirstMessage`, the username
/// and nonce come first, in that order, and every attribute after the nonce is
/// a `key=value` extension; a later duplicate key replaces an earlier one, as
/// `Utils.parseMap` does. Returns `None` on any parse failure.
fn parse_scram_client_first(bytes: &[u8]) -> Option<ClientFirst> {
    let bare = std::str::from_utf8(bytes).ok()?.strip_prefix("n,,")?;
    let mut attrs = bare.split(',');
    let username = attrs.next()?.strip_prefix("n=")?.to_string();
    attrs.next()?.strip_prefix("r=")?;
    let mut token_auth_requested = false;
    for extension in attrs {
        let (key, value) = extension.split_once('=')?;
        if key == TOKEN_AUTH_EXTENSION {
            token_auth_requested = value.eq_ignore_ascii_case("true");
        }
    }
    Some(ClientFirst {
        username,
        token_auth_requested,
    })
}

#[cfg(test)]
mod tests;
