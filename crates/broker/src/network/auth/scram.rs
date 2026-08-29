//! SASL/SCRAM (RFC 5802) over `SaslAuthenticate`, with the KIP-48 delegation-
//! token fallback.
//!
//! The two RFC 5802 rounds and the synthetic token credential that KIP-48
//! defines are one concern: the token fallback is a branch inside SCRAM
//! round 1 and cannot be read apart from it.

use krabka_protocol::owned::{
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_authenticate_response::SaslAuthenticateResponse,
};
use krabka_security::{Principal, SaslMechanism, ScramServerExchange};

use super::{
    response::fail_authenticate,
    state::{ConnectionAuth, SaslExchange},
};

/// SCRAM-SHA-512 `SaslAuthenticate` handler. It runs the two RFC 5802 rounds
/// over Kafka's `SaslAuthenticate` (`api_key` 36) wire envelope.
///
/// Round 1 (client-first):
///   - `auth_bytes` is the raw SCRAM client-first message,
///     `n,,n=<user>,r=<client-nonce>`. The handler parses the username, looks
///     up the credential in the metadata image, and builds a
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
pub fn handle_authenticate_scram(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    controller: &dyn crate::metadata_source::MetadataSource,
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
        let Some(username) = parse_scram_username(&req.auth_bytes) else {
            return fail_authenticate("malformed SCRAM client-first");
        };

        // Look up the SCRAM credential. KIP-48: when the
        // user is unknown AND the mechanism is SCRAM-SHA-256, fall
        // back to the delegation-token table (KIP-48 scopes
        // token-SCRAM to SHA-256 only). On a token hit, synthesize a
        // SCRAM credential whose stored/server keys are derived from
        // the token's HMAC bytes (see
        // `synthesize_token_scram_credential`), capture the owner
        // principal so the `Done` arm surfaces the caller as
        // `User:<owner>` rather than `User:<token-uuid>`, and capture
        // the token's `expiry_timestamp_ms` for the
        // KIP-368 re-auth ceiling.
        let image = controller.current_image();
        let (cred, principal_override, token_expiry_ms) =
            if let Some(scram_cred) = image.scram_credential(&username, mech) {
                (scram_cred.clone(), None, None)
            } else if mech == SaslMechanism::ScramSha256 {
                if let Some(token) = image.delegation_token_by_id(&username) {
                    let synth = synthesize_token_scram_credential(token);
                    let owner = Principal {
                        name: token.owner.name.clone(),
                        auth_method: krabka_security::AuthMethod::SaslScramSha256,
                        groups: vec![],
                    };
                    (synth, Some(owner), Some(token.expiry_timestamp_ms))
                } else {
                    return fail_authenticate("unknown user");
                }
            } else {
                return fail_authenticate("unknown user");
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
            // always two round trips for SHA-512. Treat as auth failure.
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
                // Two-round SCRAM-SHA-512: an extra `Continue` here is a bug.
                fail_authenticate("SCRAM second round expected Done")
            }
            krabka_security::StepResult::Done(principal, bytes) => {
                // When round-1 fell back to a delegation
                // token, `pending_token_expiry_ms` is `Some(expiry)`
                // — its presence is both the marker for
                // `authenticated_via_token: true` and the value of
                // `expires_at_ms` (the KIP-368 re-auth ceiling).
                // For regular SCRAM, it's `None` and the
                // session has no expiry.
                let session_lifetime_ms =
                    pending_token_expiry_ms.map_or(0, |e| (e - crate::time_util::now_ms()).max(0));
                *auth = ConnectionAuth::Authenticated {
                    principal,
                    mechanism,
                    expires_at_ms: pending_token_expiry_ms,
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

/// KIP-48: fixed SCRAM iteration count for delegation-token
/// credentials. Specified by KIP-48 §"Token Format".
const TOKEN_SCRAM_ITERS: u32 = 4096;

/// KIP-48: builds a synthetic SCRAM-SHA-256 credential that authenticates
/// callers against a delegation token. KIP-48 fixes these values:
///   - mechanism = SCRAM-SHA-256, the only token-SCRAM mechanism
///   - "password" = base64-encoded token HMAC bytes. This is the same value
///     that `CreateDelegationToken` returns to the client and that clients
///     present as the SCRAM password.
///   - salt = UTF-8 bytes of `token_id`. The token UUID is already uniformly
///     random, so it needs no extra randomness.
///   - iters = [`TOKEN_SCRAM_ITERS`]
///
/// The result is identical to what `hash_scram_password_with_salt` produces
/// for those inputs. The broker computes it on every auth attempt instead of
/// storing it for each token in the metadata image.
fn synthesize_token_scram_credential(
    token: &krabka_metadata::DelegationToken,
) -> krabka_security::ScramCredential {
    use base64::Engine;
    let password = base64::engine::general_purpose::STANDARD.encode(&token.hmac);
    let salt = token.token_id.as_bytes().to_vec();
    krabka_security::scram::hash_scram_password_with_salt(
        password.as_bytes(),
        SaslMechanism::ScramSha256,
        TOKEN_SCRAM_ITERS,
        salt,
    )
}

/// Parses the username from a SCRAM client-first message.
///
/// The RFC 5802 format is `n,,n=<user>,r=<nonce>[,extensions...]`. The
/// leading `n,,` is the GS2 header, with no channel binding and no authzid.
/// The bare body is a comma-separated attribute list. Returns the first `n=`
/// value, or `None` on any parse failure.
fn parse_scram_username(bytes: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(bytes).ok()?;
    let bare = s.strip_prefix("n,,")?;
    for attr in bare.split(',') {
        if let Some(v) = attr.strip_prefix("n=") {
            return Some(v.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests;
