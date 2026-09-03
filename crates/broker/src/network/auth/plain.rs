//! SASL/PLAIN (RFC 4616): the single-round `SaslAuthenticate` handler.
//!
//! PLAIN needs no per-connection exchange state, so the whole mechanism is
//! one function that splits the credential payload and verifies it.

use std::{collections::HashMap, hash::BuildHasher};

use krabka_protocol::owned::{
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_authenticate_response::SaslAuthenticateResponse,
};
use krabka_security::SaslMechanism;
use krabka_units::Time;

use super::{
    response::fail_authenticate,
    state::{ConnectionAuth, SaslExchange, begin_reauth, finish_reauth, session_expiry},
};
use crate::codes::ILLEGAL_SASL_STATE;

/// Handles `SaslAuthenticate` (`api_key` 36) for the PLAIN mechanism.
///
/// On the wire, `auth_bytes` carries `\0<authzid>\0<authcid>\0<password>`.
/// The handler ignores `authzid`, because RFC 4616 leaves it free-form and
/// Kafka clients usually send it empty. The username is `authcid`.
///
/// On a credential match, this moves `auth` to
/// [`ConnectionAuth::Authenticated`]. The caller closes the connection if the
/// returned `error_code` is non-zero.
///
/// PLAIN credentials carry no lifetime of their own, so `max_reauth` — the
/// listener's `connections.max.reauth.ms` — is the only thing that bounds the
/// session (KIP-368). When it is set, the response reports it as
/// `session_lifetime_ms` and the connection must re-authenticate in band
/// before it elapses.
pub fn handle_authenticate_plain<S: BuildHasher>(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    plain_credentials: &HashMap<String, String, S>,
    max_reauth: Option<Time>,
) -> SaslAuthenticateResponse {
    match auth {
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::Plain,
            ..
        } => authenticate_plain(req, auth, plain_credentials, max_reauth),
        ConnectionAuth::Reauthenticating {
            exchange: SaslExchange::Plain,
            ..
        } => {
            let previous = begin_reauth(auth).expect("matched Reauthenticating above");
            let resp = authenticate_plain(req, auth, plain_credentials, max_reauth);
            finish_reauth(auth, previous, resp)
        }
        // Neither mid-handshake nor mid-re-auth: Kafka answers a
        // `SaslAuthenticate` outside a SASL exchange with ILLEGAL_SASL_STATE,
        // and the dispatcher closes the connection.
        _ => SaslAuthenticateResponse {
            error_code: ILLEGAL_SASL_STATE,
            error_message: Some("not in PLAIN negotiation".to_string()),
            auth_bytes: bytes::Bytes::new(),
            session_lifetime_ms: 0,
            ..Default::default()
        },
    }
}

fn authenticate_plain<S: BuildHasher>(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    plain_credentials: &HashMap<String, String, S>,
    max_reauth: Option<Time>,
) -> SaslAuthenticateResponse {
    let parts: Vec<&[u8]> = req.auth_bytes.split(|&b| b == 0).collect();
    if parts.len() != 3 {
        return fail_authenticate("malformed PLAIN payload");
    }
    let Ok(user) = std::str::from_utf8(parts[1]) else {
        return fail_authenticate("non-utf8 username");
    };
    let password = parts[2];
    match krabka_security::verify_plain(plain_credentials, user, password) {
        Ok(p) => {
            let (expires_at_ms, session_lifetime_ms) =
                session_expiry(crate::time_util::now_ms(), None, max_reauth);
            *auth = ConnectionAuth::Authenticated {
                principal: p,
                mechanism: SaslMechanism::Plain,
                expires_at_ms,
                // PLAIN never auths via a delegation token.
                authenticated_via_token: false,
            };
            SaslAuthenticateResponse {
                error_code: 0,
                error_message: None,
                auth_bytes: bytes::Bytes::new(),
                session_lifetime_ms,
                ..Default::default()
            }
        }
        Err(_) => fail_authenticate("authentication failed"),
    }
}
