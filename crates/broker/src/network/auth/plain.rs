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

use super::{response::fail_authenticate, state::ConnectionAuth};

/// Handles `SaslAuthenticate` (`api_key` 36) for the PLAIN mechanism.
///
/// On the wire, `auth_bytes` carries `\0<authzid>\0<authcid>\0<password>`.
/// The handler ignores `authzid`, because RFC 4616 leaves it free-form and
/// Kafka clients usually send it empty. The username is `authcid`.
///
/// On a credential match, this moves `auth` to
/// [`ConnectionAuth::Authenticated`]. The caller closes the connection if the
/// returned `error_code` is non-zero.
pub fn handle_authenticate_plain<S: BuildHasher>(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    plain_credentials: &HashMap<String, String, S>,
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
            *auth = ConnectionAuth::Authenticated {
                principal: p,
                mechanism: SaslMechanism::Plain,
                expires_at_ms: None,
                // PLAIN never auths via a delegation token.
                authenticated_via_token: false,
            };
            SaslAuthenticateResponse {
                error_code: 0,
                error_message: None,
                auth_bytes: bytes::Bytes::new(),
                session_lifetime_ms: 0,
                ..Default::default()
            }
        }
        Err(_) => fail_authenticate("authentication failed"),
    }
}
