//! SASL/PLAIN (RFC 4616): the single-round `SaslAuthenticate` handler.
//!
//! PLAIN needs no per-connection exchange state, so the whole mechanism is
//! one function that splits the credential payload and verifies it.

use {
    super::{
        response::{fail_authenticate_with, sasl_ok},
        state::{ConnectionAuth, SaslExchange, SessionExpiry, session_expiry},
    },
    crate::{codes::ILLEGAL_SASL_STATE, time_util::now_ms},
    krabka_protocol::owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
    },
    krabka_security::SaslMechanism,
    krabka_units::Time,
    std::{collections::HashMap, hash::BuildHasher},
};

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
    if !matches!(
        auth,
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::Plain,
            ..
        } | ConnectionAuth::Reauthenticating {
            exchange: SaslExchange::Plain,
            ..
        }
    ) {
        return SaslAuthenticateResponse {
            error_code: ILLEGAL_SASL_STATE,
            error_message: Some("not in PLAIN negotiation".to_string()),
            auth_bytes: bytes::Bytes::new(),
            session_lifetime_ms: 0,
            ..Default::default()
        };
    }
    super::state::authenticate_with_reauth(auth, |auth| {
        // `PlainSaslServer` decodes the whole response before it splits it.
        let response = java_utf8_lossy(&req.auth_bytes);
        let parts: Vec<&str> = response.split('\0').collect();
        if parts.len() != 3 {
            // `PlainSaslServer.extractTokens` stops splitting at four tokens.
            return fail_authenticate_with(format!(
                "Invalid SASL/PLAIN response: expected 3 tokens, got {}",
                parts.len().min(4)
            ));
        }
        let (authzid, username, password) = (parts[0], parts[1], parts[2]);
        if username.is_empty() {
            return fail_authenticate_with(
                "Authentication failed: username not specified".to_owned(),
            );
        }
        if password.is_empty() {
            return fail_authenticate_with(
                "Authentication failed: password not specified".to_owned(),
            );
        }
        let Ok(p) = krabka_security::verify_plain(plain_credentials, username, password.as_bytes())
        else {
            return fail_authenticate_with(
                "Authentication failed: Invalid username or password".to_owned(),
            );
        };
        // RFC 4616 lets the client ask to act as another identity; Kafka refuses
        // any authorization id that is not the authenticated username.
        if !authzid.is_empty() && authzid != username {
            return fail_authenticate_with(
                "Authentication failed: Client requested an authorization id that is different \
             from username"
                    .to_owned(),
            );
        }
        let SessionExpiry {
            expires_at_ms,
            session_lifetime_ms,
        } = session_expiry(now_ms(), None, max_reauth);
        *auth = ConnectionAuth::Authenticated {
            principal: p,
            mechanism: SaslMechanism::Plain,
            expires_at_ms,
            // PLAIN never auths via a delegation token.
            authenticated_via_token: false,
        };
        sasl_ok(bytes::Bytes::new(), session_lifetime_ms)
    })
}

/// Decodes `bytes` as Java's `new String(bytes, StandardCharsets.UTF_8)`
/// does, replacing each malformed sequence with U+FFFD.
///
/// It matches Rust's [`String::from_utf8_lossy`] except on a UTF-8-encoded
/// surrogate, `ED A0..BF`. Rust replaces each of those bytes on its own. The
/// JDK 21 decoder reads `ED A0..BF` as the start of a three-byte character
/// and replaces the whole character, with its continuation byte when one
/// follows, by one U+FFFD. The tests hold vectors measured on JDK 21.
fn java_utf8_lossy(bytes: &[u8]) -> String {
    let mut decoded = String::with_capacity(bytes.len());
    let mut rest = bytes;
    loop {
        match std::str::from_utf8(rest) {
            Ok(valid) => {
                decoded.push_str(valid);
                return decoded;
            }
            Err(error) => {
                let (valid, malformed) = rest.split_at(error.valid_up_to());
                // `valid_up_to` marks the end of a well-formed prefix.
                decoded.push_str(std::str::from_utf8(valid).unwrap_or_default());
                decoded.push(char::REPLACEMENT_CHARACTER);
                let replaced = match malformed {
                    [0xED, 0xA0..=0xBF, 0x80..=0xBF, ..] => 3,
                    [0xED, 0xA0..=0xBF, ..] => 2,
                    _ => error.error_len().unwrap_or(malformed.len()),
                };
                rest = &malformed[replaced..];
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            network::auth::{
                state::AuthenticatedSnapshot,
                test_support::{
                    assert_failed_authenticate_response, assert_success_authenticate_response,
                },
            },
            test_support::sasl_principal,
        },
        assert2::{assert, check},
        krabka_security::AuthMethod,
    };

    fn credentials() -> HashMap<String, String> {
        let mut creds = HashMap::new();
        creds.insert("alice".to_string(), "wonderland".to_string());
        creds.insert("bob".to_string(), "builder".to_string());
        creds
    }

    use crate::network::auth::test_support::plain_request as payload;

    fn assert_restored_session(auth: ConnectionAuth) {
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                expires_at_ms,
                ..
            } => {
                check!(principal.name == "alice");
                check!(expires_at_ms == Some(9_000));
            }
            other => panic!("expected the previous session restored, got {other:?}"),
        }
    }

    fn negotiating() -> ConnectionAuth {
        ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Plain,
            exchange: SaslExchange::Plain,
            pending_token_expiry_ms: None,
        }
    }

    fn alice_session() -> AuthenticatedSnapshot {
        AuthenticatedSnapshot {
            principal: sasl_principal("alice"),
            mechanism: SaslMechanism::Plain,
            expires_at_ms: Some(9_000),
            authenticated_via_token: false,
        }
    }

    /// A `SaslAuthenticate` that arrives outside a PLAIN exchange is
    /// `ILLEGAL_SASL_STATE`, and — the part that matters — it leaves the
    /// connection's identity alone. An already-authenticated connection must
    /// not be able to overwrite its own principal with a bare
    /// `SaslAuthenticate`, and a connection negotiating some other mechanism
    /// must not be authenticated by a PLAIN payload.
    #[test]
    fn authenticate_outside_a_plain_exchange_is_illegal_and_changes_nothing() {
        let expected = SaslAuthenticateResponse {
            error_code: ILLEGAL_SASL_STATE,
            error_message: Some("not in PLAIN negotiation".to_string()),
            auth_bytes: bytes::Bytes::new(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        let cases = [
            ("anonymous", ConnectionAuth::Anonymous),
            (
                "already authenticated as someone else",
                ConnectionAuth::Authenticated {
                    principal: sasl_principal("bob"),
                    mechanism: SaslMechanism::Plain,
                    expires_at_ms: None,
                    authenticated_via_token: false,
                },
            ),
            (
                "negotiating a different mechanism",
                ConnectionAuth::Negotiating {
                    mechanism: SaslMechanism::ScramSha512,
                    exchange: SaslExchange::ScramPending,
                    pending_token_expiry_ms: None,
                },
            ),
            (
                "re-authenticating a different mechanism",
                ConnectionAuth::Reauthenticating {
                    previous: alice_session(),
                    exchange: SaslExchange::ScramPending,
                    pending_token_expiry_ms: None,
                },
            ),
        ];
        for (case, mut auth) in cases {
            let before = std::mem::discriminant(&auth);
            let resp = handle_authenticate_plain(
                &payload("alice", "wonderland"),
                &mut auth,
                &credentials(),
                None,
            );
            check!(resp == expected, "{case}");
            check!(std::mem::discriminant(&auth) == before, "{case}");
            check!(
                auth.principal().map(|p| p.name.as_str()) != Some("alice"),
                "{case}"
            );
        }
    }

    /// Every way a PLAIN credential can be refused answers with
    /// `PlainSaslServer`'s message. An unknown user and a wrong password share
    /// one message, so a peer cannot tell them apart, and none of the refusals
    /// authenticates the connection -- including an authorization id that
    /// asks to act as another user.
    #[test]
    fn every_plain_refusal_answers_kafkas_message() {
        const INVALID: &str = "Authentication failed: Invalid username or password";
        /// One refused PLAIN payload and the message it must be answered with.
        struct Refusal<'a> {
            case: &'a str,
            bytes: Vec<u8>,
            message: &'a str,
        }
        let cases: [Refusal<'_>; 10] = [
            Refusal {
                case: "empty payload",
                bytes: Vec::new(),
                message: "Invalid SASL/PLAIN response: expected 3 tokens, got 1",
            },
            Refusal {
                case: "two fields",
                bytes: b"\0alice".to_vec(),
                message: "Invalid SASL/PLAIN response: expected 3 tokens, got 2",
            },
            Refusal {
                case: "four fields",
                bytes: b"\0alice\0wonderland\0extra".to_vec(),
                message: "Invalid SASL/PLAIN response: expected 3 tokens, got 4",
            },
            Refusal {
                case: "five fields report four",
                bytes: b"\0alice\0wonderland\0a\0b".to_vec(),
                message: "Invalid SASL/PLAIN response: expected 3 tokens, got 4",
            },
            Refusal {
                case: "empty username",
                bytes: b"\0\0wonderland".to_vec(),
                message: "Authentication failed: username not specified",
            },
            Refusal {
                case: "empty password",
                bytes: b"\0alice\0".to_vec(),
                message: "Authentication failed: password not specified",
            },
            Refusal {
                // Kafka decodes `FF FE` to two U+FFFD, a user nobody configured.
                case: "malformed utf-8 username",
                bytes: vec![0, 0xff, 0xfe, 0, b'w', b'o'],
                message: INVALID,
            },
            Refusal {
                case: "unknown user",
                bytes: b"\0carol\0wonderland".to_vec(),
                message: INVALID,
            },
            Refusal {
                case: "wrong password",
                bytes: b"\0alice\0hunter2".to_vec(),
                message: INVALID,
            },
            Refusal {
                case: "authzid names another user",
                bytes: b"bob\0alice\0wonderland".to_vec(),
                message: "Authentication failed: Client requested an authorization id that is different \
                 from username",
            },
        ];
        for Refusal {
            case,
            bytes,
            message,
        } in cases
        {
            let req = SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from(bytes),
                ..Default::default()
            };
            let mut auth = negotiating();
            let resp = handle_authenticate_plain(&req, &mut auth, &credentials(), None);
            assert_failed_authenticate_response(&resp, Some(message));
            check!(!auth.is_authenticated(), "{case}");
        }
    }

    /// The decoder replaces malformed input exactly as JDK 21's
    /// `new String(bytes, StandardCharsets.UTF_8)` does. Each expected value
    /// was printed by that constructor on JDK 21. An encoded surrogate is the
    /// one place where it differs from Rust's `String::from_utf8_lossy`.
    #[yare::parameterized(
        ascii = { &[0x61], "a" },
        lone_continuation = { &[0x80], "\u{FFFD}" },
        two_continuations = { &[0x80, 0x80], "\u{FFFD}\u{FFFD}" },
        between_ascii = { &[0x61, 0x80, 0x62], "a\u{FFFD}b" },
        c0_never_valid = { &[0xC0, 0x80], "\u{FFFD}\u{FFFD}" },
        truncated_two_byte = { &[0xC2], "\u{FFFD}" },
        two_byte_lead_before_ascii = { &[0xC3, 0x28], "\u{FFFD}(" },
        overlong_three_byte = { &[0xE0, 0x80, 0x80], "\u{FFFD}\u{FFFD}\u{FFFD}" },
        truncated_three_byte = { &[0xE0, 0xA0], "\u{FFFD}" },
        truncated_three_byte_before_ascii = { &[0xE0, 0xA0, 0x41], "\u{FFFD}A" },
        last_before_surrogates = { &[0xED, 0x9F, 0xBF], "\u{D7FF}" },
        high_surrogate = { &[0xED, 0xA0, 0x80], "\u{FFFD}" },
        low_surrogate = { &[0xED, 0xB0, 0x80], "\u{FFFD}" },
        surrogate_pair = { &[0xED, 0xA0, 0x80, 0xED, 0xB0, 0x80], "\u{FFFD}\u{FFFD}" },
        truncated_surrogate = { &[0xED, 0xA0], "\u{FFFD}" },
        truncated_surrogate_before_ascii = { &[0xED, 0xBF, 0x41], "\u{FFFD}A" },
        overlong_four_byte = { &[0xF0, 0x80, 0x80, 0x80], "\u{FFFD}\u{FFFD}\u{FFFD}\u{FFFD}" },
        truncated_four_byte = { &[0xF0, 0x90, 0x80], "\u{FFFD}" },
        last_code_point = { &[0xF4, 0x8F, 0xBF, 0xBF], "\u{10FFFF}" },
        past_last_code_point = { &[0xF4, 0x90, 0x80, 0x80], "\u{FFFD}\u{FFFD}\u{FFFD}\u{FFFD}" },
        five_byte_form = { &[0xF8, 0x88, 0x80, 0x80, 0x80], "\u{FFFD}\u{FFFD}\u{FFFD}\u{FFFD}\u{FFFD}" },
        never_valid_byte = { &[0xFF], "\u{FFFD}" },
        literal_replacement_character = { &[0xEF, 0xBF, 0xBD], "\u{FFFD}" },
    )]
    fn java_utf8_lossy_decodes_as_jdk_21(bytes: &[u8], expected: &str) {
        assert2::assert!(java_utf8_lossy(bytes) == expected);
    }

    /// Kafka compares the decoded username, password and authorization id,
    /// so malformed bytes match a configured name or password that holds the
    /// U+FFFD they decode to.
    #[yare::parameterized(
        username = { b"\0\xFFeve\0secret", "\u{FFFD}eve", "secret" },
        password = { b"\0eve\0se\xED\xA0\x80cret", "eve", "se\u{FFFD}cret" },
        authzid = { b"\xC0eve\0\xFFeve\0secret", "\u{FFFD}eve", "secret" },
    )]
    fn malformed_bytes_match_what_kafka_decodes_them_to(
        bytes: &'static [u8],
        user: &str,
        password: &str,
    ) {
        let credentials = HashMap::from([(user.to_owned(), password.to_owned())]);
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from_static(bytes),
            ..Default::default()
        };
        let mut auth = negotiating();
        let resp = handle_authenticate_plain(&req, &mut auth, &credentials, None);
        assert_success_authenticate_response(&resp, b"", 0);
        assert2::assert!(auth.principal().map(|p| p.name.as_str()) == Some(user));
    }

    /// An authorization id equal to the username is accepted.
    #[test]
    fn an_authzid_equal_to_the_username_authenticates() {
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from_static(b"alice\0alice\0wonderland"),
            ..Default::default()
        };
        let mut auth = negotiating();
        let resp = handle_authenticate_plain(&req, &mut auth, &credentials(), None);
        assert_success_authenticate_response(&resp, b"", 0);
        check!(auth.principal().map(|p| p.name.as_str()) == Some("alice"));
    }

    /// KIP-368: PLAIN carries no lifetime of its own, so the listener's
    /// `connections.max.reauth.ms` is the only thing that bounds the session.
    /// The advertised `session_lifetime_ms` and the enforced `expires_at_ms`
    /// have to appear together — a window advertised without a deadline never
    /// closes, and a deadline without a window closes on a client that was
    /// never told to re-authenticate.
    #[test]
    fn a_successful_plain_login_records_the_listener_cap_as_the_only_ceiling() {
        let mut auth = negotiating();
        let resp = handle_authenticate_plain(
            &payload("alice", "wonderland"),
            &mut auth,
            &credentials(),
            Some(krabka_units::secs(30)),
        );
        check!((29_000..=30_000).contains(&resp.session_lifetime_ms));
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                expires_at_ms,
                authenticated_via_token,
            } => {
                check!(principal.name == "alice");
                check!(principal.auth_method == AuthMethod::SaslPlain);
                check!(mechanism == SaslMechanism::Plain);
                check!(expires_at_ms.is_some());
                check!(!authenticated_via_token);
            }
            other => panic!("expected Authenticated, got {other:?}"),
        }

        let mut uncapped = negotiating();
        let resp = handle_authenticate_plain(
            &payload("bob", "builder"),
            &mut uncapped,
            &credentials(),
            None,
        );
        assert_success_authenticate_response(&resp, b"", 0);
        match uncapped {
            ConnectionAuth::Authenticated { expires_at_ms, .. } => {
                check!(expires_at_ms.is_none());
            }
            other => panic!("expected Authenticated, got {other:?}"),
        }
    }

    /// KIP-368 in-band re-authentication over PLAIN: the same principal
    /// re-arms the window, and a different one is refused with the previous
    /// session left intact. The refusal is the security-relevant half — a peer
    /// that re-authenticates as another user must not end up holding that
    /// user's identity, nor be logged out of its own.
    #[test]
    fn plain_reauth_re_arms_for_the_same_principal_and_refuses_a_switch() {
        let mut auth = ConnectionAuth::Reauthenticating {
            previous: alice_session(),
            exchange: SaslExchange::Plain,
            pending_token_expiry_ms: None,
        };
        let resp = handle_authenticate_plain(
            &payload("alice", "wonderland"),
            &mut auth,
            &credentials(),
            Some(krabka_units::secs(30)),
        );
        check!(resp.error_code == 0);
        check!((29_000..=30_000).contains(&resp.session_lifetime_ms));
        match &auth {
            ConnectionAuth::Authenticated {
                principal,
                expires_at_ms,
                ..
            } => {
                check!(principal.name == "alice");
                check!(*expires_at_ms > Some(9_000), "the window must be re-armed");
            }
            other => panic!("expected Authenticated, got {other:?}"),
        }

        let mut switched = ConnectionAuth::Reauthenticating {
            previous: alice_session(),
            exchange: SaslExchange::Plain,
            pending_token_expiry_ms: None,
        };
        let resp = handle_authenticate_plain(
            &payload("bob", "builder"),
            &mut switched,
            &credentials(),
            Some(krabka_units::secs(30)),
        );
        assert!(
            resp == SaslAuthenticateResponse {
                error_code: crate::codes::SASL_AUTHENTICATION_FAILED,
                error_message: Some(
                    "Cannot change principals during re-authentication from User.alice: User.bob"
                        .to_string()
                ),
                auth_bytes: bytes::Bytes::new(),
                session_lifetime_ms: 0,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }
        );
        assert_restored_session(switched);
    }

    /// A PLAIN re-authentication whose credential is wrong keeps the peer on
    /// its existing session instead of dropping it to unauthenticated: the
    /// connection is closed by the dispatcher on the non-zero code, and the
    /// state it is closed from must still be the one it held.
    #[test]
    fn a_failed_plain_reauth_leaves_the_previous_session_in_place() {
        let mut auth = ConnectionAuth::Reauthenticating {
            previous: alice_session(),
            exchange: SaslExchange::Plain,
            pending_token_expiry_ms: None,
        };
        let resp = handle_authenticate_plain(
            &payload("alice", "hunter2"),
            &mut auth,
            &credentials(),
            None,
        );
        assert_failed_authenticate_response(
            &resp,
            Some("Authentication failed: Invalid username or password"),
        );
        assert_restored_session(auth);
    }
}
