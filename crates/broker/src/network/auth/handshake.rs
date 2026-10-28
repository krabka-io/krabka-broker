//! `SaslHandshake` (17): mechanism negotiation and the KIP-368 re-auth entry.
//!
//! This module owns the first request of every SASL connection. It answers
//! with the enabled mechanism list, moves a fresh connection to `Negotiating`,
//! and turns a handshake that arrives on an already-authenticated connection
//! into the `Reauthenticating` state that KIP-368 defines -- when Kafka would.

use {
    super::state::{AuthenticatedSnapshot, ConnectionAuth, SaslExchange},
    crate::codes::{ILLEGAL_SASL_STATE, UNSUPPORTED_SASL_MECHANISM},
    krabka_protocol::owned::{
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    krabka_security::SaslMechanism,
    krabka_verified::authz::reauthentication_begins,
    std::mem,
};

/// What the dispatcher does with a `SaslHandshake`: the response to send, and
/// whether to close the connection once it is written.
#[derive(Debug)]
pub struct HandshakeOutcome {
    pub response: SaslHandshakeResponse,
    pub close_after: bool,
}

/// The per-connection clock KIP-368 re-authentication is rate-limited by.
pub struct ReauthClock<'a> {
    /// Unix epoch ms now.
    pub now_ms: i64,
    /// When this connection last started a re-authentication, if ever.
    pub last_start_ms: &'a mut Option<i64>,
}

/// Handles `SaslHandshake` (`api_key` 17), as Kafka's
/// `SaslServerAuthenticator.handleHandshakeRequest` and
/// `KafkaChannel.maybeBeginServerReauthentication` do.
///
/// Before authentication, an enabled mechanism moves `auth` to `Negotiating`
/// and answers `NONE` with the enabled list. Any other mechanism answers
/// [`UNSUPPORTED_SASL_MECHANISM`] (33) with the enabled list, and the
/// connection closes.
///
/// On an authenticated connection, a handshake starts a KIP-368
/// re-authentication only when the session has an expiry and the last one
/// started a second or more ago. Otherwise Kafka hands the frame to
/// `KafkaApis`, which answers [`ILLEGAL_SASL_STATE`] (34) with no mechanisms
/// and keeps the session and the connection. A re-authentication that names
/// the session's own mechanism moves to `Reauthenticating`; one that names
/// another enabled mechanism answers `NONE` and moves to `ReauthBadMechanism`,
/// which fails the connection on the next frame; an unsupported one answers
/// 33 and closes.
pub fn handle_handshake(
    req: &SaslHandshakeRequest,
    auth: &mut ConnectionAuth,
    enabled: &[SaslMechanism],
    clock: &mut ReauthClock<'_>,
) -> HandshakeOutcome {
    let enabled_names: Vec<String> = enabled.iter().map(|m| m.wire_name().to_owned()).collect();
    let requested = SaslMechanism::from_wire(&req.mechanism).filter(|m| enabled.contains(m));
    let answer = |error_code, mechanisms, close_after| HandshakeOutcome {
        response: SaslHandshakeResponse {
            error_code,
            mechanisms,
            ..Default::default()
        },
        close_after,
    };

    match auth {
        ConnectionAuth::Authenticated {
            mechanism: current,
            expires_at_ms,
            ..
        } => {
            if !reauthentication_begins(*expires_at_ms, clock.now_ms, *clock.last_start_ms) {
                tracing::debug!(
                    requested = %req.mechanism,
                    "SaslHandshake on an authenticated connection that cannot re-authenticate now"
                );
                return answer(ILLEGAL_SASL_STATE, Vec::new(), false);
            }
            *clock.last_start_ms = Some(clock.now_ms);
            let current = *current;
            match requested {
                Some(m) if m == current => {
                    let ConnectionAuth::Authenticated {
                        principal,
                        mechanism,
                        expires_at_ms,
                        authenticated_via_token,
                    } = mem::replace(auth, ConnectionAuth::Anonymous)
                    else {
                        unreachable!("matched Authenticated above");
                    };
                    *auth = ConnectionAuth::Reauthenticating {
                        previous: AuthenticatedSnapshot {
                            principal,
                            mechanism,
                            expires_at_ms,
                            authenticated_via_token,
                        },
                        exchange: exchange_for_mechanism(m),
                        // A fresh exchange; a token-SCRAM round 1 populates
                        // it during re-auth exactly as during initial auth.
                        pending_token_expiry_ms: None,
                    };
                    answer(0, enabled_names, false)
                }
                Some(_) => {
                    tracing::debug!(
                        "SASL mechanism '{}' requested by client is not supported for \
                         re-authentication of mechanism '{}'",
                        req.mechanism,
                        current.wire_name()
                    );
                    *auth = ConnectionAuth::ReauthBadMechanism;
                    answer(0, enabled_names, false)
                }
                None => answer(UNSUPPORTED_SASL_MECHANISM, enabled_names, true),
            }
        }
        ConnectionAuth::Anonymous => {
            let Some(m) = requested else {
                tracing::debug!(requested = %req.mechanism, "SaslHandshake: unsupported mechanism");
                return answer(UNSUPPORTED_SASL_MECHANISM, enabled_names, true);
            };
            *auth = ConnectionAuth::Negotiating {
                mechanism: m,
                exchange: exchange_for_mechanism(m),
                // Fresh handshake; the token-fallback in
                // `handle_authenticate_scram` may populate this later during
                // SCRAM round 1.
                pending_token_expiry_ms: None,
            };
            answer(0, enabled_names, false)
        }
        // A handshake already chose a mechanism: Kafka accepts only
        // `SaslAuthenticate` now. The request gate refuses the frame before
        // it gets here; answer the same way if a caller does not gate.
        ConnectionAuth::Negotiating { .. }
        | ConnectionAuth::Reauthenticating { .. }
        | ConnectionAuth::ReauthBadMechanism => answer(ILLEGAL_SASL_STATE, Vec::new(), true),
    }
}

/// Builds the per-mechanism `SaslExchange` initial state.
///
/// It is separate from `handle_handshake` so that the initial-auth path and
/// the re-auth path construct the state in the same way.
fn exchange_for_mechanism(m: SaslMechanism) -> SaslExchange {
    match m {
        SaslMechanism::Plain => SaslExchange::Plain,
        // SCRAM exchange is built lazily on the first SaslAuthenticate
        // round, once the username is known. Until then we sit in
        // `ScramPending`. SHA-256 and SHA-512 share the same dispatch
        // state; the mechanism is preserved on the outer `Negotiating` /
        // `Reauthenticating` variant.
        SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => SaslExchange::ScramPending,
        // The token arrives in the first SaslAuthenticate; no pre-built
        // state needed.
        SaslMechanism::OAuthBearer => SaslExchange::OAuthBearer,
        // GSSAPI exchange is built lazily on the first SaslAuthenticate
        // round, once the client's AP-REQ arrives (we defer reading the
        // keytab until then). Until then we sit in `GssapiPending`.
        SaslMechanism::Gssapi => SaslExchange::GssapiPending,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::network::auth::test_support::authenticated as test_authenticated,
        assert2::assert, rstest_bdd::datatable::Rows,
    };

    fn authenticated(expires_at_ms: Option<i64>) -> ConnectionAuth {
        test_authenticated(
            "alice",
            krabka_security::AuthMethod::SaslOAuthBearer,
            SaslMechanism::OAuthBearer,
            expires_at_ms,
            false,
        )
    }

    /// The state a handshake leaves, reduced to what the table compares.
    #[derive(Debug, PartialEq, Eq)]
    enum After {
        Anonymous,
        Negotiating(SaslMechanism),
        Reauthenticating(SaslMechanism),
        ReauthBadMechanism,
        Authenticated,
    }

    fn after(auth: &ConnectionAuth) -> After {
        match auth {
            ConnectionAuth::Anonymous => After::Anonymous,
            ConnectionAuth::Negotiating { mechanism, .. } => After::Negotiating(*mechanism),
            ConnectionAuth::Reauthenticating { previous, .. } => {
                After::Reauthenticating(previous.mechanism)
            }
            ConnectionAuth::ReauthBadMechanism => After::ReauthBadMechanism,
            ConnectionAuth::Authenticated { .. } => After::Authenticated,
        }
    }

    #[derive(Default)]
    struct SaslHandshakeScenario {
        auth: Option<ConnectionAuth>,
        enabled: Vec<SaslMechanism>,
        now: i64,
        last_start: Option<i64>,
        outcome: Option<HandshakeOutcome>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct HandshakeExpectation {
        code: i16,
        mechanisms: String,
        close: bool,
        state: String,
        last_start: String,
    }

    #[rstest::fixture]
    fn sasl_handshake() -> SaslHandshakeScenario {
        // Every connection has its own exchange, clock, and handshake result.
        SaslHandshakeScenario::default()
    }

    #[rstest_bdd_macros::given(
        "the SASL listener enables OAUTHBEARER and SCRAM-SHA-512 at time {now}"
    )]
    fn sasl_listener_policy(sasl_handshake: &mut SaslHandshakeScenario, now: i64) {
        sasl_handshake.enabled = vec![SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512];
        sasl_handshake.now = now;
    }

    #[rstest_bdd_macros::given("the SASL connection has not authenticated")]
    fn unauthenticated_sasl_connection(sasl_handshake: &mut SaslHandshakeScenario) {
        sasl_handshake.auth = Some(ConnectionAuth::Anonymous);
    }

    #[rstest_bdd_macros::given("the prior OAUTHBEARER session has no expiry")]
    fn unbounded_oauth_session(sasl_handshake: &mut SaslHandshakeScenario) {
        sasl_handshake.auth = Some(authenticated(None));
    }

    #[rstest_bdd_macros::given("the prior OAUTHBEARER session expires at {expiry}")]
    fn bounded_oauth_session(sasl_handshake: &mut SaslHandshakeScenario, expiry: i64) {
        sasl_handshake.auth = Some(authenticated(Some(expiry)));
    }

    #[rstest_bdd_macros::given("an OAUTHBEARER exchange is already negotiating")]
    fn negotiating_oauth_session(sasl_handshake: &mut SaslHandshakeScenario) {
        sasl_handshake.auth = Some(ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::OAuthBearer,
            exchange: SaslExchange::OAuthBearer,
            pending_token_expiry_ms: None,
        });
    }

    #[rstest_bdd_macros::given("the last SASL reauthentication started at {last_start}")]
    fn previous_sasl_start(sasl_handshake: &mut SaslHandshakeScenario, last_start: String) {
        sasl_handshake.last_start = if last_start == "never" {
            None
        } else {
            Some(
                last_start
                    .parse()
                    .expect("a reauthentication time is an integer"),
            )
        };
    }

    #[rstest_bdd_macros::when("the client handshakes with SASL mechanism {mechanism}")]
    fn client_sasl_handshake(sasl_handshake: &mut SaslHandshakeScenario, mechanism: String) {
        let request = SaslHandshakeRequest {
            mechanism,
            ..Default::default()
        };
        let auth = sasl_handshake
            .auth
            .as_mut()
            .expect("a connection state was given");
        sasl_handshake.outcome = Some(handle_handshake(
            &request,
            auth,
            &sasl_handshake.enabled,
            &mut ReauthClock {
                now_ms: sasl_handshake.now,
                last_start_ms: &mut sasl_handshake.last_start,
            },
        ));
    }

    #[rstest_bdd_macros::then("the SASL reply, connection state, and reauthentication clock are:")]
    fn sasl_handshake_outcome(
        sasl_handshake: &SaslHandshakeScenario,
        #[datatable] rows: Rows<HandshakeExpectation>,
    ) {
        let mut rows = rows.into_iter();
        let expected = rows.next().expect("one handshake outcome was specified");
        assert!(rows.next().is_none());
        let mechanisms: Vec<String> =
            serde_json::from_str(&expected.mechanisms).expect("mechanisms are a string array");
        let next = match expected.state.as_str() {
            "Anonymous" => After::Anonymous,
            "Authenticated" => After::Authenticated,
            "Negotiating:OAUTHBEARER" => After::Negotiating(SaslMechanism::OAuthBearer),
            "Reauthenticating:OAUTHBEARER" => After::Reauthenticating(SaslMechanism::OAuthBearer),
            "ReauthBadMechanism" => After::ReauthBadMechanism,
            state => panic!("unknown expected SASL state {state}"),
        };
        let last_after = if expected.last_start == "never" {
            None
        } else {
            Some(
                expected
                    .last_start
                    .parse()
                    .expect("the expected reauthentication time is an integer"),
            )
        };
        let outcome = sasl_handshake
            .outcome
            .as_ref()
            .expect("the handshake was handled");
        assert!(
            outcome.response
                == SaslHandshakeResponse {
                    error_code: expected.code,
                    mechanisms,
                    ..Default::default()
                }
        );
        assert!(outcome.close_after == expected.close);
        assert!(after(sasl_handshake.auth.as_ref().expect("the connection exists")) == next);
        assert!(sasl_handshake.last_start == last_after);
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/network/auth/handshake.feature",
        fixtures = [sasl_handshake: SaslHandshakeScenario]
    );
}
