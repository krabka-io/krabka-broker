//! Server-side SASL negotiation for the controller listener.
//!
//! The loop here drives the `network::auth` state machine one Kafka frame at
//! a time: it answers a pre-auth `ApiVersions`, runs `SaslHandshake` to pick
//! a mechanism, and then runs as many `SaslAuthenticate` rounds as the
//! mechanism needs. It returns the authenticated `Principal` to the caller,
//! which authorizes it before the raft engine takes the connection.

use std::net::SocketAddr;

use krabka_client_core::ClientDuplex;
use krabka_protocol::{
    Decode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
    },
};
use krabka_raft::RaftHandshakeError;
use krabka_security::SaslMechanism;

use super::{
    API_KEY_API_VERSIONS, API_KEY_SASL_AUTHENTICATE, API_KEY_SASL_HANDSHAKE, BrokerRaftHandshake,
    api_versions::pre_auth_api_versions_response,
    frame::{read_kafka_request, write_response},
};
use crate::network::auth::{
    ConnectionAuth, SaslExchange, handle_authenticate_gssapi, handle_authenticate_oauthbearer,
    handle_authenticate_plain, handle_authenticate_scram, handle_handshake, is_pre_auth_allowed,
};

/// Initial per-connection auth state for an unauthenticated SASL peer.
fn pre_auth_state() -> ConnectionAuth {
    ConnectionAuth::Anonymous
}

/// The re-auth cap the controller listener advertises: none.
///
/// KIP-368 expiry is enforced lazily and per request by the data-plane
/// dispatch loop, which closes a connection when a non-SASL api arrives on an
/// expired session (`ConnectionAuth::expired_for_request`). This handshake
/// hands the raw stream to the raft engine as soon as SASL completes, so no
/// later frame passes an expiry check and a peer's re-authentication frames
/// would never reach this loop again. A finite `session_lifetime_ms` here
/// would therefore be a deadline the broker never enforces, so the response
/// advertises none.
const CONTROLLER_MAX_REAUTH: Option<krabka_units::Time> = None;

/// Writes one `Authentication` audit row for a completed controller SASL
/// exchange, so controller and inter-broker logins join the same audit trail
/// as the data plane's.
///
/// The principal is spelled in the `User:<name>` Kafka form, the same one
/// `network::dispatch::sasl` and the `PrivilegedAction` rows use, so an
/// auditor can join them.
fn emit_authentication(
    cfg: &BrokerRaftHandshake,
    peer: &SocketAddr,
    mechanism: SaslMechanism,
    principal: Option<&krabka_security::Principal>,
    outcome: krabka_audit::AuditOutcome,
    reason: Option<String>,
) {
    // The audit pipeline is built after the controller listener starts
    // accepting, so the cell is empty for the connections that arrive first.
    let Some(audit_log) = cfg.audit_log.get() else {
        return;
    };
    // A refused exchange resolved no principal, so its row names no user and
    // carries the mechanism's own auth method instead.
    let principal = principal.map_or_else(
        || krabka_audit::AuditPrincipal {
            name: String::new(),
            auth_method: format!("{:?}", krabka_security::AuthMethod::from_sasl(mechanism)),
        },
        crate::network::dispatch::sasl::audit_principal,
    );
    crate::network::dispatch::sasl::emit_authentication(
        audit_log,
        peer,
        mechanism.wire_name(),
        principal,
        outcome,
        reason,
    );
}

/// Drives the server-side SASL state machine until the connection
/// authenticates or the function writes an error response.
///
/// The loop invariant is that every iteration reads exactly one Kafka request
/// frame and writes exactly one response frame. The `auth` state machine,
/// `network::auth::ConnectionAuth`, carries continuation state across SCRAM
/// rounds.
///
/// The function returns the authenticated [`Principal`] and whether a
/// delegation token supplied the credential once
/// `auth.is_authenticated()` holds, so that `upgrade` can authorize it. It
/// returns `Err(...)` if the peer sent an unexpected frame or the auth
/// failed.
pub(super) async fn run_inbound_sasl(
    stream: &mut dyn ClientDuplex,
    cfg: &BrokerRaftHandshake,
    peer: &SocketAddr,
) -> Result<(krabka_security::Principal, bool), RaftHandshakeError> {
    let mut auth = pre_auth_state();
    loop {
        let (api_key, api_version, corr_id, body) =
            read_kafka_request(stream, cfg.max_frame_bytes).await?;
        if !is_pre_auth_allowed(api_key) && !auth.is_authenticated() {
            return Err(RaftHandshakeError::Sasl(format!(
                "pre-auth request api_key={api_key} rejected"
            )));
        }
        match api_key {
            // ApiVersions — minimal response so peers that send it first
            // (typical JVM client pattern) can proceed. Our
            // `InterBrokerClient` outbound path skips ApiVersions, so this
            // path exists for JVM-client tolerance only.
            API_KEY_API_VERSIONS => {
                let mut cur = body.as_slice();
                ApiVersionsRequest::decode(&mut cur, api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                let resp = pre_auth_api_versions_response();
                write_response(stream, api_key, api_version, corr_id, &resp).await?;
            }
            API_KEY_SASL_HANDSHAKE => {
                let mut cur = body.as_slice();
                let req = SaslHandshakeRequest::decode(&mut cur, api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                let resp = handle_handshake(&req, &mut auth, &cfg.enabled_sasl_mechanisms);
                let error_code = resp.error_code;
                write_response(stream, api_key, api_version, corr_id, &resp).await?;
                if error_code != 0 {
                    return Err(RaftHandshakeError::Sasl(format!(
                        "handshake error_code={error_code}"
                    )));
                }
            }
            API_KEY_SASL_AUTHENTICATE => {
                let mut cur = body.as_slice();
                let req = SaslAuthenticateRequest::decode(&mut cur, api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                let mech = match &auth {
                    ConnectionAuth::Negotiating { mechanism, .. } => *mechanism,
                    _ => {
                        return Err(RaftHandshakeError::Sasl(
                            "authenticate before handshake".into(),
                        ));
                    }
                };
                let resp = match mech {
                    SaslMechanism::Plain => handle_authenticate_plain(
                        &req,
                        &mut auth,
                        &cfg.plain_credentials,
                        CONTROLLER_MAX_REAUTH,
                    ),
                    SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                        let controller = cfg.controller.get().ok_or_else(|| {
                            RaftHandshakeError::Sasl(
                                "controller handle not initialised for SCRAM lookup".into(),
                            )
                        })?;
                        handle_authenticate_scram(
                            &req,
                            &mut auth,
                            controller.as_ref(),
                            CONTROLLER_MAX_REAUTH,
                        )
                    }
                    SaslMechanism::OAuthBearer => {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |duration| {
                                i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
                            });
                        handle_authenticate_oauthbearer(
                            &req,
                            &mut auth,
                            &cfg.oauthbearer_validator,
                            now_ms,
                            CONTROLLER_MAX_REAUTH,
                        )
                        .await
                    }
                    SaslMechanism::Gssapi => {
                        let config = cfg.gssapi.as_ref().ok_or_else(|| {
                            RaftHandshakeError::Sasl(
                                "GSSAPI enabled on controller listener without configuration"
                                    .into(),
                            )
                        })?;
                        handle_authenticate_gssapi(&req, &mut auth, config, CONTROLLER_MAX_REAUTH)
                    }
                };
                let error_code = resp.error_code;
                write_response(stream, api_key, api_version, corr_id, &resp).await?;
                if error_code != 0 {
                    emit_authentication(
                        cfg,
                        peer,
                        mech,
                        auth.principal(),
                        krabka_audit::AuditOutcome::Failure,
                        resp.error_message.clone(),
                    );
                    return Err(RaftHandshakeError::Sasl(format!(
                        "authenticate error_code={error_code}"
                    )));
                }
                if let ConnectionAuth::Authenticated {
                    principal,
                    authenticated_via_token,
                    ..
                } = &auth
                {
                    emit_authentication(
                        cfg,
                        peer,
                        mech,
                        Some(principal),
                        krabka_audit::AuditOutcome::Success,
                        None,
                    );
                    return Ok((principal.clone(), *authenticated_via_token));
                }
                // Multi-round mechanisms and the RFC 7628 rejection exchange
                // loop for the next `SaslAuthenticate` frame.
                assert2::assert!(
                    matches!(
                        auth,
                        ConnectionAuth::Negotiating {
                            exchange: SaslExchange::Scram(_)
                                | SaslExchange::OAuthBearerFailed
                                | SaslExchange::Gssapi(_),
                            ..
                        }
                    ),
                    "expected SASL continuation after non-authenticated success"
                );
            }
            other => {
                return Err(RaftHandshakeError::Protocol(format!(
                    "unexpected api_key={other} during handshake"
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::{
        api_versions_response::ApiVersionsResponse,
        sasl_authenticate_response::SaslAuthenticateResponse,
    };
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::raft_handshake::test_support::{
        api_versions_body, read_response_frame, request_frame, sasl_authenticate_body,
        sasl_handshake_body, sasl_test_config,
    };

    fn test_peer() -> SocketAddr {
        "192.0.2.11:9093".parse().expect("peer addr")
    }

    /// Drives one PLAIN exchange (handshake then authenticate) against
    /// `run_inbound_sasl` and returns the decoded `SaslAuthenticate` response
    /// with the loop's outcome.
    async fn plain_login(
        cfg: BrokerRaftHandshake,
        user: &str,
        password: &str,
    ) -> (
        SaslAuthenticateResponse,
        Result<(krabka_security::Principal, bool), RaftHandshakeError>,
    ) {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let task =
            tokio::spawn(async move { run_inbound_sasl(&mut server, &cfg, &test_peer()).await });
        client
            .write_all(&request_frame(
                API_KEY_SASL_HANDSHAKE,
                1,
                1,
                Some(b"c"),
                false,
                &sasl_handshake_body(),
            ))
            .await
            .expect("write handshake");
        let handshake = read_response_frame(&mut client).await;
        assert!(&handshake[4..6] == &0i16.to_be_bytes());
        client
            .write_all(&request_frame(
                API_KEY_SASL_AUTHENTICATE,
                2,
                2,
                Some(b"c"),
                true,
                &sasl_authenticate_body(user, password),
            ))
            .await
            .expect("write authenticate");
        let frame = read_response_frame(&mut client).await;
        // correlation_id (4 bytes) + the flexible header's tagged-fields byte.
        let mut body = &frame[5..];
        let resp =
            SaslAuthenticateResponse::decode(&mut body, 2).expect("decode authenticate response");
        (resp, task.await.expect("server task"))
    }

    /// The controller path hands the raw stream to the raft engine, so it must
    /// not promise a re-auth deadline nothing later enforces.
    #[tokio::test]
    async fn run_inbound_sasl_advertises_no_session_lifetime() {
        let (resp, outcome) = plain_login(sasl_test_config(), "broker", "secret").await;
        assert!(resp.error_code == 0);
        assert!(resp.session_lifetime_ms == 0);
        assert!(outcome.is_ok());
    }

    #[tokio::test]
    async fn run_inbound_sasl_audits_the_successful_controller_login() {
        let cfg = sasl_test_config();
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        cfg.audit_log.set(log).expect("audit cell unset");

        let (resp, outcome) = plain_login(cfg, "broker", "secret").await;
        assert!(resp.error_code == 0);
        assert!(outcome.is_ok());

        let event = rx.try_recv().expect("the controller authentication row");
        let krabka_audit::AuditEvent::Authentication { time_ms, .. } = event else {
            panic!("expected an Authentication event, got {event:?}");
        };
        assert!(
            event
                == krabka_audit::AuditEvent::Authentication {
                    outcome: krabka_audit::AuditOutcome::Success,
                    mechanism: "PLAIN".to_string(),
                    principal: krabka_audit::AuditPrincipal {
                        name: "User:broker".to_string(),
                        auth_method: "SaslPlain".to_string(),
                    },
                    source: krabka_audit::AuditEndpoint {
                        ip: "192.0.2.11".to_string(),
                        port: 9093,
                    },
                    reason: None,
                    time_ms,
                }
        );
    }

    #[tokio::test]
    async fn run_inbound_sasl_audits_the_failed_controller_login() {
        let cfg = sasl_test_config();
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        cfg.audit_log.set(log).expect("audit cell unset");

        let (resp, outcome) = plain_login(cfg, "broker", "wrong").await;
        assert!(resp.error_code != 0);
        assert!(outcome.is_err());

        let event = rx.try_recv().expect("the controller authentication row");
        let krabka_audit::AuditEvent::Authentication { time_ms, .. } = event else {
            panic!("expected an Authentication event, got {event:?}");
        };
        assert!(
            event
                == krabka_audit::AuditEvent::Authentication {
                    outcome: krabka_audit::AuditOutcome::Failure,
                    mechanism: "PLAIN".to_string(),
                    principal: krabka_audit::AuditPrincipal {
                        name: String::new(),
                        auth_method: "SaslPlain".to_string(),
                    },
                    source: krabka_audit::AuditEndpoint {
                        ip: "192.0.2.11".to_string(),
                        port: 9093,
                    },
                    reason: resp.error_message.clone(),
                    time_ms,
                }
        );
    }

    #[tokio::test]
    async fn run_inbound_sasl_allows_api_versions_before_plain_authentication() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move {
            let cfg = sasl_test_config();
            run_inbound_sasl(&mut server, &cfg, &test_peer()).await
        });

        client
            .write_all(&request_frame(
                API_KEY_API_VERSIONS,
                3,
                1,
                Some(b"c"),
                true,
                &api_versions_body(3),
            ))
            .await
            .expect("write api versions");
        let api_versions = read_response_frame(&mut client).await;
        assert!(&api_versions[0..4] == &1i32.to_be_bytes());
        let mut api_versions_body = &api_versions[4..];
        let response = ApiVersionsResponse::decode(&mut api_versions_body, 3)
            .expect("decode api versions v3 response");
        assert!(api_versions_body.is_empty());
        assert!(response.api_keys.len() == 3);

        client
            .write_all(&request_frame(
                API_KEY_SASL_HANDSHAKE,
                1,
                2,
                Some(b"c"),
                false,
                &sasl_handshake_body(),
            ))
            .await
            .expect("write handshake");
        let handshake = read_response_frame(&mut client).await;
        assert!(&handshake[0..4] == &2i32.to_be_bytes());
        assert!(&handshake[4..6] == &0i16.to_be_bytes());

        client
            .write_all(&request_frame(
                API_KEY_SASL_AUTHENTICATE,
                2,
                3,
                Some(b"c"),
                true,
                &sasl_authenticate_body("broker", "secret"),
            ))
            .await
            .expect("write authenticate");
        let authenticate = read_response_frame(&mut client).await;
        // corr_id 3 BE + empty tagged-fields byte (flexible header) +
        // error_code 0.
        assert!(authenticate[0..7] == [0, 0, 0, 3, 0, 0, 0]);

        let (principal, via_token) = server.await.expect("server task").expect("authenticated");
        assert!(principal.name == "broker");
        assert!(principal.auth_method == krabka_security::AuthMethod::SaslPlain);
        assert!(!via_token);
    }

    #[tokio::test]
    async fn run_inbound_sasl_rejects_disallowed_request_before_authentication() {
        let (mut client, mut server) = tokio::io::duplex(128);
        let server = tokio::spawn(async move {
            let cfg = sasl_test_config();
            run_inbound_sasl(&mut server, &cfg, &test_peer()).await
        });
        client
            .write_all(&request_frame(1, 0, 1, Some(b"c"), false, b""))
            .await
            .expect("write forbidden request");

        let err = server
            .await
            .expect("server task")
            .expect_err("pre-auth request rejected");
        assert!(
            matches!(err, RaftHandshakeError::Sasl(msg) if msg.contains("pre-auth request api_key=1 rejected"))
        );
    }
}
