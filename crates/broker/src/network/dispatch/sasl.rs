//! Inline handling of the two SASL request frames. `SaslHandshake` (17) and
//! `SaslAuthenticate` (36) mutate the per-connection authentication state,
//! which the handler registry cannot reach, so the dispatch loop intercepts
//! them here before it consults the registry.

use std::net::SocketAddr;

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
    max_reauth: Option<krabka_units::Time>,
    peer: &SocketAddr,
) -> Option<Result<SaslFrameOutcome, BrokerError>> {
    let api_key = parsed.api_key;
    if api_key != SASL_HANDSHAKE_KEY && api_key != SASL_AUTHENTICATE_KEY {
        return None;
    }
    Some(handle_sasl_frame(broker, parsed, auth, sasl_mechanisms, max_reauth, peer).await)
}

async fn handle_sasl_frame(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &mut crate::network::auth::ConnectionAuth,
    sasl_mechanisms: &[krabka_security::SaslMechanism],
    max_reauth: Option<krabka_units::Time>,
    peer: &SocketAddr,
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
            let mech_opt = auth.negotiated_mechanism();
            let resp = if let Some(mech) = mech_opt {
                match mech {
                    krabka_security::SaslMechanism::Plain => {
                        crate::network::auth::handle_authenticate_plain(
                            &req,
                            auth,
                            broker.config.plain_credentials.as_map(),
                            max_reauth,
                        )
                    }
                    krabka_security::SaslMechanism::ScramSha256
                    | krabka_security::SaslMechanism::ScramSha512 => {
                        crate::network::auth::handle_authenticate_scram(
                            &req,
                            auth,
                            &*broker.controller,
                            max_reauth,
                        )
                    }
                    krabka_security::SaslMechanism::OAuthBearer => {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
                        crate::network::auth::handle_authenticate_oauthbearer_with_jwks_cache(
                            &req,
                            auth,
                            &broker.config.oauthbearer_validator,
                            &broker.config.oauthbearer_jwks_cache_generation,
                            &broker.config.oauthbearer_jwks_last_successful_fetch_ms,
                            now_ms,
                            max_reauth,
                        )
                        .await
                    }
                    krabka_security::SaslMechanism::Gssapi => {
                        let cfg = broker
                            .config
                            .gssapi
                            .as_ref()
                            .expect("GSSAPI enabled without config");
                        crate::network::auth::handle_authenticate_gssapi(
                            &req, auth, cfg, max_reauth,
                        )
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
            let ok = resp.error_code == 0;
            broker.metrics.record_authentication(mech_label, ok);
            // One audit row per *completed* exchange, initial or KIP-368
            // re-auth alike. A multi-round mechanism answers its intermediate
            // rounds with `error_code == 0` and no principal yet, so a zero
            // code only completes the exchange once the connection is
            // authenticated. A non-zero code always ends it.
            if !ok || auth.is_authenticated() {
                emit_authentication(
                    &broker.audit_log,
                    peer,
                    mech_label,
                    auth.principal()
                        .map_or_else(|| claimed_principal(mech_opt, &req), audit_principal),
                    if ok {
                        krabka_audit::AuditOutcome::Success
                    } else {
                        krabka_audit::AuditOutcome::Failure
                    },
                    resp.error_message.clone(),
                );
            }
            let close = !ok;
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

/// Writes one `Authentication` audit row.
///
/// Every credential presentation the broker completes goes through here: a
/// `SaslAuthenticate` exchange that ended, the pre-auth gate that refused a
/// frame outright, and the mTLS binding a non-SASL listener does at accept
/// time. `source` is built the same way [`crate::handlers::admin_audit`]
/// builds it, so an auditor can join an authentication row to the
/// privileged-action rows the same session went on to write.
pub(crate) fn emit_authentication(
    audit_log: &krabka_audit::AuditLog,
    peer: &SocketAddr,
    mechanism: &str,
    principal: krabka_audit::AuditPrincipal,
    outcome: krabka_audit::AuditOutcome,
    reason: Option<String>,
) {
    audit_log.emit(krabka_audit::AuditEvent::Authentication {
        outcome,
        mechanism: mechanism.to_string(),
        principal,
        source: krabka_audit::AuditEndpoint {
            ip: peer.ip().to_string(),
            port: peer.port(),
        },
        reason,
        time_ms: crate::time_util::now_ms(),
    });
}

/// Renders a resolved [`krabka_security::Principal`] as an audit principal.
///
/// The name is the `User:<name>` Kafka form, which is what
/// `break_glass::handlers::principal_name` puts on a `PrivilegedAction` row.
/// An auditor joins the two by that string, so the two sites have to spell a
/// principal the same way.
pub(crate) fn audit_principal(
    principal: &krabka_security::Principal,
) -> krabka_audit::AuditPrincipal {
    krabka_audit::AuditPrincipal {
        name: principal.to_kafka().to_string(),
        auth_method: format!("{:?}", principal.auth_method),
    }
}

/// Names the identity a *failed* exchange claimed, where the connection state
/// resolved no principal.
///
/// PLAIN is the one mechanism that sends the user in the clear
/// (`\0<authzid>\0<authcid>\0<password>`), so its failure row can still say
/// who was refused. Every other mechanism keeps the identity inside its own
/// challenge/response and its failure row names no user.
// ponytail: PLAIN only. SCRAM's client-first `n=<user>` would need a GS2
// header parser here if a failed SCRAM row ever has to name the claimed user.
fn claimed_principal(
    mechanism: Option<krabka_security::SaslMechanism>,
    req: &krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest,
) -> krabka_audit::AuditPrincipal {
    let name = match mechanism {
        Some(krabka_security::SaslMechanism::Plain) => req
            .auth_bytes
            .split(|&b| b == 0)
            .nth(1)
            .and_then(|user| std::str::from_utf8(user).ok())
            // The same `User:<name>` form [`audit_principal`] renders, so a
            // failed row and a successful one name one person identically.
            .map(|user| format!("User:{user}"))
            .unwrap_or_default(),
        _ => String::new(),
    };
    krabka_audit::AuditPrincipal {
        name,
        auth_method: format!(
            "{:?}",
            mechanism.map_or(krabka_security::AuthMethod::Anonymous, |m| {
                krabka_security::AuthMethod::from_sasl(m)
            })
        ),
    }
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

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_security::SaslMechanism;

    use super::*;

    fn request(
        auth_bytes: &[u8],
    ) -> krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
        krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
            auth_bytes: Bytes::copy_from_slice(auth_bytes),
            ..Default::default()
        }
    }

    /// A failed exchange resolved no principal, so the audit row can only name
    /// what the peer claimed. PLAIN is the one mechanism that says it in the
    /// clear, and the field the row must name is the *authcid* — the field
    /// after it is the password, which must never reach an audit sink. Every
    /// other mechanism keeps the identity inside its own challenge/response,
    /// so its row names nobody rather than guessing.
    #[test]
    fn a_refused_login_names_the_claimed_plain_user_and_never_the_password() {
        let cases = [
            (
                "plain names the authcid",
                Some(SaslMechanism::Plain),
                request(b"\0alice\0wonderland"),
                "User:alice",
                "SaslPlain",
            ),
            (
                "plain with an authzid still names the authcid",
                Some(SaslMechanism::Plain),
                request(b"admin\0alice\0wonderland"),
                "User:alice",
                "SaslPlain",
            ),
            (
                "a plain payload with no fields names nobody",
                Some(SaslMechanism::Plain),
                request(b"garbage"),
                "",
                "SaslPlain",
            ),
            (
                "a non-utf8 plain username names nobody",
                Some(SaslMechanism::Plain),
                request(&[0, 0xff, 0xfe, 0, b'p', b'w']),
                "",
                "SaslPlain",
            ),
            (
                "scram keeps its identity to itself",
                Some(SaslMechanism::ScramSha512),
                request(b"n,,n=alice,r=nonce"),
                "",
                "SaslScramSha512",
            ),
            (
                "no handshake ran at all",
                None,
                request(b"\0alice\0wonderland"),
                "",
                "Anonymous",
            ),
        ];
        for (case, mechanism, req, name, auth_method) in cases {
            check!(
                claimed_principal(mechanism, &req)
                    == krabka_audit::AuditPrincipal {
                        name: name.to_string(),
                        auth_method: auth_method.to_string(),
                    },
                "{case}"
            );
        }
    }

    /// An auditor joins an authentication row to the privileged-action rows
    /// the same session went on to write, by the principal string. That join
    /// only holds while this renders the `User:<name>` Kafka form that
    /// `break_glass::handlers::principal_name` puts on the other side.
    #[test]
    fn a_resolved_principal_is_audited_in_the_kafka_user_form() {
        let principal = krabka_security::Principal {
            name: "alice".to_string(),
            auth_method: krabka_security::AuthMethod::SaslScramSha512,
            groups: vec![],
        };
        assert!(
            audit_principal(&principal)
                == krabka_audit::AuditPrincipal {
                    name: "User:alice".to_string(),
                    auth_method: "SaslScramSha512".to_string(),
                }
        );
    }

    /// The refusal an operator has to be able to see after the fact: one
    /// `Authentication` row, `Failure`, naming the mechanism, the user the
    /// peer claimed, the peer's own address, and the reason.
    #[test]
    fn a_refused_plain_login_emits_one_failure_row_for_the_claimed_user() {
        let peer: SocketAddr = "192.0.2.9:9092".parse().expect("peer addr");
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        emit_authentication(
            log.as_ref(),
            &peer,
            SaslMechanism::Plain.wire_name(),
            claimed_principal(Some(SaslMechanism::Plain), &request(b"\0alice\0wonderland")),
            krabka_audit::AuditOutcome::Failure,
            Some("authentication failed".to_string()),
        );

        let event = rx.try_recv().expect("the failed authentication row");
        let krabka_audit::AuditEvent::Authentication { time_ms, .. } = event else {
            panic!("expected an Authentication event, got {event:?}");
        };
        assert!(
            event
                == krabka_audit::AuditEvent::Authentication {
                    outcome: krabka_audit::AuditOutcome::Failure,
                    mechanism: "PLAIN".to_string(),
                    principal: krabka_audit::AuditPrincipal {
                        name: "User:alice".to_string(),
                        auth_method: "SaslPlain".to_string(),
                    },
                    source: krabka_audit::AuditEndpoint {
                        ip: "192.0.2.9".to_string(),
                        port: 9092,
                    },
                    reason: Some("authentication failed".to_string()),
                    time_ms,
                }
        );
        assert!(rx.try_recv().is_err(), "exactly one row per refusal");
    }

    fn parsed(
        api_key: ApiKeyCode,
        api_version: i16,
        body: &[u8],
        body_flexible: bool,
    ) -> crate::network::request::ParsedRequest<'_> {
        crate::network::request::ParsedRequest {
            api_key,
            api_version,
            correlation_id: 7,
            body,
            body_flexible,
            client_id: None,
        }
    }

    fn encode_body<T: krabka_protocol::Encode>(req: &T, version: i16) -> BytesMut {
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request body");
        buf
    }

    /// Strips the `ResponseHeader` an interception framed: `correlation_id`,
    /// plus the tagged-fields byte a flexible response carries.
    fn authenticate_response(
        bytes: &Bytes,
    ) -> krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse {
        use krabka_protocol::Decode as _;
        let mut cur = &bytes[5..];
        krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse::decode(
            &mut cur, 2,
        )
        .expect("decode the framed SaslAuthenticate response")
    }

    fn plain_payload(user: &str, password: &str) -> BytesMut {
        let mut bytes = vec![0_u8];
        bytes.extend_from_slice(user.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(password.as_bytes());
        encode_body(
            &krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
                auth_bytes: Bytes::from(bytes),
                ..Default::default()
            },
            2,
        )
    }

    /// The interception boundary and the close decision, which are the two
    /// things the dispatch loop takes from this module.
    ///
    /// Only 17 and 36 may be taken out of the registry's hands — intercepting
    /// anything else would swallow the whole data plane. A refused credential
    /// and a `SaslAuthenticate` with no handshake behind it both have to set
    /// the close flag, because a peer that may keep guessing on the same
    /// connection is an open credential-stuffing window; a successful round
    /// must not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sasl_frames_are_intercepted_and_only_a_refusal_closes_the_connection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = crate::BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.plain_credentials
            .insert("alice".to_string(), "wonderland".to_string());
        let handle = crate::Broker::start(cfg).await.expect("broker start");
        let broker = std::sync::Arc::clone(handle.broker_for_test());
        let peer: SocketAddr = "192.0.2.9:9092".parse().expect("peer addr");
        let mechanisms = [SaslMechanism::Plain];

        let mut auth = crate::network::auth::ConnectionAuth::Anonymous;

        // A non-SASL api_key is left to the registry.
        let metadata = encode_body(
            &krabka_protocol::owned::metadata_request::MetadataRequest::default(),
            12,
        );
        check!(
            try_handle_sasl_frame(
                &broker,
                &parsed(3, 12, &metadata, true),
                &mut auth,
                &mechanisms,
                None,
                &peer,
            )
            .await
            .is_none()
        );

        // SaslHandshake opens the exchange and keeps the connection.
        let handshake = encode_body(
            &krabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest {
                mechanism: "PLAIN".to_string(),
                ..Default::default()
            },
            1,
        );
        let outcome = try_handle_sasl_frame(
            &broker,
            &parsed(SASL_HANDSHAKE_KEY, 1, &handshake, false),
            &mut auth,
            &mechanisms,
            None,
            &peer,
        )
        .await
        .expect("17 is intercepted")
        .expect("handshake encodes");
        check!(!outcome.close_after);
        check!(auth.negotiated_mechanism() == Some(SaslMechanism::Plain));

        // The credential that matches authenticates and keeps the connection.
        let good = plain_payload("alice", "wonderland");
        let outcome = try_handle_sasl_frame(
            &broker,
            &parsed(SASL_AUTHENTICATE_KEY, 2, &good, true),
            &mut auth,
            &mechanisms,
            None,
            &peer,
        )
        .await
        .expect("36 is intercepted")
        .expect("authenticate encodes");
        check!(!outcome.close_after);
        check!(authenticate_response(&outcome.response_bytes).error_code == 0);
        check!(auth.principal().map(|p| p.name.as_str()) == Some("alice"));

        // A credential that does not match closes the connection.
        let mut guessing = crate::network::auth::ConnectionAuth::Anonymous;
        try_handle_sasl_frame(
            &broker,
            &parsed(SASL_HANDSHAKE_KEY, 1, &handshake, false),
            &mut guessing,
            &mechanisms,
            None,
            &peer,
        )
        .await
        .expect("17 is intercepted")
        .expect("handshake encodes");
        let bad = plain_payload("alice", "hunter2");
        let outcome = try_handle_sasl_frame(
            &broker,
            &parsed(SASL_AUTHENTICATE_KEY, 2, &bad, true),
            &mut guessing,
            &mechanisms,
            None,
            &peer,
        )
        .await
        .expect("36 is intercepted")
        .expect("authenticate encodes");
        check!(outcome.close_after, "a refused credential must close");
        check!(
            authenticate_response(&outcome.response_bytes).error_code
                == codes::SASL_AUTHENTICATION_FAILED
        );
        check!(!guessing.is_authenticated());

        // A SaslAuthenticate with no handshake behind it is ILLEGAL_SASL_STATE
        // and closes, rather than being fed to some default mechanism.
        let mut bare = crate::network::auth::ConnectionAuth::Anonymous;
        let outcome = try_handle_sasl_frame(
            &broker,
            &parsed(SASL_AUTHENTICATE_KEY, 2, &good, true),
            &mut bare,
            &mechanisms,
            None,
            &peer,
        )
        .await
        .expect("36 is intercepted")
        .expect("authenticate encodes");
        check!(outcome.close_after);
        let resp = authenticate_response(&outcome.response_bytes);
        check!(resp.error_code == codes::ILLEGAL_SASL_STATE);
        check!(
            resp.error_message.as_deref() == Some("SaslAuthenticate without prior SaslHandshake")
        );
        check!(!bare.is_authenticated());

        // The frame is routed by the mechanism the handshake named, not by a
        // default one: a SCRAM client-first reaches the SCRAM handler, which
        // knows no such user and refuses.
        let mut scram = crate::network::auth::ConnectionAuth::Anonymous;
        let scram_handshake = encode_body(
            &krabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest {
                mechanism: "SCRAM-SHA-512".to_string(),
                ..Default::default()
            },
            1,
        );
        try_handle_sasl_frame(
            &broker,
            &parsed(SASL_HANDSHAKE_KEY, 1, &scram_handshake, false),
            &mut scram,
            &[SaslMechanism::ScramSha512],
            None,
            &peer,
        )
        .await
        .expect("17 is intercepted")
        .expect("handshake encodes");
        check!(scram.negotiated_mechanism() == Some(SaslMechanism::ScramSha512));
        let client_first = encode_body(
            &krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
                auth_bytes: Bytes::from_static(b"n,,n=nobody,r=clientnonce"),
                ..Default::default()
            },
            2,
        );
        let outcome = try_handle_sasl_frame(
            &broker,
            &parsed(SASL_AUTHENTICATE_KEY, 2, &client_first, true),
            &mut scram,
            &[SaslMechanism::ScramSha512],
            None,
            &peer,
        )
        .await
        .expect("36 is intercepted")
        .expect("authenticate encodes");
        check!(outcome.close_after);
        check!(
            authenticate_response(&outcome.response_bytes).error_code
                == codes::SASL_AUTHENTICATION_FAILED
        );
        check!(!scram.is_authenticated());

        handle.shutdown().await;
    }
}
