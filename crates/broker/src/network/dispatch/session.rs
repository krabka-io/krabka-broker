//! Per-connection session state. It derives the initial `ConnectionAuth` for
//! a listener, borrows the connection's principal, holds the read off for the
//! KIP-219 mute window a throttled request earned, and arms the one deadline
//! that races the next frame read: the `connections.max.idle.ms` idle window.
//!
//! The idle deadline is a property of the listener, applies whatever the
//! connection's auth state, and is re-armed from `Instant::now()` on every
//! pass through [`next_connection_frame`] — that is, after every frame read.
//!
//! The KIP-368 SASL session deadline is deliberately not armed here. Kafka
//! runs no timer against it either: an expired session closes on the next
//! request that is not part of a re-authentication, which the dispatch loop
//! enforces through `ConnectionAuth::expired_for_request`.
//!
//! The idle window is armed after the mute rather than before it, so a pause
//! the broker itself imposed to shed load never counts as the client falling
//! silent.
//!
//! The idle window covers a connection only from the moment it reaches this
//! loop. On a TLS listener the handshake that runs before it is held to the
//! same window by `accept::handshake_within_idle_window`, so a peer that opens
//! the socket and never negotiates is reclaimed too.

use bytes::Bytes;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Process-lifetime ANONYMOUS principal.
///
/// `RequestContext` only borrows a `&Principal`, so the defensive fallback can
/// return a `&'static Principal` instead of allocating a fresh `String` and
/// `Vec` for each Produce or Fetch. That fallback covers SASL pre-auth, where
/// `auth.principal()` is `None`. `Principal` carries a `Vec<String>`, so it
/// cannot be a `const`; `LazyLock` builds it once on first use.
static ANONYMOUS_PRINCIPAL: std::sync::LazyLock<krabka_security::Principal> =
    std::sync::LazyLock::new(|| krabka_security::Principal {
        name: "ANONYMOUS".to_string(),
        auth_method: krabka_security::AuthMethod::Anonymous,
        groups: vec![],
    });

/// Borrows the connection's authenticated principal.
///
/// When the connection has no principal yet, the defensive SASL pre-auth case,
/// the function falls back to the shared process-lifetime ANONYMOUS singleton.
/// This avoids a `Principal` clone for each request.
pub(super) fn principal_or_anonymous(
    auth: &crate::network::auth::ConnectionAuth,
) -> &krabka_security::Principal {
    auth.principal().unwrap_or(&ANONYMOUS_PRINCIPAL)
}

/// Returns a future that resolves at `deadline` if it is `Some`, and never
/// resolves if it is `None`. `tokio::select!` uses it to disarm the timer arm
/// for non-OAuth connections, which have no session expiry.
async fn sleep_until_some(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

/// Resolves the connection's starting authentication state.
///
/// A SASL listener starts anonymous and authenticates over the wire. A
/// non-SASL listener is already decided at accept time: the mTLS principal the
/// cert chain produced, or `ANONYMOUS`. The mTLS case is a completed
/// credential presentation with no SASL frame behind it, so it is the one
/// place that writes its own `Authentication` audit row.
pub(super) fn initial_connection_auth(
    is_sasl_listener: bool,
    mtls_principal: Option<krabka_security::Principal>,
    audit_log: &krabka_audit::AuditLog,
    peer: &std::net::SocketAddr,
) -> crate::network::auth::ConnectionAuth {
    if is_sasl_listener {
        return crate::network::auth::ConnectionAuth::Anonymous;
    }
    let principal = match mtls_principal {
        Some(principal) => {
            super::sasl::emit_authentication(
                audit_log,
                peer,
                "SSL",
                super::sasl::audit_principal(&principal),
                krabka_audit::AuditOutcome::Success,
                None,
            );
            principal
        }
        None => krabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        },
    };
    crate::network::auth::ConnectionAuth::Authenticated {
        principal,
        mechanism: krabka_security::SaslMechanism::Plain,
        expires_at_ms: None,
        authenticated_via_token: false,
    }
}

/// What the connection is held to while it waits for its next frame: the
/// listener's idle window, the peer it belongs to, and the metrics handle the
/// close is counted on.
///
/// The idle window is `None` when the listener expires no connection, which is
/// what a non-positive `connections.max.idle.ms` asks for.
pub(super) struct FrameWaitPolicy {
    pub(super) idle: Option<std::time::Duration>,
    pub(super) peer: std::net::SocketAddr,
    pub(super) metrics: crate::metrics::BrokerMetrics,
}

/// Reads the next request frame, after honouring any KIP-219 mute window.
///
/// `mute_until` is the deadline the previous response's throttle earned. Kafka
/// enforces a quota by muting the connection: the response is already on the
/// wire, and the broker simply stops reading requests until the window closes.
/// Holding the read back here — rather than delaying the write — is what keeps
/// a throttled client from timing out its in-flight request and retrying into
/// the quota that is shedding load.
///
/// The idle window is armed once the mute has drained, so the broker's own
/// backpressure never spends the client's idle budget. The KIP-368 session
/// deadline is not armed here at all: Kafka closes an expired session on the
/// request that arrives past it, which the dispatch loop does with
/// `ConnectionAuth::expired_for_request`.
pub(super) async fn next_connection_frame<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    auth: &crate::network::auth::ConnectionAuth,
    mute_until: Option<tokio::time::Instant>,
    policy: &FrameWaitPolicy,
) -> Option<Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::metrics::ConnectionCloseReason;

    if let Some(mute_until) = mute_until {
        tokio::time::sleep_until(mute_until).await;
    }
    // Armed after the mute has drained, so every frame read resets the idle
    // window and a pause the broker imposed is not charged to the client.
    let idle_deadline = policy
        .idle
        .map(|window| tokio::time::Instant::now() + window);
    let frame_result = tokio::select! {
        biased;
        () = sleep_until_some(idle_deadline) => {
            tracing::info!(
                principal = principal_or_anonymous(auth).name.as_str(),
                peer = %policy.peer,
                idle_ms = policy.idle.unwrap_or_default().as_millis(),
                "connection idle past connections.max.idle.ms, closing"
            );
            policy
                .metrics
                .record_connection_close(ConnectionCloseReason::Idle);
            return None;
        },
        next = framed.next() => next,
    };
    match frame_result {
        Some(Ok(bytes)) => Some(bytes.freeze()),
        Some(Err(error)) => {
            tracing::warn!(%error, peer = %policy.peer, "frame decode error, closing");
            policy
                .metrics
                .record_connection_close(ConnectionCloseReason::DecodeError);
            None
        }
        None => {
            policy
                .metrics
                .record_connection_close(ConnectionCloseReason::PeerClosed);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    /// The mTLS binding is the one authentication with no SASL frame behind
    /// it, so `initial_connection_auth` is the only place that can record it.
    /// The anonymous and SASL starts presented no credential and must record
    /// nothing.
    #[test]
    fn initial_connection_auth_audits_the_mtls_binding_only() {
        let peer: std::net::SocketAddr = "192.0.2.7:9093".parse().expect("peer addr");
        let cert_dn = krabka_security::Principal {
            name: "CN=test-client,OU=integration,O=crabka".to_string(),
            auth_method: krabka_security::AuthMethod::MTls,
            groups: vec![],
        };

        for (what, is_sasl, mtls) in [
            ("a SASL listener", true, Some(cert_dn.clone())),
            ("an SSL listener with no client cert", false, None),
        ] {
            let (log, mut rx) = krabka_audit::AuditLog::new(8);
            let _auth = initial_connection_auth(is_sasl, mtls, log.as_ref(), &peer);
            assert!(rx.try_recv().is_err(), "{what} presented no credential");
        }

        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        let auth = initial_connection_auth(false, Some(cert_dn), log.as_ref(), &peer);
        assert!(
            auth.principal().map(|p| p.name.as_str())
                == Some("CN=test-client,OU=integration,O=crabka")
        );

        let event = rx.try_recv().expect("the mTLS authentication row");
        let krabka_audit::AuditEvent::Authentication { time_ms, .. } = event else {
            panic!("expected an Authentication event, got {event:?}");
        };
        assert!(
            event
                == krabka_audit::AuditEvent::Authentication {
                    outcome: krabka_audit::AuditOutcome::Success,
                    mechanism: "SSL".to_string(),
                    principal: krabka_audit::AuditPrincipal {
                        name: "User:CN=test-client,OU=integration,O=crabka".to_string(),
                        auth_method: "MTls".to_string(),
                    },
                    source: krabka_audit::AuditEndpoint {
                        ip: "192.0.2.7".to_string(),
                        port: 9093,
                    },
                    reason: None,
                    time_ms,
                }
        );
    }

    // `start_paused = true` runs these on tokio's virtual clock: with no other
    // work pending, the runtime auto-advances logical time to the next timer, so
    // the `sleep_until`/`timeout` deadlines fire instantly and deterministically
    // instead of burning real wall-clock milliseconds.
    #[tokio::test(start_paused = true)]
    async fn sleep_until_some_none_remains_pending() {
        // `None` never resolves; the 10ms timeout is the only timer, so virtual
        // time jumps to it and the timeout elapses -> Err.
        let result = tokio::time::timeout(Duration::from_millis(10), sleep_until_some(None)).await;
        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_until_some_some_waits_until_deadline() {
        let before = tokio::time::Instant::now();
        let deadline = before + Duration::from_millis(10);
        // The inner sleep (deadline) fires before the outer 1s timeout, so the
        // timeout resolves Ok and virtual time has advanced exactly to `deadline`.
        tokio::time::timeout(Duration::from_secs(1), sleep_until_some(Some(deadline)))
            .await
            .expect("deadline should resolve");
        assert!(tokio::time::Instant::now() >= deadline);
    }
}
