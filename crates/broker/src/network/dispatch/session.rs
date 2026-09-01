//! Per-connection session state. It derives the initial `ConnectionAuth` for
//! a listener, borrows the connection's principal, and arms the two deadlines
//! that race the next frame read: the KIP-368 SASL session expiry and the
//! `connections.max.idle.ms` idle window.
//!
//! The two are independent. The SASL deadline is a property of the credential
//! and only exists on a connection whose token carries an `exp`; the idle
//! deadline is a property of the listener, applies whatever the connection's
//! auth state, and is re-armed from `Instant::now()` every time
//! [`next_connection_frame`] is entered — that is, after every frame read.
//! Whichever deadline is nearer closes the connection, because both are arms
//! of the same `select!`.
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

/// Converts an "expires-at as Unix epoch ms" value into a
/// `tokio::time::Instant` for `sleep_until`.
///
/// The function computes the delta against the current wall clock and adds it
/// to `Instant::now()`. A test that calls `tokio::time::pause` can then
/// advance the tokio clock and fire the deadline deterministically.
fn instant_at_epoch_ms(epoch_ms: i64) -> tokio::time::Instant {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_i64, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    // `.max(0)` ensures delta is non-negative before the unsigned cast;
    // tokens with past `exp` fire the timer on the very next poll.
    let delta_ms = (epoch_ms - now_ms).max(0);
    tokio::time::Instant::now() + std::time::Duration::from_millis(delta_ms.cast_unsigned())
}

/// Returns the principal name for an `Authenticated` connection, or for the
/// `previous` snapshot of a `Reauthenticating` connection. It returns `None`
/// otherwise. The per-connection re-auth timer reads it for the tracing log it
/// writes on expiry.
fn auth_principal_name(auth: &crate::network::auth::ConnectionAuth) -> Option<&str> {
    match auth {
        crate::network::auth::ConnectionAuth::Authenticated { principal, .. } => {
            Some(principal.name.as_str())
        }
        crate::network::auth::ConnectionAuth::Reauthenticating { previous, .. } => {
            Some(previous.principal.name.as_str())
        }
        _ => None,
    }
}

pub(super) fn initial_connection_auth(
    is_sasl_listener: bool,
    mtls_principal: Option<krabka_security::Principal>,
) -> crate::network::auth::ConnectionAuth {
    if is_sasl_listener {
        return crate::network::auth::ConnectionAuth::Anonymous;
    }
    let principal = mtls_principal.unwrap_or_else(|| krabka_security::Principal {
        name: "ANONYMOUS".to_string(),
        auth_method: krabka_security::AuthMethod::Anonymous,
        groups: vec![],
    });
    crate::network::auth::ConnectionAuth::Authenticated {
        principal,
        mechanism: krabka_security::SaslMechanism::Plain,
        expires_at_ms: None,
        authenticated_via_token: false,
    }
}

fn auth_deadline(auth: &crate::network::auth::ConnectionAuth) -> Option<tokio::time::Instant> {
    match auth {
        crate::network::auth::ConnectionAuth::Authenticated {
            expires_at_ms: Some(expires_at_ms),
            ..
        } => Some(instant_at_epoch_ms(*expires_at_ms)),
        crate::network::auth::ConnectionAuth::Reauthenticating { previous, .. } => {
            previous.expires_at_ms.map(instant_at_epoch_ms)
        }
        _ => None,
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

pub(super) async fn next_connection_frame<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    auth: &crate::network::auth::ConnectionAuth,
    policy: &FrameWaitPolicy,
) -> Option<Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::metrics::ConnectionCloseReason;

    // Re-armed here, so every frame read resets the idle window. The SASL
    // deadline is absolute and is not reset by traffic, so the nearer of the
    // two is what closes the connection.
    let idle_deadline = policy
        .idle
        .map(|window| tokio::time::Instant::now() + window);
    let frame_result = tokio::select! {
        biased;
        next = framed.next() => next,
        () = sleep_until_some(auth_deadline(auth)) => {
            tracing::info!(
                principal = ?auth_principal_name(auth),
                peer = %policy.peer,
                "SASL session expired, closing connection (KIP-368)"
            );
            policy
                .metrics
                .record_connection_close(ConnectionCloseReason::SaslSessionExpired);
            return None;
        }
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
        }
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

    #[test]
    fn auth_principal_name_reads_authenticated_and_reauth_previous_only() {
        let authenticated = crate::network::auth::ConnectionAuth::Authenticated {
            principal: krabka_security::Principal {
                name: "alice".to_string(),
                auth_method: krabka_security::AuthMethod::SaslOAuthBearer,
                groups: vec![],
            },
            mechanism: krabka_security::SaslMechanism::OAuthBearer,
            expires_at_ms: Some(123),
            authenticated_via_token: false,
        };
        let reauth = crate::network::auth::ConnectionAuth::Reauthenticating {
            previous: crate::network::auth::AuthenticatedSnapshot {
                principal: krabka_security::Principal {
                    name: "bob".to_string(),
                    auth_method: krabka_security::AuthMethod::SaslOAuthBearer,
                    groups: vec![],
                },
                mechanism: krabka_security::SaslMechanism::OAuthBearer,
                expires_at_ms: Some(456),
            },
            exchange: crate::network::auth::SaslExchange::OAuthBearer,
        };
        let anonymous = crate::network::auth::ConnectionAuth::Anonymous;

        let cases = [
            ("authenticated", &authenticated, Some("alice")),
            ("reauthenticating uses previous", &reauth, Some("bob")),
            ("anonymous", &anonymous, None),
        ];
        for (case, auth, want) in cases {
            assert!(auth_principal_name(auth) == want, "{case}");
        }
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

    #[test]
    fn instant_at_epoch_ms_maps_future_and_past_wall_clock_to_tokio_deadlines() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_i64, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));

        let before = tokio::time::Instant::now();
        let future = instant_at_epoch_ms(now_ms + 250);
        let delay = future.duration_since(before);
        assert!(
            delay >= Duration::from_millis(100) && delay <= Duration::from_secs(2),
            "future epoch should become a near future tokio deadline, got {delay:?}"
        );

        let past = instant_at_epoch_ms(now_ms - 250);
        assert!(
            past <= tokio::time::Instant::now() + Duration::from_millis(50),
            "past epoch should fire immediately"
        );
    }
}
