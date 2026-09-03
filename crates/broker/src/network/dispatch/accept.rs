//! Per-listener accept path. It terminates TLS when the listener protocol
//! requires it, derives the mTLS principal from the peer certificate, and
//! hands the post-handshake byte stream to the generic per-connection request
//! loop in the parent module.
//!
//! The handshake is the one stretch of a connection's life that the request
//! loop's `connections.max.idle.ms` deadline cannot see, so this module bounds
//! it by the same window: see `handshake_within_idle_window`.

use std::net::SocketAddr;

use tokio::net::TcpStream;

use super::serve_connection_stream;
use crate::broker::Broker;

/// Per-listener entrypoint.
///
/// It branches between TLS termination, when the listener's protocol requires
/// TLS, and the plaintext path. Both paths converge on
/// [`serve_connection_stream`] for the per-connection request loop.
pub async fn serve_connection_on_listener(
    broker: std::sync::Arc<Broker>,
    stream: TcpStream,
    spec: crate::config::ListenerSpec,
) {
    // Capture the peer address from the underlying TCP socket before we
    // hand the stream off to the TLS layer / framing loop. ACL
    // handlers need this for host-based ACL matching. If `peer_addr`
    // fails (rare — socket closed mid-accept), fall back to the
    // unspecified address; ACL matchers treat it as a non-matching host.
    let peer = stream.peer_addr().unwrap_or_else(|e| {
        tracing::debug!(error = %e, "peer_addr() failed, using 0.0.0.0:0");
        SocketAddr::from(([0u8, 0, 0, 0], 0))
    });
    if spec.protocol.requires_tls() {
        let acceptor = if let Some(per_tls) = spec.tls_config.as_ref() {
            match per_tls.build_server_config() {
                Ok(sc) => tokio_rustls::TlsAcceptor::from(sc),
                Err(e) => {
                    tracing::error!(
                        listener = %spec.name,
                        error = %e,
                        "failed to build TlsAcceptor from per-listener tls_config"
                    );
                    return;
                }
            }
        } else {
            // Use DynamicServerConfig so hot-reload keeps working.
            let Some(dynamic) = broker.tls_dynamic.as_ref() else {
                tracing::error!(
                    listener = %spec.name,
                    "TLS listener without per-listener tls_config and no broker-wide tls_dynamic"
                );
                return;
            };
            // Snapshot per accept; an in-flight handshake keeps its captured config.
            tokio_rustls::TlsAcceptor::from(dynamic.current())
        };
        // The handshake is held to the same `connections.max.idle.ms` the
        // serve loop holds a connected peer to: a peer that opens the socket
        // and never sends a ClientHello would otherwise park a task and an fd
        // here forever, never reaching the loop that arms the idle deadline.
        let idle = broker.config.connections_max_idle_for(&spec.name);
        // Linux kTLS (Increment F): when the startup probe confirmed kTLS
        // support, terminate TLS through a `CorkStream` so `ktls` can cleanly
        // drain the rustls buffer, then hand the socket to the kernel via
        // `config_ktls_server`. The resulting `KtlsStream` is `SendfileSink`-
        // capable, so the Fetch path emits file regions and `sendfile(2)`s
        // them onto the socket — the kernel encrypts them into TLS records
        // (zero-copy over TLS). The wire bytes a client decrypts are identical
        // to the userspace path; only the encrypt locus moves kernel-side.
        #[cfg(target_os = "linux")]
        if broker.ktls_enabled {
            let handshake = acceptor.accept(ktls::CorkStream::new(stream));
            // Derive the mTLS principal from the peer cert BEFORE the kTLS
            // transition consumes the stream by value. `get_ref()` reaches the
            // rustls `ServerConnection` through the `CorkStream` wrapper
            // exactly as for a plain `TlsStream`.
            let Some(tls_stream) =
                handshake_within_idle_window(handshake, idle, peer, &broker.metrics).await
            else {
                return;
            };
            let Ok(mtls_principal) = peer_cert_principal(&tls_stream, &spec, peer) else {
                return;
            };
            // `config_ktls_server` consumes `tls_stream` by value; on error
            // the stream is gone, so we cannot fall back to userspace TLS for
            // THIS connection — we close it. This is safe precisely because
            // the startup probe already proved kTLS works on this host, so an
            // error here is an unexpected per-connection anomaly, not the
            // common case.
            match ktls::config_ktls_server(tls_stream).await {
                // NB: any post-handshake app bytes rustls already decrypted
                // are carried INSIDE `ktls_stream` (the `ktls` crate stores
                // them and replays them on the first `poll_read`), so the
                // `Framed` reader in `serve_connection_stream` sees them
                // transparently — no manual drain plumbing needed.
                Ok(ktls_stream) => {
                    serve_connection_stream(broker, ktls_stream, spec, peer, mtls_principal).await;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "kTLS configuration failed after handshake; closing connection \
                         (startup probe had reported kTLS supported)"
                    );
                }
            }
            return;
        }

        let handshake = acceptor.accept(stream);
        let Some(tls_stream) =
            handshake_within_idle_window(handshake, idle, peer, &broker.metrics).await
        else {
            return;
        };
        // Derive a Principal from the peer cert (mTLS). If the listener has
        // client_auth=Required, the handshake itself fails when no cert is
        // presented, so we always have one here. Optional or Disabled may
        // produce `None`.
        // A cert whose DN no mapping rule matches closes the connection.
        let Ok(mtls_principal) = peer_cert_principal(&tls_stream, &spec, peer) else {
            return;
        };
        serve_connection_stream(broker, tls_stream, spec, peer, mtls_principal).await;
    } else {
        serve_connection_plaintext(broker, stream, spec, peer).await;
    }
}

/// Drives a TLS handshake, giving up once the listener's idle window passes.
///
/// Apache Kafka registers a channel with its `Selector` at accept time, before
/// the handshake runs, so `connections.max.idle.ms` already covers a peer that
/// connects to an SSL listener and sends no `ClientHello`; a broker configured
/// with a 20-second window closes such a socket after 20 seconds. Without this
/// bound krabka would hold it forever, which is the fd-exhaustion shape the
/// idle window exists to close — and the pre-handshake stall is the cheapest
/// version of it, because the peer spends nothing to reach it.
///
/// `None` means the connection is finished: either the window passed, which is
/// counted as an idle close like any other, or the handshake itself failed.
async fn handshake_within_idle_window<F, T>(
    handshake: F,
    idle: Option<std::time::Duration>,
    peer: SocketAddr,
    metrics: &crate::metrics::BrokerMetrics,
) -> Option<T>
where
    F: Future<Output = std::io::Result<T>>,
{
    let outcome = match idle {
        Some(window) => match tokio::time::timeout(window, handshake).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => {
                tracing::info!(
                    peer = %peer,
                    idle_ms = window.as_millis(),
                    "TLS handshake idle past connections.max.idle.ms, closing"
                );
                metrics.record_connection_close(crate::metrics::ConnectionCloseReason::Idle);
                return None;
            }
        },
        None => handshake.await,
    };
    match outcome {
        Ok(stream) => Some(stream),
        Err(error) => {
            tracing::debug!(%error, peer = %peer, "TLS handshake failed");
            None
        }
    }
}

/// Inspects the post-handshake TLS stream for a peer certificate. If one is
/// present, this derives the Subject DN with
/// [`krabka_security::extract_principal_from_cert`] and runs it through the
/// listener's KIP-371 `ssl.principal.mapping.rules` to get the principal name.
/// The default rule list is Kafka's `DEFAULT`, under which the DN itself is
/// the principal.
///
/// `Ok(None)` is the no-certificate case, which the session layer turns into
/// `ANONYMOUS`. `Err(())` means a certificate was presented and no mapping
/// rule matched its DN: Kafka's `SslPrincipalMapper` throws `NoMatchingRule`
/// there and the channel never builds, so the caller closes the connection
/// rather than admitting the peer under its DN or as `ANONYMOUS`.
fn peer_cert_principal<S>(
    stream: &tokio_rustls::server::TlsStream<S>,
    spec: &crate::config::ListenerSpec,
    peer: SocketAddr,
) -> Result<Option<krabka_security::Principal>, ()> {
    let (_, server_conn) = stream.get_ref();
    let Some(distinguished_name) = server_conn
        .peer_certificates()
        .and_then(<[_]>::first)
        .and_then(|cert| krabka_security::extract_principal_from_cert(cert.as_ref()))
    else {
        return Ok(None);
    };
    let Some(name) = spec.principal_mapper.apply(&distinguished_name) else {
        tracing::warn!(
            listener = %spec.name,
            peer = %peer,
            distinguished_name = %distinguished_name,
            "no ssl.principal.mapping.rules rule matched the peer certificate \
             subject DN, closing connection"
        );
        return Err(());
    };
    Ok(Some(krabka_security::Principal {
        name,
        auth_method: krabka_security::AuthMethod::MTls,
        groups: vec![],
    }))
}

/// Plaintext entry point. It keeps the legacy `TcpStream`-typed signature for
/// call sites, and it records the peer's TCP address before it passes the
/// stream to the generic loop.
async fn serve_connection_plaintext(
    broker: std::sync::Arc<Broker>,
    stream: TcpStream,
    spec: crate::config::ListenerSpec,
    peer: SocketAddr,
) {
    serve_connection_stream(broker, stream, spec, peer, None).await;
}
