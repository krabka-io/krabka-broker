//! Fixtures shared by the unit tests of the `oauth_jwks` module tree.
//!
//! The module holds the JWKS test servers -- plaintext, HTTPS with a freshly
//! generated self-signed certificate, and a request-counting variant -- the
//! `JwksRefresher` builders that wire one to an injected sleeper, and the
//! polling helper that replaces a real-time sleep in a test.

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use krabka_security::{Jwks, JwksHandle};
use krabka_units::{Time, hours, millis, secs};
use qubit_clock::sleep::{AsyncSleepFuture, AsyncSleeper};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::JwksRefresher;

#[derive(Debug)]
struct PendingSleeper;

impl AsyncSleeper for PendingSleeper {
    fn sleep_for_async(&self, _duration: Duration) -> AsyncSleepFuture<'_> {
        Box::pin(std::future::pending())
    }
}

/// Yield-polls until `cond` holds. A bounded hang-guard makes a real stall
/// fail the test deterministically instead of spinning forever.
pub async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held: {what}");
}

/// Serves a fixed body at `/jwks` on an ephemeral port. It returns the
/// bound address and a shutdown token for the server task.
pub async fn serve_jwks(body: &'static str) -> (SocketAddr, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let app = axum::Router::new().route("/jwks", axum::routing::get(move || async move { body }));
    let srv_shutdown = shutdown.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { srv_shutdown.cancelled().await })
            .await
            .unwrap();
    });
    (addr, shutdown)
}

pub const JWKS_BODY: &str = r#"{"keys":[{"kty":"EC","crv":"P-256","kid":"k1","x":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU","y":"x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0"}]}"#;

/// Picks the slots that matter for on-demand refresh out of a
/// `JwksRefresher`, so that the simple refresher tests stay short. These
/// tests supply `signal_rx` but never send on it. `min_on_demand_pause`
/// does not apply. Each test has its own timestamps.
pub fn test_refresher(
    endpoint: String,
    handle: JwksHandle,
    interval: Time,
    shutdown: CancellationToken,
    tls_trust: Option<PathBuf>,
    sleeper: Arc<dyn AsyncSleeper>,
) -> JwksRefresher {
    let (_tx, rx) = mpsc::channel::<()>(1);
    JwksRefresher {
        endpoint,
        handle,
        interval,
        http_timeout: millis(37),
        shutdown,
        tls_trust,
        signal_rx: rx,
        min_on_demand_pause: secs(1),
        last_successful_fetch_ms: Arc::new(AtomicI64::new(0)),
        last_on_demand_refresh_ms: Arc::new(AtomicI64::new(0)),
        ignore_key_use: false,
        sleeper,
    }
}

/// Serves a fixed JSON body over TLS on an ephemeral port, with a newly
/// generated self-signed cert that carries `127.0.0.1` as a SAN. It
/// returns the bound address, a shutdown token, and the PEM path of the
/// cert, which the client can use as a trust bundle.
pub async fn serve_jwks_https(
    body: &'static str,
) -> (std::net::SocketAddr, CancellationToken, std::path::PathBuf) {
    use std::sync::Arc;

    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    use tokio::io::AsyncWriteExt as _;
    use tokio_rustls::TlsAcceptor;

    // Install the rustls CryptoProvider once (idempotent — discards Err
    // on re-install). Required for rustls::ServerConfig::builder.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Generate a fresh self-signed cert with 127.0.0.1 as a SAN so the
    // client's hostname-verification accepts the loopback connection.
    let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let cert = params.self_signed(&key).unwrap();

    // Leak the tempdir for the test's lifetime so the PEM remains
    // readable when the refresher task fetches.
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let cert_path = dir.path().join("cert.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    let key_path = dir.path().join("key.pem");
    std::fs::write(&key_path, key.serialize_pem()).unwrap();

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let priv_key = PrivateKeyDer::from_pem_file(&key_path).unwrap();
    let server_cfg = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, priv_key)
            .unwrap(),
    );
    let acceptor = TlsAcceptor::from(server_cfg);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let srv_shutdown = shutdown.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = srv_shutdown.cancelled() => break,
                Ok((sock, _peer)) = listener.accept() => {
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        use tokio::io::AsyncReadExt as _;
                        let Ok(mut tls) = acceptor.accept(sock).await else { return };
                        // Drain a minimal request line + headers (we
                        // don't parse — just ignore until empty line).
                        // Then write a fixed JSON reply.
                        let mut buf = [0u8; 1024];
                        let _ = tls.read(&mut buf).await;
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                            body.len(),
                        );
                        let _ = tls.write_all(header.as_bytes()).await;
                        let _ = tls.write_all(body.as_bytes()).await;
                        let _ = tls.shutdown().await;
                    });
                }
            }
        }
    });

    (addr, shutdown, cert_path)
}

/// Serves a fixed body and counts how many HTTP requests reached the
/// `/jwks` route. It returns a shared `AtomicUsize`, so the test can
/// assert on the call count after it drives the refresher.
pub async fn serve_jwks_counting(
    body: &'static str,
) -> (
    SocketAddr,
    CancellationToken,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter_cl = counter.clone();
    let app = axum::Router::new().route(
        "/jwks",
        axum::routing::get(move || {
            let c = counter_cl.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                body
            }
        }),
    );
    let srv_shutdown = shutdown.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { srv_shutdown.cancelled().await })
            .await
            .unwrap();
    });
    (addr, shutdown, counter)
}

/// Builds a refresher with a 1-hour periodic interval, so that only
/// on-demand signals matter for the test. It returns the shared signal
/// sender, the rate-limit timestamp, and the success timestamp.
pub type SignalRefresher = (
    JwksRefresher,
    mpsc::Sender<()>,
    Arc<AtomicI64>,
    Arc<AtomicI64>,
    CancellationToken,
    JwksHandle,
);

pub fn make_signal_refresher(endpoint: String, min_on_demand_pause: Time) -> SignalRefresher {
    let (signal_tx, signal_rx) = mpsc::channel::<()>(1);
    let shutdown = CancellationToken::new();
    let last_successful = Arc::new(AtomicI64::new(0));
    let last_on_demand = Arc::new(AtomicI64::new(0));
    let handle = JwksHandle::new_with_refresher_handles(
        Jwks::empty(),
        last_successful.clone(),
        signal_tx.clone(),
    );
    let refresher = JwksRefresher {
        endpoint,
        handle: handle.clone(),
        interval: hours(1),
        http_timeout: millis(37),
        shutdown: shutdown.clone(),
        tls_trust: None,
        signal_rx,
        min_on_demand_pause,
        last_successful_fetch_ms: last_successful.clone(),
        last_on_demand_refresh_ms: last_on_demand.clone(),
        ignore_key_use: false,
        // Signal tests isolate the on-demand arm; periodic refreshes have
        // dedicated mock-timeline coverage above.
        sleeper: Arc::new(PendingSleeper),
    };
    (
        refresher,
        signal_tx,
        last_successful,
        last_on_demand,
        shutdown,
        handle,
    )
}
