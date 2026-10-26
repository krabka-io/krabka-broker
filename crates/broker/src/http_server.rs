//! Shared lifecycle for the broker's auxiliary HTTP listeners.

use {std::io::Error, tokio::net::TcpListener, tokio_util::sync::CancellationToken};

pub fn spawn(
    listener: TcpListener,
    app: axum::Router,
    shutdown: CancellationToken,
    on_error: impl FnOnce(Error) + Send + 'static,
) {
    tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        });
        if let Err(error) = server.await {
            on_error(error);
        }
    });
}
