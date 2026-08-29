//! The wait for a process-termination signal, which needs a different
//! implementation on Unix and on every other target.

/// Block until a process-termination signal arrives, then return its name for
/// logging. On Unix that signal is SIGINT (Ctrl-C) **or SIGTERM**. `kubectl
/// delete pod` and the default container stop both send SIGTERM. If this
/// function caught only SIGINT, SIGKILL would hard-kill the broker with no
/// controlled shutdown. On non-Unix targets it catches Ctrl-C only.
#[cfg(unix)]
pub async fn wait_for_termination_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut sigterm) => tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
        },
        Err(e) => {
            // Couldn't install the SIGTERM handler; fall back to SIGINT only
            // rather than refusing to start.
            tracing::warn!(error = %e, "failed to install SIGTERM handler; SIGINT only");
            let _ = tokio::signal::ctrl_c().await;
            "SIGINT"
        }
    }
}

#[cfg(not(unix))]
pub async fn wait_for_termination_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "SIGINT"
}
