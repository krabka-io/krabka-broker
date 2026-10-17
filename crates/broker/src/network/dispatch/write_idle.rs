//! Close a response writer that stops making progress, returning its queue permits.

use {
    crate::network::fetch_writer::SendfileSink,
    krabka_raft::{ObservedStream, StreamObserver},
    std::{
        io,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    },
    tokio::time::Sleep,
};

/// The listener's idle window applies to pending writes as well as reads.
/// Each successful write starts a new window; handler and quota waits do not.
pub(crate) type WriteIdle<S> = ObservedStream<S, IdleTimer>;

/// The no-progress timer of a [`WriteIdle`]: armed by a write, flush or
/// shutdown that stays pending, and reset by one that completes.
pub(crate) struct IdleTimer {
    window: Option<Duration>,
    pending: Option<Pin<Box<Sleep>>>,
}

impl IdleTimer {
    /// A timer that fails a pending write once `window` passes, or never
    /// without one.
    pub(crate) fn new(window: Option<Duration>) -> Self {
        Self {
            window,
            pending: None,
        }
    }

    fn stalled<T>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        let Some(window) = self.window else {
            return Poll::Pending;
        };
        let timer = self
            .pending
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(window)));
        if timer.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "response write made no progress within connections.max.idle.ms",
            )))
        } else {
            Poll::Pending
        }
    }

    fn poll_result<T>(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        match result {
            Poll::Ready(result) => {
                self.pending = None;
                Poll::Ready(result)
            }
            Poll::Pending => self.stalled(cx),
        }
    }
}

impl StreamObserver for IdleTimer {
    fn on_write(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        self.poll_result(cx, result)
    }

    fn on_flush(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<()>>,
    ) -> Poll<io::Result<()>> {
        self.poll_result(cx, result)
    }
}

impl<S: SendfileSink> SendfileSink for WriteIdle<S> {
    #[krabka_macros::sendfile_cfg]
    fn write_idle_window(&self) -> Option<Duration> {
        self.observer()
            .window
            .or_else(|| self.get_ref().write_idle_window())
    }

    #[krabka_macros::sendfile_cfg]
    fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> {
        self.get_ref().tcp_for_sendfile()
    }

    fn write_idle_is_enforced(&self) -> bool {
        self.observer().window.is_some() || self.get_ref().write_idle_is_enforced()
    }

    fn is_sendfile_capable(&self) -> bool { self.get_ref().is_sendfile_capable() }
}

/// Wraps a response writer in the listener's `window`, applied only if no
/// lower transport layer already measures progress. TLS must measure
/// ciphertext writes below rustls.
pub(super) fn for_response<S: SendfileSink>(inner: S, window: Option<Duration>) -> WriteIdle<S> {
    let window = if inner.write_idle_is_enforced() {
        None
    } else {
        window
    };
    WriteIdle::new(inner, IdleTimer::new(window))
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tls_tests;
