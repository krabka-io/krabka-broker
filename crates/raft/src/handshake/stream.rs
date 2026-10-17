//! The stream a controller-listener handshake reads and writes, and the clock
//! that measures its silence for `connections.max.idle.ms`.

use {
    std::{
        io::{self, IoSlice},
        pin::{Pin, pin},
        sync::{
            Arc, Mutex, PoisonError,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    },
    tokio::{
        io::{AsyncRead, AsyncWrite, ReadBuf},
        net::TcpStream,
        time::Instant,
    },
};

/// An accepted controller-listener connection, handed to
/// [`RaftListenerHandshake::upgrade`](super::RaftListenerHandshake::upgrade).
///
/// Every read or write that moves bytes restarts the listener's idle window,
/// as Kafka's `Selector` refreshes a channel's idle clock on every ready key.
/// The window closes a handshake that goes silent, not one that is slow. Once
/// the listener takes the upgraded stream for its request loop, the clock is
/// disarmed and the stream only passes bytes through.
pub struct HandshakeStream<S = TcpStream> {
    inner: S,
    clock: Option<Arc<ActivityClock>>,
}

impl<S> HandshakeStream<S> {
    /// Wraps `inner` and returns the watch that expires once `window` passes
    /// with no byte read or written.
    pub(crate) fn watched(inner: S, window: Duration) -> (Self, IdleWatch) {
        let clock = Arc::new(ActivityClock {
            armed: AtomicBool::new(true),
            last_progress: Mutex::new(Instant::now()),
        });
        let watch = IdleWatch {
            clock: Arc::clone(&clock),
            window,
        };
        (
            Self {
                inner,
                clock: Some(clock),
            },
            watch,
        )
    }

    /// The accepted stream, for what a handshake reads off the socket itself,
    /// such as the peer address.
    pub fn get_ref(&self) -> &S { &self.inner }

    fn progressed(&self) {
        if let Some(clock) = &self.clock {
            clock.progressed();
        }
    }
}

/// A stream no idle window watches, for a listener without
/// `connections.max.idle.ms` and for tests that drive a handshake directly.
impl<S> From<S> for HandshakeStream<S> {
    fn from(inner: S) -> Self { Self { inner, clock: None } }
}

/// When a handshake last moved a byte, and whether anyone still asks.
struct ActivityClock {
    armed: AtomicBool,
    last_progress: Mutex<Instant>,
}

impl ActivityClock {
    fn progressed(&self) {
        if self.armed.load(Ordering::Relaxed) {
            *self
                .last_progress
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Instant::now();
        }
    }

    fn last_progress(&self) -> Instant {
        *self
            .last_progress
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// The listener's side of a [`HandshakeStream`]: it expires once the window
/// passes in silence, and dropping it disarms the stream's clock.
pub(crate) struct IdleWatch {
    clock: Arc<ActivityClock>,
    window: Duration,
}

impl IdleWatch {
    /// Resolves once `window` passes with no byte read or written. The timer
    /// is re-armed from the last progress each time it fires early, so the
    /// stream records progress without touching a timer.
    pub(crate) async fn expired(&self) {
        let mut timer = pin!(tokio::time::sleep_until(
            self.clock.last_progress() + self.window
        ));
        loop {
            timer.as_mut().await;
            let deadline = self.clock.last_progress() + self.window;
            if deadline <= Instant::now() {
                return;
            }
            timer.as_mut().reset(deadline);
        }
    }
}

impl Drop for IdleWatch {
    fn drop(&mut self) { self.clock.armed.store(false, Ordering::Relaxed); }
}

impl<S: AsyncRead + Unpin> AsyncRead for HandshakeStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if buf.filled().len() > before {
            this.progressed();
        }
        result
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for HandshakeStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            this.progressed();
        }
        result
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            this.progressed();
        }
        result
    }

    fn is_write_vectored(&self) -> bool { self.inner.is_write_vectored() }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
