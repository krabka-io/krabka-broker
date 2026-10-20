//! The stream a controller-listener handshake reads and writes, and the clock
//! that measures its silence for `connections.max.idle.ms`.

use {
    crate::{ObservedStream, StreamObserver},
    std::{
        io,
        pin::pin,
        sync::{
            Arc, Mutex, PoisonError,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    },
    tokio::{net::TcpStream, time::Instant},
};

/// An accepted controller-listener connection, handed to
/// [`RaftListenerHandshake::upgrade`](super::RaftListenerHandshake::upgrade).
///
/// Every read or write that moves bytes restarts the listener's idle window,
/// as Kafka's `Selector` refreshes a channel's idle clock on every ready key.
/// The window closes a handshake that goes silent, not one that is slow. Once
/// the listener takes the upgraded stream for its request loop, the clock is
/// disarmed and the stream only passes bytes through.
pub type HandshakeStream<S = TcpStream> = ObservedStream<S, HandshakeClock>;

/// The idle clock a [`HandshakeStream`] restarts on every read or write that
/// moves bytes, or none when no window watches the stream.
pub struct HandshakeClock(Option<Arc<ActivityClock>>);

impl HandshakeClock {
    fn progressed(&self) {
        if let Some(clock) = &self.0 {
            clock.progressed();
        }
    }
}

impl StreamObserver for HandshakeClock {
    fn on_read(&mut self, filled: usize) {
        if filled > 0 {
            self.progressed();
        }
    }

    fn on_write(
        &mut self,
        _cx: &mut Context<'_>,
        result: Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            self.progressed();
        }
        result
    }
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
        (Self::new(inner, HandshakeClock(Some(clock))), watch)
    }
}

/// A stream no idle window watches, for a listener without
/// `connections.max.idle.ms` and for tests that drive a handshake directly.
impl<S> From<S> for HandshakeStream<S> {
    fn from(inner: S) -> Self { Self::new(inner, HandshakeClock(None)) }
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

    /// Restarts the window without a byte moving, as Kafka's `Selector.send`
    /// refreshes a channel's idle clock when a response is queued.
    pub(crate) fn refresh(&self) { self.clock.progressed(); }
}

impl Drop for IdleWatch {
    fn drop(&mut self) { self.clock.armed.store(false, Ordering::Relaxed); }
}
