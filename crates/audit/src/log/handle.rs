//! The cloneable `AuditLog` handle that broker code calls to record events.
//!
//! The handle owns only the sender side of the writer channel, so `emit` is a
//! synchronous, non-blocking enqueue that is safe to call from the synchronous
//! `Authorizer::authorize` trait. Dropping an event under backpressure is
//! counted here rather than in the `AuditWriter`.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arc_swap::ArcSwapOption;
use tokio::sync::mpsc;

use crate::event::AuditEvent;

/// Cloneable, cheap handle that broker code calls to record events.
///
/// `emit` is synchronous and never blocks. It is safe to call from the
/// synchronous `Authorizer::authorize` trait and from async request handlers.
#[derive(Debug)]
pub struct AuditLog {
    tx: ArcSwapOption<mpsc::Sender<AuditEvent>>,
    dropped: AtomicU64,
}

impl AuditLog {
    /// Create an enabled log and the receiver for an [`AuditWriter`].
    ///
    /// [`AuditWriter`]: super::AuditWriter
    #[must_use]
    pub fn new(capacity: usize) -> (Arc<Self>, mpsc::Receiver<AuditEvent>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Arc::new(Self {
                tx: ArcSwapOption::new(Some(Arc::new(tx))),
                dropped: AtomicU64::new(0),
            }),
            rx,
        )
    }

    /// A no-op log for a disabled audit subsystem.
    #[must_use]
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            tx: ArcSwapOption::new(None),
            dropped: AtomicU64::new(0),
        })
    }

    /// Record an event.
    ///
    /// This method does not block. If the queue is full, it drops the event and
    /// counts the drop. Durable spooling is Slice 3 / AU-5.
    pub fn emit(&self, event: AuditEvent) {
        let Some(tx) = self.tx.load_full() else {
            return;
        };
        if tx.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("audit event dropped (queue full or writer stopped)");
        }
    }

    /// Close the event stream for every clone of this handle.
    ///
    /// Events already in the queue remain available to the writer. Once they
    /// are drained, the writer exits cleanly.
    pub fn close(&self) {
        self.tx.store(None);
    }

    /// Count of events dropped because of backpressure.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::log::test_support::life;

    #[tokio::test]
    async fn close_ends_stream_for_every_log_clone_after_queued_events() {
        let (log, mut rx) = AuditLog::new(16);
        let clone = Arc::clone(&log);
        log.emit(life(1));

        clone.close();
        log.emit(life(2));

        check!(rx.recv().await == Some(life(1)));
        check!(rx.recv().await.is_none());
        check!(log.dropped() == 0);
    }

    #[test]
    fn disabled_log_drops_without_panicking() {
        let log = AuditLog::disabled();
        log.emit(life(1)); // no receiver, no panic
        check!(log.dropped() == 0); // disabled path is a silent no-op, not a "drop"
    }

    #[tokio::test]
    async fn full_queue_increments_dropped() {
        let (log, _rx) = AuditLog::new(1); // tiny queue, receiver never drains
        // First may enqueue; subsequent ones overflow.
        for i in 0..10 {
            log.emit(life(i));
        }
        check!(log.dropped() == 9);
    }
}
