//! Capture a broadcast before reading, and allocate a pinned future only if
//! the fetch actually has to park.

use {
    std::{future::Future, pin::Pin, sync::Arc},
    tokio::sync::{Notify, futures::OwnedNotified},
};

pub(super) type WaitFut = Pin<Box<dyn Future<Output = Woken> + Send>>;

/// A notification captured before the corresponding partition read.
pub(super) struct Wait {
    pending: usize,
    notify: Arc<Notify>,
    notified: OwnedNotified,
}

/// Which partition to read again, and which notifier to capture before it.
pub(super) struct Woken {
    pub pending: usize,
    pub notify: Arc<Notify>,
}

impl Wait {
    pub(super) fn new(pending: usize, notify: Arc<Notify>) -> Self {
        // All three partition notifiers use notify_waiters. Tokio guarantees
        // that an OwnedNotified observes those broadcasts from its creation,
        // even before it is polled or enabled. Keep creation outside the
        // async block: deferring it until the park would miss a broadcast
        // that lands during the read.
        let notified = Arc::clone(&notify).notified_owned();
        Self {
            pending,
            notify,
            notified,
        }
    }

    pub(super) fn boxed(self) -> WaitFut {
        Box::pin(async move {
            self.notified.await;
            Woken {
                pending: self.pending,
                notify: self.notify,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, futures_util::FutureExt as _};

    #[test]
    fn broadcast_between_capture_and_first_poll_wakes_every_fetch() {
        let notify = Arc::new(Notify::new());
        let waits: Vec<_> = (0..48)
            .map(|index| Wait::new(index, notify.clone()))
            .collect();
        notify.notify_waiters();
        for (index, wait) in waits.into_iter().enumerate() {
            let woken = wait.boxed().now_or_never().expect("captured broadcast");
            assert!(woken.pending == index);
            assert!(Arc::ptr_eq(&woken.notify, &notify));
        }
        // A newly captured generation must not replay the old broadcast.
        assert!(Wait::new(0, notify).boxed().now_or_never().is_none());
    }

    #[test]
    fn broadcast_after_poll_and_replacement_before_poll_are_observed() {
        let notify = Arc::new(Notify::new());
        let mut wait = Wait::new(7, notify.clone()).boxed();
        assert!((&mut wait).now_or_never().is_none());
        notify.notify_waiters();
        let woken = wait.now_or_never().expect("registered broadcast");
        let replacement = Wait::new(woken.pending, woken.notify);
        notify.notify_waiters();
        assert!(
            replacement
                .boxed()
                .now_or_never()
                .expect("next broadcast")
                .pending
                == 7
        );
    }
}
