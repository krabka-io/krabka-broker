//! A connection's idle clock: when the socket last showed activity, and the
//! `connections.max.idle.ms` window measured from then.
//!
//! Kafka's `Selector` refreshes a channel's idle clock whenever the channel's
//! key is selected, that is, ready to read or to write
//! (`pollSelectionKeys`). `maybeCloseOldestConnection` closes the channel
//! once that clock is older than the window, whatever the channel is doing.
//! A channel is muted from a receive until its response is sent, and for a
//! KIP-219 throttle window after that, and a muted channel is never ready to
//! read. So a slow handler or a long mute spends the window just as silence
//! does, and a response still in a handler is lost with the channel.
//!
//! The serve loop reads and answers one request at a time, so a read that
//! delivers bytes and a response write that completes move the clock, and
//! nothing else does. The window keeps running while a handler works and
//! while a mute holds the next read back. A wait on one of the broker's own
//! request budgets (`queued.max.request.bytes`, `queued.max.requests`)
//! restarts it instead: that is backpressure the broker imposed, not the peer
//! falling silent.

use {crate::time_util::sleep_until_opt, std::time::Duration, tokio::time::Instant};

/// The last socket activity of one connection, and the window it may stay
/// idle for after it.
#[derive(Clone, Copy, Debug)]
pub(super) struct IdleClock {
    window: Option<Duration>,
    last_activity: Instant,
}

impl IdleClock {
    /// The clock of a connection accepted now. Kafka's `registerChannel`
    /// starts the idle clock at registration. With no `window` the clock never
    /// expires, which is what a non-positive `connections.max.idle.ms` asks
    /// for.
    pub(super) fn start(window: Option<Duration>) -> Self {
        Self {
            window,
            last_activity: Instant::now(),
        }
    }

    /// Records socket activity now, which starts a fresh window.
    pub(super) fn touch(&mut self) { self.last_activity = Instant::now(); }

    /// The window this clock measures, if it has one.
    pub(super) fn window(&self) -> Option<Duration> { self.window }

    /// The instant the window passes unless the socket shows activity first.
    /// A window too long to add to an `Instant` never passes.
    pub(super) fn deadline(&self) -> Option<Instant> {
        self.window
            .and_then(|window| self.last_activity.checked_add(window))
    }

    /// Resolves once the window has passed with no activity, and never when
    /// the clock has no window.
    pub(super) async fn expired(&self) { sleep_until_opt(self.deadline()).await }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The deadline is one window after the last activity, and touching the
    /// clock moves it to one window after the touch.
    #[tokio::test(start_paused = true)]
    async fn the_deadline_is_one_window_after_the_last_activity() {
        let window = Duration::from_millis(10);
        let started = Instant::now();
        let mut clock = IdleClock::start(Some(window));
        assert2::assert!(clock.deadline() == Some(started + window));

        tokio::time::advance(Duration::from_millis(7)).await;
        clock.touch();
        assert2::assert!(clock.deadline() == Some(started + Duration::from_millis(17)));

        clock.expired().await;
        assert2::assert!(Instant::now() == started + Duration::from_millis(17));
    }

    /// A clock with no window, or with one too long for an `Instant`, has no
    /// deadline and never expires.
    #[yare::parameterized(
        no_window = { None },
        unrepresentable_window = { Some(Duration::MAX) },
    )]
    #[test_macro(tokio::test(start_paused = true))]
    async fn a_clock_without_a_reachable_deadline_never_expires(window: Option<Duration>) {
        let clock = IdleClock::start(window);
        assert2::assert!(clock.deadline().is_none());
        assert2::assert!(
            tokio::time::timeout(Duration::from_secs(3600), clock.expired())
                .await
                .is_err()
        );
    }
}
