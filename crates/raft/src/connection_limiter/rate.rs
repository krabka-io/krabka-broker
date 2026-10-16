//! The connection creation rates of KIP-612 for a whole node and for one
//! listener: `max.connection.creation.rate` and its
//! `listener.name.<listener>.` override.
//!
//! Kafka records each new connection in a `Rate` sensor whose quota is the
//! configured rate, and on a violation its acceptor waits for the throttle
//! time, bounded by `quota.window.size.seconds`, and then accepts the
//! connection (`ConnectionQuotas.waitForConnectionSlot`). The gate here is a
//! generic cell rate algorithm with a burst of one second's worth of
//! connections. It spaces connections at the configured rate as Kafka's
//! sensor does over time, but it does not reproduce the sensor's sample
//! windows, so the size of the first burst Kafka admits differs.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// One rate gate: the theoretical arrival time of the next connection.
#[derive(Debug, Default)]
struct Gate {
    next: Option<Instant>,
}

impl Gate {
    /// Record a connection at `now` against `rate` per second, and return
    /// how long the acceptor waits before it accepts it, at most `max_delay`.
    /// The connection is recorded whatever the wait, as Kafka's sensor
    /// records it before it computes the throttle.
    fn admit(&mut self, now: Instant, rate: u32, max_delay: Duration) -> Duration {
        let interval = Duration::from_secs(1) / rate.max(1);
        let burst = Duration::from_secs(1).saturating_sub(interval);
        let arrival = self.next.map_or(now, |next| next.max(now));
        self.next = Some(arrival + interval);
        arrival
            .checked_sub(burst)
            .map_or(Duration::ZERO, |start| start.saturating_duration_since(now))
            .min(max_delay)
    }
}

impl Gate {
    /// Take back one connection recorded against `rate`, as Kafka's
    /// `record(-1.0)` un-records it.
    fn refund(&mut self, rate: u32) {
        let interval = Duration::from_secs(1) / rate.max(1);
        self.next = self.next.and_then(|next| next.checked_sub(interval));
    }
}

/// The node-wide gate and one gate per listener.
#[derive(Debug, Default)]
pub(super) struct CreationRates {
    node: Gate,
    listeners: HashMap<String, Gate>,
}

impl CreationRates {
    /// Record a connection on `listener` and return the wait: the longer of
    /// the node-wide and the listener throttle. A `protected` listener, the
    /// inter-broker one, is not charged to the node-wide rate, as in Kafka's
    /// `recordConnectionAndGetThrottleTimeMs`.
    pub(super) fn admit(
        &mut self,
        listener: &str,
        protected: bool,
        (node_rate, listener_rate): (Option<u32>, Option<u32>),
        now: Instant,
        max_delay: Duration,
    ) -> Duration {
        let node = match node_rate {
            Some(rate) if !protected => self.node.admit(now, rate, max_delay),
            _ => Duration::ZERO,
        };
        let listener = listener_rate.map_or(Duration::ZERO, |rate| {
            self.listeners
                .entry(listener.to_ascii_uppercase())
                .or_default()
                .admit(now, rate, max_delay)
        });
        node.max(listener)
    }

    /// Take back a connection [`Self::admit`] recorded on `listener`, for a
    /// connection the peer's `ip` quota throttles: Kafka's
    /// `updateListenerMetrics` un-records it from the broker-wide and the
    /// listener rate, so a throttled address does not spend them.
    pub(super) fn refund(
        &mut self,
        listener: &str,
        protected: bool,
        (node_rate, listener_rate): (Option<u32>, Option<u32>),
    ) {
        if let Some(rate) = node_rate
            && !protected
        {
            self.node.refund(rate);
        }
        if let Some(rate) = listener_rate
            && let Some(gate) = self.listeners.get_mut(&listener.to_ascii_uppercase())
        {
            gate.refund(rate);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    /// Delays of five connections that arrive at the same instant.
    #[yare::parameterized(
        one_per_second = { 1, SECOND, [0, 1000, 1000, 1000, 1000] },
        four_per_second = { 4, SECOND, [0, 0, 0, 0, 250] },
        two_per_second = { 2, SECOND, [0, 0, 500, 1000, 1000] },
        capped = { 1, Duration::from_millis(300), [0, 300, 300, 300, 300] },
    )]
    fn a_gate_spaces_simultaneous_connections_at_its_rate(
        rate: u32,
        max_delay: Duration,
        want_ms: [u64; 5],
    ) {
        let now = Instant::now();
        let mut gate = Gate::default();
        let got = [(); 5].map(|()| gate.admit(now, rate, max_delay));
        assert2::assert!(got == want_ms.map(Duration::from_millis));
    }

    #[test]
    fn a_gate_refills_with_time() {
        let start = Instant::now();
        let mut gate = Gate::default();
        assert2::assert!(gate.admit(start, 1, SECOND) == Duration::ZERO);
        assert2::assert!(gate.admit(start + SECOND, 1, SECOND) == Duration::ZERO);
    }

    /// A refunded connection frees its place: with no refund the third of
    /// three simultaneous connections at one a second waits two seconds, with
    /// the second refunded it waits one.
    #[yare::parameterized(
        kept = { false, 2000 },
        refunded = { true, 1000 },
    )]
    fn a_refund_frees_the_connections_place(refund: bool, third_delay_ms: u64) {
        let now = Instant::now();
        let rates = (Some(1), Some(1));
        let mut gates = CreationRates::default();
        let ten_seconds = Duration::from_secs(10);
        let _first = gates.admit("PLAINTEXT", false, rates, now, ten_seconds);
        let _second = gates.admit("PLAINTEXT", false, rates, now, ten_seconds);
        if refund {
            gates.refund("PLAINTEXT", false, rates);
        }
        let third = gates.admit("PLAINTEXT", false, rates, now, ten_seconds);
        assert2::assert!(third == Duration::from_millis(third_delay_ms));
    }

    /// The node-wide and listener gates, with and without protection.
    #[yare::parameterized(
        node_rate_only = { (Some(1), None), false, 1000 },
        listener_rate_only = { (None, Some(1)), false, 1000 },
        protected_skips_node_rate = { (Some(1), None), true, 0 },
        protected_keeps_listener_rate = { (Some(1), Some(1)), true, 1000 },
        unlimited = { (None, None), false, 0 },
    )]
    fn rates_take_the_longer_throttle(
        rates: (Option<u32>, Option<u32>),
        protected: bool,
        second_delay_ms: u64,
    ) {
        let now = Instant::now();
        let mut gates = CreationRates::default();
        let first = gates.admit("PLAINTEXT", protected, rates, now, SECOND);
        let second = gates.admit("PLAINTEXT", protected, rates, now, SECOND);
        assert2::assert!(
            (first, second) == (Duration::ZERO, Duration::from_millis(second_delay_ms))
        );
    }
}
