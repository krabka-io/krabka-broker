//! RAII metric guards for the dispatch loop. One counts a request while it is
//! in flight and records its duration when it finishes; the other counts a
//! live client connection for the lifetime of its serve loop.

/// RAII guard for one dispatched request.
///
/// The guard increments `in_flight_requests` on construction. On drop it
/// decrements the counter and records the elapsed wall-clock time on the
/// `request_duration_seconds{api}` histogram. Drop covers every exit path:
/// success, a handler error, and a panic unwind.
///
/// The guard holds a cheap `BrokerMetrics` clone, a bundle of `Arc`s, so it
/// does not borrow `broker` across the handler `.await`.
pub(super) struct InFlightGuard {
    metrics: crate::metrics::BrokerMetrics,
    api_key: i16,
    started: std::time::Instant,
}

impl InFlightGuard {
    pub(super) fn new(metrics: &crate::metrics::BrokerMetrics, api_key: i16) -> Self {
        metrics.in_flight_requests.inc();
        Self {
            metrics: metrics.clone(),
            api_key,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.in_flight_requests.dec();
        self.metrics
            .observe_request_duration(self.api_key, self.started.elapsed().as_secs_f64());
    }
}

/// RAII guard for one live client connection. It increments
/// `active_connections` on construction and decrements it on drop, when the
/// per-connection serve loop exits. It holds a cheap `BrokerMetrics` clone.
pub(super) struct ActiveConnectionGuard {
    metrics: crate::metrics::BrokerMetrics,
}

impl ActiveConnectionGuard {
    pub(super) fn new(metrics: &crate::metrics::BrokerMetrics) -> Self {
        metrics.active_connections.inc();
        Self {
            metrics: metrics.clone(),
        }
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.metrics.active_connections.dec();
    }
}

/// RAII guard for one queued request waiting for / holding execution capacity (#412).
pub(super) struct QueuedRequestGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
    /// The `queued.max.request.bytes` budget this request spent, given back
    /// when the guard drops. Absent when the knob is off.
    _bytes: Option<tokio::sync::OwnedSemaphorePermit>,
    metrics: crate::metrics::BrokerMetrics,
    /// The value this guard added to `queued_request_bytes`, kept as the
    /// gauge's own type so the decrement on drop is exactly the increment
    /// that was made and the gauge cannot drift.
    bytes: i64,
}

impl QueuedRequestGuard {
    pub(super) fn new(
        permit: tokio::sync::OwnedSemaphorePermit,
        bytes_permit: Option<tokio::sync::OwnedSemaphorePermit>,
        metrics: &crate::metrics::BrokerMetrics,
        bytes: usize,
    ) -> Self {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        metrics.queued_requests.inc();
        if bytes > 0 {
            metrics.queued_request_bytes.inc_by(bytes);
        }
        Self {
            _permit: permit,
            _bytes: bytes_permit,
            metrics: metrics.clone(),
            bytes,
        }
    }
}

impl Drop for QueuedRequestGuard {
    fn drop(&mut self) {
        self.metrics.queued_requests.dec();
        if self.bytes > 0 {
            self.metrics.queued_request_bytes.dec_by(self.bytes);
        }
    }
}
