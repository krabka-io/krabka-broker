//! Per-API request accounting: the dispatched, unsupported and errored
//! request counters, the request-latency histogram, and the resolution of a
//! wire `api_key` to the bounded label those families share.

use super::{ApiKeyLabel, BrokerMetrics, UNKNOWN_LABEL};

impl BrokerMetrics {
    /// Account one dispatched request for `api_key`. The label is the
    /// human-readable name from `api_key_label_name`; unknown keys fold under
    /// `"Unknown"`.
    pub fn record_api_request(&self, api_key: crate::handlers::ApiKeyCode) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.api_requests.get_or_create(&lbl).inc();
    }

    /// Account one request the dispatcher rejected with
    /// `UNSUPPORTED_VERSION` because no handler matched `api_key`
    /// (e.g. unknown `api_key`, or known `api_key` with no version
    /// negotiated). Mirrors the labelling of `record_api_request`.
    pub fn record_unsupported_api_request(&self, api_key: crate::handlers::ApiKeyCode) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.unsupported_api_requests.get_or_create(&lbl).inc();
    }

    /// Observe the wall-clock handling latency for one dispatched
    /// request on the `request_duration_seconds{api}` histogram. `api_key`
    /// is resolved to the same human-readable label as
    /// `record_api_request` (unknown keys fold under `"Unknown"`), so the
    /// two families share one label set. Called from the dispatch path once
    /// per frame with the elapsed seconds of the full handler round-trip.
    pub fn observe_request_duration(&self, api_key: i16, seconds: f64) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.request_duration_seconds
            .get_or_create(&lbl)
            .observe(seconds);
    }

    /// Account one request whose handler returned an error (the
    /// dispatcher closed the connection). Labelled like
    /// `record_api_request`; disjoint from the
    /// `unsupported_api_requests` family.
    pub fn record_request_error(&self, api_key: i16) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.request_errors.get_or_create(&lbl).inc();
    }
}

/// Resolve a wire `api_key` to the name used as the metric label.
///
/// A Kafka api key resolves to its `ApiKey` variant name. A krabka-private api
/// key resolves through [`krabka_private_api_key_label_name`], because
/// `ApiKey::from_i16` does not know that range. Anything else folds under
/// [`UNKNOWN_LABEL`].
fn api_key_label_name(api_key: crate::handlers::ApiKeyCode) -> &'static str {
    if api_key >= crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR {
        return krabka_private_api_key_label_name(api_key);
    }
    match krabka_protocol::api_key::ApiKey::from_i16(api_key) {
        Some(k) => k.into(),
        None => UNKNOWN_LABEL,
    }
}

/// Resolve a krabka-private wire `api_key` to its RPC name.
///
/// Without this arm every krabka-private request shares one
/// `api_requests{api_key="Unknown"}` series with genuine garbage traffic, and
/// an operator cannot tell the two apart. Cardinality stays bounded: the range
/// holds one label per krabka-private RPC, plus [`UNKNOWN_LABEL`].
fn krabka_private_api_key_label_name(api_key: crate::handlers::ApiKeyCode) -> &'static str {
    match api_key {
        crate::handlers::ALTER_BARRIER_GROUPS_API_KEY => "AlterBarrierGroups",
        crate::handlers::DESCRIBE_BARRIER_GROUPS_API_KEY => "DescribeBarrierGroups",
        crate::handlers::TRIGGER_BARRIER_API_KEY => "TriggerBarrier",
        crate::handlers::LIST_BARRIER_CUTS_API_KEY => "ListBarrierCuts",
        crate::handlers::WRITE_BARRIER_MARKERS_API_KEY => "WriteBarrierMarkers",
        _ => UNKNOWN_LABEL,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn api_key_label_name_names_every_krabka_private_api() {
        let cases = [
            (
                crate::handlers::ALTER_BARRIER_GROUPS_API_KEY,
                "AlterBarrierGroups",
            ),
            (
                crate::handlers::DESCRIBE_BARRIER_GROUPS_API_KEY,
                "DescribeBarrierGroups",
            ),
            (crate::handlers::TRIGGER_BARRIER_API_KEY, "TriggerBarrier"),
            (
                crate::handlers::LIST_BARRIER_CUTS_API_KEY,
                "ListBarrierCuts",
            ),
            (
                crate::handlers::WRITE_BARRIER_MARKERS_API_KEY,
                "WriteBarrierMarkers",
            ),
            // A Kafka api key still resolves through the generated enum.
            (0, "Produce"),
            // Garbage inside the krabka-private range, and outside it, both
            // fold under the sentinel.
            (crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR, UNKNOWN_LABEL),
            (9999, UNKNOWN_LABEL),
            (999, UNKNOWN_LABEL),
        ];

        for (api_key, want) in cases {
            assert!(api_key_label_name(api_key) == want, "api_key {api_key}");
        }
    }

    #[test]
    fn unsupported_api_requests_counter_is_disjoint_from_api_requests() {
        let m = BrokerMetrics::new();
        // Invariant: `record_unsupported_api_request` bumps
        // only the `unsupported_api_requests` family — operators
        // expect `api_requests` to count *every* dispatched frame and
        // `unsupported_api_requests` to count just the ones that hit
        // the synthetic UNSUPPORTED_VERSION arm.
        m.record_unsupported_api_request(0); // Produce, unsupported
        m.record_unsupported_api_request(999); // truly unknown

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let unknown = ApiKeyLabel {
            api_key: "Unknown".into(),
        };
        // `record_unsupported_api_request` does NOT also bump
        // `api_requests`; the dispatcher already did that for the
        // request in question via `record_api_request`.
        let cases = [
            (
                "unsupported_api_requests",
                &m.unsupported_api_requests,
                &produce,
                1,
            ),
            (
                "unsupported_api_requests",
                &m.unsupported_api_requests,
                &unknown,
                1,
            ),
            ("api_requests", &m.api_requests, &produce, 0),
            ("api_requests", &m.api_requests, &unknown, 0),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(got == want, "{family_name} for {:?}", label.api_key);
        }
    }

    #[test]
    fn api_requests_label_resolves_known_keys_and_folds_unknown() {
        let m = BrokerMetrics::new();
        // Three known + one unknown api_key. Verify per-label tallies.
        m.record_api_request(0); // Produce
        m.record_api_request(0); // Produce again
        m.record_api_request(1); // Fetch
        m.record_api_request(12_345); // out-of-range → Unknown

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let fetch = ApiKeyLabel {
            api_key: "Fetch".into(),
        };
        let unknown = ApiKeyLabel {
            api_key: "Unknown".into(),
        };
        for (label, want) in [(&produce, 2), (&fetch, 1), (&unknown, 1)] {
            let got = m.api_requests.get_or_create(label).get();
            assert!(got == want, "api_key {:?}", label.api_key);
        }
    }

    #[tokio::test]
    async fn request_duration_errors_and_gauges_render() {
        let m = BrokerMetrics::new();
        // Two Produce latency samples + one unknown-key sample.
        m.observe_request_duration(0, 0.0008);
        m.observe_request_duration(0, 0.04);
        m.observe_request_duration(12_345, 2.0); // → "Unknown" label
        m.record_request_error(1); // Fetch handler fault
        m.record_request_error(1);
        m.in_flight_requests.inc();
        m.in_flight_requests.inc();
        m.in_flight_requests.dec();
        m.active_connections.set(5);

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let fetch = ApiKeyLabel {
            api_key: "Fetch".into(),
        };
        // Histogram Family exposes sample count via the encoded `_count`;
        // assert the render + the error/gauge values here.
        assert!(m.request_errors.get_or_create(&fetch).get() == 2);
        assert!(m.in_flight_requests.get() == 1);
        assert!(m.active_connections.get() == 5);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        assert!(
            buf.contains("krabka_broker_request_duration_seconds_count{api_key=\"Produce\"} 2"),
            "expected 2 Produce latency samples in:\n{buf}"
        );
        assert!(
            buf.contains("krabka_broker_request_errors_total{api_key=\"Fetch\"} 2"),
            "expected 2 Fetch request errors in:\n{buf}"
        );
        assert!(buf.contains("krabka_broker_in_flight_requests 1"));
        assert!(buf.contains("krabka_broker_active_connections 5"));
        // Unknown api_key folds under the shared "Unknown" label.
        assert!(buf.contains("api_key=\"Unknown\""), "unknown label missing");
        // Keep `produce` referenced to document the intended label.
        let _ = produce;
    }
}
