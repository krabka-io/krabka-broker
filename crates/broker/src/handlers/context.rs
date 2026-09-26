//! Per-request connection metadata threaded through every inline-intercept
//! handler.

use std::net::SocketAddr;

use krabka_security::Principal;

/// Per-request connection metadata. Constructed once per frame in
/// `network::dispatch` from the authenticated `ConnectionAuth`, the
/// accept-time peer `SocketAddr`, and the frame's `client_id` header.
pub(crate) struct RequestContext<'a> {
    pub principal: &'a Principal,
    pub peer: &'a SocketAddr,
    /// Frame's `client_id` header. It is an empty string when the wire field
    /// is null (`-1` length) or zero-length. This matches the existing
    /// `peek_client_id(frame).unwrap_or("")` convention that the dispatch loop
    /// uses for the `request_percentage` quota.
    pub client_id: &'a str,
    /// Unique identifier for the live network connection. Share sessions use
    /// it to release acquisitions when the connection closes.
    pub connection_id: &'a str,
    /// `true` when the connection can serve the fetch records region with
    /// kernel `sendfile(2)`. That means a plaintext `TcpStream` on a
    /// SENDFILE-alias platform: Linux, Apple, FreeBSD, or `DragonFly`. The fetch
    /// handler uses this to emit the zero-copy `RecordsPayload::FileRegions`
    /// instead of `Raw` for large records runs (Increments D and E). It is
    /// `false` on TLS, on Windows, and for every non-fetch handler, which all
    /// ignore it.
    pub sendfile_capable: bool,
    /// Name of the [`crate::config::ListenerSpec`] serving this connection
    /// such as `"PLAINTEXT"`, `"SSL"`, or a configured listener name. This is
    /// the same string that self-registration writes as each
    /// [`krabka_metadata::BrokerEndpoint::name`]. Address-projecting
    /// handlers (`Metadata`, `FindCoordinator`, `DescribeCluster`) therefore
    /// advertise the endpoint that matches the listener the request arrived
    /// on, exactly as Apache Kafka does. Handlers that do not project broker
    /// addresses ignore this field.
    pub connection_listener_name: &'a str,
    /// KIP-219 throttle window for this request, filled in by whichever quota
    /// the handler charged. The dispatch loop drains it with
    /// [`RequestContext::take_throttle`] once the response is on the wire and
    /// mutes the connection for that long. Handlers that charge no quota leave
    /// it at zero.
    pub throttle: crate::quota::ThrottleSlot,
    /// Set by an `acks=0` `Produce` whose response carries an error on any
    /// partition. `KafkaApis.handleProduceRequest` closes the connection in
    /// that case instead of muting it, because a suppressed response is the
    /// only signal such a producer gets; the dispatch loop drains this with
    /// [`RequestContext::take_close_after_response`] once the (empty) response
    /// has been written and closes the connection instead of muting it.
    close_after_response: std::sync::atomic::AtomicBool,
    /// Size of the request as Kafka's `Request.sizeInBytes` counts it: the
    /// header and the body, without the four-byte length prefix. `Produce`
    /// charges it to `producer_byte_rate`. It is zero unless the dispatch loop
    /// set it with [`RequestContext::with_request_size`].
    pub request_size: u64,
}

/// Connection attributes a KIP-714 telemetry handler needs to match a
/// client to a subscription. Telemetry RPCs are unauthenticated, so this
/// carries no principal. It carries only the wire-derived and
/// connection-derived fields.
pub(crate) struct TelemetryContext<'a> {
    pub client_id: &'a str,
    pub peer: &'a std::net::SocketAddr,
    pub software_name: &'a str,
    pub software_version: &'a str,
}

impl<'a> RequestContext<'a> {
    pub(crate) fn new(
        principal: &'a Principal,
        peer: &'a SocketAddr,
        client_id: &'a str,
        connection_id: &'a str,
        sendfile_capable: bool,
        connection_listener_name: &'a str,
    ) -> Self {
        Self {
            principal,
            peer,
            client_id,
            connection_id,
            sendfile_capable,
            connection_listener_name,
            throttle: crate::quota::ThrottleSlot::default(),
            close_after_response: std::sync::atomic::AtomicBool::new(false),
            request_size: 0,
        }
    }

    /// Records the size of the request frame this context serves, header and
    /// body, as Kafka's `Request.sizeInBytes` counts it.
    #[must_use]
    pub(crate) fn with_request_size(mut self, frame_len: usize) -> Self {
        self.request_size = u64::try_from(frame_len).unwrap_or(u64::MAX);
        self
    }

    /// Records the KIP-219 window this request must be throttled for. The
    /// response still goes out immediately; the connection loop applies the
    /// window afterwards by muting the connection.
    pub(crate) fn record_throttle(&self, window: krabka_units::Time) {
        self.throttle.record(window);
    }

    /// Leaves a quota charge for the dispatch loop to resolve with the request
    /// quota in one metrics call, and records its window. See
    /// [`crate::quota::ThrottleSlot::defer`].
    pub(crate) fn defer_quota_charge(&self, charge: crate::metrics::QuotaCharge) {
        self.throttle.defer(charge);
    }

    /// Drains the recorded KIP-219 window. It returns a zero extent when no
    /// quota charged this request.
    pub(crate) fn take_throttle(&self) -> krabka_units::Time {
        self.throttle.take()
    }

    /// Marks this request's connection to be closed once its (suppressed)
    /// response has been written. Only an `acks=0` `Produce` whose response
    /// carries an error on any partition calls this.
    pub(crate) fn mark_close_after_response(&self) {
        self.close_after_response
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Drains the close-after-response flag [`Self::mark_close_after_response`]
    /// set. `false` unless that call happened.
    pub(crate) fn take_close_after_response(&self) -> bool {
        self.close_after_response
            .swap(false, std::sync::atomic::Ordering::Relaxed)
    }

    /// Kafka's group coordinator stores `InetAddress::toString()`, which is
    /// the peer IP prefixed with `/` and does not include the connection port.
    pub(crate) fn client_host(&self) -> String {
        match self.peer {
            SocketAddr::V4(peer) => format!("/{}", peer.ip()),
            SocketAddr::V6(peer) => {
                let address = peer
                    .ip()
                    .segments()
                    .map(|segment| format!("{segment:x}"))
                    .join(":");
                let scope = match peer.scope_id() {
                    0 => String::new(),
                    scope_id => format!("%{scope_id}"),
                };
                format!("/{address}{scope}")
            }
        }
    }
}

impl<'a> TelemetryContext<'a> {
    pub(crate) fn new(
        peer: &'a SocketAddr,
        client_id: &'a str,
        software_name: &'a str,
        software_version: &'a str,
    ) -> Self {
        Self {
            client_id,
            peer,
            software_name,
            software_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_security::{AuthMethod, Principal};

    use super::*;

    fn principal() -> Principal {
        Principal {
            name: "alice".to_string(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec!["operators".to_string()],
        }
    }

    #[test]
    fn request_context_new_preserves_connection_fields() {
        let principal = principal();
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = RequestContext::new(
            &principal,
            &peer,
            "client-a",
            "connection-a",
            true,
            "SASL_SSL",
        );

        assert!(ctx.principal.name == "alice");
        assert!(ctx.peer == &peer);
        assert!(ctx.client_id == "client-a");
        assert!(ctx.connection_id == "connection-a");
        assert!(ctx.sendfile_capable);
        assert!(ctx.connection_listener_name == "SASL_SSL");
        assert!(ctx.client_host() == "/127.0.0.1");
        assert!(ctx.request_size == 0);
        assert!(ctx.with_request_size(8_300).request_size == 8_300);
    }

    #[test]
    fn request_context_client_host_uses_java_ipv6_format() {
        let principal = principal();
        let peer = SocketAddr::V6(std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::LOCALHOST,
            9092,
            0,
            4,
        ));

        let ctx = RequestContext::new(
            &principal,
            &peer,
            "client-a",
            "connection-a",
            false,
            "PLAINTEXT",
        );

        assert!(ctx.client_host() == "/0:0:0:0:0:0:0:1%4");
    }

    #[test]
    fn telemetry_context_new_preserves_client_identity_fields() {
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = TelemetryContext::new(&peer, "client-a", "krabka-test", "1.2.3");

        assert!(ctx.peer == &peer);
        assert!(ctx.client_id == "client-a");
        assert!(ctx.software_name == "krabka-test");
        assert!(ctx.software_version == "1.2.3");
    }
}
