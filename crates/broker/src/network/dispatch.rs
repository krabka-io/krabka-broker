//! Per-connection request loop. Reads a frame, parses the request
//! header, looks up the handler, awaits the response, encodes the
//! response header in front of the handler's bytes, and writes the
//! result back to the client.
//!
//! Header rules, verified against Apache Kafka 4.x:
//! - The request header is v2 when the body is flexible (KIP-482), and v1
//!   otherwise. Note that `client_id` is a `NULLABLE_STRING` with an i16
//!   length in BOTH header versions. See the `RequestHeader.json` schema,
//!   where the field has `flexibleVersions: none`.
//! - The response header is v1, that is, it has a trailing tagged-fields
//!   byte, if and only if the *body* is flexible. `ApiVersions`
//!   (`api_key=18`) is the one EXCEPT case: its response header is always
//!   v0.

use {
    crate::telemetry::REQUEST_TARGET,
    bytes::Bytes,
    futures_util::SinkExt,
    krabka_protocol::{
        Decode as _, api_key::ApiKey, owned::api_versions_request::ApiVersionsRequest,
    },
    krabka_units::{Time, convert::TimeExt},
    std::{net::SocketAddr, sync::Arc, time},
    tokio::{
        io::{AsyncRead, AsyncWrite, AsyncWriteExt as _},
        sync::Semaphore,
        time::Instant,
    },
    tokio_util::codec::Framed,
    tracing::Instrument as _,
};

mod accept;
mod fetch;
mod guards;
mod idle_clock;
mod quota_accounting;
mod receive;
mod registry;
mod response;
/// Benchmark seam over the response-framing path, driven by
/// `benches/perf_deferrals.rs`.
#[cfg(any(test, feature = "test-helpers"))]
pub mod response_framing;
pub mod sasl;
mod session;
#[cfg(test)]
mod test_support;
mod write_idle;

/// The broker-wide `queued.max.request.bytes` budget (#412).
///
/// The total is carried beside the semaphore because
/// [`tokio::sync::Semaphore::acquire_many`] does not refuse a request for more
/// permits than the semaphore was built with -- it waits, and no response can
/// ever free enough room, so the wait never ends. The comparison against
/// `total` is what turns that into a refusal.
pub struct RequestByteBudget {
    permits: Arc<Semaphore>,
    total: usize,
}

impl RequestByteBudget {
    /// One permit per byte. `Semaphore` caps its permit count well below
    /// `usize::MAX`, so a budget above that ceiling is clamped to it rather
    /// than rejected: at that size the knob is off in every sense that
    /// matters.
    pub(crate) fn of(budget: usize) -> Self {
        let total = budget.min(Semaphore::MAX_PERMITS);
        Self {
            permits: Arc::new(Semaphore::new(total)),
            total,
        }
    }

    /// The permits not currently spent. Test-only: what production code cares
    /// about is whether a frame is admitted, not how much room is left.
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize { self.permits.available_permits() }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod throttle_audit;

pub use self::accept::serve_connection_on_listener;
use {
    self::{
        fetch::{FetchPlan, fetch_reply},
        guards::{ActiveConnectionGuard, InFlightGuard, QueuedRequestGuard},
        idle_clock::IdleClock,
        quota_accounting::apply_request_quota,
        registry::{DispatchContext, registry_reply},
        response::{
            ResponseShape, ThrottledResponse, encode_response, patch_response_throttle,
            send_throttled_response,
        },
        sasl::{SaslConnection, SaslFrameOutcome, SaslListener, try_handle_sasl_frame},
        session::{
            FrameSizeLimit, FrameWaitPolicy, RequestLimits, initial_connection_auth,
            next_connection_frame,
        },
    },
    crate::{
        api_catalog::{INTER_BROKER_ONLY_APIS, ListenerKind, is_disabled_version},
        broker::Broker,
        config::ListenerSpec,
        error::BrokerError,
        handlers::{
            ApiKeyCode, ApiVersion, DispatchKind,
            api_versions::{is_valid_client_info, unsupported_version_response},
            registry::DispatchEntry,
        },
        metrics::{BrokerMetrics, ConnectionCloseReason},
        network::{
            auth::ConnectionAuth,
            codec::{self, KafkaCodec},
            fetch_writer::SendfileSink,
            listener::resolve_sasl_mechanisms_for_listener,
            request::{ParsedRequest, parse_request, peek_api_key},
        },
        telemetry::request_span,
        time_util::now_ms,
    },
};

/// What the connection loop does once a response has been written.
///
/// KIP-219 splits a quota violation between the response, which tells the
/// client how long to back off, and the connection, which the broker mutes for
/// exactly that long. The write always happens first; the mute is what
/// enforces the quota.
#[derive(Clone, Copy, Debug)]
pub(super) enum AfterResponse {
    /// Keep serving, but read no further request for this window. It is a zero
    /// extent when the request tripped no quota.
    Mute(Time),
    /// Close the connection.
    Close,
}

/// What a request leaves for the socket once its handler is done.
///
/// A handler computes its reply without touching the socket, and the
/// connection loop writes it afterwards. That split is what lets the loop
/// close the socket while a handler is still running: see [`within_idle`].
enum Reply {
    /// One response frame, and the KIP-219 window it earned.
    Frame(ThrottledResponse),
    /// A `Fetch` response, as the write plan that keeps its records
    /// zero-copy.
    Fetch(FetchPlan),
    /// Nothing to write.
    Done(AfterResponse),
}

/// Writes `reply`, and records the write's completion as activity on
/// `clock`: Kafka's `Selector` refreshes a channel's idle clock each time it
/// is ready to write.
async fn respond<S>(
    framed: &mut Framed<S, KafkaCodec>,
    clock: &mut IdleClock,
    reply: Reply,
    metrics: &BrokerMetrics,
) -> AfterResponse
where
    S: AsyncRead + AsyncWrite + Unpin + SendfileSink,
{
    let after = match reply {
        Reply::Frame(response) => send_throttled_response(framed, response).await,
        Reply::Fetch(plan) => fetch::write_plan(framed, plan, metrics).await,
        Reply::Done(after) => return after,
    };
    clock.touch();
    after
}

/// Runs a request's handler `work` against the connection's idle window, and
/// returns its output, or `None` when the window passed first.
///
/// Kafka mutes a channel from the receive of a request until its response is
/// sent, so a muted channel's idle clock stands still while the request is in
/// a handler, and `maybeCloseOldestConnection` closes the channel once the
/// window passes. The handler runs on, and its response is dropped with the
/// channel. This does the same: once the window passes it shuts the socket
/// down, so the peer sees the close at once, and then lets `work` finish
/// rather than dropping it part-way. A handler cancelled between two awaits
/// could leave broker state half-applied.
async fn within_idle<S, F>(
    framed: &mut Framed<S, KafkaCodec>,
    clock: &IdleClock,
    work: F,
) -> Option<F::Output>
where
    S: AsyncWrite + Unpin,
    F: Future,
{
    let mut work = std::pin::pin!(work);
    tokio::select! {
        biased;
        output = &mut work => return Some(output),
        () = clock.expired() => {}
    }
    if let Err(error) = framed.get_mut().shutdown().await {
        tracing::debug!(%error, "shutting down a connection idle past its window");
    }
    let _discarded = work.await;
    None
}

/// Runs the handler of a request that reaches the registry, against the idle
/// window, and returns its reply, or `None` when the window passed first.
async fn handler_reply<S>(
    framed: &mut Framed<S, KafkaCodec>,
    clock: &IdleClock,
    entry: DispatchEntry,
    context: DispatchContext<'_, '_>,
    request_span: tracing::Span,
) -> Option<Reply>
where
    S: AsyncWrite + Unpin + SendfileSink,
{
    if matches!(entry.kind(), DispatchKind::Fetch) {
        let sendfile_capable = SendfileSink::is_sendfile_capable(framed.get_ref());
        let work = fetch_reply(context, request_span, sendfile_capable);
        within_idle(framed, clock, work).await
    } else {
        within_idle(framed, clock, registry_reply(entry, context, request_span)).await
    }
}

/// Writes the answer to a `SaslHandshake` or `SaslAuthenticate` frame, and
/// says whether the connection goes on, as `Mute` with no window, or closes.
async fn write_sasl_outcome<S>(
    framed: &mut Framed<S, KafkaCodec>,
    clock: &mut IdleClock,
    outcome: Result<SaslFrameOutcome, BrokerError>,
) -> AfterResponse
where
    S: AsyncWrite + Unpin,
{
    let SaslFrameOutcome {
        response_bytes,
        close_after,
    } = match outcome {
        Ok(outcome) => outcome,
        Err(e) => {
            tracing::warn!(error = %e, "SASL dispatch error, closing connection");
            return AfterResponse::Close;
        }
    };
    if let Err(e) = framed.send(response_bytes).await {
        tracing::warn!(error = %e, "framed.send error during SASL, closing");
        return AfterResponse::Close;
    }
    clock.touch();
    if close_after {
        tracing::info!("closing connection after a refused SASL frame");
        return AfterResponse::Close;
    }
    AfterResponse::Mute(<Time as TimeExt>::ZERO)
}

/// Turns a KIP-219 throttle window into the deadline the connection stays
/// muted until, measured from now — that is, from the moment the response
/// finished being written.
fn mute_deadline(window: Time) -> Option<Instant> {
    (window > <Time as TimeExt>::ZERO).then(|| Instant::now() + window.to_std())
}

/// `ApiVersions` wire `api_key`. It has its own name because it is the one API
/// whose response header is always v0, whatever the body flexibility, and
/// whose v3+ request carries the KIP-511 client software name and version.
const API_VERSIONS_KEY: ApiKeyCode = ApiKey::ApiVersions as i16;

/// Kafka's `ClientInformation.UNKNOWN_NAME_OR_VERSION`: the KIP-511 software
/// name and version of a client that has not sent them.
const UNKNOWN_CLIENT_SOFTWARE: &str = "unknown";

fn capture_client_software(parsed: &ParsedRequest<'_>, name: &mut String, version: &mut String) {
    if parsed.api_key != API_VERSIONS_KEY || parsed.api_version < 3 {
        return;
    }
    let mut body = parsed.body;
    if let Ok(request) = ApiVersionsRequest::decode(&mut body, parsed.api_version)
        && is_valid_client_info(&request.client_software_name)
        && is_valid_client_info(&request.client_software_version)
    {
        name.clone_from(&request.client_software_name);
        version.clone_from(&request.client_software_version);
    }
}

/// A request parsed off a frame, and the span that traces it.
struct SpannedRequest<'a> {
    parsed: ParsedRequest<'a>,
    span: tracing::Span,
}

fn parse_connection_request<'a>(
    broker: &Broker,
    frame: &'a Bytes,
    peer: &SocketAddr,
) -> Option<SpannedRequest<'a>> {
    let peeked_api_key = match peek_api_key(frame) {
        Ok(api_key) => api_key,
        Err(error) => {
            tracing::warn!(%error, "frame too small to peek api_key, closing");
            return None;
        }
    };
    let parsed = match parse_request(frame, |api_key, version| {
        broker.handlers().body_flexible(api_key, version)
    }) {
        Ok(parsed) => parsed,
        Err(error) => {
            tracing::warn!(%error, "request parse error, closing");
            return None;
        }
    };
    assert2::assert!((parsed.api_key) == (peeked_api_key));
    let span = if tracing::enabled!(
        target: REQUEST_TARGET,
        tracing::Level::DEBUG
    ) {
        request_span(
            parsed.api_key,
            parsed.api_version,
            parsed.correlation_id,
            parsed.client_id,
            peer,
        )
    } else {
        tracing::Span::none()
    };
    Some(SpannedRequest { parsed, span })
}

/// The instant a request began and the guard that counts it in flight.
struct BegunRequest<'a> {
    started: time::Instant,
    in_flight: InFlightGuard<'a>,
}

fn begin_request<'a>(broker: &'a Broker, parsed: &ParsedRequest<'_>) -> BegunRequest<'a> {
    let started = time::Instant::now();
    broker.metrics.record_api_request(parsed.api_key);
    // Kafka's `RequestChannel` logs each request at DEBUG on
    // `kafka.request.logger`, which its default log4j configuration keeps at
    // WARN, so a request is never an INFO line.
    tracing::debug!(
        api_key = parsed.api_key,
        api_version = parsed.api_version,
        correlation_id = parsed.correlation_id,
        body_flexible = parsed.body_flexible,
        body_len = parsed.body.len(),
        "dispatching request"
    );
    BegunRequest {
        started,
        in_flight: InFlightGuard::new(&broker.metrics, parsed.api_key),
    }
}

/// Rejects a request at a version outside its API's range, or at a version --
/// or of an api key -- that the broker's `VersionGates` leave disabled: one
/// Kafka 4.3.1 does not serve, unless `unstable.api.versions.enable` or
/// `legacy_request_versions_enable` opts into it.
///
/// Kafka's `Processor.parseRequestHeader` throws `UnsupportedVersionException`
/// for the first and `InvalidRequestException` for the second, and
/// `SocketServer` closes the channel on either: no response frame.
/// `ApiVersions` is the one exception, since `ApiKeys.isVersionEnabled`
/// accepts every version of it: the request reaches `KafkaApis`, which
/// answers `UNSUPPORTED_VERSION` with a v0 body carrying the one
/// `ApiVersions` range (KIP-511), and the client falls back to that version.
/// Whether the listener serves `parsed` at its version: inside the handler's
/// decodable range, and not a version the broker's `VersionGates` disable.
///
/// `ApiVersions` is never a disabled version (Kafka's `isVersionEnabled`
/// accepts every one of it); a version above the one the listener serves --
/// trunk's KIP-1242 v5 while `unstable.api.versions.enable` is off -- is not
/// served, and [`reject_unsupported_version`] answers it `UNSUPPORTED_VERSION`
/// as it answers an unknown version.
fn is_served_version(broker: &Broker, entry: DispatchEntry, parsed: &ParsedRequest<'_>) -> bool {
    let api_versions_unserved = parsed.api_key == API_VERSIONS_KEY
        && parsed.api_version
            > krabka_raft::api_versions_max_version(broker.config.features.unstable_api_versions);
    entry.supports_version(parsed.api_version)
        && !api_versions_unserved
        && !is_disabled_version(
            parsed.api_key,
            parsed.api_version,
            broker.config.features.version_gates(),
        )
}

async fn reject_unsupported_version<S>(
    framed: &mut Framed<S, KafkaCodec>,
    clock: &mut IdleClock,
    broker: &Broker,
    entry: DispatchEntry,
    parsed: &ParsedRequest<'_>,
    auth: &ConnectionAuth,
) -> AfterResponse
where
    S: AsyncRead + AsyncWrite + Unpin + SendfileSink,
{
    const RESPONSE_VERSION: ApiVersion = 0;
    if parsed.api_key != API_VERSIONS_KEY {
        broker.metrics.record_api_request(parsed.api_key);
        broker
            .metrics
            .record_unsupported_api_request(parsed.api_key);
        tracing::warn!(
            api_key = parsed.api_key,
            api_version = parsed.api_version,
            "unsupported api version, closing connection"
        );
        return AfterResponse::Close;
    }
    let BegunRequest {
        started,
        in_flight: _in_flight,
    } = begin_request(broker, parsed);
    tracing::warn!(
        api_key = parsed.api_key,
        api_version = parsed.api_version,
        "unsupported api version"
    );
    broker
        .metrics
        .record_unsupported_api_request(parsed.api_key);
    let body = match unsupported_version_response(broker.config.features.unstable_api_versions) {
        Ok(body) => body,
        Err(error) => {
            tracing::warn!(%error, "unsupported-version response encode error, closing");
            return AfterResponse::Close;
        }
    };
    // The reply is encoded at v0, not at the version the client asked for,
    // and the throttle patch has to read that version and its flexibility.
    let shape = ResponseShape {
        version: RESPONSE_VERSION,
        body_flexible: entry.body_flexible(RESPONSE_VERSION),
    };
    let response = match encode_response(
        parsed.api_key,
        parsed.correlation_id,
        shape.body_flexible,
        &body,
    ) {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "response exceeds the int32 frame size, closing");
            return AfterResponse::Close;
        }
    };
    let delay = apply_request_quota(broker, parsed, auth, started.elapsed(), None);
    let response = ThrottledResponse {
        bytes: patch_response_throttle(response, parsed.api_key, shape, delay),
        throttle: delay,
        deferred_charge: None,
        close_after_response: false,
    };
    respond(framed, clock, Reply::Frame(response), &broker.metrics).await
}

/// KIP scope check (#683): `crate::api_catalog::INTER_BROKER_ONLY_APIS` is
/// tagged `controller`-only by its request schema, so no Kafka broker listener
/// ever routes it to a handler.
/// `ApiVersionManager.isApiEnabled` closes the connection before the request is
/// even parsed further; krabka does the same on a pure `ListenerKind::Client`
/// listener, and accepts these keys on `InterBroker` and
/// `ClientAndInterBroker` alike, where krabka's own peers send them and the
/// per-handler `ClusterAction` check applies.
fn is_inter_broker_api_on_client_listener(
    broker: &Broker,
    spec: &ListenerSpec,
    api_key: i16,
) -> bool {
    INTER_BROKER_ONLY_APIS.contains(&api_key)
        && broker.config.listener_kind(&spec.name) == ListenerKind::Client
}

/// Generic per-connection request loop.
///
/// `S` is the post-handshake byte stream: `TcpStream` for plaintext listeners,
/// and a rustls stream over a write-idle-tracked socket for userspace TLS. `spec`
/// carries the listener's protocol, so the loop initialises `ConnectionAuth`
/// correctly and gates pre-auth requests on SASL listeners.
async fn serve_connection_stream<S>(
    broker: Arc<Broker>,
    stream: S,
    spec: ListenerSpec,
    peer: SocketAddr,
    mtls_principal: Option<krabka_security::Principal>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static + SendfileSink,
{
    let sasl_mechanisms =
        resolve_sasl_mechanisms_for_listener(&spec, &broker.config.enabled_sasl_mechanisms)
            .to_owned();
    let mut connection = ConnectionLoop::open(ConnectionSetup {
        broker: &broker,
        spec: &spec,
        sasl_mechanisms: &sasl_mechanisms,
        stream,
        peer,
        mtls_principal,
    });
    connection.run().await;
    connection.close().await;
}

/// What a connection borrows from its listener and accept loop, and the
/// stream and identity it starts with.
struct ConnectionSetup<'a, S> {
    broker: &'a Broker,
    spec: &'a ListenerSpec,
    /// The SASL mechanisms the listener enables.
    sasl_mechanisms: &'a [krabka_security::SaslMechanism],
    stream: S,
    peer: SocketAddr,
    mtls_principal: Option<krabka_security::Principal>,
}

/// The state of one connection's request loop.
///
/// Fields drop in declaration order. The order is the reverse of the order in
/// which [`ConnectionLoop::open`] builds them, so the `active_connections`
/// gauge falls before the socket closes.
struct ConnectionLoop<'a, S> {
    broker: &'a Broker,
    spec: &'a ListenerSpec,
    peer: SocketAddr,
    /// Raw-token mode (v0 handshake) and the last KIP-368 re-auth start.
    sasl_session: sasl::SaslSession,
    /// KIP-219 channel mute. A throttled response is written immediately and
    /// the quota is enforced by refusing to read the next request until this
    /// deadline passes.
    mute_until: Option<Instant>,
    /// KIP-714 client software identity, populated by the first `ApiVersions`
    /// v3+ request, so `GetTelemetrySubscriptions` can be served even on
    /// connections that never sent `ApiVersions` (e.g. early-version clients).
    /// Kafka's `ClientInformation.EMPTY` names both `unknown` until a KIP-511
    /// `ApiVersions` says otherwise, so a `client_software_name=unknown`
    /// selector matches such a client.
    client_software: (String, String),
    frame_wait: FrameWaitPolicy,
    /// Resolved once, like the idle window: the listener's KIP-368
    /// re-authentication window, which every mechanism handler clamps its
    /// session to.
    sasl_listener: SaslListener<'a>,
    /// The listener's `connections.max.idle.ms`, run from the last socket
    /// activity through handler time and mutes alike.
    clock: IdleClock,
    /// Counts the connection as live until the loop is closed, whatever ended
    /// it (EOF, decode/send error, or SASL-session expiry).
    _active: ActiveConnectionGuard<'a>,
    connection_id: String,
    auth: ConnectionAuth,
    framed: Framed<write_idle::WriteIdle<S>, KafkaCodec>,
    limits: RequestLimits,
}

impl<'a, S> ConnectionLoop<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static + SendfileSink,
{
    fn open(
        ConnectionSetup {
            broker,
            spec,
            sasl_mechanisms,
            stream,
            peer,
            mtls_principal,
        }: ConnectionSetup<'a, S>,
    ) -> Self {
        let limits = RequestLimits::of(&broker.config);
        let stream =
            write_idle::for_response(stream, broker.config.connections_max_idle_for(&spec.name));
        let framed = Framed::new(stream, codec::codec(limits.request_max()));
        let is_sasl_listener = spec.protocol.requires_sasl();
        let auth =
            initial_connection_auth(is_sasl_listener, mtls_principal, &broker.audit_log, &peer);
        let connection_id = uuid::Uuid::new_v4().to_string();
        let active = ActiveConnectionGuard::new(&broker.metrics);
        let clock = IdleClock::start(broker.config.connections_max_idle_for(&spec.name));
        let sasl_listener = SaslListener {
            is_sasl: is_sasl_listener,
            mechanisms: sasl_mechanisms,
            max_reauth: broker.config.connections_max_reauth_for(&spec.name),
        };
        let frame_wait = FrameWaitPolicy {
            peer,
            metrics: broker.metrics.clone(),
            audit_log: Arc::clone(&broker.audit_log),
            failed_authentication_delay: broker.config.failed_authentication_delay(),
        };
        // Kafka's `SocketServer` `Acceptor` and `Processor` log a new connection
        // at DEBUG, and its `Selector` logs a close at DEBUG, as this loop does.
        tracing::debug!(listener = %spec.name, sasl = is_sasl_listener, "connection opened");
        Self {
            broker,
            spec,
            peer,
            sasl_session: sasl::SaslSession::default(),
            mute_until: None,
            client_software: (
                UNKNOWN_CLIENT_SOFTWARE.to_owned(),
                UNKNOWN_CLIENT_SOFTWARE.to_owned(),
            ),
            frame_wait,
            sasl_listener,
            clock,
            _active: active,
            connection_id,
            auth,
            framed,
            limits,
        }
    }

    async fn run(&mut self) {
        let broker = self.broker;
        let is_sasl_listener = self.sasl_listener.is_sasl;
        loop {
            let Some(received) = next_connection_frame(
                &mut self.framed,
                &self.auth,
                self.mute_until.take(),
                &mut self.clock,
                &self.frame_wait,
                FrameSizeLimit {
                    maximum: self.limits.for_auth(&self.auth),
                    budget: broker.queued_request_bytes.as_ref(),
                },
            )
            .await
            else {
                break;
            };
            let receive::RequestFrame {
                bytes: frame,
                byte_permit,
            } = received;
            // The body already holds its byte budget, including while it waits
            // for request execution capacity.
            // `queued.max.requests`: Kafka's bound is the depth of the request
            // queue, so the wait belongs after the read and not before it. A
            // connection sitting idle has nothing queued and must hold no
            // capacity; taking a permit to wait on the socket would let a broker
            // with more idle connections than permits stop serving anything.
            let Ok(permit) = Arc::clone(&broker.queued_requests_sem)
                .acquire_owned()
                .await
            else {
                break;
            };
            // Broker backpressure, like the byte budget: the idle window restarts.
            self.clock.touch();
            let _queued_guard =
                QueuedRequestGuard::new(permit, byte_permit, &broker.metrics, frame.len());
            // After a `SaslHandshake` v0 the exchange carries raw size-prefixed
            // SASL tokens with no Kafka request header, both ways.
            // A failed v0 exchange closes with no response.
            if is_sasl_listener && self.sasl_session.expects_raw_token(&self.auth) {
                let session = SaslConnection {
                    listener: &self.sasl_listener,
                    session: &mut self.sasl_session,
                };
                let token = sasl::handle_raw_sasl_token(
                    broker,
                    &frame,
                    &mut self.auth,
                    session,
                    &self.peer,
                );
                let Some(token) = within_idle(&mut self.framed, &self.clock, token).await else {
                    self.frame_wait.record_idle_close(&self.auth, &self.clock);
                    break;
                };
                match token {
                    Some(token) if self.framed.send(token.clone()).await.is_ok() => {
                        self.clock.touch();
                        continue;
                    }
                    _ => break,
                }
            }
            let Some(SpannedRequest {
                parsed,
                span: req_span,
            }) = parse_connection_request(broker, &frame, &self.peer)
            else {
                // Bytes the broker cannot read as a request are the same reason
                // as bytes the codec refused, one layer further in: the peer sent
                // something that is not a Kafka request and the connection ends.
                broker
                    .metrics
                    .record_connection_close(ConnectionCloseReason::DecodeError);
                break;
            };
            // KIP-368 session expiry, enforced where Kafka enforces it: on the
            // request that arrives past the deadline, not on a timer racing the
            // read. A client re-authenticates only when it next has something to
            // send, so it routinely opens the exchange after its window closed;
            // a `SaslHandshake` that begins the re-authentication passes, and any
            // other frame ends the connection with no response.
            if is_sasl_listener
                && self
                    .sasl_session
                    .closes_expired_session(&self.auth, parsed.api_key, now_ms())
            {
                // Kafka's `Processor.processCompletedReceives` logs this at DEBUG.
                tracing::debug!(
                    api_key = parsed.api_key,
                    principal = ?self.auth.principal().map(|p| p.name.as_str()),
                    listener = %self.spec.name,
                    "SASL session expired, closing connection (KIP-368)"
                );
                broker
                    .metrics
                    .record_connection_close(ConnectionCloseReason::SaslSessionExpired);
                break;
            }
            // Per-state request gate after Kafka's `SaslServerAuthenticator`
            // (see `ConnectionAuth::allows_request`). A refused request closes
            // the connection, after the ILLEGAL_SASL_STATE answer Kafka writes
            // when there is one (`sasl::refuse_gated_request`).
            if is_sasl_listener
                && (!self.auth.allows_request(parsed.api_key)
                    || self
                        .sasl_session
                        .repeats_api_versions(&self.auth, parsed.api_key))
            {
                let refusal = sasl::refuse_gated_request(
                    broker,
                    &parsed,
                    &self.auth,
                    &self.peer,
                    &self.spec.name,
                );
                match within_idle(&mut self.framed, &self.clock, refusal).await {
                    Some(Some(response)) => _ = self.framed.send(response).await,
                    Some(None) => {}
                    None => self.frame_wait.record_idle_close(&self.auth, &self.clock),
                }
                break;
            }
            let Some(entry) = broker.handlers().get(parsed.api_key) else {
                broker.metrics.record_api_request(parsed.api_key);
                tracing::warn!(
                    api_key = parsed.api_key,
                    api_version = parsed.api_version,
                    "unknown api, closing connection"
                );
                break;
            };
            if is_inter_broker_api_on_client_listener(broker, self.spec, parsed.api_key) {
                broker.metrics.record_api_request(parsed.api_key);
                tracing::warn!(
                    api_key = parsed.api_key,
                    listener = %self.spec.name,
                    "controller-scoped api key received on a client-reachable listener, closing connection"
                );
                break;
            }
            if !is_served_version(broker, entry, &parsed) {
                match reject_unsupported_version(
                    &mut self.framed,
                    &mut self.clock,
                    broker,
                    entry,
                    &parsed,
                    &self.auth,
                )
                .await
                {
                    AfterResponse::Close => break,
                    AfterResponse::Mute(window) => self.mute_until = mute_deadline(window),
                }
                continue;
            }
            // SASL frames (api_key 17 / 36) mutate the per-connection auth state,
            // which lives in this loop. They run *before* the regular handler
            // table because handlers receive only `&Broker` and have no way to
            // touch `auth`. Returning `Some(SaslFrameOutcome)` short-circuits
            // the normal registry path for that frame.
            let sasl_frame = try_handle_sasl_frame(
                broker,
                &parsed,
                &mut self.auth,
                &self.sasl_listener,
                &mut self.sasl_session,
                &self.peer,
            )
            .instrument(req_span.clone());
            let Some(sasl_frame) = within_idle(&mut self.framed, &self.clock, sasl_frame).await
            else {
                self.frame_wait.record_idle_close(&self.auth, &self.clock);
                break;
            };
            if let Some(outcome) = sasl_frame {
                match write_sasl_outcome(&mut self.framed, &mut self.clock, outcome).await {
                    AfterResponse::Close => break,
                    AfterResponse::Mute(_) => continue,
                }
            }

            capture_client_software(
                &parsed,
                &mut self.client_software.0,
                &mut self.client_software.1,
            );
            self.sasl_session.note_api_versions(&self.auth, &parsed);

            let BegunRequest {
                in_flight: _in_flight,
                ..
            } = begin_request(broker, &parsed);

            let context = DispatchContext {
                broker,
                parsed: &parsed,
                frame: &frame,
                auth: &self.auth,
                peer: &self.peer,
                connection_id: &self.connection_id,
                listener_name: &self.spec.name,
                client_software_name: &self.client_software.0,
                client_software_version: &self.client_software.1,
            };

            let Some(reply) =
                handler_reply(&mut self.framed, &self.clock, entry, context, req_span).await
            else {
                self.frame_wait.record_idle_close(&self.auth, &self.clock);
                break;
            };
            match respond(&mut self.framed, &mut self.clock, reply, &broker.metrics).await {
                AfterResponse::Close => break,
                AfterResponse::Mute(window) => self.mute_until = mute_deadline(window),
            }
        }
    }

    async fn close(self) {
        self.broker
            .share_partition_leaders
            .release_connection(&self.connection_id)
            .await;
        // KIP-714: Kafka registers `ClientMetricsManager.connectionDisconnectListener`
        // with the socket server, which drops the client instance that this
        // connection created.
        self.broker
            .client_metrics
            .connection_closed(&self.connection_id);
        tracing::debug!("connection closed");
    }
}
