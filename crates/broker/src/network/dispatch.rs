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

use std::net::SocketAddr;

use bytes::Bytes;
use futures_util::SinkExt;
use krabka_protocol::{Decode as _, api_key::ApiKey};
use krabka_units::convert::ByteSizeExt as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::Instrument as _;

mod accept;
mod fetch;
mod guards;
mod registry;
mod response;
mod sasl;
mod session;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod throttle_audit;
mod unsupported_version;

pub use self::accept::serve_connection_on_listener;
use self::{
    fetch::dispatch_fetch,
    guards::{ActiveConnectionGuard, InFlightGuard},
    registry::{DispatchContext, send_registry_response},
    response::{ResponseShape, encode_response, maybe_apply_request_quota},
    sasl::{SaslFrameOutcome, try_handle_sasl_frame},
    session::{FrameWaitPolicy, initial_connection_auth, next_connection_frame},
};
use crate::{broker::Broker, codes, handlers::ApiKeyCode, network::codec};

/// `ApiVersions` wire `api_key`. It has its own name because it is the one API
/// whose response header is always v0, whatever the body flexibility, and
/// whose v3+ request carries the KIP-511 client software name and version.
const API_VERSIONS_KEY: ApiKeyCode = ApiKey::ApiVersions as i16;

fn capture_client_software(
    parsed: &crate::network::request::ParsedRequest<'_>,
    name: &mut String,
    version: &mut String,
) {
    if parsed.api_key != API_VERSIONS_KEY || parsed.api_version < 3 {
        return;
    }
    let mut body = parsed.body;
    if let Ok(request) = krabka_protocol::owned::api_versions_request::ApiVersionsRequest::decode(
        &mut body,
        parsed.api_version,
    ) && crate::handlers::api_versions::is_valid_client_info(&request.client_software_name)
        && crate::handlers::api_versions::is_valid_client_info(&request.client_software_version)
    {
        name.clone_from(&request.client_software_name);
        version.clone_from(&request.client_software_version);
    }
}

fn parse_connection_request<'a>(
    broker: &Broker,
    frame: &'a Bytes,
    peer: &SocketAddr,
) -> Option<(crate::network::request::ParsedRequest<'a>, tracing::Span)> {
    let peeked_api_key = match crate::network::request::peek_api_key(frame) {
        Ok(api_key) => api_key,
        Err(error) => {
            tracing::warn!(%error, "frame too small to peek api_key, closing");
            return None;
        }
    };
    let parsed = match crate::network::request::parse_request(frame, |api_key, version| {
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
        target: crate::telemetry::REQUEST_TARGET,
        tracing::Level::DEBUG
    ) {
        crate::telemetry::request_span(
            parsed.api_key,
            parsed.api_version,
            parsed.correlation_id,
            parsed.client_id,
            peer,
        )
    } else {
        tracing::Span::none()
    };
    Some((parsed, span))
}

fn begin_request(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
) -> (std::time::Instant, InFlightGuard) {
    let started = std::time::Instant::now();
    broker.metrics.record_api_request(parsed.api_key);
    tracing::info!(
        api_key = parsed.api_key,
        api_version = parsed.api_version,
        correlation_id = parsed.correlation_id,
        body_flexible = parsed.body_flexible,
        body_len = parsed.body.len(),
        "dispatching request"
    );
    (started, InFlightGuard::new(&broker.metrics, parsed.api_key))
}

async fn send_unsupported_version<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    broker: &Broker,
    entry: crate::handlers::registry::DispatchEntry,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    started: std::time::Instant,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    broker
        .metrics
        .record_unsupported_api_request(parsed.api_key);
    let response_version = if parsed.api_key == API_VERSIONS_KEY {
        0
    } else {
        entry.nearest_supported_version(parsed.api_version)
    };
    let encoded_body = if parsed.api_key == API_VERSIONS_KEY {
        Some(crate::handlers::api_versions::unsupported_version_response())
    } else {
        unsupported_version::body(parsed.api_key, response_version)
    };
    let Some(encoded_body) = encoded_body else {
        tracing::warn!(
            api_key = parsed.api_key,
            "missing unsupported-version response shape, closing"
        );
        return false;
    };
    let body = match encoded_body {
        Ok(body) => body,
        Err(error) => {
            tracing::warn!(%error, "unsupported-version response encode error, closing");
            return false;
        }
    };
    // The reply is encoded at `response_version`, not at the version the
    // client asked for, and its header flexibility follows that version. The
    // throttle patch has to read the same pair or it writes over the wrong
    // bytes: a request below a flexible-from-v0 API's minimum parses with a
    // non-flexible header while the reply carries the flexible one.
    let shape = ResponseShape {
        version: response_version,
        body_flexible: entry.body_flexible(response_version),
    };
    let response = match encode_response(
        parsed.api_key,
        parsed.correlation_id,
        shape.body_flexible,
        &body,
        broker.config.socket_request_max.bytes_usize(),
    ) {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "response exceeds configured frame maximum, closing");
            return false;
        }
    };
    let response = maybe_apply_request_quota(broker, response, parsed, shape, auth, started).await;
    if let Err(error) = framed.send(response).await {
        tracing::warn!(%error, "framed.send error, closing");
        return false;
    }
    true
}

/// Generic per-connection request loop.
///
/// `S` is the post-handshake byte stream: `TcpStream` for plaintext listeners,
/// and `tokio_rustls::server::TlsStream<TcpStream>` for TLS listeners. `spec`
/// carries the listener's protocol, so the loop initialises `ConnectionAuth`
/// correctly and gates pre-auth requests on SASL listeners.
// each api_key intercept arm adds ~15 lines.
async fn serve_connection_stream<S>(
    broker: std::sync::Arc<Broker>,
    stream: S,
    spec: crate::config::ListenerSpec,
    peer: SocketAddr,
    mtls_principal: Option<krabka_security::Principal>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static + crate::network::fetch_writer::SendfileSink,
{
    let mut framed: Framed<S, _> = Framed::new(
        stream,
        codec::codec(broker.config.socket_request_max.bytes_usize()),
    );
    let is_sasl_listener = spec.protocol.requires_sasl();
    let sasl_mechanisms = crate::network::listener::resolve_sasl_mechanisms_for_listener(
        &spec,
        &broker.config.enabled_sasl_mechanisms,
    )
    .to_owned();
    let mut auth = initial_connection_auth(is_sasl_listener, mtls_principal);
    let connection_id = uuid::Uuid::new_v4().to_string();
    // Track live connections for the duration of this serve loop. The
    // gauge is decremented when `_conn` drops on any loop exit (EOF,
    // decode/send error, or SASL-session expiry).
    let _conn = ActiveConnectionGuard::new(&broker.metrics);
    // Resolved once: the listener's `connections.max.idle.ms`, with its
    // per-listener override already applied. `next_connection_frame` re-arms
    // the deadline from it on every frame read.
    let frame_wait = FrameWaitPolicy {
        idle: broker.config.connections_max_idle_for(&spec.name),
        peer,
        metrics: broker.metrics.clone(),
    };
    tracing::info!(listener = %spec.name, sasl = is_sasl_listener, "connection opened");

    // KIP-714 client software identity, populated by the first ApiVersions v3+ request.
    // so `GetTelemetrySubscriptions` can be served even on connections that
    // never sent `ApiVersions` (e.g. early-version clients).
    let mut client_software = (String::new(), String::new());

    loop {
        let Some(frame) = next_connection_frame(&mut framed, &auth, &frame_wait).await else {
            break;
        };
        let Some((parsed, req_span)) = parse_connection_request(&broker, &frame, &peer) else {
            // Bytes the broker cannot read as a request are the same reason
            // as bytes the codec refused, one layer further in: the peer sent
            // something that is not a Kafka request and the connection ends.
            broker
                .metrics
                .record_connection_close(crate::metrics::ConnectionCloseReason::DecodeError);
            break;
        };
        // Per-state request gate: on SASL listeners, gate every api_key
        // through `auth.allows_request(api_key)`. This covers:
        //   - Anonymous / Negotiating: only the pre-auth allowlist
        //     (ApiVersions=18, SaslHandshake=17, SaslAuthenticate=36).
        //   - Reauthenticating (KIP-368 in-band re-auth in progress): only
        //     SaslAuthenticate=36 — any other request during re-auth is a
        //     protocol violation and the connection is closed.
        //   - Authenticated: all api_keys allowed.
        // Anything blocked closes the TCP connection with no body.
        //
        // Response-shape note: every api_key has a different response body,
        // so producing a typed `error_code = 34` frame from this generic
        // dispatch layer would require a switch over every api_key. The
        // SASL path sends a *typed* SaslAuthenticate(36) response with error_code=58
        // on credential failure (its specific shape is known there). For
        // the generic pre-auth gate we close the TCP connection without
        // sending a body — JVM clients surface this to the caller as an
        // auth failure (closed connection during SASL), and this matches
        // the conservative behaviour we want for unauthenticated peers.
        if is_sasl_listener && !auth.allows_request(parsed.api_key) {
            tracing::info!(
                api_key = parsed.api_key,
                listener = %spec.name,
                "request blocked by per-state auth gate (ILLEGAL_SASL_STATE), closing connection"
            );
            let _ = codes::ILLEGAL_SASL_STATE; // referenced for docs/grep
            // An ILLEGAL_SASL_STATE reject is an authentication failure, the
            // same as the one `try_handle_sasl_frame` records for a
            // `SaslAuthenticate` that arrives with no handshake behind it, so
            // it is counted the same way: under the mechanism a handshake
            // named, or under the `Unknown` sentinel when none did. Without
            // it a peer that opens a connection on a SASL listener and
            // immediately sends Produce is closed and counted nowhere.
            broker.metrics.record_authentication(
                auth.negotiated_mechanism()
                    .map_or(crate::metrics::UNKNOWN_LABEL, |mechanism| {
                        mechanism.wire_name()
                    }),
                false,
            );
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
        if !entry.supports_version(parsed.api_version) {
            let (started, _in_flight) = begin_request(&broker, &parsed);
            tracing::warn!(
                api_key = parsed.api_key,
                api_version = parsed.api_version,
                "unsupported api version"
            );
            if !send_unsupported_version(&mut framed, &broker, entry, &parsed, &auth, started).await
            {
                break;
            }
            continue;
        }
        // SASL frames (api_key 17 / 36) mutate the per-connection auth state,
        // which lives in this loop. They run *before* the regular handler
        // table because handlers receive only `&Broker` and have no way to
        // touch `auth`. Returning `Some(SaslFrameOutcome)` short-circuits
        // the normal registry path for that frame.
        if let Some(outcome) = try_handle_sasl_frame(&broker, &parsed, &mut auth, &sasl_mechanisms)
            .instrument(req_span.clone())
            .await
        {
            let SaslFrameOutcome {
                response_bytes,
                close_after,
            } = match outcome {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, "SASL dispatch error, closing connection");
                    break;
                }
            };
            if let Err(e) = framed.send(response_bytes).await {
                tracing::warn!(error = %e, "framed.send error during SASL, closing");
                break;
            }
            if close_after {
                tracing::info!("closing connection after failed SaslAuthenticate");
                break;
            }
            continue;
        }

        capture_client_software(&parsed, &mut client_software.0, &mut client_software.1);

        let (started, _in_flight) = begin_request(&broker, &parsed);

        if matches!(entry.kind(), crate::handlers::DispatchKind::Fetch) {
            if !dispatch_fetch(
                &mut framed,
                &broker,
                &parsed,
                &auth,
                &peer,
                req_span.clone(),
            )
            .await
            {
                break;
            }
            continue;
        }

        let context = DispatchContext {
            broker: &broker,
            parsed: &parsed,
            frame: &frame,
            auth: &auth,
            peer: &peer,
            connection_id: &connection_id,
            listener_name: &spec.name,
            client_software_name: &client_software.0,
            client_software_version: &client_software.1,
        };
        if !send_registry_response(&mut framed, entry, context, req_span, started).await {
            break;
        }
    }
    broker
        .share_partition_leaders
        .release_connection(&connection_id)
        .await;
    tracing::info!("connection closed");
}
