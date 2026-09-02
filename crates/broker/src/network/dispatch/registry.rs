//! Registry-backed dispatch. It builds the per-request context for the
//! handler kind that the registry entry names, calls that handler, frames the
//! body it returns, and writes the response back to the client.

use std::net::SocketAddr;

use bytes::Bytes;
use futures_util::SinkExt;
use krabka_units::convert::ByteSizeExt as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::Instrument as _;

use super::{
    AfterResponse,
    response::{ResponseShape, ThrottledResponse, apply_request_quota, encode_response},
    session::principal_or_anonymous,
};
use crate::{broker::Broker, error::BrokerError};

pub(super) async fn send_registry_response<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    entry: crate::handlers::DispatchEntry,
    context: DispatchContext<'_, '_>,
    request_span: tracing::Span,
    started: std::time::Instant,
) -> AfterResponse
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut response = match dispatch_registry_response(entry, context)
        .instrument(request_span)
        .await
    {
        Ok(Some(response)) => response,
        Ok(None) => {
            tracing::warn!("registry entry has no ordinary dispatcher, closing connection");
            return AfterResponse::Close;
        }
        Err(error) => {
            context
                .broker
                .metrics
                .record_request_error(context.parsed.api_key);
            tracing::warn!(%error, "registry dispatch error, closing connection");
            return AfterResponse::Close;
        }
    };
    if response.bytes.is_empty()
        && matches!(entry.kind(), crate::handlers::DispatchKind::Produce(_))
    {
        // `acks=0`: there is no response frame to write, but the handler may
        // still have charged a quota, so the mute window stands.
        return AfterResponse::Mute(response.throttle);
    }
    if entry.quota_policy() == crate::handlers::RequestQuotaPolicy::ApplyFallbackAccounting {
        // Kafka mutes the channel once per request, for the longest window
        // any quota asked for, so a handler-charged window is folded in with
        // `max` rather than added. No handler reaches this branch with a
        // window today: every `ApplyFallbackAccounting` entry is a
        // `DispatchEntry::plain` dispatch, whose handler takes no
        // `RequestContext` and so can charge nothing. Combining here keeps
        // the rule in the one place that has both windows, should a
        // context-taking api ever take fallback accounting.
        let handler_throttle = response.throttle;
        response = apply_request_quota(
            context.broker,
            response.bytes,
            context.parsed,
            ResponseShape::mirroring_request(context.parsed),
            context.auth,
            started,
        );
        response.throttle = response.throttle.max(handler_throttle);
    }
    if let Err(error) = framed.send(response.bytes).await {
        tracing::warn!(%error, "framed.send error, closing");
        return AfterResponse::Close;
    }
    AfterResponse::Mute(response.throttle)
}

#[derive(Clone, Copy)]
pub(super) struct DispatchContext<'a, 'request> {
    pub(super) broker: &'a Broker,
    pub(super) parsed: &'a crate::network::request::ParsedRequest<'request>,
    pub(super) frame: &'a Bytes,
    pub(super) auth: &'a crate::network::auth::ConnectionAuth,
    pub(super) peer: &'a SocketAddr,
    pub(super) connection_id: &'a str,
    pub(super) listener_name: &'a str,
    pub(super) client_software_name: &'a str,
    pub(super) client_software_version: &'a str,
}

async fn dispatch_registered_bytes(
    entry: crate::handlers::DispatchEntry,
    context: DispatchContext<'_, '_>,
) -> Option<Result<ThrottledResponse, BrokerError>> {
    let DispatchContext {
        broker,
        parsed,
        frame,
        auth,
        peer,
        connection_id,
        listener_name,
        client_software_name,
        client_software_version,
    } = context;
    match entry.kind() {
        crate::handlers::DispatchKind::Context(handler) => {
            let ctx = crate::handlers::RequestContext::new(
                principal_or_anonymous(auth),
                peer,
                parsed.client_id.unwrap_or(""),
                connection_id,
                false,
                listener_name,
            );
            let encoded = encode_dispatch_result(
                parsed,
                broker.config.socket_request_max.bytes_usize(),
                handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                    &ctx,
                )
                .await,
            );
            Some(with_recorded_throttle(&ctx, encoded))
        }
        crate::handlers::DispatchKind::Auth(handler) => Some(unthrottled(encode_dispatch_result(
            parsed,
            broker.config.socket_request_max.bytes_usize(),
            handler(
                broker,
                parsed.api_version,
                parsed.correlation_id,
                parsed.body,
                auth,
                peer,
            )
            .await,
        ))),
        crate::handlers::DispatchKind::Produce(handler) => {
            let ctx = crate::handlers::RequestContext::new(
                principal_or_anonymous(auth),
                peer,
                parsed.client_id.unwrap_or(""),
                connection_id,
                false,
                "",
            );
            let body_offset = frame.len() - parsed.body.len();
            let body_bytes = frame.slice(body_offset..);
            let response_required = match crate::handlers::produce::response_required(
                parsed.body,
                body_bytes.clone(),
                parsed.api_version,
            ) {
                Ok(required) => required,
                Err(error) => return Some(Err(error)),
            };
            let encoded = encode_dispatch_result(
                parsed,
                broker.config.socket_request_max.bytes_usize(),
                handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                    body_bytes,
                    &ctx,
                )
                .await,
            );
            let encoded = if response_required {
                encoded
            } else {
                encoded.map(|_| Bytes::new())
            };
            Some(with_recorded_throttle(&ctx, encoded))
        }
        crate::handlers::DispatchKind::Telemetry(handler) => {
            let ctx = crate::handlers::TelemetryContext::new(
                peer,
                parsed.client_id.unwrap_or(""),
                client_software_name,
                client_software_version,
            );
            Some(unthrottled(encode_dispatch_result(
                parsed,
                broker.config.socket_request_max.bytes_usize(),
                handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                    &ctx,
                )
                .await,
            )))
        }
        crate::handlers::DispatchKind::Plain(_)
        | crate::handlers::DispatchKind::Fetch
        | crate::handlers::DispatchKind::SaslMetadata => None,
    }
}

/// Pairs a handler's framed bytes with the KIP-219 window that handler
/// recorded on its [`crate::handlers::RequestContext`].
fn with_recorded_throttle(
    ctx: &crate::handlers::RequestContext<'_>,
    encoded: Result<Bytes, BrokerError>,
) -> Result<ThrottledResponse, BrokerError> {
    let throttle = ctx.take_throttle();
    encoded.map(|bytes| ThrottledResponse { bytes, throttle })
}

/// Wraps the bytes of a handler kind that has no `RequestContext` and so can
/// charge no quota of its own.
fn unthrottled(encoded: Result<Bytes, BrokerError>) -> Result<ThrottledResponse, BrokerError> {
    encoded.map(ThrottledResponse::unthrottled)
}

fn encode_dispatch_result(
    parsed: &crate::network::request::ParsedRequest<'_>,
    max_frame_bytes: usize,
    result: Result<Bytes, BrokerError>,
) -> Result<Bytes, BrokerError> {
    result.and_then(|body| {
        encode_response(
            parsed.api_key,
            parsed.correlation_id,
            parsed.body_flexible,
            &body,
            max_frame_bytes,
        )
    })
}

async fn dispatch_registry_response(
    entry: crate::handlers::DispatchEntry,
    context: DispatchContext<'_, '_>,
) -> Result<Option<ThrottledResponse>, BrokerError> {
    let DispatchContext { broker, parsed, .. } = context;
    match dispatch_registered_bytes(entry, context).await {
        Some(result) => result.map(Some),
        None => match entry.kind() {
            crate::handlers::DispatchKind::Plain(handler) => {
                let body = handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                )
                .await?;
                encode_response(
                    parsed.api_key,
                    parsed.correlation_id,
                    parsed.body_flexible,
                    &body,
                    broker.config.socket_request_max.bytes_usize(),
                )
                .map(|bytes| Some(ThrottledResponse::unthrottled(bytes)))
            }
            crate::handlers::DispatchKind::Fetch | crate::handlers::DispatchKind::SaslMetadata => {
                Ok(None)
            }
            _ => Ok(None),
        },
    }
}
