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
    response::{ResponseShape, encode_response, maybe_apply_request_quota},
    session::principal_or_anonymous,
};
use crate::{broker::Broker, error::BrokerError};

pub(super) async fn send_registry_response<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    entry: crate::handlers::DispatchEntry,
    context: DispatchContext<'_, '_>,
    request_span: tracing::Span,
    started: std::time::Instant,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut response = match dispatch_registry_response(entry, context)
        .instrument(request_span)
        .await
    {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            tracing::warn!("registry entry has no ordinary dispatcher, closing connection");
            return false;
        }
        Err(error) => {
            context
                .broker
                .metrics
                .record_request_error(context.parsed.api_key);
            tracing::warn!(%error, "registry dispatch error, closing connection");
            return false;
        }
    };
    if response.is_empty() && matches!(entry.kind(), crate::handlers::DispatchKind::Produce(_)) {
        return true;
    }
    if entry.quota_policy() == crate::handlers::RequestQuotaPolicy::ApplyFallbackAccounting {
        response = maybe_apply_request_quota(
            context.broker,
            response,
            context.parsed,
            ResponseShape::mirroring_request(context.parsed),
            context.auth,
            started,
        )
        .await;
    }
    if let Err(error) = framed.send(response).await {
        tracing::warn!(%error, "framed.send error, closing");
        return false;
    }
    true
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
) -> Option<Result<Bytes, BrokerError>> {
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
            Some(encode_dispatch_result(
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
            ))
        }
        crate::handlers::DispatchKind::Auth(handler) => Some(encode_dispatch_result(
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
        )),
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
            Some(if response_required {
                encoded
            } else {
                encoded.map(|_| Bytes::new())
            })
        }
        crate::handlers::DispatchKind::Telemetry(handler) => {
            let ctx = crate::handlers::TelemetryContext::new(
                peer,
                parsed.client_id.unwrap_or(""),
                client_software_name,
                client_software_version,
            );
            Some(encode_dispatch_result(
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
            ))
        }
        crate::handlers::DispatchKind::Plain(_)
        | crate::handlers::DispatchKind::Fetch
        | crate::handlers::DispatchKind::SaslMetadata => None,
    }
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
) -> Result<Option<Bytes>, BrokerError> {
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
                .map(Some)
            }
            crate::handlers::DispatchKind::Fetch | crate::handlers::DispatchKind::SaslMetadata => {
                Ok(None)
            }
            _ => Ok(None),
        },
    }
}
