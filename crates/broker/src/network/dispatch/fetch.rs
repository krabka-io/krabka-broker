//! Fetch (`api_key` 1) dispatch. It authorizes the request through the
//! per-connection principal and returns the response as an ordered write plan
//! rather than one contiguous buffer, so that records regions stay zero-copy.

use std::net::SocketAddr;

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::SinkExt;
use krabka_units::convert::ByteSizeExt as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::Instrument as _;

use super::{response::encode_response, session::principal_or_anonymous};
use crate::{broker::Broker, error::BrokerError};

pub(super) async fn dispatch_fetch<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    request_span: tracing::Span,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin + crate::network::fetch_writer::SendfileSink,
{
    let sendfile_capable =
        crate::network::fetch_writer::SendfileSink::is_sendfile_capable(framed.get_ref());
    match handle_fetch_frame_from_parsed(broker, parsed, auth, peer, sendfile_capable)
        .instrument(request_span)
        .await
    {
        Ok(operations) => {
            if let Err(error) = SinkExt::<Bytes>::flush(framed).await {
                tracing::warn!(%error, "framed.flush error before fetch plan, closing");
                return false;
            }
            if let Err(error) =
                crate::network::fetch_writer::write_fetch_plan(framed.get_mut(), operations).await
            {
                tracing::warn!(%error, "fetch plan write error, closing");
                return false;
            }
            true
        }
        Err(error) => {
            broker.metrics.record_request_error(parsed.api_key);
            tracing::warn!(%error, "Fetch dispatch error, closing connection");
            false
        }
    }
}

/// Decodes and dispatches a `Fetch` (`api_key` 1) frame.
///
/// The function reads the authenticated principal from the per-connection
/// `auth` state, and the peer `SocketAddr` from the accept-time capture, so
/// the handler can batch-authorize every topic in the request for `Read`. On
/// PLAINTEXT and SSL listeners the loop init makes the connection implicitly
/// `Authenticated { ANONYMOUS / Plain }`, so `principal()` always returns
/// `Some` here. The `unwrap_or_else` fallback covers the defensive SASL
/// pre-auth case.
///
/// The function builds the Fetch response as an ordered [`WriteOp`] plan
/// instead of one contiguous `Bytes`. The first op of the plan carries the
/// 4-byte frame length and the correlation header. The later ops are the
/// response envelope, interleaved with the records region of each partition.
/// A records region is a refcounted view of the verbatim `.log` bytes, with no
/// copy. The connection writer drains the plan directly on the raw stream, and
/// skips `encode_response` and the `Framed` codec copies.
///
/// The legacy v0 to v3 path down-converts and has no canonical write plan.
/// The function encodes it the old way and returns it as a single `Inline` op,
/// which is the existing copy path expressed as a one-element plan.
async fn handle_fetch_frame_from_parsed(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    sendfile_capable: bool,
) -> Result<Vec<crate::network::fetch_writer::WriteOp>, BrokerError> {
    use crate::network::fetch_writer::{WriteOp, build_fetch_plan};

    assert2::assert!((parsed.api_key) == (1));

    let principal = principal_or_anonymous(auth);
    let ctx = crate::handlers::RequestContext::new(
        principal,
        peer,
        parsed.client_id.unwrap_or(""),
        "",
        sendfile_capable && parsed.api_version >= 4,
        "",
    );

    let (resp, version) = crate::handlers::fetch::handle(
        broker,
        parsed.api_version,
        parsed.correlation_id,
        parsed.body,
        &ctx,
    )
    .await?;

    if version < 4 {
        // Legacy down-conversion path: encode the whole body the old way and
        // wrap it (plus the response header) as a single inline op.
        let body_bytes = crate::handlers::fetch::encode_fetch_response(resp, version)?;
        let framed = encode_response(
            parsed.api_key,
            parsed.correlation_id,
            parsed.body_flexible,
            &body_bytes,
            broker.config.socket_request_max.bytes_usize(),
        )?;
        // Prepend the 4-byte frame length so the writer path is uniform.
        let mut framed_with_len = BytesMut::with_capacity(4 + framed.len());
        framed_with_len.put_u32(u32::try_from(framed.len()).map_err(|_| {
            BrokerError::Io(std::io::Error::other(
                "fetch response exceeds max frame size",
            ))
        })?);
        framed_with_len.put_slice(&framed);
        return Ok(vec![WriteOp::Inline(framed_with_len.freeze())]);
    }

    // On plaintext connections (SENDFILE alias: Linux + Apple + FreeBSD/
    // DragonFly), drain file-backed records regions via sendfile; everywhere
    // else (TLS, Windows) use the portable vectored resolver. `do_read` only
    // ever emits `FileRegions` when `sendfile_capable`, so the resolver choice
    // and the payload kind stay in lock-step.
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    {
        if sendfile_capable && parsed.api_version >= 4 {
            return build_fetch_plan(
                &resp,
                version,
                parsed.correlation_id,
                parsed.body_flexible,
                broker.config.socket_request_max.bytes_usize(),
                crate::network::fetch_writer::resolve_records_sendfile,
            );
        }
    }

    build_fetch_plan(
        &resp,
        version,
        parsed.correlation_id,
        parsed.body_flexible,
        broker.config.socket_request_max.bytes_usize(),
        crate::network::fetch_writer::resolve_records_inline,
    )
}
