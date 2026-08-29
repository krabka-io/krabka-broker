//! Kafka request and response framing for the controller listener: the
//! length-prefixed frame codec, the request-header decode, and the
//! flexible-version negotiation that decides whether a frame carries tagged
//! fields.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_ids::{ApiKey, ApiVersion};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    error::RaftError,
    wire::{API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE},
};

/// Kafka request-header `correlation_id`, echoed back in the response header.
pub(super) type CorrelationId = i32;

pub(super) fn is_eof(e: &RaftError) -> bool {
    matches!(e,
        RaftError::Storage(krabka_log::LogError::Io(io))
            if io.kind() == std::io::ErrorKind::UnexpectedEof
    )
}

fn io_err(e: std::io::Error) -> RaftError {
    RaftError::Storage(krabka_log::LogError::Io(e))
}

fn truncated(needed: usize) -> RaftError {
    RaftError::Protocol(krabka_protocol::ProtocolError::UnexpectedEof { needed })
}

fn require_remaining(available: usize, required: usize) -> Result<(), RaftError> {
    match required.checked_sub(available) {
        Some(0) | None => Ok(()),
        Some(needed) => Err(truncated(needed)),
    }
}

fn request_is_flexible(
    api_key: i16,
    version: i16,
    admin_router: Option<&dyn crate::ControllerAdminRouter>,
) -> bool {
    use krabka_protocol::owned::{
        add_raft_voter_request, api_versions_request, begin_quorum_epoch_request,
        broker_heartbeat_request, broker_registration_request, controller_registration_request,
        describe_cluster_request, describe_quorum_request, end_quorum_epoch_request, fetch_request,
        fetch_snapshot_request, remove_raft_voter_request, update_raft_voter_request, vote_request,
    };

    let flexible_min = match api_key {
        api_versions_request::API_KEY => Some(api_versions_request::FLEXIBLE_MIN),
        fetch_request::API_KEY => Some(fetch_request::FLEXIBLE_MIN),
        vote_request::API_KEY => Some(vote_request::FLEXIBLE_MIN),
        begin_quorum_epoch_request::API_KEY => Some(begin_quorum_epoch_request::FLEXIBLE_MIN),
        end_quorum_epoch_request::API_KEY => Some(end_quorum_epoch_request::FLEXIBLE_MIN),
        fetch_snapshot_request::API_KEY => Some(fetch_snapshot_request::FLEXIBLE_MIN),
        describe_quorum_request::API_KEY => Some(describe_quorum_request::FLEXIBLE_MIN),
        describe_cluster_request::API_KEY => Some(describe_cluster_request::FLEXIBLE_MIN),
        broker_registration_request::API_KEY => Some(broker_registration_request::FLEXIBLE_MIN),
        broker_heartbeat_request::API_KEY => Some(broker_heartbeat_request::FLEXIBLE_MIN),
        controller_registration_request::API_KEY => {
            Some(controller_registration_request::FLEXIBLE_MIN)
        }
        add_raft_voter_request::API_KEY => Some(add_raft_voter_request::FLEXIBLE_MIN),
        remove_raft_voter_request::API_KEY => Some(remove_raft_voter_request::FLEXIBLE_MIN),
        update_raft_voter_request::API_KEY => Some(update_raft_voter_request::FLEXIBLE_MIN),
        API_KEY_SUBMIT_CHANGE | API_KEY_METADATA_FETCH => Some(i16::MIN),
        _ => admin_router.and_then(|router| {
            router
                .api_versions()
                .iter()
                .find(|api| api.api_key == api_key)
                .map(|api| api.flexible_min)
        }),
    };
    flexible_min.is_some_and(|minimum| version >= minimum)
}

pub(super) async fn read_one_request<S>(
    stream: &mut S,
    admin_router: Option<&dyn crate::ControllerAdminRouter>,
) -> Result<
    (
        ApiKey,
        ApiVersion,
        CorrelationId,
        Option<String>,
        Bytes,
        bool,
    ),
    RaftError,
>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    const REQUEST_HEADER_FIXED_LEN: usize = 8;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(io_err)?;
    let raw_len = i32::from_be_bytes(len_buf);
    let len = usize::try_from(raw_len.max(0)).unwrap_or(0);
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.map_err(io_err)?;

    // RequestHeader v2 (flexible): api_key(i16), api_version(i16),
    // correlation_id(i32), client_id(NULLABLE_STRING), tagged_fields(varint=0).
    // The two adjacent header `int16`s are wrapped into distinct newtypes here so
    // the transpose-prone pair can't be swapped by callers.
    let mut cur: &[u8] = &frame;
    require_remaining(cur.remaining(), REQUEST_HEADER_FIXED_LEN)?;
    let api_key_n = ApiKey(cur.get_i16());
    let api_version = ApiVersion(cur.get_i16());
    let correlation_id = cur.get_i32();

    // Decode client_id: NULLABLE_STRING (i16 length + bytes; -1 = null).
    require_remaining(cur.remaining(), 2)?;
    let cs_len = cur.get_i16();
    let client_id = match cs_len {
        -1 => None,
        0.. => {
            let n = usize::try_from(cs_len).expect("non-negative i16 fits usize");
            require_remaining(cur.remaining(), n)?;
            let (raw, rest) = cur.split_at(n);
            cur = rest;
            Some(
                std::str::from_utf8(raw)
                    .map_err(krabka_protocol::ProtocolError::InvalidUtf8)?
                    .to_owned(),
            )
        }
        _ => {
            return Err(RaftError::Protocol(
                krabka_protocol::ProtocolError::InvalidValue("client id length below -1"),
            ));
        }
    };
    let response_flexible = request_is_flexible(api_key_n.get(), api_version.get(), admin_router);
    if response_flexible {
        krabka_protocol::tagged_fields::read_tagged_fields(&mut cur, |_tag, _payload| Ok(false))?;
    }

    Ok((
        api_key_n,
        api_version,
        correlation_id,
        client_id,
        Bytes::copy_from_slice(cur),
        response_flexible,
    ))
}

pub(super) async fn write_response<S>(
    stream: &mut S,
    correlation_id: CorrelationId,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_response_frame(stream, correlation_id, body, true).await
}

/// Write a response without the leading tagged-fields byte. Used only by the
/// `ApiVersions` v0 path, which decodes a `ResponseHeader v0`.
pub(super) async fn write_response_no_tagged_fields<S>(
    stream: &mut S,
    correlation_id: CorrelationId,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_response_frame(stream, correlation_id, body, false).await
}

pub(super) async fn write_response_frame<S>(
    stream: &mut S,
    correlation_id: CorrelationId,
    body: Bytes,
    include_tagged_fields: bool,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(4 + usize::from(include_tagged_fields) + body.len());
    frame.put_i32(correlation_id);
    if include_tagged_fields {
        frame.put_u8(0); // empty tagged_fields (ResponseHeader v1)
    }
    frame.put_slice(&body);

    let mut len_prefix = [0u8; 4];
    len_prefix.copy_from_slice(&i32::try_from(frame.len()).unwrap_or(i32::MAX).to_be_bytes());
    stream.write_all(&len_prefix).await.map_err(io_err)?;
    stream.write_all(&frame).await.map_err(io_err)?;
    stream.flush().await.map_err(io_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// A request is flexible from its API's own `FLEXIBLE_MIN` upward, and an
    /// API nobody declares is not flexible at any version.
    ///
    /// Each arm names its own constant, so one arm reaching for another API's
    /// minimum is invisible unless the versions either side of the boundary
    /// are asked for. Getting it wrong means reading tagged fields off a wire
    /// that has none, or skipping the ones that are there.
    #[test]
    fn a_request_is_flexible_from_its_own_minimum_upward() {
        use krabka_protocol::owned::{
            add_raft_voter_request, api_versions_request, describe_quorum_request, fetch_request,
            vote_request,
        };

        // (api key, that API's flexible minimum)
        let apis = [
            (
                api_versions_request::API_KEY,
                api_versions_request::FLEXIBLE_MIN,
            ),
            (fetch_request::API_KEY, fetch_request::FLEXIBLE_MIN),
            (vote_request::API_KEY, vote_request::FLEXIBLE_MIN),
            (
                describe_quorum_request::API_KEY,
                describe_quorum_request::FLEXIBLE_MIN,
            ),
            (
                add_raft_voter_request::API_KEY,
                add_raft_voter_request::FLEXIBLE_MIN,
            ),
        ];
        for (api_key, flexible_min) in apis {
            check!(
                request_is_flexible(api_key, flexible_min, None),
                "api {api_key} at its own minimum {flexible_min}"
            );
            check!(
                request_is_flexible(api_key, flexible_min + 1, None),
                "api {api_key} above its minimum"
            );
            if let Some(below) = flexible_min.checked_sub(1) {
                check!(
                    !request_is_flexible(api_key, below, None),
                    "api {api_key} below its minimum"
                );
            }
        }

        // An API this server does not route, with no admin extension to claim it.
        check!(!request_is_flexible(i16::MAX, 0, None));
    }

    fn length_prefixed(frame: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(frame.len() + 4);
        out.extend_from_slice(&(u32::try_from(frame.len()).unwrap()).to_be_bytes());
        out.extend_from_slice(frame);
        out
    }

    fn request_frame(
        api_key: ApiKey,
        api_version: ApiVersion,
        correlation_id: i32,
        client_id: &str,
        body: &[u8],
    ) -> Vec<u8> {
        let mut frame = bytes::BytesMut::new();
        frame.put_i16(api_key.get());
        frame.put_i16(api_version.get());
        frame.put_i32(correlation_id);
        frame.put_i16(i16::try_from(client_id.len()).unwrap());
        frame.put_slice(client_id.as_bytes());
        frame.put_u8(0);
        frame.put_slice(body);
        length_prefixed(&frame)
    }

    fn raw_request_frame(
        api_key: ApiKey,
        api_version: ApiVersion,
        correlation_id: i32,
        client_id_len: i16,
        client_id_bytes: &[u8],
        tagged_or_body: &[u8],
    ) -> Vec<u8> {
        let mut frame = bytes::BytesMut::new();
        frame.put_i16(api_key.get());
        frame.put_i16(api_version.get());
        frame.put_i32(correlation_id);
        frame.put_i16(client_id_len);
        frame.put_slice(client_id_bytes);
        frame.put_slice(tagged_or_body);
        length_prefixed(&frame)
    }

    #[test]
    fn is_eof_only_matches_unexpected_eof_io_errors() {
        let io_error = |kind| {
            super::RaftError::Storage(krabka_log::LogError::Io(std::io::Error::new(kind, "io")))
        };
        let cases = [
            (
                "unexpected EOF",
                io_error(std::io::ErrorKind::UnexpectedEof),
                true,
            ),
            (
                "broken pipe",
                io_error(std::io::ErrorKind::BrokenPipe),
                false,
            ),
            (
                "protocol error",
                super::RaftError::Protocol(krabka_protocol::ProtocolError::InvalidValue("not io")),
                false,
            ),
        ];
        for (_case, err, want) in cases {
            assert2::assert!(super::is_eof(&err) == want);
        }
    }

    #[tokio::test]
    async fn read_one_request_decodes_header_variants() {
        let cases = [
            (
                "flexible header with client id and body",
                request_frame(ApiKey(52), ApiVersion(2), 123, "raft-client", b"payload"),
                b"payload".as_slice(),
            ),
            (
                "null client id with no body",
                raw_request_frame(ApiKey(52), ApiVersion(2), 123, -1, &[], &[0]),
                b"".as_slice(),
            ),
        ];
        for (case, frame, want_body) in cases {
            let (mut client, mut server) = tokio::io::duplex(128);
            let writer = tokio::spawn(async move {
                client.write_all(&frame).await.unwrap();
            });

            let (api_key, api_version, correlation_id, client_id, body, flexible) =
                super::read_one_request(&mut server, None)
                    .await
                    .expect("decode");

            check!(
                (
                    api_key,
                    api_version,
                    correlation_id,
                    client_id.as_deref(),
                    body.as_ref(),
                    flexible,
                ) == (
                    ApiKey(52),
                    ApiVersion(2),
                    123,
                    if case.starts_with("null") {
                        None
                    } else {
                        Some("raft-client")
                    },
                    want_body,
                    true,
                ),
                "case: {case}"
            );
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn read_one_request_reports_header_shortfalls() {
        let partial_fixed = {
            let mut f = bytes::BytesMut::new();
            f.put_i16(52);
            f.put_i16(2);
            f.put_i32(123);
            f
        };
        let mut partial_client_id_len = partial_fixed.clone();
        partial_client_id_len.put_u8(0x80);
        let cases = [
            // Frame ends inside the 8-byte fixed header.
            ("short fixed header", length_prefixed(&[0, 52, 0, 2]), 4),
            // Fixed header complete, client-id length missing entirely.
            (
                "missing client id length",
                length_prefixed(&partial_fixed),
                2,
            ),
            // Only one byte of the 2-byte client-id length present.
            (
                "partial client id length",
                length_prefixed(&partial_client_id_len),
                1,
            ),
            // Client-id length declares 4 bytes; only 1 present.
            (
                "client id bytes shortfall",
                raw_request_frame(ApiKey(52), ApiVersion(2), 123, 4, b"x", &[]),
                3,
            ),
        ];
        for (_case, frame, needed) in cases {
            let (mut client, mut server) = tokio::io::duplex(128);
            let writer = tokio::spawn(async move {
                client.write_all(&frame).await.unwrap();
            });

            let err = super::read_one_request(&mut server, None)
                .await
                .expect_err("short frame");

            assert2::assert!(matches!(
                err,
                super::RaftError::Protocol(
                    krabka_protocol::ProtocolError::UnexpectedEof { needed: n }
                ) if n == needed
            ));
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn read_one_request_keeps_nonflexible_body_prefix() {
        let (mut client, mut server) = tokio::io::duplex(128);
        let frame = raw_request_frame(
            ApiKey(53),
            ApiVersion(0),
            123,
            0,
            &[],
            &[1, b'p', b'a', b'y'],
        );
        let writer = tokio::spawn(async move {
            client.write_all(&frame).await.unwrap();
        });

        let (_, _, _, _, body, flexible) = super::read_one_request(&mut server, None)
            .await
            .expect("decode");

        assert2::assert!(body.as_ref() == &[1, b'p', b'a', b'y']);
        assert2::assert!(!flexible);
        writer.await.unwrap();
    }
}
