//! Raw-socket Kafka request framing and the SASL/PLAIN handshake for broker
//! integration tests that drive the wire by hand.
//!
//! [`round_trip_with`] writes one length-prefixed request with a v1 or v2
//! request header and reads back one response with its header stripped.
//! [`round_trip`] derives the response header from the request flexibility the
//! way Kafka does. [`sasl_plain_authenticate`] and
//! [`sasl_plain_authenticate_on`] run `ApiVersions`, `SaslHandshake` and
//! `SaslAuthenticate` over the same framing.
//!
//! The client id is always a parameter, because the client-quota suites key
//! their quotas on it.
//!
//! The module sits in `tests/kafka_wire/mod.rs` rather than in `support`, so a
//! suite that frames requests by hand can declare `mod kafka_wire;` without
//! compiling every cluster-boot helper. Cargo does not build a `mod.rs` under
//! `tests/` as its own test binary, and `crate_tests` in `//bazel:defs.bzl`
//! treats it as a helper source of every integration test.

// Each suite declares `mod kafka_wire;` and calls part of it, which is the
// same reason `support` carries this allow.
#![allow(dead_code)]

use std::{io, net::SocketAddr};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

/// The `ApiVersions` API key, whose response header is v0 at every version.
const API_VERSIONS_KEY: i16 = 18;

/// Sends one request and returns the response body.
///
/// The response header is flexible exactly when the request header is, except
/// for `ApiVersions`, whose response header stays v0 so that a client can read
/// it before it knows which versions the broker speaks.
///
/// # Errors
///
/// Returns the I/O error of a failed write or read, and an error when a
/// flexible response header is malformed.
pub async fn round_trip<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    client_id: &str,
    flexible: bool,
    body: &[u8],
) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let flexibility = Flexibility {
        request: flexible,
        response: flexible && api_key != API_VERSIONS_KEY,
    };
    round_trip_with(
        stream,
        api_key,
        api_version,
        corr_id,
        client_id,
        flexibility,
        body,
    )
    .await
}

/// Whether the request header and the response header of one exchange are
/// flexible (v2 request, v1 response) and so carry a tagged-fields byte.
#[derive(Clone, Copy, Debug)]
pub struct Flexibility {
    /// The request header is v2.
    pub request: bool,
    /// The response header is v1.
    pub response: bool,
}

/// Sends one request with the request and response header flexibility chosen
/// independently, and returns the response body with its header stripped.
///
/// A flexible request header ends in an empty tagged-fields byte. A flexible
/// response header must carry that byte as zero.
///
/// # Errors
///
/// Returns the I/O error of a failed write or read, and an error when a
/// flexible response header is missing its tagged-fields byte or carries tagged
/// fields.
pub async fn round_trip_with<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    client_id: &str,
    flexibility: Flexibility,
    body: &[u8],
) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(16 + client_id.len() + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits in i16"));
    frame.put_slice(client_id.as_bytes());
    if flexibility.request {
        frame.put_u8(0); // empty header tagged-fields byte
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    if cur.len() < 4 {
        return Err(io::Error::other("response missing correlation id"));
    }
    let _resp_corr_id = cur.get_i32();
    if flexibility.response {
        if cur.is_empty() {
            return Err(io::Error::other(
                "flexible response missing tagged-fields byte",
            ));
        }
        let tagged = cur.get_u8();
        if tagged != 0 {
            return Err(io::Error::other(format!(
                "response header tagged-fields byte is {tagged}, expected 0"
            )));
        }
    }
    Ok(cur.to_vec())
}

/// Connects to `addr` and authenticates the connection with SASL/PLAIN.
///
/// # Errors
///
/// Returns the error of a failed connect, of a failed round trip, or of a
/// non-zero `SaslHandshake` or `SaslAuthenticate` error code.
pub async fn sasl_plain_authenticate(
    addr: SocketAddr,
    client_id: &str,
    user: &str,
    password: &[u8],
) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    sasl_plain_authenticate_on(&mut stream, client_id, user, password).await?;
    Ok(stream)
}

/// Authenticates an open connection with SASL/PLAIN: `ApiVersions` v0, then
/// `SaslHandshake` v1 for `PLAIN`, then `SaslAuthenticate` v2 carrying
/// `\0user\0password`, on correlation ids 1, 2 and 3.
///
/// # Errors
///
/// Returns the error of a failed round trip, encode or decode, or of a
/// non-zero `SaslHandshake` or `SaslAuthenticate` error code.
pub async fn sasl_plain_authenticate_on<S>(
    stream: &mut S,
    client_id: &str,
    user: &str,
    password: &[u8],
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut av_body = BytesMut::new();
    ApiVersionsRequest::default()
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp = round_trip(stream, API_VERSIONS_KEY, 0, 1, client_id, false, &av_body).await?;
    ApiVersionsResponse::decode(&mut &av_resp[..], 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp = round_trip(stream, 17, 1, 2, client_id, false, &sh_body).await?;
    let sh_resp = SaslHandshakeResponse::decode(&mut &sh_resp[..], 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0); // empty authzid
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password);
    let mut auth_body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: Bytes::from(payload),
        ..Default::default()
    }
    .encode(&mut auth_body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let auth_resp = round_trip(stream, 36, 2, 3, client_id, true, &auth_body).await?;
    let auth_resp = SaslAuthenticateResponse::decode(&mut &auth_resp[..], 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={} message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }
    Ok(())
}
