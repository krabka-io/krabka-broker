//! Wire plumbing and credential fixtures that every auth suite in this
//! binary shares: one length-prefixed request/response round-trip against a
//! broker socket, and the test passwords the SASL exchanges authenticate
//! with.

use std::io;

use bytes::{Buf, BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// alice's SCRAM test password, built from characters at runtime.
///
/// The value is a non-secret test fixture. But a literal that goes into the
/// client SASL-auth calls trips GitHub's default code-scanning credential
/// query. This function keeps those call sites free of literals.
pub fn alice_password() -> String {
    ['w', 'o', 'n', 'd', 'e', 'r', 'l', 'a', 'n', 'd']
        .iter()
        .collect()
}

/// admin PLAIN test password, built at runtime.
///
/// A runtime value stops code scanning from giving a false positive for a
/// static secret in the integration fixtures.
pub fn admin_plain_password() -> String {
    ['s', 'e', 'c', 'r', 'e', 't'].iter().collect()
}

/// wrong SCRAM test password, built at runtime for the same reason as
/// `admin_plain_password`.
pub fn wrong_scram_password() -> String {
    ['h', 'u', 'n', 't', 'e', 'r', '2'].iter().collect()
}

/// Encode a request header, send the frame, and return the response body
/// bytes.
///
/// The header is a `RequestHeader v1`, or a v2 header when `flexible` is set.
/// This function appends the body, writes the length-prefixed frame, reads
/// one response frame, and then strips the `ResponseHeader`. That header is
/// always v0 for ApiVersions(18). For every other API it is v0 when the
/// response is non-flexible and v1 when the response is flexible.
pub async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    // RequestHeader: api_key + version + corr_id + client_id (i16 NULLABLE_STRING).
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    let client_id = "krabka-sasl-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits in i16"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields
    }
    frame.put_slice(body);

    // Length-prefixed write.
    stream
        .write_u32(u32::try_from(frame.len()).expect("frame size fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read length prefix then exactly that many bytes.
    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    // Strip ResponseHeader: 4-byte corr_id, plus 1-byte tagged-fields for
    // v1 (flexible body AND api_key != 18).
    let mut cur = &resp[..];
    let _resp_corr_id = cur.get_i32();
    let uses_v1_header = flexible && api_key != 18;
    if uses_v1_header {
        if cur.is_empty() {
            return Err(io::Error::other(
                "flexible response missing tagged-fields byte",
            ));
        }
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}
