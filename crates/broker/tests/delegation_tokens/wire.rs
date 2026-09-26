//! Raw-socket plumbing for the KIP-48 suite: the length-prefixed
//! request/response framing and the two SASL handshake drivers that every
//! delegation-token step runs over.
//!
//! The framing has the same shape as `auth_handlers/harness.rs`, and the
//! drivers have the same wire shape as the ones in `auth_handlers/plain.rs`
//! and `auth_handlers/scram.rs`. They differ in one way that matters here:
//! each returns the still-open `TcpStream`, so a caller can send admin RPCs
//! on the session it just authenticated.

use std::{io, net::SocketAddr};

use bytes::{Buf, BufMut, BytesMut};
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
use krabka_security::SaslMechanism;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

// ─────────────────────────────────────────────────────────────────────────────
// Wire framing (length-prefixed request/response). Same shape as
// `auth_handlers/harness.rs::round_trip` and `describe_user_scram_credentials.rs`.
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) async fn round_trip<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    let client_id = "krabka-deltok-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
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
    let _resp_corr_id = cur.get_i32();
    // Flexible (v2+) responses carry a 1-byte header tagged-fields prefix,
    // except ApiVersions(18) which is special-cased by the spec.
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

// ─────────────────────────────────────────────────────────────────────────────
// SASL handshake drivers. Both walk ApiVersions → SaslHandshake →
// SaslAuthenticate on a fresh TcpStream and return the still-open stream
// for follow-up requests.
// ─────────────────────────────────────────────────────────────────────────────

/// PLAIN happy-path driver. It mirrors
/// `auth_handlers/plain.rs::drive_sasl_plain_session` but stops at the post-auth
/// Metadata round trip, because callers want the open connection so that they
/// can send admin RPCs.
pub(crate) async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake(PLAIN) failed: error_code={}",
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
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    }
    .encode(&mut auth_body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let auth_resp_bytes = round_trip(&mut stream, 36, 2, 3, true, &auth_body).await?;
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate(PLAIN) failed: error_code={} message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }

    Ok(stream)
}

/// SCRAM-SHA-256 delegation-token driver. It has the same wire shape as
/// `auth_handlers/scram.rs::drive_sasl_scram_session`, but it returns the open
/// connection on success, which step (c) needs, and its client-first message
/// carries the `tokenauth=true` extension that Kafka's `ScramLoginModule`
/// sends for a token, so `username` is a token id and `password` its HMAC.
pub(crate) async fn sasl_scram_sha256_authenticate(
    addr: SocketAddr,
    username: &str,
    password: &str,
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-256".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake(SCRAM-SHA-256) failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let client = TokenScramClient::new(username, password);
    let client_first = client.client_first();

    let mut body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_first),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) encode: {e}")))?;
    let r1 = round_trip(&mut stream, 36, 2, 3, true, &body).await?;
    let mut cur: &[u8] = &r1;
    let r1_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) decode: {e}")))?;
    if r1_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SCRAM round 1 failed: code={} msg={:?}",
            r1_resp.error_code, r1_resp.error_message
        )));
    }

    let client_final = client.client_final(&r1_resp.auth_bytes)?;
    let mut body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_final),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) encode: {e}")))?;
    let r2 = round_trip(&mut stream, 36, 2, 4, true, &body).await?;
    let mut cur: &[u8] = &r2;
    let r2_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) decode: {e}")))?;
    if r2_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SCRAM round 2 failed: code={} msg={:?}",
            r2_resp.error_code, r2_resp.error_message
        )));
    }

    Ok(stream)
}

/// A SCRAM-SHA-256 client that writes the messages Kafka's `ScramSaslClient`
/// writes for a delegation token: `krabka_security::ScramClientExchange` has no
/// way to add the `tokenauth=true` extension, which is part of the signed
/// client-first message.
struct TokenScramClient {
    password: Vec<u8>,
    client_first_bare: String,
}

impl TokenScramClient {
    fn new(token_id: &str, token_hmac: &str) -> Self {
        Self {
            password: token_hmac.as_bytes().to_vec(),
            client_first_bare: format!("n={token_id},r=tokenclientnonce,tokenauth=true"),
        }
    }

    fn client_first(&self) -> Vec<u8> {
        format!("n,,{}", self.client_first_bare).into_bytes()
    }

    fn client_final(&self, server_first: &[u8]) -> Result<Vec<u8>, io::Error> {
        use base64::{Engine, engine::general_purpose::STANDARD as B64};
        use pbkdf2::{
            hmac::{Hmac, KeyInit, Mac},
            sha2::{Digest, Sha256},
        };

        let hmac = |key: &[u8], data: &[u8]| {
            let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("HMAC takes any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        };
        let server_first = std::str::from_utf8(server_first)
            .map_err(|e| io::Error::other(format!("server-first is not UTF-8: {e}")))?;
        let attribute = |name: &str| {
            server_first
                .split(',')
                .find_map(|a| a.strip_prefix(name))
                .ok_or_else(|| io::Error::other(format!("server-first lacks {name}")))
        };
        let nonce = attribute("r=")?;
        let salt = B64
            .decode(attribute("s=")?)
            .map_err(|e| io::Error::other(format!("server-first salt: {e}")))?;
        let iterations: u32 = attribute("i=")?
            .parse()
            .map_err(|e| io::Error::other(format!("server-first iterations: {e}")))?;
        let salted = krabka_security::scram::pbkdf2_salted(
            &self.password,
            SaslMechanism::ScramSha256,
            iterations,
            &salt,
        );
        let without_proof = format!("c={},r={nonce}", B64.encode(b"n,,"));
        let auth_message = format!("{},{server_first},{without_proof}", self.client_first_bare);
        let client_key = hmac(&salted, b"Client Key");
        let signature = hmac(&Sha256::digest(&client_key), auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(&signature)
            .map(|(k, s)| k ^ s)
            .collect();
        Ok(format!("{without_proof},p={}", B64.encode(proof)).into_bytes())
    }
}
