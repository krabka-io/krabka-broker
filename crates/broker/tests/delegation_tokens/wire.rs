//! Raw-socket plumbing for the KIP-48 suite: the length-prefixed
//! request/response framing and the two SASL handshake drivers that every
//! delegation-token step runs over.
//!
//! The framing and the PLAIN driver are the shared [`kafka_wire`] helpers, and
//! the SCRAM driver has the same wire shape as the one in
//! `auth_handlers/scram.rs`. Both drivers differ from `auth_handlers` in one way
//! that matters here: each returns the still-open `TcpStream`, so a caller can
//! send admin RPCs on the session it just authenticated.

use std::{io, net::SocketAddr};

use base64::Engine;
use krabka_protocol::owned::{
    api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
    sasl_handshake_request::SaslHandshakeRequest, sasl_handshake_response::SaslHandshakeResponse,
};
use krabka_security::SaslMechanism;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

use crate::kafka_wire;

// ─────────────────────────────────────────────────────────────────────────────
// Wire framing (length-prefixed request/response), bound to this suite's
// client id.
// ─────────────────────────────────────────────────────────────────────────────

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-deltok-test";

/// One length-prefixed request/response exchange; see
/// [`kafka_wire::round_trip`].
pub(crate) async fn round_trip<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> io::Result<Vec<u8>> {
    kafka_wire::round_trip(
        stream,
        api_key,
        api_version,
        corr_id,
        CLIENT_ID,
        flexible,
        body,
    )
    .await
}

// ─────────────────────────────────────────────────────────────────────────────
// SASL handshake drivers. Both walk ApiVersions → SaslHandshake →
// SaslAuthenticate on a fresh TcpStream and return the still-open stream
// for follow-up requests.
// ─────────────────────────────────────────────────────────────────────────────

/// Connects and authenticates with SASL/PLAIN; see
/// [`kafka_wire::sasl_plain_authenticate`].
pub(crate) async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> io::Result<TcpStream> {
    kafka_wire::sasl_plain_authenticate(addr, CLIENT_ID, user, password).await
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

    let _av_resp: ApiVersionsResponse = crate::kafka_wire::exchange(
        &mut stream,
        &ApiVersionsRequest::default(),
        18,
        0,
        1,
        CLIENT_ID,
        false,
    )
    .await?;

    let sh_resp: SaslHandshakeResponse = crate::kafka_wire::exchange(
        &mut stream,
        &SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-256".to_string(),
            ..Default::default()
        },
        17,
        1,
        2,
        CLIENT_ID,
        false,
    )
    .await?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake(SCRAM-SHA-256) failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let client = TokenScramClient::new(username, password);
    let client_first = client.client_first();

    let r1_resp = kafka_wire::sasl_authenticate_on(
        &mut stream,
        CLIENT_ID,
        3,
        bytes::Bytes::from(client_first),
    )
    .await?;
    if r1_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SCRAM round 1 failed: code={} msg={:?}",
            r1_resp.error_code, r1_resp.error_message
        )));
    }

    let client_final = client.client_final(&r1_resp.auth_bytes)?;
    let r2_resp = kafka_wire::sasl_authenticate_on(
        &mut stream,
        CLIENT_ID,
        4,
        bytes::Bytes::from(client_final),
    )
    .await?;
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

pub(crate) async fn token_session(
    addr: SocketAddr,
    token_id: &str,
    hmac: &[u8],
) -> Result<TcpStream, String> {
    let password = base64::engine::general_purpose::STANDARD.encode(hmac);
    let mut session = sasl_scram_sha256_authenticate(addr, token_id, &password)
        .await
        .map_err(|e| format!("token SCRAM auth: {e}"))?;
    let create = crate::rpc::send_create_delegation_token(
        &mut session,
        200,
        &krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        },
    )
    .await
    .map_err(|e| format!("CreateDelegationToken(token-auth): {e}"))?;
    assert2::assert!(
        create.error_code == crate::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        "token-authed Create must return DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64); got {} — principal override may have regressed",
        create.error_code
    );
    Ok(session)
}
