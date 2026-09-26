//! A SCRAM client (RFC 5802 §3) that can send Kafka's `tokenauth=true`
//! extension, which `krabka_security::ScramClientExchange` does not write.
//!
//! Kafka's `ScramSaslClient` appends the extension to the client-first
//! message when the JAAS login carries `tokenauth=true`, and the server
//! (`ScramSaslServer`) then looks the username up in the delegation-token
//! cache instead of the SCRAM credential store. Without the extension a
//! token id is an ordinary username, so a token login must send it.
//!
//! The same client shape backs the SCRAM handler's unit tests in
//! `src/network/auth/scram/tests.rs`. It is a second copy because a library
//! unit test and an integration test compile from disjoint source sets under
//! Bazel (`src/**` and `tests/**`), and ring is a dev-dependency the library
//! cannot export a helper over.

use std::num::NonZeroU32;

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use krabka_security::SaslMechanism;
use ring::{digest, hmac, pbkdf2};

/// The fixed client nonce. The server appends its own random half, so the
/// combined nonce is still fresh per exchange.
const CLIENT_NONCE: &str = "krabkadeltoktestnonce";

/// Client state after the client-first message has been built.
pub(crate) struct ScramClient {
    mechanism: SaslMechanism,
    password: Vec<u8>,
    client_first_bare: String,
}

/// Client state after the client-final message has been built: what the
/// server-final signature is checked against.
pub(crate) struct AwaitingServerFinal {
    hmac_alg: hmac::Algorithm,
    server_key: Vec<u8>,
    auth_message: String,
}

impl ScramClient {
    /// Builds the client-first message. With `token_auth`, the message
    /// carries `,tokenauth=true` after the nonce, the way Kafka's
    /// `ScramExtensions` serializes it.
    pub(crate) fn first(
        mechanism: SaslMechanism,
        username: &str,
        password: &[u8],
        token_auth: bool,
    ) -> (Self, Vec<u8>) {
        let extension = if token_auth { ",tokenauth=true" } else { "" };
        let client_first_bare = format!("n={username},r={CLIENT_NONCE}{extension}");
        let message = format!("n,,{client_first_bare}").into_bytes();
        let client = Self {
            mechanism,
            password: password.to_vec(),
            client_first_bare,
        };
        (client, message)
    }

    /// Answers the server-first message with the client-final message.
    ///
    /// # Errors
    /// Returns a description of the server-first message's defect.
    pub(crate) fn last(
        self,
        server_first: &[u8],
    ) -> Result<(Vec<u8>, AwaitingServerFinal), String> {
        let server_first =
            std::str::from_utf8(server_first).map_err(|e| format!("server-first utf-8: {e}"))?;
        let attr = |prefix: &str| {
            server_first
                .split(',')
                .find_map(|a| a.strip_prefix(prefix))
                .ok_or_else(|| format!("server-first lacks {prefix}: {server_first}"))
        };
        let nonce = attr("r=")?;
        if !nonce.starts_with(CLIENT_NONCE) {
            return Err(format!("server nonce does not extend ours: {nonce}"));
        }
        let salt = B64
            .decode(attr("s=")?)
            .map_err(|e| format!("server-first salt: {e}"))?;
        let iterations: NonZeroU32 = attr("i=")?
            .parse()
            .map_err(|e| format!("server-first iterations: {e}"))?;
        let (pbkdf2_alg, hmac_alg, digest_alg, len) = match self.mechanism {
            SaslMechanism::ScramSha256 => (
                pbkdf2::PBKDF2_HMAC_SHA256,
                hmac::HMAC_SHA256,
                &digest::SHA256,
                32,
            ),
            SaslMechanism::ScramSha512 => (
                pbkdf2::PBKDF2_HMAC_SHA512,
                hmac::HMAC_SHA512,
                &digest::SHA512,
                64,
            ),
            other => return Err(format!("not a SCRAM mechanism: {other:?}")),
        };
        let mut salted = vec![0; len];
        pbkdf2::derive(pbkdf2_alg, iterations, &salt, &self.password, &mut salted);
        let salted_key = hmac::Key::new(hmac_alg, &salted);
        let client_key = hmac::sign(&salted_key, b"Client Key");
        let server_key = hmac::sign(&salted_key, b"Server Key");
        let stored_key = digest::digest(digest_alg, client_key.as_ref());
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{},{server_first},{without_proof}", self.client_first_bare);
        let signature = hmac::sign(
            &hmac::Key::new(hmac_alg, stored_key.as_ref()),
            auth_message.as_bytes(),
        );
        let proof: Vec<u8> = client_key
            .as_ref()
            .iter()
            .zip(signature.as_ref())
            .map(|(k, s)| k ^ s)
            .collect();
        let message = format!("{without_proof},p={}", B64.encode(proof)).into_bytes();
        let next = AwaitingServerFinal {
            hmac_alg,
            server_key: server_key.as_ref().to_vec(),
            auth_message,
        };
        Ok((message, next))
    }
}

impl AwaitingServerFinal {
    /// Checks the server-final signature, which proves the server holds the
    /// same credential the client derived.
    ///
    /// # Errors
    /// Returns a description of the mismatch or of the malformed message.
    pub(crate) fn verify(self, server_final: &[u8]) -> Result<(), String> {
        let server_final =
            std::str::from_utf8(server_final).map_err(|e| format!("server-final utf-8: {e}"))?;
        let signature = server_final
            .strip_prefix("v=")
            .ok_or_else(|| format!("server-final lacks v=: {server_final}"))?;
        let signature = B64
            .decode(signature)
            .map_err(|e| format!("server-final signature: {e}"))?;
        let expected = hmac::sign(
            &hmac::Key::new(self.hmac_alg, &self.server_key),
            self.auth_message.as_bytes(),
        );
        if expected.as_ref() == signature.as_slice() {
            Ok(())
        } else {
            Err("server-final signature mismatch".to_string())
        }
    }
}
