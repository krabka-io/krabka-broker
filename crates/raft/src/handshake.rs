//! Pluggable inbound handshake for the controller listener.
//!
//! This hook lets the broker terminate TLS and SASL on every accepted
//! controller-listener connection before the raft frames start to flow. The
//! trait abstraction keeps `krabka-raft` free of any dependency on
//! `krabka-broker` and `krabka-security`.

use bytes::Bytes;
use krabka_client_core::ClientDuplex;
use thiserror::Error;
use tokio::net::TcpStream;

/// Authenticated controller-listener connection plus request-level grants.
pub struct RaftConnection {
    /// The raft connection handler is generic over `AsyncRead + AsyncWrite +
    /// Unpin + Send + 'static`, so a `Box<dyn ClientDuplex>` plugs in directly.
    pub stream: Box<dyn ClientDuplex>,
    /// Authenticated Kafka principal. `None` represents a PLAINTEXT or
    /// one-way TLS connection with the normal `ANONYMOUS` identity.
    pub principal: Option<krabka_security::Principal>,
    /// Whether SCRAM authenticated with a delegation token rather than a
    /// regular credential.
    pub authenticated_via_token: bool,
    /// Whether the principal may alter cluster membership.
    pub cluster_alter_authorized: bool,
}

#[derive(Debug, Error)]
pub enum RaftHandshakeError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
    #[error("sasl: {0}")]
    Sasl(String),
    #[error("protocol: {0}")]
    Protocol(String),
}

/// Per-connection handshake hook.
///
/// Implementors consume the raw `TcpStream` and return one of two things. On
/// success they return an authenticated `Box<dyn ClientDuplex>` that carries
/// the raft frames. On failure they return a `RaftHandshakeError`, and the
/// listener then drops the connection at debug level.
#[async_trait::async_trait]
pub trait RaftListenerHandshake: Send + Sync {
    /// Upgrades one accepted connection.
    ///
    /// A SASL listener answers the `ApiVersions` requests that arrive before
    /// authentication with `api_versions`, which gives the same answer as the
    /// listener gives after authentication. Kafka's `SocketServer` hands the
    /// same `apiVersionSupplier` to `SaslServerAuthenticator`.
    async fn upgrade(
        &self,
        stream: TcpStream,
        api_versions: &dyn ControllerApiVersions,
    ) -> Result<RaftConnection, RaftHandshakeError>;
}

/// The controller listener's `ApiVersions` answer, for a handshake that
/// answers `ApiVersions` before the listener gets the connection.
pub trait ControllerApiVersions: Send + Sync {
    /// Answers one `ApiVersions` request with the response body. The body goes
    /// out behind a v0 response header, at the version the body was encoded
    /// at: the request version, or v0 for a version the listener does not
    /// serve.
    ///
    /// # Errors
    /// Returns [`RaftHandshakeError::Protocol`] when the body of a served
    /// version does not decode. Kafka closes such a connection.
    fn respond(
        &self,
        request_version: i16,
        request_body: &[u8],
    ) -> Result<Bytes, RaftHandshakeError>;
}
