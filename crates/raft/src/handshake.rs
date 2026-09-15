//! Pluggable inbound handshake for the controller listener.
//!
//! This hook lets the broker terminate TLS and SASL on every accepted
//! controller-listener connection before the raft frames start to flow. The
//! trait abstraction keeps `krabka-raft` free of any dependency on
//! `krabka-broker` and `krabka-security`.

use std::sync::Arc;

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
    /// The cluster grants of the connection principal. The listener asks it
    /// once for each request, as Kafka's `ControllerApis` does.
    pub grants: Arc<dyn ClusterGrants>,
}

/// An operation on the `Cluster("kafka-cluster")` resource that a
/// controller-listener api needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterOperation {
    /// `CLUSTER_ACTION`: the raft, registration and krabka-private metadata
    /// apis.
    ClusterAction,
    /// `ALTER`: `AddRaftVoter`, `RemoveRaftVoter` and `DescribeCluster`.
    Alter,
    /// `DESCRIBE`: `DescribeQuorum`.
    Describe,
}

/// Decides whether the principal of one controller-listener connection holds
/// an operation on the cluster resource.
///
/// The listener calls [`ClusterGrants::allows`] for every request, so an ACL
/// change applies to the next request of an open connection.
pub trait ClusterGrants: Send + Sync {
    fn allows(&self, operation: ClusterOperation) -> bool;
}

/// The grants of a listener that installs no handshake: every operation is
/// allowed. The broker always installs a handshake, so only a controller
/// without a broker, such as a raft-only test, uses it.
#[derive(Debug)]
pub struct AllowAllGrants;

impl ClusterGrants for AllowAllGrants {
    fn allows(&self, _operation: ClusterOperation) -> bool {
        true
    }
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
    async fn upgrade(&self, stream: TcpStream) -> Result<RaftConnection, RaftHandshakeError>;
}
