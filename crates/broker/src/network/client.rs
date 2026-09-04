//! Outbound inter-broker client. It establishes TCP, and it optionally wraps
//! the connection in TLS and runs the SASL client handshake. It returns a
//! generic `AsyncRead + `AsyncWrite` stream that the caller uses for normal
//! RPCs.
//!
//! The replicator's Fetch path, the raft transport's outbound dial, and the
//! controller-heartbeat loop all use this client.

use std::sync::Arc;

use krabka_client_core::ClientDuplex;
use krabka_security::ListenerProtocol;
use krabka_units::{ByteSize, convert::ByteSizeExt as _, mebibytes};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::config::InterBrokerCredentials;

/// Socket buffer applied to an outbound inter-broker connection when the
/// caller does not supply the broker's configured value. It matches the
/// `socket_send_buffer` / `socket_receive_buffer` defaults the accept path
/// uses, so an untuned dial behaves like an untuned accept.
const DEFAULT_SOCKET_BUFFER: ByteSize = mebibytes(1);

/// Tune an outbound inter-broker socket before TLS and SASL run on it.
///
/// - `TCP_NODELAY`: disable Nagle. Every RPC the broker originates —
///   replica Fetch, the `KRaft` quorum exchanges, envelope forwarding to the
///   controller, and the lag poller's high-watermark probes — is a small
///   request that would otherwise wait for the peer's delayed ACK, adding up
///   to ~40 ms to a replication round trip. Apache Kafka disables Nagle on
///   every channel its `Selector` opens, connect and accept alike.
/// - `SO_SNDBUF`/`SO_RCVBUF`: the same configured buffers the accept path
///   applies, so a replication stream has in-flight headroom in both
///   directions.
///
/// All failures are non-fatal and logged at debug level, exactly as on the
/// accept side: an untuned connection still works, just less efficiently.
fn tune_outbound_socket(stream: &TcpStream, send_buffer: ByteSize, receive_buffer: ByteSize) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::debug!(error = %e, "TCP_NODELAY set failed on outbound socket");
    }
    let sock = socket2::SockRef::from(stream);
    if let Err(e) = sock.set_send_buffer_size(send_buffer.bytes_usize()) {
        tracing::debug!(error = %e, "SO_SNDBUF set failed on outbound socket");
    }
    if let Err(e) = sock.set_recv_buffer_size(receive_buffer.bytes_usize()) {
        tracing::debug!(error = %e, "SO_RCVBUF set failed on outbound socket");
    }
}

/// Map the broker's [`InterBrokerCredentials`] onto the client-core
/// [`krabka_client_core::SaslCredentials`] understood by the shared
/// [`krabka_client_core::outbound_sasl`] handshake. The two enums carry
/// the same variants, so this is a field-for-field copy. The RLMM bootstrap
/// shares it, so the dialer and the metadata client agree on the
/// mapping.
pub(crate) fn to_client_creds(c: &InterBrokerCredentials) -> krabka_client_core::SaslCredentials {
    match c {
        InterBrokerCredentials::Plain { username, password } => {
            krabka_client_core::SaslCredentials::Plain {
                username: username.clone(),
                password: password.clone(),
            }
        }
        InterBrokerCredentials::Scram {
            mechanism,
            username,
            password,
        } => krabka_client_core::SaslCredentials::Scram {
            mechanism: *mechanism,
            username: username.clone(),
            password: password.clone(),
        },
        InterBrokerCredentials::Gssapi {
            keytab_path,
            client_principal,
            service_name,
            kdc_url,
        } => krabka_client_core::SaslCredentials::Gssapi {
            keytab_path: keytab_path.clone(),
            client_principal: client_principal.clone(),
            service_name: service_name.clone(),
            kdc_url: kdc_url.clone(),
        },
        InterBrokerCredentials::OAuthBearer { token_path } => {
            krabka_client_core::SaslCredentials::OAuthBearer {
                token_path: token_path.clone(),
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum InterBrokerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
    #[error("sasl: {0}")]
    Sasl(String),
    #[error("config: {0}")]
    Config(String),
    #[error("codec: {0}")]
    Codec(String),
}

/// Constructs outbound connections to other brokers, and runs TLS and SASL
/// as the listener protocol demands. It is cheap to clone and share, because
/// it holds only a `TlsConnector`, which is an `Arc` internally, and
/// credentials.
pub struct InterBrokerClient {
    tls_connector: Option<TlsConnector>,
    creds: Option<InterBrokerCredentials>,
    dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    frame_max: krabka_client_core::ClientFrameMax,
    socket_send_buffer: ByteSize,
    socket_receive_buffer: ByteSize,
}

impl InterBrokerClient {
    fn apply_resource_policy(&self, options: &mut krabka_client_core::ConnectionOptions) {
        options.dispatch_queue_capacity = self.dispatch_queue_capacity;
        options.frame_max = self.frame_max;
    }

    #[must_use]
    pub fn new(tls_connector: Option<TlsConnector>, creds: Option<InterBrokerCredentials>) -> Self {
        Self::new_with_policy(
            tls_connector,
            creds,
            krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            krabka_client_core::ClientFrameMax::default(),
            DEFAULT_SOCKET_BUFFER,
            DEFAULT_SOCKET_BUFFER,
        )
    }

    /// Construct with the broker process's outbound client resource policy.
    /// `socket_send_buffer` and `socket_receive_buffer` are the same
    /// configured sizes the accept path applies to sockets it accepts.
    #[must_use]
    pub fn new_with_policy(
        tls_connector: Option<TlsConnector>,
        creds: Option<InterBrokerCredentials>,
        dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
        frame_max: krabka_client_core::ClientFrameMax,
        socket_send_buffer: ByteSize,
        socket_receive_buffer: ByteSize,
    ) -> Self {
        Self {
            tls_connector,
            creds,
            dispatch_queue_capacity,
            frame_max,
            socket_send_buffer,
            socket_receive_buffer,
        }
    }

    /// Open the TCP connection every outbound inter-broker RPC rides, tuned
    /// with this client's socket policy before any TLS or SASL bytes flow.
    /// Tuning has to happen here: the handshakes are themselves small
    /// round-trip-bound exchanges that Nagle would stall, and once rustls owns
    /// the stream the raw socket is no longer reachable.
    async fn dial_tuned(&self, host: &str, port: u16) -> Result<TcpStream, std::io::Error> {
        let tcp = TcpStream::connect((host, port)).await?;
        tune_outbound_socket(&tcp, self.socket_send_buffer, self.socket_receive_buffer);
        Ok(tcp)
    }

    /// Dial `host:port`, do the protocol-appropriate TLS and SASL
    /// handshakes, and return an authenticated duplex stream. Callers
    /// drive normal Kafka RPCs, such as Fetch, Vote, and `AppendEntries`,
    /// through the returned stream as if it were a fresh `TcpStream`.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn connect(
        &self,
        host: &str,
        port: u16,
        listener_protocol: ListenerProtocol,
        server_name: &str,
        options: &krabka_client_core::ConnectionOptions,
    ) -> Result<Box<dyn ClientDuplex>, InterBrokerError> {
        let tcp = self.dial_tuned(host, port).await?;
        let mut stream: Box<dyn ClientDuplex> = if listener_protocol.requires_tls() {
            let connector = self.tls_connector.clone().ok_or_else(|| {
                InterBrokerError::Config("TLS listener without TlsConnector".into())
            })?;
            let sni =
                tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
                    .map_err(|e| InterBrokerError::Tls(format!("invalid server name: {e}")))?;
            let tls = connector
                .connect(sni, tcp)
                .await
                .map_err(|e| InterBrokerError::Tls(e.to_string()))?;
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        if listener_protocol.requires_sasl() {
            let creds = self.creds.clone().ok_or_else(|| {
                InterBrokerError::Config("SASL listener without inter_broker_credentials".into())
            })?;
            krabka_client_core::outbound_sasl(
                &mut *stream,
                &to_client_creds(&creds),
                server_name,
                &options.client_id,
                options.frame_max,
            )
            .await
            .map_err(|e| InterBrokerError::Sasl(e.to_string()))?;
        }
        Ok(stream)
    }

    /// Dial `host:port`, run TLS and SASL as needed, and return a
    /// [`krabka_client_core::Connection`] over the resulting stream. The
    /// connection is fully usable for normal typed Kafka requests, such as
    /// `Fetch`, `OffsetForLeaderEpoch`, `BrokerHeartbeat`, and raft RPCs
    /// through `raw_request`.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn connect_as_connection(
        &self,
        host: &str,
        port: u16,
        listener_protocol: ListenerProtocol,
        server_name: &str,
        mut options: krabka_client_core::ConnectionOptions,
    ) -> Result<krabka_client_core::Connection, InterBrokerError> {
        self.apply_resource_policy(&mut options);
        let stream = self
            .connect(host, port, listener_protocol, server_name, &options)
            .await?;
        krabka_client_core::Connection::from_stream(stream, options)
            .await
            .map_err(|e| InterBrokerError::Config(format!("Connection::from_stream: {e}")))
    }
}

// ────────────────────────────────────────────────────────────────────────
// OutboundDialer adapter for krabka_raft::KrabkaRaftNetworkFactory.
// ────────────────────────────────────────────────────────────────────────

/// Adapter that lets `krabka_raft` reach the broker's
/// [`InterBrokerClient`] without taking a build dependency on the
/// broker crate. It wraps an `Arc<InterBrokerClient>` and the protocol and
/// SNI configuration once, and the raft network factory clones it cheaply.
pub struct InterBrokerDialer {
    client: Arc<InterBrokerClient>,
    listener_protocol: ListenerProtocol,
    server_name: String,
}

impl InterBrokerDialer {
    #[must_use]
    pub fn new(
        client: Arc<InterBrokerClient>,
        listener_protocol: ListenerProtocol,
        server_name: String,
    ) -> Self {
        Self {
            client,
            listener_protocol,
            server_name,
        }
    }
}

#[async_trait::async_trait]
impl krabka_raft::OutboundDialer for InterBrokerDialer {
    async fn dial(
        &self,
        _target: krabka_raft::NodeId,
        addr: &str,
        options: krabka_client_core::ConnectionOptions,
    ) -> Result<krabka_client_core::Connection, krabka_client_core::ClientError> {
        // The raft transport hands us an address in `host:port` form
        // (the openraft `Node.addr` string). For SocketAddr-style
        // addresses we honour the configured `server_name` for SNI
        // separately from the literal host string.
        let (host, port) = match addr.rsplit_once(':') {
            Some((h, p)) => {
                let port: u16 = p.parse().map_err(|e: std::num::ParseIntError| {
                    krabka_client_core::ClientError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid raft peer port in {addr:?}: {e}"),
                    ))
                })?;
                (h.to_string(), port)
            }
            None => {
                return Err(krabka_client_core::ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("raft peer address missing port: {addr:?}"),
                )));
            }
        };
        self.client
            .connect_as_connection(
                &host,
                port,
                self.listener_protocol,
                &self.server_name,
                options,
            )
            .await
            .map_err(|e| match e {
                InterBrokerError::Io(io) => krabka_client_core::ClientError::Io(io),
                other => krabka_client_core::ClientError::Io(std::io::Error::other(format!(
                    "InterBrokerClient dial: {other}"
                ))),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use krabka_units::kibibytes;

    use super::{DEFAULT_SOCKET_BUFFER, InterBrokerClient, to_client_creds, tune_outbound_socket};
    use crate::config::InterBrokerCredentials;

    #[tokio::test]
    async fn outbound_socket_tuning_sets_nodelay_and_large_buffers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener addr");
        let client_task = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (server, _) = listener.accept().await.expect("accept loopback client");
        let client = client_task
            .await
            .expect("connect task")
            .expect("connect loopback client");

        let sock = socket2::SockRef::from(&client);
        client.set_nodelay(false).expect("clear TCP_NODELAY");
        sock.set_send_buffer_size(4096).expect("shrink send buffer");
        sock.set_recv_buffer_size(8192).expect("shrink recv buffer");
        let send_before = sock.send_buffer_size().expect("read baseline send buffer");
        let recv_before = sock.recv_buffer_size().expect("read baseline recv buffer");

        tune_outbound_socket(&client, kibibytes(64), kibibytes(128));

        assert2::assert!(client.nodelay().expect("read TCP_NODELAY"));
        // Kernels clamp and may double requested sizes, so compare the distinct
        // configured buffers instead of asserting host-dependent exact values.
        let send_after = sock.send_buffer_size().expect("read send buffer");
        let recv_after = sock.recv_buffer_size().expect("read recv buffer");
        assert2::assert!(send_after > send_before);
        assert2::assert!(recv_after > recv_before);
        assert2::assert!(recv_after > send_after);
        drop(server);
    }

    #[tokio::test]
    async fn dialed_socket_is_tuned_before_tls_and_sasl() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener addr");
        let accept = tokio::spawn(async move { listener.accept().await });

        let client = InterBrokerClient::new_with_policy(
            None,
            None,
            krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            krabka_client_core::ClientFrameMax::default(),
            kibibytes(64),
            kibibytes(256),
        );
        let dialed = client
            .dial_tuned(&addr.ip().to_string(), addr.port())
            .await
            .expect("dial loopback peer");
        let (server, _) = accept.await.expect("accept task").expect("accept dial");

        // Read the options back off the connected socket the dialer produced,
        // the way the accept path's tuning test does on its side.
        assert2::assert!(dialed.nodelay().expect("read TCP_NODELAY"));
        let sock = socket2::SockRef::from(&dialed);
        let send = sock.send_buffer_size().expect("read send buffer");
        let recv = sock.recv_buffer_size().expect("read recv buffer");
        // Kernels clamp and may double requested sizes, so assert the two
        // distinct configured buffers stayed distinct and ordered.
        assert2::assert!(recv > send);
        drop(server);
    }

    #[test]
    fn process_policy_overrides_call_site_defaults() {
        let client = InterBrokerClient::new_with_policy(
            None,
            None,
            krabka_client_core::ConnectionDispatchQueueCapacity::new(7).unwrap(),
            krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32)).unwrap(),
            kibibytes(64),
            kibibytes(128),
        );
        let mut options = krabka_client_core::ConnectionOptions::default();
        client.apply_resource_policy(&mut options);
        assert2::assert!(options.dispatch_queue_capacity.get() == 7);
        assert2::assert!(options.frame_max.size() == krabka_units::kibibytes(32));
    }

    #[test]
    fn default_construction_uses_socket_tuning_defaults() {
        let client = InterBrokerClient::new(None, None);
        assert2::assert!(client.socket_send_buffer == DEFAULT_SOCKET_BUFFER);
        assert2::assert!(client.socket_receive_buffer == DEFAULT_SOCKET_BUFFER);
    }

    #[test]
    fn oauthbearer_credentials_preserve_rotation_path() {
        let token_path = PathBuf::from("/run/secrets/krabka/inter-broker-token");
        let credentials = to_client_creds(&InterBrokerCredentials::OAuthBearer {
            token_path: token_path.clone(),
        });
        let krabka_client_core::SaslCredentials::OAuthBearer {
            token_path: actual_path,
        } = credentials
        else {
            panic!("expected OAUTHBEARER client credentials");
        };
        assert2::assert!(actual_path == token_path);
    }
}
