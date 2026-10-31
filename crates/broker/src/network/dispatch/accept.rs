//! Per-listener accept path. It terminates TLS when the listener protocol
//! requires it, derives the mTLS principal from the peer certificate, and
//! hands the post-handshake byte stream to the generic per-connection request
//! loop in the parent module.
//!
//! The handshake is the one stretch of a connection's life that the request
//! loop's `connections.max.idle.ms` deadline cannot see, so this module bounds
//! it by the same window: see `handshake_within_idle_window`.

use {
    super::{
        serve_connection_stream,
        write_idle::{IdleTimer, WriteIdle},
    },
    crate::{
        broker::Broker,
        config::ListenerSpec,
        listener_tls::ListenerTlsCache,
        metrics::{BrokerMetrics, ConnectionCloseReason},
        network::auth::subject_dn_rfc2253,
        platform::Sockets,
    },
    krabka_metadata::{MetadataImage, NodeId},
    krabka_security::{DynamicServerConfig, TlsConfig},
    rustls::ServerConfig,
    std::{io, net::SocketAddr, sync::Arc, time::Duration},
    tokio::{net::TcpStream, time},
    tokio_rustls::{TlsAcceptor, server::TlsStream},
};

/// Per-listener entrypoint.
///
/// It branches between TLS termination, when the listener's protocol requires
/// TLS, and the plaintext path. Both paths converge on
/// [`serve_connection_stream`] for the per-connection request loop.
pub async fn serve_connection_on_listener(
    broker: Arc<Broker>,
    stream: TcpStream,
    spec: ListenerSpec,
) {
    // Capture the peer address from the underlying TCP socket before we
    // hand the stream off to the TLS layer / framing loop. ACL
    // handlers need this for host-based ACL matching. When there is none
    // (a socket closed mid-accept, or a platform with preopened sockets),
    // it is the unspecified address; ACL matchers treat it as a
    // non-matching host.
    let peer = Sockets::TARGET.peer_address(&stream);
    if spec.protocol.requires_tls() {
        let Some(acceptor) = listener_tls_acceptor(&broker, &spec) else {
            return;
        };
        // The handshake is held to the same `connections.max.idle.ms` the
        // serve loop holds a connected peer to: a peer that opens the socket
        // and never sends a ClientHello would otherwise park a task and an fd
        // here forever, never reaching the loop that arms the idle deadline.
        let idle = broker.config.connections_max_idle_for(&spec.name);
        // Linux kTLS (Increment F): when the startup probe confirmed kTLS
        // support, terminate TLS through a `CorkStream` so `ktls` can cleanly
        // drain the rustls buffer, then hand the socket to the kernel via
        // `config_ktls_server`. The resulting `KtlsStream` is `SendfileSink`-
        // capable, so the Fetch path emits file regions and `sendfile(2)`s
        // them onto the socket — the kernel encrypts them into TLS records
        // (zero-copy over TLS). The wire bytes a client decrypts are identical
        // to the userspace path; only the encrypt locus moves kernel-side.
        #[cfg(target_os = "linux")]
        if broker.ktls_enabled {
            let handshake = acceptor.accept(ktls::CorkStream::new(stream));
            // Derive the mTLS principal from the peer cert BEFORE the kTLS
            // transition consumes the stream by value. `get_ref()` reaches the
            // rustls `ServerConnection` through the `CorkStream` wrapper
            // exactly as for a plain `TlsStream`.
            let Some(AuthenticatedTls {
                stream: tls_stream,
                principal: mtls_principal,
            }) = authenticated_tls(handshake, idle, peer, &broker.metrics, &spec).await
            else {
                return;
            };
            // `config_ktls_server` consumes `tls_stream` by value; on error
            // the stream is gone, so we cannot fall back to userspace TLS for
            // THIS connection — we close it. This is safe precisely because
            // the startup probe already proved kTLS works on this host, so an
            // error here is an unexpected per-connection anomaly, not the
            // common case.
            match ktls::config_ktls_server(tls_stream).await {
                // NB: any post-handshake app bytes rustls already decrypted
                // are carried INSIDE `ktls_stream` (the `ktls` crate stores
                // them and replays them on the first `poll_read`), so the
                // `Framed` reader in `serve_connection_stream` sees them
                // transparently — no manual drain plumbing needed.
                Ok(ktls_stream) => {
                    serve_connection_stream(broker, ktls_stream, spec, peer, mtls_principal).await;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "kTLS configuration failed after handshake; closing connection \
                         (startup probe had reported kTLS supported)"
                    );
                }
            }
            return;
        }

        // rustls can make ciphertext progress while its outer poll_flush
        // remains pending. Track the socket writes beneath its buffer.
        let handshake = acceptor.accept(WriteIdle::new(stream, IdleTimer::new(idle)));
        // A required certificate was checked by rustls; optional/disabled certificates may be absent.
        // A presented DN with no configured mapping still closes the connection.
        let Some(AuthenticatedTls {
            stream: tls_stream,
            principal: mtls_principal,
        }) = authenticated_tls(handshake, idle, peer, &broker.metrics, &spec).await
        else {
            return;
        };
        // Boxed: the userspace TLS stream makes this the largest serve-loop
        // future, past `clippy::large_futures`.
        Box::pin(serve_connection_stream(
            broker,
            tls_stream,
            spec,
            peer,
            mtls_principal,
        ))
        .await;
    } else {
        serve_connection_plaintext(broker, stream, spec, peer).await;
    }
}

/// The TLS acceptor for a new connection on `spec`'s listener, or `None`
/// when the listener has no server config to serve; that case is logged.
fn listener_tls_acceptor(broker: &Broker, spec: &ListenerSpec) -> Option<TlsAcceptor> {
    TlsSources::of(broker)
        .server_config(spec, || broker.controller.current_image())
        .map(TlsAcceptor::from)
}

/// The places a TLS listener's server config comes from.
struct TlsSources<'a> {
    node_id: NodeId,
    /// The configs built from each listener's dynamic SSL keys.
    listener_keys: &'a ListenerTlsCache,
    /// The broker-wide static `tls_config`.
    broker_static: Option<&'a TlsConfig>,
    /// The hot-reloaded config built from `broker_static`.
    broker_dynamic: Option<&'a DynamicServerConfig>,
}

impl<'a> TlsSources<'a> {
    fn of(broker: &'a Broker) -> Self {
        Self {
            node_id: broker.config.node_id,
            listener_keys: &broker.listener_tls,
            broker_static: broker.config.tls_config.as_ref(),
            broker_dynamic: broker.tls_dynamic.as_deref(),
        }
    }

    /// The server config a new handshake on `spec`'s listener uses, reading
    /// the dynamic keys from `image` only when there is a static config to
    /// start them from.
    ///
    /// A listener's dynamic `listener.name.<listener>.ssl.*` keys, set for
    /// this node, win over its static `tls_config` while set: Kafka's
    /// `SslFactory.reconfigure` serves new handshakes from the new stores. A
    /// listener without either serves the broker-wide config.
    fn server_config(
        &self,
        spec: &ListenerSpec,
        image: impl FnOnce() -> Arc<MetadataImage>,
    ) -> Option<Arc<ServerConfig>> {
        let dynamic = spec
            .tls_config
            .as_ref()
            .or(self.broker_static)
            .and_then(|static_tls| {
                self.listener_keys
                    .server_config(&image(), self.node_id, &spec.name, static_tls)
            });
        if dynamic.is_some() {
            return dynamic;
        }
        if let Some(per_tls) = spec.tls_config.as_ref() {
            return match per_tls.build_server_config() {
                Ok(sc) => Some(sc),
                Err(e) => {
                    tracing::error!(
                        listener = %spec.name,
                        error = %e,
                        "failed to build TlsAcceptor from per-listener tls_config"
                    );
                    None
                }
            };
        }
        // Use DynamicServerConfig so hot-reload keeps working.
        let Some(dynamic) = self.broker_dynamic else {
            tracing::error!(
                listener = %spec.name,
                "TLS listener without per-listener tls_config and no broker-wide tls_dynamic"
            );
            return None;
        };
        // Snapshot per accept; an in-flight handshake keeps its captured config.
        Some(dynamic.current())
    }
}

/// Drives a TLS handshake, giving up once the listener's idle window passes.
///
/// Apache Kafka registers a channel with its `Selector` at accept time, before
/// the handshake runs, so `connections.max.idle.ms` already covers a peer that
/// connects to an SSL listener and sends no `ClientHello`; a broker configured
/// with a 20-second window closes such a socket after 20 seconds. Without this
/// bound krabka would hold it forever, which is the fd-exhaustion shape the
/// idle window exists to close — and the pre-handshake stall is the cheapest
/// version of it, because the peer spends nothing to reach it.
///
/// `None` means the connection is finished: either the window passed, which is
/// counted as an idle close like any other, or the handshake itself failed.
async fn handshake_within_idle_window<F, T>(
    handshake: F,
    idle: Option<Duration>,
    peer: SocketAddr,
    metrics: &BrokerMetrics,
) -> Option<T>
where
    F: Future<Output = io::Result<T>>,
{
    let outcome = match idle {
        Some(window) => match time::timeout(window, handshake).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => {
                // An idle close, which Kafka's `Selector` logs below INFO.
                tracing::debug!(
                    peer = %peer,
                    idle_ms = window.as_millis(),
                    "TLS handshake idle past connections.max.idle.ms, closing"
                );
                metrics.record_connection_close(ConnectionCloseReason::Idle);
                return None;
            }
        },
        None => handshake.await,
    };
    match outcome {
        Ok(stream) => Some(stream),
        Err(error) => {
            tracing::debug!(%error, peer = %peer, "TLS handshake failed");
            None
        }
    }
}

/// A TLS stream whose handshake finished and the principal its peer
/// certificate maps to, if it presented one.
struct AuthenticatedTls<S> {
    stream: TlsStream<S>,
    principal: Option<krabka_security::Principal>,
}

async fn authenticated_tls<F, S>(
    handshake: F,
    idle: Option<Duration>,
    peer: SocketAddr,
    metrics: &BrokerMetrics,
    spec: &ListenerSpec,
) -> Option<AuthenticatedTls<S>>
where
    F: Future<Output = io::Result<TlsStream<S>>>,
{
    let stream = handshake_within_idle_window(handshake, idle, peer, metrics).await?;
    let principal = peer_cert_principal(&stream, spec, peer).ok()?;
    Some(AuthenticatedTls { stream, principal })
}

/// Inspects the post-handshake TLS stream for a peer certificate. If one is
/// present, this derives the Subject DN with
/// [`crate::network::auth::subject_dn_rfc2253`], the RFC 2253 form Kafka's
/// `X500Principal.getName()` gives, and runs it through the listener's KIP-371
/// `ssl.principal.mapping.rules` to get the principal name.
/// The default rule list is Kafka's `DEFAULT`, under which the DN itself is
/// the principal.
///
/// `Ok(None)` is the no-certificate case, which the session layer turns into
/// `ANONYMOUS`. `Err(())` means a certificate was presented and no mapping
/// rule matched its DN: Kafka's `SslPrincipalMapper` throws `NoMatchingRule`
/// there and the channel never builds, so the caller closes the connection
/// rather than admitting the peer under its DN or as `ANONYMOUS`.
fn peer_cert_principal<S>(
    stream: &TlsStream<S>,
    spec: &ListenerSpec,
    peer: SocketAddr,
) -> Result<Option<krabka_security::Principal>, ()> {
    let (_, server_conn) = stream.get_ref();
    let distinguished_name = server_conn
        .peer_certificates()
        .and_then(<[_]>::first)
        .and_then(|cert| subject_dn_rfc2253(cert.as_ref()));
    mtls_principal(spec, peer, distinguished_name.as_deref())
}

/// The principal a peer certificate's Subject DN gives on `spec`'s listener.
///
/// Kafka's `DefaultKafkaPrincipalBuilder` applies `SslPrincipalMapper` to an
/// SSL authentication context only. On `SASL_SSL` the principal comes from
/// the SASL exchange and the client certificate is never mapped, so a DN that
/// no rule matches is no reason to close the connection there.
fn mtls_principal(
    spec: &ListenerSpec,
    peer: SocketAddr,
    distinguished_name: Option<&str>,
) -> Result<Option<krabka_security::Principal>, ()> {
    let Some(distinguished_name) = distinguished_name.filter(|_| !spec.protocol.requires_sasl())
    else {
        return Ok(None);
    };
    let Some(name) = spec.principal_mapper.apply(distinguished_name) else {
        tracing::warn!(
            listener = %spec.name,
            peer = %peer,
            distinguished_name = %distinguished_name,
            "no ssl.principal.mapping.rules rule matched the peer certificate \
             subject DN, closing connection"
        );
        return Err(());
    };
    Ok(Some(krabka_security::Principal {
        name,
        auth_method: krabka_security::AuthMethod::MTls,
        groups: vec![],
    }))
}

/// Plaintext entry point. It keeps the legacy `TcpStream`-typed signature for
/// call sites, and it records the peer's TCP address before it passes the
/// stream to the generic loop.
async fn serve_connection_plaintext(
    broker: Arc<Broker>,
    stream: TcpStream,
    spec: ListenerSpec,
    peer: SocketAddr,
) {
    serve_connection_stream(broker, stream, spec, peer, None).await;
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config::ListenerSpec, network::auth::SslPrincipalMapper,
            test_support::localhost_ecdsa_pair,
        },
        assert2::{assert, check},
        krabka_metadata::{BrokerConfigRecord, MetadataRecord},
        krabka_security::{ClientAuthMode, ListenerProtocol},
        rustls::crypto::aws_lc_rs,
        std::{fs, path::Path},
    };

    fn spec(protocol: ListenerProtocol, rules: &[&str]) -> ListenerSpec {
        ListenerSpec {
            name: "L".to_owned(),
            bind_addr: "127.0.0.1:0".parse().expect("static addr"),
            advertised: "127.0.0.1:0".to_owned(),
            protocol,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: SslPrincipalMapper::parse(rules).expect("rules parse"),
        }
    }

    /// The Subject DN is mapped on an SSL listener, where a DN that no rule
    /// matches closes the connection, and never on a `SASL_SSL` listener, whose
    /// principal comes from the SASL exchange.
    #[test]
    fn the_subject_dn_is_mapped_on_ssl_listeners_only() {
        let peer: SocketAddr = "127.0.0.1:9".parse().expect("static addr");
        let dn = Some("CN=alice,OU=integration");
        let match_rule = &["RULE:^CN=(.*?),OU=integration$/$1/"][..];
        let no_match_rule = &["RULE:^CN=(.*?),OU=ServiceUsers$/$1/"][..];
        // (protocol, rules, distinguished name, outcome: `Err` closes the
        // connection, `Ok(None)` is no principal from the certificate)
        let cases = [
            (ListenerProtocol::Ssl, match_rule, dn, Ok(Some("alice"))),
            (ListenerProtocol::Ssl, no_match_rule, dn, Err(())),
            (ListenerProtocol::Ssl, no_match_rule, None, Ok(None)),
            (ListenerProtocol::SaslSsl, no_match_rule, dn, Ok(None)),
            (ListenerProtocol::SaslSsl, match_rule, dn, Ok(None)),
        ];
        for (protocol, rules, distinguished_name, expected) in cases {
            let outcome = mtls_principal(&spec(protocol, rules), peer, distinguished_name)
                .map(|principal| principal.map(|principal| principal.name));
            check!(
                outcome == expected.map(|name| name.map(str::to_owned)),
                "{protocol:?} {rules:?} {distinguished_name:?}"
            );
        }
    }

    /// A broker-wide and a per-listener static config, a per-listener one
    /// whose files are missing, and a keystore file for the dynamic keys.
    struct TlsFixture {
        _dir: tempfile::TempDir,
        broker_static: TlsConfig,
        listener_static: TlsConfig,
        broken_static: TlsConfig,
        keystore: String,
    }

    fn static_tls(dir: &Path, name: &str) -> TlsConfig {
        TlsConfig {
            cert_chain_path: dir.join(format!("{name}-cert.pem")),
            private_key_path: dir.join(format!("{name}-key.pem")),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        }
    }

    fn tls_fixture() -> TlsFixture {
        drop(aws_lc_rs::default_provider().install_default());
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert, key) = localhost_ecdsa_pair();
        let (cert, key) = (cert.pem(), key.serialize_pem());
        let [broker_static, listener_static] =
            ["broker", "listener"].map(|name| static_tls(dir.path(), name));
        for tls in [&broker_static, &listener_static] {
            fs::write(&tls.cert_chain_path, &cert).expect("write cert");
            fs::write(&tls.private_key_path, &key).expect("write key");
        }
        let keystore = dir.path().join("dynamic.pem");
        fs::write(&keystore, format!("{key}{cert}")).expect("write keystore");
        TlsFixture {
            broken_static: static_tls(dir.path(), "missing"),
            keystore: keystore.to_str().expect("utf-8 path").to_owned(),
            _dir: dir,
            broker_static,
            listener_static,
        }
    }

    /// An image that sets this node's keystore location for listener `L`, or
    /// sets nothing.
    fn image(keystore: Option<&str>) -> Arc<MetadataImage> {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        if let Some(location) = keystore {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(1),
                config_name: "listener.name.l.ssl.keystore.location".to_owned(),
                config_value: Some(location.to_owned()),
            }));
        }
        Arc::new(image)
    }

    /// Which source a listener's server config came from.
    #[derive(Debug, PartialEq, Eq)]
    enum Served {
        ListenerKeys,
        ListenerStatic,
        BrokerWide,
        Nothing,
    }

    fn served(sources: &TlsSources<'_>, spec: &ListenerSpec, image: &Arc<MetadataImage>) -> Served {
        let Some(config) = sources.server_config(spec, || Arc::clone(image)) else {
            return Served::Nothing;
        };
        if sources
            .broker_dynamic
            .is_some_and(|dynamic| Arc::ptr_eq(&config, &dynamic.current()))
        {
            return Served::BrokerWide;
        }
        // The cache hands back the config it built while the keys are unchanged.
        let from_keys = spec
            .tls_config
            .as_ref()
            .or(sources.broker_static)
            .and_then(|tls| {
                sources
                    .listener_keys
                    .server_config(image, NodeId(1), "L", tls)
            });
        if from_keys.is_some_and(|keys| Arc::ptr_eq(&config, &keys)) {
            Served::ListenerKeys
        } else {
            Served::ListenerStatic
        }
    }

    /// The dynamic listener keys win over the listener's static config, which
    /// wins over the broker-wide one. The keys are read only when a static
    /// config, the listener's or the broker's, exists to start them from, and
    /// a per-listener config that fails to build serves nothing rather than
    /// the broker-wide config.
    #[test]
    fn a_listener_serves_its_keys_then_its_static_config_then_the_broker_wide_one() {
        let fixture = tls_fixture();
        let broker_dynamic =
            DynamicServerConfig::from_tls_config(&fixture.broker_static).expect("broker-wide");
        let keys = Some(fixture.keystore.as_str());
        // (per-listener static, broker-wide static, broker-wide dynamic,
        // listener keys, served)
        let cases = [
            (
                Some(&fixture.listener_static),
                None,
                None,
                keys,
                Served::ListenerKeys,
            ),
            (
                Some(&fixture.listener_static),
                Some(&fixture.broker_static),
                Some(&*broker_dynamic),
                None,
                Served::ListenerStatic,
            ),
            (
                None,
                Some(&fixture.broker_static),
                Some(&*broker_dynamic),
                None,
                Served::BrokerWide,
            ),
            (
                None,
                Some(&fixture.broker_static),
                Some(&*broker_dynamic),
                keys,
                Served::ListenerKeys,
            ),
            (
                None,
                Some(&fixture.broker_static),
                None,
                keys,
                Served::ListenerKeys,
            ),
            (None, None, Some(&*broker_dynamic), keys, Served::BrokerWide),
            (
                Some(&fixture.broken_static),
                None,
                Some(&*broker_dynamic),
                None,
                Served::Nothing,
            ),
            (None, None, None, None, Served::Nothing),
        ];
        for (listener_static, broker_static, broker_dynamic, keystore, expected) in cases {
            let mut listener = spec(ListenerProtocol::Ssl, &[]);
            listener.tls_config = listener_static.cloned();
            let listener_keys = ListenerTlsCache::default();
            let sources = TlsSources {
                node_id: NodeId(1),
                listener_keys: &listener_keys,
                broker_static,
                broker_dynamic,
            };
            assert!(
                served(&sources, &listener, &image(keystore)) == expected,
                "{listener_static:?} {broker_static:?} {keystore:?}"
            );
        }
    }
}
