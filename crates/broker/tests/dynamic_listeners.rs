//! Listeners a running broker opens and closes as its per-broker dynamic
//! `listeners` changes, as Kafka 4.3.1's `DynamicListenerConfig.reconfigure`
//! adds and removes them.
//!
//! Each case starts one combined broker with its static PLAINTEXT listener,
//! which carries the alters, reads its static `listeners` and
//! `listener.security.protocol.map` with `DescribeConfigs`, and sets both to
//! the static values plus one listener. Deleting them closes that listener
//! again.

mod kafka_wire;
mod support;

use {
    crate::support::{
        configs::{
            BROKER_RESOURCE, ConfigMutation, ConfigOperation, broker_alter_request,
            describe_resource,
        },
        wire::HeaderEncoding,
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle},
    krabka_protocol::owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::DescribeConfigsResponse,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    std::{
        io::ErrorKind,
        net::SocketAddr,
        path::Path,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{io::AsyncReadExt as _, net::TcpStream},
};

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-dynamic-listeners-test";

/// How long a committed change may take to reach the listeners.
const PROPAGATION: Duration = Duration::from_secs(10);

const LISTENERS: &str = "listeners";
const PROTOCOL_MAP: &str = "listener.security.protocol.map";

/// A loopback address no socket holds: bound and dropped at once.
async fn free_addr() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    listener.local_addr().expect("local address")
}

/// Whether a new connection to `addr` answers an `ApiVersions` round trip.
/// The stream is returned so the caller can hold it.
async fn connect_served(addr: SocketAddr) -> Option<TcpStream> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    kafka_wire::api_versions_answered(&mut stream, CLIENT_ID)
        .await
        .then_some(stream)
}

/// Poll until a new connection to `addr` is served, and return it.
async fn eventually_served(addr: SocketAddr) -> Option<TcpStream> {
    let deadline = Instant::now() + PROPAGATION;
    loop {
        if let Some(stream) = connect_served(addr).await {
            return Some(stream);
        }
        if Instant::now() >= deadline {
            return None;
        }
        // The alter is committed before its response, but the listener opens
        // once the broker applies the image, which has no signal a test can
        // wait on.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Whether a connect to `addr` is refused within [`PROPAGATION`]: no socket
/// listens there any more. A connect that the closing listener's accept
/// queue took and then reset is not yet that, so polling goes on.
async fn eventually_refused(addr: SocketAddr) -> bool {
    let deadline = Instant::now() + PROPAGATION;
    loop {
        if let Err(error) = TcpStream::connect(addr).await
            && error.kind() == ErrorKind::ConnectionRefused
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Whether the peer closes `stream` within [`PROPAGATION`].
async fn closed_by_peer(stream: &mut TcpStream) -> bool {
    let mut byte = [0_u8; 1];
    matches!(
        tokio::time::timeout(PROPAGATION, stream.read(&mut byte)).await,
        Ok(Ok(0) | Err(_))
    )
}

/// The response row of an alter of `resource` that succeeded.
fn accepted(resource: String) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        error_code: krabka_broker::codes::NONE,
        error_message: None,
        resource_type: BROKER_RESOURCE,
        resource_name: resource,
        ..Default::default()
    }
}

/// A running broker, the address that carries the alters, and its static
/// listener values as `DescribeConfigs` reads them.
struct ListenerBroker {
    handle: BrokerHandle,
    admin: SocketAddr,
    static_listeners: String,
    static_protocols: String,
}

impl ListenerBroker {
    /// Start a combined PLAINTEXT broker on `dir`.
    async fn start(dir: &Path) -> Self {
        let handle = Broker::start(BrokerConfig::for_tests(dir.to_path_buf()))
            .await
            .expect("broker start");
        let admin = handle.listen_addr();
        let mut broker = Self {
            handle,
            admin,
            static_listeners: String::new(),
            static_protocols: String::new(),
        };
        broker.static_listeners = broker.describe(LISTENERS).await;
        broker.static_protocols = broker.describe(PROTOCOL_MAP).await;
        broker
    }

    fn resource(&self) -> String { self.handle.node_id().to_string() }

    /// The value `DescribeConfigs` reports for `key` of this broker.
    async fn describe(&self, key: &str) -> String {
        let request = DescribeConfigsRequest {
            resources: vec![describe_resource(
                BROKER_RESOURCE,
                self.resource(),
                Some(vec![key.to_owned()]),
            )],
            ..Default::default()
        };
        let mut stream = TcpStream::connect(self.admin).await.expect("connect");
        let response: DescribeConfigsResponse = kafka_wire::exchange()
            .stream(&mut stream)
            .request(&request)
            .api_key(32)
            .version(4)
            .client_id(CLIENT_ID)
            .header(HeaderEncoding::Flexible)
            .call()
            .await
            .expect("DescribeConfigs round trip");
        response
            .results
            .into_iter()
            .flat_map(|result| result.configs)
            .find(|entry| entry.name == key)
            .and_then(|entry| entry.value)
            .unwrap_or_else(|| panic!("{key} is described"))
    }

    /// Send one `IncrementalAlterConfigs` of this broker's resource.
    async fn alter(
        &self,
        configs: Vec<(&str, ConfigOperation, Option<&str>)>,
    ) -> AlterConfigsResourceResponse {
        let request = broker_alter_request(self.resource(), configs, ConfigMutation::Apply);
        kafka_wire::alter_one_resource(self.admin, &request, CLIENT_ID).await
    }

    /// Set `listeners` and `listener.security.protocol.map` to the static
    /// values plus listener `name` of `protocol` on `addr`, with `extra`
    /// configs beside them, and return a connection the new listener serves.
    async fn open_listener(
        &self,
        name: &str,
        protocol: &str,
        addr: SocketAddr,
        extra: Vec<(&str, ConfigOperation, Option<&str>)>,
    ) -> TcpStream {
        let listeners = format!("{},{name}://{addr}", self.static_listeners);
        let protocols = format!("{},{name}:{protocol}", self.static_protocols);
        let mut configs = vec![
            (LISTENERS, ConfigOperation::Set, Some(listeners.as_str())),
            (PROTOCOL_MAP, ConfigOperation::Set, Some(protocols.as_str())),
        ];
        configs.extend(extra);
        assert!(self.alter(configs).await == accepted(self.resource()));
        eventually_served(addr)
            .await
            .expect("the added listener serves ApiVersions")
    }

    /// Delete the dynamic `listeners` and `listener.security.protocol.map`.
    async fn delete_listeners(&self) -> AlterConfigsResourceResponse {
        self.alter(vec![
            (LISTENERS, ConfigOperation::Delete, None),
            (PROTOCOL_MAP, ConfigOperation::Delete, None),
        ])
        .await
    }
}

/// A listener the dynamic `listeners` adds serves the next connection; when
/// the config drops it again, its open connection closes and its port
/// refuses a new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_added_listener_serves_until_it_is_removed() {
    let dir = TempDir::new().expect("tempdir");
    let broker = ListenerBroker::start(dir.path()).await;
    let extra = free_addr().await;

    let mut held = broker
        .open_listener("EXTRA", "PLAINTEXT", extra, Vec::new())
        .await;

    let removed = broker.delete_listeners().await;
    assert!(removed == accepted(broker.resource()));
    assert!(closed_by_peer(&mut held).await);
    assert!(eventually_refused(extra).await);
    // The static listener keeps serving.
    assert!(connect_served(broker.admin).await.is_some());
}

/// A SASL listener the dynamic `listeners` adds offers the mechanisms of its
/// own `listener.name.<listener>.sasl.enabled.mechanisms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_added_sasl_listener_offers_its_own_mechanisms() {
    let dir = TempDir::new().expect("tempdir");
    let broker = ListenerBroker::start(dir.path()).await;
    let sasl = free_addr().await;

    // ApiVersions is served before authentication.
    let mut stream = broker
        .open_listener("SASL", "SASL_PLAINTEXT", sasl, vec![(
            "listener.name.sasl.sasl.enabled.mechanisms",
            ConfigOperation::Set,
            Some("SCRAM-SHA-256"),
        )])
        .await;

    let handshake: SaslHandshakeResponse = kafka_wire::exchange()
        .stream(&mut stream)
        .request(&SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-256".to_owned(),
            ..Default::default()
        })
        .api_key(17)
        .version(1)
        .client_id(CLIENT_ID)
        .call()
        .await
        .expect("SaslHandshake round trip");
    assert!(
        handshake
            == SaslHandshakeResponse {
                error_code: krabka_broker::codes::NONE,
                mechanisms: vec!["SCRAM-SHA-256".to_owned()],
                ..Default::default()
            }
    );
}

/// A dynamic `listeners` stored before a restart opens its listener when the
/// broker starts again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stored_listener_opens_after_a_restart() {
    let dir = TempDir::new().expect("tempdir");
    let broker = ListenerBroker::start(dir.path()).await;
    let extra = free_addr().await;
    drop(
        broker
            .open_listener("EXTRA", "PLAINTEXT", extra, Vec::new())
            .await,
    );
    broker.handle.shutdown().await;
    assert!(eventually_refused(extra).await);

    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let restarted = Broker::start(config).await.expect("broker restart");
    assert!(eventually_served(extra).await.is_some());
    restarted.shutdown().await;
}

/// The broker an alter is addressed to refuses a listener change that Kafka
/// 4.3.1 refuses, with Kafka's message, and applies none of it: one that
/// drops an advertised listener (`KafkaConfig.validateValues`) and one that
/// changes the protocol of a listener it keeps
/// (`DynamicListenerConfig.validateReconfiguration`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listener_change_kafka_refuses_is_refused() {
    let dir = TempDir::new().expect("tempdir");
    let broker = ListenerBroker::start(dir.path()).await;
    let kept: Vec<&str> = broker
        .static_listeners
        .split(',')
        .filter(|entry| !entry.starts_with("PLAINTEXT://"))
        .collect();
    let without_plaintext = kept.join(",");
    let kept_names: Vec<&str> = kept
        .iter()
        .filter_map(|entry| entry.split_once("://").map(|(name, _)| name))
        .collect();
    let plaintext_as_ssl = broker
        .static_protocols
        .replace("PLAINTEXT:PLAINTEXT", "PLAINTEXT:SSL");
    let cases = [
        (
            vec![(
                LISTENERS,
                ConfigOperation::Set,
                Some(without_plaintext.as_str()),
            )],
            format!(
                "requirement failed: advertised.listeners listener names must be equal to or a \
                 subset of the ones defined in listeners. Found PLAINTEXT. The valid options \
                 based on the current configuration are {}",
                kept_names.join(",")
            ),
        ),
        (
            vec![(
                PROTOCOL_MAP,
                ConfigOperation::Set,
                Some(plaintext_as_ssl.as_str()),
            )],
            "Invalid value org.apache.kafka.common.config.ConfigException: Security protocol \
             cannot be updated for existing listener ListenerName(PLAINTEXT) for configuration \
             Invalid dynamic configuration"
                .to_owned(),
        ),
    ];
    for (configs, message) in cases {
        let refused = broker.alter(configs).await;
        assert!(
            refused
                == AlterConfigsResourceResponse {
                    error_code: krabka_broker::codes::INVALID_REQUEST,
                    error_message: Some(message),
                    ..accepted(broker.resource())
                }
        );
    }
    assert!(broker.describe(LISTENERS).await == broker.static_listeners);
    assert!(broker.describe(PROTOCOL_MAP).await == broker.static_protocols);
}
