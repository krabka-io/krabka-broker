//! Listeners a running broker opens and closes as its per-broker dynamic
//! `listeners` changes, as Kafka 4.3.1's `DynamicListenerConfig.reconfigure`
//! adds and removes them.
//!
//! Each case starts one combined broker with its static PLAINTEXT listener,
//! which carries the alters, reads its static `listeners` and
//! `listener.security.protocol.map` with `DescribeConfigs`, and sets both to
//! the static values plus one listener. Deleting them closes that listener
//! again.
#![cfg(test)]

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
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, codes},
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
    tokio::{
        io::AsyncReadExt as _,
        net::{TcpListener, TcpStream},
        time,
    },
};

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-dynamic-listeners-test";

/// How long a committed change may take to reach the listeners.
const PROPAGATION: Duration = Duration::from_secs(10);

const LISTENERS: &str = "listeners";
const PROTOCOL_MAP: &str = "listener.security.protocol.map";

/// A loopback address no socket holds: bound and dropped at once.
async fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
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
        time::sleep(Duration::from_millis(50)).await;
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
        time::sleep(Duration::from_millis(50)).await;
    }
}

/// Whether the peer closes `stream` within [`PROPAGATION`].
async fn closed_by_peer(stream: &mut TcpStream) -> bool {
    let mut byte = [0_u8; 1];
    matches!(
        time::timeout(PROPAGATION, stream.read(&mut byte)).await,
        Ok(Ok(0) | Err(_))
    )
}

/// The response row of an alter of `resource` that succeeded.
fn accepted(resource: String) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        error_code: codes::NONE,
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

    /// Delete the dynamic `listeners` and `listener.security.protocol.map`.
    async fn delete_listeners(&self) -> AlterConfigsResourceResponse {
        self.alter(vec![
            (LISTENERS, ConfigOperation::Delete, None),
            (PROTOCOL_MAP, ConfigOperation::Delete, None),
        ])
        .await
    }
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct ListenerChange {
    broker: Option<ListenerBroker>,
    directory: Option<TempDir>,
    restarted: Option<BrokerHandle>,
    added_address: Option<SocketAddr>,
    held: Option<TcpStream>,
    alteration: Option<AlterConfigsResourceResponse>,
    handshake: Option<SaslHandshakeResponse>,
    remaining_names: String,
}

#[fixture]
fn listeners() -> ListenerChange {
    // Start this scenario with fresh inputs and observations.

    ListenerChange {
        broker: None,
        directory: None,
        restarted: None,
        added_address: None,
        held: None,
        alteration: None,
        handshake: None,
        remaining_names: String::new(),
    }
}

#[given("a running combined broker with its static PLAINTEXT listener")]
async fn static_broker(listeners: &mut ListenerChange) {
    let dir = TempDir::new().expect("tempdir");
    listeners.broker = Some(ListenerBroker::start(dir.path()).await);
    listeners.directory = Some(dir);
}

#[when(
    "listener {name} using {protocol} and mechanisms {mechanisms} is added on a free loopback port"
)]
async fn add(listeners: &mut ListenerChange, name: String, protocol: String, mechanisms: String) {
    let broker = listeners.broker.as_ref().unwrap();
    let address = free_addr().await;
    let configured_listeners = format!("{},{name}://{address}", broker.static_listeners);
    let configured_protocols = format!("{},{name}:{protocol}", broker.static_protocols);
    let mechanism_key = format!(
        "listener.name.{}.sasl.enabled.mechanisms",
        name.to_lowercase()
    );
    let mut configs = vec![
        (
            LISTENERS,
            ConfigOperation::Set,
            Some(configured_listeners.as_str()),
        ),
        (
            PROTOCOL_MAP,
            ConfigOperation::Set,
            Some(configured_protocols.as_str()),
        ),
    ];
    if mechanisms != "none" {
        configs.push((
            mechanism_key.as_str(),
            ConfigOperation::Set,
            Some(mechanisms.as_str()),
        ));
    }
    listeners.alteration = Some(broker.alter(configs).await);
    listeners.added_address = Some(address);
}

#[then("the alter returns the complete successful broker resource response")]
fn accepted_change(listeners: &ListenerChange) {
    assert!(
        listeners.alteration.as_ref().unwrap()
            == &accepted(listeners.broker.as_ref().unwrap().resource())
    );
}

#[then("a fresh connection to the added listener serves ApiVersions")]
async fn serves(listeners: &mut ListenerChange) {
    listeners.held = eventually_served(listeners.added_address.unwrap()).await;
    assert!(
        listeners.held.is_some(),
        "added listener never served ApiVersions"
    );
}

#[when("the dynamic listeners and protocol map are deleted")]
async fn remove(listeners: &mut ListenerChange) {
    listeners.alteration = Some(listeners.broker.as_ref().unwrap().delete_listeners().await);
}

#[then("the held connection closes and the added port refuses new connections")]
async fn removed(listeners: &mut ListenerChange) {
    assert!(closed_by_peer(listeners.held.as_mut().unwrap()).await);
    assert!(eventually_refused(listeners.added_address.unwrap()).await);
}

#[then("the original static listener still serves ApiVersions")]
async fn static_serves(listeners: &ListenerChange) {
    assert!(
        connect_served(listeners.broker.as_ref().unwrap().admin)
            .await
            .is_some()
    );
}

#[when("the held connection requests SASL mechanism {mechanism}")]
async fn sasl(listeners: &mut ListenerChange, mechanism: String) {
    listeners.handshake = Some(
        kafka_wire::exchange()
            .stream(listeners.held.as_mut().unwrap())
            .request(&SaslHandshakeRequest {
                mechanism,
                ..Default::default()
            })
            .api_key(17)
            .version(1)
            .client_id(CLIENT_ID)
            .call()
            .await
            .expect("SaslHandshake"),
    );
}

#[then("the SASL response has error {error} and exactly mechanism {mechanism}")]
fn sasl_response(listeners: &ListenerChange, error: i16, mechanism: String) {
    assert!(
        listeners.handshake.as_ref().unwrap()
            == &SaslHandshakeResponse {
                error_code: error,
                mechanisms: vec![mechanism],
                ..Default::default()
            }
    );
}

#[when("the broker is shut down while its dynamic listener remains configured")]
async fn shutdown(listeners: &mut ListenerChange) {
    drop(listeners.held.take());
    listeners.broker.take().unwrap().handle.shutdown().await;
}

#[then("the added port refuses new connections")]
async fn not_listening(listeners: &ListenerChange) {
    assert!(eventually_refused(listeners.added_address.unwrap()).await);
}

#[when("the broker rejoins using the same data directory")]
async fn restart(listeners: &mut ListenerChange) {
    let mut config =
        BrokerConfig::for_tests(listeners.directory.as_ref().unwrap().path().to_path_buf());
    config.bootstrap_mode = BootstrapMode::Rejoin;
    listeners.restarted = Some(Broker::start(config).await.expect("broker restart"));
}

#[when("the advertised PLAINTEXT listener is removed from the static listener list")]
async fn drop_advertised(listeners: &mut ListenerChange) {
    let broker = listeners.broker.as_ref().unwrap();
    let kept: Vec<&str> = broker
        .static_listeners
        .split(',')
        .filter(|entry| !entry.starts_with("PLAINTEXT://"))
        .collect();
    let without_plaintext = kept.join(",");
    listeners.remaining_names = kept
        .iter()
        .filter_map(|entry| entry.split_once("://").map(|(name, _)| name))
        .collect::<Vec<_>>()
        .join(",");
    listeners.alteration = Some(
        broker
            .alter(vec![(
                LISTENERS,
                ConfigOperation::Set,
                Some(&without_plaintext),
            )])
            .await,
    );
}

#[when("the existing listener {name} protocol is changed from {from} to {to}")]
async fn protocol_change(listeners: &mut ListenerChange, name: String, from: String, to: String) {
    let broker = listeners.broker.as_ref().unwrap();
    let map = broker
        .static_protocols
        .replace(&format!("{name}:{from}"), &format!("{name}:{to}"));
    listeners.alteration = Some(
        broker
            .alter(vec![(PROTOCOL_MAP, ConfigOperation::Set, Some(&map))])
            .await,
    );
}

#[then("the complete broker resource response has error {error} and message")]
fn refused(listeners: &ListenerChange, error: i16, docstring: String) {
    let expected = docstring.trim().replace(
        "{remaining-static-listener-names}",
        &listeners.remaining_names,
    );
    assert!(
        listeners.alteration.as_ref().unwrap()
            == &AlterConfigsResourceResponse {
                error_code: error,
                error_message: Some(expected),
                ..accepted(listeners.broker.as_ref().unwrap().resource())
            }
    );
}

#[then("both described listener settings retain their original static values")]
async fn unchanged(listeners: &ListenerChange) {
    let broker = listeners.broker.as_ref().unwrap();
    assert!(broker.describe(LISTENERS).await == broker.static_listeners);
    assert!(broker.describe(PROTOCOL_MAP).await == broker.static_protocols);
}

impl ListenerChange {
    async fn shutdown(mut self) {
        drop(self.held.take());
        if let Some(broker) = self.broker.take() {
            broker.handle.shutdown().await;
        }
        if let Some(broker) = self.restarted.take() {
            broker.shutdown().await;
        }
    }
}

#[scenario(
    path = "tests/features/migrated/tests/dynamic_listeners.feature",
    name = "an added listener serves until it is removed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_added_listener_serves_until_it_is_removed(listeners: ListenerChange) {
    listeners.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/dynamic_listeners.feature",
    name = "an added sasl listener offers its own mechanisms"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_added_sasl_listener_offers_its_own_mechanisms(listeners: ListenerChange) {
    listeners.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/dynamic_listeners.feature",
    name = "a stored listener opens after a restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stored_listener_opens_after_a_restart(listeners: ListenerChange) {
    listeners.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/dynamic_listeners.feature",
    name = "a listener change kafka refuses is refused"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listener_change_kafka_refuses_is_refused(listeners: ListenerChange) {
    listeners.shutdown().await;
}
