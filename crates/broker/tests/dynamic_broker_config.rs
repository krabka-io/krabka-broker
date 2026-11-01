//! Dynamic broker configuration that a running broker applies without a
//! restart.
//!
//! Each case starts one broker with no connection cap, alters a dynamic
//! `BROKER` config with `IncrementalAlterConfigs`, and observes the effect on
//! the next connection the broker accepts, as Kafka 4.3.1's
//! `DynamicListenerConfig` and `ConnectionQuotas` apply it. Deleting the
//! config restores the static value, again without a restart.
#![cfg(test)]

mod kafka_wire;
mod support;

use {
    crate::support::{
        configs::{
            BROKER_RESOURCE, ConfigMutation, ConfigOperation, broker_alter_request,
            describe_resource,
        },
        sasl::start_plaintext_configured,
        wire::HeaderEncoding,
    },
    assert2::assert,
    krabka_broker::codes,
    krabka_protocol::owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResponse},
        incremental_alter_configs_response::{
            AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
        },
    },
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::{Duration, Instant},
    },
    tokio::{
        net::{TcpSocket, TcpStream},
        time,
    },
};

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-dynamic-broker-config-test";

/// `IncrementalAlterConfigs`' API key and the flexible version this suite
/// sends.
const INCREMENTAL_ALTER_CONFIGS: (i16, i16) = (44, 1);

/// `DescribeConfigs`' API key and the flexible version this suite sends.
const DESCRIBE_CONFIGS: (i16, i16) = (32, 4);

/// How long a change may take to reach the broker's metadata image.
const PROPAGATION: Duration = Duration::from_secs(10);

/// Which `BROKER` resource an alter names.
#[derive(Clone, Copy, Debug)]
enum Scope {
    /// The cluster-wide default, the empty resource name.
    ClusterDefault,
    /// The serving broker, by its node id.
    ThisBroker,
}

impl Scope {
    fn resource_name(self, node_id: u64) -> String {
        match self {
            Self::ClusterDefault => String::new(),
            Self::ThisBroker => node_id.to_string(),
        }
    }
}

/// Whether a new connection from this host is served: it answers an
/// `ApiVersions` round trip. The stream is returned so the caller can hold
/// the slot it takes.
async fn connect_served(addr: SocketAddr) -> Option<TcpStream> {
    connect_served_from(addr, None).await
}

/// [`connect_served`] from the loopback `source` address, or the default one.
async fn connect_served_from(addr: SocketAddr, source: Option<IpAddr>) -> Option<TcpStream> {
    let socket = TcpSocket::new_v4().ok()?;
    if let Some(ip) = source {
        socket.bind(SocketAddr::new(ip, 0)).ok()?;
    }
    let mut stream = socket.connect(addr).await.ok()?;
    kafka_wire::api_versions_answered(&mut stream, CLIENT_ID)
        .await
        .then_some(stream)
}

/// Poll until a new connection is served (`want`) or refused (`!want`), and
/// report whether that state was reached within [`PROPAGATION`]. A served
/// probe is closed at once, so it does not hold a slot.
async fn eventually_served(addr: SocketAddr, want: bool) -> bool {
    eventually_served_from(addr, None, want).await
}

/// [`eventually_served`] from the loopback `source` address.
async fn eventually_served_from(addr: SocketAddr, source: Option<IpAddr>, want: bool) -> bool {
    let deadline = Instant::now() + PROPAGATION;
    loop {
        if connect_served_from(addr, source).await.is_some() == want {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        // The alter is committed before the response, but the accept loop
        // reads the image the broker has applied, and a closed probe frees
        // its slot when its connection task sees the close. Neither has a
        // signal a test can wait on, so each iteration re-drives the real
        // connect and round trip.
        time::sleep(Duration::from_millis(50)).await;
    }
}

/// Send one `IncrementalAlterConfigs` for a `BROKER` resource on a connection
/// of its own, and return the resource's response row. The connection is
/// retried until the broker serves it, because a cap under test may still be
/// counting a connection the test has just closed.
#[bon::builder]
async fn alter_broker(
    addr: SocketAddr,
    resource_name: String,
    configs: Vec<(&str, ConfigOperation, Option<&str>)>,
    #[builder(default)] mutation: ConfigMutation,
) -> AlterConfigsResourceResponse {
    let request = broker_alter_request(resource_name, configs, mutation);
    let mut stream = hold_one(addr).await;
    let response: IncrementalAlterConfigsResponse = kafka_wire::exchange()
        .stream(&mut stream)
        .request(&request)
        .api_key(INCREMENTAL_ALTER_CONFIGS.0)
        .version(INCREMENTAL_ALTER_CONFIGS.1)
        .client_id(CLIENT_ID)
        .header(HeaderEncoding::Flexible)
        .call()
        .await
        .expect("IncrementalAlterConfigs round trip");
    response
        .responses
        .into_iter()
        .next()
        .expect("one resource row")
}

/// `DescribeConfigs` of one key of a `BROKER` resource, without synonyms or
/// documentation; `None` when the broker reports no such entry.
async fn describe_broker(
    addr: SocketAddr,
    resource_name: String,
    key: &str,
) -> Option<DescribeConfigsResourceResult> {
    let request = DescribeConfigsRequest {
        resources: vec![describe_resource(
            BROKER_RESOURCE,
            resource_name,
            Some(vec![key.to_owned()]),
        )],
        include_synonyms: false,
        include_documentation: false,
        ..Default::default()
    };
    let mut stream = hold_one(addr).await;
    let response: DescribeConfigsResponse = kafka_wire::exchange()
        .stream(&mut stream)
        .request(&request)
        .api_key(DESCRIBE_CONFIGS.0)
        .version(DESCRIBE_CONFIGS.1)
        .client_id(CLIENT_ID)
        .header(HeaderEncoding::Flexible)
        .call()
        .await
        .expect("DescribeConfigs round trip");
    let result = response
        .results
        .into_iter()
        .next()
        .expect("one resource result");
    assert!(result.error_code == codes::NONE, "{result:?}");
    result.configs.into_iter().find(|entry| entry.name == key)
}

/// A served connection to hold, retried until a slot is free.
async fn hold_one(addr: SocketAddr) -> TcpStream { hold_one_from(addr, None).await }

/// [`hold_one`] from the loopback `source` address.
async fn hold_one_from(addr: SocketAddr, source: Option<IpAddr>) -> TcpStream {
    let deadline = Instant::now() + PROPAGATION;
    loop {
        if let Some(stream) = connect_served_from(addr, source).await {
            return stream;
        }
        assert!(Instant::now() < deadline, "no connection slot came free");
        time::sleep(Duration::from_millis(50)).await;
    }
}

/// The response row of an alter that succeeded on `resource_name`.
fn accepted(resource_name: String) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        error_code: codes::NONE,
        error_message: None,
        resource_type: BROKER_RESOURCE,
        resource_name,
        ..Default::default()
    }
}

/// Hold two connections from `source`, and report whether a third reaches
/// the `served` state within [`PROPAGATION`].
async fn third_served_after_two(addr: SocketAddr, source: Option<IpAddr>, served: bool) -> bool {
    let held = (
        hold_one_from(addr, source).await,
        hold_one_from(addr, source).await,
    );
    let reached = eventually_served_from(addr, source, served).await;
    drop(held);
    reached
}

/// How long three connections, opened one after another, take to be served.
async fn three_connections(addr: SocketAddr) -> Duration {
    let start = Instant::now();
    for _ in 0..3 {
        drop(hold_one(addr).await);
    }
    start.elapsed()
}

use {
    krabka_broker::BrokerHandle,
    rstest_bdd::{Slot, datatable::Rows},
    rstest_bdd_macros::DataTableRow,
    tempfile::TempDir,
};
const OTHER_CLIENT: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
struct DynamicBroker {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    address: Option<SocketAddr>,
    resource: String,
    changed: Option<AlterConfigsResourceResponse>,
    described: Slot<Option<DescribeConfigsResourceResult>>,
    elapsed: Option<Duration>,
}
impl DynamicBroker {
    async fn alter_configs(
        &mut self,
        scope: &str,
        configs: Vec<(&str, ConfigOperation, Option<&str>)>,
        mutation: ConfigMutation,
    ) {
        self.resource = self.resource_name(scope);
        self.changed = Some(
            alter_broker()
                .addr(self.addr())
                .resource_name(self.resource.clone())
                .configs(configs)
                .mutation(mutation)
                .call()
                .await,
        );
    }

    fn addr(&self) -> SocketAddr { self.address.expect("dynamic broker started") }

    fn resource_name(&self, scope: &str) -> String {
        match scope {
            "default" => Scope::ClusterDefault.resource_name(
                self.broker
                    .as_ref()
                    .expect("dynamic broker started")
                    .node_id(),
            ),
            "this-broker" => Scope::ThisBroker.resource_name(
                self.broker
                    .as_ref()
                    .expect("dynamic broker started")
                    .node_id(),
            ),
            node => node.parse::<u64>().expect("broker resource id").to_string(),
        }
    }

    async fn shutdown(self) {
        self.broker
            .expect("dynamic broker started")
            .shutdown()
            .await;
    }
}
#[rstest::fixture]
fn dynamic() -> DynamicBroker {
    // Start this scenario with fresh inputs and observations.

    DynamicBroker {
        broker: None,
        directory: None,
        address: None,
        resource: String::new(),
        changed: None,
        described: Slot::default(),
        elapsed: None,
    }
}
#[rstest_bdd_macros::given("a running plaintext broker has its default static connection settings")]
async fn dynamic_broker_defaults(dynamic: &mut DynamicBroker) {
    let (broker, directory, address) = start_plaintext_configured(|_| {}).await;
    dynamic.broker = Some(broker);
    dynamic.directory = Some(directory);
    dynamic.address = Some(address);
}
#[rstest_bdd_macros::given("a plaintext broker starts with static per-IP cap {cap}")]
async fn dynamic_broker_static_cap(dynamic: &mut DynamicBroker, cap: usize) {
    let (broker, directory, address) = start_plaintext_configured(|config| {
        config.max_connections_per_ip = cap;
    })
    .await;
    dynamic.broker = Some(broker);
    dynamic.directory = Some(directory);
    dynamic.address = Some(address);
}
#[rstest_bdd_macros::when(
    "config {key} on resource {scope} is set to {value} with validate only {validate}"
)]
async fn set_live_broker_config(
    dynamic: &mut DynamicBroker,
    key: String,
    scope: String,
    value: String,
    validate: bool,
) {
    dynamic
        .alter_configs(
            &scope,
            vec![(&key, ConfigOperation::Set, Some(&value))],
            if validate {
                ConfigMutation::Validate
            } else {
                ConfigMutation::Apply
            },
        )
        .await;
}
#[rstest_bdd_macros::when("config {key} is deleted from resource {scope}")]
async fn delete_live_broker_config(dynamic: &mut DynamicBroker, key: String, scope: String) {
    dynamic
        .alter_configs(
            &scope,
            vec![(&key, ConfigOperation::Delete, None)],
            ConfigMutation::Apply,
        )
        .await;
}
#[derive(DataTableRow)]
struct BrokerMutation {
    key: String,
    operation: String,
    value: String,
}
#[rstest_bdd_macros::when("broker resource {scope} receives these config mutations:")]
async fn mutate_live_broker_config(
    dynamic: &mut DynamicBroker,
    scope: String,
    #[datatable] rows: Rows<BrokerMutation>,
) {
    let rows: Vec<_> = rows.into_iter().collect();
    let configs = rows
        .iter()
        .map(|row| match row.operation.as_str() {
            "set" => (
                row.key.as_str(),
                ConfigOperation::Set,
                Some(row.value.as_str()),
            ),
            "delete" => (row.key.as_str(), ConfigOperation::Delete, None),
            other => panic!("unknown config operation {other}"),
        })
        .collect();
    dynamic
        .alter_configs(&scope, configs, ConfigMutation::Apply)
        .await;
}
#[rstest_bdd_macros::then(
    "the complete broker config change response is successful for the requested resource"
)]
fn broker_config_accepted(dynamic: &DynamicBroker) {
    assert!(
        dynamic.changed.as_ref().expect("config mutation attempted")
            == &accepted(dynamic.resource.clone())
    );
}
#[rstest_bdd_macros::then(
    "the complete broker config change response has error 42 and message {message}"
)]
fn broker_config_refused(dynamic: &DynamicBroker, message: String) {
    assert!(
        dynamic.changed.as_ref().expect("config mutation attempted")
            == &AlterConfigsResourceResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some(message),
                ..accepted(dynamic.resource.clone())
            }
    );
}
#[rstest_bdd_macros::then(
    "with two held {listener} connections from {source} a third is eventually served {served}"
)]
async fn live_broker_third_connection(
    dynamic: &DynamicBroker,
    listener: String,
    source: String,
    served: bool,
) {
    let addr = match listener.as_str() {
        "data" => dynamic.addr(),
        "controller" => dynamic
            .broker
            .as_ref()
            .expect("dynamic broker started")
            .controller_addr(),
        other => panic!("unknown listener {other}"),
    };
    let source = if source == "default" {
        None
    } else {
        Some(source.parse::<IpAddr>().expect("client source IP"))
    };
    assert!(third_served_after_two(addr, source, served).await);
}
#[rstest_bdd_macros::then(
    "with {held_count} held data connections the next is immediately served {served}"
)]
async fn live_broker_next_connection(dynamic: &DynamicBroker, held_count: usize, served: bool) {
    let mut held = Vec::new();
    for _ in 0..held_count {
        held.push(hold_one(dynamic.addr()).await);
    }
    assert!(connect_served(dynamic.addr()).await.is_some() == served);
    drop(held);
}
#[rstest_bdd_macros::then("a new data connection from 127.0.0.2 is eventually served {served}")]
async fn live_broker_other_ip(dynamic: &DynamicBroker, served: bool) {
    assert!(eventually_served_from(dynamic.addr(), Some(OTHER_CLIENT), served).await);
}
#[rstest_bdd_macros::then(
    "two held default-source connections allow both another default-source and a 127.0.0.2 connection"
)]
async fn restored_broker_source_limits(dynamic: &DynamicBroker) {
    let addr = dynamic.addr();
    let held = (hold_one(addr).await, hold_one(addr).await);
    assert!(eventually_served(addr, true).await);
    assert!(eventually_served_from(addr, Some(OTHER_CLIENT), true).await);
    drop(held);
}
#[rstest_bdd_macros::when("the client describes config {key} on resource {scope}")]
async fn describe_live_broker_config(dynamic: &mut DynamicBroker, key: String, scope: String) {
    dynamic
        .described
        .set(describe_broker(dynamic.addr(), dynamic.resource_name(&scope), &key).await);
}
#[rstest_bdd_macros::then("the described config entry is absent")]
fn unstored_broker_config(dynamic: &DynamicBroker) {
    assert!(dynamic.described.get().expect("config described").is_none());
}
#[rstest_bdd_macros::then("the described config value is {value}")]
fn stored_broker_config_value(dynamic: &DynamicBroker, value: String) {
    assert!(
        dynamic
            .described
            .get()
            .expect("config described")
            .and_then(|entry| entry.value)
            .as_deref()
            == Some(value.as_str())
    );
}
#[derive(DataTableRow)]
struct ExpectedBrokerConfig {
    name: String,
    value: String,
    source: i8,
    sensitive: bool,
    kind: i8,
    read_only: bool,
}
#[rstest_bdd_macros::then("the complete described config entry has these fields:")]
fn live_broker_config_fields(
    dynamic: &DynamicBroker,
    #[datatable] rows: Rows<ExpectedBrokerConfig>,
) {
    let rows: Vec<_> = rows.into_iter().collect();
    let [row] = rows.as_slice() else {
        panic!("one expected config entry")
    };
    let expected = DescribeConfigsResourceResult {
        name: row.name.clone(),
        value: if row.value == "redacted" {
            None
        } else {
            Some(row.value.clone())
        },
        config_source: row.source,
        is_sensitive: row.sensitive,
        config_type: row.kind,
        read_only: row.read_only,
        synonyms: Vec::new(),
        documentation: None,
        ..Default::default()
    };
    assert!(dynamic.described.get().expect("config described") == Some(expected));
}
#[rstest_bdd_macros::when(
    "three consecutive data connections are timed until the creation rate propagates"
)]
async fn measure_live_creation_rate(dynamic: &mut DynamicBroker) {
    let deadline = Instant::now() + PROPAGATION;
    loop {
        let elapsed = three_connections(dynamic.addr()).await;
        dynamic.elapsed = Some(elapsed);
        if elapsed >= Duration::from_millis(1500) || Instant::now() >= deadline {
            break;
        }
    }
}
#[rstest_bdd_macros::then("serving three connections took at least 1500 milliseconds")]
fn live_creation_rate_delayed(dynamic: &DynamicBroker) {
    assert!(dynamic.elapsed.expect("connections timed") >= Duration::from_millis(1500));
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "connection cap applies and lifts without restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_cap_applies_and_lifts_without_restart(
    dynamic: DynamicBroker,
    key: String,
    scope: String,
) {
    let _ = (key, scope);
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "per broker connection cap overrides the cluster default"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_broker_connection_cap_overrides_the_cluster_default(dynamic: DynamicBroker) {
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "static connection cap holds without a dynamic value"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_connection_cap_holds_without_a_dynamic_value(dynamic: DynamicBroker) {
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "invalid broker config changes are refused with kafkas errors"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_broker_config_changes_are_refused_with_kafkas_errors(
    dynamic: DynamicBroker,
    scope: String,
    key: String,
    value: String,
    validate: bool,
    message: String,
) {
    let _ = (scope, key, value, validate, message);
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "validate only broker change has no effect"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validate_only_broker_change_has_no_effect(dynamic: DynamicBroker) {
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "describe reports dynamic values and redacts passwords"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_reports_dynamic_values_and_redacts_passwords(dynamic: DynamicBroker) {
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "per ip cap applies to the controller listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_ip_cap_applies_to_the_controller_listener(dynamic: DynamicBroker) {
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "zero per ip cap without overrides is stored but not applied"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_per_ip_cap_without_overrides_is_stored_but_not_applied(dynamic: DynamicBroker) {
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "per ip overrides apply and lift without restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_ip_overrides_apply_and_lift_without_restart(dynamic: DynamicBroker) {
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "listener cap applies and lifts without restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_cap_applies_and_lifts_without_restart(dynamic: DynamicBroker) {
    dynamic.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_broker_config.feature",
    name = "creation rate holds the acceptor without restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creation_rate_holds_the_acceptor_without_restart(dynamic: DynamicBroker, key: String) {
    let _ = key;
    dynamic.shutdown().await;
}

/// Exercise the actual alter/describe handlers, not only the typed roster.
#[tokio::test]
async fn dlq_configs_alter_describe_reset_and_reject_invalid_values() {
    let (broker, directory, addr) = start_plaintext_configured(|config| {
        config.features.unstable_api_versions =
            krabka_broker::api_catalog::UnstableApiVersions::Enabled;
    })
    .await;
    let local = broker.node_id().to_string();
    for (key, default, cluster, own, kind) in [
        (
            "errors.deadletterqueue.auto.create.topics.enable",
            "false",
            "true",
            "false",
            1,
        ),
        (
            "errors.deadletterqueue.topic.name.prefix",
            "dlq.",
            "cluster.",
            "",
            2,
        ),
    ] {
        for (resource, operation, value, expected) in [
            (String::new(), ConfigOperation::Set, Some(cluster), cluster),
            (local.clone(), ConfigOperation::Set, Some(own), own),
            (local.clone(), ConfigOperation::Delete, None, cluster),
            (String::new(), ConfigOperation::Delete, None, default),
        ] {
            let changed = alter_broker()
                .addr(addr)
                .resource_name(resource)
                .configs(vec![(key, operation, value)])
                .call()
                .await;
            assert!(changed.error_code == codes::NONE);
            let entry = describe_broker(addr, local.clone(), key)
                .await
                .expect("DLQ key");
            assert!(entry.value.as_deref() == Some(expected));
            assert!(entry.config_type == kind);
            assert!(!entry.read_only && !entry.is_sensitive);
        }
    }
    let invalid = alter_broker()
        .addr(addr)
        .resource_name(local.clone())
        .configs(vec![(
            "errors.deadletterqueue.auto.create.topics.enable",
            ConfigOperation::Set,
            Some("yes"),
        )])
        .call()
        .await;
    assert!(invalid.error_code == codes::INVALID_REQUEST);
    let scalar_append = alter_broker()
        .addr(addr)
        .resource_name(local)
        .configs(vec![(
            "errors.deadletterqueue.topic.name.prefix",
            ConfigOperation::Append,
            Some("x"),
        )])
        .call()
        .await;
    assert!(scalar_append.error_code == codes::INVALID_REQUEST);
    let persisted = alter_broker()
        .addr(addr)
        .resource_name(String::new())
        .configs(vec![
            (
                "errors.deadletterqueue.auto.create.topics.enable",
                ConfigOperation::Set,
                Some("true"),
            ),
            (
                "errors.deadletterqueue.topic.name.prefix",
                ConfigOperation::Set,
                Some("persisted."),
            ),
        ])
        .call()
        .await;
    assert!(persisted.error_code == codes::NONE);
    broker.shutdown().await;
    let mut config = support::heartbeat_config(directory.path());
    config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    config.features.unstable_api_versions =
        krabka_broker::api_catalog::UnstableApiVersions::Enabled;
    let restarted = krabka_broker::Broker::start(config)
        .await
        .expect("restart broker");
    for (key, value) in [
        ("errors.deadletterqueue.auto.create.topics.enable", "true"),
        ("errors.deadletterqueue.topic.name.prefix", "persisted."),
    ] {
        let entry = describe_broker(
            restarted.listen_addr(),
            restarted.node_id().to_string(),
            key,
        )
        .await
        .expect("persisted DLQ config");
        assert!(entry.value.as_deref() == Some(value));
    }
    restarted.shutdown().await;
}
