//! Dynamic broker configuration that a running broker applies without a
//! restart.
//!
//! Each case starts one broker with no connection cap, alters a dynamic
//! `BROKER` config with `IncrementalAlterConfigs`, and observes the effect on
//! the next connection the broker accepts, as Kafka 4.3.1's
//! `DynamicListenerConfig` and `ConnectionQuotas` apply it. Deleting the
//! config restores the static value, again without a restart.

mod kafka_wire;
mod support;

use {
    crate::support::{
        configs::{
            ConfigMutation, ConfigOperation, describe_resource, incremental_config,
            incremental_request, incremental_resource,
        },
        sasl::start_plaintext_configured,
        wire::HeaderEncoding,
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_protocol::{
        Encode,
        owned::{
            api_versions_request::ApiVersionsRequest,
            describe_configs_request::DescribeConfigsRequest,
            describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResponse},
            incremental_alter_configs_response::{
                AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
            },
        },
    },
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::{Duration, Instant},
    },
    tokio::net::{TcpSocket, TcpStream},
};

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-dynamic-broker-config-test";

/// Kafka's `ConfigResource.Type.BROKER`.
const BROKER_RESOURCE: i8 = 4;

/// `IncrementalAlterConfigs`' API key and the flexible version this suite
/// sends.
const INCREMENTAL_ALTER_CONFIGS: (i16, i16) = (44, 1);

/// `DescribeConfigs`' API key and the flexible version this suite sends.
const DESCRIBE_CONFIGS: (i16, i16) = (32, 4);

/// Kafka's `ConfigSource` codes.
const DYNAMIC_BROKER_CONFIG: i8 = 2;
const DYNAMIC_DEFAULT_BROKER_CONFIG: i8 = 3;

/// Kafka's `ConfigType` codes.
const INT: i8 = 3;
const PASSWORD: i8 = 9;

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
    let mut body = BytesMut::new();
    ApiVersionsRequest::default()
        .encode(&mut body, 0)
        .expect("encode ApiVersions");
    let socket = TcpSocket::new_v4().ok()?;
    if let Some(ip) = source {
        socket.bind(SocketAddr::new(ip, 0)).ok()?;
    }
    let mut stream = socket.connect(addr).await.ok()?;
    kafka_wire::round_trip()
        .stream(&mut stream)
        .client_id(CLIENT_ID)
        .body(&body)
        .call()
        .await
        .ok()
        .map(|_| stream)
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
        tokio::time::sleep(Duration::from_millis(50)).await;
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
    let request = incremental_request()
        .resources(vec![incremental_resource(
            BROKER_RESOURCE,
            resource_name,
            configs
                .into_iter()
                .map(|(name, operation, value)| {
                    incremental_config(name, value.map(ToOwned::to_owned), operation)
                })
                .collect(),
        )])
        .mutation(mutation)
        .call();
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
    assert!(
        result.error_code == krabka_broker::codes::NONE,
        "{result:?}"
    );
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The response row of an alter that succeeded on `resource_name`.
fn accepted(resource_name: String) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        error_code: krabka_broker::codes::NONE,
        error_message: None,
        resource_type: BROKER_RESOURCE,
        resource_name,
        ..Default::default()
    }
}

/// `max.connections` and `max.connections.per.ip` set to 2, on either
/// resource, refuse a third connection while two are held, and deleting the
/// config serves it again. Kafka's `ConnectionQuotas.updateBrokerMaxConnections`
/// and `updateMaxConnectionsPerIp` apply both at once. The cap is 2 rather
/// than 1 so the alter that deletes it can still connect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_cap_applies_and_lifts_without_restart() {
    let (handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    let cases = [
        ("max.connections", Scope::ClusterDefault),
        ("max.connections", Scope::ThisBroker),
        ("max.connections.per.ip", Scope::ClusterDefault),
        ("max.connections.per.ip", Scope::ThisBroker),
    ];
    for (key, scope) in cases {
        let resource = scope.resource_name(handle.node_id());

        let set = alter_broker()
            .addr(addr)
            .resource_name(resource.clone())
            .configs(vec![(key, ConfigOperation::Set, Some("2"))])
            .call()
            .await;
        assert!(set == accepted(resource.clone()));
        let held = (hold_one(addr).await, hold_one(addr).await);
        assert!(
            eventually_served(addr, false).await,
            "{key}=2 on {scope:?} must refuse a third connection while two are held"
        );
        drop(held);

        let deleted = alter_broker()
            .addr(addr)
            .resource_name(resource.clone())
            .configs(vec![(key, ConfigOperation::Delete, None)])
            .call()
            .await;
        assert!(deleted == accepted(resource));
        let held = (hold_one(addr).await, hold_one(addr).await);
        assert!(
            eventually_served(addr, true).await,
            "deleting {key} on {scope:?} must lift the cap"
        );
        drop(held);
    }
}

/// A per-broker value wins over the cluster default, as Kafka's
/// `DynamicBrokerConfig` layers `DYNAMIC_BROKER_CONFIG` over
/// `DYNAMIC_DEFAULT_BROKER_CONFIG`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_broker_connection_cap_overrides_the_cluster_default() {
    let (handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    let this_broker = Scope::ThisBroker.resource_name(handle.node_id());

    let default = alter_broker()
        .addr(addr)
        .resource_name(String::new())
        .configs(vec![(
            "max.connections.per.ip",
            ConfigOperation::Set,
            Some("2"),
        )])
        .call()
        .await;
    assert!(default == accepted(String::new()));
    let held = (hold_one(addr).await, hold_one(addr).await);
    assert!(eventually_served(addr, false).await);
    drop(held);

    let per_broker = alter_broker()
        .addr(addr)
        .resource_name(this_broker.clone())
        .configs(vec![(
            "max.connections.per.ip",
            ConfigOperation::Set,
            Some("100"),
        )])
        .call()
        .await;
    assert!(per_broker == accepted(this_broker));
    let held = (hold_one(addr).await, hold_one(addr).await);
    assert!(
        eventually_served(addr, true).await,
        "the per-broker 100 must win over the cluster default 2"
    );
    drop(held);
}

/// A dynamic cap the operator never set leaves the static one in force: a
/// broker started with `max.connections.per.ip = 1` still refuses a second
/// connection after a dynamic change to the other cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_connection_cap_holds_without_a_dynamic_value() {
    let (_handle, _dir, addr) = start_plaintext_configured(|config| {
        config.max_connections_per_ip = 1;
    })
    .await;

    let set = alter_broker()
        .addr(addr)
        .resource_name(String::new())
        .configs(vec![(
            "max.connections",
            ConfigOperation::Set,
            Some("1000"),
        )])
        .call()
        .await;
    assert!(set == accepted(String::new()));

    let held = hold_one(addr).await;
    assert!(
        connect_served(addr).await.is_none(),
        "the static max.connections.per.ip=1 still applies"
    );
    drop(held);
}

/// Kafka's `DynamicBrokerConfig.validateConfigs` refusals, on the wire: each
/// answers `INVALID_REQUEST` (42) with Kafka's message, `validate_only` or
/// not, and none of them is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_broker_config_changes_are_refused_with_kafkas_errors() {
    let (handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    let this_broker = Scope::ThisBroker.resource_name(handle.node_id());
    let cases = [
        (
            String::new(),
            "log.dirs",
            "/tmp",
            "Cannot update these configs dynamically: [log.dirs]".to_owned(),
        ),
        (
            this_broker.clone(),
            "ssl.keystore.location",
            "/k",
            "These security configs can be dynamically updated only per-listener using the \
             listener prefix: [ssl.keystore.location]"
                .to_owned(),
        ),
        (
            String::new(),
            "listener.name.plaintext.ssl.keystore.location",
            "/k",
            "Cannot update these configs at default cluster level, broker id must be \
             specified: [listener.name.plaintext.ssl.keystore.location]"
                .to_owned(),
        ),
        (
            String::new(),
            "max.connections",
            "-5",
            "Invalid value -5 for configuration max.connections: Value must be at least 0"
                .to_owned(),
        ),
        (
            this_broker.clone(),
            "max.connections.per.ip",
            "many",
            "Invalid value many for configuration max.connections.per.ip: Not a number of type \
             INT"
            .to_owned(),
        ),
        (
            "7".to_owned(),
            "max.connections",
            "1",
            format!(
                "Unexpected broker id, expected {}, but received 7",
                handle.node_id()
            ),
        ),
    ];
    for mutation in [ConfigMutation::Validate, ConfigMutation::Apply] {
        for (resource, key, value, message) in &cases {
            let row = alter_broker()
                .addr(addr)
                .resource_name(resource.clone())
                .configs(vec![(key, ConfigOperation::Set, Some(value))])
                .mutation(mutation)
                .call()
                .await;
            assert!(
                row == AlterConfigsResourceResponse {
                    error_code: krabka_broker::codes::INVALID_REQUEST,
                    error_message: Some(message.clone()),
                    ..accepted(resource.clone())
                },
                "{key}={value} on {resource:?}"
            );
        }
    }
    let held = (hold_one(addr).await, hold_one(addr).await);
    assert!(
        connect_served(addr).await.is_some(),
        "no refused cap was stored"
    );
    drop(held);
}

/// `validate_only` answers as the alter would and changes nothing: the cap it
/// validated never applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validate_only_broker_change_has_no_effect() {
    let (_handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    let row = alter_broker()
        .addr(addr)
        .resource_name(String::new())
        .configs(vec![(
            "max.connections.per.ip",
            ConfigOperation::Set,
            Some("2"),
        )])
        .mutation(ConfigMutation::Validate)
        .call()
        .await;
    assert!(row == accepted(String::new()));
    let held = (hold_one(addr).await, hold_one(addr).await);
    assert!(
        connect_served(addr).await.is_some(),
        "a validate_only cap must not apply"
    );
    drop(held);
    assert!(
        describe_broker(addr, String::new(), "max.connections.per.ip")
            .await
            .is_none(),
        "a validate_only cap must not be stored"
    );
}

/// `DescribeConfigs` withholds a dynamic `PASSWORD` value, as Kafka's
/// `KafkaConfig.maybeSensitive` does: the value is null, `is_sensitive` is
/// set, and the type is `PASSWORD`. A non-sensitive dynamic value is
/// reported with its source. Kafka's `ConfigHelper.createBrokerConfigEntry`
/// reads `read_only` as `!ALL_DYNAMIC_CONFIGS.contains(name)` on the exact
/// name, so a listener-prefixed key reports `read_only` although it was just
/// altered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_reports_dynamic_values_and_redacts_passwords() {
    let (handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    let this_broker = Scope::ThisBroker.resource_name(handle.node_id());
    let password = "listener.name.plaintext.ssl.keystore.password";
    let set = alter_broker()
        .addr(addr)
        .resource_name(this_broker.clone())
        .configs(vec![
            (password, ConfigOperation::Set, Some("s3cret")),
            ("max.connections.per.ip", ConfigOperation::Set, Some("50")),
        ])
        .call()
        .await;
    assert!(set == accepted(this_broker.clone()));
    let set = alter_broker()
        .addr(addr)
        .resource_name(String::new())
        .configs(vec![("max.connections", ConfigOperation::Set, Some("500"))])
        .call()
        .await;
    assert!(set == accepted(String::new()));

    let entry = |name: &str, value: Option<&str>, source: i8, sensitive: bool, kind: i8| {
        DescribeConfigsResourceResult {
            name: name.to_owned(),
            value: value.map(ToOwned::to_owned),
            read_only: name.starts_with("listener.name."),
            config_source: source,
            is_sensitive: sensitive,
            synonyms: Vec::new(),
            config_type: kind,
            documentation: None,
            ..Default::default()
        }
    };
    let cases = [
        (
            this_broker.clone(),
            password,
            entry(password, None, DYNAMIC_BROKER_CONFIG, true, PASSWORD),
        ),
        (
            this_broker.clone(),
            "max.connections.per.ip",
            entry(
                "max.connections.per.ip",
                Some("50"),
                DYNAMIC_BROKER_CONFIG,
                false,
                INT,
            ),
        ),
        (
            this_broker,
            "max.connections",
            entry(
                "max.connections",
                Some("500"),
                DYNAMIC_DEFAULT_BROKER_CONFIG,
                false,
                INT,
            ),
        ),
        (
            String::new(),
            "max.connections",
            entry(
                "max.connections",
                Some("500"),
                DYNAMIC_DEFAULT_BROKER_CONFIG,
                false,
                INT,
            ),
        ),
    ];
    for (resource, key, want) in cases {
        let got = describe_broker(addr, resource.clone(), key).await;
        assert!(got == Some(want), "{key} on {resource:?}: {got:#?}");
    }
}

/// Alter the cluster-default `BROKER` resource and check the alter was
/// accepted.
async fn alter_cluster_default(
    addr: SocketAddr,
    configs: Vec<(&str, ConfigOperation, Option<&str>)>,
) {
    let row = alter_broker()
        .addr(addr)
        .resource_name(String::new())
        .configs(configs)
        .call()
        .await;
    assert!(row == accepted(String::new()));
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

/// The controller listener of a combined node takes the same dynamic caps,
/// as Kafka's `DynamicBrokerConfig.addReconfigurables(controller)` registers
/// the controller's `SocketServer`. The node's own raft and forwarding
/// connections reach that listener from 127.0.0.1, so the probes come from
/// 127.0.0.2 and the per-IP cap counts them alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_ip_cap_applies_to_the_controller_listener() {
    let (handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    let controller = handle.controller_addr();
    let source = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));

    alter_cluster_default(addr, vec![(
        "max.connections.per.ip",
        ConfigOperation::Set,
        Some("2"),
    )])
    .await;
    assert!(
        third_served_after_two(controller, source, false).await,
        "max.connections.per.ip=2 must refuse a third controller connection while two are held"
    );

    alter_cluster_default(addr, vec![(
        "max.connections.per.ip",
        ConfigOperation::Delete,
        None,
    )])
    .await;
    assert!(
        third_served_after_two(controller, source, true).await,
        "deleting max.connections.per.ip must lift the controller listener's cap"
    );
}

/// `max.connections.per.ip=0` is stored, as Kafka's controller validates
/// only `atLeast(0)`, but not applied: `KafkaConfig.validateValues` allows 0
/// only beside `max.connections.per.ip.overrides`, so Kafka's broker keeps
/// its previous config. Applying it would refuse every client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_per_ip_cap_without_overrides_is_stored_but_not_applied() {
    let (_handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    alter_cluster_default(addr, vec![(
        "max.connections.per.ip",
        ConfigOperation::Set,
        Some("0"),
    )])
    .await;
    let stored = describe_broker(addr, String::new(), "max.connections.per.ip")
        .await
        .and_then(|entry| entry.value);
    assert!(stored.as_deref() == Some("0"));
    let held = hold_one(addr).await;
    assert!(
        connect_served(addr).await.is_some(),
        "a zero per-IP cap must not refuse clients"
    );
    drop(held);
}

/// The loopback address the override tests treat as a second client.
const OTHER_CLIENT: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

/// `max.connections.per.ip.overrides` gives 127.0.0.1 its own cap beside a
/// default of 0, which Kafka allows only with overrides: 127.0.0.1 gets two
/// connections and 127.0.0.2 none. Deleting both restores the static caps.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_ip_overrides_apply_and_lift_without_restart() {
    let (_handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    alter_cluster_default(addr, vec![
        ("max.connections.per.ip", ConfigOperation::Set, Some("0")),
        (
            "max.connections.per.ip.overrides",
            ConfigOperation::Set,
            Some("127.0.0.1:2"),
        ),
    ])
    .await;
    assert!(
        eventually_served_from(addr, Some(OTHER_CLIENT), false).await,
        "127.0.0.2 has no override, so the default of 0 must refuse it"
    );
    assert!(
        third_served_after_two(addr, None, false).await,
        "the override of 2 must refuse a third connection from 127.0.0.1"
    );

    alter_cluster_default(addr, vec![
        ("max.connections.per.ip", ConfigOperation::Delete, None),
        (
            "max.connections.per.ip.overrides",
            ConfigOperation::Delete,
            None,
        ),
    ])
    .await;
    let held = (hold_one(addr).await, hold_one(addr).await);
    assert!(eventually_served(addr, true).await);
    assert!(eventually_served_from(addr, Some(OTHER_CLIENT), true).await);
    drop(held);
}

/// `listener.name.plaintext.max.connections`, which Kafka allows on the
/// cluster default, caps the listener on its own, below an unlimited
/// broker-wide `max.connections`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_cap_applies_and_lifts_without_restart() {
    let (_handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    let key = "listener.name.plaintext.max.connections";
    alter_cluster_default(addr, vec![(key, ConfigOperation::Set, Some("2"))]).await;
    assert!(
        third_served_after_two(addr, None, false).await,
        "{key}=2 must refuse a third connection while two are held"
    );

    alter_cluster_default(addr, vec![(key, ConfigOperation::Delete, None)]).await;
    assert!(
        third_served_after_two(addr, None, true).await,
        "deleting {key} must lift the cap"
    );
}

/// How long three connections, opened one after another, take to be served.
async fn three_connections(addr: SocketAddr) -> Duration {
    let start = Instant::now();
    for _ in 0..3 {
        drop(hold_one(addr).await);
    }
    start.elapsed()
}

/// A creation rate of 1 per second, broker-wide or for the listener, holds
/// the acceptor between connections: three connections take at least a
/// second and a half where they took milliseconds. The bound is a lower one,
/// which a slow runner only makes easier to meet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creation_rate_holds_the_acceptor_without_restart() {
    let (_handle, _dir, addr) = start_plaintext_configured(|_| {}).await;
    for key in [
        "max.connection.creation.rate",
        "listener.name.plaintext.max.connection.creation.rate",
    ] {
        alter_cluster_default(addr, vec![(key, ConfigOperation::Set, Some("1"))]).await;
        let deadline = Instant::now() + PROPAGATION;
        let mut throttled = false;
        while !throttled && Instant::now() < deadline {
            throttled = three_connections(addr).await >= Duration::from_millis(1500);
        }
        assert!(throttled, "{key}=1 must space connections a second apart");

        alter_cluster_default(addr, vec![(key, ConfigOperation::Delete, None)]).await;
    }
}
