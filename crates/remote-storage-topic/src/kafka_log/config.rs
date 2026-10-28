//! Construction-time configuration for the Kafka-topic-backed metadata event
//! log.
//!
//! This module holds the topic name, the Apache Kafka defaults for partition
//! count and replication factor, the transport tunables of the producer and of
//! the per-partition fetch loops, and the validation that keeps every tunable
//! inside the `int32` millisecond and byte ranges the Kafka wire carries.

use {
    krabka_client_core::{
        ClientFrameMax, ConnectionDispatchQueueCapacity, security::ClientSecurity,
    },
    krabka_macros::RefinedNewtype,
    krabka_units::prelude::{
        ByteSize, ByteSizeExt as _, Time, TimeExt as _, mebibytes, millis, secs,
    },
    refined_type::rule::{GreaterI32, GreaterUsize},
    std::time::Duration,
};

/// Default name of the internal metadata topic.
pub const METADATA_TOPIC: &str = "__remote_log_metadata";

/// Default partition count for `__remote_log_metadata`, matching
/// Apache Kafka's `remote.log.metadata.topic.num.partitions`.
pub const DEFAULT_NUM_PARTITIONS: i32 = 50;

/// Default replication factor for `__remote_log_metadata`, matching
/// Apache Kafka's `remote.log.metadata.topic.replication.factor`.
pub const DEFAULT_REPLICATION: i32 = 3;

/// Default `min.insync.replicas` for `__remote_log_metadata`, matching
/// Apache Kafka's `remote.log.metadata.topic.min.isr`
/// (`TopicBasedRemoteLogMetadataManagerConfig.DEFAULT_REMOTE_LOG_METADATA_TOPIC_MIN_ISR`).
pub const DEFAULT_MIN_ISR: i32 = 2;

/// How long `CreateTopics` may take to provision `__remote_log_metadata`
/// before the broker abandons the round-trip.
pub const DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT: Time = secs(30);

/// `max_wait_ms` for the per-partition metadata `Fetch`. It is long enough
/// that an idle partition costs one RPC per interval rather than a spin, and
/// short enough that cancellation on reassignment is prompt.
pub const DEFAULT_METADATA_FETCH_MAX_WAIT: Time = millis(500);

/// Per-partition budget for the metadata `Fetch`. Metadata events are small,
/// so one mebibyte is many thousands of them per round-trip.
pub const DEFAULT_METADATA_FETCH_MAX_BYTES: ByteSize = mebibytes(1);

/// Pause before a retry of a failed metadata `Fetch`, so a broker that is down
/// does not turn the fetch loop into a busy spin.
pub const DEFAULT_METADATA_FETCH_RETRY_BACKOFF: Time = millis(200);

/// Default capacity of the shared metadata-event delivery queue.
pub const DEFAULT_METADATA_EVENT_QUEUE_CAPACITY: usize = 1024;

/// Positive capacity of the shared metadata-event delivery queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(GreaterUsize<0>),
    string_error,
    label = "metadata event queue capacity",
    getter = capacity,
    default = DEFAULT_METADATA_EVENT_QUEUE_CAPACITY
)]
pub struct MetadataEventQueueCapacity(usize);

/// Construction-time configuration for
/// [`KafkaMetadataEventLog`](super::KafkaMetadataEventLog).
#[derive(Debug, Clone)]
pub struct KafkaMetadataLogConfig {
    /// `host:port` for the Kafka client to bootstrap from. The TBRLMM in a
    /// broker connects over loopback to its own listener.
    pub bootstrap: String,
    /// Internal topic name. Production deployments keep the default. The
    /// field exists so multiple isolated clusters can share an environment in
    /// tests.
    pub topic: String,
    /// Number of partitions to create the topic with on first startup.
    /// The log ignores this value when the topic already exists, and the
    /// existing count wins. The log does not support re-bucketing on
    /// partition growth.
    pub num_partitions: i32,
    /// Replication factor to create the topic with on first startup.
    /// The log ignores this value when the topic already exists.
    pub replication: i32,
    /// `min.insync.replicas` to create the topic with on first startup.
    /// `None` leaves the setting to the cluster default. The log ignores this
    /// value when the topic already exists.
    pub min_isr: Option<i32>,
    /// `client_id` for the producer and consumer. It is diagnostic only.
    pub client_id: String,
    /// Provision and maintain this internal topic with log compaction.
    pub compacted: bool,
    /// Create the topic when it is absent. Read-only tooling disables this.
    pub provision_topic: bool,
    /// Client TLS/SASL security applied to the producer, the raw client,
    /// the admin client, and every per-partition fetch connection.
    /// `None` is plaintext loopback, and it is the default.
    pub security: Option<ClientSecurity>,
    /// Timeout for provisioning the internal topic.
    pub topic_create_timeout: Time,
    /// Maximum wait for each per-partition metadata fetch.
    pub fetch_max_wait: Time,
    /// Maximum bytes returned by each per-partition metadata fetch.
    pub fetch_max_bytes: ByteSize,
    /// Backoff after a failed metadata fetch.
    pub fetch_retry_backoff: Time,
    /// Capacity of the shared metadata-event delivery queue.
    pub event_queue_capacity: MetadataEventQueueCapacity,
    /// Capacity of every outbound Kafka connection's dispatch queue.
    pub dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    /// Maximum frame size for every outbound Kafka connection.
    pub frame_max: ClientFrameMax,
}

impl KafkaMetadataLogConfig {
    /// Construct a config with the conventional Kafka defaults.
    #[must_use]
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            topic: METADATA_TOPIC.to_owned(),
            num_partitions: DEFAULT_NUM_PARTITIONS,
            replication: DEFAULT_REPLICATION,
            min_isr: Some(DEFAULT_MIN_ISR),
            client_id: "krabka-rlmm".to_owned(),
            compacted: false,
            provision_topic: true,
            security: None,
            topic_create_timeout: DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT,
            fetch_max_wait: DEFAULT_METADATA_FETCH_MAX_WAIT,
            fetch_max_bytes: DEFAULT_METADATA_FETCH_MAX_BYTES,
            fetch_retry_backoff: DEFAULT_METADATA_FETCH_RETRY_BACKOFF,
            event_queue_capacity: MetadataEventQueueCapacity::default(),
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
            frame_max: ClientFrameMax::default(),
        }
    }

    /// Validate values that cross into the Kafka wire client.
    ///
    /// # Errors
    ///
    /// Returns an error for non-positive, non-finite, fractional, or
    /// out-of-range wire values, and for a `min_isr` below one, as Kafka's
    /// `remote.log.metadata.topic.min.isr` does (`atLeast(1)`).
    pub fn validate(&self) -> Result<(), String> {
        if let Some(min_isr) = self.min_isr {
            GreaterI32::<0>::new(min_isr).map_err(|error| format!("min_isr: {error}"))?;
        }
        validate_positive_whole_millis_i32("topic_create_timeout", self.topic_create_timeout)?;
        validate_positive_whole_millis_i32("fetch_max_wait", self.fetch_max_wait)?;
        validate_positive_whole_bytes_i32("fetch_max_bytes", self.fetch_max_bytes)?;
        validate_positive_duration("fetch_retry_backoff", self.fetch_retry_backoff)
    }
}

fn validate_positive_whole_millis_i32(name: &str, value: Time) -> Result<(), String> {
    let millis = value.millis_i64();
    if !value.secs_f64().is_finite() || Time::from_millis(millis) != value {
        return Err(format!(
            "{name} must be a positive whole number of milliseconds within 1..=i32::MAX"
        ));
    }
    let millis = i32::try_from(millis).map_err(|_| {
        format!("{name} must be a positive whole number of milliseconds within 1..=i32::MAX")
    })?;
    GreaterI32::<0>::new(millis)
        .map(|_| ())
        .map_err(|error| format!("{name}: {error}"))
}

fn validate_positive_whole_bytes_i32(name: &str, value: ByteSize) -> Result<(), String> {
    let bytes = value.bytes_i64();
    if !value.bytes_f64().is_finite() || ByteSize::from_bytes_i64(bytes) != value {
        return Err(format!(
            "{name} must be a positive whole number of bytes within 1..=i32::MAX"
        ));
    }
    let bytes = i32::try_from(bytes).map_err(|_| {
        format!("{name} must be a positive whole number of bytes within 1..=i32::MAX")
    })?;
    GreaterI32::<0>::new(bytes)
        .map(|_| ())
        .map_err(|error| format!("{name}: {error}"))
}

fn validate_positive_duration(name: &str, value: Time) -> Result<(), String> {
    let duration = Duration::try_from_secs_f64(value.secs_f64())
        .map_err(|error| format!("{name}: {error}"))?;
    if duration.is_zero() {
        return Err(format!("{name} must be positive"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        std::time::Duration,
    };

    #[derive(Default)]
    struct MetadataConfigScenario {
        bootstrap: String,
        config: Option<KafkaMetadataLogConfig>,
        validation: Option<Result<(), String>>,
        capacity: usize,
        built_capacity: Option<Result<MetadataEventQueueCapacity, String>>,
    }
    #[rstest::fixture]
    fn metadata_config() -> MetadataConfigScenario {
        let bootstrap = String::new();
        MetadataConfigScenario {
            bootstrap,
            ..MetadataConfigScenario::default()
        }
    }
    #[rstest_bdd_macros::given("metadata bootstrap address {address}")]
    fn metadata_bootstrap_address(metadata_config: &mut MetadataConfigScenario, address: String) {
        metadata_config.bootstrap = address;
    }
    #[rstest_bdd_macros::when("metadata log defaults are constructed and validated")]
    fn construct_metadata_defaults(metadata_config: &mut MetadataConfigScenario) {
        let config = KafkaMetadataLogConfig::new(&metadata_config.bootstrap);
        metadata_config.validation = Some(config.validate());
        metadata_config.config = Some(config);
    }
    #[rstest_bdd_macros::given("a default metadata client targeting {address}")]
    fn default_metadata_client(metadata_config: &mut MetadataConfigScenario, address: String) {
        metadata_config.config = Some(KafkaMetadataLogConfig::new(address));
    }
    fn metadata_policy_time(text: &str) -> Time {
        if text == "infinite" {
            return Time::from_secs_f64(f64::INFINITY);
        }
        if let Some(micros) = text.strip_suffix("us") {
            return Time::from_micros(micros.parse().expect("microsecond fixture"));
        }
        if let Some(millis) = text.strip_suffix("ms") {
            return Time::from_millis(millis.parse().expect("millisecond fixture"));
        }
        match text {
            "0" => Time::ZERO,
            _ => panic!("unknown time fixture {text}"),
        }
    }
    #[rstest_bdd_macros::given("metadata transport field {field} equals {value}")]
    fn metadata_transport_value(
        metadata_config: &mut MetadataConfigScenario,
        field: String,
        value: String,
    ) {
        let config = metadata_config
            .config
            .as_mut()
            .expect("metadata client given");
        match field.as_str() {
            "topic_create_timeout" => config.topic_create_timeout = metadata_policy_time(&value),
            "fetch_max_wait" => config.fetch_max_wait = metadata_policy_time(&value),
            "fetch_retry_backoff" => config.fetch_retry_backoff = metadata_policy_time(&value),
            "fetch_max_bytes" => {
                config.fetch_max_bytes = ByteSize::from_bytes_f64(if value == "infinite" {
                    f64::INFINITY
                } else {
                    value
                        .trim_end_matches('B')
                        .parse()
                        .expect("byte size fixture")
                });
            }
            _ => panic!("unknown metadata transport field {field}"),
        }
    }
    #[rstest_bdd_macros::given("metadata event delivery capacity {capacity}")]
    fn metadata_delivery_capacity(metadata_config: &mut MetadataConfigScenario, capacity: usize) {
        metadata_config
            .config
            .as_mut()
            .expect("metadata client given")
            .event_queue_capacity =
            MetadataEventQueueCapacity::new(capacity).expect("positive delivery queue capacity");
    }
    #[rstest_bdd_macros::when("metadata client configuration is validated")]
    fn validate_metadata_client(metadata_config: &mut MetadataConfigScenario) {
        metadata_config.validation = Some(
            metadata_config
                .config
                .as_ref()
                .expect("metadata client given")
                .validate(),
        );
    }
    #[rstest_bdd_macros::then("metadata transport validation refuses field {field}")]
    fn refused_metadata_transport(metadata_config: &MetadataConfigScenario, field: String) {
        let error = metadata_config
            .validation
            .as_ref()
            .expect("metadata client validated")
            .as_ref()
            .expect_err("invalid transport policy rejected");
        assert2::assert!(error.contains(&field), "field={field}, error={error}");
    }
    #[rstest_bdd_macros::then("custom metadata transport settings remain valid and unchanged")]
    fn custom_metadata_transport(metadata_config: &MetadataConfigScenario) {
        assert!(
            metadata_config
                .validation
                .as_ref()
                .expect("metadata client validated")
                .is_ok()
        );
        let config = metadata_config
            .config
            .as_ref()
            .expect("metadata client given");
        check!(
            (
                config.topic_create_timeout,
                config.fetch_max_wait,
                config.fetch_max_bytes,
                config.fetch_retry_backoff,
                config.event_queue_capacity.capacity()
            ) == (secs(45), millis(750), mebibytes(2), millis(300), 2048)
        );
    }
    #[rstest_bdd_macros::given("metadata min ISR selection {minimum}")]
    fn metadata_min_isr(metadata_config: &mut MetadataConfigScenario, minimum: String) {
        metadata_config
            .config
            .as_mut()
            .expect("metadata client given")
            .min_isr =
            (minimum != "cluster-default").then(|| minimum.parse().expect("minimum ISR"));
    }
    #[rstest_bdd_macros::then("the min ISR policy is {decision}")]
    fn metadata_min_isr_decision(metadata_config: &MetadataConfigScenario, decision: String) {
        let valid = match decision.as_str() {
            "accepted" => true,
            "rejected" => false,
            _ => panic!("unknown policy decision {decision}"),
        };
        let result = metadata_config
            .validation
            .as_ref()
            .expect("metadata client validated");
        assert2::assert!(result.is_ok() == valid, "{result:?}");
        if let Err(error) = result {
            assert2::assert!(error.contains("min_isr"), "{error}");
        }
    }
    #[rstest_bdd_macros::given("metadata queue capacity input {capacity}")]
    fn metadata_queue_input(metadata_config: &mut MetadataConfigScenario, capacity: usize) {
        metadata_config.capacity = capacity;
    }
    #[rstest_bdd_macros::when("the metadata event queue capacity is constructed")]
    fn construct_metadata_queue_capacity(metadata_config: &mut MetadataConfigScenario) {
        metadata_config.built_capacity =
            Some(MetadataEventQueueCapacity::new(metadata_config.capacity));
    }
    #[rstest_bdd_macros::then("the metadata event queue capacity is {decision}")]
    fn constructed_metadata_queue_capacity(
        metadata_config: &MetadataConfigScenario,
        decision: String,
    ) {
        let result = metadata_config
            .built_capacity
            .as_ref()
            .expect("queue capacity constructed");
        match decision.as_str() {
            "rejected" => {
                assert!(result.is_err());
            }
            "one" => {
                check!(result.as_ref().expect("positive capacity").capacity() == 1);
            }
            _ => panic!("unknown queue capacity result {decision}"),
        }
    }
    #[rstest_bdd_macros::given(
        "metadata client resource policy has dispatch capacity 7 and frame size 32768 bytes"
    )]
    fn metadata_resource_policy(metadata_config: &mut MetadataConfigScenario) {
        let config = metadata_config
            .config
            .as_mut()
            .expect("metadata client given");
        config.dispatch_queue_capacity =
            ConnectionDispatchQueueCapacity::new(7).expect("positive dispatch queue");
        config.frame_max =
            ClientFrameMax::try_from(krabka_units::kibibytes(32)).expect("positive frame limit");
    }
    #[rstest_bdd_macros::then("the metadata client retains its dispatch and frame limits")]
    fn carried_metadata_resource_policy(metadata_config: &MetadataConfigScenario) {
        let config = metadata_config
            .config
            .as_ref()
            .expect("metadata client given");
        check!(config.dispatch_queue_capacity.get() == 7);
        check!(config.frame_max.size() == krabka_units::kibibytes(32));
    }
    #[rstest_bdd_macros::given(
        "metadata client SASL_PLAINTEXT credentials {username} and {password}"
    )]
    fn metadata_security_policy(
        metadata_config: &mut MetadataConfigScenario,
        username: String,
        password: String,
    ) {
        use {krabka_client_core::security::SaslCredentials, krabka_security::ListenerProtocol};
        let config = metadata_config
            .config
            .as_mut()
            .expect("metadata client given");
        config.num_partitions = 1;
        config.replication = 1;
        config.client_id = "x".into();
        config.security = Some(ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain { username, password }),
            sasl_host: None,
        });
    }
    #[rstest_bdd_macros::then("the metadata client retains its security configuration")]
    fn carried_metadata_security_policy(metadata_config: &MetadataConfigScenario) {
        assert!(
            metadata_config
                .config
                .as_ref()
                .expect("metadata client given")
                .security
                .is_some()
        );
    }

    #[rstest_bdd_macros::then("metadata client defaults match Kafka's topic and transport policy")]
    fn metadata_kafka_defaults(metadata_config: &MetadataConfigScenario) {
        let cfg = metadata_config
            .config
            .as_ref()
            .expect("configuration constructed");
        check!(cfg.topic == METADATA_TOPIC);
        check!(cfg.num_partitions == 50);
        check!(cfg.replication == 3);
        check!(cfg.min_isr == Some(2));
        check!(!cfg.compacted);
        check!(cfg.provision_topic);
        check!(cfg.bootstrap == "127.0.0.1:9092");
        check!(cfg.security.is_none());
        check!(cfg.topic_create_timeout == secs(30));
        check!(cfg.fetch_max_wait == millis(500));
        check!(cfg.fetch_max_bytes == mebibytes(1));
        check!(cfg.fetch_retry_backoff == millis(200));
        check!(cfg.event_queue_capacity.capacity() == 1024);
        assert!(
            metadata_config
                .validation
                .as_ref()
                .expect("configuration validated")
                .is_ok()
        );
    }
    #[rstest_bdd_macros::then("metadata transport quantities retain their exact Kafka wire images")]
    fn metadata_wire_defaults(metadata_config: &MetadataConfigScenario) {
        let cfg = metadata_config
            .config
            .as_ref()
            .expect("configuration constructed");
        check!(cfg.topic_create_timeout.millis_i32() == 30_000);
        check!(cfg.fetch_max_wait.millis_i32() == 500);
        check!(cfg.fetch_max_bytes.bytes_i32() == 1 << 20);
        check!(cfg.fetch_retry_backoff.to_std() == Duration::from_millis(200));
    }
    rstest_bdd_macros::scenarios!("tests/features/metadata_client_policy", fixtures = [metadata_config: MetadataConfigScenario]);
}
