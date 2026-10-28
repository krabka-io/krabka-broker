//! KIP-405 tiered storage: which object store backs the remote log, which
//! remote-log metadata manager runs beside it, and the WORM archive pairing
//! the broker refuses.

use {
    crate::{
        BrokerError,
        config::{
            BrokerConfig, DEFAULT_RLMM_SNAPSHOT_INTERVAL, DEFAULT_RLMM_TOPIC_MIN_ISR,
            DEFAULT_RLMM_TOPIC_NUM_PARTITIONS, DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR,
        },
    },
    krabka_client_core::security::ClientSecurity,
    krabka_units::{ByteSize, Time, convert::TimeExt},
    std::{path::PathBuf, time::Duration},
};

/// Parameters for the topic-backed
/// [`RemoteLogMetadataManager`](krabka_remote_storage::RemoteLogMetadataManager).
///
/// Does not derive `PartialEq`/`Eq`: the `security` field holds
/// rustls-adjacent types (a `ClientConfig` connector) that are not
/// comparable, and nothing compares this config by value.
#[derive(Debug, Clone, krabka_macros::FieldDefaults)]
pub struct KafkaRlmmConfig {
    /// Capacity of every Kafka metadata-log client dispatch queue.
    pub dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    /// Maximum frame size for every Kafka metadata-log client.
    pub frame_max: krabka_client_core::ClientFrameMax,
    /// `host:port` the manager dials to reach its own broker. It is loopback
    /// in a single-broker setup, and the inter-broker listener in a
    /// multi-broker setup.
    pub bootstrap: String,
    /// Partition count to create `__remote_log_metadata` with on first
    /// startup. Ignored when the topic already exists.
    #[default(DEFAULT_RLMM_TOPIC_NUM_PARTITIONS)]
    pub num_partitions: i32,
    /// Replication factor to create `__remote_log_metadata` with on
    /// first startup. Ignored when the topic already exists.
    #[default(DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR)]
    pub replication: i32,
    /// `min.insync.replicas` to create `__remote_log_metadata` with on first
    /// startup. Maps to Kafka's `remote.log.metadata.topic.min.isr`. Ignored
    /// when the topic already exists. Default
    /// [`DEFAULT_RLMM_TOPIC_MIN_ISR`].
    #[default(DEFAULT_RLMM_TOPIC_MIN_ISR)]
    pub min_isr: i32,
    /// How often the topic-backed manager flushes its RLMM cache
    /// snapshot to disk. Maps to Kafka's
    /// `remote.log.metadata.snapshot.interval`. Default
    /// [`DEFAULT_RLMM_SNAPSHOT_INTERVAL`].
    #[default(DEFAULT_RLMM_SNAPSHOT_INTERVAL)]
    pub snapshot_interval: Time,
    /// Timeout for provisioning each internal metadata topic.
    #[default(krabka_remote_storage_topic::DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT)]
    pub topic_create_timeout: Time,
    /// Maximum wait for each per-partition metadata fetch.
    #[default(krabka_remote_storage_topic::DEFAULT_METADATA_FETCH_MAX_WAIT)]
    pub fetch_max_wait: Time,
    /// Maximum bytes returned by each per-partition metadata fetch.
    #[default(krabka_remote_storage_topic::DEFAULT_METADATA_FETCH_MAX_BYTES)]
    pub fetch_max_bytes: ByteSize,
    /// Backoff after a failed metadata fetch.
    #[default(krabka_remote_storage_topic::DEFAULT_METADATA_FETCH_RETRY_BACKOFF)]
    pub fetch_retry_backoff: Time,
    /// Capacity of the shared metadata-event delivery queue.
    pub event_queue_capacity: krabka_remote_storage_topic::MetadataEventQueueCapacity,
    /// Directory the manager writes the RLMM cache snapshot to, as one
    /// `snapshot` file. The broker derives the path from `log.dir`.
    pub snapshot_dir: PathBuf,
    /// Client TLS/SASL security for the metadata client. `None` means
    /// plaintext loopback, which suits single-broker and fully-plaintext
    /// clusters. The broker overrides this at runtime in
    /// `bootstrap_topic_rlmm` from the inter-broker listener. The TOML path
    /// always supplies `None`.
    ///
    /// The field is boxed to keep `KafkaRlmmConfig` and the enclosing
    /// `BrokerConfig` small, because `Broker::start` moves `BrokerConfig` by
    /// value into a large future.
    pub security: Option<Box<ClientSecurity>>,
}

/// Which `RemoteLogMetadataManager` the broker runs when tiered storage is enabled.
///
/// Topic-backed is the production default. It matches Kafka's
/// `TopicBasedRemoteLogMetadataManager`, the only production RLMM. In-memory
/// is an explicit opt-out for in-process integration tests that have no real
/// listener to loop the metadata client back to. The broker ignores this enum
/// entirely when [`BrokerConfig::remote_storage_backend`] is `None`.
#[derive(Debug, Clone)]
pub enum RlmmKind {
    /// Durable `__remote_log_metadata`-backed manager. `cfg.bootstrap` and
    /// `cfg.snapshot_dir` may be empty; the broker derives them at start from
    /// the inter-broker listener and `log.dir` respectively.
    TopicBacked(KafkaRlmmConfig),
    /// Non-durable in-process manager. Tests only.
    InMemory,
}

impl KafkaRlmmConfig {
    /// Validates the shared metadata transport and RLMM snapshot policy.
    ///
    /// # Errors
    ///
    /// Returns an invalid-runtime error when a value cannot safely reach its
    /// runtime consumer.
    pub fn validate(&self) -> Result<(), BrokerError> {
        let transport = krabka_remote_storage_topic::KafkaMetadataLogConfig {
            dispatch_queue_capacity: self.dispatch_queue_capacity,
            frame_max: self.frame_max,
            min_isr: Some(self.min_isr),
            topic_create_timeout: self.topic_create_timeout,
            fetch_max_wait: self.fetch_max_wait,
            fetch_max_bytes: self.fetch_max_bytes,
            fetch_retry_backoff: self.fetch_retry_backoff,
            event_queue_capacity: self.event_queue_capacity,
            ..krabka_remote_storage_topic::KafkaMetadataLogConfig::new(&self.bootstrap)
        };
        transport.validate().map_err(|error| {
            BrokerError::InvalidRuntimeConfig(format!("remote_storage.kafka_metadata: {error}"))
        })?;

        let snapshot_interval = Duration::try_from_secs_f64(self.snapshot_interval.secs_f64())
            .map_err(|error| {
                BrokerError::InvalidRuntimeConfig(format!(
                    "remote_storage.kafka_metadata.snapshot_interval: {error}"
                ))
            })?;
        if snapshot_interval.is_zero() {
            return Err(BrokerError::InvalidRuntimeConfig(
                "remote_storage.kafka_metadata.snapshot_interval must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// What backs the broker's `RemoteStorageManager` when tiered storage is on.
#[derive(Debug, Clone)]
pub enum RemoteStorageBackend {
    /// Filesystem-backed `LocalTieredStorage`. Useful for tests, single-
    /// node dev setups, and shared-filesystem multi-broker deployments.
    Local {
        /// Root directory for the segment store.
        dir: PathBuf,
    },
    /// S3-compatible `S3RemoteStorage`. This is a production backend. It
    /// works with AWS S3, `MinIO`, Cloudflare R2, and GCS through S3
    /// compatibility.
    S3(krabka_remote_storage::S3Config),
    /// Native Google Cloud Storage `S3RemoteStorage` engine. This is the
    /// production backend for GKE deployments. It supports keyless Workload
    /// Identity and ADC auth; leave all credential fields unset for that.
    Gcs(krabka_remote_storage::GcsConfig),
}

impl BrokerConfig {
    /// Rejects WORM archive mode over anything but an object store.
    ///
    /// The TOML layer rejects the same pairing, but a `BrokerConfig` built in
    /// code never passes through it.
    pub(super) fn validate_remote_storage_worm(&self) -> Result<(), BrokerError> {
        if self.remote_storage_worm.is_some()
            && !matches!(
                self.remote_storage_backend,
                Some(RemoteStorageBackend::S3(_) | RemoteStorageBackend::Gcs(_))
            )
        {
            return Err(BrokerError::InvalidRuntimeConfig(
                "remote storage WORM mode requires an S3 or GCS backend; a local storage \
                 directory cannot enforce write-once"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        krabka_remote_storage_topic::MetadataEventQueueCapacity,
        krabka_units::{bytes, convert::ByteSizeExt, mebibytes, millis, minutes, secs},
        rstest_bdd::datatable::Rows,
        std::path::PathBuf,
    };

    #[test]
    fn production_default_selects_topic_backed_rlmm() {
        let c = BrokerConfig::default();
        assert!(matches!(c.remote_log_metadata, RlmmKind::TopicBacked(_)));
    }

    #[test]
    fn test_default_selects_in_memory_rlmm() {
        let c = BrokerConfig::for_tests(PathBuf::from("/tmp"));
        assert!(matches!(c.remote_log_metadata, RlmmKind::InMemory));
    }

    #[test]
    fn broker_validation_rejects_invalid_topic_metadata_policy() {
        let config = BrokerConfig {
            remote_log_metadata: RlmmKind::TopicBacked(KafkaRlmmConfig {
                fetch_max_bytes: ByteSize::ZERO,
                ..KafkaRlmmConfig::default()
            }),
            ..BrokerConfig::default()
        };

        let error = config
            .validate()
            .expect_err("invalid embedded metadata policy must fail");
        assert!(error.to_string().contains("fetch_max_bytes"), "{error}");
    }

    #[test]
    fn worm_without_an_object_store_backend_is_rejected_by_validate() {
        // The TOML layer already rejects these, but a `BrokerConfig` built in
        // code never passes through it.
        let cases = [
            (
                "a local storage directory cannot enforce write-once",
                Some(RemoteStorageBackend::Local {
                    dir: PathBuf::from("/var/lib/krabka-remote"),
                }),
            ),
            ("tiered storage is off entirely", None),
        ];
        for (name, backend) in cases {
            let c = BrokerConfig {
                remote_storage_backend: backend,
                remote_storage_worm: Some(krabka_remote_storage::WormConfig::default()),
                ..BrokerConfig::default()
            };
            check!(
                matches!(c.validate(), Err(BrokerError::InvalidRuntimeConfig(_))),
                "case {name}"
            );
        }
    }

    #[test]
    fn worm_over_an_object_store_backend_is_accepted_by_validate() {
        let c = BrokerConfig {
            remote_storage_backend: Some(RemoteStorageBackend::S3(
                krabka_remote_storage::S3Config {
                    bucket: "archive".into(),
                    region: "us-east-1".into(),
                    ..krabka_remote_storage::S3Config::default()
                },
            )),
            remote_storage_worm: Some(krabka_remote_storage::WormConfig::default()),
            ..BrokerConfig::default()
        };

        c.validate()
            .expect("WORM over an S3 backend is the supported pairing");
    }

    #[derive(Default)]
    struct TopicMetadataPolicy {
        config: KafkaRlmmConfig,
        validation: Option<Result<(), String>>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct MetadataPolicyProperty {
        field: String,
        value: String,
    }

    #[rstest::fixture]
    fn metadata_policy() -> TopicMetadataPolicy {
        // Each topic metadata manager receives its own configured policy.
        TopicMetadataPolicy::default()
    }

    #[rstest_bdd_macros::given("the topic metadata manager uses default settings")]
    fn default_metadata_policy(metadata_policy: &mut TopicMetadataPolicy) {
        metadata_policy.config = KafkaRlmmConfig::default();
    }

    #[rstest_bdd_macros::given("the topic metadata manager has these policy overrides:")]
    fn supplied_metadata_policy(
        metadata_policy: &mut TopicMetadataPolicy,
        #[datatable] rows: Rows<MetadataPolicyProperty>,
    ) {
        for row in rows {
            let value = row.value.as_str();
            match row.field.as_str() {
                "bootstrap" => metadata_policy.config.bootstrap = value.to_owned(),
                "num_partitions" => {
                    metadata_policy.config.num_partitions = value.parse().expect("partition count");
                }
                "replication" => {
                    metadata_policy.config.replication = value.parse().expect("replication factor");
                }
                "min_isr" => metadata_policy.config.min_isr = value.parse().expect("minimum ISR"),
                "snapshot_dir" => metadata_policy.config.snapshot_dir = PathBuf::from(value),
                "snapshot_interval_ms" => {
                    metadata_policy.config.snapshot_interval = if value == "infinity" {
                        Time::from_secs_f64(f64::INFINITY)
                    } else {
                        millis(value.parse::<u32>().expect("snapshot milliseconds"))
                    }
                }
                "topic_create_timeout_ms" => {
                    metadata_policy.config.topic_create_timeout =
                        millis(value.parse::<u32>().expect("creation timeout"));
                }
                "fetch_max_wait_ms" => {
                    metadata_policy.config.fetch_max_wait =
                        millis(value.parse::<u32>().expect("fetch wait"));
                }
                "fetch_max_bytes" => {
                    metadata_policy.config.fetch_max_bytes =
                        bytes(value.parse::<u32>().expect("fetch bytes"));
                }
                "fetch_retry_backoff_ms" => {
                    metadata_policy.config.fetch_retry_backoff =
                        millis(value.parse::<u32>().expect("fetch backoff"));
                }
                "event_queue_capacity" => {
                    metadata_policy.config.event_queue_capacity =
                        MetadataEventQueueCapacity::new(value.parse().expect("queue capacity"))
                            .expect("valid queue capacity");
                }
                field => panic!("unknown metadata policy field {field}"),
            }
        }
    }

    #[rstest_bdd_macros::when("the topic metadata manager policy is validated")]
    fn validate_metadata_policy(metadata_policy: &mut TopicMetadataPolicy) {
        metadata_policy.validation = Some(
            metadata_policy
                .config
                .validate()
                .map_err(|error| error.to_string()),
        );
    }

    #[rstest_bdd_macros::then("the topic metadata manager policy is accepted")]
    fn accepted_metadata_policy(metadata_policy: &TopicMetadataPolicy) {
        check!(metadata_policy.validation == Some(Ok(())));
    }

    #[rstest_bdd_macros::then("the policy refusal names {field}")]
    fn refused_metadata_policy(metadata_policy: &TopicMetadataPolicy, field: String) {
        let Err(error) = metadata_policy
            .validation
            .as_ref()
            .expect("policy was validated")
        else {
            panic!("invalid metadata policy must fail");
        };
        assert!(error.contains(&field), "{error}");
    }

    #[rstest_bdd_macros::then(
        "the topic metadata policy retains Kafka defaults for topology, snapshots, transport and security"
    )]
    fn default_metadata_policy_values(metadata_policy: &TopicMetadataPolicy) {
        let c = &metadata_policy.config;
        check!(c.num_partitions == 50);
        check!(c.replication == 3);
        check!(c.min_isr == 2);
        check!(c.bootstrap.is_empty());
        check!(c.snapshot_dir == PathBuf::new());
        check!(c.snapshot_interval == DEFAULT_RLMM_SNAPSHOT_INTERVAL);
        check!(c.topic_create_timeout == secs(30));
        check!(c.fetch_max_wait == millis(500));
        check!(c.fetch_max_bytes == mebibytes(1));
        check!(c.fetch_retry_backoff == millis(200));
        check!(c.event_queue_capacity.capacity() == 1024);
        check!(c.security.is_none());
    }

    #[rstest_bdd_macros::then(
        "metadata snapshots use a one minute interval and directory /data/remote-log-metadata"
    )]
    fn retained_metadata_snapshot_settings(metadata_policy: &TopicMetadataPolicy) {
        assert!(metadata_policy.config.snapshot_interval == minutes(1));
        assert!(metadata_policy.config.snapshot_dir == PathBuf::from("/data/remote-log-metadata"));
    }

    #[rstest_bdd_macros::then("the metadata connection requires no security configuration")]
    fn optional_metadata_security(metadata_policy: &TopicMetadataPolicy) {
        assert!(metadata_policy.config.security.is_none());
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/config/tiered_storage",
        fixtures = [metadata_policy: TopicMetadataPolicy]
    );
}
