//! The `[runtime]` appliers for the transaction, offsets, and barrier
//! coordinator state topics.
//!
//! `apply_transactions` covers the internal state topics' partition counts,
//! replication factors, and recovery reads, together with the transaction
//! timeout bounds. `apply_barrier` covers the KFC-4 barrier group knobs.

use {
    super::{
        FileConfigError, RuntimeFileConfig,
        validate::{
            invalid_runtime_value, kafka_int_bytes, positive_i16, positive_i32, positive_time,
            positive_usize, whole_bytes_usize, whole_millis_i32_time, whole_millis_i64_time,
        },
    },
    crate::config::BrokerConfig,
    krabka_units::{ByteSize, Time, convert::TimeExt, kibibytes},
    std::time::Duration,
};

impl RuntimeFileConfig {
    pub(super) fn apply_transactions(&self, cfg: &mut BrokerConfig) -> Result<(), FileConfigError> {
        let runtime = self;
        krabka_macros::set_runtime! {
            runtime => cfg;
            whole_millis_i64_time: producer_id_expiration;
            positive_time: producer_id_expiration_scan_interval;
            positive_usize: max_produce_group, partition_writer_queue_depth;
        }
        // A topic reports a static `min.insync.replicas` at
        // `STATIC_BROKER_CONFIG` when the operator named it, whatever the value.
        cfg.static_config_origins.log.min_insync_replicas |=
            runtime.default_min_insync_replicas.is_some();
        krabka_macros::set_runtime! { runtime => cfg; positive_i32: default_min_insync_replicas; }
        // `DescribeConfigs` reports these two as `STATIC_BROKER_CONFIG` when
        // the operator named them, so the loader records the provenance.
        if let Some(value) = runtime.num_partitions {
            cfg.num_partitions = positive_i32("num_partitions", value)?;
            cfg.static_config_origins.topic_creation.num_partitions = true;
        }
        if let Some(value) = runtime.default_replication_factor {
            cfg.default_replication_factor = positive_i16("default_replication_factor", value)?;
            cfg.static_config_origins
                .topic_creation
                .default_replication_factor = true;
        }
        krabka_macros::set_runtime! {
            runtime => cfg;
            whole_bytes_usize: future_log_move_read_chunk;
            positive_i32: share_state_num_partitions => share_coordinator.state_topic_num_partitions;
            positive_i16: share_state_replication_factor
                => share_coordinator.state_topic_replication_factor;
            kafka_int_bytes: share_state_segment_bytes
                => share_coordinator.state_topic_segment_bytes;
            positive_i32: share_state_min_isr => share_coordinator.state_topic_min_isr;
        }
        // Kafka's `between(0, 500)`.
        if let Some(value) = runtime.share_snapshot_update_records_per_snapshot {
            if value > 500 {
                return Err(invalid_runtime_value(
                    "share_snapshot_update_records_per_snapshot",
                    "must be within 0..=500",
                ));
            }
            cfg.share_coordinator.snapshot_update_records_per_snapshot = value;
        }
        // Kafka's `atLeast(1)` milliseconds, as an `INT`.
        krabka_macros::set_runtime! {
            runtime => cfg;
            whole_millis_i32_duration: share_coordinator_write_timeout
                    => share_coordinator.write_timeout,
                share_state_prune_interval => share_coordinator.state_topic_prune_interval,
                share_cold_partition_snapshot_interval
                    => share_coordinator.cold_partition_snapshot_interval;
        }
        // `CompressionType.forId` over Kafka's `INT` codec id.
        if let Some(value) = runtime.share_state_compression_codec {
            cfg.share_coordinator.state_topic_compression_codec = u8::try_from(value)
                .ok()
                .filter(|id| *id <= 4)
                .and_then(krabka_compression::CompressionType::from_attribute_bits)
                .ok_or_else(|| {
                    invalid_runtime_value(
                        "share_state_compression_codec",
                        format!("Unknown compression type id: {value}"),
                    )
                })?;
        }
        // Kafka's `atLeast(1)`.
        if let Some(value) = runtime.share_coordinator_threads {
            let value = positive_i32("share_coordinator_threads", value)?;
            cfg.share_coordinator.threads = value.unsigned_abs();
        }
        // Kafka's `atLeast(-1)`, with -1 its adaptive linger.
        if let Some(value) = runtime.share_coordinator_append_linger_ms {
            cfg.share_coordinator.append_linger = match value {
                -1 => None,
                0.. => Some(Duration::from_millis(u64::from(value.unsigned_abs()))),
                _ => {
                    return Err(invalid_runtime_value(
                        "share_coordinator_append_linger_ms",
                        "must be at least -1",
                    ));
                }
            };
        }
        krabka_macros::set_runtime! {
            runtime => cfg;
            // Kafka's `atLeast(512 * 1024)` over an `INT`.
            cached_buffer_max_bytes: share_coordinator_cached_buffer_max_bytes
                => share_coordinator.cached_buffer_max_bytes;
            positive_i32: offsets_topic_num_partitions;
            positive_i16: offsets_topic_replication_factor;
            kafka_int_bytes: offsets_topic_segment_bytes;
        }
        // These two carry the operator's intent, not just a value: `Some`
        // means the key was named, which is what `DescribeConfigs` reports as
        // `STATIC_BROKER_CONFIG`.
        if let Some(value) = runtime.offsets_retention {
            cfg.offsets_retention_override = Some(positive_time("offsets_retention", value)?);
        }
        if let Some(value) = runtime.offsets_retention_check_interval {
            cfg.offsets_retention_check_interval_override =
                Some(positive_time("offsets_retention_check_interval", value)?);
        }
        krabka_macros::set_runtime! {
            runtime => cfg;
            positive_i32: transaction_state_num_partitions;
            whole_bytes_usize: transaction_recovery_read_max;
            positive_i16: transaction_state_replication_factor;
            kafka_int_bytes: transaction_state_segment_bytes;
            positive_i32: transaction_state_min_isr;
            whole_millis_i32_time: transaction_max_timeout;
            plain: transaction_partition_verification_enable;
        }
        Ok(())
    }

    /// Applies the `barrier.*` runtime keys.
    ///
    /// `barrier_min_injection_interval` is a floor. A group asks for its own
    /// periodic interval through `AlterBarrierGroups`, and the coordinator
    /// refuses one below this value.
    pub(super) fn apply_barrier(&self, cfg: &mut BrokerConfig) -> Result<(), FileConfigError> {
        let runtime = self;
        krabka_macros::set_runtime! {
            runtime => cfg;
            positive_i32: barrier_state_num_partitions;
            positive_i16: barrier_state_replication_factor;
            whole_millis_i64_time: barrier_min_injection_interval, barrier_injection_timeout;
            whole_bytes_usize: barrier_recovery_read_max;
            positive_i32: barrier_retained_cuts;
            positive_usize: barrier_max_groups, barrier_max_topics_per_group;
        }
        Ok(())
    }
}

/// Kafka's `share.coordinator.cached.buffer.max.bytes` range: an `INT` of at
/// least 512 KiB.
fn cached_buffer_max_bytes(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let value = kafka_int_bytes(name, value)?;
    if value >= kibibytes(512) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(name, "must be at least 524288 bytes"))
    }
}

/// [`whole_millis_i32_time`] as the [`std::time::Duration`] the share
/// coordinator's timers hold.
fn whole_millis_i32_duration(name: &str, value: Time) -> Result<Duration, FileConfigError> {
    whole_millis_i32_time(name, value).map(TimeExt::to_std)
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            config::BrokerConfig,
            file_config::{FileConfig, test_support::configured},
            share_coordinator::config::ShareCoordinatorConfig,
        },
        assert2::assert,
        krabka_units::{mebibytes, secs},
    };

    #[test]
    fn barrier_runtime_keys_land_in_the_broker_config() {
        let cfg = configured(
            r#"
[runtime]
barrier_state_num_partitions = 12
barrier_state_replication_factor = 2
barrier_min_injection_interval = "5s"
barrier_injection_timeout = "45s"
barrier_recovery_read_max = "4MiB"
barrier_retained_cuts = 25
barrier_max_groups = 8
barrier_max_topics_per_group = 16
"#,
            "parse barrier runtime config",
        )
        .expect("apply barrier runtime config");

        let actual = (
            cfg.barrier_state_num_partitions,
            cfg.barrier_state_replication_factor,
            cfg.barrier_min_injection_interval,
            cfg.barrier_injection_timeout,
            cfg.barrier_recovery_read_max,
            cfg.barrier_retained_cuts,
            cfg.barrier_max_groups,
            cfg.barrier_max_topics_per_group,
        );

        assert!(actual == (12, 2, secs(5), secs(45), mebibytes(4), 25, 8, 16));
    }
    #[test]
    fn barrier_runtime_keys_reject_nonpositive_values() {
        let cases = [
            "barrier_state_num_partitions = 0",
            "barrier_state_replication_factor = 0",
            "barrier_min_injection_interval = \"0s\"",
            "barrier_injection_timeout = \"0s\"",
            "barrier_recovery_read_max = \"0B\"",
            "barrier_retained_cuts = 0",
            "barrier_max_groups = 0",
            "barrier_max_topics_per_group = 0",
        ];

        for case in cases {
            let file: FileConfig = toml::from_str(&format!("[runtime]\n{case}\n"))
                .unwrap_or_else(|error| panic!("parse {case}: {error}"));
            let mut cfg = BrokerConfig::default();

            assert!(file.apply_to(&mut cfg).is_err(), "{case}");
        }
    }

    #[derive(Default)]
    struct ShareCoordinatorProperties {
        body: String,
        applied: Option<Result<BrokerConfig, String>>,
    }
    #[derive(rstest_bdd_macros::DataTableRow)]
    struct CoordinatorPolicyField {
        field: String,
        value: String,
    }
    #[rstest::fixture]
    fn share_coordinator_properties() -> ShareCoordinatorProperties {
        // Keep the submitted properties and their complete application outcome together.
        ShareCoordinatorProperties::default()
    }
    #[rstest_bdd_macros::given("the Share coordinator runtime properties are")]
    fn share_coordinator_source(
        share_coordinator_properties: &mut ShareCoordinatorProperties,
        docstring: String,
    ) {
        share_coordinator_properties.body = docstring;
    }
    #[rstest_bdd_macros::when("the broker parses and applies the Share coordinator properties")]
    fn apply_share_coordinator_properties(
        share_coordinator_properties: &mut ShareCoordinatorProperties,
    ) {
        let file: FileConfig = toml::from_str(&format!(
            "[runtime]\n{}\n",
            share_coordinator_properties.body
        ))
        .expect("runtime TOML syntax");
        let mut config = BrokerConfig::default();
        share_coordinator_properties.applied = Some(
            file.apply_to(&mut config)
                .map(|()| config)
                .map_err(|error| error.to_string()),
        );
    }
    #[rstest_bdd_macros::then("the complete Share coordinator policy equals defaults except")]
    fn share_coordinator_policy(
        share_coordinator_properties: &ShareCoordinatorProperties,
        #[datatable] datatable: rstest_bdd::datatable::Rows<CoordinatorPolicyField>,
    ) {
        let config = share_coordinator_properties
            .applied
            .as_ref()
            .expect("properties applied")
            .as_ref()
            .expect("valid policy");
        let mut expected = ShareCoordinatorConfig::default();
        for row in datatable {
            match row.field.as_str() {
                "snapshot_update_records_per_snapshot" => {
                    expected.snapshot_update_records_per_snapshot =
                        row.value.parse().expect("integer setting")
                }
                "write_timeout" => {
                    expected.write_timeout = std::time::Duration::from_millis(
                        row.value.parse().expect("timer milliseconds"),
                    )
                }
                "state_topic_prune_interval" => {
                    expected.state_topic_prune_interval = std::time::Duration::from_millis(
                        row.value.parse().expect("timer milliseconds"),
                    )
                }
                "state_topic_compression_codec" => {
                    expected.state_topic_compression_codec = match row.value.as_str() {
                        "zstd" => krabka_compression::CompressionType::Zstd,
                        other => panic!("unknown codec {other}"),
                    }
                }
                "threads" => expected.threads = row.value.parse().expect("integer setting"),
                "append_linger" => {
                    expected.append_linger = Some(std::time::Duration::from_millis(
                        row.value.parse().expect("linger milliseconds"),
                    ))
                }
                "cached_buffer_max_bytes" => {
                    expected.cached_buffer_max_bytes =
                        krabka_units::bytes(row.value.parse().expect("cache bytes"))
                }
                unknown => panic!("unknown Share coordinator field {unknown}"),
            }
        }
        assert!(config.share_coordinator.as_ref() == &expected);
    }
    #[rstest_bdd_macros::then("the Share coordinator policy is refused")]
    fn share_coordinator_refusal(share_coordinator_properties: &ShareCoordinatorProperties) {
        assert!(
            share_coordinator_properties
                .applied
                .as_ref()
                .expect("properties applied")
                .is_err()
        );
    }
    #[rstest_bdd_macros::then("the Share coordinator load buffer has {bytes} bytes")]
    fn share_coordinator_load_buffer(
        share_coordinator_properties: &ShareCoordinatorProperties,
        bytes: u32,
    ) {
        let config = share_coordinator_properties
            .applied
            .as_ref()
            .expect("properties applied")
            .as_ref()
            .expect("valid policy");
        assert!(config.share_coordinator_load_buffer_size == krabka_units::bytes(bytes));
    }
    rstest_bdd_macros::scenarios!("tests/features/migrated/src/file_config/runtime_transactions.feature", fixtures = [share_coordinator_properties: ShareCoordinatorProperties]);
}
