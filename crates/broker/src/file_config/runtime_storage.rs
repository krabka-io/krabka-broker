//! The `[runtime]` appliers for storage recovery, queue depths, and the
//! network and decompression limits.
//!
//! `apply_recovery_and_queues` covers the diskless WAL, unclean recovery,
//! share-session, and log sizing knobs; `apply_network_limits` covers the
//! socket buffers, ACL string ceilings, and the telemetry and record
//! decompression bounds that guard the broker against a hostile client.

use super::{
    FileConfigError, RuntimeFileConfig,
    validate::{
        kafka_int_bytes, positive_i64, positive_ratio, positive_time, positive_u32, positive_usize,
        whole_bytes_u32, whole_bytes_u64, whole_bytes_usize,
    },
};

impl RuntimeFileConfig {
    pub(super) fn apply_recovery_and_queues(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_time_millis!(
            runtime,
            unclean_recovery_aggressive_deadline,
            cfg.unclean_recovery_aggressive_deadline
        );
        set_runtime_time_millis!(
            runtime,
            unclean_recovery_balanced_deadline,
            cfg.unclean_recovery_balanced_deadline
        );
        set_runtime_time_millis!(
            runtime,
            operator_recovery_deadline,
            cfg.operator_recovery_deadline
        );
        set_runtime_time_millis!(runtime, quota_throttle_max, cfg.quota_throttle_max);
        set_runtime_time_millis!(
            runtime,
            controller_mutation_quota_window,
            cfg.controller_mutation_quota_window
        );
        set_runtime_u32!(
            runtime,
            self_registration_max_attempts,
            cfg.self_registration_max_attempts
        );
        set_runtime_size_bytes!(
            runtime,
            observer_fetch_max,
            cfg.observer_fetch_max,
            whole_bytes_u32
        );
        set_runtime_usize!(
            runtime,
            audit_event_queue_capacity,
            cfg.audit_event_queue_capacity
        );
        set_runtime_i64!(
            runtime,
            audit_tail_window_offsets,
            cfg.audit_tail_window_offsets
        );
        set_runtime_size_bytes!(
            runtime,
            audit_tail_read_max,
            cfg.audit_tail_read_max,
            whole_bytes_usize
        );
        set_runtime_time_millis!(
            runtime,
            offsets_topic_metadata_wait_timeout,
            cfg.offsets_topic_metadata_wait_timeout
        );
        set_runtime_u32!(
            runtime,
            client_metrics_stale_push_intervals,
            cfg.client_metrics_stale_push_intervals
        );
        set_runtime_usize!(
            runtime,
            client_metrics_otlp_queue_capacity,
            cfg.client_metrics_otlp_queue_capacity
        );
        set_runtime_usize!(
            runtime,
            coordinator_actor_mailbox_capacity,
            cfg.coordinator_actor_mailbox_capacity
        );
        set_runtime_usize!(
            runtime,
            diskless_wal_local_replica_count,
            cfg.diskless_wal_local_replica_count
        );
        set_runtime_time_millis!(
            runtime,
            diskless_wal_flush_interval,
            cfg.diskless_wal_flush_interval
        );
        set_runtime_size_bytes!(
            runtime,
            diskless_wal_flush_max_size,
            cfg.diskless_wal_flush_max_size,
            whole_bytes_usize
        );
        set_runtime_size_bytes!(
            runtime,
            diskless_wal_hot_tail_max_size,
            cfg.diskless_wal_hot_tail_max_size,
            whole_bytes_usize
        );
        if let Some(value) = runtime.diskless_wal_trim_safety_lag {
            if value.is_negative() {
                return Err(FileConfigError::InvalidConfig(
                    "diskless_wal_trim_safety_lag must be nonnegative".into(),
                ));
            }
            cfg.diskless_wal_trim_safety_lag = value;
        }
        set_runtime_time_millis!(
            runtime,
            diskless_wal_index_projection_timeout,
            cfg.diskless_wal_index_projection_timeout
        );
        set_runtime_usize!(
            runtime,
            unclean_recovery_queue_capacity,
            cfg.unclean_recovery_queue_capacity
        );
        set_runtime_size_bytes!(
            runtime,
            share_recovery_read_max,
            cfg.share_recovery_read_max,
            whole_bytes_usize
        );
        set_runtime_usize!(
            runtime,
            share_session_cache_max_when_unlimited,
            cfg.share_session_cache_max_when_unlimited
        );
        set_runtime_size_bytes!(
            runtime,
            log_read_buffer_cap,
            cfg.log_config.read_buffer_cap,
            whole_bytes_usize
        );
        set_runtime_size_bytes!(
            runtime,
            log_timestamp_scan_window,
            cfg.log_config.timestamp_scan_window,
            whole_bytes_usize
        );
        set_runtime_size_bytes!(
            runtime,
            log_segment_bytes,
            cfg.log_config.segment_size,
            whole_bytes_u64
        );
        set_runtime_size_bytes!(
            runtime,
            message_max_bytes,
            cfg.log_config.max_message_size,
            kafka_int_bytes
        );
        set_runtime_time_millis!(
            runtime,
            log_delivery_clock_uncertainty,
            cfg.log_config.delivery_clock_uncertainty
        );
        Ok(())
    }

    pub(super) fn apply_network_limits(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_size_bytes!(
            runtime,
            socket_request_max,
            cfg.socket_request_max,
            whole_bytes_u32
        );
        set_runtime_size_bytes!(runtime, sendfile_min, cfg.sendfile_min, whole_bytes_usize);
        set_runtime_size_bytes!(
            runtime,
            socket_send_buffer,
            cfg.socket_send_buffer,
            whole_bytes_usize
        );
        set_runtime_size_bytes!(
            runtime,
            socket_receive_buffer,
            cfg.socket_receive_buffer,
            whole_bytes_usize
        );
        set_runtime_size_bytes!(
            runtime,
            acl_max_principal,
            cfg.acl_max_principal,
            whole_bytes_usize
        );
        set_runtime_size_bytes!(
            runtime,
            acl_max_resource_name,
            cfg.acl_max_resource_name,
            whole_bytes_usize
        );
        if let Some(value) = runtime.telemetry_max_decompression_ratio {
            cfg.telemetry_max_decompression_ratio =
                positive_ratio("telemetry_max_decompression_ratio", value)?;
        }
        set_runtime_size_bytes!(
            runtime,
            telemetry_decompressed_output_floor,
            cfg.telemetry_decompressed_output_floor,
            whole_bytes_usize
        );
        set_runtime_size_bytes!(
            runtime,
            telemetry_decompressed_output_ceiling,
            cfg.telemetry_decompressed_output_ceiling,
            whole_bytes_usize
        );
        if let Some(value) = runtime.record_decompression_max_ratio {
            cfg.record_decompression_max_ratio =
                positive_ratio("record_decompression_max_ratio", value)?;
        }
        set_runtime_size_bytes!(
            runtime,
            record_decompression_output_floor,
            cfg.record_decompression_output_floor,
            whole_bytes_u64
        );
        set_runtime_size_bytes!(
            runtime,
            record_decompression_output_ceiling,
            cfg.record_decompression_output_ceiling,
            whole_bytes_u64
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{
        convert::{ByteSizeExt as _, TimeExt as _},
        millis,
    };

    use crate::file_config::FileConfig;

    #[test]
    fn runtime_file_config_rejects_negative_diskless_wal_trim_lag() {
        let file: FileConfig = toml::from_str("[runtime]\ndiskless_wal_trim_safety_lag = -1\n")
            .expect("parse runtime config");
        let error = file
            .apply_to(&mut crate::config::BrokerConfig::default())
            .expect_err("reject negative trim lag");

        assert!(error.to_string().contains("diskless_wal_trim_safety_lag"));
    }
    #[test]
    fn runtime_file_config_accepts_positive_diskless_wal_trim_lag() {
        let file: FileConfig = toml::from_str("[runtime]\ndiskless_wal_trim_safety_lag = 7\n")
            .expect("parse runtime config");
        let mut config = crate::config::BrokerConfig::default();

        file.apply_to(&mut config)
            .expect("accept positive trim lag");

        assert!(config.diskless_wal_trim_safety_lag == 7);
    }
    #[test]
    fn log_delivery_clock_uncertainty_round_trips_into_the_log_config() {
        // KFC-1's clock bound reaches every partition through
        // `BrokerConfig::log_config`, and it is a TOML-only key.
        let file: FileConfig =
            toml::from_str("[runtime]\nlog_delivery_clock_uncertainty = \"750ms\"\n")
                .expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        file.apply_to(&mut cfg).expect("apply runtime config");

        assert!(cfg.log_config.delivery_clock_uncertainty == millis(750));
        assert!(cfg.log_config.delivery_clock_uncertainty.millis_i64() == 750);
    }
    #[test]
    fn message_max_bytes_round_trips_into_the_log_config() {
        // Kafka's broker-wide `message.max.bytes` is the default behind every
        // topic's `max.message.bytes`, and in krabka that default is the base
        // `LogConfig` the produce gate reads when a topic sets none.
        let file: FileConfig = toml::from_str("[runtime]\nmessage_max_bytes = \"2KiB\"\n")
            .expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        file.apply_to(&mut cfg).expect("apply runtime config");

        assert!(cfg.log_config.max_message_size.bytes_u64() == 2048);
    }

    /// The TOML surface takes exactly the values Kafka's `INT` with
    /// `atLeast(0)` takes.
    ///
    /// `apache/kafka:4.3.1` starts on `message.max.bytes=0`, refuses `-1` with
    /// "Value must be at least 0", and refuses `2147483648` with "Not a number
    /// of type INT". The zero and the 2 GiB cases are the ones that separate
    /// this domain from a plain positive-whole-bytes one: a broker that took
    /// 2 GiB here would hand a topic an effective cap `kafka-configs` could
    /// not represent, and one that refused 0 would refuse a value Kafka boots
    /// on.
    #[test]
    fn message_max_bytes_takes_kafkas_int_at_least_zero() {
        for (value, expected) in [
            ("0B", Some(0)),
            ("2KiB", Some(2048)),
            ("2147483647B", Some(2_147_483_647)),
            ("-1B", None),
            ("2147483648B", None),
            ("2GiB", None),
            ("1.5B", None),
        ] {
            let applied = toml::from_str::<FileConfig>(&format!(
                "[runtime]\nmessage_max_bytes = \"{value}\"\n"
            ))
            .ok()
            .and_then(|file| {
                let mut cfg = crate::config::BrokerConfig::default();
                file.apply_to(&mut cfg)
                    .ok()
                    .map(|()| cfg.log_config.max_message_size.bytes_u64())
            });
            assert!(applied == expected, "message_max_bytes={value}");
        }
    }

    #[test]
    fn omitted_message_max_bytes_keeps_kafkas_1048588() {
        let file: FileConfig = toml::from_str("[runtime]\n").expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        file.apply_to(&mut cfg).expect("apply runtime config");

        assert!(cfg.log_config.max_message_size.bytes_u64() == 1_048_588);
    }

    #[test]
    fn omitted_log_delivery_clock_uncertainty_keeps_the_quarter_second_default() {
        let file: FileConfig = toml::from_str("[runtime]\n").expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        file.apply_to(&mut cfg).expect("apply runtime config");

        assert!(cfg.log_config.delivery_clock_uncertainty == millis(250));
    }
    #[test]
    fn log_delivery_clock_uncertainty_rejects_a_nonpositive_bound() {
        let file: FileConfig =
            toml::from_str("[runtime]\nlog_delivery_clock_uncertainty = \"0ms\"\n")
                .expect("parse runtime config");

        let error = file
            .apply_to(&mut crate::config::BrokerConfig::default())
            .expect_err("reject a zero clock bound");

        assert!(
            error.to_string().contains("log_delivery_clock_uncertainty"),
            "got: {error}"
        );
    }
    #[test]
    fn runtime_file_config_applies_record_decompression_policy() {
        let source = r#"
[runtime]
record_decompression_max_ratio = "50"
record_decompression_output_floor = "8MiB"
record_decompression_output_ceiling = "512MiB"
"#;
        let file: FileConfig = toml::from_str(source).expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).expect("apply runtime config");

        let policy = cfg
            .record_decompression_policy()
            .expect("validated decompression policy");
        assert!(policy.max_ratio() == krabka_units::fraction(50.0));
        assert!(policy.output_floor() == krabka_units::mebibytes(8));
        assert!(policy.output_ceiling() == krabka_units::mebibytes(512));
    }
    #[test]
    fn runtime_file_config_rejects_invalid_record_decompression_relations() {
        for body in [
            "record_decompression_max_ratio = \"101\"\n",
            concat!(
                "record_decompression_output_floor = \"1GiB\"\n",
                "record_decompression_output_ceiling = \"16MiB\"\n",
            ),
        ] {
            let source = format!("[runtime]\n{body}");
            let file: FileConfig = toml::from_str(&source).expect("parse runtime config");
            let error = file
                .apply_to(&mut crate::config::BrokerConfig::default())
                .expect_err("invalid record decompression policy must fail");
            assert!(error.to_string().contains("record_decompression"));
        }
    }
}
