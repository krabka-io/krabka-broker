//! The `[runtime]` appliers for the transaction, offsets, and barrier
//! coordinator state topics.
//!
//! `apply_transactions` covers the internal state topics' partition counts,
//! replication factors, and recovery reads, together with the transaction
//! timeout bounds. `apply_barrier` covers the KFC-4 barrier group knobs.

use super::{
    FileConfigError, RuntimeFileConfig,
    validate::{
        positive_i16, positive_i32, positive_time, positive_usize, whole_bytes_usize,
        whole_millis_i32_time, whole_millis_i64_time,
    },
};

impl RuntimeFileConfig {
    pub(super) fn apply_transactions(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_time_millis!(
            runtime,
            producer_id_expiration,
            cfg.producer_id_expiration,
            positive_i64
        );
        set_runtime_time_millis!(
            runtime,
            producer_id_expiration_scan_interval,
            cfg.producer_id_expiration_scan_interval
        );
        set_runtime_usize!(runtime, max_produce_group, cfg.max_produce_group);
        set_runtime_usize!(
            runtime,
            partition_writer_queue_depth,
            cfg.partition_writer_queue_depth
        );
        set_runtime_i32!(
            runtime,
            default_min_insync_replicas,
            cfg.default_min_insync_replicas
        );
        set_runtime_size_bytes!(
            runtime,
            future_log_move_read_chunk,
            cfg.future_log_move_read_chunk,
            whole_bytes_usize
        );
        set_runtime_i32!(
            runtime,
            share_state_num_partitions,
            cfg.share_coordinator.state_topic_num_partitions
        );
        if let Some(value) = runtime.share_state_replication_factor {
            cfg.share_coordinator.state_topic_replication_factor =
                positive_i16("share_state_replication_factor", value)?;
        }
        set_runtime_i32!(
            runtime,
            offsets_topic_num_partitions,
            cfg.offsets_topic_num_partitions
        );
        if let Some(value) = runtime.offsets_topic_replication_factor {
            cfg.offsets_topic_replication_factor =
                positive_i16("offsets_topic_replication_factor", value)?;
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
        set_runtime_i32!(
            runtime,
            transaction_state_num_partitions,
            cfg.transaction_state_num_partitions
        );
        set_runtime_size_bytes!(
            runtime,
            transaction_recovery_read_max,
            cfg.transaction_recovery_read_max,
            whole_bytes_usize
        );
        if let Some(value) = runtime.transaction_state_replication_factor {
            cfg.transaction_state_replication_factor =
                positive_i16("transaction_state_replication_factor", value)?;
        }
        set_runtime_time_millis!(
            runtime,
            transaction_min_timeout,
            cfg.transaction_min_timeout,
            positive_i32
        );
        set_runtime_time_millis!(
            runtime,
            transaction_max_timeout,
            cfg.transaction_max_timeout,
            positive_i32
        );
        Ok(())
    }

    /// Applies the `barrier.*` runtime keys.
    ///
    /// `barrier_min_injection_interval` is a floor. A group asks for its own
    /// periodic interval through `AlterBarrierGroups`, and the coordinator
    /// refuses one below this value.
    pub(super) fn apply_barrier(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_i32!(
            runtime,
            barrier_state_num_partitions,
            cfg.barrier_state_num_partitions
        );
        if let Some(value) = runtime.barrier_state_replication_factor {
            cfg.barrier_state_replication_factor =
                positive_i16("barrier_state_replication_factor", value)?;
        }
        set_runtime_time_millis!(
            runtime,
            barrier_min_injection_interval,
            cfg.barrier_min_injection_interval,
            positive_i64
        );
        set_runtime_time_millis!(
            runtime,
            barrier_injection_timeout,
            cfg.barrier_injection_timeout,
            positive_i64
        );
        set_runtime_size_bytes!(
            runtime,
            barrier_recovery_read_max,
            cfg.barrier_recovery_read_max,
            whole_bytes_usize
        );
        set_runtime_i32!(runtime, barrier_retained_cuts, cfg.barrier_retained_cuts);
        set_runtime_usize!(runtime, barrier_max_groups, cfg.barrier_max_groups);
        set_runtime_usize!(
            runtime,
            barrier_max_topics_per_group,
            cfg.barrier_max_topics_per_group
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{mebibytes, secs};

    use crate::file_config::FileConfig;

    #[test]
    fn barrier_runtime_keys_land_in_the_broker_config() {
        let file: FileConfig = toml::from_str(
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
        )
        .expect("parse barrier runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        file.apply_to(&mut cfg)
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
            let mut cfg = crate::config::BrokerConfig::default();

            assert!(file.apply_to(&mut cfg).is_err(), "{case}");
        }
    }
}
