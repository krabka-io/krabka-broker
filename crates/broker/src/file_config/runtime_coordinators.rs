//! The `[runtime]` appliers for the group coordinators.
//!
//! `apply_coordinators` covers the classic and consumer group protocol
//! timings, and `apply_share_group` and `apply_streams_group` cover the KIP-932
//! share groups and the KIP-1071 streams groups. All three write the same
//! coordinator layer, and all three validate the durations Kafka bounds.

use krabka_units::convert::TimeExt as _;

use super::{
    FileConfigError, RuntimeFileConfig,
    validate::{invalid_runtime_value, positive_i16, positive_i32, positive_time, positive_usize},
};

impl RuntimeFileConfig {
    pub(super) fn apply_coordinators(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_time_millis!(
            runtime,
            coordinator_session_expiry_tick,
            cfg.coordinator_session_expiry_tick
        );
        set_runtime_time_millis!(
            runtime,
            coordinator_shutdown_ack_timeout,
            cfg.coordinator_shutdown_ack_timeout
        );
        set_runtime_duration!(
            runtime,
            consumer_group_session_timeout,
            cfg.next_gen_consumer_group.session_timeout
        );
        set_runtime_duration!(
            runtime,
            consumer_group_heartbeat_interval,
            cfg.next_gen_consumer_group.heartbeat_interval
        );
        set_runtime_duration!(
            runtime,
            consumer_group_min_session_timeout,
            cfg.next_gen_consumer_group.min_session_timeout
        );
        set_runtime_duration!(
            runtime,
            consumer_group_max_session_timeout,
            cfg.next_gen_consumer_group.max_session_timeout
        );
        set_runtime_duration!(
            runtime,
            consumer_group_min_heartbeat_interval,
            cfg.next_gen_consumer_group.min_heartbeat_interval
        );
        set_runtime_duration!(
            runtime,
            consumer_group_max_heartbeat_interval,
            cfg.next_gen_consumer_group.max_heartbeat_interval
        );
        set_runtime_usize!(
            runtime,
            consumer_group_max_size,
            cfg.next_gen_consumer_group.max_size
        );
        set_runtime_time_millis!(
            runtime,
            classic_group_initial_rebalance_delay,
            cfg.classic_group_initial_rebalance_delay
        );
        set_runtime_time_millis!(
            runtime,
            sync_group_follower_wait,
            cfg.sync_group_follower_wait
        );
        Ok(())
    }

    pub(super) fn apply_share_group(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_plain!(runtime, share_group_enable, cfg.share_group.enable);
        set_runtime_duration!(
            runtime,
            share_group_session_timeout,
            cfg.share_group.session_timeout
        );
        set_runtime_duration!(
            runtime,
            share_group_heartbeat_interval,
            cfg.share_group.heartbeat_interval
        );
        set_runtime_usize!(runtime, share_group_max_size, cfg.share_group.max_size);
        set_runtime_duration!(
            runtime,
            share_group_record_lock_duration,
            cfg.share_group.record_lock_duration
        );
        if let Some(value) = runtime.share_group_max_delivery_attempts {
            let value = positive_i16("share_group_max_delivery_attempts", value)?;
            cfg.share_group.max_delivery_attempts = value;
        }
        set_runtime_i32!(
            runtime,
            share_group_max_inflight_records,
            cfg.share_group.max_inflight_records
        );
        set_runtime_duration!(
            runtime,
            share_group_backlog_poll_interval,
            cfg.share_group.backlog_poll_interval
        );
        if let Some(value) = runtime.share_group_isolation_level.take() {
            use crate::coordinator::unified::share::config::ShareIsolationLevel;
            let value = match value.as_str() {
                "read-uncommitted" => ShareIsolationLevel::ReadUncommitted,
                "read-committed" => ShareIsolationLevel::ReadCommitted,
                _ => {
                    return Err(invalid_runtime_value(
                        "share_group_isolation_level",
                        "expected `read-uncommitted` or `read-committed`",
                    ));
                }
            };
            cfg.share_group.isolation_level = value;
        }
        Ok(())
    }

    pub(super) fn apply_streams_group(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_plain!(runtime, streams_group_enable, cfg.streams_group.enable);
        set_runtime_duration!(
            runtime,
            streams_group_session_timeout,
            cfg.streams_group.session_timeout
        );
        set_runtime_duration!(
            runtime,
            streams_group_heartbeat_interval,
            cfg.streams_group.heartbeat_interval
        );
        set_runtime_usize!(runtime, streams_group_max_size, cfg.streams_group.max_size);
        if let Some(value) = runtime.streams_internal_topic_replication_factor {
            cfg.streams_group.internal_topic_replication_factor =
                positive_i16("streams_internal_topic_replication_factor", value)?;
        }
        if let Some(value) = runtime.streams_group_num_standby_replicas {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_num_standby_replicas",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.num_standby_replicas = value;
        }
        if let Some(value) = runtime.streams_group_num_warmup_replicas {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_num_warmup_replicas",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.num_warmup_replicas = value;
        }
        if let Some(value) = runtime.streams_group_acceptable_recovery_lag {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_acceptable_recovery_lag",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.acceptable_recovery_lag = value;
        }
        set_runtime_duration!(
            runtime,
            streams_group_task_offset_interval,
            cfg.streams_group.task_offset_interval
        );
        if let Some(value) = runtime.streams_group_assignor.take() {
            use crate::coordinator::unified::streams::config::StreamsAssignorKind;
            let value = match value.as_str() {
                "auto" => StreamsAssignorKind::Auto,
                "sticky" => StreamsAssignorKind::Sticky,
                "highly-available" => StreamsAssignorKind::HighlyAvailable,
                _ => {
                    return Err(invalid_runtime_value(
                        "streams_group_assignor",
                        "expected `auto`, `sticky`, or `highly-available`",
                    ));
                }
            };
            cfg.streams_group.assignor = value;
        }

        if let Some(value) = runtime.inter_broker_server_name.take() {
            cfg.inter_broker_server_name = value;
        }
        Ok(())
    }
}
