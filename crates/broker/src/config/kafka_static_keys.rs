//! The `KafkaConfig` keys whose value this node runs with, and which
//! `[runtime]` field sets each.
//!
//! `DescribeConfigs` reports a key the operator named, or one this node runs
//! with a value other than Kafka's default, at `STATIC_BROKER_CONFIG` with
//! the value the node uses. Kafka answers from the `KafkaConfig` the process
//! parsed, so a group's `consumer.session.timeout.ms` and a named broker's
//! `group.consumer.session.timeout.ms` both say what the coordinator runs.
//! This table is where krabka reads those values from
//! [`crate::config::BrokerConfig`], so the coordinators, the broker resource
//! and the group resource cannot report different numbers for one setting.
//!
//! [`KafkaStaticKey::runtime_field`] is the `[runtime]` field the loader
//! watches to record provenance in
//! [`StaticConfigOrigins::supplied_kafka_keys`](super::StaticConfigOrigins).
//! A key with no `[runtime]` field is set by code that builds the
//! configuration, and reports at `STATIC_BROKER_CONFIG` once its value leaves
//! Kafka's default.

use {
    super::BrokerConfig,
    crate::{
        api_catalog::UnstableApiVersions,
        config_keys::{
            broker_dynamic::{TOPIC_DEFAULT_SYNONYMS, topic_broker_synonyms},
            group::KAFKA_GROUP_KEYS,
            in_topic_unit,
            kafka_broker::lookup,
        },
    },
    krabka_units::{
        ByteSize, Time,
        convert::{ByteSizeExt as _, TimeExt as _},
    },
    std::time::Duration,
};

/// One `KafkaConfig` key and how this node states its value.
pub struct KafkaStaticKey {
    /// The Kafka key.
    pub(crate) name: &'static str,
    /// The `[runtime]` field that sets it, when there is one.
    pub(crate) runtime_field: Option<&'static str>,
    /// The value this node runs with, as Kafka's `ConfigDef.convertToString`
    /// renders it.
    pub(crate) value: fn(&BrokerConfig) -> String,
}

fn ms(duration: Duration) -> String { duration.as_millis().to_string() }

fn time_ms(time: Time) -> String { time.millis_i64().to_string() }

fn size(bytes: ByteSize) -> String { bytes.bytes_u64().to_string() }

/// An `INT` key that krabka holds as a wider count: Kafka's parse refuses
/// anything past `Integer.MAX_VALUE`, and its own unlimited is that value.
fn int(count: usize) -> String { i32::try_from(count).unwrap_or(i32::MAX).to_string() }

/// Every key this node can state a running value of, other than the ones
/// [`crate::config::StaticConfigOrigins`] tracks with a flag of their own.
pub const KAFKA_STATIC_KEYS: &[KafkaStaticKey] = &[
    KafkaStaticKey::new(
        "group.consumer.session.timeout.ms",
        Some("consumer_group_session_timeout"),
        |c| ms(c.next_gen_consumer_group.session_timeout),
    ),
    KafkaStaticKey::new(
        "group.consumer.heartbeat.interval.ms",
        Some("consumer_group_heartbeat_interval"),
        |c| ms(c.next_gen_consumer_group.heartbeat_interval),
    ),
    KafkaStaticKey::new("group.consumer.assignment.interval.ms", None, |c| {
        ms(c.next_gen_consumer_group.assignment_interval)
    }),
    KafkaStaticKey::new(
        "group.consumer.min.session.timeout.ms",
        Some("consumer_group_min_session_timeout"),
        |c| ms(c.next_gen_consumer_group.min_session_timeout),
    ),
    KafkaStaticKey::new(
        "group.consumer.max.session.timeout.ms",
        Some("consumer_group_max_session_timeout"),
        |c| ms(c.next_gen_consumer_group.max_session_timeout),
    ),
    KafkaStaticKey::new(
        "group.consumer.min.heartbeat.interval.ms",
        Some("consumer_group_min_heartbeat_interval"),
        |c| ms(c.next_gen_consumer_group.min_heartbeat_interval),
    ),
    KafkaStaticKey::new("fetch.max.bytes", Some("fetch_max_bytes"), |c| {
        size(c.fetch_max_bytes)
    }),
    KafkaStaticKey::new(
        "group.consumer.max.heartbeat.interval.ms",
        Some("consumer_group_max_heartbeat_interval"),
        |c| ms(c.next_gen_consumer_group.max_heartbeat_interval),
    ),
    KafkaStaticKey::new(
        "group.consumer.max.size",
        Some("consumer_group_max_size"),
        |c| int(c.next_gen_consumer_group.max_size),
    ),
    KafkaStaticKey::new("group.consumer.migration.policy", None, |c| {
        c.next_gen_consumer_group
            .migration_policy
            .as_str()
            .to_owned()
    }),
    KafkaStaticKey::new(
        "group.initial.rebalance.delay.ms",
        Some("classic_group_initial_rebalance_delay"),
        |c| time_ms(c.classic_group_initial_rebalance_delay),
    ),
    KafkaStaticKey::new(
        "group.min.session.timeout.ms",
        Some("classic_group_min_session_timeout"),
        |c| ms(c.next_gen_consumer_group.classic_min_session_timeout),
    ),
    KafkaStaticKey::new(
        "group.max.session.timeout.ms",
        Some("classic_group_max_session_timeout"),
        |c| ms(c.next_gen_consumer_group.classic_max_session_timeout),
    ),
    KafkaStaticKey::new("group.max.size", Some("classic_group_max_size"), |c| {
        int(c.next_gen_consumer_group.classic_max_size)
    }),
    KafkaStaticKey::new(
        "group.share.session.timeout.ms",
        Some("share_group_session_timeout"),
        |c| ms(c.share_group.session_timeout),
    ),
    KafkaStaticKey::new(
        "group.share.heartbeat.interval.ms",
        Some("share_group_heartbeat_interval"),
        |c| ms(c.share_group.heartbeat_interval),
    ),
    KafkaStaticKey::new("group.share.assignment.interval.ms", None, |c| {
        ms(c.share_group.assignment_interval)
    }),
    KafkaStaticKey::new(
        "group.share.min.session.timeout.ms",
        Some("share_group_min_session_timeout"),
        |c| ms(c.share_group.min_session_timeout),
    ),
    KafkaStaticKey::new(
        "group.share.max.session.timeout.ms",
        Some("share_group_max_session_timeout"),
        |c| ms(c.share_group.max_session_timeout),
    ),
    KafkaStaticKey::new(
        "group.share.min.heartbeat.interval.ms",
        Some("share_group_min_heartbeat_interval"),
        |c| ms(c.share_group.min_heartbeat_interval),
    ),
    KafkaStaticKey::new(
        "group.share.max.heartbeat.interval.ms",
        Some("share_group_max_heartbeat_interval"),
        |c| ms(c.share_group.max_heartbeat_interval),
    ),
    KafkaStaticKey::new("group.share.max.size", Some("share_group_max_size"), |c| {
        int(c.share_group.max_size)
    }),
    KafkaStaticKey::new(
        "group.share.record.lock.duration.ms",
        Some("share_group_record_lock_duration"),
        |c| ms(c.share_group.record_lock_duration),
    ),
    KafkaStaticKey::new(
        "group.share.min.record.lock.duration.ms",
        Some("share_group_min_record_lock_duration"),
        |c| ms(c.share_group.min_record_lock_duration),
    ),
    KafkaStaticKey::new(
        "group.share.max.record.lock.duration.ms",
        Some("share_group_max_record_lock_duration"),
        |c| ms(c.share_group.max_record_lock_duration),
    ),
    KafkaStaticKey::new(
        "group.share.delivery.count.limit",
        Some("share_group_delivery_count_limit"),
        |c| c.share_group.max_delivery_attempts.to_string(),
    ),
    KafkaStaticKey::new(
        "group.share.min.delivery.count.limit",
        Some("share_group_min_delivery_count_limit"),
        |c| c.share_group.min_delivery_count_limit.to_string(),
    ),
    KafkaStaticKey::new(
        "group.share.max.delivery.count.limit",
        Some("share_group_max_delivery_count_limit"),
        |c| c.share_group.max_delivery_count_limit.to_string(),
    ),
    KafkaStaticKey::new(
        "group.share.partition.max.record.locks",
        Some("share_group_partition_max_record_locks"),
        |c| c.share_group.max_inflight_records.to_string(),
    ),
    KafkaStaticKey::new(
        "group.share.min.partition.max.record.locks",
        Some("share_group_min_partition_max_record_locks"),
        |c| c.share_group.min_partition_max_record_locks.to_string(),
    ),
    KafkaStaticKey::new(
        "group.share.max.partition.max.record.locks",
        Some("share_group_max_partition_max_record_locks"),
        |c| c.share_group.max_partition_max_record_locks.to_string(),
    ),
    KafkaStaticKey::new(
        "group.streams.session.timeout.ms",
        Some("streams_group_session_timeout"),
        |c| ms(c.streams_group.session_timeout),
    ),
    KafkaStaticKey::new(
        "group.streams.heartbeat.interval.ms",
        Some("streams_group_heartbeat_interval"),
        |c| ms(c.streams_group.heartbeat_interval),
    ),
    KafkaStaticKey::new("group.streams.assignment.interval.ms", None, |c| {
        ms(c.streams_group.assignment_interval)
    }),
    KafkaStaticKey::new("group.streams.initial.rebalance.delay.ms", None, |c| {
        ms(c.streams_group.initial_rebalance_delay)
    }),
    KafkaStaticKey::new(
        "group.streams.min.session.timeout.ms",
        Some("streams_group_min_session_timeout"),
        |c| ms(c.streams_group.min_session_timeout),
    ),
    KafkaStaticKey::new(
        "group.streams.max.session.timeout.ms",
        Some("streams_group_max_session_timeout"),
        |c| ms(c.streams_group.max_session_timeout),
    ),
    KafkaStaticKey::new(
        "group.streams.min.heartbeat.interval.ms",
        Some("streams_group_min_heartbeat_interval"),
        |c| ms(c.streams_group.min_heartbeat_interval),
    ),
    KafkaStaticKey::new(
        "group.streams.max.heartbeat.interval.ms",
        Some("streams_group_max_heartbeat_interval"),
        |c| ms(c.streams_group.max_heartbeat_interval),
    ),
    KafkaStaticKey::new(
        "group.streams.max.size",
        Some("streams_group_max_size"),
        |c| int(c.streams_group.max_size),
    ),
    KafkaStaticKey::new(
        "group.streams.num.standby.replicas",
        Some("streams_group_num_standby_replicas"),
        |c| c.streams_group.num_standby_replicas.to_string(),
    ),
    KafkaStaticKey::new(
        "group.streams.num.warmup.replicas",
        Some("streams_group_num_warmup_replicas"),
        |c| c.streams_group.num_warmup_replicas.to_string(),
    ),
    KafkaStaticKey::new(
        "group.streams.acceptable.recovery.lag",
        Some("streams_group_acceptable_recovery_lag"),
        |c| c.streams_group.acceptable_recovery_lag.to_string(),
    ),
    KafkaStaticKey::new(
        "group.streams.task.offset.interval.ms",
        Some("streams_group_task_offset_interval"),
        |c| ms(c.streams_group.task_offset_interval),
    ),
    KafkaStaticKey::new(
        "group.streams.rack.aware.assignment.tags",
        Some("streams_group_rack_aware_assignment_tags"),
        |c| c.streams_group.rack_aware_assignment_tags.join(","),
    ),
    KafkaStaticKey::new(
        "group.streams.assignors",
        Some("streams_group_assignor"),
        |c| c.streams_group.assignor.config_name().to_owned(),
    ),
    KafkaStaticKey::new(
        "share.coordinator.load.buffer.size",
        Some("share_coordinator_load_buffer_size"),
        |c| size(c.share_coordinator_load_buffer_size),
    ),
    KafkaStaticKey::new(
        "share.coordinator.state.topic.num.partitions",
        Some("share_state_num_partitions"),
        |c| c.share_coordinator.state_topic_num_partitions.to_string(),
    ),
    KafkaStaticKey::new(
        "share.coordinator.state.topic.replication.factor",
        Some("share_state_replication_factor"),
        |c| {
            c.share_coordinator
                .state_topic_replication_factor
                .to_string()
        },
    ),
    KafkaStaticKey::new(
        "share.coordinator.state.topic.segment.bytes",
        Some("share_state_segment_bytes"),
        |c| size(c.share_coordinator.state_topic_segment_bytes),
    ),
    KafkaStaticKey::new(
        "share.coordinator.state.topic.min.isr",
        Some("share_state_min_isr"),
        |c| c.share_coordinator.state_topic_min_isr.to_string(),
    ),
    KafkaStaticKey::new(
        "share.coordinator.snapshot.update.records.per.snapshot",
        Some("share_snapshot_update_records_per_snapshot"),
        |c| {
            c.share_coordinator
                .snapshot_update_records_per_snapshot
                .to_string()
        },
    ),
    KafkaStaticKey::new(
        "share.coordinator.write.timeout.ms",
        Some("share_coordinator_write_timeout"),
        |c| ms(c.share_coordinator.write_timeout),
    ),
    KafkaStaticKey::new(
        "share.coordinator.state.topic.prune.interval.ms",
        Some("share_state_prune_interval"),
        |c| ms(c.share_coordinator.state_topic_prune_interval),
    ),
    KafkaStaticKey::new(
        "share.coordinator.cold.partition.snapshot.interval.ms",
        Some("share_cold_partition_snapshot_interval"),
        |c| ms(c.share_coordinator.cold_partition_snapshot_interval),
    ),
    KafkaStaticKey::new(
        "share.coordinator.state.topic.compression.codec",
        Some("share_state_compression_codec"),
        |c| {
            c.share_coordinator
                .state_topic_compression_codec
                .as_attribute_bits()
                .to_string()
        },
    ),
    KafkaStaticKey::new(
        "share.coordinator.threads",
        Some("share_coordinator_threads"),
        |c| c.share_coordinator.threads.to_string(),
    ),
    KafkaStaticKey::new(
        "share.coordinator.append.linger.ms",
        Some("share_coordinator_append_linger_ms"),
        |c| {
            c.share_coordinator
                .append_linger
                .map_or_else(|| "-1".to_owned(), ms)
        },
    ),
    KafkaStaticKey::new(
        "share.coordinator.cached.buffer.max.bytes",
        Some("share_coordinator_cached_buffer_max_bytes"),
        |c| size(c.share_coordinator.cached_buffer_max_bytes),
    ),
    KafkaStaticKey::new(
        "offsets.topic.num.partitions",
        Some("offsets_topic_num_partitions"),
        |c| c.offsets_topic_num_partitions.to_string(),
    ),
    KafkaStaticKey::new(
        "offsets.topic.replication.factor",
        Some("offsets_topic_replication_factor"),
        |c| c.offsets_topic_replication_factor.to_string(),
    ),
    KafkaStaticKey::new(
        "offsets.topic.segment.bytes",
        Some("offsets_topic_segment_bytes"),
        |c| size(c.offsets_topic_segment_bytes),
    ),
    KafkaStaticKey::new(
        "transaction.state.log.num.partitions",
        Some("transaction_state_num_partitions"),
        |c| c.transaction_state_num_partitions.to_string(),
    ),
    KafkaStaticKey::new(
        "transaction.state.log.replication.factor",
        Some("transaction_state_replication_factor"),
        |c| c.transaction_state_replication_factor.to_string(),
    ),
    KafkaStaticKey::new(
        "transaction.state.log.segment.bytes",
        Some("transaction_state_segment_bytes"),
        |c| size(c.transaction_state_segment_bytes),
    ),
    KafkaStaticKey::new(
        "transaction.state.log.min.isr",
        Some("transaction_state_min_isr"),
        |c| c.transaction_state_min_isr.to_string(),
    ),
    KafkaStaticKey::new(
        "transaction.max.timeout.ms",
        Some("transaction_max_timeout"),
        |c| time_ms(c.transaction_max_timeout),
    ),
    KafkaStaticKey::new(
        "transaction.partition.verification.enable",
        Some("transaction_partition_verification_enable"),
        |c| c.transaction_partition_verification_enable.to_string(),
    ),
    KafkaStaticKey::new(
        "transaction.abort.timed.out.transaction.cleanup.interval.ms",
        Some("txn_abort_cleanup_interval"),
        |c| time_ms(c.txn_abort_cleanup_interval),
    ),
    KafkaStaticKey::new(
        "socket.request.max.bytes",
        Some("socket_request_max"),
        |c| size(c.socket_request_max),
    ),
    KafkaStaticKey::new(
        "socket.send.buffer.bytes",
        Some("socket_send_buffer"),
        |c| size(c.socket_send_buffer),
    ),
    KafkaStaticKey::new(
        "socket.receive.buffer.bytes",
        Some("socket_receive_buffer"),
        |c| size(c.socket_receive_buffer),
    ),
    KafkaStaticKey::new("queued.max.requests", Some("queued_max_requests"), |c| {
        int(c.queued_max_requests)
    }),
    KafkaStaticKey::new(
        "queued.max.request.bytes",
        Some("queued_max_request_bytes"),
        |c| {
            c.queued_max_request_bytes
                .map_or_else(|| "-1".to_owned(), size)
        },
    ),
    KafkaStaticKey::new(
        "max.request.partition.size.limit",
        Some("max_request_partition_size_limit"),
        |c| c.max_request_partition_size_limit.to_string(),
    ),
    KafkaStaticKey::new("max.connections", Some("max_connections"), |c| {
        int(c.max_connections)
    }),
    KafkaStaticKey::new(
        "max.connections.per.ip",
        Some("max_connections_per_ip"),
        |c| int(c.max_connections_per_ip),
    ),
    KafkaStaticKey::new(
        "max.incremental.fetch.session.cache.slots",
        Some("max_incremental_fetch_session_cache_slots"),
        |c| int(c.max_incremental_fetch_session_cache_slots),
    ),
    KafkaStaticKey::new(
        "producer.id.expiration.ms",
        Some("producer_id_expiration"),
        |c| time_ms(c.producer_id_expiration),
    ),
    KafkaStaticKey::new(
        "producer.id.expiration.check.interval.ms",
        Some("producer_id_expiration_scan_interval"),
        |c| time_ms(c.producer_id_expiration_scan_interval),
    ),
    KafkaStaticKey::new("num.replica.fetchers", Some("replica_fetchers"), |c| {
        int(c.replication.fetchers)
    }),
    KafkaStaticKey::new(
        "replica.fetch.max.bytes",
        Some("replication_fetch_max"),
        |c| size(c.replication.fetch_max),
    ),
    KafkaStaticKey::new(
        "replica.fetch.response.max.bytes",
        Some("replication_fetch_response_max"),
        |c| size(c.replication.fetch_response_max),
    ),
    KafkaStaticKey::new(
        "replica.lag.time.max.ms",
        Some("replica_lag_time_max"),
        |c| time_ms(c.replica_lag_time_max),
    ),
    KafkaStaticKey::new(
        "leader.imbalance.check.interval.seconds",
        Some("leader_imbalance_check_interval"),
        |c| (c.leader_imbalance_check_interval.millis_i64() / 1_000).to_string(),
    ),
    KafkaStaticKey::new(
        "controller.quorum.election.timeout.ms",
        Some("controller_election_timeout"),
        |c| time_ms(c.controller_election_timeout),
    ),
    KafkaStaticKey::new(
        "controller.quorum.fetch.timeout.ms",
        Some("controller_fetch_timeout"),
        |c| time_ms(c.controller_fetch_timeout),
    ),
    KafkaStaticKey::new(
        "metadata.log.max.record.bytes.between.snapshots",
        Some("metadata_max_bytes_between_snapshots"),
        |c| size(c.metadata_max_bytes_between_snapshots),
    ),
    KafkaStaticKey::new(
        "metadata.log.max.snapshot.interval.ms",
        Some("metadata_max_snapshot_interval"),
        |c| time_ms(c.metadata_max_snapshot_interval),
    ),
    KafkaStaticKey::new("log.roll.jitter.ms", None, |c| {
        time_ms(c.log_config.segment_jitter)
    }),
    KafkaStaticKey::new("log.segment.delete.delay.ms", None, |c| {
        time_ms(c.log_config.file_delete_delay)
    }),
    KafkaStaticKey::new(
        "log.flush.start.offset.checkpoint.interval.ms",
        Some("log_flush_start_offset_checkpoint_interval"),
        |c| time_ms(c.log_config.start_offset_checkpoint_interval),
    ),
    KafkaStaticKey::new("log.flush.interval.messages", None, |c| {
        c.log_config
            .flush_messages
            .map_or_else(|| i64::MAX.to_string(), |n| n.to_string())
    }),
    KafkaStaticKey::new("log.flush.interval.ms", None, |c| {
        c.log_config
            .flush_interval
            .map_or_else(|| i64::MAX.to_string(), time_ms)
    }),
    KafkaStaticKey::new("compression.gzip.level", None, |c| {
        c.log_config.compression_gzip_level.to_string()
    }),
    KafkaStaticKey::new("compression.lz4.level", None, |c| {
        c.log_config.compression_lz4_level.to_string()
    }),
    KafkaStaticKey::new("compression.zstd.level", None, |c| {
        c.log_config.compression_zstd_level.to_string()
    }),
    KafkaStaticKey::new(
        "metadata.log.segment.bytes",
        Some("metadata_log_segment_bytes"),
        |c| size(c.metadata_log.segment_size),
    ),
    KafkaStaticKey::new(
        "metadata.log.segment.ms",
        Some("metadata_log_segment_roll_interval"),
        |c| time_ms(c.metadata_log.segment_roll_interval),
    ),
    KafkaStaticKey::new(
        "metadata.max.retention.bytes",
        Some("metadata_max_retention_bytes"),
        |c| {
            c.metadata_log
                .max_retention_size
                .map_or_else(|| "-1".to_owned(), size)
        },
    ),
    KafkaStaticKey::new(
        "metadata.max.retention.ms",
        Some("metadata_max_retention"),
        |c| {
            c.metadata_log
                .max_retention
                .map_or_else(|| "-1".to_owned(), time_ms)
        },
    ),
    KafkaStaticKey::new(
        "metadata.max.idle.interval.ms",
        Some("metadata_max_idle_interval"),
        |c| time_ms(c.metadata_log.max_idle_interval),
    ),
    KafkaStaticKey::new(
        "broker.heartbeat.interval.ms",
        Some("heartbeat_interval"),
        |c| time_ms(c.heartbeat_interval),
    ),
    KafkaStaticKey::new(
        "broker.session.timeout.ms",
        Some("heartbeat_timeout"),
        |c| time_ms(c.heartbeat_timeout),
    ),
    KafkaStaticKey::new("remote.log.storage.system.enable", None, |c| {
        c.remote_storage_backend.is_some().to_string()
    }),
    KafkaStaticKey::new(
        "remote.log.manager.task.interval.ms",
        Some("remote_log_manager_interval"),
        |c| time_ms(c.remote_log_manager_interval),
    ),
    KafkaStaticKey::new("remote.log.index.file.cache.total.size.bytes", None, |c| {
        size(c.remote_index_cache_size)
    }),
    KafkaStaticKey::new("remote.log.manager.copier.thread.pool.size", None, |c| {
        int(c.remote_copier_threads)
    }),
    KafkaStaticKey::new(
        "remote.log.manager.expiration.thread.pool.size",
        None,
        |c| int(c.remote_expiration_threads),
    ),
    KafkaStaticKey::new("remote.log.reader.threads", None, |c| {
        int(c.remote_reader_threads)
    }),
    KafkaStaticKey::new("remote.log.reader.max.pending.tasks", None, |c| {
        int(c.remote_reader_max_pending_tasks)
    }),
    KafkaStaticKey::new(
        "delegation.token.max.lifetime.ms",
        Some("delegation_token_max_lifetime"),
        |c| time_ms(c.delegation_token_max_lifetime),
    ),
    KafkaStaticKey::new(
        "delegation.token.expiry.check.interval.ms",
        Some("delegation_token_expiry_check_interval"),
        |c| time_ms(c.delegation_token_expiry_check_interval),
    ),
    KafkaStaticKey::new(
        "delegation.token.expiry.time.ms",
        Some("delegation_token_default_renew_period"),
        |c| time_ms(c.delegation_token_default_renew_period),
    ),
];

impl KafkaStaticKey {
    const fn new(
        name: &'static str,
        runtime_field: Option<&'static str>,
        value: fn(&BrokerConfig) -> String,
    ) -> Self {
        Self {
            name,
            runtime_field,
            value,
        }
    }

    /// Kafka's effective default, following topic broker synonyms with unit
    /// conversion when the direct row has no default, or the group default
    /// for a group synonym that Kafka 4.3.1 does not define.
    pub(crate) fn kafka_default(&self) -> Option<String> {
        lookup(self.name)
            .and_then(|row| row.default)
            .or_else(|| {
                KAFKA_GROUP_KEYS
                    .iter()
                    .find(|group_key| group_key.broker_synonym == Some(self.name))
                    .and_then(|group_key| group_key.default)
            })
            .map(str::to_owned)
            .or_else(|| {
                let (_, topic) = TOPIC_DEFAULT_SYNONYMS
                    .iter()
                    .find(|(broker, _)| *broker == self.name)?;
                topic_broker_synonyms(topic)
                    .iter()
                    .find_map(|synonym| in_topic_unit(topic, synonym.name, synonym.default?))
            })
    }

    /// The value `config` runs the key with, when it is a static setting: the
    /// operator named the key, or the node runs another value than Kafka's
    /// default. `None` is a key `DescribeConfigs` reports at its default.
    pub(crate) fn static_value(&self, config: &BrokerConfig) -> Option<String> {
        // Kafka 4.3.1 has no `group.streams.assignors` broker key. Krabka's
        // Streams assignor setting is part of trunk's GroupConfig surface and
        // is only visible when Kafka's unstable API versions are enabled.
        if self.name == "group.streams.assignors"
            && config.features.unstable_api_versions == UnstableApiVersions::Disabled
        {
            return None;
        }
        let value = (self.value)(config);
        let supplied = config
            .static_config_origins
            .supplied_kafka_keys
            .contains(self.name);
        (supplied || self.kafka_default().as_deref() != Some(value.as_str())).then_some(value)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{api_catalog::UnstableApiVersions, file_config::RuntimeFileConfig},
        assert2::check,
        krabka_units::{bytes, millis},
        rstest_bdd::datatable::Rows,
        serde_json::Value,
        std::collections::BTreeSet,
    };

    #[derive(Default)]
    struct StaticConfigScenario {
        config: BrokerConfig,
        selected: Vec<&'static str>,
        without_defaults: Option<Vec<&'static str>>,
        names: Option<Vec<&'static str>>,
        missing_runtime_fields: Option<Vec<&'static str>>,
        stated: Option<Vec<(String, String)>>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct StaticSetting {
        key: String,
        value: String,
    }

    #[rstest::fixture]
    fn static_config() -> StaticConfigScenario {
        // Operator changes and their described values are isolated per scenario.
        StaticConfigScenario::default()
    }

    #[rstest_bdd_macros::given(
        "the broker has no operator-supplied static configuration overrides"
    )]
    fn default_static_configuration(static_config: &mut StaticConfigScenario) {
        static_config.config = BrokerConfig::default();
        static_config.selected.clear();
    }

    #[rstest_bdd_macros::given("the static-config query selects metadata-log settings")]
    fn select_metadata_log_settings(static_config: &mut StaticConfigScenario) {
        static_config.selected = vec![
            "metadata.log.segment.bytes",
            "metadata.log.segment.ms",
            "metadata.max.retention.bytes",
            "metadata.max.retention.ms",
            "metadata.max.idle.interval.ms",
        ];
    }

    #[rstest_bdd_macros::given("the static-config query selects follower-fetch limits")]
    fn select_follower_fetch_settings(static_config: &mut StaticConfigScenario) {
        static_config.selected = vec![
            "replica.fetch.max.bytes",
            "replica.fetch.response.max.bytes",
        ];
    }

    #[rstest_bdd_macros::given("the static-config query selects the streams assignor list")]
    fn select_streams_assignors(static_config: &mut StaticConfigScenario) {
        static_config.selected = vec!["group.streams.assignors"];
    }

    #[rstest_bdd_macros::given("unstable API versions are enabled for static configuration")]
    fn enable_unstable_static_configuration(static_config: &mut StaticConfigScenario) {
        static_config.config.features.unstable_api_versions = UnstableApiVersions::Enabled;
    }

    #[rstest_bdd_macros::given("the metadata log runs with these changed settings:")]
    fn metadata_log_overrides(
        static_config: &mut StaticConfigScenario,
        #[datatable] rows: Rows<StaticSetting>,
    ) {
        let config = &mut static_config.config.metadata_log;
        for row in rows {
            let value = row.value.as_str();
            match row.key.as_str() {
                "metadata.log.segment.bytes" => {
                    config.segment_size =
                        bytes(value.parse::<u32>().expect("segment bytes are u32"));
                }
                "metadata.log.segment.ms" => {
                    config.segment_roll_interval =
                        millis(value.parse::<u32>().expect("roll interval is milliseconds"));
                }
                "metadata.max.retention.bytes" => {
                    config.max_retention_size = if value == "-1" {
                        None
                    } else {
                        Some(bytes(
                            value.parse::<u32>().expect("retention bytes are u32"),
                        ))
                    }
                }
                "metadata.max.retention.ms" => {
                    config.max_retention = if value == "-1" {
                        None
                    } else {
                        Some(millis(
                            value.parse::<u32>().expect("retention is milliseconds"),
                        ))
                    }
                }
                "metadata.max.idle.interval.ms" => {
                    config.max_idle_interval =
                        millis(value.parse::<u32>().expect("idle interval is milliseconds"));
                }
                key => panic!("unknown metadata log setting {key}"),
            }
        }
    }

    #[rstest_bdd_macros::given(
        "the follower requests at most {partition} bytes per partition and {response} bytes per response"
    )]
    fn follower_fetch_limits(
        static_config: &mut StaticConfigScenario,
        partition: u32,
        response: u32,
    ) {
        static_config.config.replication.fetch_max = bytes(partition);
        static_config.config.replication.fetch_response_max = bytes(response);
    }

    #[rstest_bdd_macros::when("the static Kafka key catalog is enumerated")]
    fn enumerate_static_keys(static_config: &mut StaticConfigScenario) {
        static_config.names = Some(KAFKA_STATIC_KEYS.iter().map(|key| key.name).collect());
        static_config.without_defaults = Some(
            KAFKA_STATIC_KEYS
                .iter()
                .filter(|key| key.kafka_default().is_none())
                .map(|key| key.name)
                .collect(),
        );
    }

    #[rstest_bdd_macros::when(
        "the catalog's runtime field names are resolved against the runtime configuration schema"
    )]
    fn resolve_static_runtime_fields(static_config: &mut StaticConfigScenario) {
        let Value::Object(fields) = serde_json::to_value(RuntimeFileConfig::default())
            .expect("serialize the runtime table")
        else {
            panic!("the runtime table serializes as an object");
        };
        static_config.missing_runtime_fields = Some(
            KAFKA_STATIC_KEYS
                .iter()
                .filter_map(|key| key.runtime_field)
                .filter(|field| !fields.contains_key(*field))
                .collect(),
        );
    }

    #[rstest_bdd_macros::when("the broker describes the selected static Kafka settings")]
    fn describe_static_settings(static_config: &mut StaticConfigScenario) {
        static_config.stated = Some(
            KAFKA_STATIC_KEYS
                .iter()
                .filter(|key| {
                    static_config.selected.is_empty() || static_config.selected.contains(&key.name)
                })
                .filter_map(|key| {
                    key.static_value(&static_config.config)
                        .map(|value| (key.name.to_owned(), value))
                })
                .collect(),
        );
    }

    #[rstest_bdd_macros::then("only group.streams.assignors lacks a released Kafka default")]
    fn known_static_defaults(static_config: &StaticConfigScenario) {
        check!(static_config.without_defaults.as_ref() == Some(&vec!["group.streams.assignors"]));
    }

    #[rstest_bdd_macros::then("every static Kafka key name occurs exactly once")]
    fn unique_static_key_names(static_config: &StaticConfigScenario) {
        let names = static_config
            .names
            .as_ref()
            .expect("the catalog was enumerated");
        let unique: BTreeSet<_> = names.iter().collect();
        check!(unique.len() == names.len());
    }

    #[rstest_bdd_macros::then("every referenced runtime configuration field exists")]
    fn present_static_runtime_fields(static_config: &StaticConfigScenario) {
        let missing = static_config
            .missing_runtime_fields
            .as_ref()
            .expect("runtime fields were resolved");
        check!(missing.is_empty(), "{missing:?}");
    }

    #[rstest_bdd_macros::then("the ordered stated static settings are:")]
    fn stated_static_settings(
        static_config: &StaticConfigScenario,
        #[datatable] rows: Rows<StaticSetting>,
    ) {
        let expected: Vec<_> = rows.into_iter().map(|row| (row.key, row.value)).collect();
        check!(static_config.stated.as_ref() == Some(&expected));
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/config/kafka_static_keys.feature",
        fixtures = [static_config: StaticConfigScenario]
    );
}
