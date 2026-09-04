//! The `[runtime]` TOML table and the entry point that applies it.
//!
//! [`RuntimeFileConfig`] mirrors every operational knob the broker reads from
//! `[runtime]`. Its [`apply_to`][RuntimeFileConfig::apply_to] method dispatches
//! to the per-domain appliers in the sibling `runtime_*` modules, which hold
//! the assignments themselves.

use krabka_units::{ByteSize, Ratio, Time};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::FileConfigError;

/// Validated operational policy loaded from `[runtime]`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFileConfig {
    /// Maximum time the broker waits for a controller leader during startup.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub startup_leader_wait_timeout: Option<Time>,
    /// Initial delay between broker self-registration attempts.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub self_registration_backoff_min: Option<Time>,
    /// Maximum delay between broker self-registration attempts.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub self_registration_backoff_max: Option<Time>,
    /// Cadence of the KIP-853 observer promotion poll.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub observer_poll_interval: Option<Time>,
    /// Cadence at which the audit spool replays records it could not append.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub audit_spool_replay_interval: Option<Time>,
    /// Cadence of the audit statistics poll.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub audit_stats_poll_interval: Option<Time>,
    /// Maximum wait for the audit partition to become available.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub audit_partition_wait_timeout: Option<Time>,
    /// Cadence of broker liveness maintenance.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub liveness_tick_interval: Option<Time>,
    /// Cadence at which broker gauges are refreshed.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub gauge_poll_interval: Option<Time>,
    /// Cadence of in-sync-replica maintenance.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub isr_scan_interval: Option<Time>,
    /// Cadence of log cleaner maintenance.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub cleaner_interval: Option<Time>,
    /// Retry delay after a KIP-113 future-log move fails.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub future_log_move_retry_backoff: Option<Time>,
    /// Cadence at which the KIP-714 client-metrics cache evicts entries.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub client_metrics_eviction_tick: Option<Time>,
    /// Minimum age at which a client-metrics entry counts as stale.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub client_metrics_stale_floor: Option<Time>,
    /// Default KIP-714 client telemetry subscription push interval.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub client_metrics_default_interval: Option<Time>,
    /// Maximum accepted KIP-714 client telemetry payload size.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub client_metrics_telemetry_max: Option<ByteSize>,
    /// Lifetime of a Prometheus client-metrics snapshot.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub client_metrics_prom_snapshot_ttl: Option<Time>,
    /// Cadence of KIP-405 remote-log metadata reconciliation.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub rlmm_reconcile_tick: Option<Time>,
    /// Initial retry delay while remote-log metadata bootstrap is incomplete.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub rlmm_bootstrap_backoff_initial: Option<Time>,
    /// Maximum retry delay while remote-log metadata bootstrap is incomplete.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub rlmm_bootstrap_backoff_max: Option<Time>,
    /// Maximum KIP-612 connection-creation quota delay.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub connection_creation_throttle_max: Option<Time>,
    /// Timeout for one OPA authorization request.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub opa_http_timeout: Option<Time>,
    /// Timeout for one schema-registry request.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub schema_registry_http_timeout: Option<Time>,
    /// Timeout for one OAuth JWKS fetch.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub oauth_jwks_http_timeout: Option<Time>,
    /// Retry delay between KIP-853 dynamic-quorum auto-join attempts.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub auto_join_retry_backoff: Option<Time>,
    /// Timeout carried by a dynamic-quorum `AddRaftVoter` request.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub auto_join_voter_request_timeout: Option<Time>,
    /// How many fetchers this broker runs per leader it follows, Kafka's
    /// `num.replica.fetchers`. Every partition followed from one leader is
    /// hashed onto one of that leader's fetchers, and each fetcher holds one
    /// connection and sends one batched `Fetch` per round.
    #[schemars(range(min = 1))]
    pub replica_fetchers: Option<usize>,
    /// Maximum bytes a follower requests from a leader in one replication
    /// fetch. It reaches the leader as the fetch request's `max_bytes`.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub replication_fetch_max: Option<ByteSize>,
    /// Maximum time a leader holds a replication fetch that is not yet
    /// satisfied. It reaches the leader as the fetch request's `max_wait_ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_fetch_max_wait: Option<Time>,
    /// Minimum bytes that satisfy a replication fetch. It reaches the leader
    /// as the fetch request's `min_bytes`, which the leader honours as a
    /// floor.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub replication_fetch_min: Option<ByteSize>,
    /// Delay after a follower exhausts its replication throttle budget.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_throttle_exhausted_backoff: Option<Time>,
    /// Retry delay after sending a replication request fails.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_send_error_backoff: Option<Time>,
    /// Retry delay when the leader does not yet know the topic.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_unknown_topic_retry_delay: Option<Time>,
    /// Retry delay after a leader-epoch fence.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_epoch_fence_backoff: Option<Time>,
    /// Retry delay after an unexpected replication error.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_unexpected_error_backoff: Option<Time>,
    /// Initial delay before a follower reconnects to a leader.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_reconnect_initial_delay: Option<Time>,
    /// Maximum delay between leader reconnection attempts.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replication_reconnect_delay_cap: Option<Time>,
    /// Cadence of the consumer-group session expiry scan.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub coordinator_session_expiry_tick: Option<Time>,
    /// Maximum wait for coordinator shutdown acknowledgements.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub coordinator_shutdown_ack_timeout: Option<Time>,
    /// Default KIP-848 consumer-group session timeout, Kafka's
    /// `group.consumer.session.timeout.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_session_timeout: Option<Time>,
    /// Default KIP-848 consumer-group heartbeat interval, Kafka's
    /// `group.consumer.heartbeat.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_heartbeat_interval: Option<Time>,
    /// Lower bound on the negotiated consumer-group session timeout, Kafka's
    /// `group.consumer.min.session.timeout.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_min_session_timeout: Option<Time>,
    /// Upper bound on the negotiated consumer-group session timeout, Kafka's
    /// `group.consumer.max.session.timeout.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_max_session_timeout: Option<Time>,
    /// Lower bound on the negotiated consumer-group heartbeat interval,
    /// Kafka's `group.consumer.min.heartbeat.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_min_heartbeat_interval: Option<Time>,
    /// Upper bound on the negotiated consumer-group heartbeat interval,
    /// Kafka's `group.consumer.max.heartbeat.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub consumer_group_max_heartbeat_interval: Option<Time>,
    /// Maximum number of members in one consumer group, Kafka's
    /// `group.consumer.max.size`.
    pub consumer_group_max_size: Option<usize>,
    /// Initial delay before a classic group begins rebalancing, Kafka's
    /// `group.initial.rebalance.delay.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub classic_group_initial_rebalance_delay: Option<Time>,
    /// Maximum time a classic-protocol follower waits for its `SyncGroup`
    /// assignment.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub sync_group_follower_wait: Option<Time>,
    /// Replica-log collection deadline under the aggressive unclean-recovery
    /// strategy.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub unclean_recovery_aggressive_deadline: Option<Time>,
    /// Replica-log collection deadline under the balanced unclean-recovery
    /// strategy.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub unclean_recovery_balanced_deadline: Option<Time>,
    /// Deadline for an operator-triggered unclean recovery.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub operator_recovery_deadline: Option<Time>,
    /// Maximum client quota throttle delay, which bounds how long one response
    /// mutes a client. Equivalent to Kafka's `quota.window.size.seconds *
    /// (quota.window.num - 1)`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub quota_throttle_max: Option<Time>,
    /// Time window that sizes the client byte-rate quota token bucket's burst
    /// capacity. Equivalent to Kafka's sampling window `quota.window.num *
    /// quota.window.size.seconds`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub quota_window: Option<Time>,
    /// Time window whose throughput defines the KIP-599 controller-mutation
    /// quota burst capacity.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub controller_mutation_quota_window: Option<Time>,
    /// Maximum self-registration attempts before startup fails.
    pub self_registration_max_attempts: Option<u32>,
    /// Maximum bytes fetched by one metadata observer request.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub observer_fetch_max: Option<ByteSize>,
    /// Capacity of the asynchronous audit event queue.
    pub audit_event_queue_capacity: Option<usize>,
    /// Number of offsets included in one audit tail request.
    pub audit_tail_window_offsets: Option<i64>,
    /// Maximum bytes read by one audit tail request.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub audit_tail_read_max: Option<ByteSize>,
    /// Maximum wait for `__consumer_offsets` metadata before a request fails.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub offsets_topic_metadata_wait_timeout: Option<Time>,
    /// Number of missed push intervals after which client metrics expire.
    pub client_metrics_stale_push_intervals: Option<u32>,
    /// Capacity of the client-metrics OTLP forwarding queue.
    pub client_metrics_otlp_queue_capacity: Option<usize>,
    /// Mailbox capacity of each coordinator actor.
    pub coordinator_actor_mailbox_capacity: Option<usize>,
    /// Local replica count used in diskless WAL mode.
    pub diskless_wal_local_replica_count: Option<usize>,
    /// Cadence of diskless WAL flushes to the object store.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub diskless_wal_flush_interval: Option<Time>,
    /// Maximum bytes included in one diskless WAL object-store flush.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub diskless_wal_flush_max_size: Option<ByteSize>,
    /// Broker-wide byte ceiling for quorum-committed diskless hot-tail
    /// batches.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub diskless_wal_hot_tail_max_size: Option<ByteSize>,
    /// Committed offsets retained behind the diskless WAL trim frontier.
    pub diskless_wal_trim_safety_lag: Option<i64>,
    /// Maximum wait for a published diskless WAL index record to be projected.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub diskless_wal_index_projection_timeout: Option<Time>,
    /// Capacity of the unclean-recovery work queue.
    pub unclean_recovery_queue_capacity: Option<usize>,
    /// Maximum bytes read by one share-state recovery read.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub share_recovery_read_max: Option<ByteSize>,
    /// Ceiling on the share-session cache when the group count is unlimited.
    pub share_session_cache_max_when_unlimited: Option<usize>,
    /// Cap on the initial allocation a decoded or raw segment read makes.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub log_read_buffer_cap: Option<ByteSize>,
    /// Size of the window a timestamp search reads the log in.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub log_timestamp_scan_window: Option<ByteSize>,
    /// Roll the active segment once it grows past this. Kafka's
    /// `log.segment.bytes`, the broker default for a topic's `segment.bytes`.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub log_segment_bytes: Option<ByteSize>,
    /// Kafka's broker-wide `message.max.bytes`: the largest record batch a
    /// topic that sets no `max.message.bytes` accepts.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub message_max_bytes: Option<ByteSize>,
    /// Declared bound on how far this broker's clock can be from true time. It
    /// has an effect only under the scheduled delivery policy: a batch becomes
    /// visible once `max_timestamp + log_delivery_clock_uncertainty <= now`,
    /// so delivery is never early and is late by at most twice this bound.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub log_delivery_clock_uncertainty: Option<Time>,
    /// Maximum encoded request size accepted from a socket, Kafka's
    /// `socket.request.max.bytes`.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub socket_request_max: Option<ByteSize>,
    /// Maximum number of queued requests allowed in the broker dispatch queue,
    /// Kafka's `queued.max.requests`.
    pub queued_max_requests: Option<usize>,
    /// Maximum byte size across all queued requests before accepting
    /// additional requests is paused, Kafka's `queued.max.request.bytes`.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub queued_max_request_bytes: Option<ByteSize>,
    /// Minimum response size eligible for a `sendfile` kernel drain. Smaller
    /// responses go through the `pread` and write copy.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub sendfile_min: Option<ByteSize>,
    /// Broker socket send-buffer size, Kafka's `socket.send.buffer.bytes`.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub socket_send_buffer: Option<ByteSize>,
    /// Broker socket receive-buffer size, Kafka's
    /// `socket.receive.buffer.bytes`.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub socket_receive_buffer: Option<ByteSize>,
    /// Maximum encoded ACL principal length.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub acl_max_principal: Option<ByteSize>,
    /// Maximum encoded ACL resource-name length.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub acl_max_resource_name: Option<ByteSize>,
    /// Maximum accepted decompression ratio for a KIP-714 telemetry payload.
    #[serde(default, with = "krabka_units::serde_units::human::option_ratio")]
    #[schemars(with = "Option<crate::file_config::schema_units::Ratio>")]
    pub telemetry_max_decompression_ratio: Option<krabka_units::Ratio>,
    /// Minimum decompressed-output allowance granted to a telemetry payload,
    /// whatever the ratio bound computes.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub telemetry_decompressed_output_floor: Option<ByteSize>,
    /// Maximum decompressed-output allowance granted to a telemetry payload.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub telemetry_decompressed_output_ceiling: Option<ByteSize>,
    /// Maximum accepted decompression ratio for a produced record batch.
    #[serde(default, with = "krabka_units::serde_units::human::option_ratio")]
    #[schemars(with = "Option<crate::file_config::schema_units::Ratio>")]
    pub record_decompression_max_ratio: Option<Ratio>,
    /// Minimum decompressed-output allowance granted to a record batch,
    /// whatever the ratio bound computes.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub record_decompression_output_floor: Option<ByteSize>,
    /// Maximum decompressed-output allowance granted to a record batch.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub record_decompression_output_ceiling: Option<ByteSize>,
    /// TLS SNI and SASL server name used for outbound inter-broker
    /// connections.
    pub inter_broker_server_name: Option<String>,
    /// How long a producer id may stay idle before its state expires, Kafka's
    /// `producer.id.expiration.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub producer_id_expiration: Option<Time>,
    /// Cadence of the producer-state expiry scan, Kafka's
    /// `producer.id.expiration.check.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub producer_id_expiration_scan_interval: Option<Time>,
    /// Maximum number of produce requests combined into one append group.
    pub max_produce_group: Option<usize>,
    /// Capacity of each partition-writer request queue.
    pub partition_writer_queue_depth: Option<usize>,
    /// Broker default for a topic's `min.insync.replicas`, Kafka's
    /// `min.insync.replicas`. A topic override wins over it.
    pub default_min_insync_replicas: Option<i32>,
    /// Bytes copied per read during a KIP-113 future-log move.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub future_log_move_read_chunk: Option<ByteSize>,
    /// Partition count of the `__share_group_state` internal topic, Kafka's
    /// `share.coordinator.state.topic.num.partitions`.
    pub share_state_num_partitions: Option<i32>,
    /// Replication factor of the `__share_group_state` internal topic, Kafka's
    /// `share.coordinator.state.topic.replication.factor`.
    pub share_state_replication_factor: Option<i16>,
    /// Partition count of the `__consumer_offsets` internal topic, Kafka's
    /// `offsets.topic.num.partitions`.
    pub offsets_topic_num_partitions: Option<i32>,
    /// Replication factor of the `__consumer_offsets` internal topic, Kafka's
    /// `offsets.topic.replication.factor`.
    pub offsets_topic_replication_factor: Option<i16>,
    /// How long a committed consumer offset is kept after its group becomes
    /// empty, Kafka's `offsets.retention.minutes`. It must be a whole number
    /// of minutes.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub offsets_retention: Option<Time>,
    /// Cadence of the expired-offset sweep, Kafka's
    /// `offsets.retention.check.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub offsets_retention_check_interval: Option<Time>,
    /// Partition count of the `__transaction_state` internal topic, Kafka's
    /// `transaction.state.log.num.partitions`.
    pub transaction_state_num_partitions: Option<i32>,
    /// Maximum bytes requested by one `__transaction_state` recovery read.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub transaction_recovery_read_max: Option<ByteSize>,
    /// Replication factor of the `__transaction_state` internal topic, Kafka's
    /// `transaction.state.log.replication.factor`.
    pub transaction_state_replication_factor: Option<i16>,
    /// Minimum transaction timeout a producer may request.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub transaction_min_timeout: Option<Time>,
    /// Maximum transaction timeout a producer may request, Kafka's
    /// `transaction.max.timeout.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub transaction_max_timeout: Option<Time>,
    /// Partition count of the `__barrier_state` internal topic.
    pub barrier_state_num_partitions: Option<i32>,
    /// Replication factor of the `__barrier_state` internal topic.
    pub barrier_state_replication_factor: Option<i16>,
    /// Shortest periodic injection interval a barrier group may ask for.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub barrier_min_injection_interval: Option<Time>,
    /// Deadline for one barrier injection to reach every target partition.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub barrier_injection_timeout: Option<Time>,
    /// Maximum bytes requested by one `__barrier_state` recovery read.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub barrier_recovery_read_max: Option<ByteSize>,
    /// Number of cuts a barrier group keeps before it tombstones the oldest.
    pub barrier_retained_cuts: Option<i32>,
    /// Maximum number of barrier groups the cluster accepts.
    pub barrier_max_groups: Option<usize>,
    /// Maximum number of topics in one barrier group.
    pub barrier_max_topics_per_group: Option<usize>,
    /// Cadence of the partition disk-usage scan that feeds the
    /// `partition_disk_bytes` gauge. Zero disables the scanner and spawns no
    /// background task.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub partition_disk_scan_interval: Option<Time>,
    /// KIP-853: maximum log-entry lag an observer may have and still be
    /// promotable to a voter.
    pub observer_lag_bound: Option<u64>,
    /// How often this broker sends `BrokerHeartbeat` to the controller leader.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub heartbeat_interval: Option<Time>,
    /// How long the controller waits without a heartbeat before it marks a
    /// broker dead.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub heartbeat_timeout: Option<Time>,
    /// Maximum follower lag before the leader proposes an ISR shrink. Kafka's
    /// `replica.lag.time.max.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub replica_lag_time_max: Option<Time>,
    /// Controller election timeout, Kafka's
    /// `controller.quorum.fetch.timeout.ms`. It is the follower fetch
    /// watchdog, and 1.5x of it is the leader's check-quorum window: a leader
    /// that a majority of the voters has not fetched from within that window
    /// resigns its epoch.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub controller_election_timeout: Option<Time>,
    /// Raft heartbeat interval on the controller quorum. It should stay at or
    /// below `controller_election_timeout / 3`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub controller_heartbeat_interval: Option<Time>,
    /// Consecutive follower fetch misses tolerated before a new election.
    pub controller_fetch_miss_limit: Option<u32>,
    /// Capacity of the metadata Raft engine command queue.
    pub metadata_raft_command_queue_capacity: Option<usize>,
    /// Per-read and per-snapshot-request byte budget on the metadata Raft log.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub metadata_raft_fetch_max: Option<ByteSize>,
    /// How long a controlled shutdown waits for the controller to acknowledge
    /// `should_shut_down` before it falls back to a hard shutdown.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub controlled_shutdown_drain_timeout: Option<Time>,
    /// Committed metadata-log bytes between snapshots, Kafka's
    /// `metadata.log.max.record.bytes.between.snapshots`.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub metadata_max_bytes_between_snapshots: Option<ByteSize>,
    /// Maximum time between metadata-log snapshots, Kafka's
    /// `metadata.log.max.snapshot.interval.ms`. Zero disables the time-based
    /// cap.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub metadata_max_snapshot_interval: Option<Time>,
    /// KIP-630: snapshot the metadata log once the committed offset advances
    /// this many records past the last snapshot, then prune below it.
    pub metadata_snapshot_interval_records: Option<u64>,
    /// Maximum metadata snapshot size a follower fetches. The Raft core
    /// enforces an immutable 1 GiB ceiling above it.
    #[serde(default, with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<crate::file_config::schema_units::ByteSize>")]
    pub metadata_snapshot_fetch_max: Option<ByteSize>,
    /// KIP-98: how often the idle-transaction reaper scans for `Ongoing`
    /// transactions whose timeout has elapsed and aborts them. Kafka's
    /// `transaction.abort.timed.out.transaction.cleanup.interval.ms`. Zero
    /// disables the reaper and spawns no background task.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub txn_abort_cleanup_interval: Option<Time>,
    /// KIP-98: how long a transactional id may sit in a terminal or idle state
    /// before the coordinator tombstones it out of `__transaction_state`.
    /// Kafka's `transactional.id.expiration.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub txn_id_expiration: Option<Time>,
    /// KIP-98: how often the transactional-id expiry sweep scans the
    /// `__transaction_state` partitions this broker leads. Kafka's
    /// `transaction.remove.expired.transaction.cleanup.interval.ms`. Zero
    /// disables the sweep and spawns no background task.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub txn_id_expiration_cleanup_interval: Option<Time>,
    /// How often the auto-rebalance ticker fires, Kafka's
    /// `leader.imbalance.check.interval.seconds`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub leader_imbalance_check_interval: Option<Time>,
    /// Cadence at which the TLS watcher polls the certificate, key, and
    /// client-CA files and rebuilds the server configuration if any changed.
    /// Zero disables the periodic watcher.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub tls_reload_interval: Option<Time>,
    /// KIP-227: maximum number of incremental-fetch sessions kept in the per-
    /// broker cache, Kafka's `max.incremental.fetch.session.cache.slots`. When
    /// the cache is full a non-privileged session is evicted in LRU order.
    pub max_incremental_fetch_session_cache_slots: Option<usize>,
    /// Maximum number of live broker connections across all listeners, Kafka's
    /// `max.connections`. A connection accepted past this ceiling is closed
    /// immediately.
    pub max_connections: Option<usize>,
    /// Maximum number of live connections from any single client IP, Kafka's
    /// `max.connections.per.ip`.
    pub max_connections_per_ip: Option<usize>,
    /// KIP-48: hard upper bound on a delegation token's lifetime, Kafka's
    /// `delegation.token.max.lifetime.ms`. A renew request is clamped to it.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub delegation_token_max_lifetime: Option<Time>,
    /// KIP-48: cadence of the sweep that tombstones expired delegation tokens,
    /// Kafka's `delegation.token.expiry.check.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub delegation_token_expiry_check_interval: Option<Time>,
    /// KIP-48: default renew period, Kafka's
    /// `delegation.token.expiry.time.ms`. It is the initial expiry offset at
    /// create time, and the implicit renew period when a
    /// `RenewDelegationToken` request asks for `-1`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub delegation_token_default_renew_period: Option<Time>,
    /// KIP-405: tick cadence of the `RemoteLogManager` copy and retention
    /// task. Kafka's `remote.log.manager.task.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub remote_log_manager_interval: Option<Time>,

    /// Whether the broker serves KIP-932 share groups, Kafka's
    /// `group.share.enable`.
    pub share_group_enable: Option<bool>,
    /// Default share-group session timeout, Kafka's
    /// `group.share.session.timeout.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub share_group_session_timeout: Option<Time>,
    /// Default share-group heartbeat interval, Kafka's
    /// `group.share.heartbeat.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub share_group_heartbeat_interval: Option<Time>,
    /// Maximum number of members in one share group, Kafka's
    /// `group.share.max.size`.
    pub share_group_max_size: Option<usize>,
    /// How long an acquired share record stays locked before it is released
    /// for redelivery, Kafka's `group.share.record.lock.duration.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub share_group_record_lock_duration: Option<Time>,
    /// Number of times a share record may be delivered before it is archived,
    /// Kafka's `group.share.delivery.count.limit`.
    pub share_group_max_delivery_attempts: Option<i16>,
    /// Maximum records a share partition may hold in flight, Kafka's
    /// `group.share.partition.max.record.locks`.
    pub share_group_max_inflight_records: Option<i32>,
    /// Cadence of the share-group backlog poll.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub share_group_backlog_poll_interval: Option<Time>,
    /// Transaction isolation for share-group reads, Kafka's
    /// `share.group.isolation.level`. Either `read-uncommitted`, which reads
    /// up to the high watermark, or `read-committed`, which clamps reads to
    /// the last stable offset.
    pub share_group_isolation_level: Option<String>,
    /// Whether the broker serves KIP-1071 streams groups.
    pub streams_group_enable: Option<bool>,
    /// Default streams-group session timeout, the group's
    /// `streams.session.timeout.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub streams_group_session_timeout: Option<Time>,
    /// Default streams-group heartbeat interval, the group's
    /// `streams.heartbeat.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub streams_group_heartbeat_interval: Option<Time>,
    /// Maximum number of members in one streams group.
    pub streams_group_max_size: Option<usize>,
    /// Replication factor of the internal topics a streams group creates, such
    /// as its repartition and changelog topics.
    pub streams_internal_topic_replication_factor: Option<i16>,
    /// Number of standby replicas the assignor places for each task, the
    /// group's `streams.num.standby.replicas`.
    pub streams_group_num_standby_replicas: Option<i32>,
    /// Maximum number of warm-up replicas the assignor may move at once, the
    /// group's `streams.num.warmup.replicas`.
    pub streams_group_num_warmup_replicas: Option<i32>,
    /// Changelog lag, in records, below which a task is treated as caught up,
    /// the group's `streams.acceptable.recovery.lag`.
    pub streams_group_acceptable_recovery_lag: Option<i64>,
    /// Cadence at which members report task offsets, the group's
    /// `streams.task.offset.interval.ms`.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub streams_group_task_offset_interval: Option<Time>,
    /// Server-side task assignor for streams groups: `auto`, `sticky`, or
    /// `highly-available`. `auto` picks `highly-available` when the topology
    /// has a stateful subtopology and `sticky` otherwise.
    pub streams_group_assignor: Option<String>,
}

impl RuntimeFileConfig {
    /// Apply every present runtime value, validating scalar boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`FileConfigError::InvalidConfig`] for an invalid value.
    pub fn apply_to(
        mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        self.apply_core(cfg)?;
        self.apply_replication(cfg)?;
        self.apply_coordinators(cfg)?;
        self.apply_recovery_and_queues(cfg)?;
        self.apply_network_limits(cfg)?;
        self.apply_transactions(cfg)?;
        self.apply_barrier(cfg)?;
        self.apply_broker_policy(cfg)?;
        self.apply_share_group(cfg)?;
        self.apply_streams_group(cfg)
    }
}

#[cfg(test)]
mod tests;
