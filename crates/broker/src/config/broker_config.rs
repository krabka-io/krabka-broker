//! Every setting the broker reads at construction time.

use {
    crate::{
        authorizer::{AllowAllAuthorizer, Authorizer},
        clean_shutdown::UNPROVEN,
        client_metrics::config::INTERVAL_MS_DEFAULT,
        config::{
            BreakGlassConfig, BrokerFeatureFlags, DEFAULT_AUDIT_CHECKPOINT_EVERY,
            DEFAULT_AUDIT_CHECKPOINT_EVERY_N, DEFAULT_AUDIT_SPOOL_DIR, DEFAULT_AUDIT_SPOOL_MAX,
            DEFAULT_AUDIT_SPOOL_SYNC_EVERY_N, DEFAULT_AUDIT_TOPIC, DEFAULT_BROKER_ID,
            DEFAULT_CONTROLLER_ELECTION_TIMEOUT, DEFAULT_CONTROLLER_FETCH_TIMEOUT,
            DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL, DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL,
            DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME, DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD,
            DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL, DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE,
            DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE, DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT,
            DEFAULT_DISKLESS_WAL_LOCAL_REPLICA_COUNT, DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG,
            DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TIMEOUT,
            DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE, DEFAULT_JWKS_REFRESH_INTERVAL,
            DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL, DEFAULT_LOG_FILTER,
            DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS,
            DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS, DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL,
            DEFAULT_METADATA_SNAPSHOT_FETCH_MAX, DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS,
            DEFAULT_OBSERVER_LAG_BOUND, DEFAULT_QUOTA_WINDOW, DEFAULT_REMOTE_COPIER_THREADS,
            DEFAULT_REMOTE_COPY_TIMEOUT, DEFAULT_REMOTE_EXPIRATION_THREADS,
            DEFAULT_REMOTE_INDEX_CACHE_SIZE, DEFAULT_REMOTE_LOG_MANAGER_INTERVAL,
            DEFAULT_REMOTE_READER_MAX_PENDING_TASKS, DEFAULT_REMOTE_READER_THREADS,
            DEFAULT_REPLICA_LAG_TIME_MAX, DEFAULT_TLS_RELOAD_INTERVAL,
            DEFAULT_TXN_ABORT_CLEANUP_INTERVAL, DEFAULT_TXN_ID_EXPIRATION,
            DEFAULT_TXN_ID_EXPIRATION_CLEANUP_INTERVAL, FreezeConfig, InterBrokerCredentials,
            KafkaRlmmConfig, ListenerSpec, NodeRole, RemoteStorageBackend,
            ReplicationRuntimeConfig, RlmmKind, StaticConfigOrigins, StretchProfile,
            defaults::{DEFAULT_CONTROLLER_ADDR, DEFAULT_LISTEN_ADDR},
            feature_flags::default_feature_flags,
            shared_epoch_ms,
        },
        coordinator::unified::{
            config::NextGenConfig, share::config::ShareGroupConfig,
            streams::config::StreamsGroupConfig,
        },
        file_config::{FileAuthorizationConfig, FileSchemaRegistryConfig},
        network::auth::{GssapiConfig, SslPrincipalMapper},
        operator_keys::OperatorKeys,
        replica_selector::ReplicaSelectorKind,
        schema_validation::SchemaValidator,
        share_coordinator::config::ShareCoordinatorConfig,
        topic_policy::TopicPolicy,
    },
    krabka_audit::AuditMode,
    krabka_compression::RecordDecompressionPolicy,
    krabka_log::{LogConfig, StampSource},
    krabka_raft::{
        BootstrapMode, ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity,
        MetadataRaftFetchMax, NodeId,
    },
    krabka_remote_storage::WormConfig,
    krabka_security::{ListenerProtocol, SaslMechanism, SecretBytes, TlsConfig},
    krabka_telemetry::{LogLevelController, OtlpProtocol, profiling::ProfilingConfig},
    krabka_units::{
        ByteSize, Ratio, Time, bytes, hours, kibibytes, mebibytes, millis, minutes, secs,
    },
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        net::SocketAddr,
        num::NonZeroU64,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicI64, AtomicU64},
        },
    },
    tokio::sync::mpsc::Receiver,
    uuid::Uuid,
};

mod security;

pub use self::security::PlainCredentials;

krabka_macros::broker_config! {
    #[derive(krabka_macros::FieldDefaults)]
    pub struct BrokerConfig {
        // tuning.
        /// Capacity used by every outbound Kafka client connection owned by this
        /// broker process.
        pub client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
        /// Maximum frame size used by every outbound Kafka client connection owned
        /// by this broker process.
        pub client_frame_max: krabka_client_core::ClientFrameMax,
        /// Maximum time to wait for a controller leader during startup.
        #[default(minutes(2))]
        pub startup_leader_wait_timeout: Time,
        /// Initial delay between self-registration attempts.
        #[default(millis(100))]
        pub self_registration_backoff_min: Time,
        /// Maximum delay between self-registration attempts.
        #[default(secs(5))]
        pub self_registration_backoff_max: Time,
        /// Observer promotion polling cadence.
        #[default(millis(100))]
        pub observer_poll_interval: Time,
        /// Audit spool replay cadence.
        #[default(secs(2))]
        pub audit_spool_replay_interval: Time,
        /// Audit statistics polling cadence.
        #[default(secs(1))]
        pub audit_stats_poll_interval: Time,
        /// Maximum wait for the audit partition to become available.
        #[default(secs(10))]
        pub audit_partition_wait_timeout: Time,
        /// Broker liveness maintenance cadence.
        #[default(secs(1))]
        pub liveness_tick_interval: Time,
        /// Broker gauge refresh cadence.
        #[default(secs(1))]
        pub gauge_poll_interval: Time,
        /// In-sync replica maintenance cadence.
        #[default(secs(1))]
        pub isr_scan_interval: Time,
        /// Log cleaner maintenance cadence.
        #[default(secs(30))]
        pub cleaner_interval: Time,
        /// Local-retention maintenance cadence: how often the broker
        /// applies `retention.ms`, `retention.bytes` and `segment.ms` to
        /// every log it hosts. Kafka's `log.retention.check.interval.ms`.
        #[default(minutes(5))]
        pub log_retention_check_interval: Time,
        /// Retry delay when moving a future log fails.
        #[default(millis(50))]
        pub future_log_move_retry_backoff: Time,
        /// Client-metrics cache eviction cadence.
        #[default(minutes(1))]
        pub client_metrics_eviction_tick: Time,
        /// Minimum age at which client metrics are stale.
        #[default(minutes(1))]
        pub client_metrics_stale_floor: Time,
        /// Default client telemetry subscription interval.
        #[default(millis(INTERVAL_MS_DEFAULT.unsigned_abs()))]
        pub client_metrics_default_interval: Time,
        /// Capacity of the client-metrics OTLP forwarding queue.
        #[default(256)]
        pub client_metrics_otlp_queue_capacity: usize,
        /// Maximum accepted client telemetry payload size.
        #[default(mebibytes(1))]
        pub client_metrics_telemetry_max: ByteSize,
        /// Prometheus client-metrics snapshot lifetime.
        #[default(minutes(5))]
        pub client_metrics_prom_snapshot_ttl: Time,
        /// Remote-log metadata reconciliation cadence.
        #[default(secs(30))]
        pub rlmm_reconcile_tick: Time,
        /// Initial remote-log metadata bootstrap retry delay.
        #[default(millis(250))]
        pub rlmm_bootstrap_backoff_initial: Time,
        /// Maximum remote-log metadata bootstrap retry delay.
        #[default(secs(10))]
        pub rlmm_bootstrap_backoff_max: Time,
        /// Maximum connection-creation quota delay. Governed by KIP-612 connection quota enforcement.
        #[default(secs(1))]
        pub connection_creation_throttle_max: Time,
        /// OPA authorization request timeout.
        #[default(secs(5))]
        pub opa_http_timeout: Time,
        /// Schema registry request timeout.
        #[default(secs(5))]
        pub schema_registry_http_timeout: Time,
        /// OAuth JWKS HTTP request timeout.
        #[default(secs(10))]
        pub oauth_jwks_http_timeout: Time,
        /// Dynamic-quorum auto-join retry delay.
        #[default(millis(500))]
        pub auto_join_retry_backoff: Time,
        /// Timeout carried by dynamic-quorum `AddRaftVoter` requests.
        #[default(secs(30))]
        pub auto_join_voter_request_timeout: Time,
        /// Follower replication runtime policy.
        pub replication: ReplicationRuntimeConfig,
        /// Consumer-group session expiry scan cadence.
        #[default(secs(1))]
        pub coordinator_session_expiry_tick: Time,
        /// Maximum wait for coordinator shutdown acknowledgements.
        #[default(secs(5))]
        pub coordinator_shutdown_ack_timeout: Time,
        /// Initial delay before a classic group begins rebalancing.
        #[default(secs(3))]
        pub classic_group_initial_rebalance_delay: Time,
        /// Maximum time a follower waits for a `SyncGroup` assignment.
        #[default(secs(30))]
        pub sync_group_follower_wait: Time,
        /// Aggressive unclean-recovery collection deadline.
        #[default(secs(2))]
        pub unclean_recovery_aggressive_deadline: Time,
        /// Balanced unclean-recovery collection deadline.
        #[default(secs(30))]
        pub unclean_recovery_balanced_deadline: Time,
        /// Operator-triggered recovery deadline.
        #[default(secs(25))]
        pub operator_recovery_deadline: Time,
        /// Maximum request-quota throttle delay (default 1 s), bounding how long one response over `request_percentage` mutes a client. Equivalent to Kafka's `ClientRequestQuotaManager.maxThrottleTimeMs`, one `quota.window.size.seconds`. Byte-rate and controller-mutation throttles are not bounded.
        #[default(secs(1))]
        pub quota_throttle_max: Time,
        /// Time window sizing the byte-rate quota token bucket burst capacity (default 11 s). Equivalent to Kafka's sampling window `quota.window.num * quota.window.size.seconds`.
        #[default(DEFAULT_QUOTA_WINDOW)]
        pub quota_window: Time,
        /// Time window sizing the KIP-599 controller-mutation token bucket burst capacity (default 11 s): the bucket holds `window x controller_mutation_rate` tokens. Equivalent to Kafka's `controller.quota.window.num * controller.quota.window.size.seconds`.
        #[default(DEFAULT_QUOTA_WINDOW)]
        pub controller_mutation_quota_window: Time,
        /// Maximum self-registration attempts before startup fails.
        #[default(8)]
        pub self_registration_max_attempts: u32,
        /// Maximum bytes fetched by a metadata observer request.
        #[default(mebibytes(1))]
        pub observer_fetch_max: ByteSize,
        /// Broker-wide maximum bytes returned by consumer `Fetch` and
        /// `ShareFetch` requests, Kafka's `fetch.max.bytes`.
        #[default(bytes(57_671_680))]
        pub fetch_max_bytes: ByteSize,
        /// Capacity of the asynchronous audit event queue.
        #[default(8_192)]
        pub audit_event_queue_capacity: usize,
        /// Number of offsets included in an audit tail request.
        #[default(4_096)]
        pub audit_tail_window_offsets: i64,
        /// Maximum bytes read by an audit tail request.
        #[default(mebibytes(1))]
        pub audit_tail_read_max: ByteSize,
        /// Number of stale push intervals before client metrics expire.
        #[default(3)]
        pub client_metrics_stale_push_intervals: u32,
        /// Mailbox capacity for coordinator actors.
        #[default(64)]
        pub coordinator_actor_mailbox_capacity: usize,
        /// Local replica count for diskless WAL mode.
        #[default(DEFAULT_DISKLESS_WAL_LOCAL_REPLICA_COUNT)]
        pub diskless_wal_local_replica_count: usize,
        /// Cadence of diskless WAL object-store flushes.
        #[default(DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL)]
        pub diskless_wal_flush_interval: Time,
        /// Maximum bytes included in one diskless WAL object-store flush.
        #[default(DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE)]
        pub diskless_wal_flush_max_size: ByteSize,
        /// Broker-wide byte ceiling for quorum-committed hot-tail batches.
        #[default(DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE)]
        pub diskless_wal_hot_tail_max_size: ByteSize,
        /// Committed offsets retained behind the diskless WAL trim frontier.
        #[default(DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG)]
        pub diskless_wal_trim_safety_lag: i64,
        /// Maximum wait for a published diskless WAL index record to be projected.
        #[default(DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT)]
        pub diskless_wal_index_projection_timeout: Time,
        /// Capacity of the unclean-recovery work queue.
        #[default(256)]
        pub unclean_recovery_queue_capacity: usize,
        /// Maximum bytes read while recovering share state.
        #[default(mebibytes(5))]
        pub share_coordinator_load_buffer_size: ByteSize,
        /// Share-session cache ceiling when group count is unlimited.
        #[default(10_000)]
        pub share_session_cache_max_when_unlimited: usize,
        /// Maximum encoded request size accepted from a socket (matches Kafka socket.request.max.bytes).
        #[default(mebibytes(100))]
        pub socket_request_max: ByteSize,
        /// Largest request frame a connection may send before it finishes
        /// authenticating on a SASL listener. Matches Kafka's
        /// `sasl.server.max.receive.size`, default 512 KiB. It replaces
        /// `socket_request_max` for that stretch, and a larger frame fails
        /// the authentication.
        #[default(kibibytes(512))]
        pub sasl_server_max_receive: ByteSize,
        /// How long a failed SASL authentication holds its response and
        /// the close that follows. Matches Kafka's
        /// `connection.failed.authentication.delay.ms`, default 100 ms.
        /// Zero closes at once.
        #[default(millis(100))]
        pub connection_failed_authentication_delay: Time,
        /// Maximum number of resident queued requests across all connections (matches Kafka queued.max.requests).
        #[default(500)]
        pub queued_max_requests: usize,
        /// Maximum resident request bytes across all connections (matches Kafka queued.max.request.bytes).
        pub queued_max_request_bytes: Option<ByteSize>,
        /// Minimum response size eligible for `sendfile`.
        ///
        /// The default is 4 KiB, the floor of the sweep in
        /// `benches/fetch_drain.rs`. That sweep drains a whole response
        /// over a loopback socket at 4, 16, 32, 64 and 256 KiB, through
        /// the kernel and through the `pread` + write copy, and finds no
        /// crossover inside its range: the kernel drain wins at every
        /// size, by 15% to 23% at 4 KiB and by 30% to 50% from 16 KiB up.
        ///
        /// The sweep has never measured below 4 KiB, so where the
        /// crossover actually is -- or whether there is one at all -- is
        /// unknown. The threshold sits at the smallest size that was
        /// measured rather than below it for that reason and no other.
        /// Widening the sweep downwards is a change to
        /// `benches/fetch_drain.rs`, and the wider result would appear in
        /// the `bench` job of the `ci` workflow, which runs that suite on
        /// the nightly schedule and prints it into the job summary; the
        /// numbers above are what the latest scheduled `ci` run in the
        /// Actions tab reproduces.
        // The floor of `benches/fetch_drain.rs`'s sweep, not a
        // measured crossover: nothing below 4 KiB has been benchmarked.
        // `BrokerConfig::sendfile_min`'s doc comment has the sweep, and
        // the `bench` job of the `ci` workflow is what re-measures it.
        #[default(kibibytes(4))]
        pub sendfile_min: ByteSize,
        /// Broker socket send-buffer size.
        #[default(mebibytes(1))]
        pub socket_send_buffer: ByteSize,
        /// Broker socket receive-buffer size.
        #[default(mebibytes(1))]
        pub socket_receive_buffer: ByteSize,
        /// Maximum accepted Kafka record decompression ratio.
        #[default(RecordDecompressionPolicy::default().max_ratio())]
        pub record_decompression_max_ratio: Ratio,
        /// Minimum Kafka record decompression output allowance.
        #[default(RecordDecompressionPolicy::default().output_floor())]
        pub record_decompression_output_floor: ByteSize,
        /// Maximum Kafka record decompression output allowance.
        #[default(RecordDecompressionPolicy::default().output_ceiling())]
        pub record_decompression_output_ceiling: ByteSize,
        /// TLS server name used for outbound inter-broker connections.
        #[default("localhost".to_owned())]
        pub inter_broker_server_name: String,
        /// Producer-id inactivity period before state expires.
        #[default(hours(24))]
        pub producer_id_expiration: Time,
        /// Producer-state expiry scan cadence.
        #[default(minutes(10))]
        pub producer_id_expiration_scan_interval: Time,
        /// Maximum produce requests combined into one append group.
        #[default(1_024)]
        pub max_produce_group: usize,
        /// Capacity of each partition-writer request queue.
        #[default(64)]
        pub partition_writer_queue_depth: usize,
        /// Default minimum in-sync replica count.
        #[default(1)]
        pub default_min_insync_replicas: i32,
        /// `log.message.timestamp.before.max.ms` broker default: how far
        /// behind this broker's clock a `CreateTime` record may sit on a
        /// topic that names no `message.timestamp.before.max.ms`
        /// override. `None` is Kafka's unbounded `Long.MAX_VALUE`
        /// default.
        pub default_message_timestamp_before_max_ms: Option<i64>,
        /// `log.message.timestamp.after.max.ms` broker default: how far
        /// ahead of this broker's clock a `CreateTime` record may sit on
        /// a topic that names no `message.timestamp.after.max.ms`
        /// override. Kafka's default is one hour.
        #[default(Some(3_600_000))]
        pub default_message_timestamp_after_max_ms: Option<i64>,
        /// KIP-464: the partition count of a topic that `CreateTopics`
        /// creates with `num_partitions = -1`. Kafka's `num.partitions`,
        /// default 1.
        #[default(1)]
        pub num_partitions: i32,
        /// KIP-464: the replication factor of a topic that `CreateTopics`
        /// creates with `replication_factor = -1`. Kafka's
        /// `default.replication.factor`, default 1.
        #[default(1)]
        pub default_replication_factor: i16,
        /// Whether `DeleteTopics` may delete a topic. Kafka's
        /// `delete.topic.enable`, default `true`.
        #[default(true)]
        pub delete_topic_enable: bool,
        /// Whether `Metadata` may create a topic it is asked about and
        /// that does not exist, when the request allows it. Kafka's
        /// `auto.create.topics.enable`, default `true`.
        #[default(true)]
        pub auto_create_topics_enable: bool,
        /// Upper clamp on `DescribeTopicPartitions`'
        /// `response_partition_limit`. Kafka's
        /// `max.request.partition.size.limit`
        /// (`ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_DEFAULT`),
        /// default 2000.
        #[default(2000)]
        pub max_request_partition_size_limit: i32,
        /// Longest committed metadata an `OffsetCommit` partition may
        /// carry, in UTF-16 code units. Kafka's
        /// `offset.metadata.max.bytes`, default 4096.
        #[default(4096)]
        pub offset_metadata_max_bytes: i32,
        /// Bytes copied per future-log move read.
        #[default(mebibytes(1))]
        pub future_log_move_read_chunk: ByteSize,
        /// Partition count for the consumer-offsets internal topic.
        #[default(50)]
        pub offsets_topic_num_partitions: i32,
        /// KIP-211: how long a committed offset survives after the group
        /// that owns it loses its last member, when the operator set it.
        /// Kafka's `offsets.retention.minutes`, whose default is 10080
        /// (7 days). The retention sweep tombstones an empty group's
        /// offsets once this much time has passed, and then tombstones the
        /// group itself.
        ///
        /// `None` means the operator named nothing, so the broker runs
        /// [`BrokerConfig::offsets_retention`] — and `DescribeConfigs`
        /// reports the key at `DEFAULT_CONFIG` rather than
        /// `STATIC_BROKER_CONFIG`, which is the distinction Kafka draws by
        /// asking whether the key appears in `KafkaConfig.originals`.
        pub offsets_retention_override: Option<Time>,
        /// Cadence of the offset-retention sweep, when the operator set
        /// it. Kafka's `offsets.retention.check.interval.ms`, whose
        /// default is 600000 (10 minutes). `None` carries the same meaning
        /// as on [`offsets_retention_override`](Self::offsets_retention_override).
        pub offsets_retention_check_interval_override: Option<Time>,
        /// Desired replication factor for the consumer-offsets internal topic.
        #[default(3)]
        pub offsets_topic_replication_factor: i16,
        /// `segment.bytes` of the consumer-offsets internal topic. Kafka's
        /// `offsets.topic.segment.bytes`.
        #[default(mebibytes(100))]
        pub offsets_topic_segment_bytes: ByteSize,
        /// Partition count for the transaction-state internal topic.
        #[default(50)]
        pub transaction_state_num_partitions: i32,
        /// Maximum bytes requested by each transaction-state recovery read.
        #[default(mebibytes(1))]
        pub transaction_recovery_read_max: ByteSize,
        /// Desired replication factor for the transaction-state internal topic.
        #[default(3)]
        pub transaction_state_replication_factor: i16,
        /// `segment.bytes` of the transaction-state internal topic. Kafka's
        /// `transaction.state.log.segment.bytes`.
        #[default(mebibytes(100))]
        pub transaction_state_segment_bytes: ByteSize,
        /// `min.insync.replicas` of the transaction-state internal topic.
        /// Kafka's `transaction.state.log.min.isr`.
        #[default(2)]
        pub transaction_state_min_isr: i32,
        /// Maximum accepted transaction timeout.
        #[default(minutes(15))]
        pub transaction_max_timeout: Time,
        /// The static layer of Kafka's dynamic
        /// `transaction.partition.verification.enable`
        /// (`KafkaConfig.transactionPartitionVerificationEnable`): whether a
        /// partition leader checks with the transaction coordinator that a
        /// transaction contains a partition before it appends transactional
        /// records to it. A dynamic broker config, per-broker or cluster-wide,
        /// overrides it.
        #[default(true)]
        pub transaction_partition_verification_enable: bool,
        /// Partition count for the `__barrier_state` internal topic.
        #[default(50)]
        pub barrier_state_num_partitions: i32,
        /// Desired replication factor for the `__barrier_state` internal topic.
        #[default(3)]
        pub barrier_state_replication_factor: i16,
        /// Shortest periodic injection interval a barrier group may ask for.
        #[default(secs(1))]
        pub barrier_min_injection_interval: Time,
        /// Deadline for one barrier injection to reach every target partition.
        #[default(secs(30))]
        pub barrier_injection_timeout: Time,
        /// Maximum bytes requested by each `__barrier_state` recovery read.
        #[default(mebibytes(1))]
        pub barrier_recovery_read_max: ByteSize,
        /// Number of cuts a barrier group keeps before it tombstones the oldest.
        #[default(100)]
        pub barrier_retained_cuts: i32,
        /// Maximum number of barrier groups the cluster accepts.
        #[default(100)]
        pub barrier_max_groups: usize,
        /// Maximum number of topics in one barrier group.
        #[default(100)]
        pub barrier_max_topics_per_group: usize,

        // identity.
        /// Broker id reported in `Metadata` responses. Kafka's `broker.id`:
        /// it must equal [`node_id`][Self::node_id], or
        /// [`validate`][Self::validate] refuses the config. Default: 1.
        #[default(DEFAULT_BROKER_ID)]
        pub broker_id: i32,

        /// `KRaft` `process.roles`. It controls whether this node is a metadata
        /// quorum voter (`Controller`), hosts data partitions and registers as a
        /// broker (`Broker`), or both. Default: `[Controller, Broker]`.
        #[default(vec![NodeRole::Controller, NodeRole::Broker])]
        pub roles: Vec<NodeRole>,

        /// TCP address of the client listener. Default: `127.0.0.1:9092`.
        /// A node without the `broker` role opens no client listener.
        #[default(DEFAULT_LISTEN_ADDR)]
        pub listen_addr: SocketAddr,

        /// `host:port` returned in `Metadata` responses as this broker's
        /// advertised endpoint. Defaults to `listen_addr`'s string form.
        #[default(DEFAULT_LISTEN_ADDR.to_string())]
        pub advertised_listener: String,

        /// Primary log directory, the first entry of Kafka's `log.dirs`. It is a
        /// data directory: when [`extra_log_dirs`][Self::extra_log_dirs] is
        /// empty, partition data lives only here. It also holds the
        /// `__cluster_metadata-0` raft log unless
        /// [`metadata_log_dir`][Self::metadata_log_dir] names another
        /// directory. The broker creates the directory on startup if it is
        /// missing. Default: `./krabka-data`.
        #[default(PathBuf::from("./krabka-data"))]
        pub log_dir: PathBuf,

        /// Extra JBOD data directories (KIP-113). When this list is non-empty,
        /// the broker spreads new partitions across `[log_dir] + extra_log_dirs`
        /// by least-loaded placement. Maps to a Kafka `log.dirs` value with more
        /// than one entry. Default: empty, which gives a single-directory broker.
        pub extra_log_dirs: Vec<PathBuf>,

        /// Kafka's `metadata.log.dir`: the directory of the `__cluster_metadata-0`
        /// raft log, its snapshots, the node's `meta.properties` and the
        /// bootstrap records. `None` keeps the metadata log in
        /// [`log_dir`][Self::log_dir], as Kafka keeps it in the first entry of
        /// `log.dirs`. A directory that is none of the data directories holds
        /// the metadata log only: no partition is placed there, and
        /// `DescribeLogDirs` does not report it. Read it through
        /// [`metadata_dir`][Self::metadata_dir].
        pub metadata_log_dir: Option<PathBuf>,

        /// KIP-1066 static `cordoned.log.dirs`, as the operator wrote it: a
        /// comma-separated list of entries of the configured log directories,
        /// or `*` for all of them. A cordoned directory keeps its replicas but
        /// takes no new one. A per-broker dynamic value overrides it. `None`
        /// cordons nothing, which is Kafka's default.
        pub cordoned_log_dirs: Option<String>,

        /// Per-log configuration applied to every partition this broker hosts.
        pub log_config: LogConfig,

        /// Optional internal timestamp source shared by every hosted partition.
        ///
        /// `None` is the Kafka-only default: no `.stampindex` sidecar is opened and
        /// record bytes, offsets, LSO, and high-watermark behavior stay unchanged.
        /// A combined SQL/Kafka runtime injects its tenant timestamp source here.
        pub stamp_source: Option<Arc<dyn StampSource>>,

        // quorum.
        /// Raft node id, Kafka's `node.id`. It must equal
        /// [`broker_id`][Self::broker_id].
        #[default(NodeId(1))]
        pub node_id: NodeId,

        /// Address the controller listener binds on. `KRaft` convention: same
        /// host as `listen_addr`, port 9093. Test default: `127.0.0.1:0`.
        #[default(DEFAULT_CONTROLLER_ADDR)]
        pub controller_listen_addr: SocketAddr,

        /// Static voter set: `[(node_id, "<host>:<port>"), …]`. The address is the
        /// peer controller listener's `<host>:<port>`, carried verbatim and NOT
        /// pre-resolved. The dialer resolves the host again on every connect and
        /// reconnect, so a peer that restarts on a new pod IP stays reachable.
        /// Defaults to a single-voter cluster of just this broker, so
        /// single-broker setups get a quorum of one without config changes.
        #[default(vec![(NodeId(1), DEFAULT_CONTROLLER_ADDR.to_string())])]
        pub controller_quorum_voters: Vec<(NodeId, String)>,

        /// TLS server name (SNI) presented when dialing a peer's controller
        /// listener for the KIP-595 quorum. Set to a SAN shared by every
        /// broker's serving cert, the headless-Service FQDN, so mTLS validates
        /// whichever peer the broker dials, even a pod IP. `None` falls back to
        /// `"localhost"`.
        pub controller_server_name: Option<String>,

        /// KIP-853 dynamic quorum: controller endpoints used only to discover
        /// the leader at cold start (the joiner path). Empty for a standalone
        /// bootstrap node. Maps to Kafka's `controller.quorum.bootstrap.servers`.
        pub bootstrap_servers: Vec<String>,

        /// KIP-853: this replica's stable directory id, recovered from
        /// `meta.properties` at boot. Identifies which voter this node *is*.
        #[default(Uuid::from_u128(1))]
        pub directory_id: Uuid,

        /// UUID for this broker process invocation. Startup draws a new one
        /// before self-registration, as Kafka's `BrokerLifecycleManager` does
        /// for every process, so a restart is a new incarnation. Tests
        /// generate a random UUID for each call through [`Self::for_tests`].
        #[default(Uuid::nil())]
        pub incarnation_id: Uuid,

        /// KIP-966 clean-shutdown proof: the broker epoch this node held
        /// when it last stopped gracefully, recovered from
        /// `{log_dir}/.kafka_cleanshutdown` at boot and offered back at
        /// registration. Kafka carries the same value as
        /// `BrokerRegistrationRequest.previousBrokerEpoch`. It is
        /// `clean_shutdown::UNPROVEN` when this node cannot prove
        /// it stopped gracefully, which is what a crash leaves behind and
        /// what a first-ever boot has.
        #[default(UNPROVEN)]
        pub previous_broker_epoch: i64,

        /// The broker epoch this process registered at, or -1 before its
        /// self-registration is published. Kafka keeps it as
        /// `BrokerLifecycleManager.brokerEpoch`, from the
        /// `BrokerRegistration` response, and every request that names
        /// this broker's epoch reads it there rather than out of a
        /// metadata image that may still hold the previous incarnation's.
        #[default(-1)]
        pub broker_epoch: i64,

        /// KIP-853: when true, an observer issues `AddVoter` for itself once it
        /// has caught up to the leader. The observer joins the quorum without
        /// operator action. Maps to Kafka's `controller.quorum.auto.join.enable`.
        pub auto_join: bool,

        /// KIP-853: maximum log-entry lag an observer may have and still be
        /// promotable to a voter. The broker forwards it to `ControllerConfig`.
        #[default(DEFAULT_OBSERVER_LAG_BOUND)]
        pub observer_lag_bound: u64,

        /// How often each broker sends `BrokerHeartbeat` to the controller
        /// leader. Default 3s.
        #[default(DEFAULT_HEARTBEAT_INTERVAL)]
        pub heartbeat_interval: Time,
        /// Controller marks a broker dead after this long without a
        /// heartbeat. Default 9s.
        #[default(DEFAULT_HEARTBEAT_TIMEOUT)]
        pub heartbeat_timeout: Time,
        /// Leader proposes ISR shrink when a follower lags more than this.
        /// Default 30s.
        #[default(DEFAULT_REPLICA_LAG_TIME_MAX)]
        pub replica_lag_time_max: Time,

        /// Controller quorum fetch timeout, Kafka's
        /// `controller.quorum.fetch.timeout.ms`. Default 2s, as in Kafka.
        ///
        /// It is the follower and observer fetch watchdog in the `KRaft`
        /// engine, so it bounds how long a lost leader goes unnoticed before
        /// a voter starts an election. And 1.5x of it -- Kafka's
        /// `LeaderState.CHECK_QUORUM_TIMEOUT_FACTOR` -- is the leader's
        /// check-quorum window: a leader that a majority of the voters has
        /// not fetched from within that window resigns rather than keep
        /// answering as the leader of an epoch the quorum has replaced.
        #[default(DEFAULT_CONTROLLER_FETCH_TIMEOUT)]
        pub controller_fetch_timeout: Time,

        /// Controller election timeout, Kafka's
        /// `controller.quorum.election.timeout.ms`. Default 1s, as in Kafka.
        ///
        /// It is the base of a candidate's randomized election timer: each
        /// election waits this long plus a jitter in `[0, timeout)`, like
        /// Kafka's random election timeout. Two voters whose fetch timers
        /// expired together and split the vote therefore retry within one to
        /// two election timeouts, and the jitter makes a second split
        /// unlikely.
        #[default(DEFAULT_CONTROLLER_ELECTION_TIMEOUT)]
        pub controller_election_timeout: Time,

        /// Leader heartbeat cadence. Default 500ms. The `KRaft` engine
        /// re-broadcasts `BeginQuorumEpoch` this often, so a follower that
        /// missed the original announcement, or an old leader that
        /// rejoined, re-attaches without waiting for an election.
        /// Validation rejects a value that is not below
        /// `controller_fetch_timeout`.
        #[default(DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL)]
        pub controller_heartbeat_interval: Time,
        /// Whether the heartbeat interval was explicitly configured. An
        /// omitted value leaves the engine on its derived cadence of
        /// `controller_fetch_timeout / 3`, with a floor of one
        /// millisecond.
        pub controller_heartbeat_interval_explicit: bool,
        /// Consecutive fetch misses an observer tolerates before it looks for the
        /// leader again. A voter starts an election after one fetch timeout, as
        /// Kafka does.
        pub controller_fetch_miss_limit: ControllerFetchMissLimit,
        /// Capacity of the metadata Raft engine command queue.
        pub metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
        /// Per-read and per-snapshot-request metadata Raft byte budget.
        pub metadata_raft_fetch_max: MetadataRaftFetchMax,

        /// `metadata.log.max.record.bytes.between.snapshots` (default 20 MiB).
        #[default(DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS)]
        pub metadata_max_bytes_between_snapshots: ByteSize,

        /// `metadata.log.max.snapshot.interval.ms` (default 1 h; zero = disabled).
        #[default(DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL)]
        pub metadata_max_snapshot_interval: Time,

        /// KIP-630: snapshot the metadata log once committed offset advances this
        /// many records past the last snapshot. The metadata log keeps the
        /// records below a snapshot until its retention limits let them go.
        #[default(DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS)]
        pub metadata_snapshot_interval_records: u64,

        /// Maximum metadata snapshot size a follower will fetch. The core enforces
        /// an immutable 1 GiB security ceiling.
        #[default(DEFAULT_METADATA_SNAPSHOT_FETCH_MAX)]
        pub metadata_snapshot_fetch_max: ByteSize,

        /// Kafka's `MetadataLogConfig`: `metadata.log.segment.bytes`,
        /// `metadata.log.segment.ms`, `metadata.max.retention.bytes`,
        /// `metadata.max.retention.ms` and `metadata.max.idle.interval.ms`.
        /// The defaults are Kafka's.
        pub metadata_log: krabka_raft::MetadataLogConfig,

        /// How this broker takes part in cluster formation. See
        /// [`krabka_raft::BootstrapMode`] for the trade-offs. The first broker of
        /// a fresh multi-broker cluster uses `Bootstrap`. Later brokers use
        /// `Join`. A restart of any previously-formatted broker uses `Rejoin`.
        /// Single-broker setups always use `Bootstrap`.
        #[default(BootstrapMode::Bootstrap)]
        pub bootstrap_mode: BootstrapMode,

        /// Cluster UUID that the broker forwards to
        /// `ControllerConfig::cluster_id`. The operator supplies it as the
        /// `KafkaCluster` UID through `--cluster-id`. `None` defaults to
        /// `Uuid::nil()` inside `Controller::start`.
        pub cluster_id: Option<Uuid>,

        /// KIP-392: this broker's rack identifier (`broker.rack`). The broker
        /// reports it in its `BrokerRegistrationRecord`, and the leader's
        /// rack-aware replica selector reads it. `None` (default) means no rack.
        pub rack: Option<String>,

        /// KIP-392: which replica selector the leader runs to populate
        /// `FetchResponse.preferred_read_replica` for rack-aware consumers.
        /// Default `Leader` (never redirect).
        #[default(ReplicaSelectorKind::Leader)]
        pub replica_selector: ReplicaSelectorKind,

        /// The three-site stretch deployment this node belongs to. `None`
        /// (default) is an ordinary, non-stretched cluster. When it is `Some`,
        /// [`rack`][Self::rack] must name one of the profile's sites, and
        /// [`validate`][Self::validate] checks that the roles of this node agree
        /// with that site.
        pub stretch: Option<StretchProfile>,

        // security.
        // ── Auth / listener registry ─────────────────────────────────────────
        /// Named listener definitions. When this list is empty,
        /// `effective_listeners()` builds a single PLAINTEXT listener from
        /// `listen_addr` and `advertised_listener`.
        pub listeners: Vec<ListenerSpec>,

        /// Protocol terminator for the controller listener. The default
        /// `Plaintext` keeps the legacy raw-TCP raft transport. Set it to
        /// `SaslPlaintext`, `Ssl`, or `SaslSsl` to require auth on inbound raft
        /// RPCs. Outbound raft RPCs also use auth when you pair this with
        /// `inter_broker_credentials`.
        #[default(ListenerProtocol::Plaintext)]
        pub controller_listener_protocol: krabka_security::ListenerProtocol,

        /// Name of the listener used for inter-broker traffic (raft, replication,
        /// heartbeat). Must match a name in `listeners` when `listeners` is
        /// non-empty. Default: `"PLAINTEXT"`.
        #[default("PLAINTEXT".to_owned())]
        pub inter_broker_listener_name: String,

        /// Credentials the broker uses for outbound inter-broker connections.
        /// `None` means no SASL, which gives plaintext inter-broker traffic.
        /// This is the default.
        pub inter_broker_credentials: Option<InterBrokerCredentials>,

        /// Exact authenticated principal name to broker node ID bindings used
        /// by diskless WAL follower fetches. This map is authoritative: once
        /// it is non-empty, only the names it lists may fetch as a WAL voter,
        /// on any listener. While it is empty, the built-in `broker-<id>` and
        /// `CN=broker-<id>` naming convention applies, but only to connections
        /// on `inter_broker_listener_name`; a principal of that name on a
        /// client listener is refused.
        pub inter_broker_principal_node_ids: HashMap<String, NodeId>,

        /// Static PLAIN credentials: username → password. Empty by default.
        /// PLAIN auth stays disabled until you explicitly enable the mechanisms.
        /// `[sasl_plain] credentials_path` in `broker.toml` loads the table
        /// from a file; `validate()` refuses a listener that offers PLAIN
        /// while it is empty.
        pub plain_credentials: PlainCredentials,

        /// Usernames that bypass ACL checks (super-users). The
        /// `create_delegation_token` act-as gate reads this directly; the
        /// active [`crate::authorizer::Authorizer`] impl also reads it
        /// (`SimpleAclAuthorizer` / `OpaAuthorizer`). `file_config` populates
        /// both from the same `[authorization]` TOML stanza.
        pub super_users: HashSet<String>,

        /// Pluggable cluster authorizer. There is one boxed instance for each
        /// broker, configured through `[authorization]` in `broker.toml`. The
        /// default is [`crate::authorizer::AllowAllAuthorizer`], an explicit
        /// "allow everything" policy. That default reports itself as no
        /// authorizer at all through
        /// [`crate::authorizer::Authorizer::is_configured`], which is what
        /// makes the ACL administration RPCs answer `SECURITY_DISABLED`.
        #[default(Arc::new(AllowAllAuthorizer))]
        pub authorizer: Arc<dyn Authorizer>,

        /// Resolved authorization settings, assembled into a client at startup.
        /// An absent section preserves a programmatically injected authorizer.
        pub authorization_config: Option<FileAuthorizationConfig>,

        /// KFC-7 schema validator: the registry client and its cache, shared by
        /// every produce on this broker. Configured through `[schema_registry]` in
        /// `broker.toml`. `None` is the default and means no `[schema_registry]`
        /// section was configured, so a topic that turns `schema.validation.*` on
        /// has nothing to validate against.
        pub schema_validator: Option<Arc<SchemaValidator>>,

        /// Resolved registry settings; startup reads secrets and builds the client.
        /// An absent section preserves a programmatically injected validator.
        pub schema_registry_config: Option<FileSchemaRegistryConfig>,

        /// The operator key trust set, configured through the top-level
        /// `[[operator_keys]]` array in `broker.toml`.
        ///
        /// One set serves both signature paths: a freeze record's detached
        /// signature and a break-glass approval's. Empty is the default and means
        /// no operator key is provisioned, so nothing may demand a signature.
        pub operator_keys: OperatorKeys,

        /// Topic write-freeze policy, configured through `[freeze]`.
        pub freeze: FreezeConfig,

        /// Break-glass two-person rule policy, configured through `[break_glass]`.
        pub break_glass: BreakGlassConfig,

        /// KIP-108 / KIP-133 topic policy, configured through
        /// `[topic_policy]`. The default declares no rule, which is
        /// Kafka's default of no policy class.
        pub topic_policy: TopicPolicy,

        /// TLS configuration. `None` means no TLS, and is the default.
        pub tls_config: Option<TlsConfig>,

        /// KIP-371 `ssl.principal.mapping.rules` of the top-level
        /// `[tls_config]`. The controller listener maps the Subject DN of an
        /// mTLS peer certificate to its principal with these rules. The
        /// default is `DEFAULT`, under which the DN is the principal.
        pub tls_principal_mapper: SslPrincipalMapper,

        /// Which SASL mechanisms are enabled. An empty set means no SASL.
        pub enabled_sasl_mechanisms: Vec<SaslMechanism>,

        /// Validator for SASL/OAUTHBEARER bearer tokens. The broker reads it only
        /// when `OAuthBearer` is in `enabled_sasl_mechanisms`; the handshake does
        /// not advertise the mechanism otherwise. It defaults to the
        /// unsecured-JWS validator with principal claim `sub`. Set a JWKS
        /// endpoint in `[oauthbearer].jwks_endpoint_uri` to select the signed-JWT
        /// validator.
        pub oauthbearer_validator: krabka_security::OAuthBearerValidator,

        /// SASL/GSSAPI (Kerberos) configuration. `Some` only when `Gssapi` is in
        /// `enabled_sasl_mechanisms`; carries the service keytab path,
        /// `auth_to_local` rules, and KDC/realm settings for the initiate path.
        pub gssapi: Option<GssapiConfig>,

        /// JWKS endpoint to fetch OAUTHBEARER signing keys from. `Some`
        /// only when `oauthbearer_validator` is the signed variant. When set,
        /// `Broker::start` spawns a background refresher that fetches this URL and
        /// rotates the validator's key set on `oauthbearer_jwks_refresh_interval`.
        pub oauthbearer_jwks_endpoint: Option<String>,

        /// How often to re-fetch the JWKS. Default 5 minutes.
        #[default(DEFAULT_JWKS_REFRESH_INTERVAL)]
        pub oauthbearer_jwks_refresh_interval: Time,

        /// Optional PEM path for outbound HTTPS to the `IdP`. JWKS,
        /// introspection, and userinfo all share it. `None` selects reqwest's
        /// default webpki-roots.
        pub oauthbearer_idp_tls_trust: Option<PathBuf>,

        /// Receiver half of the JWKS refresher signal channel.
        ///
        /// `apply_to` creates the channel pair. It connects the sender to the
        /// signed validator's `JwksHandle` and stores the receiver here.
        /// `Broker::start` calls `take()` on the receiver and passes it to
        /// `JwksRefresher`. This field is `None` when JWKS validation is not
        /// configured.
        ///
        /// The field is an `Arc<Mutex<…>>` so that the containing `BrokerConfig`
        /// stays `Clone`. Only `Broker::start` locks and takes the receiver, and
        /// there is only ever one `Broker::start` for each validator
        /// construction.
        #[default(Arc::new(Mutex::new(None)))]
        pub oauthbearer_jwks_signal_rx: Arc<Mutex<Option<Receiver<()>>>>,

        /// Shared timestamp of the last successful JWKS fetch.
        ///
        /// `apply_to` creates it as `AtomicI64::new(0)`. The validator's
        /// `JwksHandle` and the refresher both clone this `Arc`, so the
        /// validator's expiry check sees the refresher's writes.
        #[default(shared_epoch_ms())]
        pub oauthbearer_jwks_last_successful_fetch_ms: Arc<AtomicI64>,

        /// Seqlock generation that makes the JWKS keys and successful-fetch
        /// timestamp one validation snapshot. Even values are stable; odd
        /// values mean the refresher is publishing a new pair.
        #[default(Arc::new(AtomicU64::new(0)))]
        pub oauthbearer_jwks_cache_generation: Arc<AtomicU64>,

        /// Shared on-demand-refresh timestamp for rate-limiting.
        ///
        /// `apply_to` creates it, and `Broker::start` gives a clone to the
        /// refresher. The validator never reads it. It is refresher-only
        /// bookkeeping that `BrokerConfig` carries for symmetry.
        #[default(shared_epoch_ms())]
        pub oauthbearer_jwks_last_on_demand_refresh_ms: Arc<AtomicI64>,

        /// Minimum pause between on-demand JWKS refreshes that validator signals
        /// trigger. `apply_to` sets it from
        /// `FileOAuthBearerConfig::jwks_min_refresh_pause_seconds`, and
        /// `Broker::start` passes it into `JwksRefresher`. Strimzi's default is
        /// 1 second, and this default is 1 second too.
        #[default(DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE)]
        pub oauthbearer_jwks_min_on_demand_pause: Time,

        // coordinators.
        /// Independent compatibility and protocol feature gates.
        #[default(default_feature_flags())]
        pub features: BrokerFeatureFlags,

        /// KIP-98 / KIP-939: how often the idle-transaction reaper scans for
        /// `Ongoing` transactions whose timeout has elapsed and aborts them. The
        /// reaper never reaps 2PC transactions. Mirrors Kafka's
        /// `transaction.abort.timed.out.transaction.cleanup.interval.ms` (10s).
        /// A zero interval disables the reaper entirely and spawns no background
        /// task. Zero is the default in `for_tests`, so a background abort does
        /// not disturb unit and integration tests. Tests that exercise the reaper
        /// set this value low explicitly.
        // KIP-98/KIP-939 idle-transaction reaper cadence (Kafka's
        // `transaction.abort.timed.out.transaction.cleanup.interval.ms`).
        #[default(DEFAULT_TXN_ABORT_CLEANUP_INTERVAL)]
        pub txn_abort_cleanup_interval: Time,

        /// KIP-98: how long a transactional id may sit in a terminal or idle
        /// state before the coordinator tombstones it out of
        /// `__transaction_state`. Mirrors Kafka's
        /// `transactional.id.expiration.ms` (7 days). Only `Empty`, `Dead`,
        /// `CompleteCommit` and `CompleteAbort` ids expire; an `Ongoing` or
        /// `Prepare*` transaction, a prepared KIP-939 2PC one above all,
        /// never does.
        // KIP-98 transactional-id expiry (Kafka's
        // `transactional.id.expiration.ms` and
        // `transaction.remove.expired.transaction.cleanup.interval.ms`).
        #[default(DEFAULT_TXN_ID_EXPIRATION)]
        pub txn_id_expiration: Time,

        /// KIP-98: how often the transactional-id expiry sweep scans the
        /// `__transaction_state` partitions this broker leads. Mirrors Kafka's
        /// `transaction.remove.expired.transaction.cleanup.interval.ms` (1
        /// hour). A zero interval disables the sweep entirely and spawns no
        /// background task. Zero is the default in `for_tests`, so a background
        /// tombstone does not disturb unit and integration tests. Tests that
        /// exercise the sweep set this value low explicitly, or drive
        /// `TxnCoordinator::expire_transactional_ids` against an injected clock.
        #[default(DEFAULT_TXN_ID_EXPIRATION_CLEANUP_INTERVAL)]
        pub txn_id_expiration_cleanup_interval: Time,

        /// Which static broker settings above this node's configuration
        /// named explicitly. `DescribeConfigs` reports a supplied value at
        /// `STATIC_BROKER_CONFIG` even when it equals the built-in
        /// default, because a config source is provenance and not a value
        /// comparison. See [`crate::config::StaticConfigOrigins`].
        pub static_config_origins: StaticConfigOrigins,

        /// KIP-848 next-gen consumer group protocol configuration. It controls
        /// which rebalance protocols the broker advertises, the session and
        /// heartbeat timeout bounds, and the set of enabled server-side
        /// assignors.
        pub next_gen_consumer_group: Box<NextGenConfig>,

        /// KIP-932 share-group configuration.
        pub share_group: Box<ShareGroupConfig>,

        /// KIP-1071 streams-group (Streams rebalance protocol) configuration.
        pub streams_group: Box<StreamsGroupConfig>,

        /// KIP-932 share-coordinator (persister) configuration. It controls the
        /// `__share_group_state` internal topic geometry and snapshot folding.
        pub share_coordinator: Box<ShareCoordinatorConfig>,

        // operations.
        /// How often the auto-rebalance ticker fires. Default 5 minutes.
        /// Matches Kafka's `leader.imbalance.check.interval.seconds`.
        #[default(DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL)]
        pub leader_imbalance_check_interval: Time,

        /// Test-only: override the cleaner ticker interval.
        /// Production callers leave this as `None` (default 30s).
        #[cfg(any(test, feature = "test-helpers"))]
        pub cleaner_interval_override: Option<Time>,

        /// How often the TLS reload watcher polls cert / key /
        /// client-CA file mtimes and rebuilds the `ServerConfig` if any of them
        /// changed. Defaults to 30s. Set it lower in tests to keep the watcher
        /// latency small. A zero interval disables the periodic watcher. Callers
        /// can still trigger an immediate reload with
        /// [`crate::BrokerHandle::reload_tls`].
        #[default(DEFAULT_TLS_RELOAD_INTERVAL)]
        pub tls_reload_interval: Time,

        /// Bind address for the Prometheus `/metrics` HTTP endpoint. `None`
        /// disables the server entirely. The broker still updates its internal
        /// counters, but nothing scrapes them. The default is
        /// `Some(0.0.0.0:9404)` in production, the same port the JMX exporter
        /// uses for vanilla Kafka. The default is `None` in `for_tests`, so unit
        /// tests do not compete for port allocation.
        // Default to `None` so multi-broker library users (and
        // multi-broker tests) don't race on a fixed port. The
        // `krabka-broker` binary opts in to `Some(0.0.0.0:9404)`
        // via its `--metrics-listen-addr` CLI flag — the operator
        // sets that via env, so production deployments still get
        // metrics by default.
        pub metrics_listen_addr: Option<SocketAddr>,

        /// Dedicated pprof administration listener. Profiling is disabled when
        /// this is `None`; enabled listeners must be loopback-only and use the
        /// bearer token stored in `profiling_auth_token_file`.
        pub profiling_listen_addr: Option<SocketAddr>,

        /// File containing the bearer token for the profiling listener.
        pub profiling_auth_token_file: Option<PathBuf>,

        /// CPU and heap profiling endpoint policy.
        pub profiling: ProfilingConfig,

        /// Whether this broker advertises the KIP-714 client-metrics RPCs,
        /// `GetTelemetrySubscriptions` (71) and `PushTelemetry` (72).
        ///
        /// Kafka advertises the pair only when `metric.reporters` holds a
        /// `ClientTelemetry` implementation, so a stock broker offers no
        /// telemetry handshake and a modern Java or librdkafka client
        /// starts none. This key is krabka's equivalent, and the default is
        /// `false` for the same reason. A configured
        /// [`client_metrics_otlp_endpoint`][Self::client_metrics_otlp_endpoint]
        /// is a receiver in its own right and turns the pair on without
        /// this key; [`BrokerConfig::client_metrics_receiver`] is where the
        /// two meet.
        ///
        /// Turning advertisement off does not unregister the handlers. A
        /// client that sends either RPC anyway is still answered.
        pub client_metrics_enable: bool,

        /// Optional OTLP endpoint for KIP-714 client metrics forwarding.
        /// Binaries populate it from their parsed runtime configuration. The
        /// broker does not read it from the environment at startup.
        pub client_metrics_otlp_endpoint: Option<String>,
        /// Transport used by the KIP-714 client-metrics forwarder.
        #[default(OtlpProtocol::Grpc)]
        pub client_metrics_otlp_protocol: krabka_telemetry::OtlpProtocol,

        /// The live `tracing` filter behind Kafka's `BROKER_LOGGER` config
        /// resource, which `kafka-configs --entity-type broker-loggers`
        /// describes and alters.
        ///
        /// The binary takes it from
        /// [`krabka_telemetry::TelemetryGuard::log_levels`], so a level an
        /// operator writes lands on the subscriber this process installed.
        /// A change is node-local and never reaches cluster metadata,
        /// exactly as on a JVM broker. A config built without a
        /// subscriber gets a controller that tracks levels and drives no
        /// layer.
        // The broker binary replaces this with the controller that drives
        // the subscriber it installed. The seed matters for every other
        // way a config is built: it carries the same directives as
        // `DEFAULT_LOG_FILTER`, so a `BROKER_LOGGER` describe lists the
        // krabka targets before anything has logged through them.
        #[default(LogLevelController::new(DEFAULT_LOG_FILTER).0)]
        pub log_levels: krabka_telemetry::LogLevelController,

        /// KIP-227: maximum number of incremental-fetch sessions kept in the
        /// per-broker cache. Each session tracks the (topic, partition) set a
        /// client is subscribed to, so later fetches can be deltas. When the
        /// cache is full, a new session displaces only a session that has
        /// been unused for more than two minutes, or a smaller one older than
        /// that (a follower-fetch session may also displace any consumer
        /// session); otherwise it is refused and the fetch runs sessionless.
        /// Matches Apache Kafka's `max.incremental.fetch.session.cache.slots`
        /// (default 1000).
        #[default(DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS)]
        pub max_incremental_fetch_session_cache_slots: usize,

        /// Maximum number of live broker connections across all listeners. The
        /// broker immediately closes any new connection it accepts past this
        /// ceiling; Kafka silently drops them. Matches Apache Kafka's
        /// `max.connections`. The default is `usize::MAX`, which is unlimited
        /// and mirrors Kafka's `Integer.MAX_VALUE`.
        // Connection caps unlimited by default, matching Kafka's
        // max.connections / max.connections.per.ip (Integer.MAX_VALUE).
        #[default(usize::MAX)]
        pub max_connections: usize,

        /// Maximum number of live connections from any single client IP. The
        /// broker immediately closes connections past this per-IP ceiling.
        /// Matches Apache Kafka's `max.connections.per.ip`. The default is
        /// `usize::MAX`, which is unlimited.
        #[default(usize::MAX)]
        pub max_connections_per_ip: usize,

        /// How long a connection may go with no socket activity before the
        /// broker closes it. Matches Apache Kafka's
        /// `connections.max.idle.ms`, whose default is 600000, ten
        /// minutes. As in Kafka's `Selector`, each read that brings bytes
        /// and each response write restarts the window, and it keeps
        /// running while a request is in its handler or a throttled
        /// connection is muted. It reclaims a peer that completed the TCP
        /// (and TLS) handshake and then sent nothing, a peer that vanished
        /// without a FIN, and a request that outlasts it, whose response is
        /// then dropped. A non-positive value disables idle
        /// expiry, the way Kafka arms no `IdleExpiryManager` for one.
        ///
        /// `None` is the operator having set nothing, and runs Kafka's
        /// 600000. It is not the same state as `Some` of that same value:
        /// Kafka's `DescribeConfigs` reports a key's source from where the
        /// value came rather than from what it is, so an explicit
        /// `connections.max.idle.ms=600000` reports `STATIC_BROKER_CONFIG`
        /// while an unset one reports `DEFAULT_CONFIG`.
        /// [`BrokerConfig::effective_connections_max_idle`] resolves the
        /// two for every caller that only wants the window.
        // Unset: the broker runs Kafka's connections.max.idle.ms default
        // and reports the key at DEFAULT_CONFIG. No per-listener override
        // until an operator writes one.
        pub connections_max_idle: Option<Time>,

        /// Per-listener overrides of `connections_max_idle`, keyed by
        /// listener name. Kafka spells the same override
        /// `listener.name.<name>.connections.max.idle.ms` and reports it
        /// from `DescribeConfigs`.
        pub connections_max_idle_overrides: BTreeMap<String, Time>,

        /// KIP-368 `connections.max.reauth.ms`: the maximum lifetime of an
        /// authenticated SASL session before the client must
        /// re-authenticate in band. The broker reports the window to the
        /// client as `SaslAuthenticate.session_lifetime_ms`, and closes
        /// the connection when it elapses without a fresh
        /// `SaslHandshake` + `SaslAuthenticate` pair carrying the same
        /// principal.
        ///
        /// It applies to every mechanism. PLAIN, SCRAM and GSSAPI
        /// credentials carry no lifetime of their own, so this is the only
        /// ceiling on them; an OAUTHBEARER session or a KIP-48
        /// delegation-token session expires at the earlier of its
        /// credential's expiry and this window, as Kafka's
        /// `SaslServerAuthenticator` computes it.
        ///
        /// `None`, and any non-positive value, disable re-authentication:
        /// sessions then last until the credential's own expiry, if it has
        /// one, and otherwise for the life of the connection. That is
        /// Kafka's default of 0.
        pub connections_max_reauth: Option<Time>,

        /// Per-listener overrides of `connections_max_reauth`, keyed by
        /// listener name. Kafka spells the same override
        /// `listener.name.<name>.<mechanism>.connections.max.reauth.ms`;
        /// krabka keys it by listener alone, because the window it
        /// enforces is a property of the listener rather than of the
        /// mechanism a client happens to pick on it.
        pub connections_max_reauth_overrides: BTreeMap<String, Time>,

        /// Partition disk-usage scan cadence. A zero interval disables the
        /// scanner entirely and spawns no background task. Production default:
        /// 60s. On each tick the scanner walks every known (topic, partition)
        /// under `log_dir`, sums the regular-file sizes, and updates the
        /// `partition_disk_bytes` gauge that the rebalancer's usage scraper
        /// reads.
        #[default(secs(60))]
        pub partition_disk_scan_interval: Time,

        // delegation tokens.
        /// KIP-48: HMAC-SHA-512 master key that mints and verifies delegation
        /// tokens. When `None`, the broker rejects all four delegation-token RPCs
        /// with `DELEGATION_TOKEN_AUTH_DISABLED`, and SCRAM cannot fall back to
        /// token lookup. The broker reads the key from
        /// `KRABKA_DELEGATION_TOKEN_SECRET_KEY` or from `[delegation_token]
        /// secret_key` in `broker.toml`; the environment variable wins. The key
        /// is wrapped in `SecretBytes`, so `Debug` redacts the bytes.
        // Master key off by default. Operators flip this on
        // via `KRABKA_DELEGATION_TOKEN_SECRET_KEY` env var or the
        // `[delegation_token] secret_key` TOML stanza.
        pub delegation_token_secret_key: Option<SecretBytes>,

        /// KIP-48: hard upper bound on delegation-token lifetime.
        /// A token's `max_timestamp_ms` is set to
        /// `issue_timestamp_ms + delegation_token_max_lifetime` and the
        /// renew handler clamps any caller-requested expiry to this. Default
        /// 7 days (`delegation.token.max.lifetime.ms` in Kafka).
        #[default(DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME)]
        pub delegation_token_max_lifetime: Time,

        /// KIP-48: cadence of the background sweep task that
        /// proposes `V1DeleteDelegationToken` tombstones for tokens whose
        /// `expiry_timestamp_ms` or `max_timestamp_ms` is in the past. Default
        /// 1 hour (`delegation.token.expiry.check.interval.ms` in Kafka).
        #[default(DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL)]
        pub delegation_token_expiry_check_interval: Time,

        /// KIP-48: default renew period. The broker uses it as the *initial*
        /// `expiry_timestamp_ms` offset at create time, and as the implicit renew
        /// period when `RenewDelegationToken.renew_period_ms == -1`. It differs
        /// from `delegation_token_max_lifetime`, the absolute ceiling that
        /// `Renew` can never push `expiry_timestamp_ms` past. A fresh token gets
        /// `expiry_timestamp_ms = now + min(default_renew_period,
        /// chosen_max_lifetime)` and `max_timestamp_ms = now +
        /// chosen_max_lifetime`. Default 24 hours
        /// (`delegation.token.expiry.time.ms` in Kafka).
        #[default(DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD)]
        pub delegation_token_default_renew_period: Time,

        // remote storage.
        /// KIP-405: tiered-storage backend selection. `Some(_)` enables tiered
        /// storage broker-wide and spawns the `RemoteLogManager` copy task. This
        /// one field replaces Kafka's `remote.log.storage.system.enable` plus the
        /// RSM selection. `remote.storage.enable` still gates per-topic offload.
        /// `None` (default) leaves tiered storage off.
        ///
        /// TOML:
        /// - Local: `[remote_storage] storage_dir = "..."`
        /// - S3:    `[remote_storage.s3] bucket = "..." region = "..."`
        // Tiered storage off by default. Operators enable it
        // via `[remote_storage] storage_dir` in `broker.toml`.
        pub remote_storage_backend: Option<RemoteStorageBackend>,

        /// KIP-405: tick cadence of the `RemoteLogManager` copy /
        /// retention task. Defaults to 30s (Kafka's
        /// `remote.log.manager.task.interval.ms`). Acceptance tests lower this
        /// so segments are tiered and locally evicted in seconds rather than
        /// minutes; production deployments leave it at the default.
        #[default(DEFAULT_REMOTE_LOG_MANAGER_INTERVAL)]
        pub remote_log_manager_interval: Time,

        /// KIP-405: deadline on one segment copy to the remote tier.
        /// Defaults to 10 minutes.
        ///
        /// A partition holds one copier slot for the whole of its copy
        /// pass, so a copy that hangs -- an object store that accepts the
        /// connection and then answers nothing -- spends that slot on a
        /// partition moving no bytes. Past this deadline the copy is
        /// abandoned: the segment stays in `CopySegmentStarted`, which
        /// local retention refuses to delete against, and the next tick
        /// retries it under a fresh segment id.
        ///
        /// TOML: `[remote_storage] copy_timeout = "10m"`
        #[default(DEFAULT_REMOTE_COPY_TIMEOUT)]
        pub remote_copy_timeout: Time,

        /// KIP-405: how many partitions may be copying segments to the
        /// remote tier at once (Kafka's
        /// `remote.log.manager.copier.thread.pool.size`, default 10).
        ///
        /// One tick sweeps every partition concurrently and a partition
        /// holds one copier slot for the whole of its copy pass, so this
        /// is the number of uploads a broker will have in flight. Raising
        /// it is the remedy for a copy backlog whose object store has
        /// bandwidth left; the sweep is not serial, so a slow partition
        /// costs a slot rather than the whole tick.
        ///
        /// TOML: `[remote_storage] copier_threads = 10`
        #[default(DEFAULT_REMOTE_COPIER_THREADS)]
        pub remote_copier_threads: usize,

        /// KIP-405: how many partitions may be running their retention
        /// passes at once (Kafka's
        /// `remote.log.manager.expiration.thread.pool.size`, default 10).
        ///
        /// Local eviction and the remote-retention delete travel together
        /// under one slot, so this bounds the deletes the broker issues
        /// against the object store the way `copier_threads` bounds the
        /// uploads.
        ///
        /// TOML: `[remote_storage] expiration_threads = 10`
        #[default(DEFAULT_REMOTE_EXPIRATION_THREADS)]
        pub remote_expiration_threads: usize,

        /// KIP-405: which RLMM the broker runs when tiered storage is enabled.
        /// It defaults to [`RlmmKind::TopicBacked`] in production, and to
        /// [`RlmmKind::InMemory`] for in-process tests. The broker ignores it
        /// when `remote_storage_backend` is `None`.
        // Production default: topic-backed RLMM. `bootstrap` and
        // `snapshot_dir` are empty; the broker derives them at startup.
        #[default(RlmmKind::TopicBacked(KafkaRlmmConfig::default()))]
        pub remote_log_metadata: RlmmKind,

        // A sibling of `remote_storage_backend`, deliberately not a
        // `RemoteStorageBackend` variant. WORM is orthogonal to which object
        // store is in use — it layers over S3 and GCS alike — and
        // `RemoteStorageBackend` is also consumed by `build_diskless_read_handle`,
        // which must not change shape. Do not "tidy" this into the enum.
        /// WORM archive mode for the tiered-storage object store (`Some`), or
        /// ordinary mutable tiered storage (`None`, the default).
        ///
        /// When set, every object the `RemoteStorageManager` writes is a
        /// conditional create, each segment gets a hash-chained and optionally
        /// Ed25519-signed integrity manifest, the backend refuses every delete,
        /// and the `RemoteLogManager`'s remote-retention pass is disabled for its
        /// partitions. Local retention is unaffected: the broker still evicts
        /// local segments once they are archived.
        ///
        /// Requires an object-store backend. `storage_dir` cannot enforce
        /// write-once.
        ///
        /// TOML: `[remote_storage.worm]`
        // WORM archive mode off by default. Operators enable it via
        // `[remote_storage.worm]` in `broker.toml`.
        pub remote_storage_worm: Option<WormConfig>,

        /// KIP-405: how many cold-tier reads may be in flight at once
        /// (Kafka's `remote.log.reader.threads`, default 10).
        ///
        /// Every remote read runs on the tokio blocking pool that WAL
        /// fsync, local fetch and replica IO also use, so without a cap a
        /// burst of cold-tier consumers starves the local paths. The
        /// broker ignores it when `remote_storage_backend` is `None`.
        ///
        /// TOML: `[remote_storage] reader_threads = 10`
        #[default(DEFAULT_REMOTE_READER_THREADS)]
        pub remote_reader_threads: usize,

        /// KIP-405: how many cold-tier reads may wait for a reader slot
        /// before the broker refuses one (Kafka's
        /// `remote.log.reader.max.pending.tasks`, default 100).
        ///
        /// A `Fetch` that arrives with the queue already full is answered
        /// with an error for that partition rather than parked, which is
        /// what Kafka's `RejectedExecutionException` path does.
        ///
        /// TOML: `[remote_storage] reader_max_pending_tasks = 100`
        #[default(DEFAULT_REMOTE_READER_MAX_PENDING_TASKS)]
        pub remote_reader_max_pending_tasks: usize,

        /// KIP-405: the total-byte budget of the on-disk cache of remote
        /// segment index objects under
        /// `<log_dir>/remote-log-index-cache` (Kafka's
        /// `remote.log.index.file.cache.total.size.bytes`, default 1 GiB).
        ///
        /// Without it every `Fetch` that lands in the cold tier
        /// re-downloads the segment's `.index`, and a read-committed one
        /// its `.txnindex` as well. The broker ignores it when
        /// `remote_storage_backend` is `None`.
        ///
        /// TOML: `[remote_storage] index_cache_size = "1GiB"`
        #[default(DEFAULT_REMOTE_INDEX_CACHE_SIZE)]
        pub remote_index_cache_size: ByteSize,

        // audit.
        /// Whether the audit subsystem is active (`FedRAMP` MLA).
        // Audit enabled by default (secure-by-default / `FedRAMP` MLA).
        #[default(true)]
        pub audit_enabled: bool,
        /// Whether privileged operations continue when their audit record
        /// cannot be durably written.
        #[default(AuditMode::FailOpen)]
        pub audit_failure_mode: krabka_audit::AuditMode,
        /// Internal topic name for audit records. `BrokerConfig::validate`
        /// requires the `__` prefix, so the flag on `Metadata` and the
        /// freeze refusal cover the same name.
        #[default(DEFAULT_AUDIT_TOPIC.to_owned())]
        pub audit_topic: String,
        /// Path to the PKCS#8 Ed25519 audit checkpoint signing key. `None` means
        /// no checkpoints.
        pub audit_signing_key_path: Option<PathBuf>,
        /// Key id recorded on checkpoints (for rotation).
        pub audit_signing_key_id: Option<String>,
        /// Emit a checkpoint after this many audit records.
        #[default(DEFAULT_AUDIT_CHECKPOINT_EVERY_N)]
        pub audit_checkpoint_every_n: u64,
        /// Emit a checkpoint at least this often.
        #[default(DEFAULT_AUDIT_CHECKPOINT_EVERY)]
        pub audit_checkpoint_every: Time,
        /// Directory for the durable audit spool. A relative path resolves under
        /// the broker's log dir.
        #[default(PathBuf::from(DEFAULT_AUDIT_SPOOL_DIR))]
        pub audit_spool_dir: PathBuf,
        /// Cap on the audit spool size.
        #[default(DEFAULT_AUDIT_SPOOL_MAX)]
        pub audit_spool_max: ByteSize,
        /// Number of spool appends between durable file syncs.
        #[default(DEFAULT_AUDIT_SPOOL_SYNC_EVERY_N)]
        pub audit_spool_sync_every_n: NonZeroU64,
    }
}
