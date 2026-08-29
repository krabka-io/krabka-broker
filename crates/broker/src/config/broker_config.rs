//! The [`BrokerConfig`] struct itself: every knob the broker reads at
//! construction time, in one place, because the type is a single item that
//! cannot be divided across modules.

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};

use krabka_log::LogConfig;
use krabka_raft::{
    BootstrapMode, ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity,
    MetadataRaftFetchMax, NodeId,
};
use krabka_security::{SaslMechanism, TlsConfig};
use krabka_units::{ByteSize, Ratio, Time};

use crate::{
    config::{
        BreakGlassConfig, BrokerFeatureFlags, FreezeConfig, InterBrokerCredentials, ListenerSpec,
        NodeRole, RemoteStorageBackend, ReplicationRuntimeConfig, RlmmKind, StretchProfile,
    },
    operator_keys::OperatorKeys,
};

#[derive(Debug, Clone)]
// a broad config struct; flags are independent knobs
pub struct BrokerConfig {
    /// Capacity used by every outbound Kafka client connection owned by this
    /// broker process.
    pub client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    /// Maximum frame size used by every outbound Kafka client connection owned
    /// by this broker process.
    pub client_frame_max: krabka_client_core::ClientFrameMax,
    /// Maximum time to wait for a controller leader during startup.
    pub startup_leader_wait_timeout: Time,
    /// Initial delay between self-registration attempts.
    pub self_registration_backoff_min: Time,
    /// Maximum delay between self-registration attempts.
    pub self_registration_backoff_max: Time,
    /// Observer promotion polling cadence.
    pub observer_poll_interval: Time,
    /// Audit spool replay cadence.
    pub audit_spool_replay_interval: Time,
    /// Audit statistics polling cadence.
    pub audit_stats_poll_interval: Time,
    /// Maximum wait for the audit partition to become available.
    pub audit_partition_wait_timeout: Time,
    /// Broker liveness maintenance cadence.
    pub liveness_tick_interval: Time,
    /// Broker gauge refresh cadence.
    pub gauge_poll_interval: Time,
    /// In-sync replica maintenance cadence.
    pub isr_scan_interval: Time,
    /// Log cleaner maintenance cadence.
    pub cleaner_interval: Time,
    /// Retry delay when moving a future log fails.
    pub future_log_move_retry_backoff: Time,
    /// Client-metrics cache eviction cadence.
    pub client_metrics_eviction_tick: Time,
    /// Minimum age at which client metrics are stale.
    pub client_metrics_stale_floor: Time,
    /// Default client telemetry subscription interval.
    pub client_metrics_default_interval: Time,
    /// Capacity of the client-metrics OTLP forwarding queue.
    pub client_metrics_otlp_queue_capacity: usize,
    /// Maximum accepted client telemetry payload size.
    pub client_metrics_telemetry_max: ByteSize,
    /// Prometheus client-metrics snapshot lifetime.
    pub client_metrics_prom_snapshot_ttl: Time,
    /// Remote-log metadata reconciliation cadence.
    pub rlmm_reconcile_tick: Time,
    /// Initial remote-log metadata bootstrap retry delay.
    pub rlmm_bootstrap_backoff_initial: Time,
    /// Maximum remote-log metadata bootstrap retry delay.
    pub rlmm_bootstrap_backoff_max: Time,
    /// Maximum connection-creation quota delay.
    pub connection_creation_throttle_max: Time,
    /// OPA authorization request timeout.
    pub opa_http_timeout: Time,
    /// Schema registry request timeout.
    pub schema_registry_http_timeout: Time,
    /// OAuth JWKS HTTP request timeout.
    pub oauth_jwks_http_timeout: Time,
    /// Dynamic-quorum auto-join retry delay.
    pub auto_join_retry_backoff: Time,
    /// Timeout carried by dynamic-quorum `AddRaftVoter` requests.
    pub auto_join_voter_request_timeout: Time,
    /// Follower replication runtime policy.
    pub replication: ReplicationRuntimeConfig,
    /// Consumer-group session expiry scan cadence.
    pub coordinator_session_expiry_tick: Time,
    /// Maximum wait for coordinator shutdown acknowledgements.
    pub coordinator_shutdown_ack_timeout: Time,
    /// Initial delay before a classic group begins rebalancing.
    pub classic_group_initial_rebalance_delay: Time,
    /// Maximum time a follower waits for a `SyncGroup` assignment.
    pub sync_group_follower_wait: Time,
    /// Aggressive unclean-recovery collection deadline.
    pub unclean_recovery_aggressive_deadline: Time,
    /// Balanced unclean-recovery collection deadline.
    pub unclean_recovery_balanced_deadline: Time,
    /// Operator-triggered recovery deadline.
    pub operator_recovery_deadline: Time,
    /// Maximum quota throttle delay.
    pub quota_throttle_max: Time,
    /// Window whose throughput defines the controller-mutation burst capacity.
    pub controller_mutation_quota_window: Time,
    /// Maximum self-registration attempts before startup fails.
    pub self_registration_max_attempts: u32,
    /// Maximum bytes fetched by a metadata observer request.
    pub observer_fetch_max: ByteSize,
    /// Capacity of the asynchronous audit event queue.
    pub audit_event_queue_capacity: usize,
    /// Number of offsets included in an audit tail request.
    pub audit_tail_window_offsets: i64,
    /// Maximum bytes read by an audit tail request.
    pub audit_tail_read_max: ByteSize,
    /// Maximum wait for offsets-topic metadata.
    pub offsets_topic_metadata_wait_timeout: Time,
    /// Push intervals after which client metrics become stale.
    pub client_metrics_stale_push_intervals: u32,
    /// Capacity of each coordinator actor mailbox.
    pub coordinator_actor_mailbox_capacity: usize,
    /// Number of broker voters in each diskless WAL quorum.
    pub diskless_wal_local_replica_count: usize,
    /// Cadence of diskless WAL object-store flushes.
    pub diskless_wal_flush_interval: Time,
    /// Maximum bytes included in one diskless WAL object-store flush.
    pub diskless_wal_flush_max_size: ByteSize,
    /// Committed offsets retained behind the diskless WAL trim frontier.
    pub diskless_wal_trim_safety_lag: i64,
    /// Maximum wait for a published diskless WAL index record to be projected.
    pub diskless_wal_index_projection_timeout: Time,
    /// Capacity of the unclean-recovery work queue.
    pub unclean_recovery_queue_capacity: usize,
    /// Maximum bytes read while recovering share state.
    pub share_recovery_read_max: ByteSize,
    /// Share-session cache ceiling when group count is unlimited.
    pub share_session_cache_max_when_unlimited: usize,
    /// Maximum encoded request size accepted from a socket.
    pub socket_request_max: ByteSize,
    /// Minimum response size eligible for `sendfile`.
    pub sendfile_min: ByteSize,
    /// Broker socket send-buffer size.
    pub socket_send_buffer: ByteSize,
    /// Broker socket receive-buffer size.
    pub socket_receive_buffer: ByteSize,
    /// Maximum encoded ACL principal length.
    pub acl_max_principal: ByteSize,
    /// Maximum encoded ACL resource-name length.
    pub acl_max_resource_name: ByteSize,
    /// Maximum accepted telemetry decompression ratio.
    pub telemetry_max_decompression_ratio: Ratio,
    /// Minimum telemetry decompression output allowance.
    pub telemetry_decompressed_output_floor: ByteSize,
    /// Maximum telemetry decompression output allowance.
    pub telemetry_decompressed_output_ceiling: ByteSize,
    /// Maximum accepted Kafka record decompression ratio.
    pub record_decompression_max_ratio: Ratio,
    /// Minimum Kafka record decompression output allowance.
    pub record_decompression_output_floor: ByteSize,
    /// Maximum Kafka record decompression output allowance.
    pub record_decompression_output_ceiling: ByteSize,
    /// TLS server name used for outbound inter-broker connections.
    pub inter_broker_server_name: String,
    /// Producer-id inactivity period before state expires.
    pub producer_id_expiration: Time,
    /// Producer-state expiry scan cadence.
    pub producer_id_expiration_scan_interval: Time,
    /// Maximum produce requests combined into one append group.
    pub max_produce_group: usize,
    /// Capacity of each partition-writer request queue.
    pub partition_writer_queue_depth: usize,
    /// Default minimum in-sync replica count.
    pub default_min_insync_replicas: i32,
    /// Bytes copied per future-log move read.
    pub future_log_move_read_chunk: ByteSize,
    /// Partition count for the consumer-offsets internal topic.
    pub offsets_topic_num_partitions: i32,
    /// Desired replication factor for the consumer-offsets internal topic.
    pub offsets_topic_replication_factor: i16,
    /// Partition count for the transaction-state internal topic.
    pub transaction_state_num_partitions: i32,
    /// Maximum bytes requested by each transaction-state recovery read.
    pub transaction_recovery_read_max: ByteSize,
    /// Desired replication factor for the transaction-state internal topic.
    pub transaction_state_replication_factor: i16,
    /// Minimum accepted transaction timeout.
    pub transaction_min_timeout: Time,
    /// Maximum accepted transaction timeout.
    pub transaction_max_timeout: Time,
    /// Partition count for the `__barrier_state` internal topic.
    pub barrier_state_num_partitions: i32,
    /// Desired replication factor for the `__barrier_state` internal topic.
    pub barrier_state_replication_factor: i16,
    /// Shortest periodic injection interval a barrier group may ask for.
    pub barrier_min_injection_interval: Time,
    /// Deadline for one barrier injection to reach every target partition.
    pub barrier_injection_timeout: Time,
    /// Maximum bytes requested by each `__barrier_state` recovery read.
    pub barrier_recovery_read_max: ByteSize,
    /// Number of cuts a barrier group keeps before it tombstones the oldest.
    pub barrier_retained_cuts: i32,
    /// Maximum number of barrier groups the cluster accepts.
    pub barrier_max_groups: usize,
    /// Maximum number of topics in one barrier group.
    pub barrier_max_topics_per_group: usize,

    /// Broker id reported in `Metadata` responses. Default: 1.
    pub broker_id: i32,

    /// `KRaft` `process.roles`. It controls whether this node is a metadata
    /// quorum voter (`Controller`), hosts data partitions and registers as a
    /// broker (`Broker`), or both. Default: `[Controller, Broker]`.
    pub roles: Vec<NodeRole>,

    /// TCP address to listen on. Default: `127.0.0.1:9092`.
    pub listen_addr: SocketAddr,

    /// `host:port` returned in `Metadata` responses as this broker's
    /// advertised endpoint. Defaults to `listen_addr`'s string form.
    pub advertised_listener: String,

    /// Primary log directory. It holds the `__cluster_metadata` raft log, and
    /// the broker reads it to detect bootstrap mode. It is also a data
    /// directory: when [`extra_log_dirs`][Self::extra_log_dirs] is empty,
    /// partition data lives only here. The broker creates the directory on
    /// startup if it is missing. Default: `./krabka-data`.
    pub log_dir: PathBuf,

    /// Extra JBOD data directories (KIP-113). When this list is non-empty,
    /// the broker spreads new partitions across `[log_dir] + extra_log_dirs`
    /// by least-loaded placement. `__cluster_metadata` always stays on
    /// [`log_dir`][Self::log_dir]. Maps to a Kafka `log.dirs` value with more
    /// than one entry. Default: empty, which gives a single-directory broker.
    pub extra_log_dirs: Vec<PathBuf>,

    /// Per-log configuration applied to every partition this broker hosts.
    pub log_config: LogConfig,

    /// Optional internal timestamp source shared by every hosted partition.
    ///
    /// `None` is the Kafka-only default: no `.stampindex` sidecar is opened and
    /// record bytes, offsets, LSO, and high-watermark behavior stay unchanged.
    /// A combined SQL/Kafka runtime injects its tenant timestamp source here.
    pub stamp_source: Option<Arc<dyn krabka_log::StampSource>>,

    /// Raft node id. Conventionally equal to `broker_id as NodeId`.
    pub node_id: NodeId,

    /// Address the controller listener binds on. `KRaft` convention: same
    /// host as `listen_addr`, port 9093. Test default: `127.0.0.1:0`.
    pub controller_listen_addr: SocketAddr,

    /// Static voter set: `[(node_id, "<host>:<port>"), …]`. The address is the
    /// peer controller listener's `<host>:<port>`, carried verbatim and NOT
    /// pre-resolved. The dialer resolves the host again on every connect and
    /// reconnect, so a peer that restarts on a new pod IP stays reachable.
    /// Defaults to a single-voter cluster of just this broker, so
    /// single-broker setups get a quorum of one without config changes.
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
    /// `meta.properties.json` at boot. Identifies which voter this node *is*.
    pub directory_id: uuid::Uuid,

    /// UUID for this broker process invocation. The broker keeps it in
    /// `{log_dir}/incarnation_id` and reloads it on restart. The internal
    /// `load_or_generate` helper sets it before self-registration. Tests
    /// generate a random UUID for each call through [`Self::for_tests`].
    pub incarnation_id: uuid::Uuid,

    /// KIP-853: when true, an observer issues `AddVoter` for itself once it
    /// has caught up to the leader. The observer joins the quorum without
    /// operator action. Maps to Kafka's `controller.quorum.auto.join.enable`.
    pub auto_join: bool,

    /// KIP-853: maximum log-entry lag an observer may have and still be
    /// promotable to a voter. The broker forwards it to `ControllerConfig`.
    pub observer_lag_bound: u64,

    /// How often each broker sends `BrokerHeartbeat` to the controller
    /// leader. Default 3s.
    pub heartbeat_interval: Time,
    /// Controller marks a broker dead after this long without a
    /// heartbeat. Default 9s.
    pub heartbeat_timeout: Time,
    /// Leader proposes ISR shrink when a follower lags more than this.
    /// Default 30s.
    pub replica_lag_time_max: Time,

    /// Openraft election timeout. It sets `election_timeout_min`, and the
    /// maximum is 2×. It also sets `leader_lease = election_timeout_max`
    /// inside openraft's engine. Peers refuse to grant a new leader's vote
    /// until the lease expires, so this value is also the lower bound on how
    /// fast a 3-broker cluster recovers from a dead controller leader.
    /// Default 5s. The default is conservative and avoids a split vote on
    /// slow runners.
    pub controller_election_timeout: Time,

    /// Openraft heartbeat interval. Default 500ms. It should be ≤
    /// `controller_election_timeout / 3` by raft consensus norms.
    pub controller_heartbeat_interval: Time,
    /// Whether the heartbeat interval was explicitly configured. Omitted
    /// values preserve the Raft engine's election-timeout-derived cadence.
    pub controller_heartbeat_interval_explicit: bool,
    /// Consecutive follower fetch misses tolerated before a new election.
    pub controller_fetch_miss_limit: ControllerFetchMissLimit,
    /// Capacity of the metadata Raft engine command queue.
    pub metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
    /// Per-read and per-snapshot-request metadata Raft byte budget.
    pub metadata_raft_fetch_max: MetadataRaftFetchMax,

    /// `metadata.log.max.record.bytes.between.snapshots` (default 20 MiB).
    pub metadata_max_bytes_between_snapshots: ByteSize,

    /// `metadata.log.max.snapshot.interval.ms` (default 1 h; zero = disabled).
    pub metadata_max_snapshot_interval: Time,

    /// KIP-630: snapshot the metadata log once committed offset advances this
    /// many records past the last snapshot, then prune below it.
    pub metadata_snapshot_interval_records: u64,

    /// Maximum metadata snapshot size a follower will fetch. The core enforces
    /// an immutable 1 GiB security ceiling.
    pub metadata_snapshot_fetch_max: ByteSize,

    /// How this broker takes part in cluster formation. See
    /// [`krabka_raft::BootstrapMode`] for the trade-offs. The first broker of
    /// a fresh multi-broker cluster uses `Bootstrap`. Later brokers use
    /// `Join`. A restart of any previously-formatted broker uses `Rejoin`.
    /// Single-broker setups always use `Bootstrap`.
    pub bootstrap_mode: BootstrapMode,

    /// Cluster UUID that the broker forwards to
    /// `ControllerConfig::cluster_id`. The operator supplies it as the
    /// `KafkaCluster` UID through `--cluster-id`. `None` defaults to
    /// `Uuid::nil()` inside `Controller::start`.
    pub cluster_id: Option<uuid::Uuid>,

    /// KIP-392: this broker's rack identifier (`broker.rack`). The broker
    /// reports it in its `BrokerRegistrationRecord`, and the leader's
    /// rack-aware replica selector reads it. `None` (default) means no rack.
    pub rack: Option<String>,

    /// KIP-392: which replica selector the leader runs to populate
    /// `FetchResponse.preferred_read_replica` for rack-aware consumers.
    /// Default `Leader` (never redirect).
    pub replica_selector: crate::replica_selector::ReplicaSelectorKind,

    /// The three-site stretch deployment this node belongs to. `None`
    /// (default) is an ordinary, non-stretched cluster. When it is `Some`,
    /// [`rack`][Self::rack] must name one of the profile's sites, and
    /// [`validate`][Self::validate] checks that the roles of this node agree
    /// with that site.
    pub stretch: Option<StretchProfile>,

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
    pub controller_listener_protocol: krabka_security::ListenerProtocol,

    /// Name of the listener used for inter-broker traffic (raft, replication,
    /// heartbeat). Must match a name in `listeners` when `listeners` is
    /// non-empty. Default: `"PLAINTEXT"`.
    pub inter_broker_listener_name: String,

    /// Credentials the broker uses for outbound inter-broker connections.
    /// `None` means no SASL, which gives plaintext inter-broker traffic.
    /// This is the default.
    pub inter_broker_credentials: Option<InterBrokerCredentials>,

    /// Static PLAIN credentials: username → password. Empty by default.
    /// PLAIN auth stays disabled until you explicitly enable the mechanisms.
    pub plain_credentials: HashMap<String, String>,

    /// Usernames that bypass ACL checks (super-users). The
    /// `create_delegation_token` act-as gate reads this directly; the
    /// active [`crate::authorizer::Authorizer`] impl also reads it
    /// (`SimpleAclAuthorizer` / `OpaAuthorizer`). `file_config` populates
    /// both from the same `[authorization]` TOML stanza.
    pub super_users: std::collections::HashSet<String>,

    /// Pluggable cluster authorizer. There is one boxed instance for each
    /// broker, configured through `[authorization]` in `broker.toml`. The
    /// default is [`crate::authorizer::AllowAllAuthorizer`], an explicit
    /// "allow everything" policy.
    pub authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,

    /// KFC-7 schema validator: the registry client and its cache, shared by
    /// every produce on this broker. Configured through `[schema_registry]` in
    /// `broker.toml`. `None` is the default and means no `[schema_registry]`
    /// section was configured, so a topic that turns `schema.validation.*` on
    /// has nothing to validate against.
    pub schema_validator: Option<std::sync::Arc<crate::schema_validation::SchemaValidator>>,

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

    /// TLS configuration. `None` means no TLS, and is the default.
    pub tls_config: Option<TlsConfig>,

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
    pub gssapi: Option<krabka_security::gssapi::GssapiConfig>,

    /// JWKS endpoint to fetch OAUTHBEARER signing keys from. `Some`
    /// only when `oauthbearer_validator` is the signed variant. When set,
    /// `Broker::start` spawns a background refresher that fetches this URL and
    /// rotates the validator's key set on `oauthbearer_jwks_refresh_interval`.
    pub oauthbearer_jwks_endpoint: Option<String>,

    /// How often to re-fetch the JWKS. Default 5 minutes.
    pub oauthbearer_jwks_refresh_interval: Time,

    /// Optional PEM path for outbound HTTPS to the `IdP`. JWKS,
    /// introspection, and userinfo all share it. `None` selects reqwest's
    /// default webpki-roots.
    pub oauthbearer_idp_tls_trust: Option<std::path::PathBuf>,

    /// Optional ceiling on OAUTHBEARER session lifetime. When set, the
    /// broker reports `session_lifetime_ms = min(token_exp_ms - now_ms,
    /// cap)` and the dispatch-loop re-auth timer fires at the clamped
    /// time. When unset, sessions last until the token's natural `exp`
    /// (the default).
    pub oauthbearer_max_session_lifetime: Option<Time>,

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
    pub oauthbearer_jwks_signal_rx:
        std::sync::Arc<std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<()>>>>,

    /// Shared timestamp of the last successful JWKS fetch.
    ///
    /// `apply_to` creates it as `AtomicI64::new(0)`. The validator's
    /// `JwksHandle` and the refresher both clone this `Arc`, so the
    /// validator's expiry check sees the refresher's writes.
    pub oauthbearer_jwks_last_successful_fetch_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,

    /// Shared on-demand-refresh timestamp for rate-limiting.
    ///
    /// `apply_to` creates it, and `Broker::start` gives a clone to the
    /// refresher. The validator never reads it. It is refresher-only
    /// bookkeeping that `BrokerConfig` carries for symmetry.
    pub oauthbearer_jwks_last_on_demand_refresh_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,

    /// Minimum pause between on-demand JWKS refreshes that validator signals
    /// trigger. `apply_to` sets it from
    /// `FileOAuthBearerConfig::jwks_min_refresh_pause_seconds`, and
    /// `Broker::start` passes it into `JwksRefresher`. Strimzi's default is
    /// 1 second, and this default is 1 second too.
    pub oauthbearer_jwks_min_on_demand_pause: Time,

    /// Independent compatibility and protocol feature gates.
    pub features: BrokerFeatureFlags,

    /// KIP-98 / KIP-939: how often the idle-transaction reaper scans for
    /// `Ongoing` transactions whose timeout has elapsed and aborts them. The
    /// reaper never reaps 2PC transactions. Mirrors Kafka's
    /// `transaction.abort.timed.out.transaction.cleanup.interval.ms` (10s).
    /// A zero interval disables the reaper entirely and spawns no background
    /// task. Zero is the default in `for_tests`, so a background abort does
    /// not disturb unit and integration tests. Tests that exercise the reaper
    /// set this value low explicitly.
    pub txn_abort_cleanup_interval: Time,

    /// KIP-848 next-gen consumer group protocol configuration. It controls
    /// which rebalance protocols the broker advertises, the session and
    /// heartbeat timeout bounds, and the set of enabled server-side
    /// assignors.
    pub next_gen_consumer_group: Box<crate::coordinator::unified::config::NextGenConfig>,

    /// KIP-932 share-group configuration.
    pub share_group: Box<crate::coordinator::unified::share::config::ShareGroupConfig>,

    /// KIP-1071 streams-group (Streams rebalance protocol) configuration.
    pub streams_group: Box<crate::coordinator::unified::streams::config::StreamsGroupConfig>,

    /// KIP-932 share-coordinator (persister) configuration. It controls the
    /// `__share_group_state` internal topic geometry and snapshot folding.
    pub share_coordinator: Box<crate::share_coordinator::config::ShareCoordinatorConfig>,

    /// How often the auto-rebalance ticker fires. Default 5 minutes.
    /// Matches Kafka's `leader.imbalance.check.interval.seconds`.
    pub leader_imbalance_check_interval: Time,

    /// Minimum fraction of imbalanced partitions before the
    /// auto-rebalance ticker submits any changes. Default 10%. Matches
    /// Kafka's `leader.imbalance.per.broker.percentage`.
    pub leader_imbalance_per_broker: Ratio,

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
    pub tls_reload_interval: Time,

    /// Bind address for the Prometheus `/metrics` HTTP endpoint. `None`
    /// disables the server entirely. The broker still updates its internal
    /// counters, but nothing scrapes them. The default is
    /// `Some(0.0.0.0:9404)` in production, the same port the JMX exporter
    /// uses for vanilla Kafka. The default is `None` in `for_tests`, so unit
    /// tests do not compete for port allocation.
    pub metrics_listen_addr: Option<SocketAddr>,

    /// CPU and heap profiling endpoint policy.
    pub profiling: krabka_telemetry::profiling::ProfilingConfig,

    /// Optional OTLP endpoint for KIP-714 client metrics forwarding.
    /// Binaries populate it from their parsed runtime configuration. The
    /// broker does not read it from the environment at startup.
    pub client_metrics_otlp_endpoint: Option<String>,
    /// Transport used by the KIP-714 client-metrics forwarder.
    pub client_metrics_otlp_protocol: krabka_telemetry::OtlpProtocol,

    /// KIP-227: maximum number of incremental-fetch sessions kept in the
    /// per-broker cache. Each session tracks the (topic, partition) set a
    /// client is subscribed to, so later fetches can be deltas. When the
    /// cache is full, the broker evicts a non-privileged (consumer) session
    /// in LRU order. Only another privileged session evicts a privileged
    /// (follower-fetch) session. Matches Apache Kafka's
    /// `max.incremental.fetch.session.cache.slots` (default 1000).
    pub max_incremental_fetch_session_cache_slots: usize,

    /// Maximum number of live broker connections across all listeners. The
    /// broker immediately closes any new connection it accepts past this
    /// ceiling; Kafka silently drops them. Matches Apache Kafka's
    /// `max.connections`. The default is `usize::MAX`, which is unlimited
    /// and mirrors Kafka's `Integer.MAX_VALUE`.
    pub max_connections: usize,

    /// Maximum number of live connections from any single client IP. The
    /// broker immediately closes connections past this per-IP ceiling.
    /// Matches Apache Kafka's `max.connections.per.ip`. The default is
    /// `usize::MAX`, which is unlimited.
    pub max_connections_per_ip: usize,

    /// Partition disk-usage scan cadence. A zero interval disables the
    /// scanner entirely and spawns no background task. Production default:
    /// 60s. On each tick the scanner walks every known (topic, partition)
    /// under `log_dir`, sums the regular-file sizes, and updates the
    /// `partition_disk_bytes` gauge that the rebalancer's usage scraper
    /// reads.
    pub partition_disk_scan_interval: Time,

    /// KIP-48: HMAC-SHA-256 master key that mints and verifies delegation
    /// tokens. When `None`, the broker rejects all four delegation-token RPCs
    /// with `DELEGATION_TOKEN_AUTH_DISABLED`, and SCRAM cannot fall back to
    /// token lookup. The broker reads the key from
    /// `KRABKA_DELEGATION_TOKEN_SECRET_KEY` or from `[delegation_token]
    /// secret_key` in `broker.toml`; the environment variable wins. The key
    /// is wrapped in `SecretBytes`, so `Debug` redacts the bytes.
    pub delegation_token_secret_key: Option<krabka_security::SecretBytes>,

    /// KIP-48: hard upper bound on delegation-token lifetime.
    /// A token's `max_timestamp_ms` is set to
    /// `issue_timestamp_ms + delegation_token_max_lifetime` and the
    /// renew handler clamps any caller-requested expiry to this. Default
    /// 7 days (`delegation.token.max.lifetime.ms` in Kafka).
    pub delegation_token_max_lifetime: Time,

    /// KIP-48: cadence of the background sweep task that
    /// proposes `V1DeleteDelegationToken` tombstones for tokens whose
    /// `expiry_timestamp_ms` or `max_timestamp_ms` is in the past. Default
    /// 1 hour (`delegation.token.expiry.check.interval.ms` in Kafka).
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
    pub delegation_token_default_renew_period: Time,

    /// KIP-405: tiered-storage backend selection. `Some(_)` enables tiered
    /// storage broker-wide and spawns the `RemoteLogManager` copy task. This
    /// one field replaces Kafka's `remote.log.storage.system.enable` plus the
    /// RSM selection. `remote.storage.enable` still gates per-topic offload.
    /// `None` (default) leaves tiered storage off.
    ///
    /// TOML:
    /// - Local: `[remote_storage] storage_dir = "..."`
    /// - S3:    `[remote_storage.s3] bucket = "..." region = "..."`
    pub remote_storage_backend: Option<RemoteStorageBackend>,

    /// KIP-405: tick cadence of the `RemoteLogManager` copy /
    /// retention task. Defaults to 30s (Kafka's
    /// `remote.log.manager.task.interval.ms`). Acceptance tests lower this
    /// so segments are tiered and locally evicted in seconds rather than
    /// minutes; production deployments leave it at the default.
    pub remote_log_manager_interval: Time,

    /// KIP-405: which RLMM the broker runs when tiered storage is enabled.
    /// It defaults to [`RlmmKind::TopicBacked`] in production, and to
    /// [`RlmmKind::InMemory`] for in-process tests. The broker ignores it
    /// when `remote_storage_backend` is `None`.
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
    pub remote_storage_worm: Option<krabka_remote_storage::WormConfig>,

    /// Whether the audit subsystem is active (`FedRAMP` MLA).
    pub audit_enabled: bool,
    /// Internal topic name for audit records.
    pub audit_topic: String,
    /// Path to the PKCS#8 Ed25519 audit checkpoint signing key. `None` means
    /// no checkpoints.
    pub audit_signing_key_path: Option<std::path::PathBuf>,
    /// Key id recorded on checkpoints (for rotation).
    pub audit_signing_key_id: Option<String>,
    /// Emit a checkpoint after this many audit records.
    pub audit_checkpoint_every_n: u64,
    /// Emit a checkpoint at least this often.
    pub audit_checkpoint_every: Time,
    /// Directory for the durable audit spool. A relative path resolves under
    /// the broker's log dir.
    pub audit_spool_dir: std::path::PathBuf,
    /// Cap on the audit spool size.
    pub audit_spool_max: ByteSize,
}
