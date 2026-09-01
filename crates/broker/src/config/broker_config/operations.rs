//! The operational surface of a running broker: the preferred-leader
//! rebalance ticker, the log-cleaner and TLS-reload timers, the Prometheus,
//! profiling, and OTLP endpoints, the ceilings on connections and cached
//! fetch sessions, and the partition disk-usage scan that feeds the
//! rebalancer.

// Link 6 of the `BrokerConfig` field chain: it adds this group to the
// fields collected so far and hands them to `delegation_tokens_fields`.
macro_rules! operations_fields {
    ($($collected:tt)*) => {
        delegation_tokens_fields! {
            $($collected)*
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
            pub log_levels: krabka_telemetry::LogLevelController,

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
        }
    };
}
