//! The broker binary's command line, as the clap parser the process entry
//! point builds its configuration from.

use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;
use krabka_units::{ByteSize, Ratio, Time};

use crate::runtime_args::RuntimeArgs;

#[derive(Debug, Parser)]
#[command(
    name = "krabka-broker",
    version,
    about = "Cluster-capable Apache Kafka-compatible krabka broker"
)]
pub struct Args {
    #[command(flatten)]
    pub runtime: RuntimeArgs,

    #[command(flatten)]
    pub profiling: krabka_telemetry::profiling::ProfilingConfig,

    /// TCP address to listen on. Mutually exclusive with `--config-file`.
    #[arg(long, default_value = "127.0.0.1:9092", conflicts_with = "config_file")]
    pub listen_addr: SocketAddr,

    /// `host:port` to advertise to clients. Default: `listen_addr`.
    /// The operator sets it with the env var `KRABKA_ADVERTISED_LISTENER`.
    /// Mutually exclusive with `--config-file`.
    #[arg(
        long,
        env = "KRABKA_ADVERTISED_LISTENER",
        conflicts_with = "config_file"
    )]
    pub advertised_listener: Option<String>,

    /// Path to an operator-managed TOML config file. When it is set,
    /// `--listen-addr` and `--advertised-listener` must NOT be set. The
    /// listener configuration then comes from the file's `[[listeners]]`
    /// table. See `krabka_broker::file_config::FileConfig`.
    #[arg(long)]
    pub config_file: Option<PathBuf>,

    /// Primary log directory. Holds the cluster-metadata raft log and is
    /// the default partition data directory.
    #[arg(long, default_value = "./krabka-data")]
    pub log_dir: PathBuf,

    /// More JBOD data directories (KIP-113), comma-separated. Least-loaded
    /// placement spreads new partitions across `--log-dir` and these
    /// directories. The cluster-metadata log always stays on `--log-dir`.
    /// This maps to a Kafka `log.dirs` with more than one entry.
    #[arg(
        long,
        env = "KRABKA_EXTRA_LOG_DIRS",
        value_delimiter = ',',
        num_args = 0..
    )]
    pub extra_log_dirs: Vec<PathBuf>,

    /// Numeric broker id.
    #[arg(long, default_value_t = 1)]
    pub broker_id: i32,

    /// `KRaft` `process.roles`, comma-separated (`controller`, `broker`,
    /// `witness`). Default: the combined set. `witness` is a modifier that
    /// comes with the other two roles. The operator normally sets this in
    /// the `[process]` section of `--config-file` instead.
    #[arg(
        long,
        env = "KRABKA_PROCESS_ROLES",
        value_delimiter = ',',
        num_args = 0..
    )]
    pub process_roles: Vec<String>,

    /// Cluster UUID. Every broker in the same cluster must share this
    /// value. The operator sets it with the env var `KRABKA_CLUSTER_ID`,
    /// which holds the `KafkaCluster` UID.
    #[arg(long, env = "KRABKA_CLUSTER_ID")]
    pub cluster_id: Option<uuid::Uuid>,

    /// Bind address for the Prometheus `/metrics` HTTP endpoint.
    /// An empty string or `none` disables it. Default: `0.0.0.0:9404`.
    /// That is the same port `jmx_prometheus_javaagent` uses for vanilla
    /// Kafka, so existing scrape configs apply unchanged.
    #[arg(
        long,
        env = "KRABKA_METRICS_LISTEN_ADDR",
        default_value = "0.0.0.0:9404"
    )]
    pub metrics_listen_addr: String,

    /// Bind address for the `/healthz` and `/readyz` HTTP probes.
    /// An empty string or `none` disables them. Default: `0.0.0.0:9405`,
    /// one past the metrics port. The reference Kubernetes manifests under
    /// `packaging/k8s/` point both probes at it.
    #[arg(
        long,
        env = "KRABKA_HEALTH_LISTEN_ADDR",
        default_value = "0.0.0.0:9405"
    )]
    pub health_listen_addr: String,

    /// How many `__cluster_metadata` records this node may trail the quorum's
    /// committed offset by and still answer `/readyz` with 200.
    #[arg(long, env = "KRABKA_READINESS_MAX_METADATA_LAG")]
    pub readiness_max_metadata_lag: Option<u64>,

    /// Partition disk-usage scan cadence. `0s` disables the scanner entirely.
    /// The scanner populates the `partition_disk_bytes` gauge, and the
    /// rebalancer's usage scraper reads that gauge.
    #[arg(long, env = "KRABKA_PARTITION_DISK_SCAN_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub partition_disk_scan_interval: Option<Time>,

    /// KIP-853: controller endpoints to discover the quorum leader at cold
    /// start, comma-separated `host:port`. Joiner nodes use them, that is,
    /// nodes formatted without `--standalone` or `--initial-controllers`.
    /// This maps to Kafka's `controller.quorum.bootstrap.servers`.
    #[arg(
        long,
        env = "KRABKA_CONTROLLER_BOOTSTRAP_SERVERS",
        value_delimiter = ',',
        num_args = 0..
    )]
    pub controller_bootstrap_servers: Vec<SocketAddr>,

    /// KIP-853: auto-join the quorum as a voter after the node catches up as
    /// an observer. This maps to Kafka's
    /// `controller.quorum.auto.join.enable`.
    #[arg(long, env = "KRABKA_CONTROLLER_AUTO_JOIN")]
    pub controller_auto_join: bool,

    /// KIP-853 observer promotion lag bound.
    #[arg(long, env = "KRABKA_OBSERVER_LAG_BOUND")]
    pub observer_lag_bound: Option<u64>,

    /// Broker heartbeat interval in milliseconds.
    #[arg(
        long,
        env = "KRABKA_HEARTBEAT_INTERVAL",
        value_parser = krabka_units::parse::positive_time
    )]
    pub heartbeat_interval: Option<Time>,

    /// Broker heartbeat timeout in milliseconds.
    #[arg(
        long,
        env = "KRABKA_HEARTBEAT_TIMEOUT",
        value_parser = krabka_units::parse::positive_time
    )]
    pub heartbeat_timeout: Option<Time>,

    /// Follower lag timeout in milliseconds before ISR shrink.
    #[arg(
        long,
        env = "KRABKA_REPLICA_LAG_TIME_MAX",
        value_parser = krabka_units::parse::positive_time
    )]
    pub replica_lag_time_max: Option<Time>,

    /// Controller election timeout in milliseconds.
    #[arg(
        long,
        env = "KRABKA_CONTROLLER_ELECTION_TIMEOUT",
        value_parser = krabka_units::parse::positive_time
    )]
    pub controller_election_timeout: Option<Time>,

    /// Controller heartbeat interval in milliseconds.
    #[arg(
        long,
        env = "KRABKA_CONTROLLER_HEARTBEAT_INTERVAL",
        value_parser = krabka_units::parse::positive_time
    )]
    pub controller_heartbeat_interval: Option<Time>,

    /// Consecutive controller fetch misses tolerated before election.
    #[arg(
        long,
        env = "KRABKA_CONTROLLER_FETCH_MISS_LIMIT",
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub controller_fetch_miss_limit: Option<u32>,

    /// Capacity of the metadata Raft command queue.
    #[arg(
        long,
        env = "KRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY",
        value_parser = parse_metadata_raft_command_queue_capacity
    )]
    pub metadata_raft_command_queue_capacity: Option<usize>,

    /// Per-read and per-snapshot-request metadata Raft byte budget.
    #[arg(
        long,
        env = "KRABKA_METADATA_RAFT_FETCH_MAX",
        value_parser = krabka_units::parse::positive_byte_size
    )]
    pub metadata_raft_fetch_max: Option<ByteSize>,

    /// Controlled-shutdown leadership drain timeout in milliseconds.
    #[arg(
        long,
        env = "KRABKA_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT",
        value_parser = krabka_units::parse::positive_time
    )]
    pub controlled_shutdown_drain_timeout: Option<Time>,

    /// Maximum bytes between metadata-log snapshots.
    #[arg(
        long,
        env = "KRABKA_METADATA_MAX_BETWEEN_SNAPSHOTS",
        value_parser = krabka_units::parse::positive_byte_size
    )]
    pub metadata_max_between_snapshots: Option<ByteSize>,

    /// Maximum time between metadata-log snapshots. `0s` disables the interval cap.
    #[arg(long, env = "KRABKA_METADATA_MAX_SNAPSHOT_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub metadata_max_snapshot_interval: Option<Time>,

    /// Committed-record gap between metadata-log snapshots.
    #[arg(
        long,
        env = "KRABKA_METADATA_SNAPSHOT_INTERVAL_RECORDS",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub metadata_snapshot_interval_records: Option<u64>,

    /// Maximum metadata snapshot size a follower will fetch.
    #[arg(
        long,
        env = "KRABKA_METADATA_SNAPSHOT_FETCH_MAX",
        value_parser = krabka_units::parse::positive_byte_size
    )]
    pub metadata_snapshot_fetch_max: Option<ByteSize>,

    /// Idle-transaction abort cleanup interval. `0s` disables the reaper.
    #[arg(long, env = "KRABKA_TXN_ABORT_CLEANUP_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub txn_abort_cleanup_interval: Option<Time>,

    /// Transactional-id expiry (`transactional.id.expiration.ms`).
    #[arg(
        long,
        env = "KRABKA_TXN_ID_EXPIRATION",
        value_parser = krabka_units::parse::positive_time
    )]
    pub txn_id_expiration: Option<Time>,

    /// Transactional-id expiry sweep cadence
    /// (`transaction.remove.expired.transaction.cleanup.interval.ms`). `0s`
    /// disables the sweep.
    #[arg(long, env = "KRABKA_TXN_ID_EXPIRATION_CLEANUP_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub txn_id_expiration_cleanup_interval: Option<Time>,

    /// Auto preferred-replica election scan cadence.
    #[arg(
        long,
        env = "KRABKA_LEADER_IMBALANCE_CHECK_INTERVAL",
        value_parser = krabka_units::parse::positive_time
    )]
    pub leader_imbalance_check_interval: Option<Time>,

    /// Minimum per-broker leader imbalance percentage before auto-rebalance acts.
    #[arg(
        long,
        env = "KRABKA_LEADER_IMBALANCE_PER_BROKER",
        value_parser = krabka_units::parse::ratio
    )]
    pub leader_imbalance_per_broker: Option<Ratio>,

    /// TLS cert/key reload polling interval. `0s` disables the watcher.
    #[arg(long, env = "KRABKA_TLS_RELOAD_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub tls_reload_interval: Option<Time>,

    /// Maximum incremental fetch-session cache slots.
    #[arg(long, env = "KRABKA_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS")]
    pub max_incremental_fetch_session_cache_slots: Option<usize>,

    /// Maximum live broker connections across all listeners.
    #[arg(long, env = "KRABKA_MAX_CONNECTIONS")]
    pub max_connections: Option<usize>,

    /// Maximum live broker connections from any single client IP.
    #[arg(long, env = "KRABKA_MAX_CONNECTIONS_PER_IP")]
    pub max_connections_per_ip: Option<usize>,

    /// Delegation-token maximum lifetime.
    #[arg(
        long,
        env = "KRABKA_DELEGATION_TOKEN_MAX_LIFETIME",
        value_parser = krabka_units::parse::positive_time
    )]
    pub delegation_token_max_lifetime: Option<Time>,

    /// Delegation-token expiry sweep interval.
    #[arg(
        long,
        env = "KRABKA_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL",
        value_parser = krabka_units::parse::positive_time
    )]
    pub delegation_token_expiry_check_interval: Option<Time>,

    /// Delegation-token default renew period.
    #[arg(
        long,
        env = "KRABKA_DELEGATION_TOKEN_RENEW_PERIOD",
        value_parser = krabka_units::parse::positive_time
    )]
    pub delegation_token_default_renew_period: Option<Time>,

    /// `RemoteLogManager` copy/retention cadence in milliseconds.
    #[arg(
        long,
        env = "KRABKA_REMOTE_LOG_MANAGER_INTERVAL",
        value_parser = krabka_units::parse::positive_time
    )]
    pub remote_log_manager_interval: Option<Time>,

    /// Delegation-token HMAC master key. Prefer secrets managers over shell history.
    #[arg(
        long,
        env = "KRABKA_DELEGATION_TOKEN_SECRET_KEY",
        hide_env_values = true
    )]
    pub delegation_token_secret_key: Option<String>,

    /// Disable OpenTelemetry SDK/exporters when truthy.
    #[arg(long, env = "OTEL_SDK_DISABLED")]
    pub otel_sdk_disabled: Option<String>,

    /// KRABKA-specific OTLP endpoint override.
    #[arg(long, env = "KRABKA_OTLP_ENDPOINT")]
    pub krabka_otlp_endpoint: Option<String>,

    /// OpenTelemetry traces endpoint override.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")]
    pub otel_exporter_otlp_traces_endpoint: Option<String>,

    /// OpenTelemetry endpoint override shared by signals.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otel_exporter_otlp_endpoint: Option<String>,

    /// Enable OTLP export without setting an endpoint.
    #[arg(long, env = "KRABKA_OTLP_ENABLED")]
    pub krabka_otlp_enabled: Option<String>,

    /// OTLP protocol (`grpc` or `http/protobuf`).
    #[arg(long, env = "KRABKA_OTLP_PROTOCOL")]
    pub krabka_otlp_protocol: Option<String>,

    /// OpenTelemetry exporter protocol (`grpc` or `http/protobuf`).
    #[arg(long, env = "OTEL_EXPORTER_OTLP_PROTOCOL")]
    pub otel_exporter_otlp_protocol: Option<String>,

    /// OTLP head sampling ratio in `[0.0, 1.0]`.
    #[arg(long, env = "KRABKA_OTLP_SAMPLE_RATIO")]
    pub krabka_otlp_sample_ratio: Option<String>,

    /// OpenTelemetry sampler argument used as the trace sample ratio.
    #[arg(long, env = "OTEL_TRACES_SAMPLER_ARG")]
    pub otel_traces_sampler_arg: Option<String>,

    /// OpenTelemetry service name.
    #[arg(long, env = "OTEL_SERVICE_NAME")]
    pub otel_service_name: Option<String>,

    /// KRABKA-specific OTLP timeout.
    #[arg(long, env = "KRABKA_OTLP_TIMEOUT", value_parser = krabka_units::parse::non_negative_time)]
    pub krabka_otlp_timeout: Option<Time>,

    /// OpenTelemetry exporter timeout in seconds.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_TIMEOUT_SECS")]
    pub otel_exporter_otlp_timeout_secs: Option<String>,

    /// OTLP heartbeat interval. `0s` disables heartbeats.
    #[arg(long, env = "KRABKA_OTLP_HEARTBEAT_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub krabka_otlp_heartbeat_interval: Option<Time>,
}

fn parse_metadata_raft_command_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    krabka_raft::MetadataRaftCommandQueueCapacity::new(value)
        .map(krabka_raft::MetadataRaftCommandQueueCapacity::get)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::secs;

    use super::*;
    use crate::test_support::env_guard;

    #[test]
    fn profiling_policy_reads_environment_and_cli_wins() {
        let _guard = env_guard();

        let defaults = Args::try_parse_from(["krabka-broker"]).expect("parse defaults");
        assert!(defaults.profiling == krabka_telemetry::profiling::ProfilingConfig::default());

        temp_env::with_vars(
            [
                ("KRABKA_PROFILING_CPU_DEFAULT_DURATION", Some("2s")),
                ("KRABKA_PROFILING_CPU_SAMPLE_FREQUENCY", Some("101Hz")),
            ],
            || {
                let args = Args::try_parse_from([
                    "krabka-broker",
                    "--profiling-cpu-default-duration=3s",
                    "--profiling-cpu-sample-frequency=103Hz",
                ])
                .expect("parse profiling overrides");
                assert!(args.profiling.profiling_cpu_default_duration == secs(3));
                assert!(
                    args.profiling.profiling_cpu_sample_frequency.frequency()
                        == krabka_units::per_sec(103)
                );
            },
        );
    }

    #[test]
    fn config_file_mutually_exclusive_with_listen_addr() {
        use clap::Parser;

        let _guard = env_guard();

        let res = Args::try_parse_from([
            "krabka-broker",
            "--config-file=/tmp/a.toml",
            "--listen-addr=127.0.0.1:9092",
        ]);
        let err = res.expect_err("expected mutual-exclusion error");
        let s = err.to_string();
        assert!(
            s.contains("config-file") && s.contains("listen-addr"),
            "expected clap conflict mentioning both flags, got: {s}"
        );
    }

    #[test]
    fn config_file_mutually_exclusive_with_advertised_listener() {
        use clap::Parser;

        let _guard = env_guard();

        let res = Args::try_parse_from([
            "krabka-broker",
            "--config-file=/tmp/a.toml",
            "--advertised-listener=h:9092",
        ]);
        let err = res.expect_err("expected mutual-exclusion error");
        let s = err.to_string();
        assert!(
            s.contains("config-file") && s.contains("advertised-listener"),
            "expected clap conflict, got: {s}"
        );
    }

    #[test]
    fn config_file_alone_parses() {
        use clap::Parser;

        let _guard = env_guard();

        let args = Args::try_parse_from(["krabka-broker", "--config-file=/tmp/a.toml"]).unwrap();
        assert!(args.config_file.as_deref() == Some(std::path::Path::new("/tmp/a.toml")));
        assert!(args.advertised_listener.is_none());
    }
}
