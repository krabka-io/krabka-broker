//! Top-level `Broker` lifecycle. The broker connects the partition registry,
//! metadata image, network listener, and handler table.
//!
//! This file is the module root and holds the shared state types: [`Broker`],
//! the lifecycle [`BrokerHandle`], and the diskless WAL runtime they carry.
//! The startup phases, the handle's methods, and the accept path live in the
//! child modules declared below.

use std::{
    net::SocketAddr,
    sync::{Arc, atomic::AtomicBool},
};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    config::BrokerConfig, handlers::DispatchRegistry, partition_registry::PartitionRegistry,
};

mod accept;
mod adapters;
mod audit;
mod connection_limiter;
mod coordinators;
mod diskless_index;
mod endpoints;
mod finish;
mod gauges;
mod handle;
mod listeners;
mod liveness;
mod maintenance;
mod metadata_phase;
mod observability;
mod partition_spawn;
mod registration;
mod remote_storage;
mod replication;
mod rlmm;
mod runtime;
mod startup;
mod storage;
mod transport;

#[cfg(test)]
mod test_support;

pub(crate) use self::{
    connection_limiter::ConnectionLimiter,
    partition_spawn::{
        PartitionSpawnConfig, spawn_partition, spawn_partition_with_replication_target,
        try_spawn_partition_with_replication_target, try_spawn_partition_with_sequencer,
    },
};

/// Timeout shared by the test-helper `wait_*` awaiters. If a condition
/// does not hold within this window, the test fails.
#[cfg(any(test, feature = "test-helpers"))]
const TEST_AWAITER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
#[cfg(any(test, feature = "test-helpers"))]
const DISKLESS_FLUSHER_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// The running broker. Library callers get a [`BrokerHandle`] from
/// [`Broker::start`]; this struct is the shared internal state.
pub struct Broker {
    pub(crate) config: BrokerConfig,
    /// Metadata authority for this broker. For combined/controller nodes
    /// this is a live openraft `ControllerHandle`; for broker-only nodes it
    /// is an observer-backed source that fetches `__cluster_metadata` and
    /// forwards writes to the controller quorum. Handlers reach it through
    /// the `MetadataSource` trait, so the concrete backing is invisible to
    /// them.
    pub(crate) controller: Arc<dyn crate::metadata_source::MetadataSource>,
    /// Wrapped in `Arc` so handlers that clone the field share the same
    /// underlying registry. Lookups take a borrowed `&str` topic, so the
    /// produce/fetch hot path resolves partitions with no per-lookup `String`
    /// allocation.
    pub(crate) partitions: Arc<PartitionRegistry>,
    /// KIP-113 (`AlterReplicaLogDirs`): in-progress intra-broker
    /// log-dir moves. There is one entry per `(topic, partition)` that
    /// the broker currently copies to a different log.dir.
    /// `DescribeLogDirs` reads this to show `is_future_key=true` rows.
    /// The `AlterReplicaLogDirs` handler reads it to make a second
    /// request for the same partition idempotent, or to reject a
    /// conflicting target.
    pub(crate) future_logs:
        Arc<DashMap<(String, PartitionIndex), Arc<crate::future_log::FutureLogState>>>,
    pub(crate) group_coordinator: Arc<crate::coordinator::GroupCoordinator>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    pub(crate) producer_state: Arc<crate::producer_state::ProducerState>,
    pub(crate) txn_coordinator: Arc<crate::txn::coordinator::TxnCoordinator>,
    pub(crate) share_coordinator: Arc<crate::share_coordinator::coordinator::ShareCoordinator>,
    pub(crate) barrier_coordinator: Arc<crate::barrier::coordinator::BarrierCoordinator>,
    pub(crate) share_partition_leaders:
        Arc<crate::share_partition::manager::SharePartitionLeaderManager>,
    pub(crate) supervisor_shutdown: tokio_util::sync::CancellationToken,
    pub(crate) supervisor_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    /// Handle for the periodic disk-usage scanner spawned when
    /// `BrokerConfig::partition_disk_scan_interval > 0`. Retained on
    /// the struct so [`BrokerHandle::shutdown`] can await it after
    /// cancelling `supervisor_shutdown`. `None` when the scanner is
    /// disabled (interval = 0, typical in tests).
    pub(crate) disk_scanner_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    pub(crate) liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    /// `Some` when `BrokerConfig::tls_config` is set. Per-listener
    /// accept loops snapshot the current `Arc<ServerConfig>` with
    /// `current()` and wrap it in a fresh `TlsAcceptor`. The TLS
    /// hot-reload path swaps the inner config without restart.
    pub(crate) tls_dynamic: Option<Arc<krabka_security::DynamicServerConfig>>,
    /// Linux kTLS (Increment F): `true` when the startup probe confirmed the
    /// kernel supports kTLS TX (kernel ≥ 4.13 + the `tls` module loadable) and
    /// rustls is configured to export secrets. Set ONCE at startup.
    /// `ktls::config_ktls_server` consumes the `TlsStream` by value, so a
    /// per-connection failure is unrecoverable. Routing through kTLS only when
    /// this is `true` keeps the per-connection path infallible-by-construction.
    /// When `false` (non-Linux, no `tls` module, or no TLS configured), TLS
    /// listeners serve the exact userspace rustls path (byte-identical wire).
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) ktls_enabled: bool,
    /// Shared outbound dialer used by the replicator, raft transport,
    /// and controller-heartbeat loops. When `inter_broker_credentials`
    /// is `None` and the listener is `PLAINTEXT` the dialer falls back
    /// to a plain `TcpStream::connect`. The new wiring is transparent
    /// for the legacy PLAINTEXT-only path.
    pub(crate) inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    /// KIP-966 offset-aware unclean recovery. Cloneable handle that
    /// enqueues recovery jobs onto the Unclean Recovery Manager task.
    /// Used by the `ElectLeaders UNCLEAN` handler (which awaits the
    /// outcome) and the automatic failover path (fire-and-forget).
    pub(crate) unclean_recovery: crate::unclean_recovery::UncleanRecoveryHandle,
    /// KIP-73 throttle buckets. Updated by the throttle refresh task and
    /// consulted by the Fetch handler and replicator.
    pub throttle_state: Arc<crate::throttle::ThrottleState>,
    /// KIP-13/KIP-124 quota buckets. Updated by the quota refresh task and
    /// consulted by the Produce/Fetch handlers and request-rate enforcement.
    pub quota_buckets: Arc<crate::quota::QuotaBuckets>,
    /// Live connection accounting for the `max.connections` /
    /// `max.connections.per.ip` caps. `accept_loop` consults these before it
    /// spawns a per-connection task. An RAII [`ConnectionGuard`]
    /// decrements them when the connection ends.
    pub(crate) connections: ConnectionLimiter,
    /// KIP-227 incremental-fetch-session cache. Consulted by the Fetch
    /// handler before each read; sized by
    /// `BrokerConfig::max_incremental_fetch_session_cache_slots`.
    pub fetch_session_cache: Arc<crate::fetch_session::FetchSessionCache>,
    /// Prometheus metrics. Cloned into every subsystem that emits
    /// metrics, such as the produce/fetch handlers and the
    /// isr-maintenance loop. The `BrokerMetrics` struct clones cheaply
    /// because it holds a single Arc.
    pub metrics: crate::metrics::BrokerMetrics,
    /// The actual `SocketAddr` that the `/metrics` HTTP server binds
    /// to. Populated only when `BrokerConfig::metrics_listen_addr`
    /// is `Some`. Tests that pass `127.0.0.1:0` read this field to find
    /// the OS-assigned port.
    pub(crate) metrics_bound_addr: Option<SocketAddr>,
    /// Controlled shutdown. Set to `true` by
    /// [`BrokerHandle::controlled_shutdown`]; the heartbeat client reads
    /// this every tick and stamps `want_shut_down=true` onto outbound
    /// `BrokerHeartbeat` requests.
    pub(crate) want_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    /// Controlled shutdown. Set to `true` by the heartbeat
    /// client when the controller responds `should_shut_down=true`;
    /// [`BrokerHandle::controlled_shutdown`] awaits this before it invokes
    /// the regular shutdown path.
    pub(crate) should_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    /// KIP-405: shared remote-storage + remote-log-metadata
    /// reader. `Some` when `BrokerConfig::remote_log_storage_dir` is set;
    /// the remote-log-manager copy task and the Fetch/ListOffsets
    /// handlers share the same instance through this handle.
    pub(crate) remote_reader: Option<Arc<crate::remote_reader::RemoteReader>>,
    /// Diskless WAL cold-read handle. `Some` once object-store + committed
    /// index-log wiring is active; fetch/list-offset handlers are fail-closed
    /// when this is absent.
    pub(crate) diskless_read: Option<Arc<crate::diskless::read::DisklessReadHandle>>,
    /// Advisory cache of quorum-committed diskless WAL tail batches.
    pub(crate) hot_tail: Arc<crate::diskless::hot_tail::HotTailCache>,
    /// Shard registry used by the controller listener's diskless WAL router.
    pub(crate) wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
    /// KIP-113 (offline-dir handling): per-log-dir online/offline status,
    /// built by a writability probe at `Broker::start` time. Handlers
    /// (today: `DescribeLogDirs`; future: produce/fetch) read this through
    /// [`crate::log_dir_status::LogDirRegistry::is_offline`] before they
    /// touch the dir.
    pub(crate) log_dir_status: crate::log_dir_status::LogDirRegistry,
    /// KIP-714 client-metrics receiver: subscription manager + Prometheus
    /// collector + OTLP forwarder. Shared so the push handler
    /// and the scrape path both touch the same instance.
    pub(crate) client_metrics: Arc<crate::client_metrics::ClientMetrics>,
    /// Test-only counter of served `OffsetForLeaderEpoch` (`api_key` 23)
    /// requests. Incremented once per decoded request by the handler.
    /// The KIP-320 proactive-validation integration test uses this to prove
    /// that the consumer's validate pass issued an OFLE RPC. The reactive
    /// in-band `diverging_epoch` and `OFFSET_OUT_OF_RANGE` fetch paths also
    /// detect truncation, but they issue no OFLE.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) offset_for_leader_epoch_requests: Arc<std::sync::atomic::AtomicU64>,
    /// `FedRAMP` MLA (Slice 1): cloneable handle to the audit pipeline.
    /// Handlers and lifecycle code call `emit` to record events; the
    /// `AuditWriter` background task drains them into the
    /// `KafkaTopicAuditSink`. Disabled (`AuditLog::disabled()`) when
    /// `BrokerConfig::audit_enabled` is `false`.
    pub(crate) audit_log: std::sync::Arc<krabka_audit::AuditLog>,
    pub(crate) audit_writer_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    handlers: DispatchRegistry,
}

struct DisklessRuntime {
    hot_tail: Arc<crate::diskless::hot_tail::HotTailCache>,
    wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
}

impl DisklessRuntime {
    fn new(node_id: krabka_raft::NodeId) -> Self {
        Self {
            hot_tail: Arc::new(crate::diskless::hot_tail::HotTailCache::default()),
            wal_shards: Arc::new(crate::wal::quorum::registry::WalShardRegistry::new(node_id)),
        }
    }
}

impl Broker {
    pub(crate) fn handlers(&self) -> &DispatchRegistry {
        &self.handlers
    }

    pub(crate) fn audit_product() -> krabka_audit::ProductInfo {
        krabka_audit::ProductInfo {
            vendor_name: "Krabka".to_string(),
            name: "krabka-broker".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Test-only: clone the controller handle so the `auto_join` unit test can
    /// build `AutoJoinParams` without access to private fields.
    #[cfg(test)]
    pub(crate) fn controller_for_test(&self) -> Arc<dyn crate::metadata_source::MetadataSource> {
        self.controller.clone()
    }

    /// Test-only: clone the shared inter-broker client (same reason).
    #[cfg(test)]
    pub(crate) fn inter_broker_client_for_test(
        &self,
    ) -> Arc<crate::network::client::InterBrokerClient> {
        self.inter_broker_client.clone()
    }
}

/// Lifecycle handle returned by [`Broker::start`]. Call
/// [`shutdown`](BrokerHandle::shutdown) for an orderly stop. Dropping the
/// handle requests best-effort cancellation of all retained tasks.
pub struct BrokerHandle {
    listen_addr: SocketAddr,
    shutdown: CancellationToken,
    /// One task per `ListenerSpec` bound during `Broker::start`. `shutdown()`
    /// awaits every task after it stops all active connections.
    listener_tasks: Vec<JoinHandle<()>>,
    /// Topic-backed RLMM bootstrap and assignment task. Retained so shutdown
    /// can join it before the Tokio runtime drops.
    topic_rlmm_task: Option<JoinHandle<()>>,
    /// Topic-backed diskless WAL index projection and object flusher task.
    /// Retained so shutdown can join it before the Tokio runtime drops.
    diskless_task: Option<JoinHandle<()>>,
    /// Instance-scoped readiness for this broker's index projection/flusher.
    #[cfg_attr(not(any(test, feature = "test-helpers")), allow(dead_code))]
    diskless_flusher_ready: Option<Arc<AtomicBool>>,
    /// Shared broker state, including the registries that own background task
    /// handles.
    broker: Arc<Broker>,
}
