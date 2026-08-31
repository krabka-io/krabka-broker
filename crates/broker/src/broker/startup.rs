//! The public [`Broker::start`] entry points and the sequence they drive. This
//! module orders the startup phases -- transport, metadata quorum, storage
//! recovery, coordinators, runtime services, and final assembly -- and holds
//! the commentary that explains why that order is the one that works.

use std::sync::Arc;

use futures_util::future::BoxFuture;
use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};

use crate::{
    broker::{
        Broker, BrokerHandle, DisklessRuntime,
        coordinators::{CoordinatorStartup, start_coordinators},
        finish::{BrokerStorageStartup, finish_broker_startup},
        metadata_phase::start_metadata_phase,
        runtime::start_broker_runtime,
        storage::{StorageStartup, recover_storage_and_groups},
        transport::{StartupTransport, prepare_startup_transport},
    },
    config::BrokerConfig,
    error::BrokerError,
};

impl Broker {
    /// Build a `Broker`, scan the log dir, spawn partition writers for
    /// every existing `<topic>-<partition>/`, bind the TCP listener, and
    /// return the handle.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn start(config: BrokerConfig) -> Result<BrokerHandle, BrokerError> {
        Self::start_with_listeners(config, None, None).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr`.
    ///
    /// Thin wrapper over [`Self::start_with_listeners`] for callers that only
    /// hand off the controller port. The data plane still binds from `config`.
    /// See that method for the full handoff contract.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn start_with_controller_listener(
        config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
    ) -> Result<BrokerHandle, BrokerError> {
        Self::start_with_listeners(config, controller_listener, None).await
    }

    /// Like [`Self::start`], but adopts caller-supplied, already-bound
    /// listeners instead of binding their addresses itself:
    ///
    /// * `controller_listener`: threaded through to
    ///   [`krabka_raft::Controller::start_with_listener`]. Its local address
    ///   MUST equal `config.controller_listen_addr`.
    /// * `data_plane_listeners`: each listener is adopted for the data-plane
    ///   [`ListenerSpec`] whose `bind_addr` equals its local address (for the
    ///   legacy single-listener path that is `config.listen_addr`). Any
    ///   non-matching specs still bind from `config`.
    ///
    /// A live socket handoff closes the TOCTOU window that the bind-and-drop
    /// trick leaves open. That trick reads an ephemeral port and then drops the
    /// probe before it re-binds. In the window between the two steps, another
    /// process can claim the just-released port, which is the `AddrInUse` flake
    /// that test harnesses hit under parallel execution. The data-plane port
    /// must still be concrete in `config` up front, because the broker
    /// self-registers `listen_addr.port()` before it binds the data plane.
    /// Callers therefore read the port back from the live listener's
    /// `local_addr()` and set `config.listen_addr` and
    /// `advertised_listener` to it before the call.
    ///
    /// [`ListenerSpec`]: crate::config::ListenerSpec
    // sequential bring-up; splitting hurts readability more than it helps
    // cargo-mutants: network/socket bring-up, not unit-testable
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn start_with_listeners(
        config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
        data_plane_listeners: impl IntoIterator<Item = tokio::net::TcpListener>,
    ) -> Result<BrokerHandle, BrokerError> {
        Self::start_with_listeners_boxed(
            config,
            controller_listener,
            data_plane_listeners.into_iter().collect(),
        )
        .await
    }

    fn start_with_listeners_boxed(
        config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
        data_plane_listeners: Vec<tokio::net::TcpListener>,
    ) -> BoxFuture<'static, Result<BrokerHandle, BrokerError>> {
        Box::pin(Self::start_with_listeners_inner(
            config,
            controller_listener,
            data_plane_listeners,
        ))
    }

    async fn start_with_listeners_inner(
        mut config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
        data_plane_listeners: Vec<tokio::net::TcpListener>,
    ) -> Result<BrokerHandle, BrokerError> {
        let StartupTransport {
            tls_dynamic,
            ktls_enabled,
            inter_broker_client,
        } = prepare_startup_transport(&config).await?;
        let metrics = crate::metrics::BrokerMetrics::new();
        let diskless_runtime = DisklessRuntime::new(
            config.node_id,
            config.inter_broker_principal_node_ids.clone(),
            config
                .diskless_wal_hot_tail_max_size
                .bytes_u64()
                .try_into()
                .unwrap_or(usize::MAX),
            metrics.clone(),
        );

        // 1. Bring up the metadata quorum BEFORE the client listener so
        //    handlers can read from it the moment they accept their first
        //    connection. The controller owns its own listener bound to
        //    `controller_listen_addr`.
        //
        //    Raft dialer + handshake wiring:
        //
        //    Replication + heartbeat dials route through the data-plane
        //    inter-broker listener (which speaks SASL/TLS when
        //    configured). The Raft RPCs (`AppendEntries`, `Vote`,
        //    `SubmitChange`) dial the *controller* listener which now
        //    shares the same SASL/TLS handshake path via the
        //    `InterBrokerDialer` adapter on the outbound side and
        //    `BrokerRaftHandshake` on the inbound side. With the default
        //    `controller_listener_protocol = Plaintext`, the dialer's
        //    `dial` impl reduces to a `TcpStream::connect` and the
        //    handshake is `None`, so the legacy raw-TCP raft path is
        //    byte-identical for existing deployments and tests.
        //
        //    `BrokerRaftHandshake` needs a `ControllerHandle` to satisfy
        //    SCRAM credential lookups, but the handle isn't built until
        //    `Controller::start` returns. We bridge that with an
        //    `Arc<OnceCell<Arc<ControllerHandle>>>` that's installed into
        //    the handshake up front and `set` once the controller exists.

        // KIP-853: the bootstrap records carry the seed `VotersRecord`. Load
        // them once here so the cold-boot voter set feeds `ControllerConfig`;
        // the same records are submitted through raft after a leader is
        // elected (step 2b below). A `Join` node has no seed set and relies
        // on `bootstrap_servers` + auto-join instead. Broker-only nodes never
        // run a controller, so the records stay unused (step 2b is gated on
        // having a non-empty set and `Bootstrap` mode).
        let (controller, controller_admin_router) = start_metadata_phase(
            &mut config,
            controller_listener,
            tls_dynamic.as_ref(),
            &inter_broker_client,
            Arc::clone(&diskless_runtime.wal_shards),
        )
        .await?;

        // 1b. KIP-853 controller auto-join. Spawned BEFORE the leader-wait in
        //     step 2: a `Join` broker's empty raft log keeps it in openraft's
        //     Learner state with no leader, so `Broker::start` would block in
        //     step 2 forever. The auto-join loop concurrently sends
        //     `AddRaftVoter(self)` to a `bootstrap_servers` entry; the leader's
        //     handler runs `add_learner` (replicating the log to us) and
        //     promotes us — at which point step 2's `watch_leader` fires and
        //     start proceeds. `run` returns immediately when `auto_join` is
        //     disabled (bootstrap / standalone brokers), so this is a cheap
        //     no-op there. The loop advertises the controller's REAL bound
        //     address, known now that `Controller::start` has bound the
        //     listener.
        // The joiner sends `AddRaftVoter` to a bootstrap server's *client*
        // data-plane listener (where api_key 80 is served), so it speaks the
        // inter-broker listener protocol — not the controller-listener
        // protocol that openraft RPCs use.
        // Auto-join grows the controller *voter* quorum, so only nodes that
        // run a controller participate. A broker-only node is a pure observer
        // and never joins the quorum.
        // Auto-join, leader readiness, registration, and bootstrap submission
        // are completed by `start_metadata_phase`.

        let StorageStartup {
            log_dir_status,
            log_dir_ids,
            partitions,
            producer_state,
            group_coordinator,
            producer_ids,
        } = recover_storage_and_groups(&config, &controller, &diskless_runtime).await?;

        // The barrier coordinator reports through the process registry, and it
        // is built here rather than inside the runtime because the coordinators
        // start first. BrokerMetrics clones cheaply.
        let CoordinatorStartup {
            txn_coordinator,
            barrier_coordinator,
            share_coordinator,
            share_partition_leaders,
            share_persister,
        } = start_coordinators(
            &config,
            &controller,
            &partitions,
            &group_coordinator,
            &producer_ids,
            &inter_broker_client,
            &metrics,
        )
        .await;

        // 4b. Spawn the replicator supervisor. Started AFTER the controller
        //    is up and self-registration succeeded so the supervisor's
        //    initial reconcile already sees this broker in the brokers()
        //    set. With replication_factor=1 the desired follower set is
        //    always empty, so this is a no-op for single-broker setups.
        let runtime = start_broker_runtime(
            &mut config,
            &controller,
            &inter_broker_client,
            tls_dynamic.as_ref(),
            (&partitions, &producer_state, &log_dir_status, &log_dir_ids),
            (&txn_coordinator, &share_coordinator),
            (&diskless_runtime, metrics),
        )
        .await?;

        crate::coordinator::leadership::spawn(
            config.node_id,
            Arc::clone(&controller),
            Arc::clone(&partitions),
            Arc::clone(&group_coordinator),
            runtime.supervisor_shutdown.child_token(),
        );

        // KIP-211. Every broker sweeps the groups whose offsets partition it
        // leads, and the tombstones are idempotent, so the sweep needs no
        // config gate of its own.
        crate::coordinator::retention::spawn(
            config.node_id,
            Arc::clone(&controller),
            Arc::clone(&group_coordinator),
            config.offsets_retention_check_interval,
            config.offsets_retention,
            runtime.supervisor_shutdown.child_token(),
        );

        tokio::spawn(crate::barrier::scheduler::run(
            Arc::clone(&barrier_coordinator),
            Arc::clone(&controller),
            runtime.supervisor_shutdown.child_token(),
        ));

        // KFC-1 deliver-at-time visibility. The task is a liveness aid only: it
        // wakes a consumer parked at the delivery watermark when the next batch
        // comes due. A fetch recomputes that watermark under the log mutex, so
        // this task can die without making a single fetch wrong.
        // The bound is a constant of this process, so it is published once
        // here rather than re-set on every scheduler pass. An alert reads it to
        // compare measured clock uncertainty against the bound the broker
        // actually relies on.
        runtime
            .metrics
            .delivery_clock_uncertainty_seconds
            .set(config.log_config.delivery_clock_uncertainty.secs_f64());

        tokio::spawn(crate::delivery::scheduler::run(
            Arc::clone(&partitions),
            config.node_id,
            crate::delivery::config::DeliveryConfig::default(),
            Arc::new(crate::delivery::metrics::BrokerDeliveryMetrics::new(
                runtime.metrics.clone(),
            )),
            Arc::new(crate::delivery::DeliveryWaker::new()),
            runtime.supervisor_shutdown.child_token(),
        ));

        crate::share_partition::backlog_poller::BacklogPoller {
            node_id: config.node_id,
            coordinator: Arc::clone(&group_coordinator),
            metadata: Arc::clone(&controller),
            partitions: Arc::clone(&partitions),
            persister: share_persister,
            inter_broker: Arc::clone(&inter_broker_client),
            listener_protocol: runtime.inter_listener_protocol,
            listener_name: config.inter_broker_listener_name.clone(),
            period: config.share_group.backlog_poll_interval,
            metrics: runtime.metrics.clone(),
            shutdown: runtime.supervisor_shutdown.child_token(),
        }
        .spawn();

        finish_broker_startup(
            config,
            data_plane_listeners,
            (controller, partitions, controller_admin_router),
            (
                group_coordinator,
                producer_ids,
                producer_state,
                txn_coordinator,
                share_coordinator,
                share_partition_leaders,
                barrier_coordinator,
            ),
            (tls_dynamic, ktls_enabled, inter_broker_client),
            runtime,
            BrokerStorageStartup {
                log_dir_status,
                diskless: diskless_runtime,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn public_startup_futures_are_unboxed() {
        let boxed_size =
            std::mem::size_of::<BoxFuture<'static, Result<BrokerHandle, BrokerError>>>();
        let start = Broker::start(BrokerConfig::for_tests(std::path::PathBuf::new()));
        let start_with_controller_listener = Broker::start_with_controller_listener(
            BrokerConfig::for_tests(std::path::PathBuf::new()),
            None,
        );
        let start_with_listeners = Broker::start_with_listeners(
            BrokerConfig::for_tests(std::path::PathBuf::new()),
            None,
            None,
        );

        assert!(std::mem::size_of_val(&start) > boxed_size);
        assert!(std::mem::size_of_val(&start_with_controller_listener) > boxed_size);
        assert!(std::mem::size_of_val(&start_with_listeners) > boxed_size);
    }

    #[tokio::test]
    async fn start_recovers_existing_partition_dirs() {
        let dir = tempdir().unwrap();
        // Create a partition dir with a log inside.
        let part_dir = dir.path().join("foo-0");
        std::fs::create_dir(&part_dir).unwrap();
        {
            let _log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).unwrap();
        }

        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.unwrap();
        // We can't easily inspect the partition registry from outside the
        // crate yet, but starting cleanly is the assertion we need here.
        handle.shutdown().await;
    }
}
