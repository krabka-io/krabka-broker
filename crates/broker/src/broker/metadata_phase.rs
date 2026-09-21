//! The metadata-quorum startup phase: raft transport wiring, controller or
//! observer bring-up, auto-join, and the leader wait that gates every later
//! phase. It is separate from the rest of startup because the metadata source
//! must exist before storage recovery, the coordinators, or the listeners.

use std::sync::Arc;

use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};
use tokio::net::TcpListener;

use crate::{
    broker::{
        endpoints::static_controller_voter_set,
        registration::{
            register_broker, register_controller, spawn_deferred_controller_registration,
            submit_bootstrap_records,
        },
    },
    config::BrokerConfig,
    error::BrokerError,
};

struct RaftTransport {
    controller_cell: Arc<tokio::sync::OnceCell<Arc<krabka_raft::ControllerHandle>>>,
    /// Filled by `Broker::start` once the audit pipeline exists, so the
    /// controller listener's SASL logins reach the audit trail.
    audit_cell: crate::raft_handshake::AuditLogArc,
    handshake: Option<Arc<dyn krabka_raft::RaftListenerHandshake>>,
    dialer: Option<Arc<dyn krabka_raft::OutboundDialer>>,
    admin_router: Option<Arc<crate::controller_admin::BrokerControllerAdminRouter>>,
}

fn prepare_raft_transport(
    config: &BrokerConfig,
    tls_dynamic: Option<&Arc<krabka_security::DynamicServerConfig>>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
) -> RaftTransport {
    let controller_cell = Arc::new(tokio::sync::OnceCell::new());
    let audit_cell: crate::raft_handshake::AuditLogArc = Arc::new(tokio::sync::OnceCell::new());
    if config.controller_listener_protocol == krabka_security::ListenerProtocol::Plaintext {
        tracing::warn!(
            "controller listener is PLAINTEXT: every peer is ANONYMOUS, and each \
             controller RPC is authorized for that principal"
        );
    }
    // The handshake runs on every protocol. On `PLAINTEXT` it does no
    // authentication, but it still gives each connection the grants that the
    // listener checks for every request.
    let tls_acceptor =
        tls_dynamic.map(|dynamic| tokio_rustls::TlsAcceptor::from(dynamic.current()));
    let handshake = Some(Arc::new(crate::raft_handshake::BrokerRaftHandshake {
        tls_acceptor,
        plain_credentials: config.plain_credentials.as_map().clone(),
        enabled_sasl_mechanisms: config.enabled_sasl_mechanisms.clone(),
        gssapi: config.gssapi.clone(),
        oauthbearer_validator: config.oauthbearer_validator.clone(),
        protocol: config.controller_listener_protocol,
        controller: Arc::clone(&controller_cell),
        audit_log: Arc::clone(&audit_cell),
        max_frame_bytes: config.socket_request_max.bytes_usize(),
        authorizer: Arc::clone(&config.authorizer),
        principal_mapper: config.tls_principal_mapper.clone(),
    }) as Arc<dyn krabka_raft::RaftListenerHandshake>);
    let server_name = config
        .controller_server_name
        .clone()
        .unwrap_or_else(|| "localhost".to_owned());
    let dialer = Arc::new(crate::network::client::InterBrokerDialer::new(
        Arc::clone(inter_broker_client),
        config.controller_listener_protocol,
        server_name,
    )) as Arc<dyn krabka_raft::OutboundDialer>;
    RaftTransport {
        controller_cell,
        audit_cell,
        handshake,
        dialer: Some(dialer),
        admin_router: config
            .is_controller()
            .then(|| Arc::new(crate::controller_admin::BrokerControllerAdminRouter::new())),
    }
}

fn prepare_initial_voters(
    config: &BrokerConfig,
    bootstrap_records: &mut Vec<krabka_metadata::MetadataRecord>,
) -> krabka_metadata::VoterSet {
    let mut voters = crate::bootstrap::initial_voters(bootstrap_records);
    if !voters.is_empty() || config.controller_quorum_voters.is_empty() {
        return voters;
    }
    voters = static_controller_voter_set(
        &config.controller_quorum_voters,
        config.node_id,
        config.directory_id,
        config.controller_listen_addr,
    );
    tracing::info!(
        node_id = config.node_id.0,
        voter_count = config.controller_quorum_voters.len(),
        mode = ?config.bootstrap_mode,
        "deriving static KIP-595 voters from controller_quorum_voters"
    );
    // An explicitly formatted bootstrap stream already contains the exact
    // feature levels selected by `krabka format --feature`. KIP-853 keeps its
    // voter controls in the checkpoint rather than this stream, so reaching
    // the static discovery fallback does not mean the feature records are
    // absent. Appending release defaults here would replay after the selected
    // levels and overwrite them.
    if !bootstrap_records
        .iter()
        .any(|record| matches!(record, krabka_metadata::MetadataRecord::V1FeatureLevel(_)))
    {
        bootstrap_records.extend(krabka_metadata::bootstrap_feature_records(
            krabka_metadata::metadata_version::METADATA_VERSION_MAX,
        ));
    }
    voters
}

/// Validate `metadata_snapshot_fetch_max` for the observer's snapshot
/// transfer, the same way a controller validates it for its follower path: a
/// deployment may lower the 1 GiB ceiling but cannot raise it.
fn observer_snapshot_fetch_max(
    config: &BrokerConfig,
) -> Result<krabka_raft::kraft::snapshot_fetch::MetadataSnapshotFetchMax, BrokerError> {
    krabka_raft::kraft::snapshot_fetch::MetadataSnapshotFetchMax::new(
        config.metadata_snapshot_fetch_max,
    )
    .map_err(BrokerError::Startup)
}

async fn start_metadata_source(
    config: &BrokerConfig,
    bootstrap_records: &mut Vec<krabka_metadata::MetadataRecord>,
    controller_listener: Option<tokio::net::TcpListener>,
    transport: RaftTransport,
    wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
) -> Result<
    (
        Arc<dyn crate::metadata_source::MetadataSource>,
        Option<Arc<crate::controller_admin::BrokerControllerAdminRouter>>,
    ),
    BrokerError,
> {
    let RaftTransport {
        controller_cell,
        audit_cell: _,
        handshake,
        dialer,
        admin_router,
    } = transport;
    if config.is_controller() {
        let controller_config = krabka_raft::ControllerConfig {
            client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
            client_frame_max: config.client_frame_max,
            node_id: config.node_id,
            bootstrap_servers: config.bootstrap_servers.clone(),
            directory_id: config.directory_id,
            auto_join: config.auto_join,
            observer_lag_bound: config.observer_lag_bound,
            initial_voters: prepare_initial_voters(config, bootstrap_records),
            controller_listen_addr: config.controller_listen_addr,
            log_dir: config.log_dir.join("__cluster_metadata"),
            election_timeout: config.controller_election_timeout,
            heartbeat_interval: config
                .controller_heartbeat_interval_explicit
                .then_some(config.controller_heartbeat_interval),
            controller_fetch_miss_limit: config.controller_fetch_miss_limit,
            metadata_raft_command_queue_capacity: config.metadata_raft_command_queue_capacity,
            metadata_raft_fetch_max: config.metadata_raft_fetch_max,
            client_id: format!("krabka-broker-{}-controller", config.broker_id),
            bootstrap_mode: config.bootstrap_mode,
            cluster_id: config.cluster_id,
            dialer,
            handshake,
            shard_router: Some(Arc::new(crate::wal::quorum::registry::WalShardRouter::new(
                wal_shards,
            ))),
            admin_router: admin_router
                .clone()
                .map(|router| router as Arc<dyn krabka_raft::ControllerAdminRouter>),
            max_bytes_between_snapshots: config.metadata_max_bytes_between_snapshots,
            max_snapshot_interval: config.metadata_max_snapshot_interval,
            snapshot_interval_records: config.metadata_snapshot_interval_records,
            metadata_snapshot_fetch_max: config.metadata_snapshot_fetch_max,
        };
        let controller = Arc::new(
            krabka_raft::Controller::start_with_listener(controller_config, controller_listener)
                .await
                .map_err(|error| BrokerError::Startup(error.to_string()))?,
        );
        let _ = controller_cell.set(Arc::clone(&controller));
        return Ok((
            controller as Arc<dyn crate::metadata_source::MetadataSource>,
            admin_router,
        ));
    }

    drop(controller_listener);
    let dialer = dialer.expect("broker-only node requires a raft dialer");
    let observer = crate::metadata_observer::MetadataObserver::start(
        crate::metadata_observer::ObserverConfig {
            client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
            client_frame_max: config.client_frame_max,
            voters: config.controller_quorum_voters.clone(),
            dialer: Arc::clone(&dialer),
            client_id: format!("krabka-broker-{}-observer", config.broker_id),
            cluster_id: config.cluster_id.unwrap_or_else(uuid::Uuid::nil),
            node_id: config.node_id,
            // The metadata log directory. The observer keeps its checkpoints
            // in a subdirectory of their own beside the controller's, never in
            // it: an observer checkpoint carries no KIP-853 control state and
            // has no log to match its boundary, so a controller must not load
            // one. See `metadata_observer::store`.
            data_dir: config.log_dir.join("__cluster_metadata"),
            snapshot_interval_records: config.metadata_snapshot_interval_records,
            snapshot_fetch_max: observer_snapshot_fetch_max(config)?,
            max_bytes: config.observer_fetch_max,
            poll_interval: config.observer_poll_interval,
            timer: Arc::new(qubit_clock::StdTimer::new()),
        },
    );
    let forwarder = crate::metadata_source::QuorumForwarder {
        client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
        client_frame_max: config.client_frame_max,
        voters: config.controller_quorum_voters.clone(),
        dialer,
        client_id: format!("krabka-broker-{}-writer", config.broker_id),
        leader: observer.watch_leader(),
    };
    Ok((
        Arc::new(crate::metadata_source::ObserverSource::new(
            observer,
            Arc::new(forwarder),
        )),
        None,
    ))
}

fn spawn_auto_join(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
) {
    if !config.is_controller() {
        return;
    }
    let listener_protocol = config.controller_listener_protocol;
    let params = crate::auto_join::AutoJoinParams {
        auto_join: config.auto_join,
        retry_backoff: config.auto_join_retry_backoff,
        voter_request_timeout: config.auto_join_voter_request_timeout,
        node_id: config.node_id,
        directory_id: config.directory_id,
        cluster_id: config.cluster_id,
        bootstrap_servers: config.bootstrap_servers.clone(),
        advertised_controller: config
            .controller_quorum_voters
            .iter()
            .find(|(id, _)| *id == config.node_id)
            .map(|(_, endpoint)| endpoint.clone()),
        listener_protocol,
        inter_broker_server_name: config
            .controller_server_name
            .clone()
            .unwrap_or_else(|| config.inter_broker_server_name.clone()),
        controller: Arc::clone(controller),
        inter_broker_client: Arc::clone(inter_broker_client),
    };
    tokio::spawn(crate::auto_join::run(params.clone()));
    tokio::spawn(crate::auto_join::run_voter_updates(params));
}

async fn wait_for_metadata_leader(
    controller: &dyn crate::metadata_source::MetadataSource,
    timeout: std::time::Duration,
) -> Result<(), BrokerError> {
    let mut leaders = controller.watch_leader();
    let deadline = std::time::Instant::now() + timeout;
    while leaders.borrow().is_none() {
        if std::time::Instant::now() > deadline {
            return Err(BrokerError::Startup(format!(
                "no leader elected within {timeout:?}"
            )));
        }
        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(100), leaders.changed()).await;
    }
    Ok(())
}

/// Binds the controller listener before the quorum starts when the config asks
/// for an OS-assigned port, and publishes the port it got.
///
/// This node's own voter endpoint is where every heartbeat, `AssignReplicasToDirs`
/// and raft peer reaches its controller listener. A `:0` endpoint names nothing
/// that a client can dial. So the bound address replaces the configured one in
/// `controller_listen_addr` and in this node's `controller_quorum_voters`
/// entry, before the initial voter set and every client that reads it are
/// built. The live listener is handed on to the controller, so no other
/// process can take the port in between.
///
/// A caller-supplied listener, a concrete port, and a node without the
/// controller role keep their config as it is.
async fn bind_ephemeral_controller_listener(
    config: &mut BrokerConfig,
    prebound: Option<TcpListener>,
) -> Result<Option<TcpListener>, BrokerError> {
    if prebound.is_some() || !config.is_controller() || config.controller_listen_addr.port() != 0 {
        return Ok(prebound);
    }
    let listener = TcpListener::bind(config.controller_listen_addr).await?;
    publish_bound_controller_addr(config, listener.local_addr()?);
    Ok(Some(listener))
}

/// Writes `bound` into `controller_listen_addr` and into this node's own voter
/// entry when that entry asks for port 0. The entry keeps its host. Entries of
/// other nodes, and an entry with a concrete port, stay as configured.
fn publish_bound_controller_addr(config: &mut BrokerConfig, bound: std::net::SocketAddr) {
    config.controller_listen_addr = bound;
    let node_id = config.node_id;
    for (voter, endpoint) in &mut config.controller_quorum_voters {
        if *voter != node_id {
            continue;
        }
        if let Some((host, 0)) = crate::host_port::parse_host_port(endpoint) {
            *endpoint = format!("{host}:{}", bound.port());
        }
    }
}

pub(super) async fn start_metadata_phase(
    config: &mut BrokerConfig,
    controller_listener: Option<TcpListener>,
    tls_dynamic: Option<&Arc<krabka_security::DynamicServerConfig>>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
    wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
) -> Result<
    (
        Arc<dyn crate::metadata_source::MetadataSource>,
        Option<Arc<crate::controller_admin::BrokerControllerAdminRouter>>,
        crate::raft_handshake::AuditLogArc,
    ),
    BrokerError,
> {
    let controller_listener =
        bind_ephemeral_controller_listener(config, controller_listener).await?;
    let transport = prepare_raft_transport(config, tls_dynamic, inter_broker_client);
    let audit_cell = Arc::clone(&transport.audit_cell);
    let mut bootstrap_records = crate::bootstrap::load_bootstrap_records(&config.log_dir)?;
    let controller = start_metadata_source(
        config,
        &mut bootstrap_records,
        controller_listener,
        transport,
        wal_shards,
    )
    .await?;
    spawn_auto_join(config, &controller.0, inter_broker_client);
    wait_for_metadata_leader(&*controller.0, config.startup_leader_wait_timeout.to_std()).await?;
    if config.is_controller() || config.is_broker() {
        config.incarnation_id = crate::incarnation::load_or_generate(&config.log_dir);
        // Spend the clean-shutdown proof the last stop left, if it left one.
        // Reading it here -- before this node registers -- is what lets
        // `register_broker` tell a graceful restart from a crash.
        config.previous_broker_epoch = crate::clean_shutdown::take(&config.log_dir);
    }
    submit_bootstrap_records(config, &*controller.0, bootstrap_records).await?;
    register_controller(config, &*controller.0).await?;
    register_broker(config, &*controller.0).await?;
    spawn_deferred_controller_registration(config, &controller.0);
    Ok((controller.0, controller.1, audit_cell))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_raft::NodeId;

    use super::*;

    #[test]
    fn the_bound_port_replaces_only_this_nodes_port_zero_endpoint() {
        let bound: std::net::SocketAddr = "127.0.0.1:40123".parse().expect("static");
        let cases = [
            (
                "own ip endpoint",
                vec![(1, "127.0.0.1:0"), (2, "127.0.0.1:0")],
                vec![(1, "127.0.0.1:40123"), (2, "127.0.0.1:0")],
            ),
            (
                "own host name keeps its host",
                vec![(1, "localhost:0")],
                vec![(1, "localhost:40123")],
            ),
            (
                "own bracketed ipv6 keeps its host",
                vec![(1, "[::1]:0")],
                vec![(1, "[::1]:40123")],
            ),
            (
                "own concrete port stays",
                vec![(1, "127.0.0.1:9093")],
                vec![(1, "127.0.0.1:9093")],
            ),
            ("no own entry", vec![(2, "peer:0")], vec![(2, "peer:0")]),
        ];
        for (name, configured, published) in cases {
            let mut config = BrokerConfig::for_tests(std::path::PathBuf::new());
            config.node_id = NodeId(1);
            config.controller_quorum_voters = configured
                .iter()
                .map(|&(id, endpoint)| (NodeId(id), endpoint.to_owned()))
                .collect();

            publish_bound_controller_addr(&mut config, bound);

            let want: Vec<(NodeId, String)> = published
                .iter()
                .map(|&(id, endpoint)| (NodeId(id), endpoint.to_owned()))
                .collect();
            assert!(
                (
                    config.controller_listen_addr,
                    config.controller_quorum_voters
                ) == (bound, want),
                "{name}"
            );
        }
    }
}
