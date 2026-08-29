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
    let handshake =
        if config.controller_listener_protocol == krabka_security::ListenerProtocol::Plaintext {
            tracing::warn!(
                "controller listener is PLAINTEXT: raft/controller RPCs are unauthenticated"
            );
            None
        } else {
            let tls_acceptor =
                tls_dynamic.map(|dynamic| tokio_rustls::TlsAcceptor::from(dynamic.current()));
            let handshake = crate::raft_handshake::BrokerRaftHandshake {
                tls_acceptor,
                plain_credentials: config.plain_credentials.clone(),
                enabled_sasl_mechanisms: config.enabled_sasl_mechanisms.clone(),
                gssapi: config.gssapi.clone(),
                oauthbearer_validator: config.oauthbearer_validator.clone(),
                oauthbearer_max_session_lifetime: config.oauthbearer_max_session_lifetime,
                protocol: config.controller_listener_protocol,
                controller: Arc::clone(&controller_cell),
                max_frame_bytes: config.socket_request_max.bytes_usize(),
                authorizer: Arc::clone(&config.authorizer),
            };
            Some(Arc::new(handshake) as Arc<dyn krabka_raft::RaftListenerHandshake>)
        };
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
            max_bytes: config.observer_fetch_max,
            poll_interval: config.observer_poll_interval,
            sleeper: Arc::new(qubit_clock::sleep::SystemSleeper::new()),
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
    ),
    BrokerError,
> {
    let transport = prepare_raft_transport(config, tls_dynamic, inter_broker_client);
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
    }
    submit_bootstrap_records(config, &*controller.0, bootstrap_records).await?;
    register_controller(config, &*controller.0).await?;
    register_broker(config, &*controller.0).await?;
    spawn_deferred_controller_registration(config, &controller.0);
    Ok(controller)
}
