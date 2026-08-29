//! Cluster formation and controller start-up: the [`Controller`] factory that
//! validates the requested bootstrap mode against the on-disk log, opens or
//! recovers the engine, binds the controller listener, and the on-disk state
//! probe the broker binary shares with that validation.

use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

use super::{ControllerHandle, checkpoint::load_latest_checkpoint};
use crate::{
    config::{BootstrapMode, ControllerConfig},
    error::RaftError,
    kraft::KraftController,
    network::{OutboundDialer, PlaintextDialer, RealPeerSender},
    server,
};

/// Zero-sized factory for [`ControllerHandle`]s.
pub struct Controller;

impl Controller {
    /// Start a controller node, open the listener, and begin participating in
    /// the quorum.
    ///
    /// `bootstrap_mode` governs cluster formation: `Bootstrap` seeds a fresh
    /// quorum from `initial_voters`; `Join`/`Rejoin` recover or wait. Mismatches
    /// between mode and on-disk log state return [`RaftError::Startup`].
    ///
    /// # Errors
    /// Returns an error if configuration, storage recovery, or startup fails.
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, RaftError> {
        Self::start_with_listener(config, None).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr` itself.
    /// The supplied listener's local address MUST equal
    /// `config.controller_listen_addr`.
    ///
    /// # Errors
    /// Returns an error if the listener, storage, or controller cannot start.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = config.node_id.0, mode = ?config.bootstrap_mode),
        err
    )]
    pub async fn start_with_listener(
        config: ControllerConfig,
        prebound: Option<tokio::net::TcpListener>,
    ) -> Result<ControllerHandle, RaftError> {
        let metadata_snapshot_fetch_max =
            krabka_kraft_core::snapshot_fetch::MetadataSnapshotFetchMax::new(
                config.metadata_snapshot_fetch_max,
            )
            .map_err(RaftError::Startup)?;

        // First-boot orchestration validates mode against on-disk log state. The
        // metadata log lives directly under `log_dir` for the KraftLog engine.
        let data_dir = config.log_dir.clone();
        let log_exists = metadata_log_nonempty(&data_dir);
        let snapshot_voters = load_latest_checkpoint(&crate::kraft::checkpoint_dir(&data_dir))
            .and_then(|(_, bytes)| crate::snapshot::SnapshotReader::read(&bytes).ok())
            .and_then(|snapshot| snapshot.control_state.map(|state| state.voters))
            .unwrap_or_default();
        let voters = if config.initial_voters.is_empty() {
            snapshot_voters
        } else {
            config.initial_voters.clone()
        };
        let bootstrap_mode = if config.bootstrap_mode == BootstrapMode::Bootstrap
            && voters.is_empty()
            && config.auto_join
        {
            BootstrapMode::Join
        } else {
            config.bootstrap_mode
        };
        match (bootstrap_mode, log_exists) {
            (BootstrapMode::Bootstrap, false) => {
                if voters.is_empty() {
                    return Err(RaftError::Startup(
                        "Bootstrap mode requires a non-empty initial_voters set".into(),
                    ));
                }
            }
            (BootstrapMode::Join, false) | (BootstrapMode::Rejoin, true) => {}
            (BootstrapMode::Bootstrap, true) => {
                return Err(RaftError::Startup(
                    "Bootstrap mode requires empty raft log; existing log indicates an already-initialized broker — use Rejoin".into(),
                ));
            }
            (BootstrapMode::Rejoin, false) => {
                return Err(RaftError::Startup(
                    "Rejoin mode requires non-empty raft log; this broker has no on-disk state — use Bootstrap or Join".into(),
                ));
            }
            (BootstrapMode::Join, true) => {
                return Err(RaftError::Startup(
                    "Join mode requires empty raft log; this broker has on-disk state — use Rejoin"
                        .into(),
                ));
            }
        }

        let cluster_id = config.cluster_id.unwrap_or_else(Uuid::nil);
        let dialer: Arc<dyn OutboundDialer> = config
            .dialer
            .clone()
            .unwrap_or_else(|| Arc::new(PlaintextDialer));

        // The peer sender starts from the bootstrap view. The engine replaces
        // it immediately when it replays a dynamic voter control record.
        let peers = Arc::new(RealPeerSender::new(
            voters.clone(),
            &config.bootstrap_servers,
            config.client_id.clone(),
            Arc::clone(&dialer),
            config.client_dispatch_queue_capacity,
            config.client_frame_max,
        ));

        // Build / recover the engine. `Join` nodes with an empty log + empty
        // voter set sit unattached; `Bootstrap` seeds the static voter set.
        let engine = KraftController::open(
            data_dir.clone(),
            config.node_id,
            cluster_id,
            voters.clone(),
            config.election_timeout,
            config.heartbeat_interval,
            config.controller_fetch_miss_limit,
            config.metadata_raft_command_queue_capacity,
            config.metadata_raft_fetch_max,
            peers,
            config.snapshot_interval_records,
            metadata_snapshot_fetch_max,
        )?;

        // Controller listener.
        let listener = match prebound {
            Some(l) => l,
            None => tokio::net::TcpListener::bind(config.controller_listen_addr)
                .await
                .map_err(|e| RaftError::Storage(krabka_log::LogError::Io(e)))?,
        };
        let actual_addr = listener
            .local_addr()
            .map_err(|e| RaftError::Storage(krabka_log::LogError::Io(e)))?;
        let shutdown = CancellationToken::new();
        let leader_rx = engine.watch_leader();
        let listener_task = tokio::spawn(server::run(
            listener,
            engine.clone(),
            shutdown.clone(),
            config.handshake.clone(),
            config.shard_router.clone(),
            config.admin_router.clone(),
        ));
        info!(
            node_id = config.node_id.0,
            addr = %actual_addr,
            "controller started"
        );

        Ok(ControllerHandle {
            engine,
            leader: leader_rx,
            shutdown,
            listener_task: Mutex::new(Some(listener_task)),
            data_dir,
            client_id: config.client_id.clone(),
            client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
            client_frame_max: config.client_frame_max,
            self_node_id: config.node_id,
            voters,
            staged_learners: std::sync::Mutex::new(BTreeMap::new()),
            dialer,
            controller_bound_addr: actual_addr,
        })
    }
}

/// True when the metadata log under `dir` already holds durable raft state (a
/// previously-running node). Detects either a quorum-state file or any log
/// segment, indicating a node that has persisted state.
///
/// `dir` is the controller data dir (`<log_dir>/__cluster_metadata`). The
/// broker binary's `detect_bootstrap_mode` calls this so its Bootstrap/Rejoin
/// choice can never disagree with [`Controller::start_with_listener`]'s mode
/// validation — a node killed mid-election (segment dir created but no
/// `quorum-state` yet) reads as un-formatted and re-Bootstraps rather than
/// dying with "Rejoin requires non-empty raft log".
#[must_use]
pub fn metadata_log_nonempty(dir: &std::path::Path) -> bool {
    let qs = dir.join("quorum-state");
    if qs.exists() {
        return true;
    }
    // Any `*.log` segment indicates prior state.
    std::fs::read_dir(dir).is_ok_and(|entries| {
        entries
            .flatten()
            .any(|e| e.path().extension().is_some_and(|ext| ext == "log"))
    })
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::{
        controller::test_support::{submit_change_with_timeout, wait_for_leader},
        types::NodeId,
    };

    #[test]
    fn metadata_log_nonempty_detects_quorum_state_and_log_segments_only() {
        for (_case, file, expected) in [
            ("empty directory", None, false),
            (
                "quorum state file",
                Some(("quorum-state", b"state".as_slice())),
                true,
            ),
            (
                "log segment",
                Some(("00000000000000000000.log", b"log".as_slice())),
                true,
            ),
            (
                "non-log extension",
                Some(("00000000000000000000.txt", b"log".as_slice())),
                false,
            ),
        ] {
            let dir = TempDir::new().unwrap();
            if let Some((name, contents)) = file {
                std::fs::write(dir.path().join(name), contents).unwrap();
            }
            assert2::assert!(metadata_log_nonempty(dir.path()) == expected);
        }
    }

    #[tokio::test]
    async fn bootstrap_on_non_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("first bootstrap ok");
        // Drive a commit so the log is non-empty on the second boot.
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![krabka_metadata::MetadataRecord::V1Topic(
                krabka_metadata::TopicRecord {
                    name: "seed".into(),
                    topic_id: Uuid::new_v4(),
                    partitions: 1,
                    replication_factor: 1,
                },
            )],
            "bootstrap seed",
        )
        .await
        .expect("submit");
        ctrl.shutdown().await;

        let cfg2 = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        match Controller::start(cfg2).await {
            Err(err) => assert2::assert!(matches!(err, RaftError::Startup(_))),
            Ok(ctrl) => {
                ctrl.shutdown().await;
                panic!("Bootstrap on existing log must error but succeeded");
            }
        }
    }

    #[tokio::test]
    async fn rejoin_on_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Rejoin,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        match Controller::start(cfg).await {
            Err(err) => assert2::assert!(matches!(err, RaftError::Startup(_))),
            Ok(ctrl) => {
                ctrl.shutdown().await;
                panic!("Rejoin on empty log must error but succeeded");
            }
        }
    }

    #[tokio::test]
    async fn join_on_empty_log_starts_unattached() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Join,
            initial_voters: krabka_metadata::VoterSet::from_voters(std::iter::empty()),
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg)
            .await
            .expect("Join on empty log starts ok");
        // Without voters this node never elects.
        assert2::assert!(ctrl.watch_leader().borrow().is_none());
        ctrl.shutdown().await;
    }
}
