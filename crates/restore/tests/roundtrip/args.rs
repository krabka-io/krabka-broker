//! The command line every test that calls `restore` directly parses first, and
//! the controller listener a test that boots the restored node formats it
//! with.
//!
//! `restore` takes a `RestoreArgs`, and the flags that make `format_target`
//! succeed are the same in every test, so they are spelled once here and go
//! through `Cli`, the parser the binary itself uses.

use std::{net::SocketAddr, path::Path};

use clap::Parser as _;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, NodeId};
use krabka_restore::{Cli, RestoreArgs};
use tokio::net::TcpListener;

/// Build `RestoreArgs` with a valid target-side flag set (`--node-id`,
/// `--standalone`, `--controller-listener`) so `format_target` succeeds, plus
/// whatever `extra` flags a test needs. A test that never boots the restored
/// node can name any `controller_listener`; one that does takes it from
/// [`ControllerListener::restore_args`].
pub(crate) fn restore_args(
    archive_root: &Path,
    log_dir: &Path,
    controller_listener: &str,
    extra: &[&str],
) -> RestoreArgs {
    let mut argv = vec![
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        archive_root.display().to_string(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        controller_listener.to_owned(),
    ];
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    Cli::try_parse_from(argv).expect("valid command line").args
}

/// The controller listener of a restored node, bound before the restore
/// formats its log directory.
///
/// `--standalone` writes the node's controller endpoint into the voter set,
/// and a broker sends its heartbeats to the controller endpoint the voter set
/// names, as Kafka's `RaftControllerNodeProvider` finds the active controller.
/// A new registration stays fenced until a heartbeat unfences it, so a node
/// formatted with an address it does not listen on boots a broker that never
/// unfences. Kafka's `KafkaClusterTestKit` binds each node's controller port
/// first and formats the node with it; this does the same.
pub(crate) struct ControllerListener {
    addr: SocketAddr,
    listener: Option<TcpListener>,
}

impl ControllerListener {
    /// Bind a controller listener on an OS-assigned loopback port.
    pub(crate) async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the controller listener");
        Self {
            addr: listener.local_addr().expect("controller listener address"),
            listener: Some(listener),
        }
    }

    /// [`restore_args`] for a node that listens here.
    pub(crate) fn restore_args(
        &self,
        archive_root: &Path,
        log_dir: &Path,
        extra: &[&str],
    ) -> RestoreArgs {
        restore_args(archive_root, log_dir, &self.addr.to_string(), extra)
    }

    /// Start node 1 under `config` with its controller listening here. The
    /// first start adopts the listener bound in [`Self::bind`]; a restart binds
    /// the same address again, as a restarted node keeps its endpoint.
    pub(crate) async fn start(&mut self, mut config: BrokerConfig) -> BrokerHandle {
        config.controller_listen_addr = self.addr;
        config.controller_quorum_voters = vec![(NodeId(1), self.addr.to_string())];
        let listener = match self.listener.take() {
            Some(listener) => listener,
            None => TcpListener::bind(self.addr)
                .await
                .expect("bind the controller listener again"),
        };
        Box::pin(Broker::start_with_controller_listener(
            config,
            Some(listener),
        ))
        .await
        .expect("the restored broker starts")
    }
}
