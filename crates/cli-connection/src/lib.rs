//! The cluster connection every krabka operator CLI shares.
//!
//! `krabka-guard`, `krabka-barrier` and `krabka-backup` each reach a cluster,
//! and each spells that the same way: `--bootstrap-server` names where the
//! cluster is, and `--command-config` names a Java properties file with the
//! `security.protocol`, `ssl.*` and `sasl.*` settings, as
//! `kafka-topics --command-config` takes. [`ConnectionArgs`] is the flag group
//! a tool flattens into its command line, and [`SecurityArgs`] is the
//! `--command-config` half on its own, for a tool whose bootstrap server is
//! optional.

mod command_config;

pub use self::command_config::{CommandConfigError, SecurityArgs};
use {
    clap::Args,
    krabka_client_core::{Client, ClientError},
};

/// Where the cluster is.
///
/// A tool flattens this into its `Cli` with `#[command(flatten)]`, so every
/// tool that takes it accepts the same flags.
#[derive(Debug, Args, Clone)]
pub struct ConnectionArgs {
    /// One or more `host:port` pairs to bootstrap against.
    #[arg(long, short = 'b', env = "KRABKA_BOOTSTRAP_SERVER", required = true)]
    pub bootstrap_server: String,
}

/// Why [`ConnectionArgs::connect`] returned no client.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// No bootstrap server accepted the connection.
    #[error("cannot reach {bootstrap_server}: {source}")]
    Unreachable {
        /// The `--bootstrap-server` value the tool tried.
        bootstrap_server: String,
        /// The client's own complaint.
        source: ClientError,
    },
}

impl ConnectionArgs {
    /// Connect to the cluster as `client_id`.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Unreachable`] when no bootstrap server accepts
    /// the connection.
    pub async fn connect(&self, client_id: &str) -> Result<Client, ConnectError> {
        Client::builder()
            .bootstrap(&self.bootstrap_server)
            .client_id(client_id)
            .build()
            .await
            .map_err(|source| ConnectError::Unreachable {
                bootstrap_server: self.bootstrap_server.clone(),
                source,
            })
    }
}
