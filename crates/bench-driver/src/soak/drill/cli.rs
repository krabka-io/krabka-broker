//! `krabka-soak-drill`'s command line, and [`run`], which carries a parsed one
//! out. The binary only parses its arguments and listens for `SIGTERM`.

use {
    super::{load_contract, objectives, probe, record_load, record_readback},
    crate::soak::{
        client::{Endpoint, ScramLogin},
        recovery::Boundary,
    },
    anyhow::Result,
    clap::{Args, Parser, Subcommand},
    std::{future::Future, path::PathBuf},
};

/// `krabka-soak-drill`'s command line.
#[derive(Debug, Parser)]
#[command(name = "krabka-soak-drill", version, about)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// What `krabka-soak-drill` was asked to do.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Validate a disaster-recovery contract and print its objectives and
    /// paths as JSON, for the drill that creates the topics.
    Objectives {
        #[arg(long)]
        contract: PathBuf,
    },
    /// Run the contract's load against the source cluster until `SIGTERM`.
    Load {
        #[arg(long)]
        contract: PathBuf,
        #[command(flatten)]
        login: Login,
        /// The broker image the source cluster runs, recorded in the evidence.
        #[arg(long, default_value = "")]
        broker_image: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Write one record as the authenticated principal and read it back.
    Probe {
        #[command(flatten)]
        login: Login,
        #[arg(long)]
        topic: String,
        #[arg(long, default_value_t = 0)]
        partition: i32,
    },
    /// Read the restored cluster back and write the evidence the judge grades.
    Readback {
        #[arg(long)]
        contract: PathBuf,
        /// The `load` output.
        #[arg(long)]
        load: PathBuf,
        #[command(flatten)]
        login: Login,
        /// When the capture started, in Unix milliseconds: the capture boundary.
        #[arg(long)]
        capture_started_ms: i64,
        /// When the source cluster was lost, in Unix milliseconds.
        #[arg(long)]
        disaster_ms: i64,
        /// When the restored cluster first served authenticated produce and
        /// consume, in Unix milliseconds.
        #[arg(long)]
        serving_ms: Option<i64>,
        #[arg(long)]
        out: PathBuf,
    },
}

/// A SCRAM-SHA-512 login over TLS.
#[derive(Debug, Args)]
pub struct Login {
    #[arg(long)]
    bootstrap: String,
    /// The PEM CA bundle the brokers' certificates chain to.
    #[arg(long)]
    tls_ca: PathBuf,
    /// A name the brokers' certificate carries.
    #[arg(long)]
    tls_server_name: String,
    #[arg(long)]
    username: String,
    #[arg(long, env = "KRABKA_DRILL_PASSWORD", hide_env_values = true)]
    password: String,
}

impl Login {
    /// The endpoint this login reaches.
    #[must_use]
    pub fn endpoint(&self) -> Endpoint {
        Endpoint::sasl_ssl(self.bootstrap.clone(), ScramLogin {
            ca: self.tls_ca.clone(),
            server_name: self.tls_server_name.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
        })
    }
}

/// Carry out `cli`, and return what it prints on standard output.
///
/// `lost` is the loss of the source cluster: it ends a `load`.
///
/// # Errors
///
/// The contract, a load record or the cluster refuses the command, or a file
/// cannot be written.
pub async fn run(cli: Cli, lost: impl Future<Output = ()>) -> Result<String> {
    match cli.command {
        Command::Objectives { contract } => Ok(serde_json::to_string_pretty(&objectives(
            &load_contract(&contract)?,
        )?)?),
        Command::Load {
            contract,
            login,
            broker_image,
            out,
        } => {
            let contract = load_contract(&contract)?;
            record_load(&contract, login.endpoint(), broker_image, lost, &out).await?;
            Ok(String::new())
        }
        Command::Probe {
            login,
            topic,
            partition,
        } => probe(&login.endpoint(), &topic, partition).await,
        Command::Readback {
            contract,
            load,
            login,
            capture_started_ms,
            disaster_ms,
            serving_ms,
            out,
        } => {
            let boundary = Boundary {
                capture_started_unix_ms: capture_started_ms,
                disaster_unix_ms: disaster_ms,
            };
            record_readback(
                &load_contract(&contract)?,
                &load,
                &login.endpoint(),
                boundary,
                serving_ms,
                &out,
            )
            .await?;
            Ok(String::new())
        }
    }
}
