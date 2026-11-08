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
    clap::{Args, CommandFactory, FromArgMatches},
    krabka_client_core::{Client, ClientError},
    std::ffi::OsString,
};

/// The environment variable `--bootstrap-server` falls back to.
pub const BOOTSTRAP_SERVER_ENV: &str = "KRABKA_BOOTSTRAP_SERVER";

/// Parse `argv` as the command line `P`, which flattens [`ConnectionArgs`],
/// taking `--bootstrap-server` from [`BOOTSTRAP_SERVER_ENV`] when `argv` leaves
/// it out.
///
/// `env` looks a variable up by name: a tool's `run_from_args` passes the
/// process environment, and a test passes its own, because setting a variable
/// in process is `unsafe` in edition 2024. The flag wins when both name a
/// broker, and an empty variable counts as unset.
///
/// # Errors
///
/// Returns clap's error for an `argv` that does not parse, including one that
/// names no broker while the variable is unset.
pub fn try_parse_with_env<P, I, T>(
    argv: I,
    env: impl FnOnce(&str) -> Option<String>,
) -> Result<P, clap::Error>
where
    P: CommandFactory + FromArgMatches,
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let mut command = P::command();
    if let Some(bootstrap) = env(BOOTSTRAP_SERVER_ENV).filter(|value| !value.is_empty()) {
        command = command.mut_arg("bootstrap_server", |arg| {
            arg.required(false).default_value(bootstrap)
        });
    }
    let mut matches = command.try_get_matches_from(argv)?;
    P::from_arg_matches_mut(&mut matches).map_err(|error| error.format(&mut P::command()))
}

/// Where the cluster is, and how to authenticate to it.
///
/// A tool flattens this into its `Cli` with `#[command(flatten)]`, so every
/// tool that takes it accepts the same flags.
#[derive(Debug, Args, Clone)]
pub struct ConnectionArgs {
    /// One or more `host:port` pairs to bootstrap against. Without it the
    /// tool reads `KRABKA_BOOTSTRAP_SERVER`; see [`try_parse_with_env`].
    #[arg(long, short = 'b', required = true)]
    pub bootstrap_server: String,

    /// TLS and SASL properties for the connection. Without them the tool
    /// connects in PLAINTEXT.
    #[command(flatten)]
    pub security: SecurityArgs,
}

/// Why [`ConnectionArgs::connect`] returned no client.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The `--command-config` file could not be read, or its properties do
    /// not make a security policy. Nothing was sent.
    #[error(transparent)]
    CommandConfig(#[from] CommandConfigError),

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
    /// Connect to the cluster as `client_id`, with the TLS and SASL policy
    /// that `--command-config` gives.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::CommandConfig`] when the file cannot be read or
    /// its properties conflict, and [`ConnectError::Unreachable`] when no
    /// bootstrap server accepts the connection, which includes one that
    /// refuses the credentials.
    pub async fn connect(&self, client_id: &str) -> Result<Client, ConnectError> {
        let security = self.security.load().await?;
        Client::builder()
            .bootstrap(&self.bootstrap_server)
            .client_id(client_id)
            .maybe_security(security)
            .build()
            .await
            .map_err(|source| ConnectError::Unreachable {
                bootstrap_server: self.bootstrap_server.clone(),
                source,
            })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        clap::Parser,
        std::{fs, path::PathBuf},
    };

    /// A tool that flattens the group, as every operator CLI does.
    #[derive(Parser)]
    struct Tool {
        #[command(flatten)]
        connection: ConnectionArgs,
    }

    fn parse(extra: &[&str]) -> Result<ConnectionArgs, clap::Error> {
        let argv = ["tool", "--bootstrap-server", "broker-1:9093"];
        Tool::try_parse_from(argv.iter().chain(extra)).map(|tool| tool.connection)
    }

    /// `--command-config` names the properties file, and without it the tool
    /// has no file and connects in PLAINTEXT.
    #[yare::parameterized(
        absent = { &[], None },
        present = {
            &["--command-config", "/etc/krabka/admin.properties"],
            Some("/etc/krabka/admin.properties"),
        },
    )]
    fn the_command_config_flag_names_the_properties_file(extra: &[&str], expected: Option<&str>) {
        let connection = parse(extra).expect("parses");
        check!(connection.bootstrap_server == "broker-1:9093");
        check!(connection.security.command_config == expected.map(PathBuf::from));
    }

    /// A file that cannot be read stops the tool before it connects, and the
    /// error names the file.
    #[tokio::test]
    async fn an_unreadable_command_config_stops_the_connection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("absent.properties");
        let connection =
            parse(&["--command-config", missing.to_str().expect("utf-8 path")]).expect("parses");

        let Err(error) = connection.connect("test").await else {
            panic!("an absent file cannot connect");
        };

        assert!(let ConnectError::CommandConfig(CommandConfigError::Io(_)) = &error);
        check!(
            error.to_string().contains("absent.properties"),
            "got: {error}"
        );
    }

    /// A file whose properties make no policy stops the tool before it
    /// connects, and the error names the property, not its value.
    #[tokio::test]
    async fn a_conflicting_command_config_stops_the_connection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("client.properties");
        fs::write(
            &path,
            "security.protocol=SASL_PLAINTEXT\n\
             sasl.mechanism=PLAIN\n\
             sasl.jaas.config=x required password=hunter2;\n",
        )
        .expect("write properties");
        let connection =
            parse(&["--command-config", path.to_str().expect("utf-8 path")]).expect("parses");

        let Err(error) = connection.connect("test").await else {
            panic!("a file with no username cannot connect");
        };

        assert!(let ConnectError::CommandConfig(CommandConfigError::Invalid(_)) = &error);
        check!(error.to_string() == "command config is missing username");
    }
}
