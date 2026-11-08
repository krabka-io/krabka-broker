//! The cluster a scenario runs against, and the operator actions it takes.
//!
//! Every action builds the command line an operator would type and runs it
//! through [`run_command`], over a client that the broker authenticates as that
//! operator, with two buffers in place of stdout and stderr. The flags live
//! here and nowhere else, so a scenario says what an operator wants and this
//! module says how the tool is asked for it.

use {
    super::{
        params::{KeySource, Lifetime, LocalKeys, Operator, Scope},
        transcript::Run,
    },
    crate::{cli::Cli, command::run_command, run_with},
    clap::Parser as _,
    krabka_broker::{
        Broker, BrokerConfig, BrokerHandle, SslPrincipalMapper,
        config::{InterBrokerCredentials, ListenerSpec},
        operator_keys::{OperatorKeyEntry, OperatorKeys},
    },
    krabka_cli_connection::try_parse_with_env,
    krabka_client_core::{Client, SaslCredentials, security::ClientSecurity},
    krabka_protocol::{
        krabka::{
            break_glass::{DescribeBreakGlassRequest, DescribedBreakGlassProposal},
            freeze::{DescribeTopicFreezesRequest, DescribedTopicFreeze, PATTERN_TYPE_ANY},
        },
        owned::describe_cluster_request::DescribeClusterRequest,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    krabka_units::Time,
    ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair as _},
    },
    std::{
        collections::BTreeMap,
        fs,
        net::SocketAddr,
        path::{Path, PathBuf},
    },
    tempfile::TempDir,
};

/// The reason every freeze and thaw in the scenarios gives. No rule depends
/// on its text.
pub const REASON: &str = "incident";

/// The SASL listener's name, which is also the inter-broker listener.
const LISTENER: &str = "SASL_PLAINTEXT";

/// One operator key, on disk, in the three forms the tool and the broker read.
pub struct OperatorKeyFiles {
    /// The PKCS#8 private key that `--sign-with` reads.
    pub pkcs8: PathBuf,
    /// The raw 32-byte public key that `[[operator_keys]]` names.
    pub public: PathBuf,
    /// A TOML file with this one key in `[[operator_keys]]`, as `--operator-keys`
    /// reads it.
    pub trust_file: PathBuf,
}

/// Mint a key pair for `key_id`, bound to `principal`, under `dir`.
fn mint_key(dir: &Path, key_id: &str, principal: &str) -> OperatorKeyFiles {
    let der =
        Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("generate an Ed25519 key");
    let pair = Ed25519KeyPair::from_pkcs8(der.as_ref()).expect("parse the generated key");
    let stem = dir.join(key_id);
    let pkcs8 = stem.with_extension("pk8");
    fs::write(&pkcs8, der.as_ref()).expect("write the private key");
    let public = stem.with_extension("pub");
    fs::write(&public, pair.public_key().as_ref()).expect("write the public key");
    let trust_file = stem.with_extension("toml");
    write_trust_file(&trust_file, key_id, principal, &public);
    OperatorKeyFiles {
        pkcs8,
        public,
        trust_file,
    }
}

/// Write a trust file with one `[[operator_keys]]` entry.
fn write_trust_file(path: &Path, key_id: &str, principal: &str, public: &Path) {
    let public = toml::Value::String(public.display().to_string());
    fs::write(
        path,
        format!(
            "[[operator_keys]]\nkey_id = \"{key_id}\"\nprincipal = \"{principal}\"\n\
             public_key_path = {public}\n"
        ),
    )
    .expect("write the trust file");
}

/// A single broker behind a SASL listener, and the operator keys it trusts.
///
/// alice, bob and carol each authenticate as themselves, each holds an
/// operator key the broker trusts, and all three are break-glass approvers.
/// Two approvals are required, which is the broker's default.
pub struct Cluster {
    /// Held so the broker runs as long as the scenario does.
    _broker: BrokerHandle,
    bootstrap: SocketAddr,
    /// Held so the data directory and the key files outlive the broker.
    dir: TempDir,
    keys: Vec<(Operator, OperatorKeyFiles)>,
}

/// Boot the scenario cluster.
///
/// `proposal_ttl` is `break_glass.proposal_ttl`, the cap on how long a
/// proposal stays open. It keeps the broker's default unless a scenario says
/// otherwise.
#[bon::builder]
pub async fn boot(proposal_ttl: Option<Time>) -> Cluster {
    let dir = TempDir::new().expect("a scenario directory");
    let keys: Vec<_> = Operator::ALL
        .into_iter()
        .map(|operator| {
            let files = mint_key(dir.path(), &operator.key_id(), &operator.principal());
            (operator, files)
        })
        .collect();

    let mut config = BrokerConfig::for_tests(dir.path().join("data"));
    config.listeners = vec![ListenerSpec {
        name: LISTENER.to_owned(),
        bind_addr: "127.0.0.1:0".parse().expect("a socket address"),
        advertised: "127.0.0.1:0".to_owned(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: Some(vec![SaslMechanism::Plain]),
        principal_mapper: SslPrincipalMapper::default(),
    }];
    LISTENER.clone_into(&mut config.inter_broker_listener_name);
    config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    config.plain_credentials = Operator::ALL
        .into_iter()
        .map(|operator| (operator.user(), operator.password()))
        .collect();
    config.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: Operator::Alice.user(),
        password: Operator::Alice.password(),
    });
    let entries: Vec<_> = keys
        .iter()
        .map(|(operator, files)| OperatorKeyEntry {
            key_id: operator.key_id(),
            principal: operator.principal(),
            public_key_path: files.public.clone(),
        })
        .collect();
    config.operator_keys = OperatorKeys::load(&entries).expect("load the operator trust set");
    config.break_glass.approvers = Operator::ALL.into_iter().map(Operator::principal).collect();
    if let Some(proposal_ttl) = proposal_ttl {
        config.break_glass.proposal_ttl = proposal_ttl;
    }

    let broker = Broker::start(config).await.expect("the broker starts");
    let bootstrap = broker.listen_addr();
    Cluster {
        _broker: broker,
        bootstrap,
        dir,
        keys,
    }
}

/// A single broker behind the plaintext listener the shipped tool can reach.
///
/// The scenario [`Cluster`] authenticates every connection over SASL, and the
/// shipped tool cannot log in, so the steps that run the tool connect step and
/// all need this one.
pub struct PlaintextBroker {
    /// Held so the broker runs as long as the scenario does.
    _broker: BrokerHandle,
    bootstrap: SocketAddr,
    /// Held so the data directory outlives the broker.
    _dir: TempDir,
}

impl PlaintextBroker {
    /// Boot a broker with the test defaults and nothing else.
    pub async fn boot() -> Self {
        let dir = TempDir::new().expect("a scenario directory");
        let broker = Broker::start(BrokerConfig::for_tests(dir.path().join("data")))
            .await
            .expect("the broker starts");
        let bootstrap = broker.listen_addr();
        Self {
            _broker: broker,
            bootstrap,
            _dir: dir,
        }
    }

    /// The bootstrap address an operator types.
    pub fn bootstrap(&self) -> String { self.bootstrap.to_string() }
}

/// How an operator signs one freeze or thaw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signing {
    /// No signing flags at all.
    Unsigned,
    /// `--sign-with` the file the key source names, under the key id of the
    /// operator who owns it, naming the acting operator as the author.
    With(KeySource),
}

impl Cluster {
    /// The bootstrap address an operator types.
    pub fn bootstrap(&self) -> String { self.bootstrap.to_string() }

    /// A client the broker authenticates as `operator`.
    pub async fn client(&self, operator: Operator) -> Client {
        Client::builder()
            .bootstrap(self.bootstrap())
            .client_id("krabka-guard-scenario")
            .security(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Plain {
                    username: operator.user(),
                    password: operator.password(),
                }),
                sasl_host: None,
            })
            .build()
            .await
            .expect("an authenticated client connects")
    }

    /// The key files `operator` owns.
    pub fn key(&self, operator: Operator) -> &OperatorKeyFiles {
        self.keys
            .iter()
            .find(|(owner, _)| *owner == operator)
            .map(|(_, files)| files)
            .expect("every operator holds a minted key")
    }

    /// Run the tool as `operator` with `args` after the bootstrap flag.
    ///
    /// A command line the parser refuses never reaches a broker, which is the
    /// [`Run::command_line_refused`] outcome.
    pub async fn guard(&self, operator: Operator, args: &[&str]) -> Run {
        let mut command_line = vec!["krabka-guard", "--bootstrap-server", "scenario"];
        command_line.extend_from_slice(args);
        let Ok(cli) = Cli::try_parse_from(command_line) else {
            return Run::command_line_refused();
        };
        let client = self.client(operator).await;
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let code = run_command(&client, cli.command, &mut stdout, &mut stderr).await;
        Run::new(code, &stdout, &stderr)
    }

    /// `operator` freezes `scope`.
    pub async fn freeze(&self, operator: Operator, scope: &Scope, signing: Signing) -> Run {
        let mut args = vec![
            "freeze",
            "set",
            scope.flag(),
            scope.name(),
            "--reason",
            REASON,
        ];
        let flags = self.signing_flags(operator, signing);
        args.extend(flags.iter().map(String::as_str));
        self.guard(operator, &args).await
    }

    /// `operator` lifts the freeze on `scope`, naming `proposal`.
    pub async fn thaw(
        &self,
        operator: Operator,
        scope: &Scope,
        proposal: uuid::Uuid,
        signing: Signing,
    ) -> Run {
        let proposal = proposal.to_string();
        let mut args = vec![
            "freeze",
            "clear",
            scope.flag(),
            scope.name(),
            "--proposal",
            &proposal,
            "--reason",
            REASON,
        ];
        let flags = self.signing_flags(operator, signing);
        args.extend(flags.iter().map(String::as_str));
        self.guard(operator, &args).await
    }

    /// The `--sign-with`, `--key-id` and `--principal` flags for `signing`.
    ///
    /// The author is always the acting operator. The key id is the owner's,
    /// so signing with someone else's key is the owner's key id beside the
    /// actor's name.
    fn signing_flags(&self, operator: Operator, signing: Signing) -> Vec<String> {
        let (path, key_id) = match signing {
            Signing::Unsigned => return Vec::new(),
            Signing::With(KeySource::OperatorKey(owner)) => {
                (self.key(owner).pkcs8.clone(), owner.key_id())
            }
            Signing::With(KeySource::NotAKey) => (self.not_a_key(), operator.key_id()),
        };
        vec![
            "--sign-with".to_owned(),
            path.display().to_string(),
            "--key-id".to_owned(),
            key_id,
            "--principal".to_owned(),
            operator.principal(),
        ]
    }

    /// A file where a PKCS#8 key would be, holding text instead.
    fn not_a_key(&self) -> PathBuf {
        let path = self.dir.path().join("not-a-key.pk8");
        fs::write(&path, "this is not a key").expect("write the decoy key file");
        path
    }

    /// `operator` opens a proposal for `action` on `target`.
    ///
    /// Returns the run and, when the cluster holds a proposal it did not hold
    /// before, that proposal's id.
    pub async fn propose(
        &self,
        operator: Operator,
        action: &str,
        target: &str,
        lifetime: &Lifetime,
    ) -> (Run, Option<uuid::Uuid>) {
        let before = self.proposals().await;
        let mut args = vec![
            "break-glass",
            "propose",
            "--action",
            action,
            "--target",
            target,
            "--reason",
            REASON,
        ];
        if let Lifetime::Typed(ttl) = lifetime {
            args.extend(["--ttl", ttl.as_str()]);
        }
        let run = self.guard(operator, &args).await;
        let opened = self
            .proposals()
            .await
            .into_iter()
            .find(|stored| {
                !before
                    .iter()
                    .any(|old| old.proposal_id == stored.proposal_id)
            })
            .map(|stored| uuid::Uuid::from_bytes(stored.proposal_id.0));
        (run, opened)
    }

    /// `operator` approves `proposal`, signed with `key` when one is given.
    pub async fn approve(
        &self,
        operator: Operator,
        proposal: uuid::Uuid,
        key: Option<KeySource>,
    ) -> Run {
        let proposal = proposal.to_string();
        let mut args = vec!["break-glass", "approve", "--proposal", &proposal];
        let (path, key_id) = match key {
            None => (None, None),
            Some(KeySource::OperatorKey(owner)) => (
                Some(self.key(owner).pkcs8.display().to_string()),
                Some(owner.key_id()),
            ),
            Some(KeySource::NotAKey) => (
                Some(self.not_a_key().display().to_string()),
                Some(operator.key_id()),
            ),
        };
        if let (Some(path), Some(key_id)) = (&path, &key_id) {
            args.extend(["--sign-with", path, "--key-id", key_id]);
        }
        self.guard(operator, &args).await
    }

    /// `operator` withdraws `proposal`.
    pub async fn withdraw(&self, operator: Operator, proposal: uuid::Uuid) -> Run {
        let proposal = proposal.to_string();
        self.guard(operator, &[
            "break-glass",
            "withdraw",
            "--proposal",
            &proposal,
        ])
        .await
    }

    /// `operator` verifies the registry against `trust_file`.
    pub async fn verify(&self, operator: Operator, trust_file: &Path) -> Run {
        let trust_file = trust_file.display().to_string();
        self.guard(operator, &[
            "freeze",
            "list",
            "--verify-signatures",
            "--operator-keys",
            &trust_file,
        ])
        .await
    }

    /// `operator` lists the registry without verifying it.
    pub async fn list_registry(&self, operator: Operator) -> Run {
        self.guard(operator, &["freeze", "list"]).await
    }

    /// `operator` lists every proposal the cluster holds.
    pub async fn list_proposals(&self, operator: Operator) -> Run {
        self.guard(operator, &["break-glass", "list"]).await
    }

    /// The trust file the verifying operator's machine holds.
    pub fn trust_file(&self, local: LocalKeys) -> PathBuf {
        match local {
            LocalKeys::AlicesPublicKey => self.key(Operator::Alice).trust_file.clone(),
            LocalKeys::OnlyBobsPublicKey => self.key(Operator::Bob).trust_file.clone(),
            LocalKeys::AnotherKeyUnderAlicesId => {
                let other = self.dir.path().join("other");
                fs::create_dir_all(&other).expect("a directory for the other key");
                mint_key(
                    &other,
                    &Operator::Alice.key_id(),
                    &Operator::Alice.principal(),
                )
                .trust_file
            }
            LocalKeys::NoOperatorKeys => {
                let path = self.dir.path().join("no-operator-keys.toml");
                fs::write(&path, "operator_keys = []\n").expect("write the empty trust file");
                path
            }
        }
    }

    /// Every live registry entry, read straight from the broker.
    pub async fn registry(&self) -> Vec<DescribedTopicFreeze> {
        let response = self
            .client(Operator::Alice)
            .await
            .send(DescribeTopicFreezesRequest {
                pattern_type_filter: PATTERN_TYPE_ANY,
                ..DescribeTopicFreezesRequest::default()
            })
            .await
            .expect("describe the registry");
        assert2::assert!(response.error_code == 0);
        response.freezes
    }

    /// Every proposal the cluster holds, read straight from the broker.
    pub async fn proposals(&self) -> Vec<DescribedBreakGlassProposal> {
        let response = self
            .client(Operator::Alice)
            .await
            .send(DescribeBreakGlassRequest::default())
            .await
            .expect("describe the proposals");
        assert2::assert!(response.error_code == 0);
        response.proposals
    }

    /// The one proposal with `id`.
    pub async fn proposal(&self, id: uuid::Uuid) -> DescribedBreakGlassProposal {
        self.proposals()
            .await
            .into_iter()
            .find(|stored| stored.proposal_id.0 == *id.as_bytes())
            .expect("the cluster holds the proposal")
    }

    /// The cluster id a freeze signature covers.
    pub async fn cluster_id(&self) -> String {
        self.client(Operator::Alice)
            .await
            .send(DescribeClusterRequest::default())
            .await
            .expect("describe the cluster")
            .cluster_id
    }
}

/// Run the shipped entry point against `bootstrap`, exactly as the binary
/// runs it, connect step included, with buffers in place of its streams.
pub async fn run_binary(bootstrap: &str, args: &[&str]) -> Run {
    let mut command_line = vec!["krabka-guard", "--bootstrap-server", bootstrap];
    command_line.extend_from_slice(args);
    let cli = Cli::parse_from(command_line);
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let code = run_with(cli, &mut stdout, &mut stderr).await;
    Run::new(code, &stdout, &stderr)
}

/// Run the shipped tool with `command_line` as its whole command line, reading
/// its environment variables from `environment`, connect step included.
///
/// A scenario cannot set a variable in its own process, because that is
/// `unsafe` in edition 2024, so it hands the parser this map instead.
pub async fn run_in(command_line: &[&str], environment: &BTreeMap<String, String>) -> Run {
    let Ok(cli) =
        try_parse_with_env::<Cli, _, _>(command_line, |name| environment.get(name).cloned())
    else {
        return Run::command_line_refused();
    };
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let code = run_with(cli, &mut stdout, &mut stderr).await;
    Run::new(code, &stdout, &stderr)
}
