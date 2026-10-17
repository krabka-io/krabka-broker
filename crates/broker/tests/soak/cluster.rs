//! A three-broker krabka cluster, each broker a real container process.
//!
//! The fixture is [`process_kill_durability`]'s single-node one widened to a
//! quorum: the same packaging image, the same host-tempdir bind mount at
//! `/var/lib/krabka`, the same `--user` matched to the host directory's owner
//! so the container can write it, and the same `krabka-format` run through
//! `--entrypoint` before the broker starts.
//!
//! Three things are new, and all three come from there being peers.
//!
//! **Addressing.** A container's `127.0.0.1` is its own, so a broker cannot
//! advertise loopback to the other two. Every node publishes its client and
//! controller ports on the host and advertises `host.docker.internal:<port>`
//! for both, which the containers resolve through
//! `--add-host=host.docker.internal:host-gateway` and the host resolves through
//! the `/etc/hosts` line CI's container jobs already add. One name that both
//! sides can reach, which is what the JVM suites in this crate do for the
//! reverse direction.
//! A host without that line cannot reach any broker it is told about, so
//! [`SoakCluster::configured`] refuses to start one there, and says how to add
//! the line.
//!
//! **The quorum.** `--standalone` writes a one-voter `VotersRecord`. Here each
//! node is formatted with the same `--cluster-id` and the same
//! `--initial-controllers` list naming all three by id, controller endpoint and
//! directory id, so the three logs agree on the voter set before any of them
//! boots.
//!
//! **Sampling.** `docker inspect` gives each container's host-side pid, and
//! because the process runs as the host user that owns its data directory,
//! `/proc/<pid>/status` and `/proc/<pid>/fd` are readable from the test without
//! a shell inside the image -- which the apko base does not carry.
//!
//! **A diskless quorum**, when the caller asks for one. A WAL follower's Fetch
//! is authorized against the caller's principal: the leader serves a shard
//! only to a voter that authenticates as `broker-<id>` on the inter-broker
//! listener, so an anonymous cluster never forms a WAL quorum. Such a node
//! binds a second data-plane listener, `SASL_PLAINTEXT` on the container's
//! 9094, that `inter_broker_listener_name` selects and that carries
//! replication and the WAL follower fetches; clients stay on `PLAINTEXT`. The
//! broker's file configuration has no inter-broker PLAIN or SCRAM credential,
//! so the node authenticates with `OAUTHBEARER`: an unsecured (`alg: none`)
//! token whose `sub` is its principal, which the peers' development validator
//! accepts. Each node also reports its own rack, because the WAL placement
//! picks one voter per rack. The `diskless_jepsen` suite runs the same shape
//! in process, with SASL/PLAIN.
//!
//! [`process_kill_durability`]: ../process_kill_durability.rs

pub(crate) use crate::support::docker;
use {
    crate::support::{
        diskless::broker_principal,
        free_port,
        load_test::{Client, CreatableTopic, CreateTopicsRequest},
        remove_container_with_volumes,
        storage::host_directory_user,
    },
    assert2::assert,
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    krabka_broker::NodeId,
    std::{
        io,
        net::{SocketAddr, ToSocketAddrs as _},
        path::{Path, PathBuf},
        process::Command,
        time::{Duration, Instant},
    },
};

/// The name every node advertises, and the one the test's own clients dial.
const HOST_GATEWAY: &str = "host.docker.internal";

/// Why the host cannot reach the brokers by [`HOST_GATEWAY`], given what
/// resolving it returned, or `None` when it resolves to loopback.
///
/// The brokers publish their ports on the host and advertise this name. Inside
/// a container it is the host gateway; on the host it needs an `/etc/hosts`
/// line pointing at loopback. Without that line every client that follows a
/// broker's advertised address fails with a closed connection, the run's load
/// never starts, and the failure reads as a broker that stopped answering.
fn host_gateway_problem(resolved: io::Result<Vec<SocketAddr>>) -> Option<String> {
    let fix = format!(
        "add `127.0.0.1 {HOST_GATEWAY}` to /etc/hosts, as CI's container jobs do \
         (`echo \"127.0.0.1 {HOST_GATEWAY}\" | sudo tee -a /etc/hosts`)"
    );
    match resolved {
        Err(error) => Some(format!(
            "{HOST_GATEWAY} does not resolve on this host ({error}); {fix}"
        )),
        Ok(addrs) if addrs.iter().any(|addr| addr.ip().is_loopback()) => None,
        Ok(addrs) => Some(format!(
            "{HOST_GATEWAY} resolves to {addrs:?} on this host, not to loopback, so the brokers' \
             published ports are not reachable through it; {fix}"
        )),
    }
}

/// Refuse to boot a cluster the host's clients could not reach.
fn require_host_gateway() {
    let resolved = (HOST_GATEWAY, 0).to_socket_addrs().map(Iterator::collect);
    if let Some(problem) = host_gateway_problem(resolved) {
        panic!("{problem}");
    }
}

/// The tag `//packaging:image_load` loads the broker image under.
///
/// `//bazel/defs.bzl` sets `KRABKA_BROKER_IMAGE` to the same string from
/// `//bazel/krabka_image.bzl`.
const DEFAULT_IMAGE: &str = "docker.io/krabka-io/krabka-broker:dev";

/// Where the image's `working_dir` is, and what the data volume mounts on.
const CONTAINER_ROOT: &str = "/var/lib/krabka";

/// The formatted log directory inside that mount.
const CONTAINER_LOG_DIR: &str = "/var/lib/krabka/data";

/// The configuration file each node is written and started with, relative to
/// the mount. See [`SoakBroker::write_config`] for why a quorum needs one.
const CONFIG_FILE: &str = "broker.toml";

/// The listener a diskless-quorum node's brokers speak to each other on.
const INTER_BROKER_LISTENER: &str = "INTER_BROKER";

/// The bearer token a diskless-quorum node authenticates to its peers with,
/// relative to the mount.
const TOKEN_FILE: &str = "inter-broker.token";

/// The `exp` of that token, 2100-01-01: the validator requires one, and the
/// session it opens ends there, well after any run.
const TOKEN_EXPIRY_UNIX_SECONDS: u64 = 4_102_444_800;

/// How long a fresh cluster gets to elect a controller and answer a request.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// How often the cleaner sweeps. The default is 30s, which would give a
/// four-hour run 480 opportunities but a three-minute one six -- below the ten
/// this suite insists on. One second makes the cycle count reachable in both.
const CLEANER_INTERVAL: &str = "1s";

/// How often the local-retention sweep runs. The default is five minutes
/// (Kafka's `log.retention.check.interval.ms`), which a three-minute run would
/// not reach even once. One second gives the run about 180 opportunities, so
/// the ten retention cycles this suite insists on are reachable in the short
/// run as well as the nightly one.
const RETENTION_CHECK_INTERVAL: &str = "1s";

/// The image tag to run.
pub(crate) fn image() -> String {
    std::env::var("KRABKA_BROKER_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_owned())
}

/// One broker of the cluster.
pub(crate) struct SoakBroker {
    /// Container name, and the label its samples are reported under.
    pub(crate) name: String,
    pub(crate) node_id: u32,
    /// Published host port for the container's 9092.
    client_port: u16,
    /// Published host port for the container's 9093.
    controller_port: u16,
    /// Published host port for the container's 9404.
    metrics_port: u16,
    /// Published host port for the container's 9094, the authenticated
    /// inter-broker listener a diskless quorum needs. `None` when the cluster
    /// runs none.
    inter_broker_port: Option<u16>,
    /// Host side of the `/var/lib/krabka` mount.
    pub(crate) root: tempfile::TempDir,
}

impl SoakBroker {
    /// `uid:gid` of the host data directory, for `docker run --user`.
    fn user(&self) -> String { host_directory_user(self.host_root()) }

    fn host_root(&self) -> &Path { self.root.path() }

    /// The host-side log directory this broker writes into.
    pub(crate) fn host_log_dir(&self) -> PathBuf { self.host_root().join("data") }

    /// Where a host-side client bootstraps against this broker.
    pub(crate) fn bootstrap(&self) -> String { format!("127.0.0.1:{}", self.client_port) }

    /// Where a host-side scraper reads this broker's `/metrics`.
    pub(crate) fn metrics_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.metrics_port)
    }

    /// The controller endpoint peers and the format step name.
    fn controller_advertised(&self) -> String {
        format!("host.docker.internal:{}", self.controller_port)
    }

    /// This node's entry in `--initial-controllers`.
    fn voter_spec(&self) -> String {
        format!(
            "{}@{}:{}",
            self.node_id,
            self.controller_advertised(),
            directory_id(self.node_id)
        )
    }

    /// The container's pid on the host.
    ///
    /// `docker run --user` matched the process to the host user that owns the
    /// data directory, so that user can read this pid's `/proc` entries.
    pub(crate) fn host_pid(&self) -> u32 {
        let raw = docker(&["inspect", "--format", "{{.State.Pid}}", &self.name]);
        raw.parse()
            .unwrap_or_else(|e| panic!("{} reported pid {raw:?}: {e}", self.name))
    }

    /// Format this node's log directory against the shared voter set.
    fn format(&self, cluster_id: &str, voters: &str) {
        let mount = format!("{}:{CONTAINER_ROOT}", self.host_root().display());
        let user = self.user();
        let img = image();
        let node = format!("--node-id={}", self.node_id);
        let directory = format!("--directory-id={}", directory_id(self.node_id));
        let cluster = format!("--cluster-id={cluster_id}");
        let initial = format!("--initial-controllers={voters}");
        docker(&[
            "run",
            "--rm",
            "--user",
            &user,
            "--volume",
            &mount,
            "--entrypoint",
            "/usr/bin/krabka-format",
            &img,
            &format!("--log-dir={CONTAINER_LOG_DIR}"),
            &cluster,
            &node,
            &directory,
            &initial,
            "--ignore-formatted",
        ]);
    }

    /// Write this node's configuration file into the bind mount.
    ///
    /// A three-node quorum needs `controller_quorum_voters`, and that key has
    /// no command-line flag: without a file the broker derives a one-voter set
    /// naming itself, so three nodes started with flags alone each elect
    /// themselves and never form a quorum. The file is also where the
    /// advertised listener has to go, because `--advertised-listener` describes
    /// the flag-built listener that a file replaces.
    ///
    /// `extras` adds top-level keys after the voter list and whole tables at
    /// the end, where TOML allows each.
    fn write_config(&self, voters: &str, extras: &NodeExtras) {
        let quorum = self.inter_broker_port.map(|port| {
            std::fs::write(
                self.host_root().join(TOKEN_FILE),
                inter_broker_token(self.node_id),
            )
            .expect("write the inter-broker bearer token");
            diskless_quorum_toml(
                self.node_id,
                port,
                &format!("{CONTAINER_ROOT}/{TOKEN_FILE}"),
            )
        });
        let (quorum_root, quorum_listener, quorum_tables) = quorum.map_or_else(
            || (String::new(), String::new(), String::new()),
            |toml| (toml.root, toml.listener, toml.tables),
        );
        let toml = format!(
            "broker_id = {id}\n\
             log_dir = \"{CONTAINER_LOG_DIR}\"\n\
             controller_quorum_voters = [{voters}]\n\
             {quorum_root}\
             {root}\
             \n\
             [[listeners]]\n\
             name = \"PLAINTEXT\"\n\
             bind_addr = \"0.0.0.0:9092\"\n\
             advertised = \"host.docker.internal:{client}\"\n\
             protocol = \"Plaintext\"\n\
             {quorum_listener}\
             \n\
             [process]\n\
             roles = [\"controller\", \"broker\"]\n\
             {quorum_tables}\
             {tables}",
            id = self.node_id,
            client = self.client_port,
            root = extras.root_toml,
            tables = extras.tables_toml,
        );
        std::fs::write(self.host_root().join(CONFIG_FILE), toml)
            .expect("write the broker configuration file");
    }

    /// Boot the broker.
    ///
    /// `--broker-id` is passed on the command line as well as in the file:
    /// the node id the raft peer identifies itself by is taken from the flag
    /// before the file is read, so a node configured by file alone answers as
    /// node 1 and its peers discard its votes.
    ///
    /// `extras.shared_mount`, when set, is bind-mounted into the container as
    /// well, at the same container path on every node.
    fn run(&self, cluster_id: &str, extras: &NodeExtras) {
        let mount = format!("{}:{CONTAINER_ROOT}", self.host_root().display());
        let user = self.user();
        let img = image();
        // 0.0.0.0 rather than 127.0.0.1: a peer reaches this port through the
        // host gateway address, not through the host's loopback.
        let publish_client = format!("{}:9092", self.client_port);
        let publish_controller = format!("{}:9093", self.controller_port);
        let publish_metrics = format!("{}:9404", self.metrics_port);
        let publish_inter_broker = self.inter_broker_port.map(|port| format!("{port}:9094"));
        let shared = extras
            .shared_mount
            .as_ref()
            .map(|(host, container)| format!("{}:{container}", host.display()));
        let mut args = vec![
            "run",
            "--detach",
            "--name",
            &self.name,
            "--user",
            &user,
            "--add-host",
            "host.docker.internal:host-gateway",
            "--volume",
            &mount,
        ];
        if let Some(shared) = &shared {
            args.extend(["--volume", shared.as_str()]);
        }
        let config = format!("--config-file={CONTAINER_ROOT}/{CONFIG_FILE}");
        let broker_id = format!("--broker-id={}", self.node_id);
        let cluster = format!("--cluster-id={cluster_id}");
        let cleaner = format!("--cleaner-interval={CLEANER_INTERVAL}");
        let retention = format!("--log-retention-check-interval={RETENTION_CHECK_INTERVAL}");
        if let Some(publish) = &publish_inter_broker {
            args.extend(["--publish", publish.as_str()]);
        }
        args.extend([
            "--publish",
            &publish_client,
            "--publish",
            &publish_controller,
            "--publish",
            &publish_metrics,
            &img,
            &config,
            &broker_id,
            &cluster,
            "--metrics-listen-addr=0.0.0.0:9404",
            "--health-listen-addr=none",
            &cleaner,
            &retention,
        ]);
        docker(&args);
    }

    /// This broker's container log, for a failure message.
    pub(crate) fn container_log(&self) -> String {
        Command::new("docker")
            .args(["logs", "--tail", "40", &self.name])
            .output()
            .map_or_else(
                |e| format!("<could not read logs: {e}>"),
                |out| {
                    format!(
                        "{}{}",
                        String::from_utf8_lossy(&out.stdout),
                        String::from_utf8_lossy(&out.stderr)
                    )
                },
            )
    }
}

impl Drop for SoakBroker {
    fn drop(&mut self) {
        // Best effort: a panic in `drop` would replace the real failure.
        remove_container_with_volumes(&self.name);
    }
}

/// The directory id node `n` is formatted with.
///
/// Fixed rather than random so the `--initial-controllers` list every node is
/// given is the same string, built without a round of coordination.
/// The metadata directory id of node `n`: outside the 100 lowest ids, which
/// Kafka reserves and the broker refuses at startup.
fn directory_id(n: u32) -> uuid::Uuid { uuid::Uuid::from_u64_pair(1, u64::from(n)) }

/// What a diskless-quorum node adds to its configuration file: top-level keys,
/// the inter-broker listener entry, and whole tables.
#[derive(Debug, Clone, PartialEq, Eq)]
struct QuorumToml {
    root: String,
    listener: String,
    tables: String,
}

/// The configuration node `node_id` needs to vote in a diskless WAL quorum,
/// with its inter-broker listener published on host port `port` and its bearer
/// token at `token_path`.
///
/// Its peers dial the advertised address, which goes through the host gateway
/// as the client listener's does.
fn diskless_quorum_toml(node_id: u32, port: u16, token_path: &str) -> QuorumToml {
    QuorumToml {
        root: format!(
            "inter_broker_listener_name = \"{INTER_BROKER_LISTENER}\"\n\
             rack = \"rack-{node_id}\"\n"
        ),
        listener: format!(
            "\n\
             [[listeners]]\n\
             name = \"{INTER_BROKER_LISTENER}\"\n\
             bind_addr = \"0.0.0.0:9094\"\n\
             advertised = \"host.docker.internal:{port}\"\n\
             protocol = \"SaslPlaintext\"\n\
             \n\
             [listeners.sasl_config]\n\
             enabled_mechanisms = [\"OAUTHBEARER\"]\n"
        ),
        tables: format!(
            "\n\
             [inter_broker_credentials]\n\
             type = \"oauth-bearer\"\n\
             token_path = \"{token_path}\"\n\
             \n\
             [oauthbearer]\n\
             allow_unsecured = true\n"
        ),
    }
}

/// The unsecured JWS node `node_id` presents to its peers: an `alg: none`
/// header, a `sub` of the principal the WAL leader maps to that node id, and
/// an empty signature.
fn inter_broker_token(node_id: u32) -> String {
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#);
    let claims = URL_SAFE_NO_PAD.encode(
        serde_json::json!({
            "sub": broker_principal(NodeId(u64::from(node_id))),
            "exp": TOKEN_EXPIRY_UNIX_SECONDS,
        })
        .to_string(),
    );
    format!("{header}.{claims}.")
}

/// What every node of one cluster adds to the fixture's configuration and
/// container. See [`SoakCluster::configured`].
struct NodeExtras {
    root_toml: String,
    tables_toml: String,
    shared_mount: Option<(PathBuf, String)>,
}

/// Three brokers, formatted into one quorum and running.
pub(crate) struct SoakCluster {
    pub(crate) brokers: Vec<SoakBroker>,
}

#[bon::bon]
impl SoakCluster {
    /// Format and boot three nodes.
    pub(crate) fn start() -> Self { Self::configured().call() }

    /// Format and boot three nodes, each with the same additions.
    ///
    /// `root_toml` is top-level keys for every node's configuration file and
    /// `tables_toml` is whole tables appended after it. `shared_mount` is a
    /// host directory and the container path every node mounts it on, for a
    /// store that the three brokers share, such as a local remote tier.
    /// `diskless_quorum` gives every node the authenticated inter-broker
    /// listener and the distinct rack a diskless WAL quorum needs; see the
    /// module documentation.
    #[builder(finish_fn = call)]
    pub(crate) fn configured(
        #[builder(default)] root_toml: String,
        #[builder(default)] tables_toml: String,
        shared_mount: Option<(PathBuf, String)>,
        #[builder(default)] diskless_quorum: bool,
    ) -> Self {
        require_host_gateway();
        let extras = NodeExtras {
            root_toml,
            tables_toml,
            shared_mount,
        };
        let cluster_id = uuid::Uuid::new_v4().to_string();
        let brokers: Vec<SoakBroker> = (1..=3)
            .map(|node_id| SoakBroker {
                name: format!("krabka-soak-{}-{node_id}", std::process::id()),
                node_id,
                client_port: free_port(),
                controller_port: free_port(),
                metrics_port: free_port(),
                inter_broker_port: diskless_quorum.then(free_port),
                root: tempfile::TempDir::new().expect("host data directory"),
            })
            .collect();

        let voters = brokers
            .iter()
            .map(SoakBroker::voter_spec)
            .collect::<Vec<_>>()
            .join(",");
        // The controller endpoints again, without the directory ids: the
        // format step names the voter set the log is seeded with, and this
        // names the addresses a booting node dials before that set is
        // committed.
        let quorum = brokers
            .iter()
            .map(|broker| format!("\"{}@{}\"", broker.node_id, broker.controller_advertised()))
            .collect::<Vec<_>>()
            .join(",");
        for broker in &brokers {
            broker.format(&cluster_id, &voters);
            broker.write_config(&quorum, &extras);
        }
        for broker in &brokers {
            broker.run(&cluster_id, &extras);
        }
        Self { brokers }
    }

    /// Every broker's client address, for a bootstrap list.
    pub(crate) fn bootstrap(&self) -> String {
        self.brokers
            .iter()
            .map(SoakBroker::bootstrap)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Block until every broker answers its `/metrics` endpoint.
    ///
    /// The metrics server comes up with the broker, so this is a liveness probe
    /// that costs nothing extra and needs no Kafka client. Readiness -- a
    /// controller elected, a topic creatable -- is proved by the topic creation
    /// that follows.
    pub(crate) async fn wait_metrics_up(&self, client: &reqwest::Client) {
        let deadline = Instant::now() + READY_TIMEOUT;
        for broker in &self.brokers {
            let url = broker.metrics_url();
            loop {
                let refused = match client.get(&url).send().await {
                    Ok(response) if response.status().is_success() => break,
                    Ok(response) => format!("status {}", response.status()),
                    Err(error) => format!("{error}"),
                };
                assert!(
                    Instant::now() < deadline,
                    "{} never served /metrics within {READY_TIMEOUT:?}: {refused}\n{}",
                    broker.name,
                    broker.container_log()
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// `TOPIC_ALREADY_EXISTS`: what a retried `CreateTopics` gets when its first
/// attempt landed and only the answer was lost.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// How long a freshly booted cluster gets to accept its topics.
const CREATE_TOPICS_TIMEOUT: Duration = Duration::from_secs(180);

/// Create `topics` through `bootstrap`, retrying while a cluster that has just
/// booted is still electing: the first attempts are expected to fail on the
/// connection or on a missing controller. A topic that already exists counts
/// as created.
///
/// Returns the last answer when the cluster has not accepted every topic
/// within [`CREATE_TOPICS_TIMEOUT`].
pub(crate) async fn create_topics(
    bootstrap: &str,
    topics: &[CreatableTopic],
) -> Result<(), String> {
    let deadline = Instant::now() + CREATE_TOPICS_TIMEOUT;
    let mut last = String::new();
    while Instant::now() < deadline {
        if let Ok(admin) = Client::builder().bootstrap(bootstrap).build().await {
            match admin
                .send(CreateTopicsRequest {
                    topics: topics.to_vec(),
                    timeout_ms: 30_000,
                    ..Default::default()
                })
                .await
            {
                Ok(response)
                    if response.topics.iter().all(|topic| {
                        topic.error_code == 0 || topic.error_code == TOPIC_ALREADY_EXISTS
                    }) =>
                {
                    return Ok(());
                }
                Ok(response) => last = format!("{:?}", response.topics),
                Err(error) => last = format!("{error}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use {
        super::{
            HOST_GATEWAY, INTER_BROKER_LISTENER, diskless_quorum_toml, host_gateway_problem,
            inter_broker_token,
        },
        crate::support::configs::apply_node_file,
        assert2::check,
        krabka_broker::config::{BrokerConfig, InterBrokerCredentials},
        krabka_security::{ListenerProtocol, SaslMechanism, UnsecuredJwsValidator},
    };

    /// The node file's diskless-quorum parts, around the minimum that parses,
    /// with the token written where the file says it is.
    fn applied(node_id: u32, port: u16) -> BrokerConfig {
        let dir = tempfile::TempDir::new().expect("token directory");
        let token = dir.path().join("token");
        std::fs::write(&token, inter_broker_token(node_id)).expect("write the token");
        let toml = diskless_quorum_toml(node_id, port, &token.display().to_string());
        let text = format!(
            "broker_id = {node_id}\n{root}\n\
             [[listeners]]\n\
             name = \"PLAINTEXT\"\n\
             bind_addr = \"0.0.0.0:9092\"\n\
             advertised = \"host.docker.internal:19092\"\n\
             protocol = \"Plaintext\"\n\
             {listener}\n\
             [process]\n\
             roles = [\"controller\", \"broker\"]\n\
             {tables}",
            root = toml.root,
            listener = toml.listener,
            tables = toml.tables,
        );
        apply_node_file(&text)
    }

    // `OpaAuthorizer::new` captures the current tokio runtime handle, so the
    // apply step runs inside one, as it does in the binary.
    #[yare::parameterized(
        first = { 1, 30001 },
        third = { 3, 30003 },
    )]
    #[test_macro(tokio::test)]
    async fn a_diskless_quorum_node_dials_its_peers_authenticated(node_id: u32, port: u16) {
        let config = applied(node_id, port);
        check!(config.inter_broker_listener_name == INTER_BROKER_LISTENER);
        check!(config.rack.as_deref() == Some(format!("rack-{node_id}").as_str()));
        let inter_broker = config
            .listeners
            .iter()
            .find(|listener| listener.name == INTER_BROKER_LISTENER)
            .expect("an inter-broker listener");
        check!(inter_broker.protocol == ListenerProtocol::SaslPlaintext);
        check!(inter_broker.advertised == format!("host.docker.internal:{port}"));
        check!(inter_broker.sasl_mechanisms.as_deref() == Some(&[SaslMechanism::OAuthBearer][..]));
        check!(matches!(
            config.inter_broker_credentials,
            Some(InterBrokerCredentials::OAuthBearer { .. })
        ));
    }

    #[yare::parameterized(
        first = { 1, "broker-1" },
        third = { 3, "broker-3" },
    )]
    fn the_token_names_the_principal_a_wal_leader_maps_to_the_node(node_id: u32, principal: &str) {
        let outcome = UnsecuredJwsValidator::default()
            .validate(&inter_broker_token(node_id), 1_800_000_000_000)
            .expect("the development validator accepts the token");
        check!(outcome.principal.name == principal);
    }

    // A host without the `/etc/hosts` line is refused before any container
    // starts, with the fix in the message, rather than failing as a cluster
    // whose clients all see closed connections.
    #[yare::parameterized(
        loopback = { Ok(vec!["127.0.0.1:0".parse().unwrap()]), None },
        loopback_among_others = {
            Ok(vec!["10.0.0.5:0".parse().unwrap(), "[::1]:0".parse().unwrap()]),
            None
        },
        unresolved = {
            Err(std::io::Error::other("failed to lookup address information")),
            Some("does not resolve on this host")
        },
        elsewhere = { Ok(vec!["10.0.0.5:0".parse().unwrap()]), Some("not to loopback") },
    )]
    fn the_host_gateway_name_must_reach_loopback(
        resolved: std::io::Result<Vec<std::net::SocketAddr>>,
        problem: Option<&str>,
    ) {
        let found = host_gateway_problem(resolved);
        check!(found.is_some() == problem.is_some(), "{found:?}");
        let message = found.unwrap_or_default();
        let fix = format!("127.0.0.1 {HOST_GATEWAY}");
        check!(problem.is_none_or(|reason| message.contains(reason) && message.contains(&fix)));
    }
}
