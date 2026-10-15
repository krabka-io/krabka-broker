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
//! [`process_kill_durability`]: ../process_kill_durability.rs

pub(crate) use crate::support::docker;
use {
    crate::support::{
        free_port,
        load_test::{Client, CreatableTopic, CreateTopicsRequest},
        remove_container_with_volumes,
        storage::host_directory_user,
    },
    assert2::assert,
    std::{
        path::{Path, PathBuf},
        process::Command,
        time::{Duration, Instant},
    },
};

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
        let toml = format!(
            "broker_id = {id}\n\
             log_dir = \"{CONTAINER_LOG_DIR}\"\n\
             controller_quorum_voters = [{voters}]\n\
             {root}\
             \n\
             [[listeners]]\n\
             name = \"PLAINTEXT\"\n\
             bind_addr = \"0.0.0.0:9092\"\n\
             advertised = \"host.docker.internal:{client}\"\n\
             protocol = \"Plaintext\"\n\
             \n\
             [process]\n\
             roles = [\"controller\", \"broker\"]\n\
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
    #[builder(finish_fn = call)]
    pub(crate) fn configured(
        #[builder(default)] root_toml: String,
        #[builder(default)] tables_toml: String,
        shared_mount: Option<(PathBuf, String)>,
    ) -> Self {
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
