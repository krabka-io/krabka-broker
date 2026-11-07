//! The driver the barrier scenarios share: a single-node cluster, and an
//! operator who runs the tool against it.
//!
//! Step definitions decide what to check. How the tool is driven, which flags
//! spell an intent and how its output is read back all live here, so a
//! scenario never names a flag and a change to the command line changes one
//! file.
//!
//! Every suite that declares `mod support;` gets the whole module, and not
//! every suite uses every helper.
#![allow(dead_code)]

use {
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    krabka_client_admin::{
        AdminClient, AdminClientLike as _, CreateTopicSpec, DeleteRecordsOp, TopicMutationOptions,
    },
    std::{
        collections::BTreeMap,
        slice,
        time::{Duration, Instant},
    },
    tokio::time,
};

/// The group name a scenario uses when the name does not matter.
pub const GROUP: &str = "orders-cut";

/// A running single-node broker and its data directory.
pub struct Cluster {
    handle: BrokerHandle,
    dir: tempfile::TempDir,
    bootstrap: String,
}

impl Cluster {
    /// Boot a single-node broker and wait until its barrier coordinator
    /// answers.
    ///
    /// `BrokerConfig::for_tests` asks for 50 `__barrier_state` partitions at
    /// replication factor 3, which one node cannot host, so the partition a
    /// group hashes to might never open. One partition at factor one is led
    /// here, which makes every scenario deterministic.
    pub async fn start() -> Self {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = BrokerConfig {
            barrier_state_num_partitions: 1,
            barrier_state_replication_factor: 1,
            ..BrokerConfig::for_tests(dir.path().to_path_buf())
        };
        let handle = Broker::start(config).await.expect("broker starts");
        let bootstrap = handle.listen_addr().to_string();
        let cluster = Self {
            handle,
            dir,
            bootstrap,
        };
        cluster.wait_for_coordinator().await;
        cluster
    }

    /// The address an operator bootstraps against.
    pub fn bootstrap(&self) -> &str { &self.bootstrap }

    /// An operator pointed at this cluster.
    pub fn operator(&self) -> Operator { Operator::new(&self.bootstrap) }

    /// Block until the coordinator answers, so a scenario never races the
    /// broker's startup. Describing every group is the cheapest request that
    /// reaches it.
    async fn wait_for_coordinator(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.operator().describe_all().await.exit == 0 {
                return;
            }
            assert2::assert!(
                Instant::now() < deadline,
                "the barrier coordinator never became available"
            );
            time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn admin(&self) -> AdminClient {
        AdminClient::connect(slice::from_ref(&self.bootstrap))
            .await
            .expect("admin connect")
    }

    /// Create a topic with `partitions` partitions at replication factor one.
    pub async fn create_topic(&self, topic: &str, partitions: i32) {
        let outcomes = self
            .admin()
            .await
            .create_topics(
                &[CreateTopicSpec {
                    name: topic.to_owned(),
                    partitions,
                    replicas: 1,
                    configs: BTreeMap::default(),
                    replica_assignments: BTreeMap::new(),
                }],
                TopicMutationOptions::with_timeout(krabka_units::secs(10)),
            )
            .await
            .expect("create topic");
        assert2::assert!(
            outcomes.iter().all(|outcome| outcome.error.is_none()),
            "{outcomes:?}"
        );
    }

    /// Delete a topic, and wait until the cluster no longer knows it, so the
    /// same name can be created again at once.
    pub async fn delete_topic(&self, topic: &str) {
        let mut admin = self.admin().await;
        let outcomes = admin
            .delete_topics(
                &[topic],
                TopicMutationOptions::with_timeout(krabka_units::secs(10)),
            )
            .await
            .expect("delete topic");
        assert2::assert!(
            outcomes.iter().all(|outcome| outcome.error.is_none()),
            "{outcomes:?}"
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let known = admin
                .metadata(&[])
                .await
                .expect("metadata")
                .topics
                .iter()
                .any(|entry| entry.name == topic);
            if !known {
                return;
            }
            assert2::assert!(Instant::now() < deadline, "{topic} was never deleted");
            time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Delete every record of one partition below `offset`, as `DeleteRecords`
    /// does, so the log starts at `offset` from then on.
    pub async fn delete_records_below(&self, topic: &str, partition: i32, offset: i64) {
        let outcomes = self
            .admin()
            .await
            .delete_records(
                &[DeleteRecordsOp {
                    topic: topic.to_owned(),
                    partition,
                    offset,
                }],
                krabka_units::secs(10),
            )
            .await
            .expect("delete records");
        assert2::assert!(
            outcomes.iter().all(|outcome| outcome.error_code == 0),
            "{outcomes:?}"
        );
    }
}

/// The lazily started cluster of one scenario.
///
/// A fixture cannot await, and the broker's tasks have to run on the
/// scenario's own runtime, so the first step that needs the cluster starts it.
#[derive(Default)]
pub struct LazyCluster(Option<Cluster>);

impl LazyCluster {
    /// The scenario's cluster, started the first time.
    pub async fn get(&mut self) -> &Cluster {
        let cluster = match self.0.take() {
            Some(cluster) => cluster,
            None => Cluster::start().await,
        };
        self.0.insert(cluster)
    }
}

/// What one run of the tool printed, and how it ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transcript {
    /// The exit code. A command line the tool refuses to parse ends on the
    /// code `clap` exits with.
    pub exit: i32,
    /// Every line written for what succeeded.
    pub stdout: String,
    /// Every line written for what did not, or the parser's complaint.
    pub stderr: String,
    /// Whether the tool refused the command line before sending anything.
    pub command_line_refused: bool,
}

/// The variables an operator's shell exports, as the tool reads them.
///
/// A scenario cannot set a variable in its own process, because that is
/// `unsafe` in edition 2024, so it hands the tool this instead.
#[derive(Debug, Clone, Default)]
pub struct Environment {
    /// Each exported variable's name and value.
    pub variables: BTreeMap<String, String>,
}

impl Environment {
    /// Export `name` with `value`.
    pub fn export(&mut self, name: &str, value: &str) {
        self.variables.insert(name.to_owned(), value.to_owned());
    }

    /// What `name` holds, if the shell exports it.
    pub fn get(&self, name: &str) -> Option<String> { self.variables.get(name).cloned() }
}

/// Run the tool in process with `line` as its whole command line, reading
/// its environment variables from `environment`.
pub async fn run_line(line: Vec<String>, environment: &Environment) -> Transcript {
    let cli = match krabka_barrier::Cli::try_parse_with_env(line, |name| environment.get(name)) {
        Ok(cli) => cli,
        Err(error) => {
            return Transcript {
                exit: error.exit_code(),
                stdout: String::new(),
                stderr: error.to_string(),
                command_line_refused: true,
            };
        }
    };
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let exit =
        krabka_barrier::run_with(cli, &mut krabka_barrier::Console::new(&mut out, &mut err)).await;
    Transcript {
        exit,
        stdout: String::from_utf8(out).expect("utf-8 stdout"),
        stderr: String::from_utf8(err).expect("utf-8 stderr"),
        command_line_refused: false,
    }
}

/// One row of a cut: a topic, a partition, and the offset the cut names.
pub type CutRow = (String, i32, i64);

/// One offset a verify found the log does not back, and why.
pub type MismatchRow = (String, i32, i64, String);

/// The lines of `text` that hold exactly `N` tab-separated fields.
fn tab_rows<const N: usize>(text: &str) -> Vec<[&str; N]> {
    text.lines()
        .filter_map(|line| <[&str; N]>::try_from(line.split('\t').collect::<Vec<_>>()).ok())
        .collect()
}

impl Transcript {
    /// The epoch line of a trigger.
    pub fn epoch(&self) -> i64 {
        self.stdout
            .lines()
            .find_map(|line| line.strip_prefix("epoch "))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|epoch| epoch.parse().ok())
            .unwrap_or_else(|| panic!("no epoch line in {self:?}"))
    }

    /// The cut rows a trigger printed, in the order it printed them.
    pub fn cut_rows(&self) -> Vec<CutRow> {
        tab_rows::<3>(&self.stdout)
            .into_iter()
            .filter_map(|[topic, partition, offset]| {
                Some((
                    topic.to_owned(),
                    partition.parse().ok()?,
                    offset.parse().ok()?,
                ))
            })
            .collect()
    }

    /// The rows a verify wrote for offsets the log does not back: topic,
    /// partition, offset and reason.
    pub fn mismatch_rows(&self) -> Vec<MismatchRow> {
        tab_rows::<4>(&self.stderr)
            .into_iter()
            .filter_map(|[topic, partition, offset, reason]| {
                Some((
                    topic.to_owned(),
                    partition.parse().ok()?,
                    offset.parse().ok()?,
                    reason.to_owned(),
                ))
            })
            .collect()
    }

    /// The epochs a listing printed, in the order it printed them.
    pub fn listed_epochs(&self) -> Vec<i64> {
        self.stdout
            .lines()
            .filter_map(|line| line.strip_prefix("epoch "))
            .filter_map(|rest| rest.split_whitespace().next()?.parse().ok())
            .collect()
    }

    /// The value a description prints beside `label`, such as `topics`.
    pub fn described(&self, label: &str) -> Option<String> {
        self.stdout.lines().find_map(|line| {
            let rest = line.strip_prefix(label)?;
            rest.starts_with("  ").then(|| rest.trim().to_owned())
        })
    }
}

/// How a group is defined. Every field the scenario does not set is left to
/// the tool's default.
#[derive(bon::Builder)]
pub struct GroupDefinition {
    #[builder(into, default = GROUP.to_owned())]
    pub group: String,
    #[builder(default)]
    pub topics: Vec<String>,
    /// The cadence as the operator writes it, such as `5m`.
    #[builder(into)]
    pub cadence: Option<String>,
    pub retained_cuts: Option<i32>,
}

/// Which cuts a listing asks for. Every field the scenario does not set is
/// left to the tool's default.
#[derive(bon::Builder)]
pub struct CutQuery {
    #[builder(into, default = GROUP.to_owned())]
    pub group: String,
    pub from_epoch: Option<i64>,
    pub how_many: Option<i32>,
}

/// An operator who runs the tool against one bootstrap address.
pub struct Operator {
    bootstrap: String,
}

impl Operator {
    /// An operator pointed at `bootstrap`.
    pub fn new(bootstrap: &str) -> Self {
        Self {
            bootstrap: bootstrap.to_owned(),
        }
    }

    /// Run the tool with `args` after the bootstrap flag, in process.
    pub async fn run(&self, args: &[String]) -> Transcript {
        let mut line = vec![
            "krabka-barrier".to_owned(),
            "--bootstrap-server".to_owned(),
            self.bootstrap.clone(),
        ];
        line.extend_from_slice(args);
        run_line(line, &Environment::default()).await
    }

    /// Define a group, or redefine one that is live.
    /// Define a group, or redefine one that is live.
    pub async fn define(&self, definition: GroupDefinition) -> Transcript {
        let mut args = vec!["define".to_owned(), "--group".to_owned(), definition.group];
        for topic in definition.topics {
            args.extend(["--topic".to_owned(), topic]);
        }
        if let Some(cadence) = definition.cadence {
            args.extend(["--interval".to_owned(), cadence]);
        }
        if let Some(retained) = definition.retained_cuts {
            args.extend(["--retained-cuts".to_owned(), retained.to_string()]);
        }
        self.run(&args).await
    }

    /// Delete a group.
    pub async fn delete(&self, group: &str) -> Transcript {
        self.run(&["delete".to_owned(), "--group".to_owned(), group.to_owned()])
            .await
    }

    /// Describe one group.
    pub async fn describe(&self, group: &str) -> Transcript {
        self.run(&[
            "describe".to_owned(),
            "--group".to_owned(),
            group.to_owned(),
        ])
        .await
    }

    /// Describe every group.
    pub async fn describe_all(&self) -> Transcript { self.run(&["describe".to_owned()]).await }

    /// Trigger a cut of a group now.
    pub async fn trigger(&self, group: &str) -> Transcript {
        self.run(&["trigger".to_owned(), "--group".to_owned(), group.to_owned()])
            .await
    }

    /// List the cuts a group retains.
    pub async fn list(&self, query: CutQuery) -> Transcript {
        let mut args = vec!["list".to_owned(), "--group".to_owned(), query.group];
        if let Some(from) = query.from_epoch {
            args.extend(["--from-epoch".to_owned(), from.to_string()]);
        }
        if let Some(how_many) = query.how_many {
            args.extend(["--max-results".to_owned(), how_many.to_string()]);
        }
        self.run(&args).await
    }

    /// Verify one cut of a group against the log.
    pub async fn verify(&self, group: &str, epoch: i64) -> Transcript {
        self.run(&[
            "verify".to_owned(),
            "--group".to_owned(),
            group.to_owned(),
            "--epoch".to_owned(),
            epoch.to_string(),
        ])
        .await
    }
}
