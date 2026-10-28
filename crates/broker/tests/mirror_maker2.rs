//! `MirrorMaker 2` (KIP-382), out of the stock Apache image, mirroring a Kafka
//! cluster onto krabka.
//!
//! # Why this suite exists
//!
//! Nobody adopts a Kafka-compatible broker greenfield. They adopt it by
//! mirroring the cluster they already run onto it and cutting their consumers
//! over, and MM2 is the one tool the ecosystem uses for that. The migration
//! path onto krabka is therefore exactly as good as one MM2 run against it,
//! and this suite is that run: the stock `connect-mirror-maker.sh` of
//! `mirror.gcr.io/apache/kafka:4.3.1`, with a stock broker of the same release
//! as the source cluster and krabka as the target.
//!
//! `tests/jvm_connect_distributed.rs` already holds the Connect half of this
//! against that image -- the three `KafkaBasedLog` stores, the idempotent
//! producer on the startup path and the `connect` classic-protocol group. What
//! MM2 adds is the part a migration turns on:
//!
//! - the `heartbeats`, `<source>.checkpoints.internal` and
//!   `mm2-offset-syncs.<alias>.internal` topics, all three created through
//!   `MirrorUtils.createSinglePartitionCompactedTopic`;
//! - `Admin.listConsumerGroupOffsets` on the source and
//!   `alterConsumerGroupOffsets` on the target, which together are offset
//!   translation -- the one place a 99%-compatible broker silently loses or
//!   replays data, because a consumer cut over to the wrong offset either
//!   skips records or reads them twice and nothing reports an error;
//! - `describeConfigs` and `incrementalAlterConfigs` for `sync.topic.configs`;
//! - `describeAcls` and `createAcls` for `sync.topic.acls`.
//!
//! `docs/operations/migrate-from-kafka.md` is the procedure this run stands
//! behind.
//!
//! # How the two clusters are addressed
//!
//! Both are named by the Docker bridge gateway, which is the one address that
//! is routable from the host AND from every container on the default bridge:
//! `tests/jvm_kip320_divergence/docker.rs` established that shape for its
//! mixed cluster, for the same reason. The source broker publishes its
//! listener on a host port and advertises `<gateway>:<port>`; krabka listens
//! on the host and is told to advertise `<gateway>:<port>` rather than the
//! `host.docker.internal` name the rest of the JVM suites use. MM2 and every
//! admin tool then run in containers that name the two clusters exactly as the
//! test does. `--network host` is not used, for the reason
//! `tests/jvm_acceptance/mod.rs` writes down.
//!
//! # The MM2 settings this suite pins, and why none is a workaround
//!
//! - `replication.factor`, `checkpoints.topic.replication.factor`,
//!   `heartbeats.topic.replication.factor` and
//!   `offset-syncs.topic.replication.factor` are 1. MM2 defaults them to 2 and
//!   3, which a single-node target cannot open. That is the size of the
//!   cluster, not anything about krabka.
//! - `offset-syncs.topic.location=target` puts the offset-syncs topic on
//!   krabka. MM2 defaults it to the source, where it would be the stock
//!   broker's business and this suite would learn nothing from it.
//! - `offset.lag.max=0` makes MM2 emit an offset sync for every record. Under
//!   the default of 100 the sparse `OffsetSyncStore` translates
//!   conservatively -- an upstream offset with no exact sync becomes one past
//!   the nearest earlier sync's downstream offset -- so a translated position
//!   would be a range rather than a number, and the case below asserts a
//!   number.
//! - `sync.group.offsets.enabled=true`. MM2 defaults it off; it is the feature
//!   under test.
//!
//! `sync.topic.acls.enabled` and `sync.topic.configs.enabled` are left at
//! their defaults, both true. The ACL default matters here: the milestone that
//! landed this suite also made krabka answer the ACL RPCs `SECURITY_DISABLED`
//! (54) under its default `allow_all` authorizer, which is what a stock Kafka
//! with no `authorizer.class.name` answers. Because the source broker here
//! also has no authorizer, MM2's ACL sync stops at the source --
//! `MirrorSourceConnector` catches the source's `SecurityDisabledException`
//! from `describeAcls` and skips the sync -- so the target-side `createAcls`
//! is not reached from MM2 in this arrangement. The case therefore does two
//! things: it holds MM2's own log to having raised no ACL-sync failure, and it
//! makes the target-side call itself with `kafka-acls` and asserts krabka's
//! answer is the `SecurityDisabledException` a stock target would give. The
//! migration guide says what an operator whose source cluster *does* have an
//! authorizer sees.
//!
//! # What a failure here prints
//!
//! Every wait announces itself and its result as `KRABKA[test]`, so the log
//! says which of them the case reached without anyone waiting for it to fail,
//! and each one names itself again in its own failure. A wait also ends the
//! moment MM2's container stops, because every one of them is a wait on MM2
//! doing something.
//!
//! No container's log is inlined whole into a message. `log_excerpt` carries
//! the `ERROR`/`WARN` lines and the tail, a few KB, and `save_log` writes the
//! rest to `TEST_UNDECLARED_OUTPUTS_DIR`. The first run of this suite inlined
//! MM2's whole log instead: GitHub's job-log API keeps roughly the last 470 KB
//! of a job, one dump filled it, and the suites sharded beside this one lost
//! their diagnostics along with this one's.
//!
//! The container-driven case is gated `#[ignore = "requires Docker"]`; the
//! Bazel `docker` lane runs it. The parsing this suite reads the JVM tools
//! with is covered by ordinary tests that need no daemon.
#![cfg(test)]

mod jvm_acceptance;
mod support;

use {
    assert2::assert,
    jvm_acceptance::{host_port, start_host_broker_with},
    std::{
        env, fs,
        path::PathBuf,
        process::{Command, Output, Stdio},
        time::Duration,
    },
    support::bridge_gateway,
    tokio::{time, time::Instant},
};

/// The release both clusters and every tool come from: the source broker is
/// this image, MM2 is its `connect-mirror-maker.sh`, and the admin tools aimed
/// at krabka are its tools.
const IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// Where the tools live inside [`IMAGE`]. They are not on `PATH`.
const BIN: &str = "/opt/kafka/bin";

/// The MM2 cluster aliases. `DefaultReplicationPolicy` prefixes a mirrored
/// topic with the source alias and a dot, so these are part of every topic
/// name the target side asserts on.
const SOURCE_ALIAS: &str = "source";
const TARGET_ALIAS: &str = "target";

/// The topic the migration moves. Its name matches the `topics` filter MM2 is
/// given, which is what keeps the filter off `__consumer_offsets` and off
/// MM2's own internal topics.
const TOPIC: &str = "krabka-mm2-orders";

/// The consumer group whose position has to survive the cutover.
const GROUP: &str = "krabka-mm2-riders";

/// How many records the source topic holds.
const RECORDS: usize = 10;

/// Where the source group's committed offset is pinned before MM2 starts, and
/// therefore the target offset the translated position has to land on.
const COMMITTED: usize = 4;

/// The `retention.ms` set on the source topic after mirroring is established.
/// `sync.topic.configs` has to carry it to the target copy. It is not a Kafka
/// default, so reading it back on the target cannot be a default echoed at us.
const RETENTION_MS: &str = "1234000";

/// How long the source broker gets to answer its own tools.
const SOURCE_BUDGET: Duration = Duration::from_secs(180);

/// How long MM2 gets to create the target topic and carry every record over.
/// It boots a Connect worker, creates and replays three internal stores and
/// then starts its connectors, all on a cold JVM.
const MIRROR_BUDGET: Duration = Duration::from_secs(300);

/// How long each later MM2 effect gets, once the run is known to be up.
const EFFECT_BUDGET: Duration = Duration::from_secs(180);

/// The pause between one poll and the next.
const POLL_GAP: Duration = Duration::from_secs(2);

/// How many of a container's last log lines a failure message may carry.
///
/// A JVM container's log is tens of thousands of lines. Inlining one whole
/// into an assertion message cost more than this suite's own diagnosis: the
/// GitHub job-log API hands back roughly the last 470 KB of a job, and one
/// dump filled all of it, taking the `sarama_conformance` evidence in the same
/// Bazel shard with it. Nothing here inlines a container's output unbounded
/// again; [`log_excerpt`] is a few KB and [`save_log`] keeps the rest.
const LOG_TAIL_LINES: usize = 40;

/// How many of a log's `ERROR`, `WARN` and exception lines the excerpt carries.
///
/// The tail alone does not explain a JVM that stopped on purpose. MM2's
/// shutdown writes several hundred INFO lines -- one per connector, task,
/// producer, consumer, metrics reporter and `KafkaBasedLog` it closes -- after
/// whatever made it stop, so the cause is pushed out of any tail short enough
/// to print. These lines are where it survives.
const LOG_PROBLEM_LINES: usize = 40;

// ------------------------------------------------------------------ records

/// One record as the console tools render it: the header block, the key and
/// the value, which is every part of it a migration is allowed to change.
///
/// Headers are the part that is easy to lose. A converter that drops them, or
/// a broker that does not carry them through the record batch, shows up here
/// as `NO_HEADERS` -- what `kafka-console-consumer` prints for a record with
/// none -- rather than as a difference nobody looks at.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    /// `<k>:<v>,<k>:<v>`, the console tools' rendering of the header block.
    headers: String,
    key: String,
    value: String,
}

impl Record {
    /// The tab-separated `<headers> <key> <value>` line both console tools
    /// speak.
    ///
    /// `kafka-console-producer` reads it under `parse.headers=true` and
    /// `parse.key=true`, whose delimiters both default to a tab, and
    /// `kafka-console-consumer` writes it under `print.headers=true` and
    /// `print.key=true`, whose separator defaults to the same tab.
    fn line(&self) -> String { format!("{}\t{}\t{}", self.headers, self.key, self.value) }

    /// One consumed line back into a record, or `None` when the line is not a
    /// record at all -- the console consumer's trailing
    /// `Processed a total of N messages` among them.
    fn parse(line: &str) -> Option<Self> {
        let mut fields = line.split('\t');
        let headers = fields.next()?.to_owned();
        let key = fields.next()?.to_owned();
        let value = fields.next()?.to_owned();
        if fields.next().is_some() {
            return None;
        }
        Some(Self {
            headers,
            key,
            value,
        })
    }
}

/// The records the source topic is seeded with.
///
/// Every one carries two headers, so a surviving header block cannot be the
/// empty one, and the `order` header repeats the value, so a record that
/// arrives carrying another record's headers is caught too.
fn seeded_records() -> Vec<Record> {
    (0..RECORDS)
        .map(|index| Record {
            headers: format!("order:{index},origin:{SOURCE_ALIAS}"),
            key: format!("key-{index}"),
            value: format!("order-{index}"),
        })
        .collect()
}

// -------------------------------------------------------------- MM2 topics

/// The part MM2 plays for one of its internal topics on the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Role {
    /// `<source>.checkpoints.internal`, the translated offsets MM2 emits.
    Checkpoints,
    /// `heartbeats`, which MM2 emits into the target on its own interval.
    Heartbeats,
    /// `mm2-offset-syncs.<alias>.internal`, the upstream-to-downstream offset
    /// pairs translation is computed from.
    OffsetSyncs,
}

/// A topic on the target and the part MM2 plays for it.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RoleTopic {
    role: Role,
    name: String,
}

/// Every role, in the order [`Role`]'s own `Ord` puts them, which is the order
/// a sorted reading of the target's topics comes back in.
const ROLES: [Role; 3] = [Role::Checkpoints, Role::Heartbeats, Role::OffsetSyncs];

/// Which of MM2's internal topics `name` is, if it is one of them.
///
/// The names are matched by shape rather than spelled out, because two of the
/// three carry a cluster alias in the middle and the third does not: a shape
/// says which topic MM2 meant without this suite encoding Kafka's naming rule
/// as a constant a later release could move.
fn role_of(name: &str) -> Option<Role> {
    if name == "heartbeats" {
        return Some(Role::Heartbeats);
    }
    if name.ends_with(".checkpoints.internal") {
        return Some(Role::Checkpoints);
    }
    if name.starts_with("mm2-offset-syncs.") && name.ends_with(".internal") {
        return Some(Role::OffsetSyncs);
    }
    None
}

/// One internal topic as the target holds it.
///
/// `MirrorUtils.createSinglePartitionCompactedTopic` creates all three, so all
/// three are one partition at `cleanup.policy=compact`. A target that opened
/// them at its own defaults instead is one MM2 would keep re-reading from the
/// wrong place after every restart.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InternalTopic {
    role: Role,
    partitions: u32,
    /// The `cleanup.policy` the topic really carries, as `kafka-configs`
    /// prints it. Only explicitly-set configs are printed, so a value here is
    /// MM2's create-time config having survived rather than a default.
    cleanup_policy: String,
}

// ------------------------------------------------------------ docker plumbing

/// The Docker bridge gateway, as an address the host and every container on
/// that bridge can both reach.
///
/// `host.docker.internal` is container-only, and this suite needs one way of
/// naming two clusters that are addressed from both sides.
/// `tests/jvm_kip320_divergence/docker.rs` reads the same pair for the same
/// reason, fallback included: a daemon inside a Firecracker microVM leaves
/// `Gateway` empty while still reporting `Subnet`, and Docker gives the
/// subnet's first address to the bridge itself.
fn docker_bridge_gateway() -> String {
    let out = Command::new("docker")
        .args([
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{(index .IPAM.Config 0).Gateway}}|{{(index .IPAM.Config 0).Subnet}}",
        ])
        .output()
        .expect("spawn docker network inspect");
    assert!(
        out.status.success(),
        "docker network inspect bridge failed: {}",
        String::from_utf8_lossy(&out.stderr),
    );
    let rendered = String::from_utf8_lossy(&out.stdout).into_owned();
    bridge_gateway(&rendered).unwrap_or_else(|| panic!("no bridge gateway in {rendered:?}"))
}

/// Run `<tool>.sh <args>` from a throwaway container on the default bridge and
/// hand back what it did, without asserting that it succeeded.
fn tool_allowing_failure(tool_name: &str, args: &[&str]) -> Output {
    tool_with_stdin(tool_name, args, None)
}

/// [`tool_allowing_failure`], asserting the tool exited zero.
fn tool(tool_name: &str, args: &[&str]) -> Output {
    let out = tool_allowing_failure(tool_name, args);
    assert!(
        out.status.success(),
        "{tool_name} {args:?} failed:\n{}",
        both_streams(&out),
    );
    out
}

/// [`tool_allowing_failure`] with text fed to the tool's standard input.
fn tool_with_stdin(tool_name: &str, args: &[&str], stdin: Option<&str>) -> Output {
    let mut command = Command::new("docker");
    command.args(["run", "--rm"]);
    if stdin.is_some() {
        command.arg("-i");
    }
    command
        .arg(IMAGE)
        .arg(format!("{BIN}/{tool_name}.sh"))
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = support::docker_output(&mut command, stdin, "write to the tool's stdin");
    eprintln!(
        "KRABKA[test] {tool_name} {args:?} status={}\n{}",
        out.status,
        both_streams(&out),
    );
    out
}

/// A finished tool's stdout followed by its stderr, as one text. The JVM tools
/// split a failure across the two in ways that differ per tool.
fn both_streams(out: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    )
}

/// Everything a container has printed.
///
/// Only two callers want this whole: [`container_excerpt`], which bounds it
/// before anything prints it, and the ACL assertion, which searches it for a
/// line and prints nothing.
fn container_logs(name: &str) -> String {
    let out = Command::new("docker")
        .args(["logs", name])
        .output()
        .expect("spawn docker logs");
    both_streams(&out)
}

/// What a container has printed, cut down to something a failure message can
/// carry, with the whole log written out and named.
fn container_excerpt(name: &str) -> String {
    let full = container_logs(name);
    format!(
        "{name} log ({}):\n{}",
        save_log(name, &full),
        log_excerpt(&full)
    )
}

/// Write one container's whole log where a CI run can pick it up, and say
/// where it went.
///
/// Bazel stages `TEST_UNDECLARED_OUTPUTS_DIR` into the test's outputs, which is
/// where a log too big to print belongs. Under Cargo there is no such
/// directory; the temporary one still beats losing it.
fn save_log(name: &str, text: &str) -> String {
    let dir = env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(env::temp_dir, PathBuf::from);
    let path = dir.join(format!("{name}.log"));
    match fs::write(&path, text) {
        Ok(()) => format!("whole log at {}", path.display()),
        Err(error) => format!("whole log not written to {}: {error}", path.display()),
    }
}

/// The two parts of a log a failure is explained by: the lines that reported a
/// problem, and the last lines before it stopped.
fn log_excerpt(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let problems: Vec<&str> = lines
        .iter()
        .copied()
        .filter(|line| is_problem_line(line))
        .collect();
    let kept = last_lines(&problems, LOG_PROBLEM_LINES);
    let tail = last_lines(&lines, LOG_TAIL_LINES);
    format!(
        "{} lines, of which {} reported a problem; the last {} of those:\n{}\nthe last {} lines:\n{}",
        lines.len(),
        problems.len(),
        kept.len(),
        kept.join("\n"),
        tail.len(),
        tail.join("\n"),
    )
}

/// Whether one log line is worth keeping whatever else is dropped.
///
/// The JVM's log4j layout puts the level between the timestamp and the
/// message, and a stack trace's first line ends in the exception's own name,
/// so a substring of each is what selects them.
fn is_problem_line(line: &str) -> bool {
    line.contains("ERROR") || line.contains("WARN") || line.contains("Exception")
}

/// The last `keep` of `lines`, or all of them when there are fewer.
fn last_lines<'a>(lines: &[&'a str], keep: usize) -> Vec<&'a str> {
    lines[lines.len().saturating_sub(keep)..].to_vec()
}

/// Whether a container is still running, and what it exited with if not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContainerState {
    running: bool,
    exit_code: i64,
}

/// Ask the daemon whether `name` is still running.
fn container_state(name: &str) -> ContainerState {
    let out = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.State.Running}}|{{.State.ExitCode}}",
            name,
        ])
        .output()
        .expect("spawn docker inspect");
    let rendered = String::from_utf8_lossy(&out.stdout).into_owned();
    parse_container_state(&rendered).unwrap_or_else(|| {
        panic!(
            "no state for {name} in {rendered:?}:\n{}",
            both_streams(&out)
        )
    })
}

/// The state out of the `Running|ExitCode` pair `docker inspect` renders.
fn parse_container_state(rendered: &str) -> Option<ContainerState> {
    let (running, exit_code) = rendered.trim().split_once('|')?;
    Some(ContainerState {
        running: running.parse().ok()?,
        exit_code: exit_code.parse().ok()?,
    })
}

// ------------------------------------------------------------- source cluster

/// The cluster being migrated away from: one stock `KRaft` broker of [`IMAGE`]
/// in combined mode, with its listener published on a host port.
struct SourceKafka {
    container: String,
    /// `<gateway>:<port>`, what both the tools and MM2 bootstrap against.
    bootstrap: String,
}

impl Drop for SourceKafka {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .output();
    }
}

impl SourceKafka {
    /// Boot the source broker and return once its own tools reach it.
    async fn start(gateway: &str) -> Self {
        let container = support::unique_container_name("krabka-mm2-source");
        let port = support::free_port();
        let bootstrap = format!("{gateway}:{port}");
        // A single-partition coordinator topic at replication factor one and
        // no initial rebalance delay: one node cannot open the 50-partition
        // default at factor three, and the group below has to form promptly.
        let env = [
            "KAFKA_NODE_ID=1".to_owned(),
            "KAFKA_PROCESS_ROLES=broker,controller".to_owned(),
            "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093".to_owned(),
            format!("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{bootstrap}"),
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER".to_owned(),
            "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT".to_owned(),
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
                .to_owned(),
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093".to_owned(),
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1".to_owned(),
            "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS=1".to_owned(),
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0".to_owned(),
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1".to_owned(),
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1".to_owned(),
            "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk".to_owned(),
        ];
        let mut command = Command::new("docker");
        command.args(["run", "-d", "--name", &container, "-p"]);
        command.arg(format!("{port}:9092"));
        for entry in &env {
            command.arg("-e").arg(entry);
        }
        command.arg(IMAGE);
        let out = command.output().expect("spawn docker run -d");
        assert!(
            out.status.success(),
            "starting the source Kafka failed: {}",
            String::from_utf8_lossy(&out.stderr),
        );

        let source = Self {
            container,
            bootstrap,
        };
        source.wait_ready().await;
        source
    }

    /// Poll `kafka-topics --list` until the source broker answers it.
    async fn wait_ready(&self) {
        let start = Instant::now();
        let deadline = start + SOURCE_BUDGET;
        eprintln!(
            "KRABKA[test] waiting for: the source Kafka answers on {} (up to {SOURCE_BUDGET:?})",
            self.bootstrap,
        );
        loop {
            let out = tool_allowing_failure("kafka-topics", &[
                "--bootstrap-server",
                &self.bootstrap,
                "--list",
            ]);
            if out.status.success() {
                eprintln!(
                    "KRABKA[test] reached: the source Kafka answers, after {:.1}s",
                    start.elapsed().as_secs_f64(),
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the source Kafka did not answer within {SOURCE_BUDGET:?}; {}",
                container_excerpt(&self.container),
            );
            // intentional: a JVM broker's readiness is its own internal state,
            // and asking its tools is the only observation there is.
            time::sleep(POLL_GAP).await;
        }
    }
}

// ------------------------------------------------------------- MirrorMaker 2

/// The stock `connect-mirror-maker.sh` in a container of its own.
struct MirrorMaker {
    container: String,
}

impl Drop for MirrorMaker {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .output();
    }
}

impl MirrorMaker {
    /// Start MM2 against the two clusters. It returns once `docker run` has
    /// accepted the container; the JVM inside is still booting.
    ///
    /// `target->source.emit.heartbeats.enabled=false` is what keeps the run to
    /// the one flow this suite is about. `enabled=false` alone does not:
    /// `MirrorMakerConfig.clusterPairs` keeps a pair whose flow is disabled as
    /// long as heartbeats are on for it, which they are by default, so the
    /// first run of this suite carried a second Connect worker
    /// (`clientId=target->source, groupId=target-mm2`) with its own three
    /// `KafkaBasedLog` stores and its own heartbeat connector, all of them
    /// against the source cluster. Nothing asserted below reads any of it.
    ///
    /// The properties are written by the container's own entrypoint rather
    /// than bind-mounted: the image runs as a non-root user, and a file under
    /// a `tempfile` directory is `0700` on the host and unreadable inside.
    fn start(source: &str, target: &str) -> Self {
        let container = support::unique_container_name("krabka-mm2");
        let entry = format!(
            "set -e\n\
             cat > /tmp/mm2.properties <<'PROPS'\n\
             clusters={SOURCE_ALIAS},{TARGET_ALIAS}\n\
             {SOURCE_ALIAS}.bootstrap.servers={source}\n\
             {TARGET_ALIAS}.bootstrap.servers={target}\n\
             {SOURCE_ALIAS}->{TARGET_ALIAS}.enabled=true\n\
             {TARGET_ALIAS}->{SOURCE_ALIAS}.enabled=false\n\
             {TARGET_ALIAS}->{SOURCE_ALIAS}.emit.heartbeats.enabled=false\n\
             {SOURCE_ALIAS}->{TARGET_ALIAS}.topics=krabka-mm2-.*\n\
             {SOURCE_ALIAS}->{TARGET_ALIAS}.groups=krabka-mm2-.*\n\
             replication.factor=1\n\
             checkpoints.topic.replication.factor=1\n\
             heartbeats.topic.replication.factor=1\n\
             offset-syncs.topic.replication.factor=1\n\
             offset-syncs.topic.location={TARGET_ALIAS}\n\
             offset.lag.max=0\n\
             emit.heartbeats.interval.seconds=1\n\
             emit.checkpoints.interval.seconds=1\n\
             sync.group.offsets.enabled=true\n\
             sync.group.offsets.interval.seconds=1\n\
             sync.topic.configs.interval.seconds=1\n\
             refresh.topics.interval.seconds=1\n\
             refresh.groups.interval.seconds=1\n\
             {TARGET_ALIAS}.config.storage.replication.factor=1\n\
             {TARGET_ALIAS}.offset.storage.replication.factor=1\n\
             {TARGET_ALIAS}.status.storage.replication.factor=1\n\
             {TARGET_ALIAS}.offset.storage.partitions=1\n\
             {TARGET_ALIAS}.status.storage.partitions=1\n\
             PROPS\n\
             exec {BIN}/connect-mirror-maker.sh /tmp/mm2.properties\n"
        );
        let out = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &container,
                "--entrypoint",
                "bash",
                IMAGE,
                "-c",
                &entry,
            ])
            .output()
            .expect("spawn docker run -d");
        assert!(
            out.status.success(),
            "starting MM2 failed: {}",
            String::from_utf8_lossy(&out.stderr),
        );
        eprintln!("KRABKA[test] MM2 started in {container}, mirroring {source} onto {target}");
        Self { container }
    }

    /// Everything MM2 has printed, for the one assertion that searches it
    /// rather than printing it.
    fn logs(&self) -> String { container_logs(&self.container) }

    /// MM2's log, cut to what explains a failure. Every wait below ends with
    /// this, because MM2's own log is where a refused topic creation, a failed
    /// `alterConsumerGroupOffsets` or an ACL sync that could not run is
    /// written down.
    fn diagnostics(&self) -> String { container_excerpt(&self.container) }

    /// How MM2 stopped, or `None` while it is still running.
    ///
    /// Every wait below is a wait on MM2 doing something, so a stopped MM2
    /// makes all of them hopeless. The first real run of this suite spent
    /// three further minutes polling a JVM that had already written
    /// `Kafka MirrorMaker stopped.`, and then reported a timeout rather than
    /// the exit.
    fn stopped(&self) -> Option<String> {
        let state = container_state(&self.container);
        (!state.running).then(|| format!("exited with status {}", state.exit_code))
    }
}

// ----------------------------------------------------------------- polling

/// Poll `probe` until it answers, or fail the case with MM2's log.
///
/// Each wait says what it is waiting for when it starts and how long it took
/// when it ends, in the `KRABKA[test]` format the other container suites use,
/// so a reader sees how far the case got without waiting for it to die. A
/// wait that ends any other way names itself in the failure too: the first
/// real run of this suite failed inside here and the message said only
/// `mirror_maker2.rs:564`, because nothing above had announced which of the
/// four waits was running.
async fn poll_until<T>(
    budget: Duration,
    what: &str,
    mm2: &MirrorMaker,
    mut probe: impl FnMut() -> Option<T>,
) -> T {
    let start = Instant::now();
    let deadline = start + budget;
    eprintln!("KRABKA[test] waiting for: {what} (up to {budget:?})");
    loop {
        if let Some(found) = probe() {
            eprintln!(
                "KRABKA[test] reached: {what}, after {:.1}s",
                start.elapsed().as_secs_f64(),
            );
            return found;
        }
        let stopped = mm2.stopped();
        assert!(
            stopped.is_none(),
            "MM2 {} while the case waited for {what}; {}",
            stopped.unwrap_or_default(),
            mm2.diagnostics(),
        );
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {budget:?}; {}",
            mm2.diagnostics(),
        );
        // intentional: every effect below is an MM2 interval expiring inside a
        // JVM, which krabka cannot be awaited on.
        time::sleep(POLL_GAP).await;
    }
}

// -------------------------------------------------------------- tool readers

/// Consume from `topic` with the console consumer and parse what it printed.
///
/// `group` is `None` for a read that must not disturb any group's position, in
/// which case the tool invents a group of its own and commits nothing. It is
/// `Some` for the cutover read, which is the point of the case that passes it.
fn consume(
    bootstrap: &str,
    topic: &str,
    group: Option<&str>,
    max_messages: usize,
    from_beginning: bool,
) -> Vec<Record> {
    let count = max_messages.to_string();
    let mut args = vec![
        "--bootstrap-server",
        bootstrap,
        "--topic",
        topic,
        "--max-messages",
        &count,
        "--timeout-ms",
        "30000",
        "--property",
        "print.headers=true",
        "--property",
        "print.key=true",
    ];
    if from_beginning {
        args.push("--from-beginning");
    }
    if let Some(group) = group {
        args.push("--group");
        args.push(group);
    }
    let out = tool_allowing_failure("kafka-console-consumer", &args);
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(Record::parse)
        .collect()
}

/// The topics a cluster holds, sorted.
fn list_topics(bootstrap: &str) -> Vec<String> {
    let out = tool("kafka-topics", &["--bootstrap-server", bootstrap, "--list"]);
    let mut topics: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    topics.sort();
    topics
}

/// The partition count `kafka-topics --describe` reports for one topic.
fn partition_count(bootstrap: &str, topic: &str) -> u32 {
    let out = tool("kafka-topics", &[
        "--bootstrap-server",
        bootstrap,
        "--describe",
        "--topic",
        topic,
    ]);
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    described_partitions(&text)
        .unwrap_or_else(|| panic!("no partition count in the description of {topic}:\n{text}"))
}

/// The partition count out of one `kafka-topics --describe` rendering.
///
/// The tool prints `Topic: <name> TopicId: <id> PartitionCount: <n> ...` as
/// the topic's first line, tab-separated. The field is found by its label
/// rather than by position, because the fields before it differ by release.
fn described_partitions(text: &str) -> Option<u32> {
    let mut fields = text
        .split_whitespace()
        .skip_while(|field| *field != "PartitionCount:");
    fields.next()?;
    fields.next()?.parse().ok()
}

/// One explicitly-set topic config, as `kafka-configs --describe` prints it,
/// or `None` when the topic does not carry it.
fn topic_config(bootstrap: &str, topic: &str, key: &str) -> Option<String> {
    let out = tool_allowing_failure("kafka-configs", &[
        "--bootstrap-server",
        bootstrap,
        "--describe",
        "--entity-type",
        "topics",
        "--entity-name",
        topic,
    ]);
    if !out.status.success() {
        return None;
    }
    described_config(&String::from_utf8_lossy(&out.stdout), key)
}

/// One config value out of a `kafka-configs --describe` rendering.
///
/// The tool prints only configs that were set, so a value here was really
/// stored on the broker rather than defaulted back at the caller. The value is
/// taken from the field that *starts* with `<key>=`, so the trailing
/// `synonyms={...}` field, which repeats the key inside itself, cannot be
/// mistaken for the config.
fn described_config(text: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    text.split_whitespace()
        .find_map(|field| field.strip_prefix(&prefix).map(str::to_owned))
}

/// A group's committed offset for partition zero of `topic`, or `None` when
/// the group has no position there yet.
fn committed_offset(bootstrap: &str, group: &str, topic: &str) -> Option<i64> {
    let out = tool_allowing_failure("kafka-consumer-groups", &[
        "--bootstrap-server",
        bootstrap,
        "--describe",
        "--group",
        group,
    ]);
    if !out.status.success() {
        return None;
    }
    described_offset(&String::from_utf8_lossy(&out.stdout), group, topic)
}

/// The committed offset for partition zero out of a
/// `kafka-consumer-groups --describe` rendering.
///
/// The tool prints one row per assigned partition under a
/// `GROUP TOPIC PARTITION CURRENT-OFFSET ...` header. A group with no
/// committed offset prints `-` in that column, which parses to `None` and
/// keeps a poller waiting rather than being read as a zero.
fn described_offset(text: &str, group: &str, topic: &str) -> Option<i64> {
    text.lines().find_map(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 4 || fields[0] != group || fields[1] != topic || fields[2] != "0" {
            return None;
        }
        fields[3].parse().ok()
    })
}

// ------------------------------------------------------------- source set-up

/// Create the topic being migrated and seed it with `records`.
fn seed_source(bootstrap: &str, records: &[Record]) {
    tool("kafka-topics", &[
        "--bootstrap-server",
        bootstrap,
        "--create",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ]);
    let lines: Vec<String> = records.iter().map(Record::line).collect();
    let stdin = format!("{}\n", lines.join("\n"));
    let out = tool_with_stdin(
        "kafka-console-producer",
        &[
            "--bootstrap-server",
            bootstrap,
            "--topic",
            TOPIC,
            "--property",
            "parse.key=true",
            "--property",
            "parse.headers=true",
            "--producer-property",
            "acks=all",
        ],
        Some(&stdin),
    );
    assert!(
        out.status.success(),
        "seeding the source topic failed:\n{}",
        both_streams(&out),
    );
}

/// Give the source cluster a consumer group whose committed position is
/// exactly [`COMMITTED`].
///
/// Two steps, because neither alone is both possible and exact. A console
/// consumer is what brings the group into being -- `--reset-offsets` refuses a
/// group that does not exist -- and the reset is what pins the position,
/// because what a console consumer commits before it stops at `--max-messages`
/// is its own business. The reset is retried: it also refuses a group that is
/// not yet empty, and the consumer's departure is asynchronous.
async fn commit_source_group(bootstrap: &str) {
    let read = consume(bootstrap, TOPIC, Some(GROUP), COMMITTED, true);
    assert!(
        read.len() == COMMITTED,
        "the source group read {} records rather than {COMMITTED}",
        read.len(),
    );

    let target_offset = i64::try_from(COMMITTED).expect("the committed offset fits an i64");
    let offset = target_offset.to_string();
    let deadline = Instant::now() + SOURCE_BUDGET;
    loop {
        let out = tool_allowing_failure("kafka-consumer-groups", &[
            "--bootstrap-server",
            bootstrap,
            "--group",
            GROUP,
            "--topic",
            TOPIC,
            "--reset-offsets",
            "--to-offset",
            &offset,
            "--execute",
        ]);
        if out.status.success() && committed_offset(bootstrap, GROUP, TOPIC) == Some(target_offset)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the source group's offset could not be pinned to {COMMITTED} within \
             {SOURCE_BUDGET:?}:\n{}",
            both_streams(&out),
        );
        // intentional: a group leaves the emptying state on the source
        // broker's own schedule, which nothing here can be awaited on.
        time::sleep(POLL_GAP).await;
    }
}

// ------------------------------------------------------------------ the case

use {
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

struct Migration {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    source: Option<SourceKafka>,
    mirror: Option<MirrorMaker>,
    target: String,
    mirrored: String,
    records: Vec<Record>,
    replicated: Vec<Record>,
    translated: Option<i64>,
    resumed: Option<Vec<Record>>,
    acl_output: Option<String>,
    mirror_logs: Option<String>,
}
#[fixture]
fn migration() -> Migration {
    // Start this scenario with fresh inputs and observations.

    Migration {
        broker: None,
        directory: None,
        source: None,
        mirror: None,
        target: String::new(),
        mirrored: format!("{SOURCE_ALIAS}.{TOPIC}"),
        records: Vec::new(),
        replicated: Vec::new(),
        translated: None,
        resumed: None,
        acl_output: None,
        mirror_logs: None,
    }
}
#[given(
    "a stock Kafka source holds ten keyed and headered records with its consumer group committed at four and krabka is the empty target"
)]
async fn migration_source(migration: &mut Migration) {
    let gateway = docker_bridge_gateway();
    let target = format!("{gateway}:{}", host_port());
    let advertised = target.clone();
    let (broker, directory) =
        start_host_broker_with(move |config| config.advertised_listener = advertised).await;
    let source = SourceKafka::start(&gateway).await;
    let records = seeded_records();
    seed_source(&source.bootstrap, &records);
    commit_source_group(&source.bootstrap).await;
    migration.broker = Some(broker);
    migration.directory = Some(directory);
    migration.source = Some(source);
    migration.records = records;
    migration.target = target;
}
#[when("stock MirrorMaker starts and copies the source topic onto krabka")]
async fn mirror_records(migration: &mut Migration) {
    let source = migration.source.as_ref().expect("source seeded");
    let mirror = MirrorMaker::start(&source.bootstrap, &migration.target);
    migration.replicated = poll_until(
        MIRROR_BUDGET,
        "the source topic was mirrored",
        &mirror,
        || {
            let read = consume(&migration.target, &migration.mirrored, None, RECORDS, true);
            (read.len() == RECORDS).then_some(read)
        },
    )
    .await;
    migration.mirror = Some(mirror);
}
#[then("every mirrored record retains its source key value and headers")]
fn records_preserved(migration: &Migration) {
    assert!(
        migration.replicated == migration.records,
        "the mirrored records differ from source records; {}",
        migration
            .mirror
            .as_ref()
            .expect("mirror started")
            .diagnostics()
    );
}
#[when("the source operator changes retention after mirroring is established")]
fn change_retention(migration: &mut Migration) {
    alter_source_retention(&migration.source.as_ref().expect("source seeded").bootstrap);
}
#[then("the mirror internal topics are single partition compacted topics")]
async fn internal_topics(migration: &Migration) {
    assert_internal_topics(
        &migration.target,
        migration.mirror.as_ref().expect("mirror started"),
    )
    .await;
}
#[when("the consumer group cuts over to krabka and resumes reading")]
async fn cutover(migration: &mut Migration) {
    let mirror = migration.mirror.as_ref().expect("mirror started");
    migration.translated = Some(
        poll_until(
            EFFECT_BUDGET,
            "MM2 translated the group's offset onto krabka",
            mirror,
            || committed_offset(&migration.target, GROUP, &migration.mirrored),
        )
        .await,
    );
    migration.resumed = Some(consume(
        &migration.target,
        &migration.mirrored,
        Some(GROUP),
        RECORDS - COMMITTED,
        false,
    ));
}
#[then(
    "the translated position is four and the consumer receives exactly the unread source suffix"
)]
fn resumed(migration: &Migration) {
    assert!(migration.translated == Some(i64::try_from(COMMITTED).expect("offset fits")));
    assert!(
        migration.resumed.as_ref().expect("cutover read completed")
            == &migration.records[COMMITTED..]
    );
}
#[then("the live source retention change reaches the target topic")]
async fn retention_synced(migration: &Migration) {
    assert_topic_config_sync(
        &migration.target,
        &migration.mirrored,
        migration.mirror.as_ref().expect("mirror started"),
    )
    .await;
}
#[when("the operator inspects mirror logs and attempts the target side ACL operation")]
fn inspect_acl(migration: &mut Migration) {
    migration.mirror_logs = Some(migration.mirror.as_ref().expect("mirror started").logs());
    let output = tool_allowing_failure("kafka-acls", &[
        "--bootstrap-server",
        &migration.target,
        "--add",
        "--allow-principal",
        "User:krabka-mm2",
        "--operation",
        "Read",
        "--topic",
        TOPIC,
    ]);
    migration.acl_output = Some(both_streams(&output));
}
#[then(
    "ACL sync logs contain no failure and the unsecured target reports Kafka's security disabled exception"
)]
fn acl_compatibility(migration: &Migration) {
    assert!(
        !migration
            .mirror_logs
            .as_ref()
            .expect("mirror logs read")
            .contains("Could not sync ACL")
    );
    let text = migration
        .acl_output
        .as_ref()
        .expect("target ACL request completed");
    assert!(
        text.contains("SecurityDisabledException")
            && text.contains("No Authorizer is configured on the broker"),
        "unexpected target ACL response: {text}"
    );
}
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/mirror_maker2.feature",
    name = "Stock MirrorMaker migrates records consumer position and live configuration"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mirror_maker2_migrates_a_kafka_cluster_onto_krabka(migration: Migration) {
    drop(migration.mirror);
    drop(migration.source);
    migration
        .broker
        .expect("target broker started")
        .shutdown()
        .await;
}

/// MM2's three internal topics exist on the target with the shape
/// `MirrorUtils.createSinglePartitionCompactedTopic` asks for.
async fn assert_internal_topics(target: &str, mm2: &MirrorMaker) {
    let found = poll_until(
        EFFECT_BUDGET,
        "MM2's internal topics appeared on krabka",
        mm2,
        || {
            let mut topics: Vec<RoleTopic> = list_topics(target)
                .into_iter()
                .filter_map(|name| role_of(&name).map(|role| RoleTopic { role, name }))
                .collect();
            topics.sort();
            // Every role present, rather than a count of three. The two are
            // the same when the target holds what it should, and different
            // when it holds a fourth topic of one of these shapes: a count
            // waits out the whole budget and reports a timeout, where this
            // hands the extra topic to the comparison below and the failure
            // says what was found.
            let covered = |role: Role| topics.iter().any(|topic| topic.role == role);
            ROLES.into_iter().all(covered).then_some(topics)
        },
    )
    .await;

    let described: Vec<InternalTopic> = found
        .iter()
        .map(|RoleTopic { role, name }| InternalTopic {
            role: *role,
            partitions: partition_count(target, name),
            cleanup_policy: topic_config(target, name, "cleanup.policy").unwrap_or_default(),
        })
        .collect();
    let expected: Vec<InternalTopic> = ROLES
        .into_iter()
        .map(|role| InternalTopic {
            role,
            partitions: 1,
            cleanup_policy: "compact".to_owned(),
        })
        .collect();
    assert!(
        described == expected,
        "MM2's internal topics on krabka are {described:?}, not {expected:?}; the topics \
         found were {found:?} and {}",
        mm2.diagnostics(),
    );
}

/// Set [`RETENTION_MS`] on the source topic, which mirroring is by now live
/// over, so `sync.topic.configs` has a change to carry.
fn alter_source_retention(source: &str) {
    tool("kafka-configs", &[
        "--bootstrap-server",
        source,
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        &format!("retention.ms={RETENTION_MS}"),
    ]);
}

/// The `retention.ms` [`alter_source_retention`] set on the source topic
/// reaches the target copy through `sync.topic.configs`.
async fn assert_topic_config_sync(target: &str, mirrored: &str, mm2: &MirrorMaker) {
    let synced = poll_until(
        EFFECT_BUDGET,
        "MM2 synced retention.ms onto krabka",
        mm2,
        || topic_config(target, mirrored, "retention.ms"),
    )
    .await;
    assert!(
        synced == RETENTION_MS,
        "krabka holds retention.ms={synced} on {mirrored}, not {RETENTION_MS}; {}",
        mm2.diagnostics(),
    );
}

// -------------------------------------------------------------------- units

/// The readings above, held to renderings the JVM tools really produce. They
/// need no Docker daemon, so they run in the ordinary lane and a parser that
/// stops understanding a tool fails there rather than only under `--ignored`.
#[cfg(test)]
mod tests {
    use {
        super::{
            ContainerState, LOG_PROBLEM_LINES, LOG_TAIL_LINES, Record, Role, bridge_gateway,
            described_config, described_offset, described_partitions, log_excerpt,
            parse_container_state, role_of, seeded_records,
        },
        assert2::assert,
        rstest::fixture,
        rstest_bdd::Slot,
        rstest_bdd_macros::{given, scenarios, then, when},
    };
    #[derive(Default)]
    struct RecordLines {
        source: Vec<Record>,
        lines: Vec<String>,
        parsed: Vec<Option<Record>>,
    }
    #[fixture]
    fn records() -> RecordLines {
        // Start this scenario with fresh inputs and observations.

        RecordLines {
            source: Vec::new(),
            lines: Vec::new(),
            parsed: Vec::new(),
        }
    }
    #[given("the migration fixture's keyed and headered records")]
    fn seeded(records: &mut RecordLines) { records.source = seeded_records(); }
    #[given("the console formatter outputs these lines")]
    fn console_lines(records: &mut RecordLines, datatable: Vec<Vec<String>>) {
        records.lines = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                row.into_iter()
                    .next()
                    .expect("console line")
                    .replace("\\t", "\t")
            })
            .collect();
    }
    #[when("the seeded records are rendered and parsed as console lines")]
    fn round_trip(records: &mut RecordLines) {
        records.parsed = records
            .source
            .iter()
            .map(|record| Record::parse(&record.line()))
            .collect();
    }
    #[when("the console record parser interprets those lines")]
    fn parse_lines(records: &mut RecordLines) {
        records.parsed = records
            .lines
            .iter()
            .map(|line| Record::parse(line))
            .collect();
    }
    #[then("every rendered record retains the original headers key and value")]
    fn retained(records: &RecordLines) {
        assert!(!records.source.is_empty());
        assert!(records.parsed == records.source.iter().cloned().map(Some).collect::<Vec<_>>());
    }
    #[then("all notices and malformed lines remain absent records")]
    fn ignored(records: &RecordLines) {
        assert!(records.parsed.len() == 3);
        assert!(records.parsed.iter().all(Option::is_none));
    }
    #[then("the decoded record has the expected headers key and value")]
    fn headerless_record(records: &RecordLines, datatable: Vec<Vec<String>>) {
        let expected: Vec<Option<Record>> = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [headers, key, value]: [String; 3] = row.try_into().expect("headers key value");
                Some(Record {
                    headers,
                    key,
                    value,
                })
            })
            .collect();
        assert!(records.parsed == expected);
    }

    #[derive(Default)]
    struct TopicRoles {
        names: Vec<String>,
        roles: Vec<Option<Role>>,
    }
    #[fixture]
    fn topics() -> TopicRoles {
        // Start this scenario with fresh inputs and observations.

        TopicRoles {
            names: Vec::new(),
            roles: Vec::new(),
        }
    }
    #[given("the target has topic names")]
    fn names(topics: &mut TopicRoles, datatable: Vec<Vec<String>>) {
        topics.names = datatable
            .into_iter()
            .skip(1)
            .map(|row| row.into_iter().next().expect("topic name"))
            .collect();
    }
    #[when("the migration inspector classifies the internal topic roles")]
    fn classify(topics: &mut TopicRoles) {
        topics.roles = topics.names.iter().map(|name| role_of(name)).collect();
    }
    #[then("only heartbeat checkpoint and offset sync topic shapes have these roles")]
    fn roles(topics: &TopicRoles, datatable: Vec<Vec<String>>) {
        let expected: Vec<Option<Role>> = datatable
            .into_iter()
            .skip(1)
            .map(|row| match row[0].as_str() {
                "Heartbeats" => Some(Role::Heartbeats),
                "Checkpoints" => Some(Role::Checkpoints),
                "OffsetSyncs" => Some(Role::OffsetSyncs),
                "none" => None,
                role => panic!("unknown expected role {role}"),
            })
            .collect();
        assert!(topics.roles == expected);
    }

    #[derive(Default)]
    struct ToolReport {
        text: String,
        partitions: Slot<Option<u32>>,
        configs: Vec<Option<String>>,
        offsets: Vec<Option<i64>>,
    }
    #[fixture]
    fn report() -> ToolReport {
        // Start this scenario with fresh inputs and observations.

        ToolReport {
            text: String::new(),
            partitions: Slot::default(),
            configs: Vec::new(),
            offsets: Vec::new(),
        }
    }
    #[given("stock Kafka prints the migration inspection report")]
    fn text(report: &mut ToolReport, docstring: String) { report.text = docstring; }
    #[when("the inspector reads the partition count")]
    fn partitions(report: &mut ToolReport) {
        report.partitions.set(described_partitions(&report.text));
    }
    #[then("the report describes one partition")]
    fn one_partition(report: &ToolReport) {
        assert!(report.partitions.get() == Some(Some(1)));
    }
    #[then("the missing partition count remains absent")]
    fn no_partition(report: &ToolReport) {
        assert!(report.partitions.get() == Some(None));
    }
    #[when("the inspector reads cleanup policy and an absent retention setting")]
    fn configs(report: &mut ToolReport) {
        report.configs = ["cleanup.policy", "retention.ms"]
            .into_iter()
            .map(|key| described_config(&report.text, key))
            .collect();
    }
    #[then("the effective cleanup policy is compact and no retention setting is invented")]
    fn cleanup(report: &ToolReport) {
        assert!(report.configs == vec![Some("compact".into()), None]);
    }
    #[when("the inspector reads the rider group's mirrored orders offset and an absent topic")]
    fn offsets(report: &mut ToolReport) {
        report.offsets = ["source.krabka-mm2-orders", "source.absent"]
            .into_iter()
            .map(|topic| described_offset(&report.text, "krabka-mm2-riders", topic))
            .collect();
    }
    #[then("the orders offset is four and the absent topic has no offset")]
    fn committed(report: &ToolReport) {
        assert!(report.offsets == vec![Some(4), None]);
    }
    #[when("the inspector reads the rider group's uncommitted orders offset")]
    fn uncommitted(report: &mut ToolReport) {
        report.offsets = vec![described_offset(
            &report.text,
            "krabka-mm2-riders",
            "source.krabka-mm2-orders",
        )];
    }
    #[then("an uncommitted group has no offset rather than zero")]
    fn no_commit(report: &ToolReport) {
        assert!(report.offsets == vec![None]);
    }

    #[derive(Default)]
    struct LogSummary {
        log: String,
        excerpt: Option<String>,
    }
    #[fixture]
    fn logs() -> LogSummary {
        // Start this scenario with fresh inputs and observations.

        LogSummary {
            log: String::new(),
            excerpt: None,
        }
    }
    #[given(
        "a herder error is followed by five hundred shutdown notices and the final stopped notice"
    )]
    fn shutdown_log(logs: &mut LogSummary) {
        let mut lines = vec![
            "[2026-09-08 03:39:20,001] ERROR Uncaught exception in herder work thread, exiting"
                .to_owned(),
        ];
        for index in 0..500 {
            lines.push(format!(
                "[2026-09-08 03:39:21,{index:03}] INFO Metrics reporters closed"
            ));
        }
        lines.push("[2026-09-08 03:40:08,761] INFO Kafka MirrorMaker stopped.".into());
        logs.log = lines.join("\n");
    }
    #[given("the mirror log contains five hundred warning lines")]
    fn warnings(logs: &mut LogSummary) {
        logs.log = (0..500)
            .map(|index| format!("[ts] WARN something went wrong: {index}"))
            .collect::<Vec<_>>()
            .join("\n");
    }
    #[given("the mirror log is empty")]
    fn empty(logs: &mut LogSummary) { logs.log.clear(); }
    #[when("the migration diagnostic excerpts the mirror log")]
    fn excerpt(logs: &mut LogSummary) { logs.excerpt = Some(log_excerpt(&logs.log)); }
    #[then(
        "the bounded excerpt retains both the original herder cause and the final shutdown notice"
    )]
    fn cause(logs: &LogSummary) {
        let excerpt = logs.excerpt.as_ref().expect("log excerpt generated");
        assert!(excerpt.contains("Uncaught exception in herder work thread"));
        assert!(excerpt.contains("Kafka MirrorMaker stopped."));
        assert!(excerpt.contains("502 lines, of which 1 reported a problem"));
        assert!(excerpt.lines().count() <= LOG_PROBLEM_LINES + LOG_TAIL_LINES + 2);
    }
    #[then("the bounded excerpt retains the newest warning and omits the first warning")]
    fn bounded(logs: &LogSummary) {
        let excerpt = logs.excerpt.as_ref().expect("log excerpt generated");
        assert!(excerpt.lines().count() <= LOG_PROBLEM_LINES + LOG_TAIL_LINES + 2);
        assert!(excerpt.contains("something went wrong: 499"));
        assert!(!excerpt.contains("something went wrong: 0\n"));
    }
    #[then("the empty excerpt reports zero lines problems and section lengths")]
    fn empty_excerpt(logs: &LogSummary) {
        let expected = [
            "0 lines, of which 0 reported a problem; the last 0 of those:",
            "",
            "the last 0 lines:",
            "",
        ]
        .join("\n");
        assert!(logs.excerpt.as_ref().expect("log excerpt generated") == &expected);
    }

    #[derive(Default)]
    struct DockerReadings {
        renderings: Vec<String>,
        states: Vec<Option<ContainerState>>,
        gateways: Vec<Option<String>>,
    }
    #[fixture]
    fn docker() -> DockerReadings {
        // Start this scenario with fresh inputs and observations.

        DockerReadings {
            renderings: Vec::new(),
            states: Vec::new(),
            gateways: Vec::new(),
        }
    }
    #[given("Docker inspect prints these container or network outputs")]
    fn docker_outputs(docker: &mut DockerReadings, datatable: Vec<Vec<String>>) {
        docker.renderings = datatable
            .into_iter()
            .skip(1)
            .map(|row| row.into_iter().next().expect("Docker stdout"))
            .collect();
    }
    #[when("the inspector parses running zero stopped 143 and missing container renderings")]
    fn containers(docker: &mut DockerReadings) {
        docker.states = docker
            .renderings
            .iter()
            .map(|text| parse_container_state(text))
            .collect();
    }
    #[then("the interpreted container states are")]
    fn states(docker: &DockerReadings, datatable: Vec<Vec<String>>) {
        let expected: Vec<Option<ContainerState>> = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [running, exit_code]: [String; 2] =
                    row.try_into().expect("running and exit code");
                if running == "missing" {
                    None
                } else {
                    Some(ContainerState {
                        running: running.parse().expect("boolean running state"),
                        exit_code: exit_code.parse().expect("exit code"),
                    })
                }
            })
            .collect();
        assert!(docker.states == expected);
    }
    #[when("the inspector reads explicit subnet fallback and malformed network metadata")]
    fn gateways(docker: &mut DockerReadings) {
        docker.gateways = docker
            .renderings
            .iter()
            .map(|text| bridge_gateway(text))
            .collect();
    }
    #[then("the interpreted bridge gateways are")]
    fn gateway_results(docker: &DockerReadings, datatable: Vec<Vec<String>>) {
        let expected: Vec<Option<String>> = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let gateway = row.into_iter().next().expect("expected gateway");
                (gateway != "missing").then_some(gateway)
            })
            .collect();
        assert!(docker.gateways == expected);
    }

    scenarios!("tests/features/migrated/tests/mirror_maker2_readings.feature", fixtures=[records:RecordLines, topics:TopicRoles, report:ToolReport, logs:LogSummary, docker:DockerReadings]);
}
