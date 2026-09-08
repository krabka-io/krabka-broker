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
//! The container-driven case is gated `#[ignore = "requires Docker"]`; the
//! Bazel `docker` lane runs it. The parsing this suite reads the JVM tools
//! with is covered by ordinary tests that need no daemon.

mod jvm_acceptance;
mod support;

use std::{
    io::Write as _,
    process::{Command, Output, Stdio},
    time::Duration,
};

use assert2::assert;
use jvm_acceptance::{broker0_listen, host_port, start_host_broker_with};

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
    fn line(&self) -> String {
        format!("{}\t{}\t{}", self.headers, self.key, self.value)
    }

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

/// The gateway out of the `Gateway|Subnet` pair `docker network inspect`
/// renders, falling back to the subnet's first address when the daemon reports
/// no gateway of its own.
fn bridge_gateway(rendered: &str) -> Option<String> {
    let (gateway, subnet) = rendered.trim().split_once('|')?;
    if gateway.parse::<std::net::IpAddr>().is_ok() {
        return Some(gateway.to_owned());
    }
    let (base, _prefix) = subnet.split_once('/')?;
    let base: std::net::Ipv4Addr = base.parse().ok()?;
    Some(std::net::Ipv4Addr::from(u32::from(base).checked_add(1)?).to_string())
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
    let out = match stdin {
        None => {
            command.stdin(Stdio::null());
            command.output().expect("spawn docker run")
        }
        Some(text) => {
            command.stdin(Stdio::piped());
            let mut child = command.spawn().expect("spawn docker run");
            child
                .stdin
                .as_mut()
                .expect("the container has a piped stdin")
                .write_all(text.as_bytes())
                .expect("write to the tool's stdin");
            drop(child.stdin.take());
            child.wait_with_output().expect("wait for docker run")
        }
    };
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
fn container_logs(name: &str) -> String {
    let out = Command::new("docker")
        .args(["logs", name])
        .output()
        .expect("spawn docker logs");
    both_streams(&out)
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
        let deadline = tokio::time::Instant::now() + SOURCE_BUDGET;
        loop {
            let out = tool_allowing_failure(
                "kafka-topics",
                &["--bootstrap-server", &self.bootstrap, "--list"],
            );
            if out.status.success() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the source Kafka did not answer within {SOURCE_BUDGET:?}:\n{}",
                container_logs(&self.container),
            );
            // intentional: a JVM broker's readiness is its own internal state,
            // and asking its tools is the only observation there is.
            tokio::time::sleep(POLL_GAP).await;
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
        Self { container }
    }

    /// Everything MM2 has printed. Every wait below ends with this, because
    /// MM2's own log is where a refused topic creation, a failed
    /// `alterConsumerGroupOffsets` or an ACL sync that could not run is
    /// written down.
    fn logs(&self) -> String {
        container_logs(&self.container)
    }
}

// ----------------------------------------------------------------- polling

/// Poll `probe` until it answers, or fail the case with MM2's log.
async fn poll_until<T>(
    budget: Duration,
    what: &str,
    mm2: &MirrorMaker,
    mut probe: impl FnMut() -> Option<T>,
) -> T {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Some(found) = probe() {
            return found;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what} did not happen within {budget:?}; MM2 logs:\n{}",
            mm2.logs(),
        );
        // intentional: every effect below is an MM2 interval expiring inside a
        // JVM, which krabka cannot be awaited on.
        tokio::time::sleep(POLL_GAP).await;
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
    let out = tool(
        "kafka-topics",
        &[
            "--bootstrap-server",
            bootstrap,
            "--describe",
            "--topic",
            topic,
        ],
    );
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
    let out = tool_allowing_failure(
        "kafka-configs",
        &[
            "--bootstrap-server",
            bootstrap,
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
        ],
    );
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
    let out = tool_allowing_failure(
        "kafka-consumer-groups",
        &[
            "--bootstrap-server",
            bootstrap,
            "--describe",
            "--group",
            group,
        ],
    );
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
    tool(
        "kafka-topics",
        &[
            "--bootstrap-server",
            bootstrap,
            "--create",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
    );
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
    let deadline = tokio::time::Instant::now() + SOURCE_BUDGET;
    loop {
        let out = tool_allowing_failure(
            "kafka-consumer-groups",
            &[
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
            ],
        );
        if out.status.success() && committed_offset(bootstrap, GROUP, TOPIC) == Some(target_offset)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the source group's offset could not be pinned to {COMMITTED} within \
             {SOURCE_BUDGET:?}:\n{}",
            both_streams(&out),
        );
        // intentional: a group leaves the emptying state on the source
        // broker's own schedule, which nothing here can be awaited on.
        tokio::time::sleep(POLL_GAP).await;
    }
}

// ------------------------------------------------------------------ the case

/// MM2 migrates a stock Kafka cluster onto krabka: the records and their
/// headers, the internal topics it needs, the consumer group's translated
/// position, and a topic config changed after the fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn mirror_maker2_migrates_a_kafka_cluster_onto_krabka() {
    let gateway = docker_bridge_gateway();
    let target = format!("{gateway}:{}", host_port());
    let advertised = target.clone();
    // krabka advertises the bridge gateway rather than the
    // `host.docker.internal` name the other JVM suites use, so MM2 and the
    // source broker are named the same way from every side.
    let (broker, _dir) = start_host_broker_with(move |config| {
        config.advertised_listener = advertised;
    })
    .await;
    eprintln!(
        "KRABKA[test] krabka listens on {} and is advertised as {target}",
        broker0_listen(),
    );

    let source = SourceKafka::start(&gateway).await;
    let records = seeded_records();
    seed_source(&source.bootstrap, &records);
    commit_source_group(&source.bootstrap).await;

    let mm2 = MirrorMaker::start(&source.bootstrap, &target);
    let mirrored = format!("{SOURCE_ALIAS}.{TOPIC}");

    let replicated = poll_until(MIRROR_BUDGET, "the source topic was mirrored", &mm2, || {
        let read = consume(&target, &mirrored, None, RECORDS, true);
        (read.len() == RECORDS).then_some(read)
    })
    .await;
    assert!(
        replicated == records,
        "the mirrored records differ from the source records; MM2 logs:\n{}",
        mm2.logs(),
    );

    assert_internal_topics(&target, &mm2).await;
    assert_translated_offsets(&target, &mirrored, &records, &mm2).await;
    assert_topic_config_sync(&source.bootstrap, &target, &mirrored, &mm2).await;
    assert_acl_sync(&target, &mm2);

    drop(mm2);
    drop(source);
    broker.shutdown().await;
}

/// MM2's three internal topics exist on the target with the shape
/// `MirrorUtils.createSinglePartitionCompactedTopic` asks for.
async fn assert_internal_topics(target: &str, mm2: &MirrorMaker) {
    let found = poll_until(
        EFFECT_BUDGET,
        "MM2's internal topics appeared on krabka",
        mm2,
        || {
            let mut topics: Vec<(Role, String)> = list_topics(target)
                .into_iter()
                .filter_map(|name| role_of(&name).map(|role| (role, name)))
                .collect();
            topics.sort();
            (topics.len() == 3).then_some(topics)
        },
    )
    .await;

    let described: Vec<InternalTopic> = found
        .iter()
        .map(|(role, name)| InternalTopic {
            role: *role,
            partitions: partition_count(target, name),
            cleanup_policy: topic_config(target, name, "cleanup.policy").unwrap_or_default(),
        })
        .collect();
    let expected: Vec<InternalTopic> = [Role::Checkpoints, Role::Heartbeats, Role::OffsetSyncs]
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
         found were {found:?} and MM2 logs:\n{}",
        mm2.logs(),
    );
}

/// The source group's position survives the cutover: MM2 translates it onto
/// the target, and a consumer of that group on the target resumes at the
/// record the source group had not read yet.
///
/// This is the case a broker that is 99% compatible fails silently. A position
/// translated one record early replays; one record late skips; and neither
/// reports anything to anybody.
async fn assert_translated_offsets(
    target: &str,
    mirrored: &str,
    records: &[Record],
    mm2: &MirrorMaker,
) {
    let expected_offset = i64::try_from(COMMITTED).expect("the committed offset fits an i64");
    let translated = poll_until(
        EFFECT_BUDGET,
        "MM2 translated the group's offset onto krabka",
        mm2,
        || committed_offset(target, GROUP, mirrored),
    )
    .await;
    assert!(
        translated == expected_offset,
        "the group resumes on krabka at {translated}, not at the source's {COMMITTED}; \
         MM2 logs:\n{}",
        mm2.logs(),
    );

    let resumed = consume(target, mirrored, Some(GROUP), RECORDS - COMMITTED, false);
    let expected: Vec<Record> = records[COMMITTED..].to_vec();
    assert!(
        resumed == expected,
        "the cut-over group read {resumed:?} rather than resuming at {expected:?}; \
         MM2 logs:\n{}",
        mm2.logs(),
    );
}

/// A `retention.ms` set on the source topic after mirroring is established
/// reaches the target copy through `sync.topic.configs`.
async fn assert_topic_config_sync(source: &str, target: &str, mirrored: &str, mm2: &MirrorMaker) {
    tool(
        "kafka-configs",
        &[
            "--bootstrap-server",
            source,
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            TOPIC,
            "--add-config",
            &format!("retention.ms={RETENTION_MS}"),
        ],
    );
    let synced = poll_until(
        EFFECT_BUDGET,
        "MM2 synced retention.ms onto krabka",
        mm2,
        || topic_config(target, mirrored, "retention.ms"),
    )
    .await;
    assert!(
        synced == RETENTION_MS,
        "krabka holds retention.ms={synced} on {mirrored}, not {RETENTION_MS}; MM2 logs:\n{}",
        mm2.logs(),
    );
}

/// The default `sync.topic.acls.enabled=true` path leaves the run alone, and
/// the target-side call it would make is refused the way a stock Kafka target
/// with no authorizer refuses it.
///
/// MM2's ACL sync stops at the source here, which has no authorizer either, so
/// the first half is a negative: nothing in MM2's log says a sync failed. The
/// second half makes the call MM2 would have made and reads krabka's answer
/// directly, which is the `SECURITY_DISABLED` (54) this milestone landed.
fn assert_acl_sync(target: &str, mm2: &MirrorMaker) {
    let logs = mm2.logs();
    assert!(
        !logs.contains("Could not sync ACL"),
        "MM2 reported an ACL sync failure:\n{logs}",
    );

    let out = tool_allowing_failure(
        "kafka-acls",
        &[
            "--bootstrap-server",
            target,
            "--add",
            "--allow-principal",
            "User:krabka-mm2",
            "--operation",
            "Read",
            "--topic",
            TOPIC,
        ],
    );
    let text = both_streams(&out);
    assert!(
        text.contains("SecurityDisabledException")
            && text.contains("No Authorizer is configured on the broker"),
        "krabka answered MM2's target-side ACL creation with something other than Kafka's \
         SecurityDisabledException:\n{text}",
    );
}

// -------------------------------------------------------------------- units

/// The readings above, held to renderings the JVM tools really produce. They
/// need no Docker daemon, so they run in the ordinary lane and a parser that
/// stops understanding a tool fails there rather than only under `--ignored`.
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        Record, Role, bridge_gateway, described_config, described_offset, described_partitions,
        role_of, seeded_records,
    };

    #[test]
    fn a_produced_line_parses_back_into_the_same_record() {
        for record in seeded_records() {
            assert!(Record::parse(&record.line()) == Some(record));
        }
    }

    #[test]
    fn a_line_that_is_not_a_record_is_not_read_as_one() {
        for line in ["Processed a total of 10 messages", "", "a\tb\tc\td"] {
            assert!(Record::parse(line) == None);
        }
    }

    #[test]
    fn a_record_without_headers_parses_as_the_tools_render_it() {
        assert!(
            Record::parse("NO_HEADERS\tkey-0\torder-0")
                == Some(Record {
                    headers: "NO_HEADERS".to_owned(),
                    key: "key-0".to_owned(),
                    value: "order-0".to_owned(),
                })
        );
    }

    #[test]
    fn each_mirror_maker_internal_topic_is_recognised_by_its_shape() {
        let cases = [
            ("heartbeats", Some(Role::Heartbeats)),
            ("source.checkpoints.internal", Some(Role::Checkpoints)),
            ("mm2-offset-syncs.target.internal", Some(Role::OffsetSyncs)),
            ("mm2-offset-syncs.source.internal", Some(Role::OffsetSyncs)),
            // The Connect stores MM2 keeps on the target are not among them.
            ("mm2-offsets.source.internal", None),
            ("mm2-configs.source.internal", None),
            ("mm2-status.source.internal", None),
            ("source.krabka-mm2-orders", None),
            ("__consumer_offsets", None),
        ];
        for (name, role) in cases {
            assert!(role_of(name) == role, "{name}");
        }
    }

    /// `kafka-topics --describe` as Kafka 4.3 renders a compacted internal
    /// topic, tabs and all.
    #[test]
    fn the_partition_count_is_read_out_of_a_topic_description() {
        let text = "Topic: heartbeats\tTopicId: sVCK0oNBQtOb4tGXEmuJHw\tPartitionCount: 1\t\
                    ReplicationFactor: 1\tConfigs: cleanup.policy=compact\n\
                    \tTopic: heartbeats\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\n";
        assert!(described_partitions(text) == Some(1));
        assert!(described_partitions("Topic: heartbeats\n") == None);
    }

    /// `kafka-configs --describe` prints the value and then a `synonyms=`
    /// field that repeats the key inside itself.
    #[test]
    fn a_config_value_is_not_taken_from_the_synonyms_field() {
        let text = "Dynamic configs for topic heartbeats are:\n  \
                    cleanup.policy=compact sensitive=false \
                    synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=compact}\n";
        assert!(described_config(text, "cleanup.policy") == Some("compact".to_owned()));
        assert!(described_config(text, "retention.ms") == None);
    }

    #[test]
    fn a_groups_committed_offset_is_read_off_its_own_row() {
        let text = "\nGROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG \
                    CONSUMER-ID HOST CLIENT-ID\n\
                    krabka-mm2-riders source.krabka-mm2-orders 0 4 10 6 - - -\n\
                    krabka-mm2-riders source.other 0 9 9 0 - - -\n";
        assert!(described_offset(text, "krabka-mm2-riders", "source.krabka-mm2-orders") == Some(4));
        assert!(described_offset(text, "krabka-mm2-riders", "source.absent") == None);
    }

    /// A group that has joined but committed nothing prints a dash, which must
    /// not be read as offset zero.
    #[test]
    fn an_uncommitted_group_reads_as_no_offset_rather_than_zero() {
        let text = "krabka-mm2-riders source.krabka-mm2-orders 0 - 10 - - - -\n";
        assert!(described_offset(text, "krabka-mm2-riders", "source.krabka-mm2-orders") == None);
    }

    #[test]
    fn the_bridge_gateway_falls_back_to_the_subnets_first_address() {
        assert!(bridge_gateway("172.17.0.1|172.17.0.0/16\n") == Some("172.17.0.1".to_owned()));
        assert!(bridge_gateway("|172.20.0.0/16\n") == Some("172.20.0.1".to_owned()));
        assert!(bridge_gateway("no separator here") == None);
    }
}
