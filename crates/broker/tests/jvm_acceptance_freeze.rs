//! JVM cross-validation: the stock Apache Kafka admin tools read a KFC-9
//! refusal as the refusal it is.
//!
//! # This suite has never been executed
//!
//! It was written against the pattern that `jvm_barrier_markers` and the
//! `jvm_acceptance_*` suites established, and no case in it has ever run. The
//! machine it was written on has no working Docker daemon, so every case here
//! is a statement of what the behaviour must be rather than a report of what
//! it was observed to be. The first real execution has to happen on a
//! Docker-capable host, with the invocation below. A reviewer should treat a
//! first-run failure as a bug in this file until they have shown otherwise.
//!
//! ```text
//! cargo test -p krabka-broker --test jvm_acceptance_freeze -- --ignored --nocapture
//! ```
//!
//! # The claim
//!
//! KFC-9 adds two ways for a healthy, correctly configured cluster to refuse a
//! request that the caller is fully authorized to make: a topic write freeze,
//! and a break-glass two-person rule over the privileged transitions. Both
//! refuse with `POLICY_VIOLATION` (44), and the read-only `write.freeze` topic
//! config refuses an alter with `INVALID_CONFIG` (40).
//!
//! An error code is only worth what the client makes of it. `Errors.forCode`
//! maps 44 onto `PolicyViolationException` and 40 onto
//! `InvalidConfigurationException`, and it maps an unassigned code onto
//! `UnknownServerException`, which a JVM client classifies as retriable and a
//! JVM tool reports as an internal broker fault. That difference is the whole
//! reason KFC-9 reuses two existing Kafka codes rather than minting new ones,
//! and it is a claim about somebody else's code. This suite is the only place
//! the claim is checked against Apache Kafka's own client rather than against
//! krabka's.
//!
//! # What a weaker suite would miss
//!
//! Every other KFC-9 test drives krabka's client against krabka's broker. Such
//! a test passes just as happily on a private error code, on a code the JVM
//! calls retriable, and on a response that carries no `error_message` at all,
//! because krabka's client reads the number it was given. Three failures live
//! in that gap, and all three are invisible without a JVM in the loop.
//!
//! 1. **The exception class.** A JVM operator sees a class name, not a number.
//!    Each case asserts the fully-qualified exception the tool printed, so a
//!    code that stops mapping to `PolicyViolationException` fails here.
//! 2. **The broker's message.** KFC-9 promises that the refusal names the
//!    freeze scope, or the action and target the two-person rule wants an
//!    approval for. A response that dropped `error_message` still carries the
//!    right code, and leaves the on-call with nothing to act on. Each case
//!    asserts the broker's own sentence, reconstructed here, appears in what
//!    the tool printed.
//! 3. **The exit code.** A runbook branches on `$?`. A tool that reports a
//!    failure on stdout and exits zero is worse than one that fails loudly,
//!    and no in-process test can see it. This is why the producer case runs
//!    with `--sync`: the asynchronous console producer hands a failed send to
//!    a logging callback and still exits zero, so the synchronous path is the
//!    only one whose exit code means anything.
//!
//! The two positive controls carry the rest of the weight. A refusal that
//! refuses everything is not a feature, so the producer case produces to an
//! unfrozen control topic through the same tool and the same broker, and the
//! break-glass cases show the same command succeeding once an approval exists.
//! Without them a broker that refused every write would pass this file.
//!
//! # Networking
//!
//! The broker runs on the host and advertises `host.docker.internal`; the JVM
//! tools run in cp-kafka containers on the default bridge with
//! `--add-host=host.docker.internal:host-gateway`. [`jvm_acceptance`] documents
//! that arrangement, and why `--network host` is not used.
//!
//! Each case allocates its own listener pair through
//! [`support::JvmListeners::allocate`] rather than sharing the process-wide
//! set, so the four cases can run concurrently without racing for a port.
//!
//! # What this suite deliberately does not cover
//!
//! Operator signatures. `break_glass.signed_actions` is set empty in every
//! case here, so the approvals are unsigned. The canonical byte layout, the
//! attack table, and the verification rules are the in-process suites' work;
//! a container can say nothing about them that a unit test does not say
//! better. What a container can say is what the JVM tool prints and what it
//! exits with, and that is all this file asserts.

mod jvm_acceptance;
mod support;

use std::{
    io::Write as _,
    net::SocketAddr,
    process::{Command, ExitStatus, Output, Stdio},
};

use assert2::{assert, check};
use jvm_acceptance::{ClientPropsFile, KAFKA_IMAGE_TXN, plain_jaas, write_client_props};
use krabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId, config::ListenerSpec,
};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_core::{Client, security::ClientSecurity};
use krabka_log::LogConfig;
use krabka_protocol::krabka::{
    break_glass::{ApproveBreakGlassRequest, ProposeBreakGlassRequest},
    freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED, SetTopicFreezeRequest},
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

// ── The wire vocabulary these cases pin ──────────────────────────────────────

/// The JVM exception that `Errors.forCode` gives `POLICY_VIOLATION` (44).
///
/// The fully-qualified name is the assertion, and not the bare class name. A
/// bare `PolicyViolationException` would also match a sentence that merely
/// mentioned it, and the point of the check is that Kafka's own client
/// constructed this class.
const POLICY_VIOLATION_EXCEPTION: &str = "org.apache.kafka.common.errors.PolicyViolationException";

/// The JVM exception that `Errors.forCode` gives `INVALID_CONFIG` (40).
const INVALID_CONFIG_EXCEPTION: &str =
    "org.apache.kafka.common.errors.InvalidConfigurationException";

/// The break-glass action name that `break_glass.signed_actions`, the audit
/// event, the metric label and the refusal message all spell one way.
const ACTION_DELETE_TOPIC: &str = "delete_topic";

/// The action name of an unclean `ElectLeaders`.
const ACTION_UNCLEAN_ELECT_LEADERS: &str = "unclean_elect_leaders";

/// The wire value of `BreakGlassAction::UncleanElectLeaders` on the
/// krabka-private `ProposeBreakGlass` request (api key 1017).
///
/// The broker's own mapping is crate-private, so the value is written out
/// here. It is part of the private API's contract, and a change to it that
/// this constant did not follow would show up as a proposal that authorizes
/// nothing.
const WIRE_UNCLEAN_ELECT_LEADERS: i8 = 2;

/// Where [`ClientPropsFile::mount_str`] puts the properties file inside the
/// container, so every JVM tool flag can name a fixed path.
const CONTAINER_PROPS: &str = "/client.properties";

/// The listener name the SASL case gives its one listener.
const SASL_LISTENER: &str = "SASL_PLAINTEXT";

// ── Topics, scopes and the operators ─────────────────────────────────────────

/// The topic that a literal-scope freeze covers.
const LITERAL_TOPIC: &str = "kfc9-orders";
/// What the operator typed when they froze [`LITERAL_TOPIC`]. It rides in the
/// refusal, so the JVM producer must print it back.
const LITERAL_REASON: &str = "DR cutover";

/// The namespace a prefixed-scope freeze covers.
const PREFIX_SCOPE: &str = "kfc9-tenant-a.";
/// A topic inside [`PREFIX_SCOPE`]. Its own name is in no registry entry, so
/// refusing it exercises the prefix index rather than the literal one.
const PREFIX_TOPIC: &str = "kfc9-tenant-a.events";
/// What the operator typed when they froze [`PREFIX_SCOPE`].
const PREFIX_REASON: &str = "tenant offboarding";

/// The unfrozen control topic. It exists so that a refusal is shown to be the
/// freeze rather than a produce path that stopped working.
const CONTROL_TOPIC: &str = "kfc9-control";

/// The topic the `kafka-topics --delete` case creates and fails to delete.
const DOOMED_TOPIC: &str = "kfc9-doomed";

/// The topic the `kafka-leader-election` case elects a leader for.
const ELECT_TOPIC: &str = "kfc9-elect";

/// The topic the `kafka-configs` case freezes, describes and fails to alter.
const CONFIGS_TOPIC: &str = "kfc9-configs";
/// What the operator typed when they froze [`CONFIGS_TOPIC`].
const CONFIGS_REASON: &str = "config-path check";

/// The operator who opens the break-glass proposal. They may not approve it.
const PROPOSER: (&str, &str) = ("alice", "alice-secret");
/// The first approving operator.
const APPROVER_ONE: (&str, &str) = ("bob", "bob-secret");
/// The second approving operator. Two distinct principals is what makes the
/// rule a two-person rule rather than a two-click rule.
const APPROVER_TWO: (&str, &str) = ("carol", "carol-secret");

/// Every operator, in the `KafkaPrincipal` spelling `break_glass.approvers`
/// takes.
///
/// The proposer is in the set because the broker refuses a proposal from a
/// principal outside it: a proposer who is a stranger, with two approvers,
/// would make a rule about three people into a rule about two people and a
/// stranger.
fn approver_set() -> Vec<String> {
    [PROPOSER, APPROVER_ONE, APPROVER_TWO]
        .iter()
        .map(|(user, _)| format!("User:{user}"))
        .collect()
}

// ── The refusals the broker words, rebuilt on this side ──────────────────────

/// The `error_message` that rides beside `POLICY_VIOLATION` on a produce to a
/// frozen topic.
///
/// `pattern` is `literal` or `prefixed`, and the scope is quoted because the
/// broker renders it with its `Debug` form. The quotes are part of what the
/// operator reads, so they are part of the assertion.
fn freeze_refusal(pattern: &str, scope: &str, reason: &str) -> String {
    format!("a write freeze on the {pattern} scope {scope:?} refuses this write: {reason}")
}

/// The `error_message` that rides beside `POLICY_VIOLATION` when the
/// two-person rule finds no approval at all.
///
/// This is the `NoProposal` wording specifically. A proposal that exists but
/// is short of approvals, withdrawn, expired or already spent gets a different
/// sentence, and asserting this one is what shows the tool reached the gate
/// with an empty registry rather than tripping over a half-built proposal.
fn no_proposal_refusal(action: &str, target: &str) -> String {
    format!("break-glass refused {action} on {target}: no approved proposal covers the request")
}

/// The refusal both alter paths give for the read-only `write.freeze` key.
///
/// KFC-9 requires the message to name the command that does set the key,
/// because a refusal with no next step leaves an operator stuck mid-incident.
const WRITE_FREEZE_ALTER_REFUSAL: &str = "topic config write.freeze is controller-managed and read-only; \
     use `krabka-guard freeze set` to set it and `krabka-guard freeze clear` to clear it";

/// The `write.freeze` value a `DescribeConfigs` reports for a topic frozen by
/// its own name, in the `frozen:<pattern>:<scope>` form KFC-9 specifies.
fn write_freeze_value(scope: &str) -> String {
    format!("write.freeze=frozen:literal:{scope}")
}

// ── Running a JVM tool that is allowed to fail ───────────────────────────────

/// One JVM tool run: how it exited, and everything it printed.
#[derive(Debug)]
struct ToolRun {
    /// The container's exit status. Three of the four cases here are about a
    /// non-zero one, which is why this suite cannot use
    /// [`jvm_acceptance::docker_run_kafka_tool_with_image`]: that helper
    /// asserts success.
    status: ExitStatus,
    /// stdout followed by stderr. The JVM tools split a failure across the two
    /// in ways that differ per tool -- `kafka-topics` prints the message on
    /// stdout and the stack trace on stderr, an uncaught exception goes only
    /// to stderr -- so both are searched as one text.
    output: String,
}

impl ToolRun {
    /// Merge one finished `docker run` into a run, and echo it for
    /// `--nocapture`.
    fn from_output(out: &Output, args: &[&str]) -> Self {
        let mut output = String::from_utf8_lossy(&out.stdout).into_owned();
        output.push_str(&String::from_utf8_lossy(&out.stderr));
        let status = out.status;
        eprintln!("KRABKA[test] jvm tool {args:?} status={status}\n{output}");
        Self { status, output }
    }

    /// Whether the tool exited zero.
    fn succeeded(&self) -> bool {
        self.status.success()
    }

    /// Whether the tool printed `needle` on either stream.
    fn says(&self, needle: &str) -> bool {
        self.output.contains(needle)
    }
}

/// Run one Kafka CLI tool in a cp-kafka container, and hand back what it did.
///
/// This mirrors [`jvm_acceptance::docker_run_kafka_tool_with_image`] -- same
/// image, same `--add-host` mapping onto the bridge gateway, same capture of
/// both streams -- and differs in the one way this suite needs: it does not
/// assert that the tool succeeded.
///
/// `stdin`, when given, adds `-i` and feeds the text in. Without it the
/// container has no stdin at all, and `kafka-console-producer` then reads EOF
/// immediately, produces nothing, and exits zero -- which would pass the
/// frozen-topic case for entirely the wrong reason.
///
/// The container is named rather than anonymous so that a run killed
/// mid-flight leaves something an operator can find and remove.
fn run_tool(props: Option<&ClientPropsFile>, stdin: Option<&str>, args: &[&str]) -> ToolRun {
    let name = support::unique_container_name("kfc9-jvm");
    let mut command = Command::new("docker");
    command.args(["run", "--rm", "--name", &name]);
    if stdin.is_some() {
        command.arg("-i");
    }
    command.arg("--add-host=host.docker.internal:host-gateway");
    if let Some(props) = props {
        command.arg("-v").arg(props.mount_str());
    }
    command
        .arg(KAFKA_IMAGE_TXN)
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
                .expect("write the record to the tool's stdin");
            drop(child.stdin.take());
            child.wait_with_output().expect("wait for docker run")
        }
    };
    ToolRun::from_output(&out, args)
}

/// Produce one record to `topic` with `kafka-console-producer`.
///
/// `--sync` is load-bearing, and not a stylistic choice. Without it the
/// console producer hands each send to an error-logging callback, prints the
/// failure, and exits zero; with it the producer awaits the future, the
/// broker's refusal surfaces as an `ExecutionException`, and the tool exits
/// one. An exit code is the only part of this that a runbook can branch on.
fn jvm_produce(
    bootstrap: &str,
    topic: &str,
    props: Option<&ClientPropsFile>,
    record: &str,
) -> ToolRun {
    let mut args = vec![
        "kafka-console-producer",
        "--bootstrap-server",
        bootstrap,
        "--topic",
        topic,
        "--sync",
    ];
    if props.is_some() {
        args.extend_from_slice(&["--producer.config", CONTAINER_PROPS]);
    }
    run_tool(props, Some(&format!("{record}\n")), &args)
}

/// Ask `kafka-leader-election` for an unclean election of partition 0.
///
/// Preferred election is not gated, so the election type is the whole of what
/// puts this request in front of the two-person rule.
fn jvm_unclean_election(bootstrap: &str, props: &ClientPropsFile) -> ToolRun {
    run_tool(
        Some(props),
        None,
        &[
            "kafka-leader-election",
            "--bootstrap-server",
            bootstrap,
            "--election-type",
            "unclean",
            "--topic",
            ELECT_TOPIC,
            "--partition",
            "0",
            "--admin.config",
            CONTAINER_PROPS,
        ],
    )
}

/// Read one topic's dynamic configs with `kafka-configs --describe`.
fn jvm_describe_configs(bootstrap: &str) -> ToolRun {
    run_tool(
        None,
        None,
        &[
            "kafka-configs",
            "--bootstrap-server",
            bootstrap,
            "--entity-type",
            "topics",
            "--entity-name",
            CONFIGS_TOPIC,
            "--describe",
        ],
    )
}

// ── The broker under test ────────────────────────────────────────────────────

/// One broker on the host, addressed the two ways this suite needs.
struct JvmBroker {
    /// Dropping the handle stops the broker, so the case owns it to the end.
    _handle: BrokerHandle,
    /// The log directory, which outlives the broker only by a moment.
    _dir: TempDir,
    /// Loopback. The host-side clients in this file dial this.
    host: String,
    /// `host.docker.internal`. The containers bootstrap against this, and the
    /// broker advertises it in `Metadata`.
    container: String,
}

/// Boot one broker that the cp-kafka containers can reach.
///
/// The shape follows [`jvm_acceptance::start_host_broker_with`], with one
/// difference: the listeners come from [`support::JvmListeners::allocate`]
/// rather than from the process-wide set, so two cases in this binary never
/// contend for a port.
///
/// `adjust` sees a config whose addresses are already filled in, which is what
/// lets [`sasl_listener`] build a listener spec on the same bind and
/// advertised addresses.
async fn start_jvm_broker(adjust: impl FnOnce(&mut BrokerConfig)) -> JvmBroker {
    support::init_tracing();
    let listeners = support::JvmListeners::allocate();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen: SocketAddr = listeners
        .listen
        .parse()
        .expect("an allocated listen address");
    let controller: SocketAddr = listeners
        .controller
        .parse()
        .expect("an allocated controller address");

    let mut config = BrokerConfig {
        broker_id: 1,
        listen_addr: listen,
        advertised_listener: listeners.advertised.clone(),
        log_dir: dir.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: NodeId(1),
        controller_listen_addr: controller,
        controller_quorum_voters: vec![(NodeId(1), controller.to_string())],
        heartbeat_interval: krabka_units::millis(3_000),
        heartbeat_timeout: krabka_units::millis(9_000),
        replica_lag_time_max: krabka_units::millis(30_000),
        controller_election_timeout: krabka_units::secs(5),
        controller_heartbeat_interval: krabka_units::millis(500),
        bootstrap_mode: BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    adjust(&mut config);

    // `Broker::start` waits for a metadata leader before it returns, so the
    // control-plane writes below need no retry loop of their own.
    let handle = Broker::start(config).await.expect("start broker");
    JvmBroker {
        _handle: handle,
        _dir: dir,
        host: format!("127.0.0.1:{}", listen.port()),
        container: listeners.advertised,
    }
}

/// Turn the broker's one listener into `SASL_PLAINTEXT`/`PLAIN` over the same
/// addresses, and install `users` as PLAIN credentials.
///
/// The break-glass case needs this and the other three do not. Over a
/// plaintext listener every connection authenticates as one anonymous
/// principal, which can prove that the gate refuses and can never prove that
/// two distinct people got past it.
fn sasl_listener(config: &mut BrokerConfig, users: &[(&str, &str)]) {
    config.listeners = vec![ListenerSpec {
        name: SASL_LISTENER.to_owned(),
        bind_addr: config.listen_addr,
        advertised: config.advertised_listener.clone(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    SASL_LISTENER.clone_into(&mut config.inter_broker_listener_name);
    config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (user, pass) in users {
        config
            .plain_credentials
            .insert((*user).to_owned(), (*pass).to_owned());
    }
}

/// Turn the break-glass two-person rule on, with no signature demanded.
///
/// An empty `approvers` turns the whole workflow off, so naming the set is
/// what puts the five gated transitions behind an approval. `signed_actions`
/// is emptied explicitly rather than left to the default: the file-config
/// default names three actions, and this suite has no operator key material to
/// sign with. What it asserts is what the JVM tool sees, and a signature
/// changes nothing about that.
fn gate_on(config: &mut BrokerConfig) {
    config.break_glass.approvers = approver_set();
    config.break_glass.signed_actions = Vec::new();
}

/// The JVM client properties for one PLAIN operator.
fn sasl_props(user: &str, pass: &str) -> String {
    format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(user, pass)
    )
}

// ── The host-side control plane ──────────────────────────────────────────────

/// A plaintext host-side client for the krabka-private APIs.
async fn plain_client(bootstrap: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id("kfc9-jvm-acceptance")
        .build()
        .await
        .expect("client build")
}

/// Create every topic a case needs, and fail the case when one does not open.
async fn create_topics(bootstrap: &str, security: Option<ClientSecurity>, names: &[&str]) {
    let mut admin = AdminClient::connect_secured(&[bootstrap.to_owned()], security)
        .await
        .expect("admin connect");
    let specs: Vec<CreateTopicSpec> = names
        .iter()
        .map(|name| CreateTopicSpec {
            name: (*name).to_owned(),
            partitions: 1,
            replicas: 1,
            configs: std::collections::BTreeMap::default(),
        })
        .collect();
    let outcomes = admin
        .create_topics(&specs, krabka_units::secs(30))
        .await
        .expect("create topics");
    for outcome in outcomes {
        let name = outcome.name;
        let error = outcome.error;
        assert!(error.is_none(), "create topic {name}: {error:?}");
    }
}

/// Freeze one scope through the krabka-private `SetTopicFreeze` (api key
/// 1015).
///
/// The request is unsigned, which the broker accepts for a freeze while
/// `freeze.require_signature` is off. A freeze is the safe direction, and
/// KFC-9 keeps it reachable in one command on a cluster with no key material.
async fn freeze(client: &Client, scope: &str, pattern_type: i8, reason: &str) {
    let response = client
        .send(SetTopicFreezeRequest {
            scope: scope.to_owned(),
            pattern_type,
            frozen: true,
            reason: reason.to_owned(),
            ..SetTopicFreezeRequest::default()
        })
        .await
        .expect("SetTopicFreeze");
    let code = response.error_code;
    let message = response.error_message;
    assert!(code == 0, "freeze {scope}: code={code} message={message:?}");
}

/// How far along a proposal is after one approval.
#[derive(Debug)]
struct Approvals {
    /// Distinct principals that have approved it.
    held: i32,
    /// Distinct principals it needs. The broker refuses a configured value
    /// below two.
    required: i32,
}

/// Open a break-glass proposal as `PROPOSER`, and have both approvers sign off.
///
/// The target is the bare topic name rather than `<topic>-<partition>`. KFC-9
/// lets a proposal on a topic cover every partition of it for the actions that
/// name a partition, and an unclean election is one of those, so this also
/// checks that widening on the way through.
async fn approved_unclean_election(bootstrap: &str, target: &str) {
    let proposer = support::sasl_client(bootstrap, PROPOSER.0, PROPOSER.1).await;
    let opened = proposer
        .send(ProposeBreakGlassRequest {
            action: WIRE_UNCLEAN_ELECT_LEADERS,
            target: target.to_owned(),
            reason: "the whole ISR is gone and the site has to come back".to_owned(),
            ttl_ms: 0,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass");
    let code = opened.error_code;
    let message = opened.error_message;
    assert!(code == 0, "propose: code={code} message={message:?}");

    let first = approve(bootstrap, APPROVER_ONE, opened.proposal_id).await;
    check!(
        first.held == 1,
        "one approval is one distinct principal, not {first:?}"
    );
    check!(
        first.held < first.required,
        "one person must not be enough: {first:?}"
    );

    let second = approve(bootstrap, APPROVER_TWO, opened.proposal_id).await;
    check!(
        second.held == second.required,
        "two distinct principals must satisfy the rule: {second:?}"
    );
}

/// Add one approval to a proposal as `operator`.
async fn approve(
    bootstrap: &str,
    operator: (&str, &str),
    proposal_id: krabka_protocol::primitives::uuid::Uuid,
) -> Approvals {
    let client = support::sasl_client(bootstrap, operator.0, operator.1).await;
    let response = client
        .send(ApproveBreakGlassRequest {
            proposal_id,
            withdraw: false,
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass");
    let code = response.error_code;
    let message = response.error_message;
    let who = operator.0;
    assert!(
        code == 0,
        "approve as {who}: code={code} message={message:?}"
    );
    Approvals {
        held: response.approvals_held,
        required: response.approvals_required,
    }
}

// ── The cases ────────────────────────────────────────────────────────────────

/// A produce to a frozen topic reaches `kafka-console-producer` as a
/// `PolicyViolationException`, and an unfrozen topic is untouched.
///
/// This is the case KFC-9's compatibility section turns on. A stock producer
/// must fail the batch, must not retry, and must surface the broker's own
/// sentence -- which names the scope that matched, because a topic can be
/// frozen by its own name or by a prefix over a thousand topics and the thaw
/// is a different command in each case.
///
/// The three rows differ only in the topic the same tool writes to. The
/// prefixed row matters on its own: its topic appears in no registry entry by
/// name, so it exercises the prefix index rather than the literal one. The
/// control row is what keeps the other two honest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn the_jvm_console_producer_reads_a_freeze_as_a_policy_violation() {
    let broker = start_jvm_broker(|_| {}).await;
    let client = plain_client(&broker.host).await;
    create_topics(
        &broker.host,
        None,
        &[LITERAL_TOPIC, PREFIX_TOPIC, CONTROL_TOPIC],
    )
    .await;
    freeze(&client, LITERAL_TOPIC, PATTERN_TYPE_LITERAL, LITERAL_REASON).await;
    freeze(&client, PREFIX_SCOPE, PATTERN_TYPE_PREFIXED, PREFIX_REASON).await;

    for (label, topic, refusal) in [
        (
            "a topic frozen by its own name",
            LITERAL_TOPIC,
            Some(freeze_refusal("literal", LITERAL_TOPIC, LITERAL_REASON)),
        ),
        (
            "a topic frozen by the namespace above it",
            PREFIX_TOPIC,
            Some(freeze_refusal("prefixed", PREFIX_SCOPE, PREFIX_REASON)),
        ),
        ("the unfrozen control topic", CONTROL_TOPIC, None),
    ] {
        let run = jvm_produce(&broker.container, topic, None, "a record");
        if let Some(message) = refusal {
            check!(
                !run.succeeded(),
                "{label}: a refused synchronous send must exit non-zero"
            );
            check!(
                run.says(POLICY_VIOLATION_EXCEPTION),
                "{label}: the producer must name the policy violation, got {run:?}"
            );
            check!(
                run.says(&message),
                "{label}: the producer must print {message:?}, got {run:?}"
            );
        } else {
            check!(
                run.succeeded(),
                "{label}: an unfrozen topic still takes writes, got {run:?}"
            );
            check!(
                !run.says(POLICY_VIOLATION_EXCEPTION),
                "{label}: no freeze covers this topic, got {run:?}"
            );
        }
    }
}

/// `kafka-topics --delete` with no break-glass proposal fails, and carries the
/// broker's own refusal.
///
/// The tool holds every right Kafka asks for and the cluster is healthy, so
/// nothing in the Kafka protocol explains the failure except the message. That
/// makes the message the feature, and a response that dropped `error_message`
/// would still carry the right code while telling the operator nothing.
///
/// The create and the list around the delete are the control. The same binary,
/// against the same broker, must still create a topic and still list it
/// afterwards -- so a refusal here is the gate, and the topic really did
/// survive it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn kafka_topics_delete_carries_the_brokers_break_glass_refusal() {
    let broker = start_jvm_broker(gate_on).await;
    let bootstrap = broker.container.as_str();

    let created = run_tool(
        None,
        None,
        &[
            "kafka-topics",
            "--bootstrap-server",
            bootstrap,
            "--create",
            "--topic",
            DOOMED_TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
    );
    check!(
        created.succeeded(),
        "creating a topic is not gated, got {created:?}"
    );

    let deleted = run_tool(
        None,
        None,
        &[
            "kafka-topics",
            "--bootstrap-server",
            bootstrap,
            "--delete",
            "--topic",
            DOOMED_TOPIC,
        ],
    );
    let refusal = no_proposal_refusal(ACTION_DELETE_TOPIC, DOOMED_TOPIC);
    check!(
        !deleted.succeeded(),
        "a gated delete with no approval must exit non-zero"
    );
    check!(
        deleted.says(POLICY_VIOLATION_EXCEPTION),
        "the delete must name the policy violation, got {deleted:?}"
    );
    check!(
        deleted.says(&refusal),
        "the delete must print {refusal:?}, got {deleted:?}"
    );

    let listed = run_tool(
        None,
        None,
        &["kafka-topics", "--bootstrap-server", bootstrap, "--list"],
    );
    check!(listed.succeeded(), "listing is not gated, got {listed:?}");
    check!(
        listed.says(DOOMED_TOPIC),
        "the refusal must have left the topic in place, got {listed:?}"
    );
}

/// `kafka-leader-election --election-type unclean` fails without an approved
/// proposal, and stops failing once two people approve one.
///
/// This is the case that proves an approval is a standing authorization rather
/// than a request field. `kafka-leader-election` sends the stock `ElectLeaders`
/// that KIP-460 defines, with nowhere to put a proposal id, and the operator
/// gets the approval out of band through the krabka-private APIs. The tool is
/// byte-for-byte the same on both runs; only the metadata image differs.
///
/// The success run exits zero rather than electing anything. The partition is
/// healthy on a single node, so an unclean election answers
/// `ELECTION_NOT_NEEDED` (84), which `LeaderElectionCommand` counts as a no-op
/// and not a failure. That is the honest signal available here: the request
/// got past the two-person rule, which is the whole of what the approval
/// changed. The assertions say exactly that and no more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn kafka_leader_election_needs_an_approved_proposal_for_an_unclean_election() {
    let users = [PROPOSER, APPROVER_ONE, APPROVER_TWO];
    let broker = start_jvm_broker(|config| {
        sasl_listener(config, &users);
        gate_on(config);
    })
    .await;
    let props = write_client_props(&sasl_props(PROPOSER.0, PROPOSER.1));
    create_topics(
        &broker.host,
        Some(support::sasl_plain_security(PROPOSER.0, PROPOSER.1)),
        &[ELECT_TOPIC],
    )
    .await;

    let target = format!("{ELECT_TOPIC}-0");
    let refusal = no_proposal_refusal(ACTION_UNCLEAN_ELECT_LEADERS, &target);
    let refused = jvm_unclean_election(&broker.container, &props);
    check!(
        !refused.succeeded(),
        "an unclean election with no approval must exit non-zero"
    );
    check!(
        refused.says(POLICY_VIOLATION_EXCEPTION),
        "the election must name the policy violation, got {refused:?}"
    );
    check!(
        refused.says(&refusal),
        "the election must print {refusal:?}, got {refused:?}"
    );

    approved_unclean_election(&broker.host, ELECT_TOPIC).await;

    let allowed = jvm_unclean_election(&broker.container, &props);
    check!(
        allowed.succeeded(),
        "an approved election must exit zero, got {allowed:?}"
    );
    check!(
        !allowed.says(POLICY_VIOLATION_EXCEPTION),
        "an approved election must not be refused, got {allowed:?}"
    );
    check!(
        !allowed.says("break-glass refused"),
        "an approved election must not be refused, got {allowed:?}"
    );
    check!(
        allowed.says(&target),
        "the tool must report on the partition it was asked about, got {allowed:?}"
    );
}

/// `kafka-configs` reads the freeze and cannot write it.
///
/// KFC-9 synthesises a read-only `write.freeze` topic config so that an
/// operator holding only the JVM tools can see a freeze at all: they cannot
/// call `DescribeTopicFreezes`, and the value is the one place the scope that
/// froze the topic is legible to them. The other half of the rule is that the
/// key is never writable, because a key that could be set through
/// `AlterConfigs` would put the freeze registry behind an ordinary topic-config
/// ACL and let a snapshot restore resurrect a stale freeze.
///
/// The three alter rows differ only in the operation the same tool sends: set
/// a freeze, clear one, and delete the key. All three are refused with one
/// wording, and the `--delete-config` row reaches the broker at all only
/// because the frozen topic reports the key as a dynamic config, which is what
/// `kafka-configs` checks before it sends a delete.
///
/// The describe after the loop is the proof that the refusals refused. Three
/// rejected alters that had quietly changed the registry would pass every
/// assertion above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn kafka_configs_reads_the_freeze_and_cannot_write_it() {
    let broker = start_jvm_broker(|_| {}).await;
    let client = plain_client(&broker.host).await;
    create_topics(&broker.host, None, &[CONFIGS_TOPIC]).await;
    freeze(&client, CONFIGS_TOPIC, PATTERN_TYPE_LITERAL, CONFIGS_REASON).await;

    let frozen = write_freeze_value(CONFIGS_TOPIC);
    let described = jvm_describe_configs(&broker.container);
    check!(
        described.succeeded(),
        "describing a frozen topic is not gated, got {described:?}"
    );
    check!(
        described.says(&frozen),
        "the describe must show {frozen:?}, got {described:?}"
    );

    for (label, flag, value) in [
        (
            "set a freeze through the config key",
            "--add-config",
            "write.freeze=true",
        ),
        (
            "clear a freeze through the config key",
            "--add-config",
            "write.freeze=false",
        ),
        (
            "delete the config key outright",
            "--delete-config",
            "write.freeze",
        ),
    ] {
        let run = run_tool(
            None,
            None,
            &[
                "kafka-configs",
                "--bootstrap-server",
                &broker.container,
                "--entity-type",
                "topics",
                "--entity-name",
                CONFIGS_TOPIC,
                "--alter",
                flag,
                value,
            ],
        );
        check!(!run.succeeded(), "{label}: must exit non-zero");
        check!(
            run.says(INVALID_CONFIG_EXCEPTION),
            "{label}: must name the invalid configuration, got {run:?}"
        );
        check!(
            run.says(WRITE_FREEZE_ALTER_REFUSAL),
            "{label}: must name the command that does set the key, got {run:?}"
        );
    }

    let after = jvm_describe_configs(&broker.container);
    check!(
        after.says(&frozen),
        "the refusals must have left the freeze exactly as it was, got {after:?}"
    );
}
