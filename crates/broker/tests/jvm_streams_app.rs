//! A compiled Kafka Streams application, run against krabka.
//!
//! `jvm_streams_groups.rs` drives the KIP-1071 admin surface with the native
//! client and points the JVM `kafka-streams-groups.sh` tool at the result. It
//! cannot run a topology: `mirror.gcr.io/apache/kafka:4.1.0` is JRE-only. This
//! suite runs the real thing. [`STREAMS_APP_JAVA`] is compiled inside
//! [`KAFKA_IMAGE_TXN`], which ships `javac` beside the Kafka 3.5 jars, and the
//! resulting classes are then run against krabka.
//!
//! Two runs, one class file:
//!
//! - [`compiled_streams_app_runs_on_the_classic_protocol`] runs it in the same
//!   `cp-kafka:7.5.0` container it was compiled in, on the classic rebalance
//!   protocol. This is `StreamsPartitionAssignor` carrying Streams' own
//!   subscription and assignment userdata through `JoinGroup`/`SyncGroup`,
//!   `InternalTopicManager` creating the repartition and changelog topics, and
//!   -- under `processing.guarantee=exactly_once_v2` -- one transactional
//!   producer per stream thread writing every sink and changelog record inside
//!   a transaction.
//!
//! - [`compiled_streams_app_runs_on_the_streams_protocol`] mounts the same
//!   classes into `mirror.gcr.io/apache/kafka:4.3.1` and runs them with
//!   `group.protocol=streams`, which is KIP-1071: the app sends its topology in
//!   `StreamsGroupHeartbeat` rather than in an assignor's userdata. `cp-kafka:
//!   7.5.0` is Kafka 3.5 and has no such protocol, so the class has to cross
//!   images; it is compiled with `--release 11`, and `group.protocol` is set as
//!   a raw key, because Kafka 3.5 has no constant for it.
//!
//! The topology is `source -> selectKey -> repartition -> windowed count ->
//! sink`, so one run creates both internal topic shapes Kafka Streams asks a
//! broker for. What the suite then asserts on them is exactly what
//! `RepartitionTopicConfig`, `WindowedChangelogTopicConfig` and
//! `InternalTopicConfig` set:
//!
//! | topic | configs |
//! | --- | --- |
//! | `<app>-shuffle-repartition` | `cleanup.policy=delete`, `retention.ms=-1`, `message.timestamp.type=CreateTime` |
//! | `<app>-krabka-counts-changelog` | `cleanup.policy=compact,delete`, `message.timestamp.type=CreateTime` |
//!
//! Networking is the `jvm_acceptance` harness's: the broker binds an allocated
//! port on `0.0.0.0` and advertises `host.docker.internal:<port>`, which the
//! containers resolve through `--add-host=host.docker.internal:host-gateway`.
//!
//! Gated `#[ignore = "requires Docker"]`; the Bazel `docker` lane runs it with
//! `--ignored --test-threads=1`.
#![cfg(test)]

mod jvm_acceptance;
mod support;

use {
    crate::{
        jvm_acceptance::{create_console_topic_at, describe_console_topic_configs},
        support::client::connect_owned,
    },
    assert2::assert,
    jvm_acceptance::{
        KAFKA_IMAGE_TXN, STREAMS_APP_JAVA, broker0_advertised, docker_run_kafka_tool_with_image,
        rlmm_broker0_advertised, start_host_broker,
    },
    std::{
        io::Write,
        path::Path,
        process::{Command, Output, Stdio},
    },
};

/// The Apache Kafka image whose jars carry KIP-1071. It is JRE-only, which is
/// why the class it runs is compiled elsewhere.
const KAFKA_IMAGE_STREAMS_PROTOCOL: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// The classpath expression for the Confluent image, as
/// `jvm_acceptance_durability::transactional_eos` builds it.
const CP_CONFLUENT: &str = r"$(ls /usr/share/java/kafka/*.jar | tr '\n' ':')$(ls /usr/share/java/cp-base-new/*.jar | tr '\n' ':')";

/// The classpath expression for the Apache Kafka image, whose tarball layout
/// puts every jar in one directory.
const CP_APACHE: &str = r"$(ls /opt/kafka/libs/*.jar | tr '\n' ':')";

/// What the topology emits, in order, for the five seeded input records
/// (`alpha alpha beta alpha beta`, all at timestamps inside one ten-minute
/// window). A windowed `count()` with no suppression and the record cache
/// disabled emits the running count on every update, so the sink holds one
/// record per input record.
/// The window store the topology names in its `Materialized`, and so the
/// middle of its changelog topic's name. It is named because the topology asks
/// for an in-memory store; an unnamed one would be
/// `KSTREAM-AGGREGATE-STATE-STORE-<n>`.
const WINDOW_STORE_NAME: &str = "krabka-counts";

/// The JVM linkage failures that mean the Kafka-3.5-compiled class cannot run
/// against the 4.3 jars. The KIP-1071 case reports these rather than failing:
/// they say the cross-image compile did not work out, not that krabka
/// answered wrongly.
const LINKAGE_ERRORS: [&str; 4] = [
    "NoSuchMethodError",
    "NoClassDefFoundError",
    "NoSuchFieldError",
    "UnsupportedClassVersionError",
];

/// Compile [`STREAMS_APP_JAVA`] into `dest` on the host.
///
/// The compile happens inside [`KAFKA_IMAGE_TXN`] -- the only pinned image
/// that ships a JDK alongside Kafka jars -- with `dest` bind-mounted at
/// `/out`, so both runs below share one class file.
///
/// `--release 11` rather than a bare compile: the class has to load under the
/// Apache image's JRE 21 as well as run under the Confluent image's own JVM,
/// and 11 is the floor both Kafka 3.5 and Kafka 4.3 support. A class file
/// built for an older release loads on a newer JVM; the reverse does not.
fn compile_streams_app(dest: &Path) {
    let mut javac = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &format!("{}:/out", dest.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE_TXN,
            "-c",
            &format!(
                r#"set -e; cat >/tmp/StreamsApp.java; \
                   CP={CP_CONFLUENT}; \
                   javac --release 11 -cp "$CP" -d /out /tmp/StreamsApp.java; \
                   ls -l /out"#
            ),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn javac container");
    javac
        .stdin
        .as_mut()
        .expect("javac stdin")
        .write_all(STREAMS_APP_JAVA.as_bytes())
        .expect("write the Streams app source");
    drop(javac.stdin.take());
    let out = javac.wait_with_output().expect("wait for javac");
    eprintln!(
        "KRABKA[test] javac status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "compiling the Streams app failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Run the compiled class in `image`, with `classes` mounted read-only and
/// `classpath` naming where that image keeps its jars.
///
/// Both streams and stderr are echoed whatever the exit status: this is the
/// only place a Streams failure -- a refused internal topic, a transaction
/// that never initialised, an assignment that never arrived -- is written
/// down, and the caller decides what to make of it.
fn run_streams_app(image: &str, classes: &Path, classpath: &str, args: &[&str]) -> Output {
    let mut cmd = Command::new("docker");
    cmd.args([
        "run",
        "--rm",
        "-v",
        &format!("{}:/classes:ro", classes.display()),
        "--add-host=host.docker.internal:host-gateway",
        "--entrypoint",
        "bash",
        image,
        "-c",
        &format!(r#"set -e; CP={classpath}; java -cp "/classes:$CP" StreamsApp "$@""#),
        "--",
    ])
    .args(args)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    let out = cmd.output().expect("spawn the Streams app container");
    eprintln!(
        "KRABKA[test] StreamsApp image={image} args={args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// A host-side directory the container's non-root user can write class files
/// into. `tempfile` makes it `0700`, which the image's `appuser` cannot use.
fn shared_class_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    #[cfg(unix)]
    {
        use crate::support::chmod_for_container;

        chmod_for_container(dir.path(), 0o777, "chmod the class directory");
    }
    dir
}

/// Create a one-partition topic through the JVM `kafka-topics` tool.
///
/// Streams creates its own internal topics, but not the source and sink
/// topics of the topology: `builder.stream(...)` and `.to(...)` both require
/// the topic to exist already.
fn create_topic(bootstrap: &str, topic: &str) { create_console_topic_at(bootstrap, topic); }

/// Every topic the broker holds, one per line.
fn list_topics(bootstrap: &str) -> Vec<String> {
    let out = docker_run_kafka_tool_with_image(KAFKA_IMAGE_TXN, &[
        "kafka-topics",
        "--list",
        "--bootstrap-server",
        bootstrap,
    ]);
    support::jvm_output_lines(&out)
}

/// `kafka-configs --describe --entity-type topics` for one topic, as text.
///
/// The tool prints only the configs that were explicitly set, which is what
/// makes it the right reading: every key asserted below is one Kafka Streams
/// sent on the `CreateTopics` request, so a broker that dropped it renders
/// nothing rather than a default.
fn describe_topic_configs(bootstrap: &str, topic: &str) -> String {
    describe_console_topic_configs(bootstrap, topic)
}

/// Read the sink topic under `read_committed`, which is the isolation level
/// `exactly_once_v2` output is only visible at.
fn read_committed_sink(bootstrap: &str, topic: &str) -> Vec<String> {
    let out = docker_run_kafka_tool_with_image(KAFKA_IMAGE_TXN, &[
        "kafka-console-consumer",
        "--bootstrap-server",
        bootstrap,
        "--topic",
        topic,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        "5",
        "--timeout-ms",
        "60000",
    ]);
    support::jvm_output_lines(&out)
}

/// Finalize `streams.version` to level 1, so the KIP-1071 heartbeat handlers
/// stop answering `UNSUPPORTED_VERSION`. `upgrade_type: 1` is UPGRADE, the
/// same call `jvm_streams_groups.rs` makes.
async fn finalize_streams_version() {
    let client = connect_owned(
        rlmm_broker0_advertised(),
        "krabka-streams-app-test",
        "client build",
    )
    .await;
    support::streams::finalize_streams_version(&client).await;
}

use {
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::collections::BTreeMap,
    tempfile::TempDir,
};
#[derive(Clone, Copy)]
enum StreamsRuntime {
    Classic,
    Streams,
}
struct StreamsApplication {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    classes: Option<TempDir>,
    runtime: Option<StreamsRuntime>,
    input: String,
    output: String,
    application_id: String,
    run: Option<Output>,
    sink: Option<Vec<String>>,
    topics: Vec<String>,
    configs: BTreeMap<String, String>,
}
#[fixture]
fn application() -> StreamsApplication {
    // Start this scenario with fresh inputs and observations.

    StreamsApplication {
        broker: None,
        directory: None,
        classes: None,
        runtime: None,
        input: String::new(),
        output: String::new(),
        application_id: String::new(),
        run: None,
        sink: None,
        topics: Vec::new(),
        configs: BTreeMap::new(),
    }
}
#[given("a broker is ready for the compiled windowed count Streams application")]
async fn broker(application: &mut StreamsApplication) {
    let (broker, directory) = start_host_broker().await;
    application.broker = Some(broker);
    application.directory = Some(directory);
}
#[given("Kafka 3.5 runs the application with the classic group protocol")]
fn classic(application: &mut StreamsApplication) {
    application.runtime = Some(StreamsRuntime::Classic);
}
#[given(
    "Kafka 4.3 runs the application with the streams group protocol and streams version one finalized"
)]
async fn streams(application: &mut StreamsApplication) {
    finalize_streams_version().await;
    application.runtime = Some(StreamsRuntime::Streams);
}
#[given("application {id} reads topic {input} and writes topic {output}")]
fn topics(application: &mut StreamsApplication, id: String, input: String, output: String) {
    create_topic(broker0_advertised(), &input);
    create_topic(broker0_advertised(), &output);
    application.application_id = id;
    application.input = input;
    application.output = output;
}
#[given("the windowed count application is compiled against Kafka 3.5 for Java eleven")]
fn compile(application: &mut StreamsApplication) {
    let classes = shared_class_dir();
    compile_streams_app(classes.path());
    application.classes = Some(classes);
}
#[when("the compiled application runs with exactly once version two processing")]
async fn run(application: &mut StreamsApplication) {
    let runtime = application.runtime.expect("Streams runtime configured");
    let (image, classpath) = match runtime {
        StreamsRuntime::Classic => (KAFKA_IMAGE_TXN, CP_CONFLUENT),
        StreamsRuntime::Streams => (KAFKA_IMAGE_STREAMS_PROTOCOL, CP_APACHE),
    };
    let mut args = vec![
        broker0_advertised(),
        application.input.as_str(),
        application.output.as_str(),
        application.application_id.as_str(),
    ];
    if matches!(runtime, StreamsRuntime::Streams) {
        args.push("streams");
    }
    let output = run_streams_app(
        image,
        application
            .classes
            .as_ref()
            .expect("application compiled")
            .path(),
        classpath,
        &args,
    );
    if matches!(runtime, StreamsRuntime::Streams) {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if let Some(error) = LINKAGE_ERRORS
            .iter()
            .find(|error| stdout.contains(*error) || stderr.contains(*error))
        {
            application
                .broker
                .take()
                .expect("Streams broker started")
                .shutdown()
                .await;
            rstest_bdd::skip!(
                "Kafka3.5 compiled class cannot load against Kafka4.3 jars: {}; stdout:{}; stderr:{}",
                error,
                stdout,
                stderr
            );
        }
    }
    application.run = Some(output);
}
#[then("the application reports STREAMSPROBE OK")]
fn completed(application: &StreamsApplication) {
    let output = application
        .run
        .as_ref()
        .expect("compiled application completed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("STREAMSPROBE OK"),
        "Streams application did not emit all expected records: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[when(
    "the operator reads the committed sink and inspects the application's repartition and window changelog topics"
)]
fn inspect(application: &mut StreamsApplication) {
    let bootstrap = broker0_advertised();
    application.sink = Some(read_committed_sink(bootstrap, &application.output));
    application.topics = list_topics(bootstrap);
    for name in [
        format!("{}-shuffle-repartition", application.application_id),
        format!(
            "{}-{WINDOW_STORE_NAME}-changelog",
            application.application_id
        ),
    ] {
        application
            .configs
            .insert(name.clone(), describe_topic_configs(bootstrap, &name));
    }
}
#[then("the committed sink contains the running window counts in this exact order")]
fn sink(application: &StreamsApplication, datatable: Vec<Vec<String>>) {
    let expected: Vec<String> = datatable
        .into_iter()
        .skip(1)
        .map(|row| row.into_iter().next().expect("window count"))
        .collect();
    assert!(application.sink.as_ref().expect("committed sink read") == &expected);
}
#[then("exactly one of each internal topic exists and its config report includes")]
fn configurations(application: &StreamsApplication, datatable: Vec<Vec<String>>) {
    for row in datatable.into_iter().skip(1) {
        let [name, config]: [String; 2] =
            row.try_into().expect("internal topic and expected config");
        assert!(
            application
                .topics
                .iter()
                .filter(|topic| *topic == &name)
                .count()
                == 1,
            "expected one internal topic {name}, found {:?}",
            application.topics
        );
        let actual = application
            .configs
            .get(&name)
            .expect("internal topic config inspected");
        assert!(
            actual.contains(&config),
            "internal topic {name} missing {config}: {actual}"
        );
    }
}
async fn shutdown(application: StreamsApplication) {
    if let Some(broker) = application.broker {
        broker.shutdown().await;
    }
}
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_streams_app.feature",
    name = "A classic protocol Streams topology writes committed counts and configures its internal topics"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compiled_streams_app_runs_on_the_classic_protocol(application: StreamsApplication) {
    shutdown(application).await;
}
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_streams_app.feature",
    name = "A streams protocol topology writes committed counts when its cross compiled class loads"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compiled_streams_app_runs_on_the_streams_protocol(application: StreamsApplication) {
    shutdown(application).await;
}
