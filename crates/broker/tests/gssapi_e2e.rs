//! End-to-end GSSAPI (Kerberos) parity tests.
//!
//! These tests prove that a stock cp-kafka GSSAPI client authenticates to
//! Krabka end-to-end against a real MIT KDC. They also prove that two Krabka
//! brokers authenticate to each other over a GSSAPI inter-broker listener.
//!
//! # Topology
//!
//! The Krabka broker runs in-process on the host, bound to `0.0.0.0:9092`, and
//! advertises `host.docker.internal:9092`. The cp-kafka CLI tools run inside
//! `mirror.gcr.io/confluentinc/cp-kafka` containers started with
//! `--add-host=host.docker.internal:host-gateway`, so they reach the host
//! broker through the advertised name. `jvm_acceptance.rs` uses the same
//! method.
//!
//! The KDC runs in its own container, see
//! `crates/broker/tests/fixtures/security/kdc`, and maps `88/tcp+udp` to the host.
//! From inside the CLI containers it is reachable at `host.docker.internal:88`,
//! which `tests/fixtures/gssapi/krb5.conf` configures.
//!
//! Because the broker advertises `host.docker.internal`, the stock Kafka GSSAPI
//! client derives the server principal `kafka/host.docker.internal@CRABKA.TEST`.
//! The KDC fixture supplies that SPN together with `kafka/localhost`, and
//! exports both keys into the single `kafka.keytab` the broker loads.
//!
//! # Running
//!
//! ```bash
//! bazel run //crates/broker/tests/fixtures/security/kdc:image_load
//! docker compose -f crates/broker/tests/fixtures/security/kdc/docker-compose.yml up -d
//! # wait for KDC_READY in the logs
//! KRB5_CONFIG=crates/broker/tests/fixtures/security/kdc/krb5.conf \
//!   SSPI_KDC_URL=tcp://localhost:88 \
//!   cargo test -p krabka-broker --test gssapi_e2e -- --ignored
//! ```
#![cfg(test)]

mod support;

use {
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec},
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::{
        env,
        io::Write,
        path::PathBuf,
        process::{Command, Output, Stdio},
    },
};

/// cp-kafka image that holds the GSSAPI-capable console tools.
const KAFKA_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:6.1.1";
/// Host bind address for the broker's `SASL_PLAINTEXT` data-plane listener.
const LISTEN: &str = "0.0.0.0:9092";
/// Advertised name the CLI containers resolve with `--add-host`. It is also the
/// host component of the server SPN the stock client derives, `kafka/<this>`.
const BOOTSTRAP: &str = "host.docker.internal:9092";

use crate::support::init_tracing_with;
/// Absolute path to the KDC fixture dir, which holds `kafka.keytab` and
/// `alice.keytab`.
use support::{free_port, manifest_dir};

fn kdc_fixtures() -> PathBuf { manifest_dir().join("tests/fixtures/security/kdc") }

/// Absolute path to the GSSAPI client config dir, which holds `krb5.conf`,
/// `client_jaas.conf`, and `client.properties`.
fn gssapi_fixtures() -> PathBuf { manifest_dir().join("tests/fixtures/gssapi") }

/// A running broker and the directory that backs it.
struct HostGssapiBroker {
    broker: BrokerHandle,
    dir: tempfile::TempDir,
}

/// Spawn an in-process Krabka broker on `LISTEN`.
///
/// The broker has one `SASL_PLAINTEXT` listener that advertises `GSSAPI`. The
/// KDC fixture's `kafka.keytab` backs that listener.
async fn start_host_gssapi_broker() -> HostGssapiBroker {
    init_tracing_with("krabka_broker=debug,info");

    let kdc_url = env::var("SSPI_KDC_URL").unwrap_or_else(|_| "tcp://localhost:88".to_owned());
    let dir = tempfile::tempdir().expect("tempdir");

    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.broker_id = 1;
    cfg.listen_addr = LISTEN.parse().expect("static addr");
    cfg.advertised_listener = BOOTSTRAP.into();
    let controller = format!("127.0.0.1:{}", free_port());
    cfg.controller_listen_addr = controller.parse().expect("controller addr");
    cfg.controller_quorum_voters = vec![(cfg.node_id, controller)];
    let internal = format!("127.0.0.1:{}", free_port());
    cfg.listeners = vec![
        ListenerSpec {
            name: "SASL_PLAINTEXT".to_owned(),
            bind_addr: LISTEN.parse().expect("static addr"),
            advertised: BOOTSTRAP.to_owned(),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: Some(vec![SaslMechanism::Gssapi]),
            principal_mapper: krabka_broker::SslPrincipalMapper::default(),
        },
        ListenerSpec {
            name: "INTERNAL".to_owned(),
            bind_addr: internal.parse().expect("internal addr"),
            advertised: internal,
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: krabka_broker::SslPrincipalMapper::default(),
        },
    ];
    cfg.inter_broker_listener_name = "INTERNAL".to_owned();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Gssapi];
    cfg.gssapi = Some(support::gssapi::broker_config(
        kdc_fixtures().join("kafka.keytab"),
        kdc_url,
    ));

    let handle = Broker::start(cfg).await.expect("start gssapi broker");
    eprintln!("KRABKA[test] gssapi broker started listen={LISTEN} advertised={BOOTSTRAP}");
    HostGssapiBroker {
        broker: handle,
        dir,
    }
}

/// Build the common `docker run` argument prefix for a cp-kafka GSSAPI tool.
///
/// The prefix holds the host-gateway hosts entry, the two fixture mounts, and a
/// `KAFKA_OPTS` value that points the JVM at the JAAS and krb5 configs.
///
/// Mounts:
///   - KDC fixtures  → `/fixtures` (supplies `alice.keytab`, which JAAS references)
///   - GSSAPI config → `/gssapi`   (`krb5.conf`, `client_jaas.conf`, `client.properties`)
fn gssapi_docker_prefix() -> Vec<String> {
    let kdc = kdc_fixtures().canonicalize().expect("canonicalize kdc dir");
    let gss = gssapi_fixtures()
        .canonicalize()
        .expect("canonicalize gssapi dir");
    vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "--user".to_owned(),
        "0".to_owned(),
        "--add-host=host.docker.internal:host-gateway".to_owned(),
        "-v".to_owned(),
        format!("{}:/fixtures", kdc.display()),
        "-v".to_owned(),
        format!("{}:/gssapi", gss.display()),
        "-e".to_owned(),
        "KAFKA_OPTS=-Djava.security.auth.login.config=/gssapi/client_jaas.conf \
         -Djava.security.krb5.conf=/gssapi/krb5.conf -Dsun.security.krb5.debug=true"
            .to_owned(),
    ]
}

/// Run a cp-kafka GSSAPI tool and retain its status and output.
fn run_gssapi_tool(tool_args: &[&str]) -> Output {
    let mut args = gssapi_docker_prefix();
    args.push(KAFKA_IMAGE.to_owned());
    args.extend(tool_args.iter().map(|s| (*s).to_owned()));
    let out = Command::new("docker")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn docker run");
    eprintln!(
        "KRABKA[test] gssapi tool {tool_args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len()
    );
    out
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct KerberosRoundTrip {
    broker: Option<HostGssapiBroker>,
    topic: String,
    tool: Option<Output>,
}

#[fixture]
fn kerberos() -> KerberosRoundTrip {
    // Start this scenario with fresh inputs and observations.

    KerberosRoundTrip {
        broker: None,
        topic: String::new(),
        tool: None,
    }
}

#[given("a GSSAPI broker backed by the MIT KDC kafka.keytab and the alice keytab JVM client")]
async fn kerberos_broker(kerberos: &mut KerberosRoundTrip) {
    kerberos.broker = Some(start_host_gssapi_broker().await);
}

#[when("the authenticated JVM admin creates topic {topic} with one sole-replica partition")]
fn create(kerberos: &mut KerberosRoundTrip, topic: String) {
    kerberos.tool = Some(run_gssapi_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        &topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
        "--command-config",
        "/gssapi/client.properties",
    ]));
    kerberos.topic = topic;
}

#[then("the authenticated JVM command succeeds")]
fn success(kerberos: &KerberosRoundTrip) {
    let out = kerberos.tool.as_ref().unwrap();
    assert!(
        out.status.success(),
        "GSSAPI command failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[when("the authenticated JVM producer writes records {records}")]
fn produce(kerberos: &mut KerberosRoundTrip, records: String) {
    let mut prefix = gssapi_docker_prefix();
    prefix.insert(2, "-i".to_owned());
    let mut child = Command::new("docker")
        .args(&prefix)
        .args([
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            &kerberos.topic,
            "--producer.config",
            "/gssapi/client.properties",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    let mut input = records.replace(',', "\n");
    input.push('\n');
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    kerberos.tool = Some(child.wait_with_output().expect("wait producer"));
}

#[when(
    "the authenticated JVM consumer reads {count} records from partition zero with timeout {timeout} ms"
)]
fn consume(kerberos: &mut KerberosRoundTrip, count: String, timeout: String) {
    kerberos.tool = Some(run_gssapi_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        &kerberos.topic,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        &count,
        "--timeout-ms",
        &timeout,
        "--consumer.config",
        "/gssapi/client.properties",
    ]));
}

#[then("the consumer output contains records {records}")]
fn received(kerberos: &KerberosRoundTrip, records: String) {
    let stdout = String::from_utf8_lossy(&kerberos.tool.as_ref().unwrap().stdout);
    for needle in records.split(',') {
        assert!(
            stdout.contains(needle),
            "consumer did not emit {needle}: {stdout:?}"
        );
    }
}

#[ignore = "requires Docker + the KDC fixture (docker compose up) + KRB5_CONFIG/SSPI_KDC_URL"]
#[scenario(
    path = "tests/features/migrated/tests/gssapi_e2e.feature",
    name = "cp kafka gssapi client round trip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cp_kafka_gssapi_client_round_trip(kerberos: KerberosRoundTrip) {
    let HostGssapiBroker {
        broker,
        dir: _directory,
    } = kerberos.broker.unwrap();
    broker.shutdown().await;
}
