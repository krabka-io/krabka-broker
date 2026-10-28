//! `krabka-soak-drill`, end to end, against one in-process broker that is
//! never lost.
//!
//! The drill runs the declared load of `bench/soak/dr-secured.yaml` as a SCRAM
//! principal over TLS until the source cluster is lost. It then reads the
//! restored cluster back, probes it with a write and a read, and hands the
//! evidence to the judge. Here the "restored" cluster is the source itself, so
//! every record the load had acknowledged must read back. The judge must find
//! no loss and no committed offset out of place, and must measure a recovery
//! point of zero.
//!
//! The commands run through the binary's own argument parser and `run`, over a
//! `SASL_SSL` listener with SCRAM-SHA-512, the drill's only login. The
//! qualification gate runs the same commands against two secured clusters,
//! through a real capture and restore. This suite keeps the commands honest
//! without Docker.
#![cfg(test)]

mod support;

use {
    crate::support::{
        client::{connect_client, create_topic},
        free_port,
        listeners::listener,
        manifest_dir,
        sasl::provision_scram,
        start_configured,
    },
    assert2::{assert, check},
    clap::Parser,
    krabka_bench_driver::soak::{
        contract::Contract,
        drill::{
            Objectives,
            cli::{Cli, run},
            load_contract, objectives, read_load,
        },
        evidence::Evidence,
        recovery::RecoveryVerdict,
        verdict::judge,
    },
    krabka_security::{ClientAuthMode, ListenerProtocol, SaslMechanism, TlsConfig},
    rustls::crypto::aws_lc_rs,
    std::{
        fs, iter, mem,
        net::SocketAddr,
        path::{Path, PathBuf},
        time::Duration,
    },
    tokio::time,
};

/// How long the load runs before the loss.
const LOAD: Duration = Duration::from_secs(14);
const PASSWORD: &str = "qualification-soak";

/// `bench/soak/dr-secured.yaml`, written to `dir` with a shorter
/// post-capture load and a lower throughput floor. The declared 20 s after the
/// capture, and the declared floor, would make this suite slow and sensitive
/// to a loaded runner without making it stricter. What is under test is that
/// every record comes back, which the judge checks however long and however
/// fast the load ran.
fn shortened_contract(dir: &Path) -> PathBuf {
    let declared =
        fs::read_to_string(manifest_dir().join("../../bench/soak/dr-secured.yaml")).unwrap();
    let shortened = declared
        .replace("post_capture_load: 20s", "post_capture_load: 2s")
        .replace("throughput_floor: 15/s", "throughput_floor: 1/s");
    assert!(shortened != declared);
    let path = dir.join("dr-secured.yaml");
    fs::write(&path, shortened).unwrap();
    path
}

/// Parse `args` as the binary does, run them, and return what they print. A
/// `load` ends after `LOAD`, as if the source cluster were lost then.
async fn drill(args: &[&str]) -> String {
    let command_line = iter::once("krabka-soak-drill").chain(args.iter().copied());
    let cli = Cli::try_parse_from(command_line).unwrap();
    run(cli, time::sleep(LOAD)).await.unwrap()
}

#[derive(Default)]
struct SecuredSoakDrill {
    broker: Option<support::InProcess>,
    files: Option<tempfile::TempDir>,
    bootstrap: String,
    ca: PathBuf,
    contract_file: PathBuf,
    load_file: PathBuf,
    evidence_file: PathBuf,
    contract: Option<Contract>,
    declared: Option<Objectives>,
    printed: Option<serde_json::Value>,
    load_stdout: Option<String>,
    readback_stdout: Option<String>,
    capture_started_ms: Option<i64>,
    ended_ms: Option<i64>,
    evidence: Option<Evidence>,
    probes: Vec<(String, String)>,
}

#[rstest::fixture]
fn soak_drill() -> SecuredSoakDrill {
    // The surviving source is also the restored cluster for this drill.
    SecuredSoakDrill::default()
}

impl SecuredSoakDrill {
    fn contract(&self) -> &Contract { self.contract.as_ref().expect("secured recovery contract") }

    fn declared(&self) -> &Objectives { self.declared.as_ref().expect("declared objectives") }

    fn login(&self) -> Vec<&str> {
        vec![
            "--bootstrap",
            &self.bootstrap,
            "--tls-ca",
            self.ca.to_str().unwrap(),
            "--tls-server-name",
            "localhost",
            "--username",
            "soak",
            "--password",
            PASSWORD,
        ]
    }

    async fn shutdown(&mut self) {
        let Self { broker, files, .. } = mem::take(self);
        let support::InProcess {
            broker,
            client,
            _tempdir: directory,
        } = broker.expect("secured broker");
        client.close();
        broker.shutdown().await;
        drop((directory, files));
    }
}

#[rstest_bdd_macros::given(
    "the source broker serves SASL_SSL with SCRAM-SHA-512 user soak and password qualification-soak using the development TLS certificate"
)]
async fn secured_drill_source(soak_drill: &mut SecuredSoakDrill) {
    let _ = aws_lc_rs::default_provider().install_default();
    let security = manifest_dir().join("tests/fixtures/security");
    let secured: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    soak_drill.broker = Some(
        start_configured(|config| {
            let mut sasl_ssl = listener("SASL_SSL", secured, ListenerProtocol::SaslSsl);
            sasl_ssl.tls_config = Some(TlsConfig {
                cert_chain_path: security.join("dev_cert.pem"),
                private_key_path: security.join("dev_key.pem"),
                trust_roots_path: None,
                client_ca_path: None,
                client_auth: ClientAuthMode::Disabled,
            });
            sasl_ssl.sasl_mechanisms = Some(vec![SaslMechanism::ScramSha512]);
            // The test config's implicit listener, named, so the secured one can
            // stand beside it.
            let plaintext = listener("PLAINTEXT", config.listen_addr, ListenerProtocol::Plaintext);
            config.listeners = vec![plaintext, sasl_ssl];
        })
        .await,
    );
    provision_scram()
        .broker(&soak_drill.broker.as_ref().expect("secured broker").broker)
        .user("soak".to_owned())
        .password(PASSWORD.as_bytes().to_vec())
        .call()
        .await;

    soak_drill.files = Some(tempfile::tempdir().unwrap());
    soak_drill.contract_file =
        shortened_contract(soak_drill.files.as_ref().expect("drill files").path());
    soak_drill.load_file = soak_drill
        .files
        .as_ref()
        .expect("drill files")
        .path()
        .join("load.json");
    soak_drill.evidence_file = soak_drill
        .files
        .as_ref()
        .expect("drill files")
        .path()
        .join("evidence.json");
    soak_drill.ca = security.join("dev_cert.pem");
    soak_drill.bootstrap = secured.to_string();
    soak_drill.contract = Some(load_contract(&soak_drill.contract_file).unwrap());
    soak_drill.declared = Some(objectives(soak_drill.contract()).unwrap());
}

#[rstest_bdd_macros::when(
    "the objectives command prints bench/soak/dr-secured.yaml with post-capture load 2s and throughput floor 1/s"
)]
async fn print_secured_drill_objectives(soak_drill: &mut SecuredSoakDrill) {
    soak_drill.printed = Some(
        serde_json::from_str(
            &drill(&[
                "objectives",
                "--contract",
                soak_drill.contract_file.to_str().unwrap(),
            ])
            .await,
        )
        .unwrap(),
    );
}

#[rstest_bdd_macros::then(
    "the printed objectives exactly match the parsed contract's declared objectives"
)]
fn declared_secured_drill_objectives(soak_drill: &SecuredSoakDrill) {
    check!(
        soak_drill.printed.as_ref() == Some(&serde_json::to_value(soak_drill.declared()).unwrap())
    );
}

#[rstest_bdd_macros::given(
    "every path declared by the objectives has its required topic and partition count"
)]
async fn secured_drill_topics(soak_drill: &SecuredSoakDrill) {
    let admin = connect_client(
        soak_drill
            .broker
            .as_ref()
            .expect("secured broker")
            .broker
            .listen_addr()
            .to_string(),
        None,
    )
    .await;
    for path in &soak_drill.declared().paths {
        create_topic(&admin, &path.name, path.partitions).await;
    }
    admin.close();
}

#[rstest_bdd_macros::when(
    "the load command authenticates over TLS and runs for fourteen seconds before source loss is signalled"
)]
async fn run_secured_drill_load(soak_drill: &mut SecuredSoakDrill) {
    let mut args = vec![
        "load",
        "--contract",
        soak_drill.contract_file.to_str().unwrap(),
        "--out",
        soak_drill.load_file.to_str().unwrap(),
    ];
    args.extend(soak_drill.login());
    soak_drill.load_stdout = Some(drill(&args).await);
    let load = read_load(&soak_drill.load_file).unwrap();
    soak_drill.capture_started_ms = Some(
        load.commits
            .iter()
            .map(|history| history.acks.first().unwrap().unix_ms + 1)
            .max()
            .unwrap(),
    );
    soak_drill.ended_ms = Some(load.run.ended_unix_ms);
}

#[rstest_bdd_macros::then(
    "the load command prints nothing and every group has acknowledged a commit over two seconds before source loss"
)]
fn captured_secured_drill_load(soak_drill: &SecuredSoakDrill) {
    check!(soak_drill.load_stdout.as_deref() == Some(""));
    assert!(
        soak_drill.capture_started_ms.expect("capture boundary") + 2_000
            < soak_drill.ended_ms.expect("source loss boundary")
    );
}

#[rstest_bdd_macros::when(
    "readback authenticates to the surviving source as the restored cluster, using the acknowledged-commit capture boundary and serving time one second after loss"
)]
async fn read_back_secured_drill(soak_drill: &mut SecuredSoakDrill) {
    let capture = soak_drill
        .capture_started_ms
        .expect("capture boundary")
        .to_string();
    let disaster = soak_drill
        .ended_ms
        .expect("source loss boundary")
        .to_string();
    let serving = (soak_drill.ended_ms.expect("source loss boundary") + 1_000).to_string();
    let mut args = vec![
        "readback",
        "--contract",
        soak_drill.contract_file.to_str().unwrap(),
        "--load",
        soak_drill.load_file.to_str().unwrap(),
        "--capture-started-ms",
        &capture,
        "--disaster-ms",
        &disaster,
        "--serving-ms",
        &serving,
        "--out",
        soak_drill.evidence_file.to_str().unwrap(),
    ];
    args.extend(soak_drill.login());
    soak_drill.readback_stdout = Some(drill(&args).await);
    soak_drill.evidence = Some(
        serde_json::from_str(&fs::read_to_string(&soak_drill.evidence_file).unwrap()).unwrap(),
    );
}

#[rstest_bdd_macros::then(
    "readback prints nothing, reconciliation has no findings, recovery has RPO 0 and RTO 1000ms with no loss or problems, and each path has records on both sides of capture"
)]
fn recovered_secured_drill_records(soak_drill: &SecuredSoakDrill) {
    check!(soak_drill.readback_stdout.as_deref() == Some(""));
    let evidence = soak_drill.evidence.as_ref().expect("read-back evidence");
    let verdict = judge(soak_drill.contract(), evidence);
    check!(verdict.reconciliation.findings == []);
    assert!(
        verdict.recovery
            == Some(RecoveryVerdict {
                objectives: soak_drill.contract().disaster_recovery.clone().unwrap(),
                rpo_ms: 0,
                rto_ms: Some(1_000),
                lost_beyond_boundary: 0,
                problems: Vec::new()
            })
    );
    for path in evidence
        .recovery
        .iter()
        .flat_map(|recovery| &recovery.paths)
    {
        check!(
            0 < path.before_boundary && path.before_boundary < path.acknowledged,
            "{}: {} of {} before the boundary",
            path.path,
            path.before_boundary,
            path.acknowledged
        );
    }
}

#[rstest_bdd_macros::when(
    "the probe command writes and reads partition 0 of every declared path over the same TLS login"
)]
async fn probe_secured_drill_paths(soak_drill: &mut SecuredSoakDrill) {
    let paths: Vec<String> = soak_drill
        .declared()
        .paths
        .iter()
        .map(|path| path.name.clone())
        .collect();
    for path in paths {
        let mut args = vec!["probe", "--topic", &path];
        args.extend(soak_drill.login());
        let output = drill(&args).await;
        soak_drill.probes.push((path, output));
    }
}

#[rstest_bdd_macros::then(
    "every probe reports its topic partition 0 wrote and read back an offset"
)]
fn responsive_secured_drill_paths(soak_drill: &SecuredSoakDrill) {
    check!(soak_drill.probes.len() == soak_drill.declared().paths.len());
    for (path, line) in &soak_drill.probes {
        check!(
            line.starts_with(&format!("{path}-0: wrote and read back offset ")),
            "{line}"
        );
    }
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/soak_drill.feature",
    name = "the drill reads back every record a surviving source acknowledged"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drill_reads_back_every_record_a_surviving_source_acknowledged(
    soak_drill: SecuredSoakDrill,
) {
    soak_drill.shutdown().await;
    drop(soak_drill);
}
