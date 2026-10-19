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
    std::{
        net::SocketAddr,
        path::{Path, PathBuf},
        time::Duration,
    },
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
        std::fs::read_to_string(manifest_dir().join("../../bench/soak/dr-secured.yaml")).unwrap();
    let shortened = declared
        .replace("post_capture_load: 20s", "post_capture_load: 2s")
        .replace("throughput_floor: 15/s", "throughput_floor: 1/s");
    assert!(shortened != declared);
    let path = dir.join("dr-secured.yaml");
    std::fs::write(&path, shortened).unwrap();
    path
}

/// Parse `args` as the binary does, run them, and return what they print. A
/// `load` ends after `LOAD`, as if the source cluster were lost then.
async fn drill(args: &[&str]) -> String {
    let command_line = std::iter::once("krabka-soak-drill").chain(args.iter().copied());
    let cli = Cli::try_parse_from(command_line).unwrap();
    run(cli, tokio::time::sleep(LOAD)).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drill_reads_back_every_record_a_surviving_source_acknowledged() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let security = manifest_dir().join("tests/fixtures/security");
    let secured: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let broker = start_configured(|config| {
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
    .await;
    provision_scram()
        .broker(&broker.broker)
        .user("soak".to_owned())
        .password(PASSWORD.as_bytes().to_vec())
        .call()
        .await;

    let files = tempfile::tempdir().unwrap();
    let contract_file = shortened_contract(files.path());
    let contract_arg = contract_file.to_str().unwrap();
    let load_file = files.path().join("load.json");
    let load_arg = load_file.to_str().unwrap();
    let evidence_file = files.path().join("evidence.json");
    let ca = security.join("dev_cert.pem");
    let bootstrap = secured.to_string();
    let login = [
        "--bootstrap",
        &bootstrap,
        "--tls-ca",
        ca.to_str().unwrap(),
        "--tls-server-name",
        "localhost",
        "--username",
        "soak",
        "--password",
        PASSWORD,
    ];

    // The topics, as the gate creates them from the objectives it prints.
    let contract = load_contract(&contract_file).unwrap();
    let declared: Objectives = objectives(&contract).unwrap();
    let printed: serde_json::Value =
        serde_json::from_str(&drill(&["objectives", "--contract", contract_arg]).await).unwrap();
    check!(printed == serde_json::to_value(&declared).unwrap());
    let admin = connect_client(broker.broker.listen_addr().to_string(), None).await;
    for path in &declared.paths {
        create_topic(&admin, &path.name, path.partitions).await;
    }

    let mut load_args = vec!["load", "--contract", contract_arg, "--out", load_arg];
    load_args.extend(login);
    check!(drill(&load_args).await == "");
    let load = read_load(&load_file).unwrap();

    // The capture starts once every group has had a commit acknowledged, as
    // it does an hour into the gate's load. Before that a group has nothing to
    // be held to on a partition it has not read yet.
    let capture_started_ms = load
        .commits
        .iter()
        .map(|history| history.acks.first().unwrap().unix_ms + 1)
        .max()
        .unwrap();
    assert!(capture_started_ms + 2_000 < load.run.ended_unix_ms);
    let capture = capture_started_ms.to_string();
    let disaster = load.run.ended_unix_ms.to_string();
    let serving = (load.run.ended_unix_ms + 1_000).to_string();
    let mut readback_args = vec![
        "readback",
        "--contract",
        contract_arg,
        "--load",
        load_arg,
        "--capture-started-ms",
        &capture,
        "--disaster-ms",
        &disaster,
        "--serving-ms",
        &serving,
        "--out",
        evidence_file.to_str().unwrap(),
    ];
    readback_args.extend(login);
    check!(drill(&readback_args).await == "");
    let evidence: Evidence =
        serde_json::from_str(&std::fs::read_to_string(&evidence_file).unwrap()).unwrap();

    let verdict = judge(&contract, &evidence);
    check!(verdict.reconciliation.findings == []);
    assert!(
        verdict.recovery
            == Some(RecoveryVerdict {
                objectives: contract.disaster_recovery.clone().unwrap(),
                rpo_ms: 0,
                rto_ms: Some(1_000),
                lost_beyond_boundary: 0,
                problems: Vec::new(),
            })
    );
    // Every path was loaded on both sides of the boundary, so the cut was
    // made inside the run rather than past either end of it.
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

    for path in &declared.paths {
        let mut probe_args = vec!["probe", "--topic", &path.name];
        probe_args.extend(login);
        let line = drill(&probe_args).await;
        check!(
            line.starts_with(&format!("{}-0: wrote and read back offset ", path.name)),
            "{line}"
        );
    }
}
