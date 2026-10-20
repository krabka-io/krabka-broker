//! The executor of the sustained-load contract: a contract under
//! `bench/soak/` run against three real broker containers, with its faults
//! injected and every measurement recorded as evidence the judge grades.
//!
//! # What it runs
//!
//! The contract is the file `KRABKA_SOAK_CONTRACT` names, or
//! `bench/soak/self-check.yaml`. The executor refuses it unless
//! `Contract::validate` accepts it, and refuses a contract it cannot run
//! (see [`plan::unsupported`]) before any container starts.
//!
//! The cluster is the `soak` suite's [`cluster`]: three containers of
//! `//packaging`'s image in one three-voter quorum, configured with the
//! contract's check-quorum window, replica lag, replication factor and minimum
//! ISR (see [`plan::node_toml`]). A tiered or diskless path gets a local object
//! store on a host directory that all three containers mount at the same path.
//! A diskless path also gets a diskless WAL quorum: every node binds a second,
//! authenticated inter-broker listener and reports its own rack, while every
//! client of the run stays on the anonymous `PLAINTEXT` listener (see
//! [`cluster`]).
//!
//! For the contract's `duration`, each path runs one paced producer and one
//! live consumer group per declared isolation level ([`load`]). Meanwhile the
//! fault schedule `schedule::expand` gives is executed one fault at a time, at
//! its offset from the start of the load ([`executor`], [`faults`]). The
//! `settle` tail runs the same load with no fault in it.
//!
//! Then the load stops. A self-check deletes one acknowledged record with
//! `DeleteRecords`, and every path is read back from the earliest offset at
//! every isolation level it declares ([`readback`]).
//!
//! # What it writes
//!
//! Into `KRABKA_SOAK_EVIDENCE_DIR`, else Bazel's undeclared-outputs directory,
//! else a kept temporary directory whose path it prints:
//!
//! | file | written |
//! | --- | --- |
//! | `contract.yaml` | first: the contract as run |
//! | `schedule.json` | before the load starts |
//! | `faults.jsonl` | as each fault is injected and as it is cleared |
//! | `telemetry.jsonl` | every [`TELEMETRY_INTERVAL`]: one row per node of resources and controller progress ([`telemetry`]) |
//! | `prometheus.yml` | when the alert sidecar starts ([`alerts`]) |
//! | `alerts.json` | every [`CHECKPOINT_INTERVAL`] and at the end: each shipped alert rule's firing history |
//! | `evidence.partial.json` | after each fault: the faults, recoveries and commit checks so far |
//! | `evidence.json` | after the read-back |
//! | `telemetry-summary.json` | after the read-back |
//! | `verdict.json` | last: the verdict, or the self-check report |
//!
//! # What fails it
//!
//! A soak fails on any failure `verdict::judge` reports. A self-check fails
//! unless `verdict::judge_self_check` passes: exactly the deleted record is
//! reported lost, by every reader of its path, and exactly the one impossible
//! deadline is reported missed.
//!
//! # Running it
//!
//! ```text
//! bazel run -c opt //packaging:image_load
//! cargo test -p krabka-broker --test soak_contract -- --ignored --nocapture
//! ```
//!
//! The planning, the window arithmetic, target resolution, ballast sizing and
//! the read-back's end detection are hermetic and run without Docker:
//!
//! ```text
//! cargo test -p krabka-broker --test soak_contract
//! ```

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto a sibling
// directory, which keeps the parts out of `tests/` where every `.rs` file
// would become another test binary.
mod support;

// Shared with the `soak` suite, which uses a different part of each.
#[allow(dead_code)]
#[path = "soak/cluster.rs"]
mod cluster;
#[allow(dead_code)]
#[path = "soak/probe.rs"]
mod probe;

#[path = "soak_contract/alerts.rs"]
mod alerts;
#[path = "soak_contract/executor.rs"]
mod executor;
#[path = "soak_contract/faults.rs"]
mod faults;
#[path = "soak_contract/host.rs"]
mod host;
#[path = "soak_contract/injection.rs"]
mod injection;
#[path = "soak_contract/lifecycle_run.rs"]
mod lifecycle_run;
#[path = "soak_contract/plan.rs"]
mod plan;
#[path = "soak_contract/rollback_guard.rs"]
mod rollback_guard;
#[path = "soak_contract/telemetry.rs"]
mod telemetry;
#[path = "soak_contract/topology.rs"]
mod topology;
#[path = "soak_contract/transitions.rs"]
mod transitions;

use {
    crate::{
        alerts::AlertSidecar,
        cluster::{SoakCluster, image},
        executor::{Executor, await_full_isr},
        host::{FaultLog, environment, evidence_dir, image_identity, now_ms, write_json},
        injection::delete_one_record,
        lifecycle_run::{LifecycleRun, checkpoint, supported_features},
        support::{manifest_dir, topics::topic_configs},
        telemetry::Telemetry,
        topology::{Node, Nodes},
    },
    assert2::assert,
    krabka_bench_driver::soak::{
        client::{
            Endpoint,
            load::{LoadContext, prepare, producer_name, start},
            readback::{commit_checks, read_back, wait_stable},
            windows::{Span, Windows, planned_span, steady_millis},
        },
        contract::{Contract, Profile},
        evidence::{Evidence, FINAL_COMMIT_CHECK},
        lifecycle::{Checkpoint, Side, is_transition},
        schedule::{ScheduledFault, expand},
        verdict::{judge, judge_self_check},
    },
    krabka_protocol::owned::create_topics_request::CreatableTopic,
    krabka_units::prelude::*,
    std::{
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    },
    tokio::{sync::oneshot, task::JoinHandle},
};

/// How often every node's resources and controller progress are sampled.
/// Over 72 hours and three nodes that is about 78 000 rows.
const TELEMETRY_INTERVAL: Duration = Duration::from_secs(10);

/// How often the alert history so far is written to `alerts.json`, so a run
/// that dies keeps it.
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(300);

/// The contract this run executes.
fn contract_source() -> PathBuf {
    std::env::var("KRABKA_SOAK_CONTRACT")
        .ok()
        .filter(|path| !path.is_empty())
        .map_or_else(
            || manifest_dir().join("../../bench/soak/self-check.yaml"),
            PathBuf::from,
        )
}

/// The lifecycle contract whose source release the refused rollback runs.
fn lifecycle_contract() -> Contract {
    let source = manifest_dir().join("../../bench/soak/lifecycle.yaml");
    let text = std::fs::read_to_string(&source)
        .unwrap_or_else(|error| panic!("read {}: {error}", source.display()));
    Contract::from_yaml(&text).unwrap_or_else(|error| panic!("{}: {error}", source.display()))
}

/// Create every path's topic, retrying while the fresh cluster elects.
async fn create_topics(bootstrap: &str, contract: &Contract) {
    let topics: Vec<CreatableTopic> = contract
        .paths
        .iter()
        .map(|path| {
            let configs = plan::topic_configs(path, contract.cluster.min_insync_replicas);
            CreatableTopic {
                name: path.name.clone(),
                num_partitions: path.partitions,
                replication_factor: contract.cluster.replication_factor,
                configs: topic_configs(configs.iter().map(|(name, value)| (*name, value.as_str()))),
                ..Default::default()
            }
        })
        .collect();
    if let Err(last) = cluster::create_topics(bootstrap, &topics).await {
        panic!("could not create the contract's topics on {bootstrap}: {last}");
    }
}

/// Write the alert history to `alerts.json` every [`CHECKPOINT_INTERVAL`]
/// until `stop` fires. A failed checkpoint is printed and leaves the previous
/// one in place.
async fn checkpoint_alerts(
    alerts: Arc<AlertSidecar>,
    http: reqwest::Client,
    dir: PathBuf,
    start_ms: i64,
    mut stop: oneshot::Receiver<()>,
) {
    let mut ticks = tokio::time::interval(CHECKPOINT_INTERVAL);
    ticks.tick().await;
    loop {
        tokio::select! {
            _ = &mut stop => return,
            _ = ticks.tick() => {
                if let Err(error) = alerts.checkpoint(&http, start_ms, now_ms(), &dir).await {
                    eprintln!("soak-contract: alert checkpoint failed: {error}");
                }
            }
        }
    }
}

/// What runs beside the load for the whole run: the telemetry sampler and
/// the alert sidecar, with its periodic checkpoints.
struct Recorders {
    telemetry: Telemetry,
    alerts: Arc<AlertSidecar>,
    http: reqwest::Client,
    stop_checkpoints: oneshot::Sender<()>,
    checkpoints: JoinHandle<()>,
}

impl Recorders {
    /// Start sampling `nodes` and evaluating the alert rules against them,
    /// with the evidence in `dir` and the history counted from `start_ms`.
    fn start(nodes: &Arc<Nodes>, dir: &Path, start_ms: i64) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("build an HTTP client");
        let telemetry = Telemetry::start(Arc::clone(nodes), dir, TELEMETRY_INTERVAL);
        let alerts = Arc::new(
            AlertSidecar::start(nodes, dir)
                .unwrap_or_else(|error| panic!("start the alert sidecar: {error}")),
        );
        let (stop_checkpoints, stopped) = oneshot::channel();
        let checkpoints = tokio::spawn(checkpoint_alerts(
            Arc::clone(&alerts),
            http.clone(),
            dir.to_path_buf(),
            start_ms,
            stopped,
        ));
        Self {
            telemetry,
            alerts,
            http,
            stop_checkpoints,
            checkpoints,
        }
    }

    /// Stop both, write the final `alerts.json` and `telemetry-summary.json`,
    /// and return their summary lines.
    async fn finish(self, dir: &Path, start_ms: i64) -> Vec<String> {
        // The task is gone only if it panicked, which the await reports.
        let _ = self.stop_checkpoints.send(());
        self.checkpoints.await.expect("the alert checkpoint task");
        let history = self
            .alerts
            .checkpoint(&self.http, start_ms, now_ms(), dir)
            .await
            .unwrap_or_else(|error| panic!("read the alert history: {error}"));
        let resources = self.telemetry.stop().await;
        write_json(dir, "telemetry-summary.json", &resources);
        resources
            .lines()
            .into_iter()
            .chain(history.lines())
            .collect()
    }
}

/// Sleep until `at`, Unix milliseconds.
async fn sleep_until(at: i64) {
    let left = at - now_ms();
    if left > 0 {
        tokio::time::sleep(Time::from_millis(left).to_std()).await;
    }
}

/// One line per fault and per path: what ran, and how long each recovery
/// took. Printed at the end of every run.
fn timing_summary(evidence: &Evidence, load_start: i64) -> Vec<String> {
    let mut lines = Vec::new();
    for fault in &evidence.faults {
        let cleared = fault.cleared_unix_ms.map_or_else(
            || "never cleared".to_owned(),
            |cleared| format!("cleared after {}ms", cleared - fault.injected_unix_ms),
        );
        let took: Vec<String> = evidence
            .recoveries
            .iter()
            .filter(|recovery| recovery.fault == fault.id)
            .map(|recovery| {
                format!(
                    "{} {}",
                    recovery.check,
                    recovery.took().map_or_else(
                        || "never".to_owned(),
                        |took| format!("{}ms", took.millis_i64())
                    )
                )
            })
            .collect();
        lines.push(format!(
            "fault {} at +{}ms on {} ({}): {cleared}; {}",
            fault.id,
            fault.injected_unix_ms - load_start,
            fault.target,
            fault.role,
            took.join(", ")
        ));
    }
    for path in &evidence.paths {
        lines.push(format!(
            "path {}: steady {:.1}/s; produce p50/p99/p999 {}/{}/{}ms over {} steady ({} disturbed); consume {}/{}/{}ms over {} steady ({} disturbed)",
            path.path,
            path.steady_rate.per_sec_f64(),
            path.produce_steady.p50.millis_i64(),
            path.produce_steady.p99.millis_i64(),
            path.produce_steady.p999.millis_i64(),
            path.produce_steady.count,
            path.produce_disturbed.count,
            path.consume_steady.p50.millis_i64(),
            path.consume_steady.p99.millis_i64(),
            path.consume_steady.p999.millis_i64(),
            path.consume_steady.count,
            path.consume_disturbed.count,
        ));
    }
    for producer in &evidence.producers {
        lines.push(format!(
            "producer {}: {} attempted, {} acknowledged, {} failed, {} resent, {} transactions",
            producer.producer,
            producer.attempted,
            producer.acked.len(),
            producer.failed.len(),
            producer.retried.len(),
            producer.transactions.len()
        ));
    }
    for reader in &evidence.readers {
        lines.push(format!(
            "reader {} at {}: {} read, {} read more than once",
            reader.producer,
            reader.isolation,
            reader.seen.len(),
            reader.duplicates.len()
        ));
    }
    lines.push(format!("commit checks: {}", evidence.commits.len()));
    lines
}

/// After the load: wait out open transactions, delete the self-check's one
/// record, and read every path back at every isolation level it declares.
async fn inject_and_read_back(
    contract: &Contract,
    endpoint: &Endpoint,
    nodes: &Nodes,
    evidence: &mut Evidence,
) {
    for path in &contract.paths {
        wait_stable(endpoint, path, contract.clients.transactions.timeout * 2.0).await;
    }
    if contract.profile == Profile::SelfCheck
        && let Some(self_check) = &contract.self_check
        && let Some(path) = contract.path(&self_check.missing_record_path)
        && let Some(ledger) = evidence
            .producers
            .iter()
            .find(|ledger| ledger.path == path.name)
    {
        match delete_one_record(endpoint, nodes, path, ledger).await {
            Ok(loss) => {
                eprintln!(
                    "soak-contract: deleted {} seq {} at {}-{} offset {}",
                    loss.producer, loss.seq, loss.path, loss.partition, loss.offset
                );
                evidence.injected.push(loss);
            }
            Err(error) => {
                eprintln!("soak-contract: could not delete a record for the self-check: {error}");
            }
        }
    }
    for path in &contract.paths {
        for &isolation in &path.consumers {
            evidence
                .readers
                .push(read_back(endpoint, path, isolation, &producer_name(&path.name)).await);
        }
    }
}

/// Boot the contract's cluster, create its topics and wait for their ISRs.
///
/// Returns the cluster, the object-store directory its nodes share (kept
/// alive for as long as the cluster), and the executor's view of its nodes.
async fn boot(
    contract: &Contract,
    lifecycle: Option<&LifecycleRun>,
) -> (SoakCluster, Option<tempfile::TempDir>, Arc<Nodes>) {
    let remote_tier = plan::object_store(contract)
        .then(|| tempfile::TempDir::new().expect("object store directory"));
    let toml = plan::node_toml(contract);
    // A lifecycle run is formatted and started on its source release, with a
    // TLS listener for its rotations.
    let tls = lifecycle.map(|run| {
        run.authority
            .lock()
            .expect("the certificate authority")
            .issue()
            .unwrap_or_else(|error| panic!("issue the first certificates: {error}"))
    });
    let cluster = SoakCluster::configured()
        .root_toml(toml.root)
        .tables_toml(toml.tables)
        .diskless_quorum(plan::diskless(contract))
        .maybe_shared_mount(
            remote_tier
                .as_ref()
                .map(|tier| (tier.path().to_path_buf(), plan::REMOTE_TIER.to_owned())),
        )
        .brokers(contract.cluster.brokers)
        .maybe_image(lifecycle.map(|run| run.image(Side::Source)))
        .maybe_tls(tls.as_ref())
        .call();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("build an HTTP client");
    cluster.wait_metrics_up(&http).await;
    let bootstrap = cluster.bootstrap();
    let nodes = Arc::new(Nodes::new(
        cluster
            .brokers
            .iter()
            .map(|broker| Node {
                id: i32::try_from(broker.node_id).expect("a node id fits an i32"),
                container: broker.name.clone(),
                bootstrap: broker.bootstrap(),
                metrics_url: broker.metrics_url(),
                root: broker.root.path().to_path_buf(),
                launch: broker.launch.clone(),
                tls: broker.tls_port.map(|port| format!("127.0.0.1:{port}")),
            })
            .collect(),
    ));
    create_topics(&bootstrap, contract).await;
    let topics: Vec<(String, i32)> = contract
        .paths
        .iter()
        .map(|path| (path.name.clone(), path.partitions))
        .collect();
    if let Err(error) = await_full_isr(&nodes, &topics, secs(120)).await {
        for broker in &cluster.brokers {
            eprintln!("--- {}\n{}", broker.name, broker.container_log());
        }
        panic!("{error}");
    }
    (cluster, remote_tier, nodes)
}

/// Record a lifecycle run's release images and the feature ranges its source
/// advertises, before the load starts.
async fn record_releases(run: &LifecycleRun, nodes: &Nodes, evidence: &mut Evidence) {
    evidence.releases = run
        .release_ids()
        .unwrap_or_else(|error| panic!("identify the release images: {error}"));
    evidence.source_supports = supported_features(nodes)
        .await
        .unwrap_or_else(|error| panic!("read the source's feature ranges: {error}"));
    eprintln!(
        "soak-contract: {} on {} supports {:?}; releases {:?}",
        run.lifecycle.source.version,
        run.image(Side::Source),
        evidence.source_supports,
        evidence.releases
    );
}

/// The checkpoint after transition `fault`, printed as it is taken.
async fn take_checkpoint(
    fault: &str,
    run: &LifecycleRun,
    nodes: &Nodes,
    ctx: &LoadContext,
) -> Checkpoint {
    let taken = checkpoint(fault, run, nodes, ctx).await;
    eprintln!(
        "soak-contract: checkpoint after {fault}: images {:?}, finalized {:?}, {:?}, {} config mismatches",
        taken.images,
        taken.finalized,
        taken.rollback,
        taken.config_mismatches.len()
    );
    taken
}

/// Judge `evidence` against `contract`, write `verdict.json` into `dir`, print
/// every failure or detection, and say whether the run passed.
fn grade(contract: &Contract, evidence: &Evidence, dir: &Path) -> bool {
    match contract.profile {
        // `plan::unsupported` refuses a drill before any container starts.
        Profile::Soak | Profile::Lifecycle | Profile::DisasterRecovery => {
            let verdict = judge(contract, evidence);
            write_json(dir, "verdict.json", &verdict);
            for failure in verdict.failures() {
                eprintln!("soak-contract: FAILED {failure}");
            }
            verdict.passed()
        }
        Profile::SelfCheck => {
            let report = judge_self_check(contract, evidence);
            write_json(dir, "verdict.json", &report);
            for detected in &report.detected {
                eprintln!("soak-contract: detected {detected}");
            }
            for problem in &report.problems {
                eprintln!("soak-contract: PROBLEM {problem}");
            }
            report.passed
        }
    }
}

/// The sustained-load contract, executed against three broker containers and
/// judged.
///
/// See the module documentation for what runs, what it writes and what fails
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires Docker and //packaging:image; runs in the container matrix and, with KRABKA_SOAK_CONTRACT, the scheduled soak"]
async fn the_sustained_load_contract_holds() {
    let source = contract_source();
    let text = std::fs::read_to_string(&source)
        .unwrap_or_else(|error| panic!("read {}: {error}", source.display()));
    let contract =
        Contract::from_yaml(&text).unwrap_or_else(|error| panic!("{}: {error}", source.display()));
    let errors: Vec<String> = contract
        .validate()
        .into_iter()
        .map(|error| error.0)
        .collect();
    assert!(
        errors.is_empty(),
        "{} is not a runnable contract:\n{}",
        source.display(),
        errors.join("\n")
    );
    let refused = plan::unsupported(&contract);
    assert!(
        refused.is_empty(),
        "this executor cannot run {}:\n{}",
        source.display(),
        refused.join("\n")
    );
    let schedule: Vec<ScheduledFault> = expand(&contract).expect("a validated contract expands");

    let dir = evidence_dir();
    eprintln!(
        "soak-contract: running `{}` from {}; evidence in {}",
        contract.name,
        source.display(),
        dir.display()
    );
    std::fs::write(dir.join("contract.yaml"), &text).expect("write contract.yaml");
    let log = Arc::new(FaultLog::create(&dir));

    let image = image();
    let lifecycle = contract
        .lifecycle
        .clone()
        .map(|lifecycle| Arc::new(LifecycleRun::new(lifecycle, image.clone())));
    let (cluster, _remote_tier, nodes) = boot(&contract, lifecycle.as_deref()).await;
    let bootstrap = cluster.bootstrap();

    let mut evidence = Evidence {
        contract: contract.name.clone(),
        contract_digest: contract.digest(),
        seed: contract.seed,
        broker_image: image_identity(&image),
        environment: environment(cluster.brokers[0].root.path(), &image),
        ..Evidence::default()
    };
    if let Some(run) = &lifecycle {
        record_releases(run, &nodes, &mut evidence).await;
    }
    write_json(&dir, "schedule.json", &schedule);

    let endpoint = Endpoint::plaintext(bootstrap.clone());
    let prepared = prepare(&contract, &endpoint)
        .await
        .expect("prepare the load consumers");
    let load_start = now_ms();
    evidence.started_unix_ms = load_start;
    let recorders = Recorders::start(&nodes, &dir, load_start);
    let windows = Arc::new(Windows::new(
        schedule
            .iter()
            .map(|fault| planned_span(&contract, fault, load_start))
            .collect(),
    ));
    let ctx = Arc::new(
        LoadContext::builder()
            .contract(contract.clone())
            .endpoint(endpoint.clone())
            .windows(Arc::clone(&windows))
            .build(),
    );
    let load = start(&ctx, prepared);
    let trackers = load.trackers.clone();

    let executor = Executor {
        contract: contract.clone(),
        nodes: Arc::clone(&nodes),
        windows: Arc::clone(&windows),
        log,
        trackers: trackers.clone(),
        endpoint: endpoint.clone(),
        lifecycle: lifecycle.clone(),
    };
    for fault in &schedule {
        let due = load_start + fault.start.millis_i64();
        if now_ms() > due {
            eprintln!(
                "soak-contract: {} starts {}ms late: the fault before it ran long",
                fault.id,
                now_ms() - due
            );
        }
        sleep_until(due).await;
        let outcome = executor.run(fault).await;
        evidence.faults.push(outcome.record);
        evidence.recoveries.extend(outcome.recoveries);
        evidence.commits.extend(outcome.commits);
        if let Some(run) = &lifecycle
            && is_transition(&fault.kind)
        {
            evidence
                .checkpoints
                .push(take_checkpoint(&fault.id, run, &nodes, &ctx).await);
        }
        write_json(&dir, "evidence.partial.json", &evidence);
    }
    sleep_until(load_start + contract.duration.millis_i64()).await;
    let load_end = now_ms();
    ctx.stop();
    eprintln!(
        "soak-contract: load stopped after {}ms",
        load_end - load_start
    );

    let outcome = load.finish().await;
    for tracker in &trackers {
        evidence.commits.extend(
            commit_checks(&endpoint, tracker, FINAL_COMMIT_CHECK, &tracker.snapshot()).await,
        );
    }
    let steady = Time::from_millis(steady_millis(
        Span {
            start: load_start,
            end: load_end,
        },
        &windows.spans(),
    ));
    evidence.paths = outcome.measurements(&contract, steady);
    evidence.producers = outcome
        .producers
        .into_iter()
        .map(|output| output.ledger)
        .collect();

    inject_and_read_back(&contract, &endpoint, &nodes, &mut evidence).await;
    evidence.ended_unix_ms = now_ms();
    write_json(&dir, "evidence.json", &evidence);

    let recorded = recorders.finish(&dir, load_start).await;
    for line in timing_summary(&evidence, load_start)
        .into_iter()
        .chain(recorded)
    {
        eprintln!("soak-contract: {line}");
    }
    let passed = grade(&contract, &evidence, &dir);
    if !passed {
        for broker in &cluster.brokers {
            eprintln!("--- {}\n{}", broker.name, broker.container_log());
        }
    }
    assert!(
        passed,
        "`{}` did not hold; evidence and verdict in {}",
        contract.name,
        dir.display()
    );
}

/// The rollback the lifecycle contract may not take is refused by its source
/// release before it changes the log, and the candidate reopens the log.
///
/// See the `rollback_guard` module documentation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, //packaging:image and the lifecycle contract's source release image"]
async fn an_unsupported_rollback_is_refused_before_it_damages_data() {
    let contract = lifecycle_contract();
    let dir = evidence_dir();
    let outcome = rollback_guard::refuse_unsupported_rollback(&contract, &image())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    write_json(&dir, "unsupported-rollback.json", &outcome);
    eprintln!("soak-contract: unsupported rollback: {outcome:?}");
    assert!(
        outcome.changed.is_empty(),
        "the refused rollback changed {:?}",
        outcome.changed
    );
    assert!(
        outcome.record_read_back,
        "the candidate did not read the record back"
    );
}
