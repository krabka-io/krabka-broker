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
//! | `evidence.json` | after the read-back |
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
#[path = "soak_contract/load.rs"]
mod load;
#[path = "soak_contract/plan.rs"]
mod plan;
#[path = "soak_contract/readback.rs"]
mod readback;
#[path = "soak_contract/telemetry.rs"]
mod telemetry;
#[path = "soak_contract/topology.rs"]
mod topology;
#[path = "soak_contract/windows.rs"]
mod windows;

use {
    crate::{
        cluster::{SoakCluster, image},
        executor::{Executor, await_full_isr},
        host::{FaultLog, environment, evidence_dir, image_identity, now_ms, write_json},
        load::{
            CommitTracker, LoadContext, ProducerOutput, group_name, producer_name, run_consumer,
            run_idempotent, run_transactional,
        },
        readback::{commit_checks, delete_one_record, read_back, wait_stable},
        support::{manifest_dir, topics::topic_configs},
        topology::{Node, Nodes},
        windows::{Latencies, Span, Windows, measurement, planned_span, steady_millis},
    },
    assert2::assert,
    krabka_bench_driver::soak::{
        contract::{Contract, Profile, Semantics},
        evidence::{Evidence, FINAL_COMMIT_CHECK},
        schedule::{ScheduledFault, expand},
        verdict::{judge, judge_self_check},
    },
    krabka_protocol::owned::create_topics_request::CreatableTopic,
    krabka_units::prelude::*,
    std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    },
    tokio::task::JoinHandle,
};

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
    bootstrap: &str,
    nodes: &Nodes,
    evidence: &mut Evidence,
) {
    for path in &contract.paths {
        wait_stable(bootstrap, path, contract.clients.transactions.timeout * 2.0).await;
    }
    if contract.profile == Profile::SelfCheck
        && let Some(self_check) = &contract.self_check
        && let Some(path) = contract.path(&self_check.missing_record_path)
        && let Some(ledger) = evidence
            .producers
            .iter()
            .find(|ledger| ledger.path == path.name)
    {
        match delete_one_record(bootstrap, nodes, path, ledger).await {
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
                .push(read_back(bootstrap, path, isolation, &producer_name(&path.name)).await);
        }
    }
}

/// Boot the contract's cluster, create its topics and wait for their ISRs.
///
/// Returns the cluster, the object-store directory its nodes share (kept
/// alive for as long as the cluster), and the executor's view of its nodes.
async fn boot(contract: &Contract) -> (SoakCluster, Option<tempfile::TempDir>, Arc<Nodes>) {
    let remote_tier = plan::object_store(contract)
        .then(|| tempfile::TempDir::new().expect("object store directory"));
    let toml = plan::node_toml(contract);
    let cluster = SoakCluster::configured()
        .root_toml(toml.root)
        .tables_toml(toml.tables)
        .diskless_quorum(plan::diskless(contract))
        .maybe_shared_mount(
            remote_tier
                .as_ref()
                .map(|tier| (tier.path().to_path_buf(), plan::REMOTE_TIER.to_owned())),
        )
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

/// Judge `evidence` against `contract`, write `verdict.json` into `dir`, print
/// every failure or detection, and say whether the run passed.
fn grade(contract: &Contract, evidence: &Evidence, dir: &Path) -> bool {
    match contract.profile {
        Profile::Soak => {
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

    let (cluster, _remote_tier, nodes) = boot(&contract).await;
    let bootstrap = cluster.bootstrap();

    let image = image();
    let mut evidence = Evidence {
        contract: contract.name.clone(),
        contract_digest: contract.digest(),
        seed: contract.seed,
        broker_image: image_identity(&image),
        environment: environment(cluster.brokers[0].root.path(), &image),
        ..Evidence::default()
    };
    write_json(&dir, "schedule.json", &schedule);

    let load_start = now_ms();
    evidence.started_unix_ms = load_start;
    let windows = Arc::new(Windows::new(
        schedule
            .iter()
            .map(|fault| planned_span(&contract, fault, load_start))
            .collect(),
    ));
    let ctx = Arc::new(LoadContext::new(
        contract.clone(),
        bootstrap.clone(),
        Arc::clone(&windows),
    ));
    let producers: Vec<JoinHandle<ProducerOutput>> = contract
        .paths
        .iter()
        .map(|path| match path.semantics {
            Semantics::Transactional => {
                tokio::spawn(run_transactional(Arc::clone(&ctx), path.clone()))
            }
            Semantics::Idempotent | Semantics::Compacted => {
                tokio::spawn(run_idempotent(Arc::clone(&ctx), path.clone()))
            }
        })
        .collect();
    let mut trackers = Vec::new();
    let mut consumers: Vec<(String, JoinHandle<Latencies>)> = Vec::new();
    for path in &contract.paths {
        for &isolation in &path.consumers {
            let tracker = path.commit_offsets.then(|| {
                Arc::new(CommitTracker::new(
                    group_name(&path.name, isolation),
                    path.name.clone(),
                    path.partitions,
                ))
            });
            if let Some(tracker) = &tracker {
                trackers.push(Arc::clone(tracker));
            }
            consumers.push((
                path.name.clone(),
                tokio::spawn(run_consumer(
                    Arc::clone(&ctx),
                    path.clone(),
                    isolation,
                    tracker,
                )),
            ));
        }
    }

    let executor = Executor {
        contract: contract.clone(),
        nodes: Arc::clone(&nodes),
        windows: Arc::clone(&windows),
        log,
        trackers: trackers.clone(),
        bootstrap: bootstrap.clone(),
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
    }
    sleep_until(load_start + contract.duration.millis_i64()).await;
    let load_end = now_ms();
    ctx.stop();
    eprintln!(
        "soak-contract: load stopped after {}ms",
        load_end - load_start
    );

    let mut produce_latency: BTreeMap<String, Latencies> = BTreeMap::new();
    for task in producers {
        let output = task.await.expect("a producer task");
        produce_latency.insert(output.ledger.path.clone(), output.latencies);
        evidence.producers.push(output.ledger);
    }
    let mut consume_latency: BTreeMap<String, Latencies> = BTreeMap::new();
    for (path, task) in consumers {
        let latencies = task.await.expect("a consumer task");
        consume_latency
            .entry(path)
            .or_insert_with(Latencies::new)
            .merge(&latencies);
    }
    for tracker in &trackers {
        evidence.commits.extend(
            commit_checks(&bootstrap, tracker, FINAL_COMMIT_CHECK, &tracker.snapshot()).await,
        );
    }
    let steady = Time::from_millis(steady_millis(
        Span {
            start: load_start,
            end: load_end,
        },
        &windows.spans(),
    ));
    for path in &contract.paths {
        let empty = Latencies::new();
        evidence.paths.push(measurement(
            &path.name,
            produce_latency.get(&path.name).unwrap_or(&empty),
            consume_latency.get(&path.name).unwrap_or(&empty),
            steady,
        ));
    }

    inject_and_read_back(&contract, &bootstrap, &nodes, &mut evidence).await;
    evidence.ended_unix_ms = now_ms();
    write_json(&dir, "evidence.json", &evidence);

    for line in timing_summary(&evidence, load_start) {
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
