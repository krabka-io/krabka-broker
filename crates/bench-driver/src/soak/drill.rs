//! The disaster-recovery drill's side of a soak: the declared load run against
//! the source cluster until it is lost, and the read-back of the cluster
//! restored from the capture.
//!
//! `krabka-soak-drill` is the command-line front end. Everything it does
//! besides parsing arguments and listening for `SIGTERM` is here, so the load
//! and the read-back are exercised against a live broker by the
//! `soak_drill` suite of `krabka-broker`.

pub mod cli;
#[cfg(test)]
mod tests;

use {
    super::{
        client::{
            Endpoint,
            load::{CommitTracker, LoadContext, producer_name, start},
            now_ms,
            readback::{commit_checks, read_back, round_trip},
            windows::Windows,
        },
        contract::{Contract, Profile},
        evidence::{Evidence, FINAL_COMMIT_CHECK},
        recovery::{
            Boundary, CommitHistory, LoadRecord, ProducerAcks, RestoredReadings, bound_evidence,
            committed_by,
        },
    },
    anyhow::{Context, Result, bail},
    krabka_units::prelude::*,
    serde::Serialize,
    std::{future::Future, path::Path, sync::Arc},
};

/// A disaster-recovery contract's objectives and paths, in the units the
/// drill's shell side reads: whole milliseconds and topic shapes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Objectives {
    pub post_capture_load_ms: i64,
    pub rpo_ms: i64,
    pub rto_ms: i64,
    pub paths: Vec<PathShape>,
}

/// The topic a path writes to, as the drill must create it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PathShape {
    pub name: String,
    pub storage: String,
    pub partitions: i32,
    pub min_insync_replicas: i16,
}

/// Read and validate the contract at `path`, which must be a
/// `disaster_recovery` contract.
///
/// # Errors
///
/// The file cannot be read, does not parse, or is another profile.
pub fn load_contract(path: &Path) -> Result<Contract> {
    let contract = Contract::from_file(path)?;
    if contract.profile != Profile::DisasterRecovery {
        bail!(
            "{} is a {:?} contract; the drill runs only disaster_recovery contracts",
            path.display(),
            contract.profile
        );
    }
    Ok(contract)
}

/// The objectives and the paths of a disaster-recovery contract.
///
/// # Errors
///
/// The contract declares no `disaster_recovery` objectives.
pub fn objectives(contract: &Contract) -> Result<Objectives> {
    let declared = contract
        .disaster_recovery
        .as_ref()
        .context("a disaster-recovery contract declares its objectives")?;
    Ok(Objectives {
        post_capture_load_ms: declared.post_capture_load.millis_i64(),
        rpo_ms: declared.rpo.millis_i64(),
        rto_ms: declared.rto.millis_i64(),
        paths: contract
            .paths
            .iter()
            .map(|path| PathShape {
                name: path.name.clone(),
                storage: serde_json::to_value(path.storage)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_default(),
                partitions: path.partitions,
                min_insync_replicas: contract.cluster.min_insync_replicas,
            })
            .collect(),
    })
}

/// Write `value` as JSON through a rename, so a reader never sees half a file.
///
/// # Errors
///
/// The file cannot be written or renamed into place.
pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let partial = path.with_extension("partial");
    std::fs::write(&partial, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", partial.display()))?;
    std::fs::rename(&partial, path).with_context(|| format!("rename to {}", path.display()))
}

/// Read a load record that [`run_load`] wrote.
///
/// # Errors
///
/// The file cannot be read or is not a load record.
pub fn read_load(path: &Path) -> Result<LoadRecord> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse load record {}", path.display()))
}

/// Run the contract's load until `lost` completes or the contract's duration
/// elapses, and record every acknowledgement and commit with its time.
///
/// When `lost` completes first, every send in flight is abandoned, because no
/// broker is left to answer it.
pub async fn run_load(
    contract: &Contract,
    endpoint: Endpoint,
    broker_image: String,
    lost: impl Future<Output = ()>,
) -> LoadRecord {
    let ctx = Arc::new(
        LoadContext::builder()
            .contract(contract.clone())
            .endpoint(endpoint)
            .windows(Arc::new(Windows::new(Vec::new())))
            .record_ack_times(true)
            .build(),
    );
    let started = now_ms();
    let running = start(&ctx);
    tracing::info!(contract = %contract.name, "drill load started");
    tokio::select! {
        () = lost => {
            ctx.abandon();
            tracing::info!("the source cluster is lost; abandoning the sends in flight");
        }
        () = tokio::time::sleep(contract.duration.to_std()) => {
            ctx.stop();
            tracing::info!("the load ran its whole duration without a loss");
        }
    }
    let ended = now_ms();
    let trackers = running.trackers.clone();
    let outcome = running.finish().await;
    let paths = outcome.measurements(contract, Time::from_millis(ended - started));
    LoadRecord {
        acks: outcome
            .producers
            .iter()
            .map(|output| ProducerAcks {
                producer: output.ledger.producer.clone(),
                acks: output.acked_at.clone(),
            })
            .collect(),
        commits: trackers
            .iter()
            .map(|tracker| CommitHistory {
                group: tracker.group.clone(),
                topic: tracker.topic.clone(),
                partitions: tracker.partitions,
                acks: tracker.history(),
            })
            .collect(),
        run: Evidence {
            contract: contract.name.clone(),
            contract_digest: contract.digest(),
            seed: contract.seed,
            broker_image,
            started_unix_ms: started,
            ended_unix_ms: ended,
            producers: outcome
                .producers
                .into_iter()
                .map(|output| output.ledger)
                .collect(),
            paths,
            ..Evidence::default()
        },
    }
}

/// Read every path of the restored cluster back, check each committing
/// group's restored offsets against its last commit before the boundary, and
/// return the evidence the judge grades.
pub async fn read_back_restored(
    contract: &Contract,
    load: &LoadRecord,
    endpoint: &Endpoint,
    boundary: Boundary,
    serving_unix_ms: Option<i64>,
) -> Evidence {
    let mut readers = Vec::new();
    for path in &contract.paths {
        for &isolation in &path.consumers {
            readers.push(read_back(endpoint, path, isolation, &producer_name(&path.name)).await);
        }
    }
    let mut commits = Vec::new();
    for history in &load.commits {
        let tracker = CommitTracker::new(
            history.group.clone(),
            history.topic.clone(),
            history.partitions,
        );
        let acknowledged = committed_by(&history.acks, boundary.capture_started_unix_ms);
        commits.extend(commit_checks(endpoint, &tracker, FINAL_COMMIT_CHECK, &acknowledged).await);
    }
    bound_evidence(load, boundary, RestoredReadings {
        readers,
        commits,
        serving_unix_ms,
        ended_unix_ms: now_ms(),
    })
}

/// `krabka-soak-drill load`: run the load until `lost`, report each
/// producer's totals, and write the record to `out`.
///
/// # Errors
///
/// The record cannot be written.
pub async fn record_load(
    contract: &Contract,
    endpoint: Endpoint,
    broker_image: String,
    lost: impl Future<Output = ()>,
    out: &Path,
) -> Result<LoadRecord> {
    let record = run_load(contract, endpoint, broker_image, lost).await;
    for ledger in &record.run.producers {
        tracing::info!(
            producer = %ledger.producer,
            attempted = ledger.attempted,
            acknowledged = ledger.acked.len(),
            failed = ledger.failed.len(),
            "drill load recorded",
        );
    }
    write_json(out, &record)?;
    Ok(record)
}

/// `krabka-soak-drill readback`: read the restored cluster back against the
/// load recorded at `load`, report each path's split at the boundary, and
/// write the evidence to `out`.
///
/// # Errors
///
/// The load record cannot be read, or the evidence cannot be written.
pub async fn record_readback(
    contract: &Contract,
    load: &Path,
    endpoint: &Endpoint,
    boundary: Boundary,
    serving_unix_ms: Option<i64>,
    out: &Path,
) -> Result<Evidence> {
    let record = read_load(load)?;
    let evidence = read_back_restored(contract, &record, endpoint, boundary, serving_unix_ms).await;
    for path in evidence
        .recovery
        .iter()
        .flat_map(|recovery| &recovery.paths)
    {
        tracing::info!(
            path = %path.path,
            acknowledged = path.acknowledged,
            before_boundary = path.before_boundary,
            lost_beyond_boundary = path.lost_beyond_boundary.len(),
            oldest_loss_unix_ms = ?path.oldest_loss_unix_ms,
            "restored cluster read back",
        );
    }
    write_json(out, &evidence)?;
    Ok(evidence)
}

/// `krabka-soak-drill probe`: write one record to `topic`'s `partition` and
/// read it back, and describe the outcome in the line the gate checks for.
///
/// # Errors
///
/// The write or the read fails.
pub async fn probe(endpoint: &Endpoint, topic: &str, partition: i32) -> Result<String> {
    let offset = round_trip(endpoint, topic, partition)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(format!(
        "{topic}-{partition}: wrote and read back offset {offset}"
    ))
}
