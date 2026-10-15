//! The performance record: every raw trial of one comparison, with the
//! provenance needed to reproduce it.
//!
//! `packaging/performance/qualify.sh budget` writes one directory per trial.
//! [`assemble`] reads that layout into a [`PerformanceRecord`], which is what
//! is published and what [`evaluate`](crate::budget::evaluate) judges.
//!
//! ```text
//! <artifact-dir>/
//!   contract.json              the contract, copied before the first trial
//!   contract.sha256            its digest, as `sha256sum` printed it
//!   provenance.env             revisions, images, digests, settings
//!   host.env                   host resources
//!   trials/round-<r>/<side>/<cell>/
//!     order                    the trial's position in the run, from 1
//!     workload.json            BrokerPerformanceWorkload's result, if it finished
//!     workload.log             its stderr
//!     resources-before.txt     per-broker /proc snapshot before the trial
//!     resources-after.txt      the same snapshot after it
//! ```

use {
    crate::{
        contract::{BudgetError, Contract, FORMAT_VERSION, Mode, contract_digest},
        num::to_f64,
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
    },
};

/// Which side of the comparison a trial measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// The revision or broker the candidate is compared against.
    Reference,
    /// The revision under test.
    Candidate,
}

impl Side {
    /// Both sides, reference first.
    pub const BOTH: [Side; 2] = [Side::Reference, Side::Candidate];

    /// The directory name and record spelling of this side.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Side::Reference => "reference",
            Side::Candidate => "candidate",
        }
    }
}

/// One side's exact build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subject {
    /// What the report calls this side, such as `krabka` or `kafka`.
    pub label: String,
    /// The git revision, or the Kafka release for a pinned Kafka reference.
    pub revision: String,
    /// The image reference the cluster ran.
    pub image: String,
    /// The image's content digest.
    pub image_digest: String,
}

/// The host every trial ran on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Host {
    /// `model name` from `/proc/cpuinfo`.
    pub cpu_model: String,
    /// Logical CPUs online.
    pub logical_cpus: u32,
    /// `MemTotal`, in bytes.
    pub memory_bytes: u64,
    /// Bytes available on the artifact filesystem when the run started.
    pub disk_available_bytes: u64,
    /// `uname -r`.
    pub kernel: String,
    /// `getconf CLK_TCK`, the unit of `/proc/<pid>/stat` CPU times.
    pub clock_ticks_per_second: u32,
}

/// What one completed workload measured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Measurement {
    /// Records the producer saw acknowledged.
    pub sent: u64,
    /// Distinct records the consumer read back.
    pub consumed: u64,
    /// Records the consumer read more than once.
    pub duplicates: u64,
    /// Producer send failures.
    pub errors: u64,
    /// Wall time from the first send to the last record consumed.
    pub seconds: f64,
    /// Consumed records per second over that wall time.
    pub records_per_second: f64,
    /// Consumed payload MiB per second over that wall time.
    pub mib_per_second: f64,
    /// Median end-to-end latency.
    pub latency_ms_p50: f64,
    /// 95th percentile end-to-end latency.
    pub latency_ms_p95: f64,
    /// 99th percentile end-to-end latency.
    pub latency_ms_p99: f64,
    /// Broker CPU seconds over the trial, summed over every broker.
    pub cpu_seconds: f64,
    /// Peak resident set size, summed over every broker, in bytes.
    pub peak_rss_bytes: u64,
}

/// How a trial ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TrialOutcome {
    /// The workload finished and reported a result.
    Completed(Measurement),
    /// The workload or the harness failed; nothing is measured.
    Failed {
        /// Why, as specifically as the artifacts say.
        reason: String,
    },
}

/// One raw trial.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trial {
    /// The contract cell this trial ran.
    pub cell: String,
    /// The interleaved round, from 1.
    pub round: usize,
    /// Which side ran.
    pub side: Side,
    /// The trial's position in the whole run, from 1.
    pub order: usize,
    /// How it ended.
    pub outcome: TrialOutcome,
}

/// Every trial of one comparison and where it came from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceRecord {
    /// Must be [`FORMAT_VERSION`].
    pub format_version: u32,
    /// The contract's name.
    pub contract_name: String,
    /// The SHA-256 of the contract's bytes, captured before the first trial.
    pub contract_sha256: String,
    /// What the candidate was compared against.
    pub mode: Mode,
    /// The reference side.
    pub reference: Subject,
    /// The candidate side.
    pub candidate: Subject,
    /// The client image whose jars drove both sides.
    pub client_image: String,
    /// That image's digest.
    pub client_image_digest: String,
    /// The host.
    pub host: Host,
    /// When the first trial started, RFC 3339 UTC.
    pub started_at: String,
    /// Every other harness setting, verbatim from `provenance.env`.
    pub settings: BTreeMap<String, String>,
    /// Every trial that ran, in no particular order.
    pub trials: Vec<Trial>,
}

impl PerformanceRecord {
    /// Read a record from its JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] when the bytes are not a record or declare
    /// another format version.
    pub fn from_json(bytes: &[u8]) -> Result<Self, BudgetError> {
        let record: PerformanceRecord = serde_json::from_slice(bytes)?;
        if record.format_version != FORMAT_VERSION {
            return Err(BudgetError::FormatVersion {
                what: "record",
                found: record.format_version,
            });
        }
        Ok(record)
    }
}

fn artifact_error(path: &str, reason: impl Into<String>) -> BudgetError {
    BudgetError::InvalidArtifact {
        path: path.to_string(),
        reason: reason.into(),
    }
}

/// Parse `key=value` lines. Blank lines and `#` comments are skipped; a
/// repeated key is an error, so a snapshot cannot silently lose a value.
///
/// # Errors
///
/// Returns [`BudgetError::InvalidArtifact`] for a line without `=` or a
/// repeated key.
pub fn parse_env(path: &str, text: &str) -> Result<BTreeMap<String, String>, BudgetError> {
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(artifact_error(path, format!("line {line:?} has no '='")));
        };
        if values
            .insert(key.trim().to_string(), value.trim().to_string())
            .is_some()
        {
            return Err(artifact_error(path, format!("key {key} appears twice")));
        }
    }
    Ok(values)
}

fn take(
    path: &str,
    values: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<String, BudgetError> {
    values
        .remove(key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| artifact_error(path, format!("missing {key}")))
}

fn take_number<T: std::str::FromStr>(
    path: &str,
    values: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<T, BudgetError> {
    let raw = take(path, values, key)?;
    raw.parse()
        .map_err(|_| artifact_error(path, format!("{key}={raw} is not a number")))
}

/// Parse `host.env`.
///
/// # Errors
///
/// Returns [`BudgetError::InvalidArtifact`] for a missing or malformed key.
pub fn parse_host(text: &str) -> Result<Host, BudgetError> {
    let path = "host.env";
    let mut values = parse_env(path, text)?;
    let host = Host {
        cpu_model: take(path, &mut values, "cpu_model")?,
        logical_cpus: take_number(path, &mut values, "logical_cpus")?,
        memory_bytes: take_number(path, &mut values, "memory_bytes")?,
        disk_available_bytes: take_number(path, &mut values, "disk_available_bytes")?,
        kernel: take(path, &mut values, "kernel")?,
        clock_ticks_per_second: take_number(path, &mut values, "clock_ticks_per_second")?,
    };
    if host.clock_ticks_per_second == 0 {
        return Err(artifact_error(
            path,
            "clock_ticks_per_second must be positive",
        ));
    }
    Ok(host)
}

/// One broker process in a resource snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessSample {
    /// `utime + stime` from `/proc/<pid>/stat`, in clock ticks.
    pub cpu_ticks: u64,
    /// `starttime` from `/proc/<pid>/stat`, in clock ticks since boot. A
    /// container restart keeps the pod name but changes this.
    pub process_start_ticks: u64,
    /// `VmHWM` from `/proc/<pid>/status`: peak RSS since the process started.
    pub rss_hwm_kib: u64,
}

/// Parse a resource snapshot: blocks that each open with `pod=<name>`.
///
/// Keys this function does not need, such as `fd_count` or the network
/// counters, are kept in the raw artifact and skipped here.
///
/// # Errors
///
/// Returns [`BudgetError::InvalidArtifact`] for a key outside a pod block, a
/// pod named twice, or a pod without `cpu_ticks`, `process_start_ticks` or
/// `rss_hwm_kib`.
pub fn parse_snapshot(
    path: &str,
    text: &str,
) -> Result<BTreeMap<String, ProcessSample>, BudgetError> {
    let mut pods: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(artifact_error(path, format!("line {line:?} has no '='")));
        };
        if key == "pod" {
            if pods.insert(value.to_string(), BTreeMap::new()).is_some() {
                return Err(artifact_error(path, format!("pod {value} appears twice")));
            }
            current = Some(value.to_string());
            continue;
        }
        let Some(pod) = &current else {
            return Err(artifact_error(
                path,
                format!("{key} precedes the first pod="),
            ));
        };
        if let Some(block) = pods.get_mut(pod) {
            block.insert(key.to_string(), value.to_string());
        }
    }
    if pods.is_empty() {
        return Err(artifact_error(path, "no pod in the snapshot"));
    }
    pods.into_iter()
        .map(|(pod, mut block)| {
            let sample = ProcessSample {
                cpu_ticks: take_number(path, &mut block, "cpu_ticks")?,
                process_start_ticks: take_number(path, &mut block, "process_start_ticks")?,
                rss_hwm_kib: take_number(path, &mut block, "rss_hwm_kib")?,
            };
            Ok((pod, sample))
        })
        .collect()
}

/// Broker CPU seconds and summed peak RSS bytes between two snapshots.
///
/// # Errors
///
/// Returns the failure reason when the two snapshots name different pods, or a
/// pod's process start time or CPU counter differs in a way one process cannot
/// produce. Each means a broker process was replaced during the trial, so its
/// numbers describe no single process. A restarted container keeps its pod
/// name, so the start time is what catches it.
pub fn resource_delta(
    before: &BTreeMap<String, ProcessSample>,
    after: &BTreeMap<String, ProcessSample>,
    clock_ticks_per_second: u32,
) -> Result<(f64, u64), String> {
    if before.keys().ne(after.keys()) {
        return Err("the broker pod set changed during the trial".to_string());
    }
    let mut ticks = 0_u64;
    let mut rss_bytes = 0_u64;
    for (pod, start) in before {
        let end = after[pod];
        if end.process_start_ticks != start.process_start_ticks {
            return Err(format!(
                "{pod}'s broker process started again during the trial"
            ));
        }
        let Some(used) = end.cpu_ticks.checked_sub(start.cpu_ticks) else {
            return Err(format!(
                "{pod}'s CPU counter went backwards: its broker process restarted"
            ));
        };
        ticks = ticks.saturating_add(used);
        rss_bytes = rss_bytes.saturating_add(end.rss_hwm_kib.saturating_mul(1024));
    }
    Ok((to_f64(ticks) / f64::from(clock_ticks_per_second), rss_bytes))
}

/// The JSON object `BrokerPerformanceWorkload` prints on success.
#[derive(Debug, Deserialize)]
struct WorkloadResult {
    sent: u64,
    consumed: u64,
    duplicates: u64,
    errors: u64,
    seconds: f64,
    records_per_second: f64,
    mib_per_second: f64,
    latency_ms_p50: f64,
    latency_ms_p95: f64,
    latency_ms_p99: f64,
}

/// The last non-empty line of a workload log, which is where the workload's
/// own `IllegalStateException` names the counts that disagreed.
fn last_log_line(log: &str) -> Option<&str> {
    log.lines().map(str::trim).rfind(|line| !line.is_empty())
}

fn read_optional(path: &Path) -> Result<Option<String>, BudgetError> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Read one trial directory into its outcome.
fn trial_outcome(
    directory: &Path,
    relative: &str,
    clock_ticks_per_second: u32,
) -> Result<TrialOutcome, BudgetError> {
    let failed = |reason: String| Ok(TrialOutcome::Failed { reason });
    let Some(workload) = read_optional(&directory.join("workload.json"))? else {
        let log = read_optional(&directory.join("workload.log"))?.unwrap_or_default();
        return failed(match last_log_line(&log) {
            Some(line) => format!("the workload reported no result: {line}"),
            None => "the workload reported no result".to_string(),
        });
    };
    let Ok(result) = serde_json::from_str::<WorkloadResult>(&workload) else {
        return failed("workload.json is not a workload result".to_string());
    };
    let snapshot = |name: &str| -> Result<Option<BTreeMap<String, ProcessSample>>, BudgetError> {
        read_optional(&directory.join(name))?
            .map(|text| parse_snapshot(&format!("{relative}/{name}"), &text))
            .transpose()
    };
    let (Some(before), Some(after)) = (
        snapshot("resources-before.txt")?,
        snapshot("resources-after.txt")?,
    ) else {
        return failed("a resource snapshot is missing".to_string());
    };
    let (cpu_seconds, peak_rss_bytes) =
        match resource_delta(&before, &after, clock_ticks_per_second) {
            Ok(delta) => delta,
            Err(reason) => return failed(reason),
        };
    Ok(TrialOutcome::Completed(Measurement {
        sent: result.sent,
        consumed: result.consumed,
        duplicates: result.duplicates,
        errors: result.errors,
        seconds: result.seconds,
        records_per_second: result.records_per_second,
        mib_per_second: result.mib_per_second,
        latency_ms_p50: result.latency_ms_p50,
        latency_ms_p95: result.latency_ms_p95,
        latency_ms_p99: result.latency_ms_p99,
        cpu_seconds,
        peak_rss_bytes,
    }))
}

fn parse_mode(path: &str, raw: &str) -> Result<Mode, BudgetError> {
    [Mode::BeforeAfter, Mode::PinnedKafka]
        .into_iter()
        .find(|mode| mode.name() == raw)
        .ok_or_else(|| artifact_error(path, format!("mode={raw} is not a mode")))
}

/// Assemble the record of a `qualify.sh budget` artifact directory.
///
/// Every (round, side, cell) the contract expects and whose trial directory
/// exists becomes one [`Trial`]. A directory that is absent means the harness
/// stopped before reaching it: that trial is left out, and evaluation reports
/// it missing.
///
/// # Errors
///
/// Returns [`BudgetError`] when the copied contract is invalid or does not
/// match its recorded digest, or a provenance, host or snapshot file is
/// malformed.
pub fn assemble(artifact_dir: &Path) -> Result<PerformanceRecord, BudgetError> {
    let contract_bytes = fs::read(artifact_dir.join("contract.json"))?;
    let contract = Contract::from_json(&contract_bytes)?;
    let mut provenance = parse_env(
        "provenance.env",
        &fs::read_to_string(artifact_dir.join("provenance.env"))?,
    )?;
    let path = "provenance.env";
    let recorded_digest = take(path, &mut provenance, "contract_sha256")?;
    if recorded_digest != contract_digest(&contract_bytes) {
        return Err(artifact_error(
            "contract.json",
            "its bytes do not match the contract_sha256 recorded before the run",
        ));
    }
    let mode = parse_mode(path, &take(path, &mut provenance, "mode")?)?;
    let mut subject = |side: Side| -> Result<Subject, BudgetError> {
        let prefix = side.name();
        Ok(Subject {
            label: take(path, &mut provenance, &format!("{prefix}_label"))?,
            revision: take(path, &mut provenance, &format!("{prefix}_revision"))?,
            image: take(path, &mut provenance, &format!("{prefix}_image"))?,
            image_digest: take(path, &mut provenance, &format!("{prefix}_image_digest"))?,
        })
    };
    let reference = subject(Side::Reference)?;
    let candidate = subject(Side::Candidate)?;
    let client_image = take(path, &mut provenance, "client_image")?;
    let client_image_digest = take(path, &mut provenance, "client_image_digest")?;
    let started_at = take(path, &mut provenance, "started_at")?;
    let host = parse_host(&fs::read_to_string(artifact_dir.join("host.env"))?)?;

    let mut trials = Vec::new();
    for round in 1..=contract.rounds {
        for side in Side::BOTH {
            for cell in &contract.cells {
                let relative = format!("trials/round-{round}/{}/{}", side.name(), cell.id);
                let directory: PathBuf = artifact_dir.join(&relative);
                if !directory.is_dir() {
                    continue;
                }
                let order_path = format!("{relative}/order");
                let order_text = fs::read_to_string(directory.join("order"))?;
                let order = order_text
                    .trim()
                    .parse()
                    .map_err(|_| artifact_error(&order_path, "not a trial position"))?;
                trials.push(Trial {
                    cell: cell.id.clone(),
                    round,
                    side,
                    order,
                    outcome: trial_outcome(&directory, &relative, host.clock_ticks_per_second)?,
                });
            }
        }
    }
    trials.sort_by_key(|trial| trial.order);
    Ok(PerformanceRecord {
        format_version: FORMAT_VERSION,
        contract_name: contract.name,
        contract_sha256: recorded_digest,
        mode,
        reference,
        candidate,
        client_image,
        client_image_digest,
        host,
        started_at,
        settings: provenance,
        trials,
    })
}
