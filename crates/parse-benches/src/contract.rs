//! The regression contract: the workload matrix and the budgets that a
//! performance comparison is judged by.
//!
//! The contract is declared before any trial runs. The harness records the
//! SHA-256 of the contract's bytes when it starts, and
//! [`evaluate`](crate::budget::evaluate) refuses a record whose digest does
//! not match the contract it is handed. A budget edited after the numbers are
//! in therefore cannot judge them.

use {
    serde::{Deserialize, Serialize},
    sha2::{Digest, Sha256},
    std::collections::{BTreeMap, BTreeSet},
    thiserror::Error,
};

/// The only contract and record format version this crate reads and writes.
pub const FORMAT_VERSION: u32 = 1;

/// The fewest interleaved rounds a contract may declare. The criterion lane's
/// [`compare_summaries`](crate::compare_summaries) has the same floor: a
/// median absolute deviation over fewer than three samples says nothing.
pub const MINIMUM_ROUNDS: usize = 3;

/// The smallest record the workload can carry: it writes an 8-byte send
/// timestamp and an 8-byte sequence number into every value.
pub const MINIMUM_RECORD_BYTES: u32 = 16;

/// Errors from loading a contract, assembling a record, or evaluating one.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BudgetError {
    /// The contract or record declares a format version this crate cannot read.
    #[error("{what} format version {found} is not {FORMAT_VERSION}")]
    FormatVersion {
        /// Which document carried the version.
        what: &'static str,
        /// The version it declared.
        found: u32,
    },

    /// The contract is internally inconsistent.
    #[error("invalid contract: {0}")]
    InvalidContract(String),

    /// The record was measured under a different contract.
    #[error("record was measured under contract {recorded} but evaluation was given {supplied}")]
    ContractMismatch {
        /// The contract name and digest the record carries.
        recorded: String,
        /// The contract name and digest supplied for evaluation.
        supplied: String,
    },

    /// The record is structurally invalid.
    #[error("invalid record: {0}")]
    InvalidRecord(String),

    /// A harness artifact could not be parsed.
    #[error("invalid artifact {path}: {reason}")]
    InvalidArtifact {
        /// The artifact path, relative to the artifact directory.
        path: String,
        /// What was wrong with it.
        reason: String,
    },

    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Underlying JSON error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// What a comparison measures the candidate against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// The reference is an earlier krabka revision.
    BeforeAfter,
    /// The reference is the pinned Apache Kafka image.
    PinnedKafka,
}

impl Mode {
    /// The name the harness and the record use for this mode.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Mode::BeforeAfter => "before_after",
            Mode::PinnedKafka => "pinned_kafka",
        }
    }
}

/// Which way a metric gets better.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// A larger value is better, as for throughput.
    HigherIsBetter,
    /// A smaller value is better, as for latency, CPU and memory.
    LowerIsBetter,
}

/// A quantity every completed trial reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    /// Acknowledged and consumed records per second, end to end.
    RecordsPerSecond,
    /// Median end-to-end latency in milliseconds.
    LatencyMsP50,
    /// 95th percentile end-to-end latency in milliseconds.
    LatencyMsP95,
    /// 99th percentile end-to-end latency in milliseconds.
    LatencyMsP99,
    /// Broker CPU, summed over every broker, per acknowledged record.
    CpuMicrosPerRecord,
    /// Peak resident set size, summed over every broker, in MiB.
    PeakRssMib,
}

impl Metric {
    /// Every metric, in report order.
    pub const ALL: [Metric; 6] = [
        Metric::RecordsPerSecond,
        Metric::LatencyMsP50,
        Metric::LatencyMsP95,
        Metric::LatencyMsP99,
        Metric::CpuMicrosPerRecord,
        Metric::PeakRssMib,
    ];

    /// Which way this metric gets better.
    #[must_use]
    pub fn direction(self) -> Direction {
        match self {
            Metric::RecordsPerSecond => Direction::HigherIsBetter,
            Metric::LatencyMsP50
            | Metric::LatencyMsP95
            | Metric::LatencyMsP99
            | Metric::CpuMicrosPerRecord
            | Metric::PeakRssMib => Direction::LowerIsBetter,
        }
    }

    /// The column heading the rendered report uses.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Metric::RecordsPerSecond => "records/s",
            Metric::LatencyMsP50 => "p50 ms",
            Metric::LatencyMsP95 => "p95 ms",
            Metric::LatencyMsP99 => "p99 ms",
            Metric::CpuMicrosPerRecord => "CPU µs/record",
            Metric::PeakRssMib => "peak RSS MiB",
        }
    }
}

/// How a cell's record values are filled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    /// Bytes from a seeded random pool, which compression cannot shrink.
    Random,
    /// Zero bytes, which compress almost completely.
    Zeros,
}

impl Payload {
    /// The argument `BrokerPerformanceWorkload` takes for this payload.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Payload::Random => "random",
            Payload::Zeros => "zeros",
        }
    }
}

/// One workload in the matrix.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    /// Stable identifier, also the trial directory name: `[a-z0-9-]+`.
    pub id: String,
    /// Bytes per record value.
    pub record_bytes: u32,
    /// How the values are filled.
    pub payload: Payload,
    /// The producer's `compression.type`.
    pub compression: String,
    /// Partitions of the cell's fresh topic.
    pub partitions: u32,
    /// Records every trial must produce and consume.
    pub records: u64,
    /// Requested records per second, or `None` to saturate.
    pub rate: Option<u64>,
    /// Seconds the subscribed consumer idles before the first record is sent.
    pub idle_seconds: u32,
    /// Seconds the workload may run before it fails.
    pub timeout_seconds: u32,
    /// The metrics a budget judges for this cell. Every metric is recorded.
    pub budgeted: Vec<Metric>,
}

/// The materiality and variance budget for one metric.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    /// A median change smaller than this percentage is never material.
    pub materiality_percent: f64,
    /// A median change must also exceed this many times the observed spread.
    pub spread_multiplier: f64,
    /// A side whose spread exceeds this percentage makes the cell inconclusive.
    pub max_spread_percent: f64,
}

/// What a mode gates on beyond correctness and variance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModePolicy {
    /// Whether a material regression rejects the comparison.
    pub gate_performance: bool,
}

/// The cluster shape every trial runs against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    /// Combined broker/controller nodes.
    pub brokers: u32,
    /// Replication factor of every cell's topic.
    pub replication_factor: u32,
    /// `min.insync.replicas` of every cell's topic.
    pub min_insync_replicas: u32,
}

/// The whole regression contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    /// Must be [`FORMAT_VERSION`].
    pub format_version: u32,
    /// The contract's name, which every record and report carries.
    pub name: String,
    /// Interleaved rounds; each round runs every cell once per side.
    pub rounds: usize,
    /// The cluster every trial runs against.
    pub cluster: Cluster,
    /// The pinned Kafka image a [`Mode::PinnedKafka`] comparison runs, and
    /// whose client jars drive both sides in every mode.
    pub kafka_image: String,
    /// The workload matrix.
    pub cells: Vec<Cell>,
    /// One budget per budgeted metric.
    pub budgets: BTreeMap<Metric, Budget>,
    /// What each mode gates on.
    pub modes: BTreeMap<Mode, ModePolicy>,
}

fn invalid(reason: impl Into<String>) -> BudgetError { BudgetError::InvalidContract(reason.into()) }

fn is_cell_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

impl Contract {
    /// Parse and validate a contract from its JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] when the bytes are not a contract or the
    /// contract fails [`Contract::validate`].
    pub fn from_json(bytes: &[u8]) -> Result<Self, BudgetError> {
        let contract: Contract = serde_json::from_slice(bytes)?;
        contract.validate()?;
        Ok(contract)
    }

    /// Check that the contract can judge a comparison.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::InvalidContract`] naming the first problem.
    pub fn validate(&self) -> Result<(), BudgetError> {
        if self.format_version != FORMAT_VERSION {
            return Err(BudgetError::FormatVersion {
                what: "contract",
                found: self.format_version,
            });
        }
        if self.rounds < MINIMUM_ROUNDS {
            return Err(invalid(format!(
                "rounds must be at least {MINIMUM_ROUNDS}, got {}",
                self.rounds
            )));
        }
        let cluster = &self.cluster;
        if cluster.replication_factor == 0
            || cluster.replication_factor > cluster.brokers
            || cluster.min_insync_replicas == 0
            || cluster.min_insync_replicas > cluster.replication_factor
        {
            return Err(invalid(
                "cluster needs 0 < min_insync_replicas <= replication_factor <= brokers",
            ));
        }
        if self.cells.is_empty() {
            return Err(invalid("the workload matrix has no cells"));
        }
        let mut ids = BTreeSet::new();
        for cell in &self.cells {
            if !is_cell_id(&cell.id) {
                return Err(invalid(format!("cell id {:?} is not [a-z0-9-]+", cell.id)));
            }
            if !ids.insert(cell.id.as_str()) {
                return Err(invalid(format!("cell id {} appears twice", cell.id)));
            }
            if cell.record_bytes < MINIMUM_RECORD_BYTES {
                return Err(invalid(format!(
                    "cell {} has records smaller than {MINIMUM_RECORD_BYTES} bytes",
                    cell.id
                )));
            }
            if cell.records == 0 || cell.partitions == 0 || cell.timeout_seconds == 0 {
                return Err(invalid(format!(
                    "cell {} needs positive records, partitions and timeout",
                    cell.id
                )));
            }
            if cell.rate == Some(0) {
                return Err(invalid(format!(
                    "cell {} has rate 0; omit it to saturate",
                    cell.id
                )));
            }
            if cell.budgeted.is_empty() {
                return Err(invalid(format!("cell {} budgets no metric", cell.id)));
            }
            for metric in &cell.budgeted {
                if !self.budgets.contains_key(metric) {
                    return Err(invalid(format!(
                        "cell {} budgets {metric:?}, which has no budget",
                        cell.id
                    )));
                }
            }
        }
        for (metric, budget) in &self.budgets {
            let values = [
                budget.materiality_percent,
                budget.spread_multiplier,
                budget.max_spread_percent,
            ];
            if values
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
            {
                return Err(invalid(format!(
                    "budget for {metric:?} must be finite and non-negative"
                )));
            }
        }
        for mode in [Mode::BeforeAfter, Mode::PinnedKafka] {
            if !self.modes.contains_key(&mode) {
                return Err(invalid(format!("no policy for mode {}", mode.name())));
            }
        }
        Ok(())
    }

    /// The cell with this id.
    #[must_use]
    pub fn cell(&self, id: &str) -> Option<&Cell> { self.cells.iter().find(|cell| cell.id == id) }
}

/// The lowercase hex SHA-256 of a contract's bytes, as `sha256sum` prints it.
#[must_use]
pub fn contract_digest(bytes: &[u8]) -> String { hex::encode(Sha256::digest(bytes)) }
