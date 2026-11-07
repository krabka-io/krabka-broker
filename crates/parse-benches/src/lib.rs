//! Parses Criterion benchmark output in bencher format into structured JSON
//! summaries, and judges repeated reference/candidate measurements against
//! variance-calibrated budgets.
//!
//! Two lanes share one set of statistics. The criterion lane compares
//! [`BenchmarkSummary`] files with [`compare_summaries`]. The broker
//! performance lane judges a [`record::PerformanceRecord`] against a
//! [`contract::Contract`] with [`budget::evaluate`]. Both take medians over
//! repeated runs and allow the larger of a fixed floor and a multiple of the
//! relative median absolute deviation: [`calibrated_tolerance`].
//!
//! Each criterion summary also names the bench executables its pass ran, as
//! [`executables`] verified them, so a verdict says which binaries produced
//! its numbers.
//!
//! Note: If a shared `krabka-tools` repository is established across the
//! `krabka-io` organization, this crate can be migrated there as a common
//! benchmark utility for `krabka-broker`, `krabka-protocol`, and `krabka-client-rs`.

mod bencher;
pub mod budget;
mod cli;
mod compare;
pub mod contract;
pub mod executables;
mod num;
pub mod record;
pub mod render;
mod stats;

pub use {
    bencher::{NamedMetric, parse_bencher_file, parse_bencher_line, parse_benchmark_dir},
    cli::{Args, Command, CommandReport, Outcome, run_command},
    compare::{BenchmarkComparison, BenchmarkVerdict, compare_summaries, read_summaries},
    stats::{calibrated_tolerance, median, relative_mad},
};
use {
    contract::BudgetError,
    executables::{BenchExecutable, ExecutableManifest, verify_pass},
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        env,
        fs::{self, File},
        io::{self, Write},
        path::PathBuf,
        time::SystemTime,
    },
    thiserror::Error,
    time::format_description::well_known::Rfc3339,
};

/// Errors encountered while parsing benchmark output.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ParseBenchesError {
    /// Benchmark results directory does not exist.
    #[error("directory '{0}' does not exist")]
    DirectoryNotFound(PathBuf),

    /// No `.txt` output files were found in the results directory.
    #[error("no benchmark output files (*.txt) found in '{0}'")]
    NoTxtFiles(PathBuf),

    /// Output files were found, but no benchmark metrics could be parsed.
    #[error("parsed 0 benchmark metrics from '{0}'")]
    NoMetricsParsed(PathBuf),

    /// A duplicate benchmark identifier was found.
    #[error("duplicate benchmark metric '{name}' found in '{file}'")]
    DuplicateBenchmark {
        /// Name of the duplicated benchmark identifier.
        name: String,
        /// Path of the file containing the collision.
        file: PathBuf,
    },

    /// Failed to parse a numeric value from benchmark output.
    #[error("failed to parse numeric benchmark value: {0}")]
    InvalidNumber(String),

    /// The bencher line pattern did not compile.
    #[error("bencher line pattern: {0}")]
    Pattern(#[source] regex::Error),

    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Underlying JSON serialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Fewer than three repeated summaries were supplied for one side.
    #[error("{side} needs at least 3 repeated benchmark summaries, got {count}")]
    TooFewSamples {
        /// The comparison side.
        side: &'static str,
        /// Number of supplied summaries.
        count: usize,
    },

    /// Reference and candidate summaries did not contain the same benchmarks.
    #[error("reference and candidate benchmark sets differ")]
    BenchmarkSetMismatch,

    /// A sample was non-finite or non-positive without every sample being zero.
    #[error("invalid sample for benchmark '{0}'")]
    InvalidSample(String),

    /// A supplied summary had no benchmark metrics.
    #[error("benchmark summaries contain no metrics")]
    EmptyBenchmarkSet,

    /// The configured tolerance was negative, NaN, or infinite.
    #[error("minimum tolerance must be a finite non-negative ratio")]
    InvalidTolerance,

    /// Cargo's JSON messages named no bench executable.
    #[error("cargo built no bench executable")]
    NoBenchExecutables,

    /// A pass's Cargo log named no executable that it ran.
    #[error("the cargo log names no bench executable that ran")]
    NoExecutableRan,

    /// An executable manifest was built from a different commit than the pass.
    #[error("executables were built from {manifest} but the pass is labelled {pass}")]
    CommitMismatch {
        /// The commit the manifest records.
        manifest: String,
        /// The commit the pass was labelled with.
        pass: String,
    },

    /// A pass ran an executable that its side's manifest does not list.
    #[error("pass ran '{0}', which its side did not prebuild")]
    UnrecordedExecutable(PathBuf),

    /// A prebuilt executable's bytes changed after the manifest recorded them.
    #[error("executable '{0}' changed after it was prebuilt")]
    ExecutableChanged(PathBuf),

    /// A side's summaries name no executable.
    #[error("{0} summaries do not record which executables ran")]
    MissingExecutableIdentity(&'static str),

    /// A side's passes ran different executables.
    #[error("{0} passes ran different executables")]
    ExecutableDrift(&'static str),

    /// Reference and candidate ran the same executable file.
    #[error(
        "reference and candidate both ran '{0}'; they must build into separate target directories"
    )]
    SharedExecutable(PathBuf),

    /// A broker performance lane command failed.
    #[error(transparent)]
    Budget(#[from] BudgetError),
}

/// A single benchmark measurement in nanoseconds per iteration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkMetric {
    /// Mean nanoseconds per iteration.
    pub ns_per_iter: f64,
    /// Variance in nanoseconds per iteration (+/- bound).
    pub variance_ns: f64,
}

/// Aggregated benchmark summary document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    /// Benchmark suite name (e.g. `krabka-broker`).
    pub suite: String,
    /// Git commit SHA of the run.
    pub commit: String,
    /// ISO8601/RFC3339 UTC timestamp when the summary was generated.
    pub timestamp: String,
    /// Map of canonical benchmark names to metrics, sorted alphabetically.
    pub benchmarks: BTreeMap<String, BenchmarkMetric>,
    /// The bench executables this pass ran; empty when none were recorded.
    #[serde(default)]
    pub executables: Vec<BenchExecutable>,
}

/// Formats a [`SystemTime`] as an ISO8601 / RFC3339 UTC string (`YYYY-MM-DDTHH:MM:SSZ`).
#[must_use]
pub fn format_rfc3339_utc(time: SystemTime) -> String {
    let offset_time = time::OffsetDateTime::from(time)
        .replace_nanosecond(0)
        .unwrap_or_else(|_| time::OffsetDateTime::from(time));
    offset_time
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

/// Resolves commit SHA from an optional CLI argument, falling back to an optional environment SHA.
#[must_use]
pub fn resolve_commit_sha(commit_arg: Option<&str>, env_sha: Option<&str>) -> String {
    let raw = commit_arg
        .and_then(|s| {
            let t = s.trim();
            (!t.is_empty()).then_some(t)
        })
        .or_else(|| {
            let s = env_sha?;
            let t = s.trim();
            (!t.is_empty()).then_some(t)
        });

    let Some(trimmed) = raw else {
        return "unknown".to_owned();
    };

    trimmed.to_owned()
}

/// Generates a [`BenchmarkSummary`] from the provided command-line arguments.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] if directory scanning or metric parsing fails.
pub fn generate_summary(args: &Args) -> Result<BenchmarkSummary, ParseBenchesError> {
    let benchmarks = parse_benchmark_dir(&args.results_dir)?;
    let env_sha = env::var("GITHUB_SHA").ok();
    let commit = resolve_commit_sha(args.commit.as_deref(), env_sha.as_deref());
    let timestamp = format_rfc3339_utc(SystemTime::now());
    let executables = match (&args.executables, &args.cargo_log) {
        (Some(manifest), Some(cargo_log)) => {
            let manifest: ExecutableManifest = serde_json::from_reader(File::open(manifest)?)?;
            let source_dir = args.source_dir.clone().unwrap_or_default();
            verify_pass(
                &manifest,
                &fs::read_to_string(cargo_log)?,
                &source_dir,
                &commit,
            )?
        }
        _ => Vec::new(),
    };

    Ok(BenchmarkSummary {
        suite: args.suite.clone(),
        commit,
        timestamp,
        benchmarks,
        executables,
    })
}

/// Executes benchmark parsing and writes the resulting JSON summary to disk.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] if parsing, directory creation, or file write fails.
pub fn run_from_args(args: &Args) -> Result<BenchmarkSummary, ParseBenchesError> {
    let summary = generate_summary(args)?;

    let out_path = args
        .output
        .clone()
        .unwrap_or_else(|| args.results_dir.join("broker-benchmarks.json"));

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let json_bytes = serde_json::to_vec_pretty(&summary)?;
    let mut file = File::create(&out_path)?;
    file.write_all(&json_bytes)?;
    file.write_all(b"\n")?;

    Ok(summary)
}
