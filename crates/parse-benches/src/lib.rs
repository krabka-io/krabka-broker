//! Parses Criterion benchmark output in bencher format into structured JSON
//! summaries.
//!
//! Note: If a shared `krabka-tools` repository is established across the
//! `krabka-io` organization, this crate can be migrated there as a common
//! benchmark utility for `krabka-broker`, `krabka-protocol`, and `krabka-client-rs`.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::LazyLock,
    time::SystemTime,
};

use clap::Parser;
use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

static BENCHER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"test\s+([\w\/\-\.]+)\s+\.\.\.\s+bench:\s+([\d,]+)\s+ns\/iter\s+\(\+\/-\s+([\d,]+)\)",
    )
    .expect("static regex pattern is valid")
});

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

    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Underlying JSON serialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
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
}

/// CLI configuration arguments for parsing benchmarks.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "krabka-parse-benches",
    about = "Parses Criterion benchmark output in bencher format into a structured JSON summary"
)]
pub struct Args {
    /// Directory containing benchmark `.txt` output files.
    #[arg(long, default_value = "bench-results")]
    pub results_dir: PathBuf,

    /// Output JSON summary file path (defaults to `<results_dir>/broker-benchmarks.json`).
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Suite identifier for the benchmark summary.
    #[arg(long, default_value = "krabka-broker")]
    pub suite: String,

    /// Git commit SHA (defaults to `GITHUB_SHA` env var or `"unknown"`).
    #[arg(long)]
    pub commit: Option<String>,
}

/// Parses a single line of Criterion bencher output.
///
/// Matches patterns of the shape:
/// `test log/append/100rec_1024B ... bench: 95,979 ns/iter (+/- 1,377,729)`
///
/// # Errors
///
/// Returns [`ParseBenchesError::InvalidNumber`] if matched numeric fields cannot
/// be parsed into floats.
pub fn parse_bencher_line(
    line: &str,
) -> Result<Option<(String, BenchmarkMetric)>, ParseBenchesError> {
    let Some(caps) = BENCHER_REGEX.captures(line) else {
        return Ok(None);
    };

    let name = caps[1].to_string();
    let ns_raw = caps[2].replace(',', "");
    let variance_raw = caps[3].replace(',', "");

    let ns_per_iter: f64 = ns_raw
        .parse()
        .map_err(|_| ParseBenchesError::InvalidNumber(caps[2].to_string()))?;
    let variance_ns: f64 = variance_raw
        .parse()
        .map_err(|_| ParseBenchesError::InvalidNumber(caps[3].to_string()))?;

    Ok(Some((
        name,
        BenchmarkMetric {
            ns_per_iter,
            variance_ns,
        },
    )))
}

/// Parses all benchmark metrics from a `.txt` file into the target map.
///
/// # Errors
///
/// Returns an error if the file cannot be read, numeric parsing fails, or a
/// duplicate benchmark name is encountered.
pub fn parse_bencher_file(
    path: &Path,
    benchmarks: &mut BTreeMap<String, BenchmarkMetric>,
) -> Result<(), ParseBenchesError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        if let Some((name, metric)) = parse_bencher_line(&line)?
            && benchmarks.insert(name.clone(), metric).is_some()
        {
            return Err(ParseBenchesError::DuplicateBenchmark {
                name,
                file: path.to_path_buf(),
            });
        }
    }

    Ok(())
}

/// Scans the given directory for all `*.txt` files and parses their benchmark metrics.
///
/// # Errors
///
/// Returns an error if:
/// - The directory does not exist ([`ParseBenchesError::DirectoryNotFound`]).
/// - No `*.txt` files are found ([`ParseBenchesError::NoTxtFiles`]).
/// - No metrics were parsed from any found files ([`ParseBenchesError::NoMetricsParsed`]).
/// - Duplicate benchmark metrics are detected across files.
pub fn parse_benchmark_dir(
    results_dir: &Path,
) -> Result<BTreeMap<String, BenchmarkMetric>, ParseBenchesError> {
    if !results_dir.exists() || !results_dir.is_dir() {
        return Err(ParseBenchesError::DirectoryNotFound(
            results_dir.to_path_buf(),
        ));
    }

    let mut txt_files = Vec::new();
    for entry in fs::read_dir(results_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("txt") {
            txt_files.push(path);
        }
    }

    if txt_files.is_empty() {
        return Err(ParseBenchesError::NoTxtFiles(results_dir.to_path_buf()));
    }

    txt_files.sort();

    let mut benchmarks = BTreeMap::new();
    for file_path in &txt_files {
        parse_bencher_file(file_path, &mut benchmarks)?;
    }

    if benchmarks.is_empty() {
        return Err(ParseBenchesError::NoMetricsParsed(
            results_dir.to_path_buf(),
        ));
    }

    Ok(benchmarks)
}

/// Formats a [`SystemTime`] as an ISO8601 / RFC3339 UTC string (`YYYY-MM-DDTHH:MM:SSZ`).
#[must_use]
pub fn format_rfc3339_utc(time: SystemTime) -> String {
    let offset_time = time::OffsetDateTime::from(time)
        .replace_nanosecond(0)
        .unwrap_or_else(|_| time::OffsetDateTime::from(time));
    offset_time
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
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
            env_sha.and_then(|s| {
                let t = s.trim();
                (!t.is_empty()).then_some(t)
            })
        });

    let Some(trimmed) = raw else {
        return "unknown".to_string();
    };

    trimmed.chars().take(8).collect()
}

/// Generates a [`BenchmarkSummary`] from the provided command-line arguments.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] if directory scanning or metric parsing fails.
pub fn generate_summary(args: &Args) -> Result<BenchmarkSummary, ParseBenchesError> {
    let benchmarks = parse_benchmark_dir(&args.results_dir)?;
    let env_sha = std::env::var("GITHUB_SHA").ok();
    let commit = resolve_commit_sha(args.commit.as_deref(), env_sha.as_deref());
    let timestamp = format_rfc3339_utc(SystemTime::now());

    Ok(BenchmarkSummary {
        suite: args.suite.clone(),
        commit,
        timestamp,
        benchmarks,
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
