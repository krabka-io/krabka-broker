//! The command line: its arguments, the broker performance lane's
//! subcommands, and what running one reports.

use {
    crate::{
        ParseBenchesError,
        budget::evaluate,
        contract::{BudgetError, Contract, contract_digest},
        executables::manifest_from_cargo_messages,
        record::{PerformanceRecord, assemble},
        render::render_markdown,
    },
    clap::{Parser, Subcommand},
    serde::Serialize,
    std::{
        fs,
        path::{Path, PathBuf},
    },
};

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

    /// Directory of repeated reference summary JSON files to compare.
    #[arg(long, requires = "candidate_dir")]
    pub reference_dir: Option<PathBuf>,

    /// Directory of repeated candidate summary JSON files to compare.
    #[arg(long, requires = "reference_dir")]
    pub candidate_dir: Option<PathBuf>,

    /// The executable manifest this pass's side was prebuilt into. With it,
    /// the summary records which executables the pass ran, and parsing fails
    /// unless they are the manifest's, unchanged.
    #[arg(long, requires = "cargo_log")]
    pub executables: Option<PathBuf>,

    /// Cargo's standard error from this pass, whose `Running` lines name the
    /// executables it ran.
    #[arg(long, requires = "executables")]
    pub cargo_log: Option<PathBuf>,

    /// The directory Cargo ran in, against which relative `Running` paths
    /// resolve (defaults to the current directory).
    #[arg(long, requires = "executables")]
    pub source_dir: Option<PathBuf>,

    /// Minimum allowed slowdown percentage for comparison mode.
    #[arg(long, default_value_t = 3.0)]
    pub minimum_tolerance_percent: f64,

    /// The broker performance lane; without one, the criterion lane runs.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// The commands besides parsing and comparing criterion output.
#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Record the bench executables `cargo bench --no-run
    /// --message-format=json` built, with their digests.
    Executables {
        /// Cargo's JSON messages.
        cargo_messages: PathBuf,
        /// The commit the executables were built from.
        #[arg(long)]
        commit: String,
        /// Where to write the manifest JSON.
        #[arg(long)]
        output: PathBuf,
    },
    /// Validate a regression contract and print its digest.
    CheckContract {
        /// The contract JSON.
        contract: PathBuf,
    },
    /// Assemble a `qualify.sh budget` artifact directory into a record.
    Assemble {
        /// The artifact directory.
        artifact_dir: PathBuf,
        /// Where to write the record JSON.
        #[arg(long)]
        output: PathBuf,
    },
    /// Judge a record against its contract; a rejection exits 1.
    Evaluate {
        /// The record JSON.
        record: PathBuf,
        /// The contract the record was measured under.
        #[arg(long)]
        contract: PathBuf,
        /// Where to write the verdict JSON.
        #[arg(long)]
        output: PathBuf,
        /// Where to write the published Markdown.
        #[arg(long)]
        markdown: Option<PathBuf>,
    },
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), BudgetError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    fs::write(path, bytes)?;
    Ok(())
}

/// Whether a subcommand that ran found what it checks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The command did its work and found nothing to reject.
    Passed,
    /// The command ran, and the comparison it judges was rejected.
    Rejected,
}

/// What one subcommand found, and the line that says so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandReport {
    /// Whether the command passed or rejected what it judged.
    pub outcome: Outcome,
    /// The line to print: on standard output for a pass, on standard error
    /// for a rejection.
    pub message: String,
}

impl CommandReport {
    fn passed(message: String) -> Self {
        Self {
            outcome: Outcome::Passed,
            message,
        }
    }
}

/// Run one subcommand and say what it found.
///
/// An [`Outcome::Rejected`] report means the command ran and the comparison
/// was rejected; the verdict and Markdown are written either way, so a
/// rejection is published with its evidence.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] when an input cannot be read, parsed or
/// judged.
pub fn run_command(command: &Command) -> Result<CommandReport, ParseBenchesError> {
    match command {
        Command::Executables {
            cargo_messages,
            commit,
            output,
        } => {
            let manifest =
                manifest_from_cargo_messages(&fs::read_to_string(cargo_messages)?, commit)?;
            write_json(output, &manifest)?;
            Ok(CommandReport::passed(format!(
                "{} bench executables of {commit} recorded in {}",
                manifest.executables.len(),
                output.display()
            )))
        }
        Command::CheckContract { contract } => {
            let bytes = fs::read(contract)?;
            let parsed = Contract::from_json(&bytes)?;
            Ok(CommandReport::passed(format!(
                "{}: {} cells, {} rounds, sha256 {}",
                parsed.name,
                parsed.cells.len(),
                parsed.rounds,
                contract_digest(&bytes)
            )))
        }
        Command::Assemble {
            artifact_dir,
            output,
        } => {
            let record = assemble(artifact_dir)?;
            write_json(output, &record)?;
            Ok(CommandReport::passed(format!(
                "{} trials written to {}",
                record.trials.len(),
                output.display()
            )))
        }
        Command::Evaluate {
            record,
            contract,
            output,
            markdown,
        } => {
            let contract_bytes = fs::read(contract)?;
            let parsed = Contract::from_json(&contract_bytes)?;
            let record = PerformanceRecord::from_json(&fs::read(record)?)?;
            let verdict = evaluate(&parsed, &contract_bytes, &record)?;
            write_json(output, &verdict)?;
            if let Some(markdown) = markdown {
                if let Some(parent) = markdown.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(markdown, render_markdown(&parsed, &record, &verdict))?;
            }
            let summary = if verdict.passed {
                format!("performance budget passed: {}", output.display())
            } else {
                format!(
                    "performance budget rejected ({} reasons): {}",
                    verdict.rejections.len(),
                    output.display()
                )
            };
            Ok(CommandReport {
                outcome: if verdict.passed {
                    Outcome::Passed
                } else {
                    Outcome::Rejected
                },
                message: summary,
            })
        }
    }
}
