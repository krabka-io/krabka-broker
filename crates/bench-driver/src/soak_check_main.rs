//! `krabka-soak-check` validates a sustained-load contract, prints the fault
//! schedule it expands to, and judges a run's evidence against it.
//!
//! ```text
//! krabka-soak-check plan --contract bench/soak/rf3-72h.yaml
//! krabka-soak-check judge --contract bench/soak/rf3-72h.yaml --evidence run/evidence.json
//! krabka-soak-check self-check --contract bench/soak/self-check.yaml
//! ```
//!
//! `self-check` without `--evidence` judges a synthetic run with one record
//! removed. That needs no cluster, and it proves the reconciler and the
//! deadline checker each catch their injection. With `--evidence` it judges
//! the evidence of a live self-check run instead.

use {
    anyhow::{Context, Result, bail},
    clap::{Parser, Subcommand},
    krabka_bench_driver::soak::{
        contract::Contract,
        evidence::Evidence,
        schedule::expand,
        synthetic::{clean_run, inject_missing_record},
        verdict::{judge, judge_self_check},
    },
    std::path::{Path, PathBuf},
};

#[derive(Debug, Parser)]
#[command(name = "krabka-soak-check", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate a contract and print the fault schedule it expands to, as JSON.
    Plan {
        #[arg(long)]
        contract: PathBuf,
    },
    /// Judge a run's evidence against its contract. Exits non-zero on any
    /// failure.
    Judge {
        #[arg(long)]
        contract: PathBuf,
        #[arg(long)]
        evidence: PathBuf,
        /// Also write the whole verdict here, as JSON.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Prove the judge catches an injected missing record and an injected
    /// missed deadline. Exits non-zero unless both, and nothing else, are
    /// reported.
    SelfCheck {
        #[arg(long)]
        contract: PathBuf,
        /// A live self-check run's evidence. Without it, a synthetic run of the
        /// contract with one record removed is judged.
        #[arg(long)]
        evidence: Option<PathBuf>,
        /// Also write the whole report here, as JSON.
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

fn load_contract(path: &Path) -> Result<Contract> { Contract::from_file(path) }

fn load_evidence(path: &Path) -> Result<Evidence> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse evidence {}", path.display()))
}

fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    std::fs::write(path, json).with_context(|| format!("write {}", path.display()))
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Plan { contract } => {
            let contract = load_contract(&contract)?;
            let schedule = expand(&contract).map_err(|errors| {
                anyhow::anyhow!(
                    "{}",
                    errors
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            })?;
            println!("{}", serde_json::to_string_pretty(&schedule)?);
            eprintln!("{}: {} faults scheduled", contract.name, schedule.len());
        }
        Command::Judge {
            contract,
            evidence,
            out,
        } => {
            let contract = load_contract(&contract)?;
            let verdict = judge(&contract, &load_evidence(&evidence)?);
            if let Some(out) = out {
                write_json(&out, &verdict)?;
            }
            let failures = verdict.failures();
            for failure in &failures {
                eprintln!("{failure}");
            }
            if !failures.is_empty() {
                bail!("{}: {} failure(s)", contract.name, failures.len());
            }
            println!("{}: PASS", contract.name);
        }
        Command::SelfCheck {
            contract,
            evidence,
            out,
        } => {
            let contract = load_contract(&contract)?;
            let evidence = if let Some(path) = evidence {
                load_evidence(&path)?
            } else {
                let mut synthetic = clean_run(&contract);
                inject_missing_record(&contract, &mut synthetic);
                synthetic
            };
            let report = judge_self_check(&contract, &evidence);
            if let Some(out) = out {
                write_json(&out, &report)?;
            }
            for line in &report.detected {
                println!("detected: {line}");
            }
            for problem in &report.problems {
                eprintln!("problem: {problem}");
            }
            if !report.passed {
                bail!("{}: self-check FAILED", contract.name);
            }
            println!("{}: self-check PASS", contract.name);
        }
    }
    Ok(())
}
