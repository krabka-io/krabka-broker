//! `krabka-parse-benches` — parse Criterion benchmark outputs into a JSON summary.

use std::process::ExitCode;

use clap::Parser;
use krabka_parse_benches::{Args, run_from_args};

fn main() -> ExitCode {
    let args = Args::parse();
    let out_path = args
        .output
        .clone()
        .unwrap_or_else(|| args.results_dir.join("broker-benchmarks.json"));

    match run_from_args(&args) {
        Ok(summary) => {
            println!(
                "Successfully parsed {} benchmark metrics to {}",
                summary.benchmarks.len(),
                out_path.display()
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::FAILURE
        }
    }
}
