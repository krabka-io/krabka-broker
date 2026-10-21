//! `krabka-soak-drill` executes a disaster-recovery contract inside the
//! qualification drill: the soak's load against the secured source cluster
//! until it is lost, and the soak's read-back of the cluster restored from the
//! capture.
//!
//! ```text
//! krabka-soak-drill objectives --contract bench/soak/dr-secured.yaml
//! krabka-soak-drill load --contract bench/soak/dr-secured.yaml \
//!   --bootstrap broker-1:9094 --tls-ca ca.pem --tls-server-name broker-1 \
//!   --username soak --out drill/load.json
//! krabka-soak-drill probe --bootstrap restored-1:9094 ... --topic tiered-idempotent
//! krabka-soak-drill readback --contract bench/soak/dr-secured.yaml \
//!   --load drill/load.json --bootstrap restored-1:9094 ... \
//!   --capture-started-ms 1792022400000 --disaster-ms 1792022430000 \
//!   --serving-ms 1792022500000 --out drill/evidence.json
//! krabka-soak-check judge --contract bench/soak/dr-secured.yaml --evidence drill/evidence.json
//! ```
//!
//! `load` runs until it receives `SIGTERM`, which the drill sends when it
//! loses the source cluster, or until the contract's `duration`. On `SIGTERM`
//! it abandons every send in flight, because nothing can answer it any more,
//! and writes what it recorded. The password is read from
//! `KRABKA_DRILL_PASSWORD`, so it is never on a command line.

use {
    anyhow::{Context, Result},
    clap::Parser,
    krabka_bench_driver::soak::drill::cli::{Cli, run},
    tokio::signal::unix::{SignalKind, signal},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cli = Cli::parse();
    let mut terminate = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    let lost = async move {
        terminate.recv().await;
    };
    let output = run(cli, lost).await?;
    if !output.is_empty() {
        println!("{output}");
    }
    Ok(())
}
