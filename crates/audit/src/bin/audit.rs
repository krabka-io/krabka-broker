//! `krabka-audit`: offline audit-log tools.

use {
    clap::{Parser, Subcommand},
    krabka_audit::{TrustedKeys, verify_partition_dir},
    std::{fs, path::PathBuf, process::ExitCode},
    tracing::{field, field::Empty},
};

#[derive(Parser)]
#[command(name = "krabka-audit", about = "Krabka audit-log tools (FedRAMP MLA)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Verify a partition's hash-chain and signed checkpoints offline.
    Verify {
        /// Path to the audit partition directory, for example `<log_dir>/__krabka_audit-0`
        #[arg(long)]
        partition_dir: PathBuf,
        /// A `key_id` to trust. Repeat it once per key, for example the
        /// keys before and after a rotation; each pairs with the
        /// `--public-key` at the same position.
        #[arg(long, required = true)]
        key_id: Vec<String>,
        /// Path to a trusted Ed25519 public key (raw 32 bytes). Repeat it
        /// once per `--key-id`, in the same order.
        #[arg(long, required = true)]
        public_key: Vec<PathBuf>,
    },
}

/// Read every public key file and pair it with the key id at its position.
fn trusted_keys(key_ids: Vec<String>, public_keys: &[PathBuf]) -> Result<TrustedKeys, String> {
    let keys = public_keys
        .iter()
        .map(|path| fs::read(path).map_err(|e| format!("read public key {}: {e}", path.display())))
        .collect::<Result<Vec<_>, _>>()?;
    TrustedKeys::paired(key_ids, keys)
        .map_err(|e| format!("{e} (pass one --public-key for every --key-id, in the same order)"))
}

#[tracing::instrument(
    level = "info",
    skip_all,
    fields(command = Empty, partition_dir = Empty, key_id = Empty)
)]
fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Verify {
            partition_dir,
            key_id,
            public_key,
        } => {
            let span = tracing::Span::current();
            span.record("command", "verify");
            span.record("partition_dir", field::display(partition_dir.display()));
            span.record("key_id", field::display(key_id.join(",")));
            let trusted = match trusted_keys(key_id, &public_key) {
                Ok(trusted) => trusted,
                Err(e) => {
                    eprintln!("error: {e}");
                    return ExitCode::FAILURE;
                }
            };
            match verify_partition_dir(&partition_dir, &trusted) {
                Ok(report) if !report.ok => {
                    match &report.first_break {
                        Some(b) => eprintln!(
                            "TAMPER DETECTED at offset {} (seq {:?}): {}",
                            b.offset, b.seq, b.reason
                        ),
                        None => eprintln!("TAMPER DETECTED (the report names no break)"),
                    }
                    eprintln!(
                        "verified {} records, {} checkpoints before the break",
                        report.records, report.checkpoints
                    );
                    ExitCode::FAILURE
                }
                Ok(report) if !report.losses.is_empty() => {
                    for loss in &report.losses {
                        eprintln!(
                            "RECORDS LOST at offset {} (seq {}): {} record(s)",
                            loss.offset, loss.seq, loss.records
                        );
                    }
                    ExitCode::FAILURE
                }
                Ok(report) if report.records == 0 => {
                    println!("OK: empty partition");
                    ExitCode::SUCCESS
                }
                Ok(report) if report.checkpoints == 0 || report.unanchored_records > 0 => {
                    eprintln!(
                        "INCOMPLETE ATTESTATION: chain continuous over {} records, but {} \
                        record(s) are not covered by a signed checkpoint ({} checkpoint(s) \
                        present). Integrity is not cryptographically attested for the unsigned \
                        portion.",
                        report.records, report.unanchored_records, report.checkpoints
                    );
                    ExitCode::FAILURE
                }
                Ok(report) => {
                    println!(
                        "OK: {} records, {} checkpoints, chain continuous, all signatures valid, \
                        fully attested",
                        report.records, report.checkpoints
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}
