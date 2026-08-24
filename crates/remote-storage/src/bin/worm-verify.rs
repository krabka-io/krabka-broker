//! `crabka-worm-verify` — verify a WORM archive with read-only credentials.
//!
//! The auditor runs this against a bucket or a directory with no broker and no
//! cluster: everything it checks is written into the archive. It reads the
//! per-segment manifests, recomputes the partition hash chain, checks every
//! signature against a key the auditor supplies, and confirms that every object
//! a manifest names is present with the recorded size. `--deep` additionally
//! re-hashes every object body.
//!
//! Credentials come from the ambient AWS chain. There is deliberately no
//! `--access-key` flag: an auditor should hold a read-only role, not a copy of
//! the writer's keys.

use std::{fmt::Write as _, path::PathBuf, process::ExitCode, sync::Arc};

use clap::{Args, Parser, Subcommand};
use crabka_audit::chain::from_hex32;
use crabka_object_store::{ObjectStoreConfig, S3Config, build_object_store};
use crabka_remote_storage::{
    ArchiveVerifyReport, ChainHead, PartitionVerifyReport, TrustedManifestKeys, VerifyDepth,
    VerifyRequest, verify_archive,
};

/// Orphan keys listed before the output is truncated.
const ORPHANS_SHOWN: usize = 10;

#[derive(Parser)]
#[command(
    name = "crabka-worm-verify",
    about = "Verify a Crabka WORM archive with read-only credentials"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Verify an archive's manifest chain, signatures, and objects.
    Verify(VerifyArgs),
}

#[derive(Args)]
struct VerifyArgs {
    /// Directory holding the archive, for a local or mounted copy.
    #[arg(long, value_name = "PATH", conflicts_with = "bucket")]
    local_dir: Option<PathBuf>,
    /// S3-compatible bucket holding the archive. Credentials come from the
    /// ambient AWS chain, so run this under a read-only role.
    #[arg(long, value_name = "NAME", required_unless_present = "local_dir")]
    bucket: Option<String>,
    /// AWS region. Any value serves for an S3-compatible endpoint that
    /// ignores it, and `us-east-1` is the usual placeholder.
    #[arg(long, value_name = "REGION", default_value = "us-east-1")]
    region: String,
    /// Custom S3 endpoint, for example `http://minio:9000`.
    #[arg(long, value_name = "URL")]
    endpoint: Option<String>,
    /// Allow plaintext HTTP, for an S3-compatible endpoint served without TLS.
    #[arg(long)]
    allow_http: bool,
    /// Key prefix inside the bucket or directory. Verifies everything when
    /// absent.
    #[arg(long, value_name = "PREFIX")]
    prefix: Option<String>,
    /// Verify only the partitions of this topic.
    #[arg(long, value_name = "TOPIC")]
    topic: Option<String>,
    /// Verify only this partition index.
    #[arg(long, value_name = "INDEX")]
    partition: Option<i32>,
    /// The key id that `--public-key` holds the key for.
    #[arg(long, value_name = "ID", requires = "public_key")]
    key_id: Option<String>,
    /// Path to the trusted Ed25519 public key, raw 32 bytes.
    #[arg(long, value_name = "PATH", requires = "key_id")]
    public_key: Option<PathBuf>,
    /// Chain head the newest manifest must produce, obtained outside the
    /// archive. Without it, tail truncation is undetectable: removing the
    /// newest manifests leaves a shorter chain that verifies perfectly. A
    /// successful run prints each partition's tip, so use that value here on
    /// the next run.
    #[arg(long, value_name = "HEX", value_parser = parse_head)]
    expect_head: Option<ChainHead>,
    /// Download every object and recompute its SHA-256. The only check that
    /// catches a body replaced with different bytes of the same length.
    #[arg(long)]
    deep: bool,
    /// Accept a chain restart instead of grading it as an attestation hole.
    #[arg(long)]
    allow_epoch_restarts: bool,
}

/// Parses a chain head written as 64 hex characters.
fn parse_head(text: &str) -> Result<ChainHead, String> {
    from_hex32(text)
        .map(ChainHead)
        .ok_or_else(|| format!("expected 64 hex characters, got `{text}`"))
}

#[tokio::main]
async fn main() -> ExitCode {
    // The subscriber writes to stderr so the graded verdict on stdout stays
    // clean for a script that reads it.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Verify(args) => run_verify(args).await,
    }
}

/// Builds the store and the trusted key set, verifies, and grades the report.
async fn run_verify(args: VerifyArgs) -> ExitCode {
    let trusted = match trusted_keys(&args) {
        Ok(trusted) => trusted,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::FAILURE;
        }
    };
    let store = match open_store(&args) {
        Ok(store) => store,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::FAILURE;
        }
    };
    let request = VerifyRequest {
        prefix: args.prefix.clone(),
        topic: args.topic.clone(),
        partition: args.partition,
        depth: if args.deep {
            VerifyDepth::Deep
        } else {
            VerifyDepth::Shallow
        },
        // The tip comparison happens below, so a tip mismatch grades as its own
        // outcome and is never reported as tampering.
        expect_head: None,
        allow_epoch_restarts: args.allow_epoch_restarts,
    };
    match verify_archive(&store, &request, &trusted).await {
        Ok(report) => grade(&report, &args),
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Reads the trusted public key the run was given, if it was given one.
fn trusted_keys(args: &VerifyArgs) -> Result<TrustedManifestKeys, String> {
    let (Some(key_id), Some(path)) = (args.key_id.as_ref(), args.public_key.as_ref()) else {
        return Ok(TrustedManifestKeys::default());
    };
    let public_key =
        std::fs::read(path).map_err(|e| format!("read public key {}: {e}", path.display()))?;
    Ok(TrustedManifestKeys::single(key_id.clone(), public_key))
}

/// Opens the archive named by `--local-dir` or by `--bucket`.
fn open_store(args: &VerifyArgs) -> Result<Arc<dyn object_store::ObjectStore>, String> {
    let config = match (args.local_dir.as_ref(), args.bucket.as_ref()) {
        (Some(root), _) => ObjectStoreConfig::Local { root: root.clone() },
        (None, Some(bucket)) => ObjectStoreConfig::S3(S3Config {
            bucket: bucket.clone(),
            region: args.region.clone(),
            endpoint: args.endpoint.clone(),
            allow_http: args.allow_http,
            ..Default::default()
        }),
        // clap enforces that one of the two is present.
        (None, None) => return Err("give either --local-dir or --bucket".to_string()),
    };
    build_object_store(&config).map_err(|e| e.to_string())
}

/// Grades the report, prints the verdict, and returns the exit code.
fn grade(report: &ArchiveVerifyReport, args: &VerifyArgs) -> ExitCode {
    if let Some(found) = report.first_break() {
        let seq = found
            .seq
            .map_or_else(|| "unknown".to_string(), |seq| seq.to_string());
        eprintln!(
            "TAMPER DETECTED at {} (seq {seq}): {}",
            found.manifest_key, found.reason
        );
        for line in summary(report) {
            eprintln!("{line}");
        }
        return ExitCode::FAILURE;
    }

    if let Some(expected) = args.expect_head {
        // An archive with nothing left in it is the extreme of the same
        // attack, so an empty report is a mismatch and not a pass.
        let mismatch = tip_mismatch(report, expected);
        if mismatch.is_some() || report.partitions.is_empty() {
            let (name, tip) = mismatch.map_or_else(
                || ("(none found)".to_string(), "none".to_string()),
                |partition| {
                    (
                        partition.partition_dir.clone(),
                        partition
                            .head
                            .map_or_else(|| "none".to_string(), |head| head.to_string()),
                    )
                },
            );
            eprintln!("HEAD MISMATCH: expected {expected}, archive tip {tip}");
            eprintln!(
                "  partition {name}: the chain it holds is internally perfect but stops short of \
                 the expected head. This is what tail truncation looks like: an attacker who \
                 deletes the newest manifests, and the objects they name, leaves a shorter \
                 archive that verifies.",
            );
            return ExitCode::FAILURE;
        }
    }

    let orphans: Vec<&String> = report
        .partitions
        .iter()
        .flat_map(|partition| partition.orphan_objects.iter())
        .collect();
    if !orphans.is_empty() {
        eprintln!(
            "ORPHAN OBJECTS: {} object(s) with no manifest",
            orphans.len()
        );
        for key in orphans.iter().take(ORPHANS_SHOWN) {
            eprintln!("  {key}");
        }
        if let Some(hidden) = orphans.len().checked_sub(ORPHANS_SHOWN).filter(|n| *n > 0) {
            eprintln!("  ... and {hidden} more");
        }
        eprintln!(
            "  An object no manifest names makes no integrity claim: nothing signed says what \
             its bytes should be."
        );
        return ExitCode::FAILURE;
    }

    let restarts: usize = report
        .partitions
        .iter()
        .map(|partition| partition.epochs.len().saturating_sub(1))
        .sum();
    if restarts > 0 && !args.allow_epoch_restarts {
        eprintln!("INCOMPLETE ATTESTATION: chain restarted {restarts} time(s)");
        eprintln!(
            "  A restart happens when the broker could not read back its chain tip, so it began \
             a new epoch at genesis rather than silently continuing the old chain. Nothing binds \
             the manifests before a restart to the manifests after it, so the archive is \
             attested in pieces."
        );
        eprintln!(
            "  Expected with the non-durable in-memory RLMM. The fix is a topic-backed \
             `remote_log_metadata`, which survives a broker restart. Pass \
             --allow-epoch-restarts to accept the restarts instead."
        );
        return ExitCode::FAILURE;
    }

    let unsigned = total(report, |partition| partition.unsigned_manifests);
    let untrusted = total(report, |partition| partition.untrusted_manifests);
    if unsigned > 0 || untrusted > 0 {
        eprintln!(
            "INCOMPLETE ATTESTATION: {unsigned} manifest(s) unsigned, {untrusted} signed by an \
             untrusted key"
        );
        if untrusted > 0 && args.public_key.is_none() {
            eprintln!(
                "  No trusted key was given. Pass --key-id and --public-key to check the \
                 signatures the archive carries."
            );
        }
        return ExitCode::FAILURE;
    }

    if report.manifests() == 0 {
        println!("OK: empty archive");
        return ExitCode::SUCCESS;
    }

    println!(
        "OK: {} manifests over {} partition(s), chain continuous, all signatures valid",
        report.manifests(),
        report.partitions.len()
    );
    for line in summary(report) {
        println!("{line}");
    }
    ExitCode::SUCCESS
}

/// The first partition whose tip is not `expected`.
fn tip_mismatch(
    report: &ArchiveVerifyReport,
    expected: ChainHead,
) -> Option<&PartitionVerifyReport> {
    report
        .partitions
        .iter()
        .find(|partition| partition.head != Some(expected))
}

/// Sums one per-partition count over the whole report.
fn total(report: &ArchiveVerifyReport, count: impl Fn(&PartitionVerifyReport) -> u64) -> u64 {
    report
        .partitions
        .iter()
        .map(count)
        .fold(0u64, u64::saturating_add)
}

/// One line per partition, naming the tip an operator feeds to `--expect-head`.
fn summary(report: &ArchiveVerifyReport) -> Vec<String> {
    report
        .partitions
        .iter()
        .map(|partition| {
            let tip = partition
                .head
                .map_or_else(|| "none".to_string(), |head| head.to_string());
            let start = partition
                .epochs
                .iter()
                .map(|epoch| epoch.start_offset)
                .min();
            let end = partition.epochs.iter().map(|epoch| epoch.end_offset).max();
            let offsets = match (start, end) {
                (Some(start), Some(end)) => format!("offsets {start}..{end}"),
                _ => "no offsets".to_string(),
            };
            let mut line = format!(
                "  {}: tip {tip}, {} manifest(s), {} object(s), {offsets}, {} epoch(s)",
                partition.partition_dir,
                partition.manifests,
                partition.objects_checked,
                partition.epochs.len()
            );
            if !partition.offset_gaps.is_empty() {
                let _ = write!(line, ", {} offset gap(s)", partition.offset_gaps.len());
            }
            if !partition.orphan_objects.is_empty() {
                let _ = write!(
                    line,
                    ", {} orphan object(s)",
                    partition.orphan_objects.len()
                );
            }
            line
        })
        .collect()
}
