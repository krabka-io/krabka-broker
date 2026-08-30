//! Offline point-in-time restore of a krabka cluster.
//!
//! `krabka restore` reads a KIP-405 tiered-storage archive from object storage
//! and materializes a complete, bootable krabka data directory, replayed up to
//! a bound and verified segment by segment as it rehydrates. The bound is an
//! offset, a timestamp, or a set of exclude-record predicates, so an operator
//! can recover an event-sourced system to the state it held just before a bad
//! write. Recovery of that kind is a hand-built runbook everywhere else.
//!
//! The tool runs when the cluster does not. It reads the archive through the
//! object-store layer and formats the target through [`krabka_format`], and it
//! does not depend on the broker.
//!
//! # Stages
//!
//! A restore runs five stages, and [`restore`] drives them in order:
//!
//! 1. **Discover** lists the archive and groups the keys into segments per
//!    topic partition.
//! 2. **Verify** checks the framing and the CRC of every archived segment, and
//!    derives its true end offset and maximum timestamp.
//! 3. **Bound** compiles the operator's predicates and decides which batches
//!    and records survive.
//! 4. **Materialize** formats the target log directory and writes the
//!    partition data.
//! 5. **Report** renders what happened, as text or as JSON.
//!
//! # Limits
//!
//! A batch that the bound filters is re-encoded from the records that survive,
//! so its bytes are not identical to the archived bytes. `--exclude-key` and
//! `--exclude-header` match raw bytes and decode no payload. Without
//! `--rlmm-snapshot` a segment the old cluster had marked for deletion is
//! indistinguishable from a live one. Without `--metadata-snapshot`, topic
//! configuration, ACLs, client quotas, SCRAM credentials, and finalized
//! feature levels cannot be recovered.
//!
//! # Errors and exit codes
//!
//! The library returns [`RestoreError`]. Only [`run`] and the binary map one
//! onto an exit code, so an embedding tool keeps the structured error. See
//! [`EXIT_OK`] for the codes.

use std::{ffi::OsString, path::Path};

use clap::Parser;

mod args;
mod backend;
mod bound;
mod discover;
mod error;
mod materialize;
mod report;
mod verify;

pub use self::{
    args::{
        ArchiveArgs, HeaderPattern, OffsetBound, OffsetRange, PartitionRef, RestoreArgs, TargetArgs,
    },
    backend::{ArchiveStore, object_store_config, open_archive},
    bound::{BatchDecision, Predicates, RecordDecision},
    discover::{ArchiveInventory, ArchiveObject, PartitionInventory, SegmentInventory, inventory},
    error::{
        EXIT_ARCHIVE_UNREADABLE, EXIT_BAD_ARGUMENTS, EXIT_DIRTY_LOG_DIR, EXIT_INTEGRITY,
        EXIT_MATERIALIZE, EXIT_OK, RestoreError,
    },
    materialize::{FormatTargetOutcome, SegmentOutcome, format_target, write_segment},
    report::{MetadataRestoreReport, PartitionReport, ReportFormat, RestoreReport, SkippedSegment},
    verify::{SegmentFacts, VerifiedSegment, verify_segment},
};

/// The restore command line.
///
/// The binary and [`run_from_args`] share it, so both accept exactly the same
/// flags.
#[derive(Parser)]
// `long_about = None` keeps the struct's rustdoc out of `--help`. Without it
// clap renders the doc comment, intra-doc links and all, as the long help.
#[command(
    name = "krabka-restore",
    version,
    about = "Rebuild a bootable krabka log directory from a KIP-405 tiered-storage archive, \
             replayed to a point in time",
    long_about = None
)]
pub struct Cli {
    /// The restore's arguments, flattened so they are top-level flags.
    #[command(flatten)]
    pub args: RestoreArgs,
}

/// Run a restore and return the process exit code.
///
/// Every failure is reported on stderr and mapped onto the exit code
/// [`RestoreError::exit_code`] gives, rather than raised.
pub async fn run(args: RestoreArgs) -> i32 {
    let format = args.report;
    match restore(&args).await {
        Ok(report) => {
            if let Some(warning) = report.metadata.warning() {
                eprintln!("krabka restore: {warning}");
            }
            println!("{}", report.render(format));
            EXIT_OK
        }
        Err(error) => {
            eprintln!("krabka restore: {error}");
            error.exit_code()
        }
    }
}

/// Run a restore from an argv-style iterator and return the process exit code.
///
/// `--help` and `--version` render to stdout and return [`EXIT_OK`]. A
/// malformed command line renders to stderr and returns [`EXIT_BAD_ARGUMENTS`].
pub async fn run_from_args<I, T>(argv: I) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    match Cli::try_parse_from(argv) {
        Ok(cli) => run(cli.args).await,
        Err(error) => {
            let _ = error.print();
            if error.use_stderr() {
                EXIT_BAD_ARGUMENTS
            } else {
                EXIT_OK
            }
        }
    }
}

/// Run a restore and return its report.
///
/// This is the entry point for a caller that wants the structured outcome and
/// the structured error rather than an exit code.
///
/// # Errors
///
/// Returns [`RestoreError`] for a bad bound, a target that is not empty, an
/// archive that cannot be read, a segment that fails verification, or a target
/// that rejects a write. `--continue-on-corrupt` turns a verification failure
/// into a skipped segment in the report instead of an error.
pub async fn restore(args: &RestoreArgs) -> Result<RestoreReport, RestoreError> {
    args.validate()?;
    // Check the target before the archive scan. An operator who pointed at a
    // live data directory learns it in a second, not after a full download.
    ensure_empty_log_dir(&args.target.log_dir)?;

    let store = open_archive(args)?;
    let archive = inventory(&store, args).await?;
    let predicates = Predicates::from_args(args)?;
    let format = format_target(args, &archive).await?;

    let mut partitions = Vec::with_capacity(archive.partitions.len());
    let mut skipped = Vec::new();
    for entry in &archive.partitions {
        let mut segments = Vec::with_capacity(entry.segments.len());
        for segment in &entry.segments {
            let verified = match verify_segment(&store, &entry.partition, segment).await {
                Ok(verified) => verified,
                Err(error) if args.continue_on_corrupt => {
                    skipped.push(SkippedSegment {
                        topic: entry.partition.topic.clone(),
                        partition: entry.partition.partition,
                        segment_id: segment.segment_id,
                        reason: error.to_string(),
                    });
                    continue;
                }
                Err(error) => return Err(error),
            };
            segments.push(write_segment(args, &entry.partition, &verified, &predicates).await?);
        }
        partitions.push(PartitionReport {
            topic: entry.partition.topic.clone(),
            partition: entry.partition.partition,
            topic_id: entry.partition.topic_id,
            segments,
        });
    }

    Ok(RestoreReport {
        dry_run: args.dry_run,
        log_dir: args.target.log_dir.clone(),
        cluster_id: format.cluster_id,
        metadata: format.metadata,
        partitions,
        skipped,
    })
}

/// Refuse a target that already holds entries.
///
/// A restore writes a whole cluster, so it never merges into existing state.
/// An absent path is acceptable, and the formatter creates it.
fn ensure_empty_log_dir(log_dir: &Path) -> Result<(), RestoreError> {
    if !log_dir.exists() {
        return Ok(());
    }
    if std::fs::read_dir(log_dir)?.next().is_some() {
        return Err(RestoreError::LogDirNotEmpty(log_dir.to_path_buf()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn the_command_line_is_internally_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn an_absent_target_is_acceptable() {
        let parent = tempfile::tempdir().expect("temp dir");
        check!(ensure_empty_log_dir(&parent.path().join("missing")).is_ok());
    }

    #[test]
    fn an_empty_target_is_acceptable() {
        let target = tempfile::tempdir().expect("temp dir");
        check!(ensure_empty_log_dir(target.path()).is_ok());
    }

    #[test]
    fn a_target_that_holds_anything_is_refused() {
        let target = tempfile::tempdir().expect("temp dir");
        std::fs::write(target.path().join("meta.properties.json"), b"{}").expect("write");
        let refused = ensure_empty_log_dir(target.path());
        check!(matches!(refused, Err(RestoreError::LogDirNotEmpty(_))));
        check!(refused.expect_err("refused").exit_code() == EXIT_DIRTY_LOG_DIR);
    }

    #[tokio::test]
    async fn help_renders_and_succeeds() {
        check!(run_from_args(["krabka-restore", "--help"]).await == EXIT_OK);
    }

    #[tokio::test]
    async fn a_malformed_command_line_is_a_bad_argument() {
        check!(run_from_args(["krabka-restore", "--not-a-flag"]).await == EXIT_BAD_ARGUMENTS);
        check!(run_from_args(["krabka-restore"]).await == EXIT_BAD_ARGUMENTS);
    }
}
