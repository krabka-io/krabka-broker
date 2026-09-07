//! The `krabka-backup` command line.
//!
//! Four subcommands, and every one of them takes the same `--archive-*` flag
//! group: a capture is written into the archive a restore reads, so the tool
//! never introduces a second place to point at.

use clap::{Parser, Subcommand};

use crate::archive::ArchiveArgs;

/// The capture selector that means "the newest capture in the archive".
pub const LATEST: &str = "latest";

/// Captures the inputs a point-in-time restore needs, and puts committed group
/// offsets back after one.
#[derive(Parser, Debug)]
#[command(name = "krabka-backup", version, about, long_about = None)]
pub struct Cli {
    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// One thing the tool does.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Copy this node's restore inputs into the archive.
    Capture {
        /// The broker's `log.dir`, mounted read-only. Without it the capture
        /// takes group offsets only.
        #[arg(long, value_name = "DIR")]
        log_dir: Option<std::path::PathBuf>,

        /// A broker to read committed group offsets from. Without it the
        /// capture takes the on-disk snapshots only.
        #[arg(
            long,
            short = 'b',
            value_name = "HOST:PORT",
            env = "KRABKA_BOOTSTRAP_SERVER"
        )]
        bootstrap_server: Option<String>,

        /// Where the capture goes.
        #[command(flatten)]
        archive: ArchiveArgs,
    },

    /// List the captures the archive holds.
    List {
        /// Where the captures are.
        #[command(flatten)]
        archive: ArchiveArgs,
    },

    /// Re-read a capture and check every artifact against its recorded digest.
    Verify {
        /// Which capture, or `latest`.
        #[arg(long, value_name = "ID", default_value = LATEST)]
        capture: String,

        /// Where the captures are.
        #[command(flatten)]
        archive: ArchiveArgs,
    },

    /// Commit a capture's group offsets into a restored cluster.
    RestoreOffsets {
        /// Which capture, or `latest`.
        #[arg(long, value_name = "ID", default_value = LATEST)]
        capture: String,

        /// The restored cluster.
        #[arg(
            long,
            short = 'b',
            value_name = "HOST:PORT",
            env = "KRABKA_BOOTSTRAP_SERVER"
        )]
        bootstrap_server: String,

        /// Report what would be committed and commit nothing.
        #[arg(long)]
        dry_run: bool,

        /// Where the captures are.
        #[command(flatten)]
        archive: ArchiveArgs,
    },
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use clap::Parser as _;

    use super::{Cli, Command};

    #[test]
    fn a_capture_takes_both_sources_and_one_archive() {
        let cli = Cli::try_parse_from([
            "krabka-backup",
            "capture",
            "--log-dir",
            "/var/lib/krabka",
            "--bootstrap-server",
            "broker-1:9092",
            "--archive-s3-bucket",
            "krabka-tier",
            "--archive-prefix",
            "prod/",
        ])
        .expect("valid command line");

        assert!(let Command::Capture { .. } = &cli.command);
        let Command::Capture {
            log_dir,
            bootstrap_server,
            archive,
        } = cli.command
        else {
            unreachable!("the assertion above rejected every other command")
        };
        check!(log_dir == Some(std::path::PathBuf::from("/var/lib/krabka")));
        check!(bootstrap_server == Some("broker-1:9092".to_owned()));
        check!(archive.s3_bucket == Some("krabka-tier".to_owned()));
        check!(archive.prefix == Some("prod/".to_owned()));
    }

    #[test]
    fn a_capture_without_an_archive_is_rejected() {
        check!(Cli::try_parse_from(["krabka-backup", "capture", "--log-dir", "/data"]).is_err());
    }

    #[test]
    fn verify_and_restore_offsets_default_to_the_newest_capture() {
        let verify =
            Cli::try_parse_from(["krabka-backup", "verify", "--archive-local", "/mnt/backups"])
                .expect("valid command line");
        assert!(let Command::Verify { .. } = &verify.command);
        let Command::Verify { capture, .. } = verify.command else {
            unreachable!("the assertion above rejected every other command")
        };
        check!(capture == "latest");

        let restore = Cli::try_parse_from([
            "krabka-backup",
            "restore-offsets",
            "-b",
            "broker-1:9092",
            "--archive-local",
            "/mnt/backups",
        ])
        .expect("valid command line");
        assert!(let Command::RestoreOffsets { .. } = &restore.command);
        let Command::RestoreOffsets {
            capture, dry_run, ..
        } = restore.command
        else {
            unreachable!("the assertion above rejected every other command")
        };
        check!(capture == "latest");
        check!(!dry_run);
    }

    #[test]
    fn restoring_offsets_needs_a_cluster_to_commit_into() {
        check!(
            Cli::try_parse_from([
                "krabka-backup",
                "restore-offsets",
                "--archive-local",
                "/mnt/backups",
            ])
            .is_err()
        );
    }
}
