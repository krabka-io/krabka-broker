//! Driving a restore the way the binary does, and reading the partition it
//! wrote back.
//!
//! Every scenario differs only in the bound flags it passes and the archive it
//! points at, so the command line, the fresh target directory, and the reopen
//! are built once here.

use std::path::{Path, PathBuf};

use clap::Parser as _;
use krabka_log::{Log, LogConfig, name};
use krabka_restore::{Cli, RestoreArgs, restore};
use tempfile::TempDir;

/// Build the `RestoreArgs` every scenario shares -- a local archive, a
/// fresh target directory, and a standalone node 1, matching the shape
/// `materialize.rs`'s own tests use to satisfy `format_target`'s
/// `--node-id` requirement -- via `Cli::try_parse_from`, the same path the
/// binary and the crate's own tests use. `extra` carries the bound flags
/// under test.
pub(crate) fn restore_args(archive_dir: &Path, target_dir: &Path, extra: &[&str]) -> RestoreArgs {
    let mut argv: Vec<String> = vec![
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        archive_dir.display().to_string(),
        "--log-dir".to_owned(),
        target_dir.display().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:9093".to_owned(),
    ];
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    Cli::try_parse_from(argv).expect("valid command line").args
}

/// Run a restore of `archive_dir` with `extra` bound flags, into a fresh
/// empty target directory. Returns the target's `TempDir` (keep it alive
/// for as long as the returned path is read) and the target log directory
/// itself.
pub(crate) async fn run_restore(archive_dir: &Path, extra: &[&str]) -> (TempDir, PathBuf) {
    let target = tempfile::tempdir().expect("target tempdir");
    let target_dir = target.path().join("restored");
    let args = restore_args(archive_dir, &target_dir, extra);
    restore(&args).await.expect("restore");
    (target, target_dir)
}

/// Reopen the partition `restore()` wrote, the way an operator would after
/// the tool exits.
pub(crate) fn reopen(target_dir: &Path, topic: &str, partition: i32) -> Log {
    let dir = name::partition_dir(target_dir, topic, partition);
    Log::open(&dir, LogConfig::default()).expect("reopen restored partition")
}
