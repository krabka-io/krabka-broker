//! The command line every test that calls `restore` directly parses first.
//!
//! `restore` takes a `RestoreArgs`, and the flags that make `format_target`
//! succeed are the same in every test, so they are spelled once here and go
//! through `Cli`, the parser the binary itself uses.

use std::path::Path;

use clap::Parser as _;
use krabka_restore::{Cli, RestoreArgs};

/// Build `RestoreArgs` with a valid target-side flag set (`--node-id`,
/// `--standalone`, `--controller-listener`) so `format_target` succeeds, plus
/// whatever `extra` flags a test needs.
pub(crate) fn restore_args(archive_root: &Path, log_dir: &Path, extra: &[&str]) -> RestoreArgs {
    let mut argv = vec![
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        archive_root.display().to_string(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:9093".to_owned(),
    ];
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    Cli::try_parse_from(argv).expect("valid command line").args
}
