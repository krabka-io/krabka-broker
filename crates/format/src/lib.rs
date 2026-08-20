//! Formats a fresh krabka broker log directory.
//!
//! A `KRaft` node will not boot against an unformatted directory: the broker
//! treats one as operator error and aborts startup. Formatting seeds
//! `meta.properties.json`, the bootstrap records, and the singleton
//! `VotersRecord`, and can provision seed SCRAM credentials at the same time.
//!
//! This is the `crabka format` command from the monorepo's `crabka-cli`. That
//! crate also drives the gres layer, which is why it could not follow the broker
//! into this repository; the command itself needs only [`crabka_metadata`] and
//! [`crabka_security`]. It is a library as well as a binary so `crabka-cli` can
//! call it rather than carry a second copy.

use clap::Parser;

mod format;
mod ids;

pub use format::{FormatArgs, ScramSpec, run};
pub use ids::{ClusterId, DirectoryId};

/// The formatter's command line.
///
/// Shared by the binary and by [`run_from_args`], so both accept exactly the
/// same flags.
#[derive(Parser)]
#[command(
    name = "crabka-format",
    version,
    about = "Format a fresh log directory, with optional seed SCRAM credentials"
)]
pub struct Cli {
    /// The formatter's arguments, flattened so they are top-level flags.
    #[command(flatten)]
    pub args: FormatArgs,
}

/// Run the formatter from an argv-style iterator, returning its exit code.
///
/// Every broker test that boots a node needs a formatted log directory first.
/// Calling this beats spawning the binary: a subprocess needs a Cargo working
/// tree to build from, which a Bazel test sandbox does not have, and the
/// formatting is setup rather than the thing under test. The binary itself is
/// covered end to end by `tests/format_smoke.rs`.
///
/// # Panics
///
/// Panics if `argv` does not parse, which for a caller passing a literal
/// argument list is a bug in that list rather than a runtime condition.
pub async fn run_from_args<I, T>(argv: I) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    run(Cli::parse_from(argv).args).await
}
