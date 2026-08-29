//! Formats a fresh krabka broker log directory.
//!
//! A `KRaft` node will not boot against an unformatted directory: the broker
//! treats one as operator error and aborts startup. Formatting seeds
//! `meta.properties.json`, the bootstrap records, and the singleton
//! `VotersRecord`, and can provision seed SCRAM credentials at the same time.
//!
//! [`run_with_records`] takes further [`MetadataRecord`]s from the caller and
//! seeds them into the same stream. A restore tool that rebuilds a cluster from
//! tiered-storage archives hands over the topic and partition records it
//! recovered that way, and the broker then boots with those topics present.
//!
//! This is the `krabka format` command from the monorepo's `krabka-cli`. That
//! crate also drives the gres layer, which is why it could not follow the broker
//! into this repository; the command itself needs only [`krabka_metadata`] and
//! [`krabka_security`]. It is a library as well as a binary so `krabka-cli` can
//! call it rather than carry a second copy.

use clap::Parser;

mod format;
mod ids;

pub use format::{FormatArgs, ScramSpec, run, run_with_records};
pub use ids::{ClusterId, DirectoryId};
/// The seed record type [`run_with_records`] accepts, re-exported so a caller
/// building a bootstrap stream does not have to name [`krabka_metadata`]
/// itself.
pub use krabka_metadata::MetadataRecord;

/// The formatter's command line.
///
/// Shared by the binary and by [`run_from_args`], so both accept exactly the
/// same flags.
#[derive(Parser)]
#[command(
    name = "krabka-format",
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
    run_from_args_with_records(argv, Vec::new()).await
}

/// Run the formatter from an argv-style iterator, seeding `extra` alongside the
/// records the flags produce, and return its exit code.
///
/// This is the entry point for a tool that materializes a cluster and then has
/// to hand the formatter the metadata it recovered, such as the topic and
/// partition records behind a point-in-time restore. [`FormatArgs`] holds
/// private fields, so an argv is how such a caller states the rest of the
/// format; [`run_with_records`] documents where `extra` lands in the seed
/// stream.
///
/// # Panics
///
/// Panics if `argv` does not parse, which for a caller passing a literal
/// argument list is a bug in that list rather than a runtime condition.
pub async fn run_from_args_with_records<I, T>(argv: I, extra: Vec<MetadataRecord>) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    run_with_records(Cli::parse_from(argv).args, extra).await
}
