//! Administers krabka barrier groups.
//!
//! A barrier group is a named set of topics. A coordinator injects an
//! epoch-stamped marker into every partition of the set and publishes the
//! resulting offsets as a cut, which is an exact and reproducible point in
//! every one of those topics at once. This is the operator's side of that:
//! define a group, trigger a cut, read the cuts back, and prove that the
//! marker a cut names is really in the log.
//!
//! This is the `krabka barrier` command from the monorepo's `krabka-cli`. That
//! crate also drives the gres layer, which is why it could not follow the
//! broker into this repository. It is a library as well as a binary so tests
//! call [`run_from_args`] in process: a test that spawns the binary needs a
//! Cargo working tree to build it from, and a Bazel test sandbox has none.
//!
//! Every subcommand speaks one krabka-private api key, in the 1010 to 1014
//! range. A JVM `AdminClient` cannot send those, which is why the cuts are also
//! published to `__barrier_state` where any consumer can read them.

mod cli;
mod dispatch;
mod report;
mod verify;

pub use self::{
    cli::{Cli, Command},
    dispatch::{run, run_from_args},
    verify::{Mismatch, VerifyOutcome},
};

/// The exit code for a request the broker refused.
const EXIT_REFUSED: i32 = 1;
/// The exit code for a transport failure, where nothing is known about the
/// request's outcome.
const EXIT_UNREACHABLE: i32 = 2;
/// The exit code for a cut whose log does not match what it claims.
const EXIT_MISMATCH: i32 = 3;
