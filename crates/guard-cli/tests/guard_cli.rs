//! The tool driven end to end against a real broker.
//!
//! Every case calls [`krabka_guard::run_from_args`] in process rather than
//! spawning the binary. A subprocess needs a Cargo working tree to build from
//! and a Bazel test sandbox has none, which is the same reason `krabka-barrier`
//! and `krabka-format` are libraries as well as binaries.
//!
//! What these cover that the unit tests cannot: that each subcommand's request
//! reaches a broker that answers it, that a signature this machine makes
//! verifies inside that broker, and that the exit code reports what happened.
//!
//! - [`freeze`] — the safe direction: `freeze set` and `freeze list`, signed
//!   and unsigned, and the scope the broker refuses.
//! - [`thaw`] — the dangerous direction: the refusals `freeze clear` reports
//!   when an approval is missing or a signature does not hold.
//! - [`verification`] — `freeze list --verify-signatures` against local key
//!   material that cannot prove the entry.
//! - [`break_glass`] — a proposal proposed, approved, and withdrawn, and one
//!   that never existed.
//! - [`transport`] — a bootstrap address nothing listens on.
//! - [`support`] — the broker, the operator key, and the exit codes they share.
//!
//! # The principal
//!
//! A plaintext listener authenticates every connection as `ANONYMOUS`, and both
//! the freeze path and the break-glass path name that connection
//! `User:ANONYMOUS`. One `[[operator_keys]]` entry therefore serves both, which
//! is what the shared trust set is for.
//!
//! A two-person rule cannot be completed over one such listener, because the
//! proposer and the approver are then the same name. The refusal that says so
//! is asserted below; the completion belongs to the broker's own suite, which
//! can mint two principals.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `guard_cli/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "guard_cli/break_glass.rs"]
mod break_glass;
#[path = "guard_cli/freeze.rs"]
mod freeze;
#[path = "guard_cli/support.rs"]
mod support;
#[path = "guard_cli/thaw.rs"]
mod thaw;
#[path = "guard_cli/transport.rs"]
mod transport;
#[path = "guard_cli/verification.rs"]
mod verification;
