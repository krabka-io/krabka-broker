//! KFC-9 break-glass, driven over the wire by more than one principal.
//!
//! The broker half of the two-person rule is unit-tested, and the tool half is
//! covered by `crates/guard-cli/tests/guard_cli.rs`. Neither of those can prove
//! the thing the feature promises. A `PLAINTEXT` listener authenticates every
//! connection as one principal, `User:ANONYMOUS`, so a suite that speaks over
//! one can show that a proposer may not approve their own proposal and can
//! never show two distinct approvers completing a proposal. The guard-cli
//! module doc says so, and defers the completion cases here.
//!
//! This suite boots the broker behind a `SASL_PLAINTEXT` listener with four
//! credentials, so `User:alice` proposes, `User:bob` and `User:carol` approve,
//! and `User:mallory` stands outside the approver set. Every case below rests
//! on that: the refusals prove the rule bites, and the completions prove the
//! rule can be satisfied at all.
//!
//! # What each tier covers
//!
//! * The loop — a proposal, two approvals, the transition, the spent proposal.
//! * The three distinct-principal refusals — a second approval by one person, a
//!   proposer approving themselves, and a principal outside the set.
//! * Expiry, on the approve path and on the consume path.
//! * A signature, demanded by `break_glass.signed_actions`.
//! * Atomicity: the consume and the transition in one raft append, read back
//!   out of the committed metadata log.
//! * Durability: an approved proposal across a controller failover.
//! * Every gated transition, refused with no proposal and run with one.
//! * The background unclean-recovery path, under all three settings.
//!
//! # What this suite does not cover, and why
//!
//! The thaw of a topic freeze is the sixth gated transition, and it is the one
//! that names its proposal on the request rather than out of band. It needs the
//! freeze signing layout as well as this one, so it belongs with the freeze
//! suite. The transaction admission path is gated by the freeze and not by
//! break-glass, so it belongs there too.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `break_glass/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "break_glass/atomicity.rs"]
mod atomicity;
#[path = "break_glass/background_recovery.rs"]
mod background_recovery;
#[path = "break_glass/cluster.rs"]
mod cluster;
#[path = "break_glass/expiry.rs"]
mod expiry;
#[path = "break_glass/failover.rs"]
mod failover;
#[path = "break_glass/gated_transitions.rs"]
mod gated_transitions;
#[path = "break_glass/principals.rs"]
mod principals;
#[path = "break_glass/proposals.rs"]
mod proposals;
#[path = "break_glass/signatures.rs"]
mod signatures;
#[path = "break_glass/topics.rs"]
mod topics;
#[path = "break_glass/transitions.rs"]
mod transitions;
#[path = "break_glass/two_person_rule.rs"]
mod two_person_rule;
