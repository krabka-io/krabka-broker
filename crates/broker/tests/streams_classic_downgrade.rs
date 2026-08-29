//! KIP-1071 integration tests for the cold downgrade from streams to classic,
//! and for admin type-awareness (slice 2).
//!
//! A drained streams group converts to classic on a classic `JoinGroup`, and
//! keeps its offsets. A streams group with a live member rejects that
//! `JoinGroup`. The admin handlers List, Describe, and Delete respect the type
//! lock.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `streams_classic_downgrade/` directory, which keeps the parts out of
// `tests/` where every `.rs` file would become another test binary.
#[path = "streams_classic_downgrade/downgrade_classic_join.rs"]
mod downgrade_classic_join;
#[path = "streams_classic_downgrade/downgrade_conversion.rs"]
mod downgrade_conversion;
#[path = "streams_classic_downgrade/downgrade_harness.rs"]
mod downgrade_harness;
#[path = "streams_classic_downgrade/downgrade_streams_join.rs"]
mod downgrade_streams_join;
#[path = "streams_classic_downgrade/downgrade_type_lock.rs"]
mod downgrade_type_lock;

// ── error codes ──────────────────────────────────────────────────────────────
const ERR_NONE: i16 = 0;
const ERR_COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;
const ERR_GROUP_ID_NOT_FOUND: i16 = 69;
const ERR_NON_EMPTY_GROUP: i16 = 68;

/// The number of heartbeat rounds a streams member gets to converge on its
/// assignment. After that, the test continues with whatever state it
/// reached.
const CONVERGE_TRIES: usize = 15;
