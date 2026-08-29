//! KIP-1071 classic→streams cold upgrade integration tests.
//!
//! The tests verify that a `StreamsGroupHeartbeat` for a **drained** classic
//! group converts it in place, and that committed offsets survive. They also
//! verify that the broker rejects a classic group with **live members** with
//! `GROUP_ID_NOT_FOUND` (69).

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `streams_classic_upgrade/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
#[path = "streams_classic_upgrade/upgrade_classic.rs"]
mod upgrade_classic;
#[path = "streams_classic_upgrade/upgrade_conversion.rs"]
mod upgrade_conversion;
#[path = "streams_classic_upgrade/upgrade_harness.rs"]
mod upgrade_harness;
#[path = "streams_classic_upgrade/upgrade_live_member.rs"]
mod upgrade_live_member;
#[path = "streams_classic_upgrade/upgrade_streams.rs"]
mod upgrade_streams;
