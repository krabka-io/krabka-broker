//! Multi-broker durability: three-node round-trips and byte-for-byte replica
//! comparison, `acks=all` across a leader crash, and transactional EOS.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.
//!
//! The binary root carries only the module tree. One child covers each
//! durability scenario: [`quorum_round_trip`] for the three-node produce and
//! consume across a controller-leader kill, [`replica_byte_compare`] for the
//! byte-identical replica segments, [`transactional_eos`] for the committed and
//! aborted transaction isolation levels, [`acks_all`] for the steady-state
//! high-watermark gate, and [`leader_crash`] for that same gate across a
//! partition-leader crash.
//!
//! Cargo compiles this file as its own test binary, so a `mod` declaration in
//! it resolves against `tests/` rather than against a directory named for the
//! file. Each child therefore carries an explicit `#[path]` onto the sibling
//! `jvm_acceptance_durability/` directory. `jvm_acceptance` and `support` are
//! `tests/<name>/mod.rs` helpers, which the crate-root rule already resolves.

#[path = "jvm_acceptance_durability/acks_all.rs"]
mod acks_all;
mod jvm_acceptance;
#[path = "jvm_acceptance_durability/leader_crash.rs"]
mod leader_crash;
#[path = "jvm_acceptance_durability/quorum_round_trip.rs"]
mod quorum_round_trip;
#[path = "jvm_acceptance_durability/replica_byte_compare.rs"]
mod replica_byte_compare;
mod support;
#[path = "jvm_acceptance_durability/transactional_eos.rs"]
mod transactional_eos;
