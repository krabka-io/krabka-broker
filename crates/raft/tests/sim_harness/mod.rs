//! Shared deterministic, multi-node simulation harness for the KIP-595/996
//! `KRaft` consensus core (`krabka_raft::kraft`). This module is included by both
//! integration test binaries:
//!
//! - `kraft_sim.rs` runs the core over an in-memory [`SimLog`] (slice 3a).
//! - `kraft_log_sim.rs` runs the *same* core over a real on-disk
//!   [`krabka_raft::kraft::KraftLog`] (slice 3b).
//!
//! The harness wires N [`QuorumStateMachine`]s together through an in-memory
//! message bus and a logical clock. It translates every emitted [`Action`] into
//! the [`Event`]s its peers would observe, and drives the cluster to its
//! canonical fixed point of one leader and an agreed high watermark. The
//! [`SimNodeLog`] trait abstracts the per-node log, so the exact same scheduler
//! and action-translation logic drives both the fake log and the real log.
//!
//! Determinism is non-negotiable. There is no `Instant::now`, no `rand`, and no
//! `HashMap` iteration-order dependence anywhere. The clock is a `u64` of
//! logical milliseconds. All node containers and message containers are
//! `BTreeMap` or `BTreeSet`, so the iteration order is fixed. Election timeouts
//! are staggered by node id, so ties break deterministically and elections
//! converge.

// The two test binaries each include this module but exercise different subsets
// of its surface (the fake-log binary never constructs a `KraftBackedLog`, etc.),
// so per-binary dead-code warnings are expected and harmless.
#![allow(dead_code)]

mod actions;
pub mod cluster;
mod node;
pub mod node_log;
mod scheduler;
pub mod sim_log;
mod timers;
