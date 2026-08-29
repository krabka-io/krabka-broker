//! Multi-node `KraftController` async driver simulation. This is the isolation
//! acceptance for the KIP-595 consensus engine (Slice 3c, Task 6).
//!
//! Three real [`KraftController`](krabka_raft::kraft::KraftController)s run over
//! tempdir [`KraftLog`](krabka_raft::kraft::KraftLog)s on a tokio multi-thread
//! runtime. They are wired to each other through an in-memory
//! [`PeerSender`](krabka_raft::kraft::PeerSender), the
//! [`SimNet`](crate::sim_net::SimNet), and they use no TCP. Each engine's
//! `PeerSender` routes a `(peer, api_key, body)` to the target engine's
//! [`KraftController::deliver`](krabka_raft::kraft::KraftController::deliver)
//! and returns the response body. Every engine's loop is non-blocking, because
//! peer sends are spawned fire-and-forget and the loop never `.await`s a send
//! inline. Reciprocal RPCs between engines therefore cannot deadlock.
//!
//! This exercises the real engine, loop, log, and apply path: election,
//! record-carrying Fetch replication, leader failover, and restart recovery. It
//! is deterministic enough to be the debugging anchor when the TCP integration
//! (Task 10) misbehaves.
//!
//! This root only declares the parts. The in-memory network lives in `sim_net`,
//! the engine builders and convergence waits in `harness`, and each acceptance
//! sits in the module named for the behaviour it drives.

#[path = "kraft_engine_sim/election.rs"]
mod election;
#[path = "kraft_engine_sim/failover.rs"]
mod failover;
#[path = "kraft_engine_sim/harness.rs"]
mod harness;
#[path = "kraft_engine_sim/replication.rs"]
mod replication;
#[path = "kraft_engine_sim/restart.rs"]
mod restart;
#[path = "kraft_engine_sim/sim_net.rs"]
mod sim_net;
#[path = "kraft_engine_sim/snapshot_catchup.rs"]
mod snapshot_catchup;
