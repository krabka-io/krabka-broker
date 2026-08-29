//! Stateright model of the KIP-595 and KIP-996 `KRaft` consensus core.
//!
//! The model state holds the REAL `QuorumStateMachine` for each node, plus an
//! in-memory log and an unordered message network. `next_state` runs the
//! production `on_event`, and the checker explores every interleaving. The
//! committed-log linearizability tester lives here too. Message loss, message
//! duplication, and node crashes are modeled as explicit `ModelAction`s.
#![allow(dead_code)]

mod checker;
mod commit;
mod config;
mod log;
mod spec;
mod state;
mod transitions;

pub use self::config::ConsensusModel;
