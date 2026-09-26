//! Stateright model of the KIP-595 and KIP-996 `KRaft` consensus core.
//!
//! The model state holds the REAL `QuorumStateMachine` for each node, plus an
//! in-memory log and an unordered message network. `next_state` runs the
//! production `on_event`, and the checker explores every interleaving. The
//! committed-log linearizability tester lives here too. Message loss, message
//! duplication, and node crashes are modeled as explicit `ModelAction`s.
//!
//! Replication is the one abstracted exchange. The records a fetch carries
//! back are applied when the follower sends it, not when a response arrives,
//! and only when the leader's log extends the follower's; the high watermark
//! reaches a follower on the same terms. A follower whose log disagrees with
//! the leader's is never overwritten: the production `handle_fetch` answers
//! its fetch with a diverging epoch, the model delivers that as a fetch
//! response, and the production `handle_fetch_response` emits the `TruncateTo`
//! that cuts it back. Log matching and leader completeness therefore rest on
//! the production truncation path rather than on the model.
#![allow(dead_code)]

mod checker;
mod commit;
mod config;
mod log;
mod spec;
mod state;
mod transitions;

pub use self::config::ConsensusModel;
