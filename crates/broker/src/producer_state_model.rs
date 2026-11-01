//! Exhaustive stateright models of the idempotent-producer tracker.
//!
//! # Module layout
//!
//! This file is the module root. Each child is one model:
//!
//! - [`classification`] submits one batch at a time and checks every answer of
//!   the tracker's dedup and ordering check against Kafka's rules over the
//!   five batches Kafka retains.
//! - [`requests`] splits each produce request into the steps that run on the
//!   broker (the dedup gate against the tracker, the writer's append, the
//!   tracker commit and the `acks=all` wait), lets a client retry while the
//!   first copy is still in flight, and checks that no batch reaches the log
//!   twice.

// Each child is declared with an explicit `#[path]`. The declaration then names
// the same file whether or not this root is itself reached through a `#[path]`.
#[path = "producer_state_model/classification.rs"]
mod classification;
#[path = "producer_state_model/requests.rs"]
mod requests;
