//! Bounded stateright model composing producer-client failover routing with the
//! broker-side idempotent-producer dedup check.
//!
//! # Module layout
//!
//! This file is the module root. It declares the children and holds the two
//! tests that run the checker, on the leader as it is and on the leader before
//! #1947. Each child holds one concern: [`bounds`] the
//! bounded cluster and client shape, [`witness`] the bitset that keeps the
//! `sometimes` properties honest, [`state`] the search space and the pure
//! queries over it, [`produce`] one client produce attempt against the real
//! `check_pure`, which reads a leader tracker that takes each answer only
//! when its request commits it, through the real `committed_entry`, and the
//! writer's lookup of a passed batch among the log's retained batches,
//! [`properties`] what the checker proves, [`model`] the
//! stateright [`Model`](stateright::Model) implementation, and [`runner`] the
//! checker bounds.

// Each child is declared with an explicit `#[path]`. The declaration then names
// the same file whether or not this root is itself reached through a `#[path]`.
#[path = "client_server_failover_model/bounds.rs"]
mod bounds;
#[path = "client_server_failover_model/model.rs"]
mod model;
#[path = "client_server_failover_model/produce.rs"]
mod produce;
#[path = "client_server_failover_model/properties.rs"]
mod properties;
#[path = "client_server_failover_model/runner.rs"]
mod runner;
#[path = "client_server_failover_model/state.rs"]
mod state;
#[path = "client_server_failover_model/witness.rs"]
mod witness;

use self::runner::{run_gate_only, run_model};

#[test]
fn client_server_failover_preserves_acked_batch() { run_model(); }

/// RED: the leader before #1947 appends a retry a second time. See
/// [`runner`]'s "RED regression".
#[test]
fn before_the_writer_lookup_a_retry_appends_again() { run_gate_only(); }
