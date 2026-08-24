//! Cross-topic barrier markers and the cuts they define.
//!
//! A barrier group is a named set of topics. An injection writes an
//! epoch-stamped marker into every partition of the group, and the offset of
//! epoch N's marker in each partition defines cut N. The coordinator then
//! publishes those offsets as a cut record, so a client can read the cut with
//! an ordinary Kafka consumer.
//!
//! # What a cut guarantees, and what it does not
//!
//! Epoch N's marker sits at exactly one offset in each partition of the group,
//! totally ordered against every other append to that partition. Every
//! partition gets the same epoch. The cut is durable across restart, failover
//! and replication, and compaction keeps the markers.
//!
//! A cut is **not** causally consistent across independent producers. A
//! producer can write to topic A after A's marker lands and to topic B before
//! B's marker lands, so its second write falls before the cut and its first
//! falls after. Chandy-Lamport consistency needs the markers to travel along
//! the channels between the processes, and a broker cannot supply that for a
//! producer it does not control.
//!
//! That is enough for disaster-recovery replay points, audit snapshots,
//! shadow-run alignment and stream-processor checkpoints. It is not enough to
//! reason about cross-topic write causality.
//!
//! A marker survives compaction but not retention. `Log::tick` applies time and
//! size retention whatever the cleanup policy says, so an operator should keep
//! a group's cut retention at or below the shortest retention of its member
//! topics.
//!
//! [`marker`] and [`persistence`] carry the wire formats. The same cut bytes
//! are asserted in `krabka-streams-rs`, `krabka-streams-java` and
//! `krabka-streams-go`, which read the format that only this crate writes.
//!
//! # Key Modules
//!
//! - [`marker`] builds and parses the control record that lands in a data
//!   partition.
//! - [`persistence`] is the byte-exact codec of the `__barrier_state` records.
//! - [`coordinator`] owns the groups, the epochs, and the injection protocol.
//! - [`injection`] writes the markers of one epoch and collects their offsets.
//! - [`state`] holds the in-memory group entry and the pure decisions over it.
//! - [`scheduler`] drives the per-group interval.

pub(crate) mod bootstrap;
pub(crate) mod config;
pub(crate) mod coordinator;
pub(crate) mod error;
pub(crate) mod injection;
pub(crate) mod marker;
pub(crate) mod metrics;
pub(crate) mod partitioner;
pub(crate) mod persistence;
pub(crate) mod scheduler;
pub(crate) mod state;

#[cfg(test)]
mod test_support;

/// The internal topic that carries group definitions, injection-start records,
/// and cuts.
pub(crate) const STATE_TOPIC: &str = "__barrier_state";
