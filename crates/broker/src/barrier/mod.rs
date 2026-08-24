//! Cross-topic barrier markers and the cuts they define.
//!
//! A barrier group is a named set of topics. An injection writes an
//! epoch-stamped marker into every partition of the group, and the offset of
//! epoch N's marker in each partition defines cut N. The coordinator then
//! publishes those offsets as a cut record, so a client can read the cut with
//! an ordinary Kafka consumer.
//!
//! `docs/superpowers/specs/2026-08-24-barrier-markers-design.md` holds the
//! design, the frozen wire formats, and the guarantee this primitive gives.

pub(crate) mod persistence;

/// The internal topic that carries group definitions, injection-start records,
/// and cuts.
pub(crate) const STATE_TOPIC: &str = "__barrier_state";
