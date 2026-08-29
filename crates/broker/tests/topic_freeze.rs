//! KFC-9 topic write freeze, proved over the wire against a live broker.
//!
//! The broker half of the freeze is unit-tested inside `krabka-broker`, and
//! nothing in that tier reaches the wire. A resolver that answers correctly and
//! a produce path that ignores its answer both pass those tests. This suite is
//! the tier that says a refusal refuses, on a real socket, through the real
//! Kafka codecs.
//!
//! # Every case runs a control topic
//!
//! Each case creates an unfrozen topic beside the frozen one and produces to it
//! in the same shape. That is the form KFC-1's suite established, and it is
//! what separates "this topic is frozen" from "the produce path is broken".
//! Delete the control half and a suite that refused *every* write would still
//! be green.
//!
//! # Every refusal asserts the log end offset
//!
//! A rejection is checked as a whole [`wire::ProduceOutcome`]: the error code,
//! the `error_message` the producer's on-call reads, and the partition's log end
//! offset. The third field is the load-bearing one. The freeze gate sits ahead
//! of the idempotent-sequence gate precisely so a refused batch leaves producer
//! state and the log untouched, and an error code alone does not rule out a
//! broker that answered `POLICY_VIOLATION` *and* appended. That is the worst
//! failure this feature can have, so it is asserted rather than assumed.
//!
//! # The signing bytes are reproduced here
//!
//! `krabka_broker::freeze::signing::freeze_signing_bytes` is `pub(crate)`
//! inside a `pub(crate)` module, so no test crate can call it. `signing_bytes`
//! rebuilds the layout that `crates/broker/src/freeze/signing.rs` documents,
//! field by field. `krabka-guard` carries a third copy for the same reason, and
//! a drift between any two of them fails here: a signature this suite makes has
//! to verify inside the broker, which is only true while both layouts agree.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `topic_freeze/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "topic_freeze/config_surface.rs"]
mod config_surface;
#[path = "topic_freeze/control_plane.rs"]
mod control_plane;
#[path = "topic_freeze/deletion.rs"]
mod deletion;
#[path = "topic_freeze/durability.rs"]
mod durability;
#[path = "topic_freeze/in_flight_transaction.rs"]
mod in_flight_transaction;
#[path = "topic_freeze/operator_signature.rs"]
mod operator_signature;
#[path = "topic_freeze/produce_gate.rs"]
mod produce_gate;
#[path = "topic_freeze/read_paths.rs"]
mod read_paths;
#[path = "topic_freeze/signing.rs"]
mod signing;
#[path = "topic_freeze/thaw.rs"]
mod thaw;
#[path = "topic_freeze/wire.rs"]
mod wire;
