//! Kafka's `max.message.bytes`, proved over the wire against a live broker.
//!
//! The cap is two features that have to hold together, and this suite asserts
//! both against a real socket.
//!
//! # The config surface
//!
//! `max.message.bytes` is one of the most commonly set topic configs in the
//! ecosystem, so `kafka-topics --create --config max.message.bytes=...` and
//! `kafka-configs --alter --add-config max.message.bytes=...` have to work.
//! [`config_surface`] drives both paths and reads the key back through
//! `DescribeConfigs`, which is what those tools show an operator.
//!
//! # The produce gate
//!
//! A batch above the cap is refused with `MESSAGE_TOO_LARGE` (10). Every case
//! that expects a refusal produces a batch one byte *under* the same cap first
//! or afterwards, because a broker that refused every write would pass a
//! one-sided test, and it reads the log end offset back, because an error code
//! alone does not rule out a broker that answered `MESSAGE_TOO_LARGE` and
//! appended anyway.
//!
//! # The sizes are exact, not approximate
//!
//! Kafka measures a record batch over its entire wire encoding, the 61-byte v2
//! header included, and refuses one *strictly* larger than the cap. Both halves
//! of that were settled against `apache/kafka:4.1.0`: with
//! `max.message.bytes=2048` a 2048-byte batch is accepted and a 2049-byte one
//! raises `RecordTooLargeException`. So [`wire::batch_of_wire_len`] builds a
//! batch of an exact encoded length rather than an approximate one, and the
//! cases sit on `cap` and `cap + 1` rather than somewhere either side.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `max_message_bytes/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "max_message_bytes/config_surface.rs"]
mod config_surface;
#[path = "max_message_bytes/produce_gate.rs"]
mod produce_gate;
#[path = "max_message_bytes/wire.rs"]
mod wire;
