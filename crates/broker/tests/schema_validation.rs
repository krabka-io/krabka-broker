//! KFC-7 broker-side schema validation, end to end against an in-process
//! broker and a faked Schema Registry.
//!
//! Every case drives the real Kafka wire path — `CreateTopics` and `Produce` —
//! and every rejection asserts two things: the error code the producer sees,
//! and that the partition's log end offset did not move. The second assertion
//! is the one that matters. A rejection that still appended the batch would be
//! the worst possible failure of this feature, and an error code alone does
//! not rule it out.
//!
//! Most cases also run against an unvalidated control topic, in the shape
//! KFC-1's suite established. The control half is what shows that a validated
//! topic's behaviour is its configuration and not a path every topic now
//! takes.
//!
//! # The registry is a mock, deliberately
//!
//! These cases are about what the broker does with an answer, not about
//! whether `krabka-schema-registry` gives the right one. `wiremock` serves the
//! two endpoints the broker reads, which is how the OPA authorizer's suite
//! already fakes its decision service. The registry's own conformance to
//! Confluent is asserted in that repository, against a real
//! `cp-schema-registry` container.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `schema_validation/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "schema_validation/accepted.rs"]
mod accepted;
#[path = "schema_validation/full_mode.rs"]
mod full_mode;
#[path = "schema_validation/harness.rs"]
mod harness;
#[path = "schema_validation/registry_availability.rs"]
mod registry_availability;
#[path = "schema_validation/rejected.rs"]
mod rejected;
