//! Gherkin scenarios for the audit crate's business rules.
//!
//! The features live under `tests/features/<capability>/`. Each child module
//! binds one or two capability directories to its step definitions, and holds
//! the driver those steps use. Step text must be unique across the modules:
//! every `scenarios!` call checks every step against the whole test binary.

mod ocsf;
mod verification;
mod writer;
