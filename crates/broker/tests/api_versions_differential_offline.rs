//! The half of the `ApiVersions` differential evidence that needs no Docker.
//!
//! `api_versions_differential` boots two brokers and compares their tables, so
//! its Bazel target runs under the digest-pinned Docker lane -- which passes
//! `--ignored`, and therefore runs only the two container cases. The reader that
//! turns `kafka-broker-api-versions` output into rows, the join that turns two
//! tables into the checked-in report, and the deadlines that bound waiting for
//! the oracle to boot are ordinary code with ordinary tests, and this root
//! exists so those run in `bazel test //...` beside everything else.
//!
//! Both roots name the same three module files, so either suite type-checks a
//! change to them and this one exercises them.

#[path = "api_versions_differential/divergence.rs"]
mod divergence;
#[path = "api_versions_differential/parse.rs"]
mod parse;
#[path = "api_versions_differential/probe.rs"]
mod probe;
