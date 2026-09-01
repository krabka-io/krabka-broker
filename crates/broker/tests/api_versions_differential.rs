//! Differential evidence for the advertised `ApiVersions` table.
//!
//! `ApiVersions` (api key 18) is the first call every Kafka client makes and
//! the only input to every version negotiation that follows, so a wrong row in
//! it is a wrong decision in every later request. The broker builds the
//! response from [`krabka_broker::api_catalog::supported_apis`], and the rest of
//! the JVM harness only ever proved that a client could complete the exchange,
//! never what the exchange said.
//!
//! Two cases close that:
//!
//! 1. [`cases::krabka_advertises_exactly_the_api_catalog`] reads krabka's table
//!    back with the official `kafka-broker-api-versions` tool and compares the
//!    whole parsed table against the whole catalog.
//! 2. [`cases::divergence_from_real_kafka_matches_the_expectation`] reads the
//!    same table from a pinned Apache Kafka broker and compares the outer join
//!    of the two against `tests/fixtures/api_versions/divergence.json`. A range
//!    krabka advertises that Kafka does not -- or one that has moved away from
//!    Kafka's -- then arrives as a diff on that file.
//!
//! That file is also what `tools/generate-kip-matrix.py` reads for the version
//! columns of `docs/KIP_MATRIX.md`, so the matrix and the tests cannot disagree.
//!
//! Both cases are `#[ignore]`d because they need Docker, and the Bazel lane
//! that owns this suite runs it with `--ignored`. The unit tests over the output
//! reader in [`parse`], the join in [`divergence`] and the readiness deadlines
//! in [`probe`] therefore live under a second root,
//! `api_versions_differential_offline`, which names the same three modules and
//! runs in the ordinary lane.
//!
//! ```text
//! cargo test -p krabka-broker --test api_versions_differential -- --ignored
//! ```
//!
//! Cargo compiles this file as its own test binary, so a `mod` declaration in
//! it resolves against `tests/` rather than against a directory named for the
//! file. Each child therefore carries an explicit `#[path]` onto the sibling
//! `api_versions_differential/` directory; `support` is a `tests/<name>/mod.rs`
//! helper, which the crate-root rule already resolves.

#[path = "api_versions_differential/cases.rs"]
mod cases;
#[path = "api_versions_differential/divergence.rs"]
mod divergence;
#[path = "api_versions_differential/oracle.rs"]
mod oracle;
#[path = "api_versions_differential/parse.rs"]
mod parse;
#[path = "api_versions_differential/probe.rs"]
mod probe;
mod support;
