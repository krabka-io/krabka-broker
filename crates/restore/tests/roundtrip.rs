//! End-to-end proof that `krabka-restore`'s discover/verify/bound/materialize
//! stages compose correctly against a REAL KIP-405 archive.
//!
//! Every module under `src/` already has thorough unit-level tests, built
//! against a hand-faked `SegmentInventory`/`VerifiedSegment`. This file does
//! not repeat that: it builds a real archive the same way the broker's own
//! tiered-storage copy path builds one -- a real `krabka_log::Log`, appended
//! real batches, sealed into real segments, archived through the real
//! `LocalTieredStorage::copy_log_segment_data` (the pattern
//! `crates/remote-storage/tests/jvm_tiered_storage.rs` uses to prove the JVM
//! reads a Krabka-offloaded segment) -- then drives the whole pipeline
//! through `krabka_restore::run_from_args`/`restore`, the crate's own
//! documented in-process entry points (a subprocess needs a Cargo working
//! tree, which a Bazel sandbox does not have; `crates/format/src/lib.rs`'s
//! `run_from_args` doc gives the same rationale for `krabka format`).
//!
//! The fixture spans two topics, one of them ("orders") with two partitions,
//! and one partition ("orders-0") with two archived segments, so discovery
//! has more than one topic and partition to group and materialize has to
//! continue a partition's log across a segment boundary.

#[path = "roundtrip/args.rs"]
mod args;
#[path = "roundtrip/batches.rs"]
mod batches;
#[path = "roundtrip/bootstrap_metadata.rs"]
mod bootstrap_metadata;
#[path = "roundtrip/cli_entry.rs"]
mod cli_entry;
#[path = "roundtrip/fixture.rs"]
mod fixture;
#[path = "roundtrip/partition_data.rs"]
mod partition_data;
#[path = "roundtrip/report.rs"]
mod report;
