//! End-to-end proof that the restore bound changes what a full restore
//! actually WRITES, through the whole `discover -> verify -> bound ->
//! materialize` pipeline behind [`krabka_restore::restore`] -- not just what
//! `Predicates::decide_batch`/`decide_record` decide in isolation, and not
//! just what `materialize::write_segment` does when handed a hand-built
//! `VerifiedSegment` directly.
//!
//! Every scenario archives real batches through a real `krabka_log::Log`
//! and a real `LocalTieredStorage`, the same pattern
//! `crates/remote-storage/tests/jvm_tiered_storage.rs` uses to build a
//! KIP-405 archive, then drives `restore()` and reads the restored
//! partition back with a fresh `krabka_log::Log`.

#[path = "bounds/archive.rs"]
mod archive;
#[path = "bounds/exclude_producer.rs"]
mod exclude_producer;
#[path = "bounds/exclude_record.rs"]
mod exclude_record;
#[path = "bounds/fixtures.rs"]
mod fixtures;
#[path = "bounds/harness.rs"]
mod harness;
#[path = "bounds/truncation.rs"]
mod truncation;
