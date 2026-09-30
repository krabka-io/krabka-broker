//! Exhaustive stateright enumeration of the KIP-534 log-compaction retention
//! contract, driving the pure decision cores in [`super`]
//! ([`super::retain_decision`], [`super::should_index_key`],
//! [`super::compute_horizon`]). See the design spec
//! `crates/log/docs/design.md`
//! and [KIP-534](https://cwiki.apache.org/confluence/display/KAFKA/KIP-534).
//!
//! # The control-batch dedup bug
//!
//! The legacy `LogCleaner` built the key→latest-offset dedup map over *every*
//! record. That included the control-type key, a commit or abort marker, that a
//! transactional control batch carries. Two commit markers from different
//! producers share the same control-key bytes, so the cleaner treated the older
//! marker as a superseded duplicate and **deleted** it. A committed
//! transaction's data was then left with no surviving marker. A
//! `read_committed` consumer would then either re-expose aborted data or fail
//! to advance the last-stable-offset. In the fix, [`super::should_index_key`]
//! returns `false` for control batches, which keeps control batches out of the
//! dedup map completely. A marker ages out only through the KIP-534 delete
//! horizon, and only once compaction has removed all of its transaction's
//! *data*.
//!
//! # The KIP-534 retention contract
//!
//! KIP-534 repurposes record-batch attribute bit 6 as a *delete horizon*. The
//! cleaner stamps the batch with `base_timestamp = now + delete.retention.ms`
//! and bit 6 set in two cases: when a tombstone, a keyed record with a null
//! value, becomes the newest entry for its key, and when a transaction marker's
//! data is fully gone. The log keeps the record until the wall clock reaches
//! the horizon, and a later compaction then drops it. The cleaner stamps the
//! horizon exactly once and never stamps it again.
//!
//! # What this model checks
//!
//! The state is an abstract log `Vec<Entry>` and a clock. `Compact` runs one
//! pass of a [`state::Cleaner`], the production cores by default, and records
//! the log it compacted in the next state. Five `always` properties then check
//! the pass's output against its input. Each is a rule stated from the two logs
//! alone, in `invariants.rs`, so a violation comes back as a stateright
//! counterexample path.
//!
//!   1. **control-not-deduped** — a marker leaves the log only through its
//!      delete horizon: every input marker whose horizon has not elapsed is in
//!      the output.
//!   2. **marker-data-precedence** — a marker never leaves the log while the
//!      pass reads data of its own transaction in front of it, elapsed horizon
//!      or not. Kafka decides this per transaction, so a marker whose data is
//!      gone ages out even when its producer has newer live data.
//!   3. **tombstone-aging** — no surviving tombstone has an elapsed horizon,
//!      and a newest-for-key tombstone that has not reached its horizon
//!      survives.
//!   4. **idempotent-stamp** — the output is the input with entries deleted
//!      and unstamped horizons stamped to `now + delete.retention.ms`, nothing
//!      else: no existing horizon changes.
//!   5. **no-data-loss** — the set of keys whose newest value is live is
//!      unchanged: nothing is lost, and nothing superseded is resurrected.
//!
//! The legacy cleaner in `legacy.rs` indexes control keys and dedups markers
//! by them. Run through the same pass and the same checker, it breaks rules 1
//! and 2 and nothing else. That is the RED witness.

// `compact_model.rs` is pulled in with `#[path]`, which makes rustc treat it as
// a `mod.rs`: a bare `mod state;` here would look for `src/state.rs`. Each
// child therefore names its file explicitly.
#[path = "compact_model/invariants.rs"]
mod invariants;
#[path = "compact_model/legacy.rs"]
mod legacy;
#[path = "compact_model/model.rs"]
mod model;
#[path = "compact_model/pass.rs"]
mod pass;
#[path = "compact_model/runner.rs"]
mod runner;
#[path = "compact_model/state.rs"]
mod state;

/// `delete.retention.ms` used throughout the model. It is small so that
/// `clock` can overtake stamped horizons inside the bounded clock window. A
/// horizon stamped at clock `c` elapses once `clock >= c + 2`, which `clock`
/// reaches inside `max_clock`.
const DELETE_RETENTION_MS: i64 = 2;
