//! Exhaustive stateright enumeration of the KIP-534 log-compaction retention
//! contract, driving the pure decision cores in [`super`]
//! ([`super::retain_decision`], [`super::should_index_key`],
//! [`super::compute_horizon`]). See the design spec
//! `docs/superpowers/specs/2026-06-14-krabka-data-plane-safety-models-design.md`
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
//! The state is an abstract log `Vec<Entry>`. `Compact` runs the same pure
//! cores that the production rewrite path uses, builds the `next` log, and
//! asserts the five safety invariants below directly in `next_state`. A
//! violation panics, and that shows up as a stateright counterexample or a test
//! failure.
//!
//!   1. **control-not-deduped** — every distinct input marker that the pass
//!      keeps or horizon-stamps appears exactly once in the output. Two markers
//!      are never merged, and neither is ever dropped against the other.
//!   2. **marker-data-precedence** — if a producer has surviving data in the
//!      output, that producer's marker survives too, unless it has aged out.
//!   3. **tombstone-aging** — no surviving tombstone has an elapsed horizon.
//!   4. **idempotent-stamp** — once a horizon is `Some(_)`, nothing ever stamps
//!      it to a different value.
//!   5. **no-data-loss** — every key with a newest live `Data(value=Some)` in
//!      the input has a live entry in the output.
//!
//! A deliberately-broken [`legacy_retain`] reproduces the old control-dedup bug.
//! A `#[should_panic]` test proves that the control-not-deduped assert fires
//! against it. This is the RED witness.

// `compact_model.rs` is pulled in with `#[path]`, which makes rustc treat it as
// a `mod.rs`: a bare `mod state;` here would look for `src/state.rs`. Each
// child therefore names its file explicitly.
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
