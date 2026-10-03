//! Exhaustive stateright interleaving model of the `krabka_throttle::TokenBucket`
//! concurrency, stepped one shared-memory access at a time.
//!
//! # What is modeled
//!
//! The state is the bucket's shared group `{rate, burst, available,
//! last_refill}`, the fast-path rate mirror, the lock, a clock, a dedicated
//! resetter thread, and a program counter per consumer thread. Each action is
//! one access to shared memory, in the order production makes it; values a
//! thread read earlier travel in its program counter. A `Tick` advances the
//! clock by one unit at any point, so a consumer's clock reading can be stale
//! by the time it acts on it.
//!
//! Two algorithms are modeled:
//!
//! * [`Algorithm::Locked`] follows production locking with whole-credit clocks.
//!   A consumer reads the rate mirror
//!   without the lock and grants a rate-0 request there. Otherwise it takes the
//!   lock, reads the clock, stores the claimed `last_refill`, stores
//!   `available`, and releases. A reset takes the lock, reads the clock, refills
//!   the old configuration to now, keeps that balance under the new burst,
//!   stores the group, publishes the mirror, and releases. A bucket with no old
//!   or no new rate starts full instead.
//! * [`Algorithm::SeqlockCas`] is the lock-free design production used before.
//!   A consumer reads the rate, then the seqlock generation (spinning while it
//!   is odd), the rate and the burst, the clock, and `last_refill`, then claims
//!   the refill with a compare-and-exchange on `last_refill`, reads
//!   `available`, re-checks the generation, and commits `available` with a
//!   second compare-and-exchange. Any failure restarts from the generation
//!   read. A reset makes the generation odd, stores rate, burst, `available`,
//!   and `last_refill` one at a time, and makes the generation even again.
//!   The runs over it are RED witnesses: they must find its two bugs.
//!
//! # Driven and modeled
//!
//! DRIVEN: the production cap-and-grant arithmetic,
//! [`krabka_verified::throttle::plan_consume`], and the whole-token request
//! cut, [`krabka_throttle::whole_token_request`], at every commit.
//!
//! MODELED: one clock unit is one second and every quantity is in the
//! bucket's storage unit, `units_per_token` to a token; production stores
//! micro-tokens. Every rate is a whole number of storage units per second, so
//! a refill is `elapsed * rate` and a consume claims the whole elapsed gap.
//! Production's part-unit remainder is covered by the unit tests in
//! `src/runtime/consume.rs`. A consume asks for whole tokens and is granted
//! whole tokens, so with `units_per_token > 1` a rate or a burst of a
//! fraction of a token leaves part tokens in the bucket, as production does
//! under a fractional quota rate. Memory is sequentially consistent: the
//! model checks the protocol, not the atomic orderings, and `Locked` needs no
//! ordering beyond what the lock gives.
//!
//! # Properties
//!
//! The ghost state records, for the configuration in force since the last
//! reset, the reset's clock reading `t0`, the balance the reset started with,
//! the tokens granted, and the tokens the burst capped away. Both safety properties are checked whenever no reset
//! is part-way through its stores; a seqlock reader never acts on such a
//! state, and under the lock nobody can see one.
//!
//! * `available_within_burst`: `available <= burst`.
//! * `grants_whole_tokens`: every grant in the configuration is a whole
//!   number of tokens, even when the bucket holds a part token.
//! * `claimed_refill_conserved`: `available + granted + capped + in_flight ==
//!   base + rate * (last_refill - t0)`. The right side is every token the
//!   configuration has made available: the balance it started with plus the
//!   refill for all the time consumers have claimed. `in_flight` is the refill a consumer
//!   has claimed time for in this configuration but not committed yet. The
//!   equation fails if a claimed refill vanishes (tokens lost) or if a commit
//!   adds tokens the configuration never made (tokens over-granted).
//!
//! Reachability witnesses show that consumers overlap, a reset overlaps a
//! consume, a claimed refill is in flight, a refill is granted and another is
//! capped, a reset shrinks the burst, the bucket drains, and a reset keeps a
//! balance under the new burst. Debt is outside the model: `try_consume`
//! never creates it, and `record` differs from a consume only in what it does
//! with the part it cannot grant, which it keeps as debt, at most what the
//! refill repays in `max_wait` for `record_bounded`. The unit tests in
//! `src/runtime/consume.rs` cover both. Fractional micro-token numerators and
//! nanosecond clock claims are also outside this bounded model; the production
//! adapter tests and the verified refill-partition composition cover them.
//!
//! # Runs
//!
//! `bucket_basic`, `bucket_wide` and `bucket_fractional` check `Locked`
//! exhaustively and pin their unique-state counts. `bucket_fractional` runs
//! at two storage units to a token, so its rates and bursts are half tokens. The RED runs check `SeqlockCas` in the smallest search
//! that shows each bug, and `locked_holds_where_seqlock_cas_fails` checks
//! `Locked` in the same searches. Two scripted schedules replay each bug step
//! by step, and show that the lock disables the same interleaving.
//!
//! An earlier version of this model made the generation check and the
//! `available` write one atomic step, tied the burst to the rate, blocked a
//! consume from starting while a reset ran, and claimed the refill with a swap
//! on an abstract `pending` counter. It could not reach either bug, and
//! production shipped both.

use krabka_throttle::whole_token_request;
use krabka_verified::throttle::{
    AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume,
};
use stateright::{Checker, Model, Path, Property};

const TARGET_STATE_COUNT: usize = 4_000_000;

const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// These pins replaced 1,352 and 7,635 when the model was rewritten to step
// like production: separate shared accesses per step, a burst apart from the
// rate, a clock and `last_refill` instead of a `pending` counter, a dedicated
// resetter over several configurations, the rate mirror and the lock, and the
// conservation ghosts. They then moved from 41,601, 222,236 and 25,984 when a
// reset began to keep the balance: a reset now lands in states with any
// carried balance, and the ghost `base` tells configurations apart by it.
const PINNED_UNIQUE_STATES_BASIC: usize = 56_259;

const PINNED_UNIQUE_STATES_WIDE: usize = 372_204;

const PINNED_UNIQUE_STATES_FRACTIONAL: usize = 33_964;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct BucketState {
    rate: u64,
    burst: u64,
    available: u64,
    last_refill: u64,
    /// `Locked`: the lock-free copy of `rate` the fast path reads.
    mirror: u64,
    /// `Locked`: whether a thread holds the lock.
    locked: bool,
    /// `SeqlockCas`: the seqlock generation.
    generation: u64,
    now: u64,
    resets: usize,
    consumers: Vec<Consumer>,
    resetter: Resetter,
    // Ghost state for the configuration in force since the last reset.
    epoch: u64,
    t0: u64,
    /// The balance the configuration started with: the burst under the first
    /// configuration and under the seqlock's resets, the balance the reset
    /// carried over under production's.
    base: u64,
    granted: u64,
    capped: u64,
}

impl BucketState {
    /// A reset has begun storing the group and has not finished.
    fn resetting(&self) -> bool {
        matches!(
            self.resetter,
            Resetter::StoreRate { .. }
                | Resetter::StoreBurst { .. }
                | Resetter::StoreAvail { .. }
                | Resetter::StoreLast
                | Resetter::Leave
        )
    }

    /// Refill claimed in the current configuration and not yet committed.
    fn in_flight(&self) -> u64 {
        self.consumers
            .iter()
            .filter_map(|c| c.claim())
            .filter(|claim| claim.epoch == self.epoch)
            .map(|claim| claim.refill)
            .sum()
    }

    /// Commits the planned consume of `req` whole tokens from `cur` plus
    /// `refill` under `burst`, in storage units, `units_per_token` to a token,
    /// recording the grant and the capped remainder in the ghosts.
    ///
    /// Production's whole-token consume: cap the refill, cut the request to
    /// the whole tokens the capped balance holds, then grant.
    fn commit(&mut self, cur: u64, refill: u64, burst: u64, req: u64, units_per_token: u64) {
        let (_, total) = plan_consume(
            AvailableTokens(cur),
            RefillTokens(refill),
            BurstCapacity(burst),
            RequestedTokens(0),
        );
        let (grant, new) = plan_consume(
            AvailableTokens(total.0),
            RefillTokens(0),
            BurstCapacity(burst),
            RequestedTokens(whole_token_request(req, total.0, units_per_token)),
        );
        self.available = new.0;
        self.granted += grant.0;
        self.capped += cur + refill - total.0;
    }

    /// Starts a new ghost configuration after a reset stored the group.
    fn start_epoch(&mut self) {
        self.epoch += 1;
        self.granted = 0;
        self.capped = 0;
    }
}

struct BucketModel {
    algorithm: Algorithm,
    /// Storage units to a token: `1` meters whole tokens, `2` half tokens.
    units_per_token: u64,
    consumers: usize,
    /// `configs[0]` is installed at start; a reset installs any of them.
    configs: Vec<Config>,
    max_resets: usize,
    max_time: u64,
    max_req: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Act {
    Tick,
    StartConsume { consumer: usize, req: u64 },
    StartReset { config: usize },
    StepConsumer(usize),
    StepResetter,
}

#[path = "bucket_model/state.rs"]
mod state;
use state::{Algorithm, Claim, Config, Consumer, Resetter, SeqRead, SeqStep};

#[path = "bucket_model/helpers.rs"]
mod helpers;
use helpers::{available_within_burst, claimed_refill_conserved, consume, replay, reset};

#[path = "bucket_model/transitions.rs"]
mod transitions;

#[path = "bucket_model/checker.rs"]
mod checker;

#[path = "bucket_model/checks.rs"]
mod checks;
use checks::{contention_config, green_run, run, straddle_config};

#[cfg(test)]
#[path = "bucket_model/tests.rs"]
mod tests;
