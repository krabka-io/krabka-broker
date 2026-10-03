/// Which consume and reset protocol the model steps through.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum Algorithm {
    /// Production: one lock around each consume and each reset.
    Locked,
    /// The former lock-free seqlock with two compare-and-exchanges.
    SeqlockCas,
}

/// One `(rate, burst)` configuration a reset can install.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct Config {
    pub(super) rate: u64,
    pub(super) burst: u64,
}

/// Refill a consumer has claimed time for and not yet committed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct Claim {
    pub(super) refill: u64,
    /// The ghost configuration epoch the claim moved `last_refill` in.
    pub(super) epoch: u64,
}

/// A consumer's program counter. Each variant names the next shared access.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum Consumer {
    Idle,
    /// Both algorithms: read the rate without the lock or the generation.
    FastRate {
        req: u64,
    },

    /// `Locked`: take the lock.
    Acquire {
        req: u64,
    },
    /// `Locked`, lock held: read the clock.
    LockedClock {
        req: u64,
    },
    /// `Locked`, lock held: store the claimed `last_refill`.
    LockedClaim {
        req: u64,
        now: u64,
    },
    /// `Locked`, lock held: store `available`.
    LockedCommit {
        req: u64,
        claim: Claim,
    },
    /// `Locked`, lock held: release it.
    Release,

    /// `SeqlockCas`: the next access, and what this attempt has read so far.
    Seqlock {
        next: SeqStep,
        read: SeqRead,
    },
}

/// The next shared access of a `SeqlockCas` consume attempt.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum SeqStep {
    /// Read the generation; spin while it is odd.
    LoadGen,
    /// Read the rate.
    LoadRate,
    /// Read the burst.
    LoadBurst,
    /// Zero burst: re-read the generation before granting 0.
    ZeroBurstCheck,
    /// Read the clock.
    Clock,
    /// Read `last_refill`.
    LoadLast,
    /// Compare-and-exchange `last_refill` from `last` to `last + claimed`.
    ClaimCas,
    /// Read `available`.
    LoadAvail,
    /// Re-read the generation.
    GenCheck,
    /// Compare-and-exchange `available` from `cur`.
    AvailCas,
}

/// The values a `SeqlockCas` consume attempt has read or computed. A restart
/// clears every field but `req`, so a claim held by a failed attempt is gone.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub(super) struct SeqRead {
    pub(super) req: u64,
    pub(super) generation: u64,
    pub(super) rate: u64,
    pub(super) burst: u64,
    pub(super) now: u64,
    pub(super) last: u64,
    pub(super) claimed: u64,
    pub(super) refill: u64,
    pub(super) cur: u64,
    pub(super) claim: Option<Claim>,
}

impl SeqRead {
    /// A fresh attempt: the program counter of a (re)start.
    pub(super) fn restart(req: u64) -> Consumer {
        Consumer::Seqlock {
            next: SeqStep::LoadGen,
            read: Self {
                req,
                ..Self::default()
            },
        }
    }
}

impl Consumer {
    /// The claim this consumer holds, if it holds one.
    pub(super) fn claim(self) -> Option<Claim> {
        match self {
            Self::LockedCommit { claim, .. } => Some(claim),
            Self::Seqlock { read, .. } => read.claim,
            _ => None,
        }
    }
}

/// The resetter's program counter. Each variant names the next shared access.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum Resetter {
    Idle,
    /// `Locked`: take the lock.
    Acquire {
        config: Config,
    },
    /// `Locked`, lock held: read the clock and store the whole group.
    Write {
        config: Config,
    },
    /// `Locked`, lock held: publish the rate mirror.
    Publish {
        config: Config,
    },
    /// `Locked`, lock held: release it.
    Release,
    /// `SeqlockCas`: make the generation odd.
    Enter {
        config: Config,
    },
    /// `SeqlockCas`: store the rate.
    StoreRate {
        config: Config,
    },
    /// `SeqlockCas`: store the burst.
    StoreBurst {
        config: Config,
    },
    /// `SeqlockCas`: store `available`.
    StoreAvail {
        config: Config,
    },
    /// `SeqlockCas`: read the clock and store `last_refill`.
    StoreLast,
    /// `SeqlockCas`: make the generation even.
    Leave,
}
