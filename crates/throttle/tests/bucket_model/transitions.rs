use super::*;

impl BucketModel {
    pub(super) fn step_consumer(&self, s: &mut BucketState, t: usize) -> Option<()> {
        s.consumers[t] = match s.consumers[t] {
            Consumer::Idle => return None,
            Consumer::FastRate { req } => match self.algorithm {
                Algorithm::Locked if s.mirror == 0 => Consumer::Idle,
                Algorithm::Locked => Consumer::Acquire { req },
                Algorithm::SeqlockCas if s.rate == 0 => Consumer::Idle,
                Algorithm::SeqlockCas => SeqRead::restart(req),
            },
            Consumer::Acquire { req } => {
                if s.locked {
                    return None;
                }
                s.locked = true;
                Consumer::LockedClock { req }
            }
            Consumer::LockedClock { req } => Consumer::LockedClaim { req, now: s.now },
            Consumer::LockedClaim { req, now } => {
                if s.rate == 0 {
                    Consumer::Release
                } else {
                    let claimed = now.saturating_sub(s.last_refill);
                    s.last_refill += claimed;
                    let claim = Claim {
                        refill: claimed * s.rate,
                        epoch: s.epoch,
                    };
                    Consumer::LockedCommit { req, claim }
                }
            }
            Consumer::LockedCommit { req, claim } => {
                s.commit(
                    s.available,
                    claim.refill,
                    s.burst,
                    req,
                    self.units_per_token,
                );
                Consumer::Release
            }
            Consumer::Release => {
                s.locked = false;
                Consumer::Idle
            }
            Consumer::Seqlock { next, read } => Self::step_seqlock(s, next, read)?,
        };
        Some(())
    }

    /// One shared access of a `SeqlockCas` consume attempt, mirroring the
    /// former `try_consume` loop line by line. `None` means the access spins.
    pub(super) fn step_seqlock(
        s: &mut BucketState,
        next: SeqStep,
        mut read: SeqRead,
    ) -> Option<Consumer> {
        let restart = SeqRead::restart(read.req);
        let next = match next {
            SeqStep::LoadGen => {
                if !s.generation.is_multiple_of(2) {
                    return None;
                }
                read.generation = s.generation;
                SeqStep::LoadRate
            }
            SeqStep::LoadRate => {
                read.rate = s.rate;
                if read.rate == 0 {
                    return Some(Consumer::Idle);
                }
                SeqStep::LoadBurst
            }
            SeqStep::LoadBurst => {
                read.burst = s.burst;
                if read.burst == 0 {
                    SeqStep::ZeroBurstCheck
                } else {
                    SeqStep::Clock
                }
            }
            SeqStep::ZeroBurstCheck => {
                return Some(if s.generation == read.generation {
                    Consumer::Idle
                } else {
                    restart
                });
            }
            SeqStep::Clock => {
                read.now = s.now;
                SeqStep::LoadLast
            }
            SeqStep::LoadLast => {
                read.last = s.last_refill;
                read.claimed = read.now.saturating_sub(read.last);
                read.refill = read.claimed * read.rate;
                if read.claimed == 0 {
                    SeqStep::LoadAvail
                } else {
                    SeqStep::ClaimCas
                }
            }
            SeqStep::ClaimCas => {
                if s.last_refill != read.last {
                    return Some(restart);
                }
                s.last_refill = read.last + read.claimed;
                read.claim = Some(Claim {
                    refill: read.refill,
                    epoch: s.epoch,
                });
                SeqStep::LoadAvail
            }
            SeqStep::LoadAvail => {
                read.cur = s.available;
                SeqStep::GenCheck
            }
            SeqStep::GenCheck => {
                if s.generation != read.generation {
                    return Some(restart);
                }
                SeqStep::AvailCas
            }
            SeqStep::AvailCas => {
                if s.available != read.cur {
                    return Some(restart);
                }
                s.commit(read.cur, read.refill, read.burst, read.req, 1);
                return Some(Consumer::Idle);
            }
        };
        Some(Consumer::Seqlock { next, read })
    }

    pub(super) fn step_resetter(s: &mut BucketState) -> Option<()> {
        s.resetter = match s.resetter {
            Resetter::Idle => return None,
            Resetter::Acquire { config } => {
                if s.locked {
                    return None;
                }
                s.locked = true;
                Resetter::Write { config }
            }
            Resetter::Write { config } => {
                // Production's reset keeps the balance: the time up to now is
                // refilled at the old rate and capped at the old burst, then
                // the balance is capped at the new burst. A bucket with no old
                // or no new rate starts full.
                s.available = if s.rate == 0 || config.rate == 0 {
                    config.burst
                } else {
                    let (_, refilled) = plan_consume(
                        AvailableTokens(s.available),
                        RefillTokens(s.now.saturating_sub(s.last_refill) * s.rate),
                        BurstCapacity(s.burst),
                        RequestedTokens(0),
                    );
                    refilled.0.min(config.burst)
                };
                s.rate = config.rate;
                s.burst = config.burst;
                s.last_refill = s.now;
                s.t0 = s.now;
                s.base = s.available;
                s.start_epoch();
                Resetter::Publish { config }
            }
            Resetter::Publish { config } => {
                s.mirror = config.rate;
                Resetter::Release
            }
            Resetter::Release => {
                s.locked = false;
                Resetter::Idle
            }
            Resetter::Enter { config } => {
                s.generation += 1;
                Resetter::StoreRate { config }
            }
            Resetter::StoreRate { config } => {
                s.rate = config.rate;
                Resetter::StoreBurst { config }
            }
            Resetter::StoreBurst { config } => {
                s.burst = config.burst;
                Resetter::StoreAvail { config }
            }
            Resetter::StoreAvail { config } => {
                s.available = config.burst;
                s.base = config.burst;
                Resetter::StoreLast
            }
            Resetter::StoreLast => {
                s.last_refill = s.now;
                s.t0 = s.now;
                Resetter::Leave
            }
            Resetter::Leave => {
                s.generation += 1;
                s.start_epoch();
                Resetter::Idle
            }
        };
        Some(())
    }
}
