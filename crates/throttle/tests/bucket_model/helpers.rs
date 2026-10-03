use super::*;

pub(super) fn available_within_burst(s: &BucketState) -> bool {
    s.resetting() || s.available <= s.burst
}

pub(super) fn claimed_refill_conserved(s: &BucketState) -> bool {
    s.resetting()
        || s.available + s.granted + s.capped + s.in_flight()
            == s.base + s.rate * (s.last_refill - s.t0)
}

/// Replays `schedule` from the initial state, or returns `None` if one of its
/// actions is not enabled where it is taken.
pub(super) fn replay(model: &BucketModel, schedule: &[Vec<Act>]) -> Option<BucketState> {
    Path::from_actions(
        model,
        model.init_states().remove(0),
        schedule.concat().iter(),
    )
    .map(|path| path.last_state().clone())
}

/// Consumer `consumer` starts a consume of `req` and takes `steps` steps.
///
/// A consume that finds time to claim takes 10 steps under the seqlock (9 to
/// reach its `available` commit) and 6 under the lock, which it holds from
/// its second step to its last.
pub(super) fn consume(consumer: usize, req: u64, steps: usize) -> Vec<Act> {
    let mut acts = vec![Act::StartConsume { consumer, req }];
    acts.extend(std::iter::repeat_n(Act::StepConsumer(consumer), steps));
    acts
}

/// A reset to `configs[1]` that takes `steps` steps: 6 under the seqlock and 4
/// under the lock.
pub(super) fn reset(steps: usize) -> Vec<Act> {
    let mut acts = vec![Act::StartReset { config: 1 }];
    acts.extend(std::iter::repeat_n(Act::StepResetter, steps));
    acts
}
