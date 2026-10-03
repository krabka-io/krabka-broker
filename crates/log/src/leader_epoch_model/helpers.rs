use super::*;

/// Kafka's `Partition.readRecords` divergence gate for a fetch at
/// `fetch_offset` that carries `last_fetched_epoch`. The model's log start is
/// always 0, so the below-log-start check never fires.
pub(in super::super) fn leader_answer(
    leader: &Replica,
    fetch_offset: Offset,
    last_fetched_epoch: Option<LeaderEpoch>,
) -> FetchAnswer {
    if let Some(last_fetched_epoch) = last_fetched_epoch {
        let (epoch, end_offset) = leader.end_offset_for(last_fetched_epoch);
        if end_offset == Offset(-1) || epoch == LeaderEpoch::UNKNOWN {
            return FetchAnswer::OutOfRange;
        }
        if epoch < last_fetched_epoch || end_offset < fetch_offset {
            return FetchAnswer::Diverging { epoch, end_offset };
        }
    }
    if fetch_offset > leader.log_end() {
        return FetchAnswer::OutOfRange;
    }
    FetchAnswer::Records
}

/// Kafka's `AbstractFetcherThread.getOffsetTruncationState` for a
/// `diverging_epoch`. The leader never sends an undefined one (it answers
/// `OFFSET_OUT_OF_RANGE` instead), so those two branches are not reached.
pub(in super::super) fn follower_truncation(
    follower: &Replica,
    leader_epoch: LeaderEpoch,
    leader_end_offset: Offset,
) -> Truncation {
    let log_end = follower.log_end();
    let (follower_epoch, follower_end) = follower.end_offset_for(leader_epoch);
    if follower_end == Offset(-1) {
        return Truncation {
            offset: leader_end_offset.min(log_end),
            complete: true,
        };
    }
    if follower_epoch == leader_epoch {
        Truncation {
            offset: follower_end.min(leader_end_offset).min(log_end),
            complete: true,
        }
    } else {
        Truncation {
            offset: follower_end.min(log_end),
            complete: false,
        }
    }
}

/// The number of leading records two logs share.
pub(super) fn common_prefix(a: &[LeaderEpoch], b: &[LeaderEpoch]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

pub(super) fn is_strictly_increasing(entries: &[EpochEntry]) -> bool {
    entries
        .windows(2)
        .all(|w| w[0].epoch < w[1].epoch && w[0].start_offset < w[1].start_offset)
}
