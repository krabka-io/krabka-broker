//! Pure `KRaft` offset-frontier and half-open-window kernels.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::*;

/// The only response-derived mutation an admitted `KRaft` Fetch may perform.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FetchResponseMutation {
    /// The response belongs to another leader, role, or epoch, or carries an
    /// error; apply nothing.
    Reject,
    /// Attach to the advertised leader and refetch from it. The responder's
    /// identity plays no part: the host reaches the leader through the
    /// leader's own `NodeEndpoints` entry or its voter-set listener.
    Discover,
    Snapshot,
    Truncate,
    Append,
    HighWatermark,
}

/// The receiving node's live fence for one Fetch response.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FetchFence {
    /// The node is an observer with no known leader.
    pub discovering: bool,
    /// The leader the live role fetches from.
    pub role_leader: Option<u64>,
    /// The leader in the durable quorum state.
    pub current_leader: Option<u64>,
    pub current_epoch: u32,
}

/// Who answered a Fetch, and which leader, epoch and error the answer names.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FetchResponseFacts {
    /// The node the request was sent to.
    pub from: u64,
    /// The response's `CurrentLeader.LeaderId`, `None` for Kafka's -1.
    pub leader: Option<u64>,
    /// The response's `CurrentLeader.LeaderEpoch`.
    pub epoch: u32,
    /// The partition's `ErrorCode` is `NONE`. A Kafka `KRaft` replica answers
    /// `NONE` only as the leader of its epoch; any other replica answers
    /// `validateLeaderOnlyRequest`'s error with the leader it knows.
    pub error_none: bool,
}

/// Which mutations the response body carries.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FetchContent {
    pub has_snapshot: bool,
    pub has_divergence: bool,
    pub has_records: bool,
}

/// The response is a successful answer from the leader this node already
/// follows, in this node's epoch: only such a response may change the log or
/// the HWM (`KafkaRaftClient.handleFetchResponse` applies content only for
/// `Errors.NONE` from the followed leader).
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn leader_fence_holds(fence: FetchFence, response: FetchResponseFacts) -> bool {
    pearlite! {
        fence.role_leader == Some(response.from)
            && fence.current_leader == Some(response.from)
            && response.leader == Some(response.from)
            && response.epoch == fence.current_epoch
            && response.error_none
    }
}

/// Kafka's `maybeHandleCommonResponse` discovery case: a response in this
/// node's epoch names a leader while the node knows none. It applies whatever
/// the error is, since a follower that knows the leader answers
/// `NOT_LEADER_OR_FOLLOWER` naming it. A newer epoch is the host's
/// `BeginQuorumEpoch` path and never reaches this kernel.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn discovery_applies(fence: FetchFence, response: FetchResponseFacts) -> bool {
    pearlite! {
        fence.discovering
            && fence.role_leader == None
            && fence.current_leader == None
            && response.epoch == fence.current_epoch
            && response.leader != None
    }
}

/// Fence a Fetch response against the live role, leader, and epoch, then
/// select exactly one mutation path.
///
/// A discovery response carries no mutation: the node attaches to the
/// advertised leader and fetches again under the leader fence, which admits
/// content only from that leader itself. So no response is ever applied on
/// the strength of who the responder is assumed to be.
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Discover)
    == discovery_applies(fence, response)))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Reject)
    == (!discovery_applies(fence, response) && !leader_fence_holds(fence, response))))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Snapshot)
    == (leader_fence_holds(fence, response) && content.has_snapshot)))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Truncate)
    == (leader_fence_holds(fence, response)
        && !content.has_snapshot
        && content.has_divergence)))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Append)
    == (leader_fence_holds(fence, response)
        && !content.has_snapshot
        && !content.has_divergence
        && content.has_records)))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::HighWatermark)
    == (leader_fence_holds(fence, response)
        && !content.has_snapshot
        && !content.has_divergence
        && !content.has_records)))]
#[must_use]
pub fn fetch_response_mutation(
    fence: FetchFence,
    response: FetchResponseFacts,
    content: FetchContent,
) -> FetchResponseMutation {
    if fence.discovering
        && fence.role_leader.is_none()
        && fence.current_leader.is_none()
        && response.epoch == fence.current_epoch
        && response.leader.is_some()
    {
        FetchResponseMutation::Discover
    } else if fence.role_leader != Some(response.from)
        || fence.current_leader != Some(response.from)
        || response.leader != Some(response.from)
        || response.epoch != fence.current_epoch
        || !response.error_none
    {
        FetchResponseMutation::Reject
    } else if content.has_snapshot {
        FetchResponseMutation::Snapshot
    } else if content.has_divergence {
        FetchResponseMutation::Truncate
    } else if content.has_records {
        FetchResponseMutation::Append
    } else {
        FetchResponseMutation::HighWatermark
    }
}

/// Advance a high watermark monotonically without passing the log end.
#[must_use]
#[cfg_attr(creusot, ensures(result@ >= previous@))]
#[cfg_attr(creusot, ensures(previous@ <= log_end@ ==> result@ <= log_end@))]
#[cfg_attr(creusot, ensures(result@ ==
    if requested@ <= log_end@ {
        if previous@ >= requested@ { previous@ } else { requested@ }
    } else if previous@ >= log_end@ { previous@ } else { log_end@ }))]
pub const fn advance_high_watermark(previous: i64, requested: i64, log_end: i64) -> i64 {
    let clamped = if requested <= log_end {
        requested
    } else {
        log_end
    };
    if previous >= clamped {
        previous
    } else {
        clamped
    }
}

/// Whether `value` lies in `[start, end)`.
#[must_use]
#[cfg_attr(creusot, ensures(result == (start@ <= value@ && value@ < end@)))]
pub const fn in_half_open_window(value: i64, start: i64, end: i64) -> bool {
    start <= value && value < end
}

/// Whether a monotonic frontier has reached a target offset.
#[must_use]
#[cfg_attr(creusot, ensures(result == (frontier@ >= target@)))]
pub const fn frontier_reaches(frontier: i64, target: i64) -> bool {
    frontier >= target
}

/// Return the length of the strictly ordered control-record prefix below a
/// half-open frontier.
#[cfg_attr(creusot, requires(forall<i: Int, j: Int>
    0 <= i && i < j && j < offsets@.len() ==> offsets@[i]@ < offsets@[j]@))]
#[cfg_attr(creusot, ensures(result@ <= offsets@.len()))]
#[cfg_attr(creusot, ensures(forall<i: Int>
    0 <= i && i < result@ ==> offsets@[i]@ < frontier@))]
#[cfg_attr(creusot, ensures(forall<i: Int>
    result@ <= i && i < offsets@.len() ==> frontier@ <= offsets@[i]@))]
#[must_use]
pub fn control_history_frontier(offsets: &[i64], frontier: i64) -> usize {
    let mut lo = 0usize;
    let mut hi = offsets.len();
    #[cfg_attr(creusot, invariant(lo@ <= hi@ && hi@ <= offsets@.len()))]
    #[cfg_attr(creusot, invariant(forall<i: Int>
        0 <= i && i < lo@ ==> offsets@[i]@ < frontier@))]
    #[cfg_attr(creusot, invariant(forall<i: Int>
        hi@ <= i && i < offsets@.len() ==> frontier@ <= offsets@[i]@))]
    #[cfg_attr(creusot, variant(hi - lo))]
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if offsets[mid] < frontier {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// The contiguous offset deltas `0, 1, ..., record_count - 1` of one metadata
/// batch, in record order; the last element is the batch's `lastOffsetDelta`.
///
/// An empty batch, and a batch whose final delta Kafka's signed 32-bit
/// `lastOffsetDelta` cannot represent, fail closed. The deltas are counted in
/// `i32` beside the vector's length rather than narrowed from a `usize`
/// index, so the one executable body is the one Creusot proves.
#[must_use]
#[cfg_attr(creusot, ensures(match result {
    Some(deltas) => record_count@ > 0
        && record_count@ <= i32::MAX@ + 1
        && deltas@.len() == record_count@
        && forall<i: Int> 0 <= i && i < deltas@.len() ==> deltas@[i]@ == i,
    None => record_count@ == 0 || record_count@ > i32::MAX@ + 1,
}))]
pub fn metadata_record_offset_deltas(record_count: usize) -> Option<Vec<i32>> {
    // `i32::MAX + 1` records have final delta `i32::MAX`.
    let max_records: usize = 0x8000_0000;
    if record_count == 0 || record_count > max_records {
        return None;
    }
    let mut deltas = Vec::with_capacity(record_count);
    deltas.push(0);
    let mut last: i32 = 0;
    #[cfg_attr(creusot, invariant(1 <= deltas@.len() && deltas@.len() <= record_count@))]
    #[cfg_attr(creusot, invariant(last@ == deltas@.len() - 1))]
    #[cfg_attr(creusot, invariant(forall<i: Int>
        0 <= i && i < deltas@.len() ==> deltas@[i]@ == i))]
    #[cfg_attr(creusot, variant(record_count@ - deltas@.len()))]
    while deltas.len() < record_count {
        // `last + 1 == deltas.len() < record_count <= i32::MAX + 1`.
        last += 1;
        deltas.push(last);
    }
    Some(deltas)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn high_watermark_is_monotonic_and_clamped() {
        for (previous, requested, log_end, expected) in
            [(2, 5, 4, 4), (2, 1, 4, 2), (2, 3, 4, 3), (5, 1, 4, 5)]
        {
            assert!(advance_high_watermark(previous, requested, log_end) == expected);
        }
    }

    #[test]
    fn windows_and_frontiers_use_exact_boundaries() {
        assert!(in_half_open_window(5, 5, 8));
        assert!(in_half_open_window(7, 5, 8));
        assert!(!in_half_open_window(4, 5, 8));
        assert!(!in_half_open_window(8, 5, 8));
        assert!(!frontier_reaches(4, 5));
        assert!(frontier_reaches(5, 5));
    }

    #[test]
    fn control_history_frontier_is_strictly_half_open() {
        let offsets = [-1, 2, 5, 9];
        for (frontier, expected) in [(-1, 0), (0, 1), (2, 1), (5, 2), (9, 3), (10, 4)] {
            assert!(control_history_frontier(&offsets, frontier) == expected);
        }
        assert!(control_history_frontier(&[], 5) == 0);
    }

    #[test]
    fn metadata_offset_deltas_are_contiguous_and_fail_closed() {
        for (record_count, expected) in [
            (0, None),
            (1, Some(vec![0])),
            (3, Some(vec![0, 1, 2])),
            // One past the largest `lastOffsetDelta` Kafka can encode.
            (0x8000_0001, None),
            (usize::MAX, None),
        ] {
            assert!(metadata_record_offset_deltas(record_count) == expected);
        }
    }

    #[test]
    fn fetch_response_is_fenced_before_one_exclusive_mutation() {
        use FetchResponseMutation::{Append, Discover, HighWatermark, Reject, Snapshot, Truncate};

        // Following leader 2 in epoch 3.
        let following = FetchFence {
            discovering: false,
            role_leader: Some(2),
            current_leader: Some(2),
            current_epoch: 3,
        };
        // A leaderless observer in epoch 3.
        let discovering = FetchFence {
            discovering: true,
            role_leader: None,
            current_leader: None,
            current_epoch: 3,
        };
        let from_leader = FetchResponseFacts {
            from: 2,
            leader: Some(2),
            epoch: 3,
            error_none: true,
        };
        // Follower 1 answers NOT_LEADER_OR_FOLLOWER naming leader 2
        // (`validateLeaderOnlyRequest`).
        let from_follower = FetchResponseFacts {
            from: 1,
            error_none: false,
            ..from_leader
        };
        let content = |has_snapshot, has_divergence, has_records| FetchContent {
            has_snapshot,
            has_divergence,
            has_records,
        };
        let everything = content(true, true, true);
        let cases = [
            (
                "snapshot wins",
                following,
                from_leader,
                everything,
                Snapshot,
            ),
            (
                "divergence",
                following,
                from_leader,
                content(false, true, true),
                Truncate,
            ),
            (
                "records",
                following,
                from_leader,
                content(false, false, true),
                Append,
            ),
            (
                "watermark only",
                following,
                from_leader,
                content(false, false, false),
                HighWatermark,
            ),
            // KafkaRaftClient.maybeHandleCommonResponse: same epoch, a leader,
            // and no known leader transitions to follower of that leader,
            // whoever answered and whatever the error.
            (
                "leader answers observer",
                discovering,
                from_leader,
                everything,
                Discover,
            ),
            (
                "follower redirects observer",
                discovering,
                from_follower,
                everything,
                Discover,
            ),
            (
                "leaderless answer to observer",
                discovering,
                FetchResponseFacts {
                    leader: None,
                    ..from_follower
                },
                everything,
                Reject,
            ),
            // An older epoch is no longer relevant; a newer one is the host's
            // BeginQuorumEpoch path.
            (
                "stale epoch while discovering",
                discovering,
                FetchResponseFacts {
                    epoch: 2,
                    ..from_leader
                },
                everything,
                Reject,
            ),
            (
                "newer epoch while discovering",
                discovering,
                FetchResponseFacts {
                    epoch: 4,
                    ..from_leader
                },
                everything,
                Reject,
            ),
            (
                "observer that knows a leader",
                FetchFence {
                    role_leader: Some(2),
                    ..discovering
                },
                from_follower,
                everything,
                Reject,
            ),
            (
                "voter without a leader",
                FetchFence {
                    discovering: false,
                    ..discovering
                },
                from_leader,
                everything,
                Reject,
            ),
            (
                "durable leader differs",
                FetchFence {
                    current_leader: Some(3),
                    ..following
                },
                from_leader,
                everything,
                Reject,
            ),
            (
                "role has no leader",
                FetchFence {
                    role_leader: None,
                    ..following
                },
                from_leader,
                everything,
                Reject,
            ),
            (
                "follower answers a follower",
                following,
                from_follower,
                everything,
                Reject,
            ),
            (
                "sender names another leader",
                following,
                FetchResponseFacts {
                    leader: Some(3),
                    ..from_leader
                },
                everything,
                Reject,
            ),
            (
                "leader answers with an error",
                following,
                FetchResponseFacts {
                    error_none: false,
                    ..from_leader
                },
                everything,
                Reject,
            ),
            (
                "newer epoch while following",
                following,
                FetchResponseFacts {
                    epoch: 4,
                    ..from_leader
                },
                everything,
                Reject,
            ),
        ];
        for (case, fence, response, body, expected) in cases {
            assert!(
                fetch_response_mutation(fence, response, body) == expected,
                "{case}"
            );
        }
    }
}
