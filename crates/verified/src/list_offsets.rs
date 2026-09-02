//! End-to-end `ListOffsets` request and visibility decisions.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
use creusot_std::prelude::ensures;

/// The meaning of one `ListOffsets` request timestamp at its wire version.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ListOffsetsKind {
    Unsupported,
    Earliest,
    Latest,
    MaxTimestamp,
    EarliestLocal,
    LatestTiered,
    EarliestPendingUpload,
    Timestamp,
}

/// Classify every timestamp sentinel, including its first supported version.
#[must_use]
#[ensures(match result {
    ListOffsetsKind::Earliest => timestamp@ == -2,
    ListOffsetsKind::Latest => timestamp@ == -1,
    ListOffsetsKind::MaxTimestamp => timestamp@ == -3 && version@ >= 7,
    ListOffsetsKind::EarliestLocal => timestamp@ == -4 && version@ >= 8,
    ListOffsetsKind::LatestTiered => timestamp@ == -5 && version@ >= 9,
    ListOffsetsKind::EarliestPendingUpload => timestamp@ == -6 && version@ >= 11,
    ListOffsetsKind::Timestamp => timestamp@ >= 0,
    ListOffsetsKind::Unsupported => timestamp@ < -6
        || (timestamp@ == -6 && version@ < 11)
        || (timestamp@ == -5 && version@ < 9)
        || (timestamp@ == -4 && version@ < 8)
        || (timestamp@ == -3 && version@ < 7),
})]
pub const fn list_offsets_kind(timestamp: i64, version: i16) -> ListOffsetsKind {
    match timestamp {
        -2 => ListOffsetsKind::Earliest,
        -1 => ListOffsetsKind::Latest,
        -3 if version >= 7 => ListOffsetsKind::MaxTimestamp,
        -4 if version >= 8 => ListOffsetsKind::EarliestLocal,
        -5 if version >= 9 => ListOffsetsKind::LatestTiered,
        -6 if version >= 11 => ListOffsetsKind::EarliestPendingUpload,
        value if value >= 0 => ListOffsetsKind::Timestamp,
        _ => ListOffsetsKind::Unsupported,
    }
}

/// KIP-320 leader-epoch admission for one partition row.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ListOffsetsEpochDecision {
    RejectMalformed,
    Proceed,
    Fenced,
    Unknown,
}

/// Fence every asserted epoch except the exact `-1` no-epoch sentinel.
#[must_use]
#[ensures((result == ListOffsetsEpochDecision::RejectMalformed) == (current_epoch@ < 0))]
#[ensures((result == ListOffsetsEpochDecision::Proceed) == (current_epoch@ >= 0
    && (requested_epoch@ == -1 || requested_epoch@ == current_epoch@)))]
#[ensures((result == ListOffsetsEpochDecision::Fenced) == (current_epoch@ >= 0
    && requested_epoch@ != -1 && requested_epoch@ < current_epoch@))]
#[ensures((result == ListOffsetsEpochDecision::Unknown) == (current_epoch@ >= 0
    && requested_epoch@ > current_epoch@))]
pub const fn list_offsets_epoch_decision(
    requested_epoch: i32,
    current_epoch: i32,
) -> ListOffsetsEpochDecision {
    if current_epoch < 0 {
        ListOffsetsEpochDecision::RejectMalformed
    } else if requested_epoch == -1 || requested_epoch == current_epoch {
        ListOffsetsEpochDecision::Proceed
    } else if requested_epoch < current_epoch {
        ListOffsetsEpochDecision::Fenced
    } else {
        ListOffsetsEpochDecision::Unknown
    }
}

/// The offsets that select one request's isolation bound.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ListOffsetsBoundFacts {
    pub replica_id: i32,
    pub isolation_level: i8,
    pub log_end: i64,
    pub high_watermark: i64,
    pub last_stable: i64,
}

/// A valid last-fetchable offset, or a fail-closed malformed-state result.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ListOffsetsBoundDecision {
    RejectMalformed,
    Bound { offset: i64 },
}

/// Select LEO for replicas, HWM for ordinary consumers, and `min(LSO, HWM)`
/// for read-committed consumers.
#[must_use]
#[ensures(match result {
    ListOffsetsBoundDecision::RejectMalformed => {
        (facts.replica_id@ != -1 && facts.log_end@ < 0)
            || (facts.replica_id@ == -1 && facts.high_watermark@ < 0)
            || (facts.replica_id@ == -1 && facts.isolation_level@ == 1
                && facts.last_stable@ < 0)
    }
    ListOffsetsBoundDecision::Bound { offset } => {
        offset@ >= 0
            && (facts.replica_id@ != -1 ==> offset@ == facts.log_end@)
            && (facts.replica_id@ == -1 && facts.isolation_level@ != 1
                ==> offset@ == facts.high_watermark@)
            && (facts.replica_id@ == -1 && facts.isolation_level@ == 1
                ==> offset@ <= facts.high_watermark@
                    && offset@ <= facts.last_stable@)
    }
})]
pub const fn list_offsets_bound_decision(facts: ListOffsetsBoundFacts) -> ListOffsetsBoundDecision {
    if (facts.replica_id != -1 && facts.log_end < 0)
        || (facts.replica_id == -1 && facts.high_watermark < 0)
        || (facts.replica_id == -1 && facts.isolation_level == 1 && facts.last_stable < 0)
    {
        return ListOffsetsBoundDecision::RejectMalformed;
    }
    let offset = if facts.replica_id != -1 {
        facts.log_end
    } else if facts.isolation_level == 1 {
        if facts.last_stable < facts.high_watermark {
            facts.last_stable
        } else {
            facts.high_watermark
        }
    } else {
        facts.high_watermark
    };
    ListOffsetsBoundDecision::Bound { offset }
}

/// Local and optional cold-tier candidates for the `EARLIEST` sentinel.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ListOffsetsEarliestFacts {
    pub local: i64,
    pub has_remote: bool,
    pub remote: i64,
    pub has_diskless: bool,
    pub diskless: i64,
}

/// Select the equal logical minimum across every available tier.
#[must_use]
#[ensures(match result {
    None => facts.local@ < 0
        || (facts.has_remote && facts.remote@ < 0)
        || (facts.has_diskless && facts.diskless@ < 0),
    Some(offset) => facts.local@ >= 0
        && (!facts.has_remote || facts.remote@ >= 0)
        && (!facts.has_diskless || facts.diskless@ >= 0)
        && offset@ <= facts.local@
        && (!facts.has_remote || offset@ <= facts.remote@)
        && (!facts.has_diskless || offset@ <= facts.diskless@)
        && (offset@ == facts.local@
            || (facts.has_remote && offset@ == facts.remote@)
            || (facts.has_diskless && offset@ == facts.diskless@)),
})]
pub const fn list_offsets_earliest(facts: ListOffsetsEarliestFacts) -> Option<i64> {
    if facts.local < 0
        || (facts.has_remote && facts.remote < 0)
        || (facts.has_diskless && facts.diskless < 0)
    {
        return None;
    }
    let local_or_remote = if facts.has_remote && facts.remote < facts.local {
        facts.remote
    } else {
        facts.local
    };
    let offset = if facts.has_diskless && facts.diskless < local_or_remote {
        facts.diskless
    } else {
        local_or_remote
    };
    Some(offset)
}

/// One tier-specific candidate before the final visibility clamp.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ListOffsetsSelectionFacts {
    pub kind: ListOffsetsKind,
    pub candidate_offset: i64,
    pub candidate_timestamp: i64,
    pub candidate_epoch: i32,
    pub last_fetchable: i64,
}

/// The final visible partition-row value.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ListOffsetsSelectionDecision {
    RejectMalformed,
    Unknown,
    Resolved {
        offset: i64,
        timestamp: i64,
        leader_epoch: i32,
    },
}

/// Apply the common final clamp after any local, diskless, or remote lookup.
///
/// Earliest sentinels are starts and remain unmeasured. Latest is the lower of
/// its candidate and the isolation bound. Every record-derived result must be
/// strictly below that bound.
#[must_use]
#[ensures(match result {
    ListOffsetsSelectionDecision::RejectMalformed => {
        facts.kind == ListOffsetsKind::Unsupported
            || facts.candidate_offset@ < -1
            || facts.candidate_epoch@ < -1
            || facts.last_fetchable@ < 0
    }
    ListOffsetsSelectionDecision::Unknown => {
        facts.kind != ListOffsetsKind::Unsupported
            && facts.candidate_offset@ >= -1
            && facts.candidate_epoch@ >= -1
            && facts.last_fetchable@ >= 0
            && (facts.candidate_offset@ == -1
                || (facts.kind != ListOffsetsKind::Earliest
                    && facts.kind != ListOffsetsKind::EarliestLocal
                    && facts.kind != ListOffsetsKind::Latest
                    && facts.candidate_offset@ >= facts.last_fetchable@))
    }
    ListOffsetsSelectionDecision::Resolved { offset, timestamp, leader_epoch } => {
        facts.kind != ListOffsetsKind::Unsupported
            && facts.candidate_offset@ >= 0
            && facts.candidate_epoch@ >= -1
            && facts.last_fetchable@ >= 0
            && offset@ >= 0
            && timestamp@ == facts.candidate_timestamp@
            && leader_epoch@ == facts.candidate_epoch@
            && (facts.kind == ListOffsetsKind::Earliest
                || facts.kind == ListOffsetsKind::EarliestLocal
                || offset@ < facts.last_fetchable@
                || (facts.kind == ListOffsetsKind::Latest
                    && offset@ == facts.last_fetchable@))
    }
})]
pub const fn list_offsets_selection_decision(
    facts: ListOffsetsSelectionFacts,
) -> ListOffsetsSelectionDecision {
    let mode = match facts.kind {
        ListOffsetsKind::Unsupported => {
            return ListOffsetsSelectionDecision::RejectMalformed;
        }
        ListOffsetsKind::Earliest | ListOffsetsKind::EarliestLocal => 0,
        ListOffsetsKind::Latest => 1,
        _ => 2,
    };
    if facts.candidate_offset < -1 || facts.candidate_epoch < -1 || facts.last_fetchable < 0 {
        return ListOffsetsSelectionDecision::RejectMalformed;
    }
    if facts.candidate_offset == -1 {
        return ListOffsetsSelectionDecision::Unknown;
    }
    let offset = if mode == 1 && facts.last_fetchable < facts.candidate_offset {
        facts.last_fetchable
    } else {
        facts.candidate_offset
    };
    if mode == 2 && offset >= facts.last_fetchable {
        return ListOffsetsSelectionDecision::Unknown;
    }
    ListOffsetsSelectionDecision::Resolved {
        offset,
        timestamp: facts.candidate_timestamp,
        leader_epoch: facts.candidate_epoch,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        ListOffsetsBoundDecision, ListOffsetsBoundFacts, ListOffsetsEarliestFacts,
        ListOffsetsEpochDecision, ListOffsetsKind, ListOffsetsSelectionDecision,
        ListOffsetsSelectionFacts, list_offsets_bound_decision, list_offsets_earliest,
        list_offsets_epoch_decision, list_offsets_kind, list_offsets_selection_decision,
    };

    #[test]
    fn sentinels_and_epochs_fail_closed_at_boundaries() {
        assert!(list_offsets_kind(-3, 6) == ListOffsetsKind::Unsupported);
        assert!(list_offsets_kind(-3, 7) == ListOffsetsKind::MaxTimestamp);
        assert!(list_offsets_kind(-6, 11) == ListOffsetsKind::EarliestPendingUpload);
        assert!(list_offsets_kind(i64::MAX, 0) == ListOffsetsKind::Timestamp);
        assert!(list_offsets_epoch_decision(-2, 3) == ListOffsetsEpochDecision::Fenced);
        assert!(list_offsets_epoch_decision(4, 3) == ListOffsetsEpochDecision::Unknown);
        assert!(list_offsets_epoch_decision(-1, 3) == ListOffsetsEpochDecision::Proceed);
    }

    #[test]
    fn bounds_and_selection_cover_isolation_tiers_and_overflow_edges() {
        use ListOffsetsBoundDecision::{Bound, RejectMalformed};
        use ListOffsetsSelectionDecision::{Resolved, Unknown};

        let facts = |replica_id, isolation_level| ListOffsetsBoundFacts {
            replica_id,
            isolation_level,
            log_end: 10,
            high_watermark: 8,
            last_stable: 6,
        };
        assert!(list_offsets_bound_decision(facts(2, 1)) == Bound { offset: 10 });
        assert!(list_offsets_bound_decision(facts(-1, 0)) == Bound { offset: 8 });
        assert!(list_offsets_bound_decision(facts(-1, 1)) == Bound { offset: 6 });
        assert!(
            list_offsets_bound_decision(ListOffsetsBoundFacts {
                high_watermark: -1,
                ..facts(-1, 0)
            }) == RejectMalformed
        );
        assert!(
            list_offsets_bound_decision(ListOffsetsBoundFacts {
                log_end: 0,
                high_watermark: 6,
                ..facts(-1, 0)
            }) == Bound { offset: 6 }
        );

        assert!(
            list_offsets_earliest(ListOffsetsEarliestFacts {
                local: 9,
                has_remote: true,
                remote: 2,
                has_diskless: true,
                diskless: 0,
            }) == Some(0)
        );
        assert!(
            list_offsets_earliest(ListOffsetsEarliestFacts {
                local: 9,
                has_remote: true,
                remote: -1,
                has_diskless: false,
                diskless: 0,
            }) == None
        );

        let selection = |kind, candidate_offset, last_fetchable| {
            list_offsets_selection_decision(ListOffsetsSelectionFacts {
                kind,
                candidate_offset,
                candidate_timestamp: -1,
                candidate_epoch: -1,
                last_fetchable,
            })
        };
        assert!(
            selection(ListOffsetsKind::Latest, 10, 6)
                == Resolved {
                    offset: 6,
                    timestamp: -1,
                    leader_epoch: -1,
                }
        );
        assert!(selection(ListOffsetsKind::Timestamp, 6, 6) == Unknown);
        assert!(
            selection(ListOffsetsKind::Earliest, 8, 0)
                == Resolved {
                    offset: 8,
                    timestamp: -1,
                    leader_epoch: -1,
                }
        );
        assert!(selection(ListOffsetsKind::Timestamp, i64::MAX, i64::MAX) == Unknown);
    }
}
