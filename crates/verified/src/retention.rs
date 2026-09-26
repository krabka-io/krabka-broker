//! Retention-prefix selection for the local log and the remote tier.
//!
//! Kafka deletes retained-away segments oldest first and stops at the first
//! segment it keeps, so every selection here is a contiguous prefix. The local
//! walk and the remote walk combine their predicates differently, because
//! Kafka does: `UnifiedLog.deleteOldSegments` runs separate passes over the
//! local log, while `RemoteLogManager`'s `RemoteLogRetentionHandler` checks
//! every predicate per remote segment. Each walk is proved equal to a
//! reference fold that states the Kafka rule.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Decide whether one held barrier cut falls outside the retained epoch
/// window.
///
/// A nonpositive retention count and an unrepresentable cutoff both fail
/// closed by expiring nothing.
#[ensures(result == (retained_cuts@ > 0
    && published_epoch@ - retained_cuts@ >= i64::MIN@
    && held_epoch@ <= published_epoch@ - retained_cuts@))]
#[must_use]
pub fn barrier_cut_expired(published_epoch: i64, retained_cuts: i32, held_epoch: i64) -> bool {
    if retained_cuts <= 0 {
        return false;
    }
    let retained_cuts = i64::from(retained_cuts);
    if published_epoch < i64::MIN + retained_cuts {
        return false;
    }
    held_epoch <= published_epoch - retained_cuts
}

/// What the local retention walk knows about one local segment.
#[cfg_attr(creusot, derive(Clone, Copy))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct LocalRetentionSegment {
    /// No pass may delete this segment, and every pass stops at it. The host
    /// sets it for a segment that holds a record whose delivery time has not
    /// arrived, or for a tiered segment that the remote tier does not cover
    /// whole. It plays the part of Kafka's high-watermark bound and its
    /// `isSegmentEligibleForDeletion` check.
    pub blocked: bool,
    /// The segment breaches `retention.ms`, or it lies wholly below the log
    /// start offset.
    pub expired: bool,
    /// The segment's exact size in bytes.
    pub size: u64,
}

/// What the remote retention walk knows about one finished remote segment.
#[cfg_attr(creusot, derive(Clone, Copy))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RemoteRetentionSegment {
    /// The segment's whole offset range lies below the log start offset.
    pub log_start_breached: bool,
    /// The segment breaches `retention.ms`.
    pub time_expired: bool,
    /// The segment's exact size in bytes.
    pub size: u64,
}

/// How many segments the local walk may consider: every one of them, except
/// a newest segment that is empty. That is Kafka's `deletableSegments`, which
/// never returns a last segment of size zero (`isLastSegmentAndEmpty`), so a
/// log never loses the segment it appends to without a record having left
/// with it.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn local_retention_limit(segments: Seq<LocalRetentionSegment>) -> Int {
    pearlite! {
        if segments.len() > 0 && segments[segments.len() - 1].size@ == 0 {
            segments.len() - 1
        } else {
            segments.len()
        }
    }
}

/// The reference rule for local retention, from segment `i` onwards.
///
/// Kafka's `UnifiedLog.deleteOldSegments` runs three passes over the local
/// log in this order: the log-start-offset breach, then `retention.bytes`,
/// then `retention.ms`. Each pass deletes an oldest prefix and stops at the
/// first segment its predicate rejects. The size pass
/// (`deleteRetentionSizeBreachedSegments`) deletes a segment only while
/// `diff - segment.size() >= 0`, and it runs only when the log is at least
/// its budget.
///
/// While `sizing` holds, the fold is in the size pass with `debt` bytes left
/// to reclaim. A segment that does not fit ends the size pass, and the
/// fold continues through the time pass. The time flag also carries the
/// log-start breach. That breach covers an oldest prefix, so handling it
/// inside the time pass deletes the same segments as Kafka's separate first
/// pass. A breached segment that fits the size debt is charged against it,
/// exactly as Kafka's size pass sees the log after the breach pass. A
/// breached segment that does not fit leaves Kafka's recomputed debt
/// negative, and then Kafka runs no size pass either.
///
/// The fold stops at a blocked segment and at `limit`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
#[variant(limit - i)]
pub fn local_retention_model(
    segments: Seq<LocalRetentionSegment>,
    limit: Int,
    i: Int,
    debt: Int,
    sizing: bool,
) -> Int {
    pearlite! {
        if i >= limit || i >= segments.len() || segments[i].blocked {
            i
        } else if sizing && segments[i].size@ <= debt {
            local_retention_model(segments, limit, i + 1, debt - segments[i].size@, true)
        } else if segments[i].expired {
            local_retention_model(segments, limit, i + 1, debt, false)
        } else {
            i
        }
    }
}

/// Count the oldest local segments that Kafka's local retention passes
/// delete.
///
/// `segments` is every local segment, oldest first, and the newest one is
/// the segment the log appends to: Kafka's `deletableSegments` walks the
/// active segment too. A result that covers every segment tells the host to
/// roll first and then delete them all, the way Kafka's `deleteSegments`
/// does (`if (numberOfSegments == numToDelete) roll()`); the walk never
/// takes a newest segment that is empty, so the roll always leaves a fresh
/// empty segment behind and never has to recreate the one it deletes.
///
/// `size_debt` is `None` when Kafka skips the size pass: `retention.bytes`
/// is unset, the policy does not delete, or the log is smaller than the
/// budget. Otherwise it is the log size minus the budget. The walk never
/// passes a blocked segment.
///
/// The result equals [`local_retention_model`] from the oldest segment,
/// limited by [`local_retention_limit`]. That fold states the Kafka rule.
#[ensures(result@ == match size_debt {
    None => local_retention_model(
        segments@, local_retention_limit(segments@), 0, 0, false),
    Some(debt) => local_retention_model(
        segments@, local_retention_limit(segments@), 0, debt@, true),
})]
#[must_use]
pub fn local_retention_prefix(segments: &[LocalRetentionSegment], size_debt: Option<u64>) -> usize {
    let limit = match segments.len().checked_sub(1) {
        Some(newest) if segments[newest].size == 0 => newest,
        _ => segments.len(),
    };
    let (mut debt, mut sizing) = match size_debt {
        Some(debt) => (debt, true),
        None => (0, false),
    };
    let mut len = 0usize;
    #[invariant(len@ <= limit@)]
    #[invariant(limit@ == local_retention_limit(segments@))]
    #[invariant(local_retention_model(segments@, limit@, len@, debt@, sizing) == match size_debt {
        None => local_retention_model(segments@, limit@, 0, 0, false),
        Some(initial) => local_retention_model(segments@, limit@, 0, initial@, true),
    })]
    #[variant(limit@ - len@)]
    while len < limit {
        let segment = segments[len];
        if segment.blocked {
            break;
        }
        if sizing && segment.size <= debt {
            debt -= segment.size;
        } else if segment.expired {
            sizing = false;
        } else {
            break;
        }
        len += 1;
    }
    len
}

/// The reference rule for remote retention, from segment `i` onwards, with
/// `debt` bytes of `retention.bytes` breach left to reclaim.
///
/// This is Kafka's `RemoteLogManager.cleanupExpiredRemoteLogSegments`, which
/// asks `RemoteLogRetentionHandler` about each segment in turn and stops at
/// the first one it keeps. A log-start breach deletes the segment and leaves
/// the debt alone. A `retention.ms` breach deletes it and lowers the debt by
/// its size, but not below zero. Otherwise the segment goes only when the
/// debt is positive and still covers the whole segment
/// (`isSegmentBreachedByRetentionSize`).
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
#[variant(segments.len() - i)]
pub fn remote_retention_model(segments: Seq<RemoteRetentionSegment>, i: Int, debt: Int) -> Int {
    pearlite! {
        if i >= segments.len() {
            i
        } else if segments[i].log_start_breached {
            remote_retention_model(segments, i + 1, debt)
        } else if segments[i].time_expired {
            remote_retention_model(
                segments,
                i + 1,
                if segments[i].size@ <= debt { debt - segments[i].size@ } else { 0 },
            )
        } else if debt > 0 && segments[i].size@ <= debt {
            remote_retention_model(segments, i + 1, debt - segments[i].size@)
        } else {
            i
        }
    }
}

/// Count the oldest finished remote segments that Kafka's remote retention
/// deletes.
///
/// `size_debt` is the total size minus `retention.bytes`, or zero when
/// `retention.bytes` is unset or not exceeded. A tier that accepts no delete
/// (`deletes_allowed` false) selects nothing.
///
/// The result equals [`remote_retention_model`] from the oldest segment.
/// That fold states the Kafka rule.
#[ensures(result@ == if deletes_allowed {
    remote_retention_model(segments@, 0, size_debt@)
} else {
    0
})]
#[must_use]
pub fn remote_retention_prefix(
    deletes_allowed: bool,
    segments: &[RemoteRetentionSegment],
    size_debt: u64,
) -> usize {
    if !deletes_allowed {
        return 0;
    }
    let mut debt = size_debt;
    let mut len = 0usize;
    #[invariant(len@ <= segments@.len())]
    #[invariant(remote_retention_model(segments@, len@, debt@)
        == remote_retention_model(segments@, 0, size_debt@))]
    #[variant(segments@.len() - len@)]
    while len < segments.len() {
        let segment = segments[len];
        if segment.log_start_breached {
            // Deleted below the floor; the size debt is untouched.
        } else if segment.time_expired {
            debt = debt.saturating_sub(segment.size);
        } else if debt > 0 && segment.size <= debt {
            debt -= segment.size;
        } else {
            break;
        }
        len += 1;
    }
    len
}

/// The exclusive delete-through target after an inclusive last offset.
///
/// No selected segment gives no target. An inclusive last offset of
/// `i64::MAX` has no representable successor, so it fails closed with no
/// target too.
#[ensures(match (last_offset, result) {
    (None, result) => result == None,
    (Some(last), None) => last@ == i64::MAX@,
    (Some(last), Some(target)) => last@ < i64::MAX@ && target@ == last@ + 1,
})]
#[must_use]
pub fn retention_delete_target(last_offset: Option<i64>) -> Option<i64> {
    match last_offset {
        Some(last) if last < i64::MAX => Some(last + 1),
        Some(_) | None => None,
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn barrier_cut_expiry_is_exact_and_fails_closed_at_extremes() {
        for (published, retained, held, expected) in [
            (10, 3, 6, true),
            (10, 3, 7, true),
            (10, 3, 8, false),
            (10, 0, i64::MIN, false),
            (10, -1, i64::MIN, false),
            (i64::MIN, 1, i64::MIN, false),
            (i64::MIN + 1, 1, i64::MIN, true),
            (i64::MAX, i32::MAX, i64::MAX, false),
        ] {
            check!(barrier_cut_expired(published, retained, held) == expected);
        }
    }

    /// A local segment that is neither blocked nor expired.
    const fn fresh(size: u64) -> LocalRetentionSegment {
        LocalRetentionSegment {
            blocked: false,
            expired: false,
            size,
        }
    }

    /// A local segment past `retention.ms` or below the log start.
    const fn expired(size: u64) -> LocalRetentionSegment {
        LocalRetentionSegment {
            blocked: false,
            expired: true,
            size,
        }
    }

    /// A local segment no pass may delete.
    const fn blocked(expired: bool, size: u64) -> LocalRetentionSegment {
        LocalRetentionSegment {
            blocked: true,
            expired,
            size,
        }
    }

    /// Kafka scenarios for `UnifiedLog.deleteOldSegments`: the log-start
    /// breach, `retention.bytes` and `retention.ms` passes, in that order,
    /// over every local segment with the active one last. Every expected
    /// count is worked from Kafka's passes by hand.
    #[test]
    fn local_prefix_matches_kafkas_delete_old_segments_passes() {
        /// A name, the segments (the active one last), the size debt, and how
        /// many Kafka deletes.
        type Case<'a> = (&'a str, &'a [LocalRetentionSegment], Option<u64>, usize);

        let cases: [Case<'_>; 21] = [
            ("no segments", &[], Some(100), 0),
            (
                "no pressure keeps everything",
                &[fresh(10), fresh(10)],
                None,
                0,
            ),
            (
                "a debt of 15 over three 10-byte segments deletes one, not two",
                &[fresh(10), fresh(10), fresh(10)],
                Some(15),
                1,
            ),
            (
                "a debt of exactly two segments deletes two",
                &[fresh(10), fresh(10), fresh(10)],
                Some(20),
                2,
            ),
            (
                "a debt below the oldest segment deletes nothing",
                &[fresh(10), fresh(1)],
                Some(5),
                0,
            ),
            (
                "a zero debt still deletes leading empty segments",
                &[fresh(0), fresh(0), fresh(10)],
                Some(0),
                2,
            ),
            (
                "no size pass keeps empty segments",
                &[fresh(0), fresh(10)],
                None,
                0,
            ),
            (
                "time pass deletes the expired prefix",
                &[expired(10), expired(10), fresh(10), expired(10)],
                None,
                2,
            ),
            (
                "the size pass ends at the first misfit and never resumes",
                &[expired(10), fresh(1)],
                Some(5),
                1,
            ),
            (
                "the size pass runs first and the time pass continues after it",
                &[fresh(10), expired(10), fresh(1)],
                Some(12),
                2,
            ),
            (
                "an expired segment that fits is charged to the debt",
                &[expired(3), fresh(10)],
                Some(10),
                1,
            ),
            (
                "the size pass reaches past an expired prefix",
                &[expired(3), fresh(3), fresh(10)],
                Some(6),
                2,
            ),
            (
                "a blocked segment stops every pass",
                &[expired(10), blocked(true, 10), expired(10)],
                Some(100),
                1,
            ),
            (
                "a blocked oldest segment deletes nothing",
                &[blocked(true, 10), expired(10)],
                Some(100),
                0,
            ),
            (
                "an expired log goes whole, the active segment included",
                &[expired(10), expired(10)],
                None,
                2,
            ),
            (
                "a debt that covers the whole log takes the active segment too",
                &[fresh(10), fresh(10)],
                Some(20),
                2,
            ),
            (
                "an empty active segment is never deleted",
                &[expired(10), expired(0)],
                None,
                1,
            ),
            (
                "an empty active segment stays under any debt",
                &[fresh(10), fresh(0)],
                Some(u64::MAX),
                1,
            ),
            ("a lone empty segment stays", &[expired(0)], Some(0), 0),
            (
                "a lone active segment that breaches goes",
                &[expired(1)],
                None,
                1,
            ),
            (
                "the full u64 range fits without overflow",
                &[fresh(u64::MAX), fresh(1)],
                Some(u64::MAX),
                1,
            ),
        ];
        for (name, segments, size_debt, expected) in cases {
            check!(
                local_retention_prefix(segments, size_debt) == expected,
                "{name}"
            );
        }
    }

    /// A finished remote segment with the given axes.
    const fn remote(
        log_start_breached: bool,
        time_expired: bool,
        size: u64,
    ) -> RemoteRetentionSegment {
        RemoteRetentionSegment {
            log_start_breached,
            time_expired,
            size,
        }
    }

    /// Kafka scenarios for `RemoteLogRetentionHandler`: per segment, the
    /// log-start breach, then `retention.ms`, then `retention.bytes`. Every
    /// expected count is worked from Kafka's handler by hand.
    #[test]
    fn remote_prefix_matches_kafkas_remote_log_retention_handler() {
        /// A name, the segments, the size debt, and how many Kafka deletes.
        type Case<'a> = (&'a str, &'a [RemoteRetentionSegment], u64, usize);

        let kept = remote(false, false, 10);
        let old = remote(false, true, 10);
        let breached = remote(true, false, 10);
        let cases: [Case<'_>; 12] = [
            ("no segments", &[], 100, 0),
            ("no pressure keeps everything", &[kept, kept], 0, 0),
            (
                "a debt of 15 over three 10-byte segments deletes one, not two",
                &[kept, kept, kept],
                15,
                1,
            ),
            (
                "a debt of exactly two segments deletes two",
                &[kept, kept, kept],
                20,
                2,
            ),
            (
                "a zero debt never deletes an empty segment",
                &[remote(false, false, 0)],
                0,
                0,
            ),
            (
                "a positive debt deletes an empty segment",
                &[remote(false, false, 0), kept],
                5,
                1,
            ),
            (
                "the time axis stops at the first segment in the window",
                &[old, old, kept, old],
                0,
                2,
            ),
            (
                "a time deletion lowers the debt, never below zero",
                &[remote(false, true, 30), remote(false, false, 1)],
                20,
                1,
            ),
            (
                "size resumes after a time deletion charged against the debt",
                &[old, remote(false, false, 5), kept],
                20,
                2,
            ),
            (
                "a log-start breach leaves the debt alone",
                &[breached, kept, kept],
                10,
                2,
            ),
            (
                "the breach covers what the time axis would stop at",
                &[breached, old, kept],
                0,
                2,
            ),
            (
                "the breach alone takes every breached segment",
                &[breached; 3],
                0,
                3,
            ),
        ];
        for (name, segments, size_debt, expected) in cases {
            check!(
                remote_retention_prefix(true, segments, size_debt) == expected,
                "{name}"
            );
            check!(
                remote_retention_prefix(false, segments, size_debt) == 0,
                "{name}: a tier that accepts no delete keeps everything"
            );
        }
    }

    #[test]
    fn delete_target_rejects_offset_exhaustion() {
        for (last_offset, expected) in [
            (None, None),
            (Some(9), Some(10)),
            (Some(i64::MAX - 1), Some(i64::MAX)),
            (Some(i64::MAX), None),
        ] {
            check!(retention_delete_target(last_offset) == expected);
        }
    }
}
