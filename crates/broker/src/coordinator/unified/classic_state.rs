//! Classic-protocol per-`group_id` state machine (`Group`).
//!
//! This module holds pure data and transitions. The unified per-group actor
//! owns them. Committed offsets are protocol-agnostic and live on the unified
//! [`super::group::CoordinatorGroup`] container, not here.
//!
//! This file is the module root. The [`ClassicGroup`] record and its
//! rebalance-lifecycle transitions live in `group`, the membership transitions
//! in `membership`, the [`Member`] record in `member`, and the protocol vote in
//! `protocol`. [`OffsetEntry`] stays here: it is a leaf record with no
//! behaviour of its own.

use krabka_log::Offset;

mod group;
mod member;
mod membership;
mod protocol;
mod rebalance;

#[cfg(test)]
mod test_support;

pub use self::{
    group::{ClassicGroup, GroupState},
    member::{AddMemberOutcome, Member},
    protocol::select_protocol,
};

/// A committed offset entry, keyed by `(topic, partition)` in
/// [`Group::committed_offsets`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetEntry {
    pub offset: Offset,
    pub leader_epoch: i32,
    pub metadata: String,
    pub commit_timestamp_ms: i64,
    /// KIP-211: the absolute expiry a v2-v4 `OffsetCommit` asked for through
    /// `retention_time_ms`. `None` means the entry expires on the broker's
    /// `offsets.retention.minutes` instead.
    pub expire_timestamp_ms: Option<i64>,
}

impl OffsetEntry {
    /// `true` when the retention sweep may tombstone this entry at `now_ms`.
    ///
    /// A per-commit expiry wins outright, which is what Kafka's
    /// `OffsetExpirationConditionImpl.isOffsetExpired` does. Otherwise the
    /// entry expires `retention_ms` after a base timestamp, and
    /// `empty_since_ms` says which one: it is Kafka's `currentStateTimestamp`,
    /// so `None` falls back to the commit alone the way Kafka's
    /// `currentStateTimestamp.orElse(commitTimestamp)` does, and a caller that
    /// has one gets the later of it and the commit. Taking the later of the
    /// two keeps a commit made against an already-empty group for its full
    /// retention rather than expiring it on the next sweep.
    #[must_use]
    pub fn is_expired(&self, now_ms: i64, empty_since_ms: Option<i64>, retention_ms: i64) -> bool {
        if let Some(expire_timestamp_ms) = self.expire_timestamp_ms {
            return now_ms >= expire_timestamp_ms;
        }
        let base = empty_since_ms.map_or(self.commit_timestamp_ms, |empty_since_ms| {
            empty_since_ms.max(self.commit_timestamp_ms)
        });
        now_ms >= base.saturating_add(retention_ms)
    }
}

#[cfg(test)]
mod offset_entry_tests {
    use assert2::check;

    use super::*;

    fn entry(commit_timestamp_ms: i64, expire_timestamp_ms: Option<i64>) -> OffsetEntry {
        OffsetEntry {
            offset: Offset(7),
            leader_epoch: -1,
            metadata: String::new(),
            commit_timestamp_ms,
            expire_timestamp_ms,
        }
    }

    #[test]
    fn expiry_reads_the_per_commit_deadline_then_the_later_of_empty_and_commit() {
        const RETENTION_MS: i64 = 1_000;
        // (commit_ts, per-commit expiry, empty_since, now, expired?)
        let cases = [
            // No per-commit expiry: the group emptied last, so retention runs
            // from there.
            (0, None, Some(5_000), 5_999, false),
            (0, None, Some(5_000), 6_000, true),
            // The commit came after the group emptied, so retention runs from
            // the commit. A fresh commit against a long-dead group survives.
            (9_000, None, Some(0), 9_999, false),
            (9_000, None, Some(0), 10_000, true),
            // No group-empty clock at all: the commit alone decides, which is
            // what Kafka does for a simple group and for a KIP-848 group.
            (9_000, None, None, 9_999, false),
            (9_000, None, None, 10_000, true),
            // A per-commit expiry wins outright, early...
            (0, Some(10), Some(0), 9, false),
            (0, Some(10), Some(0), 10, true),
            // ...and late, past the broker retention that would have expired it.
            (0, Some(1_000_000), Some(0), 999_999, false),
        ];
        for (commit_ts, expire_ts, empty_since, now, want) in cases {
            let entry = entry(commit_ts, expire_ts);
            check!(
                entry.is_expired(now, empty_since, RETENTION_MS) == want,
                "commit_ts={commit_ts} expire_ts={expire_ts:?} empty_since={empty_since:?} now={now}"
            );
        }
    }
}

#[cfg(test)]
#[path = "classic_state_model.rs"]
mod classic_state_model;
