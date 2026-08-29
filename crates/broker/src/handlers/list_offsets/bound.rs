//! The one offset every `ListOffsets` answer is measured against.
//!
//! Kafka's `Partition.fetchOffsetForTimestamp` opens by choosing a
//! `lastFetchableOffset` and then uses it twice over: LATEST *is* that offset,
//! and every sentinel that resolves against record data is refused when it
//! lands at or above it. [`fetch_bound`] makes that choice from the request,
//! and [`last_fetchable_offset`] reads the offset it names.

/// Request `replica_id` (-1) that marks an ordinary client. Kafka's
/// `ListOffsetsRequest.CONSUMER_REPLICA_ID`. A follower sends its own node id
/// and the offset-debugging path sends -2.
const CONSUMER_REPLICA_ID: i32 = -1;
/// Request `isolation_level` (1) asking for committed data only. Kafka's
/// `IsolationLevel.READ_COMMITTED`; 0 is `READ_UNCOMMITTED`. `Fetch` reads the
/// same field the same way.
const READ_COMMITTED: i8 = 1;

/// Which of the log's three offsets bounds this request's answers.
///
/// Kafka's `Partition.fetchOffsetForTimestamp` opens by choosing one of these
/// and names it `lastFetchableOffset`. Every answer it goes on to give either
/// is that offset or is measured against it, so the choice made here decides
/// every sentinel's answer and not only LATEST's. The three variants are the
/// three arms of its `isolationLevel match`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FetchBound {
    /// The log end offset, for a request that is not a client's. Kafka's
    /// `case None`, reached whenever it passed no isolation level down.
    LogEnd,
    /// The high watermark, for a `read_uncommitted` client.
    HighWatermark,
    /// The last stable offset, for a `read_committed` client.
    LastStable,
}

/// The bound a request's `replica_id` and `isolation_level` select, one arm per
/// line of `ReplicaManager.fetchOffset`'s `isolationLevelOpt`.
///
/// Kafka passes the isolation level down only when `replicaId ==
/// ListOffsetsRequest.CONSUMER_REPLICA_ID` (-1), and passes `None` for every
/// other `replicaId`. A follower probing its leader must not be bounded by
/// either watermark: it replicates committed and uncommitted records alike, so
/// bounding it would stall replication behind an open transaction until that
/// transaction resolved. `ListOffsetsRequest.DEBUGGING_REPLICA_ID` (-2) is not
/// -1 either, so it files with the replicas even though no follower sends it.
pub(super) const fn fetch_bound(replica_id: i32, isolation_level: i8) -> FetchBound {
    if replica_id != CONSUMER_REPLICA_ID {
        FetchBound::LogEnd
    } else if isolation_level == READ_COMMITTED {
        FetchBound::LastStable
    } else {
        FetchBound::HighWatermark
    }
}

/// Reads the offset a [`FetchBound`] names.
///
/// The high watermark keeps a client's answer inside what the ISR has
/// acknowledged, and the last stable offset additionally pins a
/// `read_committed` client in front of the oldest transaction that has not
/// resolved. Kafka's `UnifiedLog.lastStableOffset` is itself
/// `min(first offset of the oldest open transaction, high watermark)`, so the
/// minimum below is what makes [`FetchBound::LastStable`] that same value
/// rather than a first-offset that could sit above the watermark.
///
/// Both accessors are the ones [`krabka_verified::fetch_visibility`] uses to
/// compute a `Fetch` response's `last_stable_offset`, so a consumer that seeks
/// to end and then fetches from there sees one consistent end of partition by
/// construction rather than by coincidence.
///
/// The high watermark lives behind an async mutex of its own, so this is called
/// before the log mutex is taken and never under it.
pub(super) async fn last_fetchable_offset(
    partition: &crate::partition::Partition,
    bound: FetchBound,
    local_end: i64,
) -> i64 {
    match bound {
        FetchBound::LogEnd => local_end,
        FetchBound::HighWatermark => partition.high_watermark().await.0,
        FetchBound::LastStable => partition.lso().0.min(partition.high_watermark().await.0),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Kafka reads a `ListOffsets` request's `isolation_level` only when the
    /// request came from a client, so choosing the bound that decides every
    /// answer has to read `replica_id` as well as `isolation_level`. Every row
    /// here is one line of `ReplicaManager.fetchOffset`'s `isolationLevelOpt`
    /// paired with the arm of `Partition.fetchOffsetForTimestamp`'s
    /// `isolationLevel match` it lands in.
    #[test]
    fn the_bound_follows_the_replica_id_before_the_isolation_level() {
        let cases = [
            // The transactional consumer this whole path exists for. It is the
            // one shape that must stop in front of an open transaction.
            (
                "read_committed consumer",
                CONSUMER_REPLICA_ID,
                READ_COMMITTED,
                FetchBound::LastStable,
            ),
            // The same client asking for everything. It still cannot see past
            // what the ISR acknowledged, which is the divergence this bound
            // closed: the log end offset would have handed it records no
            // fetch would serve.
            (
                "read_uncommitted consumer",
                CONSUMER_REPLICA_ID,
                0,
                FetchBound::HighWatermark,
            ),
            // A follower probing its leader. Kafka discards the byte for any
            // replica id, and it has to: a replica copies uncommitted records
            // too, so bounding it would stall replication behind an open
            // transaction until that transaction resolved.
            ("follower replica", 3, READ_COMMITTED, FetchBound::LogEnd),
            // The same follower with the byte it actually sends. `None` is
            // chosen by the replica id alone, so the isolation level cannot
            // reach the answer either way.
            (
                "follower replica, read_uncommitted",
                3,
                0,
                FetchBound::LogEnd,
            ),
            // `ListOffsetsRequest.DEBUGGING_REPLICA_ID`. It is not -1, so Kafka
            // files it with the replicas rather than with the clients even
            // though no follower sends it.
            ("debugging replica", -2, READ_COMMITTED, FetchBound::LogEnd),
        ];
        for (label, replica_id, isolation_level, expected) in cases {
            check!(
                fetch_bound(replica_id, isolation_level) == expected,
                "{label}"
            );
        }
    }
}
