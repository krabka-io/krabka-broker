//! The producer state that `DescribeProducers` reports and that log compaction
//! keeps, and the expiry of the producers that stopped writing.
//!
//! Kafka keeps one producer state for each partition log. Every replica
//! updates it for each batch that it appends: a leader for a client batch, and
//! a follower for a batch that it replicates. `ReplicaManager
//! .activeProducerState` answers `DescribeProducers` from that state on every
//! replica that hosts the partition, through `Partition.activeProducerState`
//! and `UnifiedLog.activeProducers`. The log cleaner of every replica reads the
//! same state: `Cleaner.cleanSegments` takes `UnifiedLog
//! .lastRecordsOfActiveProducers`. `ProducerStateManager
//! .removeExpiredProducers` removes a producer when `producer.id.expiration.ms`
//! has passed since its last write and it has no open transaction.

use {
    super::Log,
    crate::compact::ProducerLastRecord,
    krabka_ids::{Offset, ProducerId},
    std::collections::HashMap,
};

/// One producer as `DescribeProducers` reports it.
///
/// This is Kafka's `DescribeProducersResponseData.ProducerState`, which
/// `UnifiedLog.activeProducers` fills from a `ProducerStateEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveProducer {
    pub producer_id: ProducerId,
    pub producer_epoch: i16,
    /// The last sequence of the last data batch of the producer at
    /// `producer_epoch`, or `-1` when the log holds no such batch.
    pub last_sequence: i32,
    /// The max timestamp of the last data batch of the producer, or the
    /// timestamp of the transaction marker that came after it.
    pub last_timestamp: i64,
    /// The coordinator epoch of the last transaction marker of the producer,
    /// or `-1` before its first marker.
    pub coordinator_epoch: i32,
    /// The first offset of the open transaction of the producer, or `None`
    /// when it has no open transaction.
    pub current_txn_start_offset: Option<Offset>,
}

impl Log {
    /// Every producer that this log holds state for, in producer id order.
    ///
    /// This is Kafka's `UnifiedLog.activeProducers`. An append from a client,
    /// a replicated append and a reopen all update the same state, so a
    /// leader and its followers give the same answer at the same log end.
    #[must_use]
    pub fn active_producers(&self) -> Vec<ActiveProducer> { self.producers.active_producers() }

    /// The last record of every producer that this log holds state for. A
    /// compaction pass keeps that record, so that the producer state
    /// survives the pass.
    ///
    /// This is Kafka's `UnifiedLog.lastRecordsOfActiveProducers`: the last
    /// offset of the last data batch of the producer, and its epoch. A
    /// producer has no last data offset when a transaction marker at a new
    /// epoch has cleared its batches (transaction version 2), or when it has
    /// written only markers. Every append path updates this state, so a
    /// leader and a follower give the same answer at the same log end.
    pub(crate) fn last_records_of_active_producers(
        &self,
    ) -> HashMap<ProducerId, ProducerLastRecord> {
        self.producers.last_records()
    }

    /// Remove every producer that has no open transaction and whose last
    /// timestamp is `expiration_ms` or more before `now_ms`. Also remove the
    /// transaction verification state that is that old.
    ///
    /// This is Kafka's `ProducerStateManager.removeExpiredProducers` with
    /// `isProducerExpired`, for `producer.id.expiration.ms`. The next
    /// producer-state snapshot does not hold a removed producer.
    pub fn remove_expired_producers(&mut self, now_ms: i64, expiration_ms: i64) {
        self.producers.expire(now_ms, expiration_ms);
    }
}

#[cfg(test)]
mod tests;
