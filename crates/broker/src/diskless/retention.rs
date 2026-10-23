//! Retention and topic-deletion policy for the committed diskless index.

use {
    super::{flusher::FlushPartition, index_log::DisklessIndexLog},
    crate::{
        error::BrokerError,
        freeze::resolve::{FreezeMutationResolution, resolve_freeze_mutation},
        metrics::BrokerMetrics,
    },
    krabka_metadata::MetadataImage,
    krabka_units::convert::{ByteSizeExt, TimeExt},
    krabka_verified::{FreezeMutationKind, diskless::DisklessRetentionPolicy},
};

pub(super) struct Retention<'a> {
    pub(super) index_log: &'a DisklessIndexLog,
    pub(super) metrics: &'a BrokerMetrics,
}

impl Retention<'_> {
    pub(super) async fn tombstone_deleted_topics(
        &self,
        image: &MetadataImage,
    ) -> Result<(), BrokerError> {
        let topic_ids = self.index_log.cache().lock().await.topic_ids();
        for topic_id in topic_ids {
            if image.topic_by_id(&topic_id).is_none() {
                self.index_log.tombstone_topic(topic_id).await?;
                let floors = self
                    .index_log
                    .cache()
                    .lock()
                    .await
                    .delete_floor_partitions(topic_id);
                for partition in floors {
                    self.index_log
                        .tombstone_delete_floor(topic_id, partition)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Tombstone the index ranges each led partition's retention has expired.
    ///
    /// Kafka runs the same three predicates on every `LogManager` cleanup tick,
    /// against both the local segments and, on a tiered topic, the remote ones.
    /// On a diskless topic the object tier is the only tier that holds the
    /// records, so the tombstone is what makes retention mean anything at all.
    ///
    /// The object itself is freed by the reclaimer on a later sweep, and only once
    /// no range in it is referenced. One object holds runs from several
    /// partitions, so a partition whose ranges all expire can still leave the
    /// object in the bucket until its co-tenants expire too.
    ///
    /// KFC-9: a write freeze stops this pass, as it stops the cleaner and both of
    /// the remote-log-manager's retention passes. A frozen topic's prefix has to
    /// stay byte-identical, and expiring a range here would let the reclaimer
    /// delete the object bytes behind it. A thaw makes the next tick eligible
    /// again with no further operator step.
    pub(super) async fn expire_retention_breached_ranges(
        &self,
        image: &MetadataImage,
        partitions: &[FlushPartition],
        now_ms: i64,
    ) -> Result<(), BrokerError> {
        for partition in partitions {
            if matches!(
                resolve_freeze_mutation(
                    image,
                    &partition.handle.topic,
                    true,
                    FreezeMutationKind::Retention,
                ),
                FreezeMutationResolution::Frozen(_)
            ) {
                tracing::debug!(
                    topic = %partition.handle.topic,
                    partition = partition.handle.index.get(),
                    "diskless WAL retention held by a write freeze"
                );
                continue;
            }
            let config = partition
                .handle
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .config_snapshot();
            let retention_ms = config.retention.map(TimeExt::millis_i64_trunc);
            let retention_bytes = config.retention_size.map(ByteSizeExt::bytes_u64);
            let index = partition.handle.index.get();
            let keys = {
                let cache = self.index_log.cache();
                let cache = cache.lock().await;
                // The `DeleteRecords` floor, not the local log start: the flusher
                // trims the local log behind the committed index on every tick, so
                // the log start says only how much of the WAL has reached the
                // bucket, never what an operator asked to delete.
                let log_start_offset = cache.delete_floor(partition.topic_id, index);
                cache.retention_expired_keys(partition.topic_id, index, DisklessRetentionPolicy {
                    retention_ms,
                    retention_bytes,
                    log_start_offset,
                    now_ms,
                })
            };
            if keys.is_empty() {
                continue;
            }
            self.index_log.tombstone_ranges(&keys).await?;
            self.metrics
                .diskless_wal_expired_ranges_total
                .inc_by(u64::try_from(keys.len()).unwrap_or(u64::MAX));
        }
        Ok(())
    }
}
