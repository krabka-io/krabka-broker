//! KFC-1 scheduled-delivery accounting: the per-partition watermark and
//! pending-record gauges the delivery scheduler publishes after it recomputes
//! a partition.

use std::sync::Arc;

use super::{BrokerMetrics, PartitionLabel};

impl BrokerMetrics {
    /// KFC-1: publish one scheduled partition's delivery watermark and the
    /// count of records that are durable but not visible yet. Called from the
    /// delivery scheduler after it recomputes the partition. A partition whose
    /// topic delivers immediately never reaches this method, so an ordinary
    /// topic creates no series.
    pub fn record_delivery_watermark(
        &self,
        topic: &str,
        partition: i32,
        watermark: i64,
        pending: i64,
    ) {
        let lbl = PartitionLabel {
            topic: Arc::from(topic),
            partition,
        };
        self.delivery_watermark.get_or_create(&lbl).set(watermark);
        self.delivery_pending_records
            .get_or_create(&lbl)
            .set(pending);
    }
}
