//! The fixtures that the injection unit tests share.
//!
//! A target partition, a marker, a fast retry configuration, and a metadata
//! source over a fixed image are each needed by more than one of the test
//! modules under this module, so the builders live in one file.

use std::sync::Arc;

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataRecord;
use krabka_units::millis;

use crate::{
    barrier::{
        config::BarrierConfig, marker::BarrierMarker, state::TargetPartition,
        test_support::StaticSource,
    },
    metadata_source::MetadataSource,
};

pub(super) fn at(topic: &str, partition: i32) -> TargetPartition {
    TargetPartition {
        topic: topic.to_owned(),
        partition: PartitionIndex(partition),
    }
}

pub(super) fn marker() -> BarrierMarker {
    BarrierMarker {
        group: "orders-cut".to_owned(),
        epoch: 4,
        triggered_at: 1_724_500_000_000,
    }
}

pub(super) fn fast_config() -> BarrierConfig {
    BarrierConfig {
        injection_timeout: millis(60),
        retry_backoff: millis(1),
        retry_backoff_max: millis(4),
        ..BarrierConfig::default()
    }
}

pub(super) fn source(records: &[MetadataRecord]) -> Arc<dyn MetadataSource> {
    Arc::new(StaticSource::new(records))
}
