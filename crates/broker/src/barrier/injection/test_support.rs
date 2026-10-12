//! The fixtures that the injection unit tests share.
//!
//! A target partition, a marker, a fast retry configuration, and a metadata
//! source over a fixed image are each needed by more than one of the test
//! modules under this module, so the builders live in one file.

use std::sync::Arc;

use krabka_metadata::MetadataRecord;
use krabka_units::millis;

pub(super) use crate::barrier::test_support::{at, marker};
use crate::{
    barrier::{config::BarrierConfig, test_support},
    metadata_source::MetadataSource,
};

pub(super) fn fast_config() -> BarrierConfig {
    BarrierConfig {
        injection_timeout: millis(60),
        retry_backoff: millis(1),
        retry_backoff_max: millis(4),
        ..BarrierConfig::default()
    }
}

pub(super) fn source(records: &[MetadataRecord]) -> Arc<dyn MetadataSource> {
    Arc::new(test_support::metadata_source(records))
}
