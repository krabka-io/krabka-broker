//! Fixtures shared by the `OffsetDelete` submodule tests: a request builder
//! and the fully-specified expected response rows.
//!
//! The response-shaping tests and the row-building tests assert against the
//! same `OffsetDeleteResponse` shapes, so the builders live here once instead
//! of being copied into each sibling test module.

use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        offset_delete_request::{
            OffsetDeleteRequest, OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
        },
        offset_delete_response::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic},
    },
};

/// Fully-specified expected partition row (no struct-update syntax).
pub(super) fn expected_row(partition_index: i32, error_code: i16) -> OffsetDeleteResponsePartition {
    OffsetDeleteResponsePartition {
        partition_index,
        error_code,
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}

/// Fully-specified expected topic row (no struct-update syntax).
pub(super) fn expected_topic(
    name: &str,
    partitions: Vec<OffsetDeleteResponsePartition>,
) -> OffsetDeleteResponseTopic {
    OffsetDeleteResponseTopic {
        name: name.to_string(),
        partitions,
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}

/// Builds a decoded `OffsetDeleteRequest` for group `g` from `(topic name,
/// partition indexes)` pairs.
pub(super) fn req_with_topics(topics: &[(&str, &[i32])]) -> OffsetDeleteRequest {
    OffsetDeleteRequest {
        group_id: "g".to_string(),
        topics: topics
            .iter()
            .map(|(n, ps)| OffsetDeleteRequestTopic {
                name: (*n).to_string(),
                partitions: ps
                    .iter()
                    .map(|p| OffsetDeleteRequestPartition {
                        partition_index: *p,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}
