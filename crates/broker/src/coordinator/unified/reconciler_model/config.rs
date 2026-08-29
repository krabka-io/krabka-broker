//! The bounded model configuration: the member-id pool, the partition count and
//! the epoch cap, together with the static metadata image and the coordinator
//! config that the driven code reads.
//!
//! The shape of a run lives here rather than in the model state, so that the
//! state stays the part the checker enumerates and the bounds stay the part a
//! test picks.

use super::{TOPIC, TOPIC_NAME};
use crate::coordinator::unified::{
    actor::MetadataProvider, config::NextGenConfig, reconciler::ReconcileInput,
};

/// Bounded config. It lives here, not in the state.
pub(super) struct ReconModel {
    /// Member-id pool. A member can join, leave, and rejoin.
    pub(super) pool: Vec<&'static str>,
    pub(super) partitions: i32,
    pub(super) max_epoch: i32,
}

/// Static metadata image with one topic that has `partitions` partitions.
#[derive(Debug)]
pub(super) struct ModelMetadata {
    input: ReconcileInput,
}
impl MetadataProvider for ModelMetadata {
    fn snapshot(&self) -> ReconcileInput {
        self.input.clone()
    }
}

impl ReconModel {
    pub(super) fn basic() -> Self {
        Self {
            pool: vec!["a", "b"],
            partitions: 2,
            max_epoch: 8,
        }
    }

    pub(super) fn wide() -> Self {
        Self {
            pool: vec!["a", "b", "c"],
            partitions: 2,
            max_epoch: 6,
        }
    }

    pub(super) fn metadata(&self) -> ModelMetadata {
        ModelMetadata {
            input: ReconcileInput {
                topic_id_by_name: [(TOPIC_NAME.to_string(), TOPIC)].into(),
                partitions_per_topic: [(TOPIC, self.partitions)].into(),
                ..Default::default()
            },
        }
    }
}

pub(super) fn config() -> NextGenConfig {
    NextGenConfig::default() // seeds UniformAssignor (and RangeAssignor)
}
