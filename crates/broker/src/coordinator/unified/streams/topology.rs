//! KIP-1071 topology resolution, task derivation, copartition validation, and
//! internal-topic management.
//!
//! A streams *topology* is a DAG of subtopologies. Each subtopology consumes
//! external *source topics*, regex-matched topics, or both. It can produce
//! *repartition sink* topics that another subtopology consumes again as
//! *repartition source* topics. It also keeps the *state changelog* topics that
//! back its local stores. A *task* is `(subtopology_id, partition)`. The number
//! of tasks for a subtopology equals its partition count, which comes from the
//! partition counts of the topics it reads.
//!
//! This module is pure. Every function is synchronous and has no side
//! effects, and each one takes a [`MetadataImage`] for topic lookups. The
//! coordinator drives the flow. [`to_stored_topology`] ingests the topology of
//! the client into a [`StreamsGroupTopologyValue`]. [`configure_topics`]
//! decides the task counts and the internal topic partition counts, and it
//! gives the status that keeps the group `NotReady`. [`internal_topic_specs`]
//! names the internal topics that the heartbeat must create.
//!
//! [`MetadataImage`]: krabka_metadata::MetadataImage
//! [`StreamsGroupTopologyValue`]: super::persistence::StreamsGroupTopologyValue

pub mod status;

mod configured;
mod internal_topics;
mod metadata_hash;
mod stored;
mod tasks;

#[cfg(test)]
mod test_support;

pub use self::{
    configured::{
        ConfigureTopicsError, ConfiguredInternalTopic, ConfiguredSubtopology, ConfiguredTopology,
        configure_topics,
    },
    internal_topics::{InternalTopicSpec, internal_topic_specs},
    metadata_hash::{metadata_hash, required_topics},
    stored::to_stored_topology,
    tasks::{partition_metadata, task_set},
};
