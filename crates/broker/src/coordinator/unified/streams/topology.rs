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
//! This module is almost pure. Every function except [`ensure_internal_topics`]
//! is synchronous and has no side effects, and each one takes a
//! [`MetadataImage`] for topic lookups. The coordinator drives the flow.
//! [`to_stored_topology`] ingests the topology of the client into a
//! [`StreamsGroupTopologyValue`]. [`derive_tasks`] derives the task counts and
//! the external-topic partition snapshot. [`validate_topology`] validates that
//! result. [`required_internal_topics`] and [`ensure_internal_topics`]
//! materialize the internal topics that the topology needs. The coordinator
//! then reports each unsatisfied condition as a status list, which holds the
//! output of [`validate_topology`] and the internal topics that are still
//! missing.
//!
//! [`MetadataImage`]: krabka_metadata::MetadataImage
//! [`StreamsGroupTopologyValue`]: super::persistence::StreamsGroupTopologyValue

pub mod status;

mod internal_topics;
mod stored;
mod tasks;
mod validation;

#[cfg(test)]
mod test_support;

pub use self::{
    internal_topics::{InternalTopicSpec, ensure_internal_topics, required_internal_topics},
    stored::to_stored_topology,
    tasks::{DerivedTasks, derive_tasks, task_set},
    validation::validate_topology,
};
