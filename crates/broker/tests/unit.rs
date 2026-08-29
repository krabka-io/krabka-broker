//! Single-broker integration tests over the Kafka wire protocol.
//!
//! Each child module drives one in-process broker through the requests of one
//! concern: topic administration, cluster discovery, the record round trip,
//! the classic consumer group, the idempotent producer, and `InitProducerId`.
//! A `tests/*.rs` file is a crate root, so every child names its path.

mod support;

#[path = "unit/consumer_group.rs"]
mod consumer_group;
#[path = "unit/discovery.rs"]
mod discovery;
#[path = "unit/harness.rs"]
mod harness;
#[path = "unit/idempotent_produce.rs"]
mod idempotent_produce;
#[path = "unit/produce_fetch.rs"]
mod produce_fetch;
#[path = "unit/producer_id.rs"]
mod producer_id;
#[path = "unit/topic_admin.rs"]
mod topic_admin;
