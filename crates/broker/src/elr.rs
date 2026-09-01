//! KIP-966 eligible leader replicas: the controller state and the projection
//! `DescribeTopicPartitions` reads back out of it.
//!
//! ELR is the set of replicas that left the ISR while the partition still had
//! `min.insync.replicas` members, so their logs are known to hold every
//! committed record and the controller may elect one of them without
//! accepting data loss. Last-known ELR is what an operator falls back to once
//! the partition has no leader at all. Kafka keeps both on
//! `PartitionRegistration` and reports them on
//! `DescribeTopicPartitionsResponsePartition`; `kafka-topics --describe`
//! prints them as the `Elr:` and `LastKnownElr:` columns.
//!
//! Only `DescribeTopicPartitions` carries them. `MetadataResponsePartition`
//! has no ELR field in any version of Kafka's schema, so the Metadata API
//! answers with `error_code`, `leader`, `replicas`, `isr` and
//! `offline_replicas` and nothing more; there is no encoding on that API for
//! a broker to get wrong.
//!
//! ## Where the state lives
//!
//! [`krabka_metadata::PartitionRecord`] lives in the protocol crate and
//! carries no ELR field, so krabka publishes the state as a controller-managed
//! topic config, exactly as it publishes broker fencing as
//! [`BROKER_FENCED`](crate::config_keys::BROKER_FENCED). The key is
//! [`ELIGIBLE_LEADER_REPLICAS`](crate::config_keys::ELIGIBLE_LEADER_REPLICAS)
//! and it holds every partition of the topic that has ELR state, in the
//! grammar [`TopicElr::parse`] documents. Publishing it through the metadata
//! log is what lets a request served by *any* node answer with the same
//! columns as one served by the controller, and it survives snapshot and
//! restore because it is an ordinary `V1TopicConfig` record.
//!
//! ## The two halves
//!
//! [`state`] parses the published value and projects one partition's two
//! lists. [`maintain`] is the controller half: every path that changes a
//! partition's ISR or leader hands its emitted records to an
//! [`ElrPublisher`], which recomputes the affected partitions and appends the
//! `V1TopicConfig` records that carry the new state.
//!
//! The columns are not the only reader. KIP-966's point is that the set is
//! *elected from*: `unclean_recovery`'s
//! [`select_leader`](crate::unclean_recovery::select_leader) reads the same
//! projection and elects a surviving ELR member ahead of any longer log that
//! is not one, because only the ELR member is known to hold every committed
//! record.
//!
//! That is also why the state has to be withdrawn when it stops being true.
//! The one event that ends a membership without any partition changing is a
//! broker returning under a new incarnation id, whose current log need not be
//! the log that made it eligible. [`records_for_restarted_broker`] withdraws
//! the published half of that, and
//! [`compute_unclean_restart_changes`](crate::leader_election::compute_unclean_restart_changes)
//! -- which is what the registration handler actually calls -- drops the
//! broker from the ISRs the next eligibility would be derived from and runs
//! [`ElrPublisher`] over the whole batch with the broker excluded, so the
//! batch cannot re-derive what it just withdrew.

pub(crate) mod maintain;
pub(crate) mod state;

#[cfg(test)]
mod tests;

pub(crate) use self::{
    maintain::{ElrPublisher, records_for_restarted_broker},
    state::TopicElr,
};
