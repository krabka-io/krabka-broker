//! The KIP-966 eligible-leader-replica projection that
//! `DescribeTopicPartitions` reads.
//!
//! ELR is the set of replicas that left the ISR while the partition still had
//! `min.insync.replicas` members, so their logs are known to be complete and
//! the controller may elect one of them without accepting data loss. Last-known
//! ELR is the ELR the partition carried when it lost its last eligible leader,
//! and it is what an operator falls back to during unclean recovery. Kafka
//! keeps both on `PartitionRegistration` and reports them on
//! `DescribeTopicPartitionsResponsePartition`; `kafka-topics --describe` prints
//! them as the `Elr:` and `LastKnownElr:` columns.
//!
//! Only `DescribeTopicPartitions` carries them. `MetadataResponsePartition`
//! has no ELR field in any version of Kafka's schema, so the Metadata API
//! answers with `error_code`, `leader`, `replicas`, `isr` and
//! `offline_replicas` and nothing more; there is no encoding on that API for a
//! broker to get wrong.
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
//! Nothing in krabka writes the key yet: the controller-side ISR transitions
//! that move a replica into ELR are a separate change. Until they land every
//! partition projects as "no ELR", which is what a Kafka cluster running below
//! `eligible.leader.replicas.version=1` also reports.
//!
//! ## Nullable versus empty
//!
//! Both response fields are nullable in the schema and both default to null,
//! but a real broker never sends null: `KRaftMetadataCache` builds them with
//! `Replicas.toList`, which returns an empty list for an empty replica array.
//! The distinction is visible in the tool. `TopicCommand.PartitionDescription`
//! prints `Elr: N/A` and `LastKnownElr: N/A` for a null and prints the joined
//! ids -- empty for an empty list -- otherwise, so a null would make
//! `kafka-topics --describe` read as "this broker does not know" against a
//! cluster where the true answer is "none". `apache/kafka:4.3.1` renders
//! `Elr: ` and `LastKnownElr: ` for a healthy topic. So does krabka:
//! [`TopicElr::partition`] always returns lists, and the handler always wraps
//! them in `Some`.

use std::collections::BTreeMap;

use krabka_metadata::MetadataImage;

use crate::config_keys::ELIGIBLE_LEADER_REPLICAS;

/// One partition's ELR state, in the wire types the response uses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PartitionElr {
    /// `DescribeTopicPartitionsResponsePartition.eligible_leader_replicas`.
    pub(crate) eligible_leader_replicas: Vec<i32>,
    /// `DescribeTopicPartitionsResponsePartition.last_known_elr`.
    pub(crate) last_known_elr: Vec<i32>,
}

/// Every partition of one topic that carries ELR state.
///
/// A topic with no state parses to an empty map, and every partition then
/// projects as [`PartitionElr::default`]: two empty lists, never null.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TopicElr(BTreeMap<i32, PartitionElr>);

impl TopicElr {
    /// Read the ELR state `image` holds for `topic`.
    ///
    /// One call per topic, not per partition: the handler walks a topic's
    /// partitions in a single pass and each lookup here is a config-map hit
    /// plus a parse of the whole value.
    pub(crate) fn of_topic(image: &MetadataImage, topic: &str) -> Self {
        image
            .topic_config(topic)
            .and_then(|configs| configs.get(ELIGIBLE_LEADER_REPLICAS))
            .map_or_else(Self::default, |value| Self::parse(value))
    }

    /// Parse the [`ELIGIBLE_LEADER_REPLICAS`] config value.
    ///
    /// The grammar is
    ///
    /// ```text
    /// value := entry (";" entry)*
    /// entry := partition ":" ids ":" ids
    /// ids   := (id ("," id)*)?
    /// ```
    ///
    /// where `partition` is the partition index and each `ids` is the
    /// eligible-leader set and then the last-known set, as node ids. So
    /// `0:2,3:;4::5` says partition 0 has ELR `{2,3}` and no last-known ELR,
    /// and partition 4 has no ELR and a last-known ELR of `{5}`. Partitions
    /// with neither are left out of the value entirely.
    ///
    /// Parsing is total: a malformed entry is dropped rather than failing the
    /// request, because the alternative is a `DescribeTopicPartitions` that
    /// errors for a whole topic over a config the client cannot even see. A
    /// dropped entry reads as "no ELR", the same answer the topic gives before
    /// the controller has ever published the key.
    pub(crate) fn parse(value: &str) -> Self {
        Self(
            value
                .split(';')
                .filter(|entry| !entry.is_empty())
                .filter_map(parse_entry)
                .collect(),
        )
    }

    /// The ELR state of one partition. Absent partitions project as two empty
    /// lists, which is the "no ELR" answer Kafka gives.
    pub(crate) fn partition(&self, partition: i32) -> PartitionElr {
        self.0.get(&partition).cloned().unwrap_or_default()
    }
}

/// Parse one `partition:ids:ids` entry. `None` drops the entry.
fn parse_entry(entry: &str) -> Option<(i32, PartitionElr)> {
    let mut fields = entry.split(':');
    let partition: i32 = fields.next()?.parse().ok()?;
    let eligible_leader_replicas = parse_ids(fields.next()?)?;
    let last_known_elr = parse_ids(fields.next()?)?;
    // A fourth field means the writer used a grammar this reader does not
    // know, so the entry is not safe to project.
    if fields.next().is_some() {
        return None;
    }
    Some((
        partition,
        PartitionElr {
            eligible_leader_replicas,
            last_known_elr,
        },
    ))
}

/// Parse a possibly-empty comma-separated node-id list.
fn parse_ids(ids: &str) -> Option<Vec<i32>> {
    if ids.is_empty() {
        return Some(Vec::new());
    }
    ids.split(',').map(|id| id.parse().ok()).collect()
}

#[cfg(test)]
mod tests;
