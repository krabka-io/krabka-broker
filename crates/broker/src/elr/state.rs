//! The published ELR value: its grammar, the projection
//! `DescribeTopicPartitions` reads, and the edit the controller applies to it.
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

use krabka_metadata::{MetadataImage, NodeId};

use crate::config_keys::ELIGIBLE_LEADER_REPLICAS;

/// One partition's ELR state, in the wire types the response uses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PartitionElr {
    /// `DescribeTopicPartitionsResponsePartition.eligible_leader_replicas`.
    pub(crate) eligible_leader_replicas: Vec<i32>,
    /// `DescribeTopicPartitionsResponsePartition.last_known_elr`.
    pub(crate) last_known_elr: Vec<i32>,
}

impl PartitionElr {
    /// `true` when the partition carries no ELR state at all. Such a
    /// partition is left out of the published value entirely.
    pub(crate) fn is_empty(&self) -> bool {
        self.eligible_leader_replicas.is_empty() && self.last_known_elr.is_empty()
    }
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

    /// Render the value back into the grammar [`Self::parse`] documents.
    ///
    /// The empty string is the value of a topic with no ELR state anywhere,
    /// and [`ElrPublisher`](super::ElrPublisher) tombstones the key rather
    /// than storing it. Entries come out in partition order because the map is
    /// ordered, so one state renders to one string and a re-publication of
    /// unchanged state compares equal to what the image already holds.
    pub(crate) fn render(&self) -> String {
        self.0
            .iter()
            .map(|(partition, elr)| {
                format!(
                    "{partition}:{}:{}",
                    render_ids(&elr.eligible_leader_replicas),
                    render_ids(&elr.last_known_elr),
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }

    /// Every partition this value carries state for, in partition order.
    ///
    /// The caller gets an owned list because the walks that use it -- taking
    /// one replica out of a whole topic's ELR, for one -- edit the value as
    /// they go.
    pub(crate) fn partitions(&self) -> Vec<i32> {
        self.0.keys().copied().collect()
    }

    /// The ELR state of one partition. Absent partitions project as two empty
    /// lists, which is the "no ELR" answer Kafka gives.
    pub(crate) fn partition(&self, partition: i32) -> PartitionElr {
        self.0.get(&partition).cloned().unwrap_or_default()
    }

    /// Replace one partition's state. A partition with neither set leaves the
    /// value, so a topic that recovers renders back to the empty string and
    /// the key is tombstoned rather than left holding `0::`.
    pub(crate) fn set_partition(&mut self, partition: i32, elr: PartitionElr) {
        if elr.is_empty() {
            self.0.remove(&partition);
        } else {
            self.0.insert(partition, elr);
        }
    }
}

/// Render one node-id list.
fn render_ids(ids: &[i32]) -> String {
    ids.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// Narrow metadata node ids to the wire type, dropping any that do not fit.
///
/// A node id wider than `i32` cannot be named in a
/// `DescribeTopicPartitionsResponsePartition`, so it could never be reported
/// even if it were published. Dropping it here keeps the published value
/// readable rather than storing an id the projection would have to invent an
/// answer for.
pub(crate) fn wire_node_ids(nodes: impl IntoIterator<Item = NodeId>) -> Vec<i32> {
    nodes
        .into_iter()
        .filter_map(|node| i32::try_from(node.0).ok())
        .collect()
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
