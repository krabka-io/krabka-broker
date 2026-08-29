//! The streams topology record at key version 17.
//!
//! The value holds the topology epoch and one [`StoredSubtopology`] per
//! subtopology. A subtopology names its source topics, both exact and regex,
//! the repartition sinks it produces, and the changelog and repartition-source
//! topics the coordinator must materialize, each described by a
//! [`StoredTopicInfo`]. A [`StoredCopartitionGroup`] records which of those
//! topics must be copartitioned, by index into the subtopology's own lists.

use bytes::{BufMut, Bytes, BytesMut};

use super::codec::{decode_i16_list, decode_string_list, encode_i16_list, encode_string_list};
use crate::{
    coordinator::unified::persistence::{get_i16, get_i32, get_string, put_string},
    error::BrokerError,
};

/// An internal, changelog, or repartition topic that a subtopology refers to.
/// It carries the partition count, the replication factor, and the per-topic
/// config overrides that the coordinator should materialize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTopicInfo {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub topic_configs: Vec<(String, String)>,
}

impl StoredTopicInfo {
    fn encode_into(&self, buf: &mut BytesMut) {
        put_string(buf, &self.name);
        buf.put_i32(self.partitions);
        buf.put_i16(self.replication_factor);
        let n = i32::try_from(self.topic_configs.len()).expect("fits");
        buf.put_i32(n);
        for (k, v) in &self.topic_configs {
            put_string(buf, k);
            put_string(buf, v);
        }
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let name = get_string(buf)?;
        let partitions = get_i32(buf)?;
        let replication_factor = get_i16(buf)?;
        let n = get_i32(buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut topic_configs = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let k = get_string(buf)?;
            let v = get_string(buf)?;
            topic_configs.push((k, v));
        }
        Ok(Self {
            name,
            partitions,
            replication_factor,
            topic_configs,
        })
    }
}

/// A copartition group. It holds the indices, into the enclosing subtopology's
/// topic lists, of the topics that must be copartitioned with one another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCopartitionGroup {
    pub source_topics: Vec<i16>,
    pub source_topic_regex: Vec<i16>,
    pub repartition_source_topics: Vec<i16>,
}

impl StoredCopartitionGroup {
    fn encode_into(&self, buf: &mut BytesMut) {
        encode_i16_list(buf, &self.source_topics);
        encode_i16_list(buf, &self.source_topic_regex);
        encode_i16_list(buf, &self.repartition_source_topics);
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        Ok(Self {
            source_topics: decode_i16_list(buf)?,
            source_topic_regex: decode_i16_list(buf)?,
            repartition_source_topics: decode_i16_list(buf)?,
        })
    }
}

/// One subtopology of a streams topology. It holds the source topics, both
/// exact and regex, the repartition sinks it produces and the repartition
/// sources it consumes, its changelog topics, and any copartition
/// constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSubtopology {
    pub subtopology_id: String,
    pub source_topics: Vec<String>,
    pub source_topic_regex: Vec<String>,
    pub repartition_sink_topics: Vec<String>,
    pub state_changelog_topics: Vec<StoredTopicInfo>,
    pub repartition_source_topics: Vec<StoredTopicInfo>,
    pub copartition_groups: Vec<StoredCopartitionGroup>,
}

impl StoredSubtopology {
    fn encode_into(&self, buf: &mut BytesMut) {
        put_string(buf, &self.subtopology_id);
        encode_string_list(buf, &self.source_topics);
        encode_string_list(buf, &self.source_topic_regex);
        encode_string_list(buf, &self.repartition_sink_topics);
        let scn = i32::try_from(self.state_changelog_topics.len()).expect("fits");
        buf.put_i32(scn);
        for t in &self.state_changelog_topics {
            t.encode_into(buf);
        }
        let rsn = i32::try_from(self.repartition_source_topics.len()).expect("fits");
        buf.put_i32(rsn);
        for t in &self.repartition_source_topics {
            t.encode_into(buf);
        }
        let cgn = i32::try_from(self.copartition_groups.len()).expect("fits");
        buf.put_i32(cgn);
        for cg in &self.copartition_groups {
            cg.encode_into(buf);
        }
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let subtopology_id = get_string(buf)?;
        let source_topics = decode_string_list(buf)?;
        let source_topic_regex = decode_string_list(buf)?;
        let repartition_sink_topics = decode_string_list(buf)?;
        let scn = get_i32(buf)?;
        let mut state_changelog_topics = Vec::with_capacity(usize::try_from(scn.max(0)).unwrap());
        for _ in 0..scn.max(0) {
            state_changelog_topics.push(StoredTopicInfo::decode_from(buf)?);
        }
        let rsn = get_i32(buf)?;
        let mut repartition_source_topics =
            Vec::with_capacity(usize::try_from(rsn.max(0)).unwrap());
        for _ in 0..rsn.max(0) {
            repartition_source_topics.push(StoredTopicInfo::decode_from(buf)?);
        }
        let cgn = get_i32(buf)?;
        let mut copartition_groups = Vec::with_capacity(usize::try_from(cgn.max(0)).unwrap());
        for _ in 0..cgn.max(0) {
            copartition_groups.push(StoredCopartitionGroup::decode_from(buf)?);
        }
        Ok(Self {
            subtopology_id,
            source_topics,
            source_topic_regex,
            repartition_sink_topics,
            state_changelog_topics,
            repartition_source_topics,
            copartition_groups,
        })
    }
}

/// Key v17 value: the group's resolved topology, that is the epoch and the
/// subtopologies.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupTopologyValue {
    pub epoch: i32,
    pub subtopologies: Vec<StoredSubtopology>,
}

impl StreamsGroupTopologyValue {
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        let n = i32::try_from(self.subtopologies.len()).expect("fits");
        buf.put_i32(n);
        for s in &self.subtopologies {
            s.encode_into(&mut buf);
        }
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let epoch = get_i32(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut subtopologies = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            subtopologies.push(StoredSubtopology::decode_from(&mut buf)?);
        }
        Ok(Self {
            epoch,
            subtopologies,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::streams::persistence::{
        KEY_STREAMS_TOPOLOGY, StreamsGroupKey, encode_topology_key, parse_streams_key,
        test_support::peek_version,
    };

    #[test]
    fn topology_round_trip() {
        let kb = encode_topology_key("g1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TOPOLOGY);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::Topology {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupTopologyValue {
            epoch: 2,
            subtopologies: vec![
                StoredSubtopology {
                    subtopology_id: "0".into(),
                    source_topics: vec!["in-a".into(), "in-b".into()],
                    source_topic_regex: vec!["^orders-.*".into()],
                    repartition_sink_topics: vec!["rp-1".into()],
                    state_changelog_topics: vec![StoredTopicInfo {
                        name: "store-changelog".into(),
                        partitions: 4,
                        replication_factor: 3,
                        topic_configs: vec![("cleanup.policy".into(), "compact".into())],
                    }],
                    repartition_source_topics: vec![StoredTopicInfo {
                        name: "rp-1".into(),
                        partitions: 4,
                        replication_factor: 3,
                        topic_configs: vec![],
                    }],
                    copartition_groups: vec![StoredCopartitionGroup {
                        source_topics: vec![0, 1],
                        source_topic_regex: vec![0],
                        repartition_source_topics: vec![0],
                    }],
                },
                StoredSubtopology {
                    subtopology_id: "1".into(),
                    source_topics: vec![],
                    source_topic_regex: vec![],
                    repartition_sink_topics: vec![],
                    state_changelog_topics: vec![],
                    repartition_source_topics: vec![],
                    copartition_groups: vec![],
                },
            ],
        };
        assert!(StreamsGroupTopologyValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn topology_empty_round_trip() {
        let v = StreamsGroupTopologyValue::default();
        assert!(StreamsGroupTopologyValue::decode(&v.encode()).unwrap() == v);
    }
}
