//! The streams topology record at key version 23.
//!
//! The value holds the topology epoch and one [`StoredSubtopology`] per
//! subtopology. A subtopology names its source topics, both exact and regex,
//! the repartition sinks it produces, and the changelog and repartition-source
//! topics the coordinator must materialize, each described by a
//! [`StoredTopicInfo`]. A [`StoredCopartitionGroup`] records which of those
//! topics must be copartitioned, by index into the subtopology's own lists.
//!
//! # Layout
//!
//! From `StreamsGroupTopologyValue.json` at Apache Kafka tag `4.3.1`, which
//! declares `"flexibleVersions": "0+"`. `Epoch` (int32), then `Subtopologies`
//! (`[]Subtopology`), whose fields are in this order: `SubtopologyId` (string),
//! `SourceTopics` (`[]string`), `SourceTopicRegex` (`[]string`),
//! `StateChangelogTopics` (`[]TopicInfo`), `RepartitionSinkTopics`
//! (`[]string`), `RepartitionSourceTopics` (`[]TopicInfo`) and
//! `CopartitionGroups` (`[]CopartitionGroup`).
//!
//! `TopicInfo` is `{Name string, Partitions int32, ReplicationFactor int16,
//! TopicConfigs []TopicConfig{key string, value string}}`, and
//! `CopartitionGroup` is three `[]int16` of indices. Every string and array is
//! compact, and every struct as well as the message ends with a tagged-field
//! count.

use bytes::{BufMut, Bytes, BytesMut};

use super::codec::{
    decode_i16_list, decode_key_value_list, encode_i16_list, encode_key_value_list,
};
use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_array_len, get_compact_string, get_string_array, put_compact_array_len,
            put_compact_string, put_empty_tagged_fields, put_string_array, skip_tagged_fields,
        },
        get_i16, get_i32,
    },
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
        put_compact_string(buf, &self.name);
        buf.put_i32(self.partitions);
        buf.put_i16(self.replication_factor);
        encode_key_value_list(buf, &self.topic_configs);
        put_empty_tagged_fields(buf);
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let name = get_compact_string(buf)?;
        let partitions = get_i32(buf)?;
        let replication_factor = get_i16(buf)?;
        let topic_configs = decode_key_value_list(buf)?;
        skip_tagged_fields(buf)?;
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
        put_empty_tagged_fields(buf);
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let group = Self {
            source_topics: decode_i16_list(buf)?,
            source_topic_regex: decode_i16_list(buf)?,
            repartition_source_topics: decode_i16_list(buf)?,
        };
        skip_tagged_fields(buf)?;
        Ok(group)
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
        put_compact_string(buf, &self.subtopology_id);
        put_string_array(buf, &self.source_topics);
        put_string_array(buf, &self.source_topic_regex);
        put_compact_array_len(buf, self.state_changelog_topics.len());
        for t in &self.state_changelog_topics {
            t.encode_into(buf);
        }
        put_string_array(buf, &self.repartition_sink_topics);
        put_compact_array_len(buf, self.repartition_source_topics.len());
        for t in &self.repartition_source_topics {
            t.encode_into(buf);
        }
        put_compact_array_len(buf, self.copartition_groups.len());
        for cg in &self.copartition_groups {
            cg.encode_into(buf);
        }
        put_empty_tagged_fields(buf);
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let subtopology_id = get_compact_string(buf)?;
        let source_topics = get_string_array(buf)?;
        let source_topic_regex = get_string_array(buf)?;
        let scn = get_compact_array_len(buf)?;
        let mut state_changelog_topics = Vec::with_capacity(scn);
        for _ in 0..scn {
            state_changelog_topics.push(StoredTopicInfo::decode_from(buf)?);
        }
        let repartition_sink_topics = get_string_array(buf)?;
        let rsn = get_compact_array_len(buf)?;
        let mut repartition_source_topics = Vec::with_capacity(rsn);
        for _ in 0..rsn {
            repartition_source_topics.push(StoredTopicInfo::decode_from(buf)?);
        }
        let cgn = get_compact_array_len(buf)?;
        let mut copartition_groups = Vec::with_capacity(cgn);
        for _ in 0..cgn {
            copartition_groups.push(StoredCopartitionGroup::decode_from(buf)?);
        }
        skip_tagged_fields(buf)?;
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

/// Key v23 value: the group's resolved topology, that is the epoch and the
/// subtopologies.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupTopologyValue {
    pub epoch: i32,
    pub subtopologies: Vec<StoredSubtopology>,
}

impl StreamsGroupTopologyValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        put_compact_array_len(&mut buf, self.subtopologies.len());
        for s in &self.subtopologies {
            s.encode_into(&mut buf);
        }
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let epoch = get_i32(&mut buf)?;
        let n = get_compact_array_len(&mut buf)?;
        let mut subtopologies = Vec::with_capacity(n);
        for _ in 0..n {
            subtopologies.push(StoredSubtopology::decode_from(&mut buf)?);
        }
        skip_tagged_fields(&mut buf)?;
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
    fn topology_bytes_match_kafka_schema() {
        let v = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![StoredSubtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["s".into()],
                source_topic_regex: vec![],
                repartition_sink_topics: vec![],
                state_changelog_topics: vec![StoredTopicInfo {
                    name: "c".into(),
                    partitions: 0,
                    replication_factor: 3,
                    topic_configs: vec![],
                }],
                repartition_source_topics: vec![],
                copartition_groups: vec![],
            }],
        };
        let mut want: Vec<u8> = vec![0x00, 0x00];
        want.extend_from_slice(&1i32.to_be_bytes()); // Epoch
        want.push(0x02); // one Subtopology
        want.extend_from_slice(b"\x020"); // SubtopologyId
        want.extend_from_slice(b"\x02\x02s"); // SourceTopics
        want.push(0x01); // empty SourceTopicRegex
        want.push(0x02); // one StateChangelogTopics entry
        want.extend_from_slice(b"\x02c"); // Name
        want.extend_from_slice(&0i32.to_be_bytes()); // Partitions
        want.extend_from_slice(&3i16.to_be_bytes()); // ReplicationFactor
        want.push(0x01); // empty TopicConfigs
        want.push(0x00); // TopicInfo tagged fields
        want.push(0x01); // empty RepartitionSinkTopics
        want.push(0x01); // empty RepartitionSourceTopics
        want.push(0x01); // empty CopartitionGroups
        want.push(0x00); // Subtopology tagged fields
        want.push(0x00); // message tagged fields
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn topology_rejects_a_missing_tagged_trailer() {
        let full = StreamsGroupTopologyValue::default().encode();
        assert!(StreamsGroupTopologyValue::decode(&full[..full.len() - 1]).is_err());
    }

    #[test]
    fn topology_empty_round_trip() {
        let v = StreamsGroupTopologyValue::default();
        assert!(StreamsGroupTopologyValue::decode(&v.encode()).unwrap() == v);
    }
}
