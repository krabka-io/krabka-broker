//! The two streams records whose value is a single epoch counter.
//!
//! The group metadata record at key version 17 carries the group epoch, and
//! the target-assignment metadata record at key version 20 carries the
//! assignment epoch.
//!
//! # Layout
//!
//! From `StreamsGroupMetadataValue.json` and
//! `StreamsGroupTargetAssignmentMetadataValue.json` at Apache Kafka tag
//! `4.3.1`. Both declare `"flexibleVersions": "0+"`.
//!
//! - `StreamsGroupMetadataValue`: `Epoch` (int32) and `MetadataHash` (int64),
//!   both plain fields, then the tagged `ValidatedTopologyEpoch` (int32, tag 0,
//!   default -1) and `LastAssignmentConfigs` (tag 1, a nullable array of
//!   `LastAssignmentConfig { Key: string, Value: string }`, default null). As
//!   Kafka's generated writer does, it writes each tagged field only when it is
//!   not its default. Kafka trunk adds KIP-1331's
//!   `StoredDescriptionTopologyEpoch` (int32, tag 2, default -1) and
//!   `FailedDescriptionTopologyEpoch` (int32, tag 3, default -1), which the
//!   value keeps as [`DescriptionEpochs`]. Kafka 4.3.1 does not have them. A
//!   read always keeps them, as 4.3.1 keeps the unknown tagged fields it
//!   reads, and the writer leaves them at -1, and so out of the record,
//!   unless the topology description plugin is configured: see
//!   `ActorState::trunk_records`.
//! - `StreamsGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32),
//!   then the tagged `AssignmentTimestamp` (int64, tag 0, default 0) from
//!   KIP-1263, which Kafka's generated writer omits when it is 0.

pub use crate::coordinator::unified::streams::description::DescriptionEpochs;
use {
    crate::{
        coordinator::unified::persistence::{
            flex::{
                get_value_version, put_compact_array_len, put_compact_string,
                put_empty_tagged_fields, put_tagged_fields, read_tagged,
            },
            get_i32, get_i64,
        },
        error::BrokerError,
    },
    bytes::{BufMut, Bytes, BytesMut},
    krabka_protocol::primitives::fixed,
};

/// The tag of `ValidatedTopologyEpoch`.
const TAG_VALIDATED_TOPOLOGY_EPOCH: u32 = 0;
/// The tag of `LastAssignmentConfigs`.
const TAG_LAST_ASSIGNMENT_CONFIGS: u32 = 1;
/// The tag of Kafka trunk's `StoredDescriptionTopologyEpoch` (KIP-1331).
const TAG_STORED_DESCRIPTION_TOPOLOGY_EPOCH: u32 = 2;
/// The tag of Kafka trunk's `FailedDescriptionTopologyEpoch` (KIP-1331).
const TAG_FAILED_DESCRIPTION_TOPOLOGY_EPOCH: u32 = 3;

/// The schema default of `ValidatedTopologyEpoch`: no validated topology.
pub const NO_VALIDATED_TOPOLOGY_EPOCH: i32 = -1;

/// One `LastAssignmentConfig` of `StreamsGroupMetadataValue`: a
/// configuration parameter of the last assignment, as a key and a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastAssignmentConfig {
    pub key: String,
    pub value: String,
}

/// Key v17 value of a streams group.
///
/// It holds the streams group epoch, the metadata hash that the group last
/// configured its topology against, the topology epoch that the group
/// validated, and the assignment configuration of that epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupMetadataValue {
    pub epoch: i32,
    /// Kafka's `MetadataHash`: see
    /// [`metadata_hash`](crate::coordinator::unified::streams::topology::metadata_hash).
    pub metadata_hash: i64,
    /// Kafka's `ValidatedTopologyEpoch`: the epoch of the topology whose
    /// topics the metadata held in a valid configuration, or
    /// [`NO_VALIDATED_TOPOLOGY_EPOCH`].
    pub validated_topology_epoch: i32,
    /// Kafka's `LastAssignmentConfigs`, in the order of the record, or `None`
    /// for the null default.
    pub last_assignment_configs: Option<Vec<LastAssignmentConfig>>,
    /// Kafka trunk's KIP-1331 tags 2 and 3, each written only when it is not
    /// [`DescriptionEpochs::NONE`].
    pub description: DescriptionEpochs,
}

impl StreamsGroupMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        buf.put_i64(self.metadata_hash);
        let mut tags = Vec::new();
        if self.validated_topology_epoch != NO_VALIDATED_TOPOLOGY_EPOCH {
            tags.push((
                TAG_VALIDATED_TOPOLOGY_EPOCH,
                Bytes::copy_from_slice(&self.validated_topology_epoch.to_be_bytes()),
            ));
        }
        if let Some(configs) = &self.last_assignment_configs {
            let mut payload = BytesMut::new();
            put_compact_array_len(&mut payload, configs.len());
            for config in configs {
                put_compact_string(&mut payload, &config.key);
                put_compact_string(&mut payload, &config.value);
                put_empty_tagged_fields(&mut payload);
            }
            tags.push((TAG_LAST_ASSIGNMENT_CONFIGS, payload.freeze()));
        }
        for (tag, epoch) in [
            (
                TAG_STORED_DESCRIPTION_TOPOLOGY_EPOCH,
                self.description.stored,
            ),
            (
                TAG_FAILED_DESCRIPTION_TOPOLOGY_EPOCH,
                self.description.failed,
            ),
        ] {
            if epoch != DescriptionEpochs::NONE {
                tags.push((tag, Bytes::copy_from_slice(&epoch.to_be_bytes())));
            }
        }
        put_tagged_fields(&mut buf, tags);
        buf.freeze()
    }

    /// # Errors
    /// Returns an error when `buf` ends before a field, when the value version
    /// is not 0, or when a tagged field does not decode.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        get_value_version(&mut buf, 0, "unknown StreamsGroupMetadataValue version")?;
        let epoch = get_i32(&mut buf)?;
        let metadata_hash = get_i64(&mut buf)?;
        let mut validated_topology_epoch = NO_VALIDATED_TOPOLOGY_EPOCH;
        let mut last_assignment_configs = None;
        let mut description = DescriptionEpochs::default();
        read_tagged(&mut buf, |tag, payload| match tag {
            TAG_VALIDATED_TOPOLOGY_EPOCH => {
                validated_topology_epoch = fixed::get_i32(payload)?;
                Ok(true)
            }
            TAG_LAST_ASSIGNMENT_CONFIGS => {
                last_assignment_configs = get_last_assignment_configs(payload)?;
                Ok(true)
            }
            TAG_STORED_DESCRIPTION_TOPOLOGY_EPOCH => {
                description.stored = fixed::get_i32(payload)?;
                Ok(true)
            }
            TAG_FAILED_DESCRIPTION_TOPOLOGY_EPOCH => {
                description.failed = fixed::get_i32(payload)?;
                Ok(true)
            }
            _ => Ok(false),
        })?;
        Ok(Self {
            epoch,
            metadata_hash,
            validated_topology_epoch,
            last_assignment_configs,
            description,
        })
    }
}

/// Reads the nullable compact array of `LastAssignmentConfig` structs.
fn get_last_assignment_configs(
    buf: &mut &[u8],
) -> Result<Option<Vec<LastAssignmentConfig>>, krabka_protocol::ProtocolError> {
    use krabka_protocol::{
        primitives::{array, string_bytes},
        tagged_fields::read_tagged_fields,
    };
    let Some(count) = array::get_nullable_array_len(buf, true)? else {
        return Ok(None);
    };
    let mut configs = Vec::with_capacity(count.min(buf.len()));
    for _ in 0..count {
        let key = string_bytes::get_compact_string_owned(buf)?;
        let value = string_bytes::get_compact_string_owned(buf)?;
        read_tagged_fields(buf, |_, _| Ok(false))?;
        configs.push(LastAssignmentConfig { key, value });
    }
    Ok(Some(configs))
}

/// Key v20 value: the target-assignment epoch and the KIP-1263
/// `AssignmentTimestamp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::FlexibleEpochValue)]
#[epoch_value(record = "StreamsGroupTargetAssignmentMetadataValue")]
pub struct StreamsGroupTargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
    /// Kafka's `AssignmentTimestamp`: the wall-clock time in milliseconds at
    /// which the assignment calculation finished, or 0 when it is unknown.
    pub assignment_timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            streams::persistence::{
                KEY_STREAMS_GROUP_METADATA, KEY_STREAMS_TARGET_ASSIGNMENT_METADATA,
                StreamsGroupKey, encode_group_metadata_key, encode_target_assignment_metadata_key,
                parse_streams_key,
            },
            test_support::peek_version,
        },
        assert2::assert,
    };

    /// The bytes that follow the head of a `StreamsGroupMetadataValue`.
    /// A last-assignment configuration parameter, as text.
    /// A value and Kafka's bytes for it.
    /// Kafka 4.3.1's generated writer puts a tagged field only when it is not
    /// its default, in tag order. Each row is the whole value: version, epoch,
    /// hash, then the trailer.
    /// Kafka trunk's KIP-1331 tags 2 and 3 follow the 4.3.1 tags, each only
    /// when it is not -1. A value that holds neither, which is what the
    /// broker writes unless the topology description plugin is configured,
    /// is byte for byte the 4.3.1 record; a record with them decodes whole
    /// whatever the broker writes.
    #[test]
    fn group_metadata_round_trip() {
        let kb = encode_group_metadata_key("g1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_GROUP_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::GroupMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupMetadataValue {
            epoch: 7,
            metadata_hash: -9,
            validated_topology_epoch: 2,
            last_assignment_configs: Some(vec![LastAssignmentConfig {
                key: "num.standby.replicas".into(),
                value: "1".into(),
            }]),
            description: DescriptionEpochs {
                stored: 2,
                failed: -1,
            },
        };
        assert!(StreamsGroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    /// i16 version | i32 `AssignmentEpoch` | tagged-field trailer, which holds
    /// `AssignmentTimestamp` as tag 0 with an eight-byte payload unless it is
    /// the default 0.
    #[test]
    fn target_assignment_metadata_round_trip() {
        let kb = encode_target_assignment_metadata_key("g1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TARGET_ASSIGNMENT_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::TargetAssignmentMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupTargetAssignmentMetadataValue {
            assignment_epoch: 12,
            assignment_timestamp_ms: 7,
        };
        assert!(StreamsGroupTargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn epoch_records_reject_a_missing_tagged_trailer() {
        let g = StreamsGroupMetadataValue {
            epoch: 1,
            metadata_hash: 0,
            validated_topology_epoch: NO_VALIDATED_TOPOLOGY_EPOCH,
            last_assignment_configs: None,
            description: DescriptionEpochs::default(),
        }
        .encode();
        assert!(StreamsGroupMetadataValue::decode(&g[..g.len() - 1]).is_err());
    }
    use {
        crate::coordinator::unified::test_support::wire_bytes,
        rstest_bdd_macros::{given, scenarios, then, when},
    };
    #[derive(Debug, PartialEq)]
    enum StreamsEpochRecord {
        Group(StreamsGroupMetadataValue),
        Target(StreamsGroupTargetAssignmentMetadataValue),
    }
    #[derive(Default)]
    struct StreamsEpochCodec {
        value: Option<StreamsEpochRecord>,
        encoded: Option<Bytes>,
        decoded: Option<StreamsEpochRecord>,
        golden: Vec<u8>,
    }
    #[rstest::fixture]
    fn streams_epochs() -> StreamsEpochCodec {
        // Start this scenario with fresh inputs and observations.
        StreamsEpochCodec::default()
    }
    #[given(
        "Streams group epoch 7 has validated topology {validated} configs {configs} stored description {stored} and failed description {failed}"
    )]
    fn streams_epoch_fields(
        streams_epochs: &mut StreamsEpochCodec,
        validated: i32,
        configs: String,
        stored: i32,
        failed: i32,
    ) {
        let configs = match configs.as_str() {
            "null" => None,
            "empty" => Some(vec![]),
            _ => Some(
                configs
                    .split(',')
                    .map(|entry| {
                        let (key, value) = entry
                            .split_once('=')
                            .expect("assignment configuration key=value");
                        LastAssignmentConfig {
                            key: key.into(),
                            value: value.into(),
                        }
                    })
                    .collect(),
            ),
        };
        streams_epochs.value = Some(StreamsEpochRecord::Group(StreamsGroupMetadataValue {
            epoch: 7,
            metadata_hash: 0x0102_0304_0506_0708,
            validated_topology_epoch: validated,
            last_assignment_configs: configs,
            description: DescriptionEpochs { stored, failed },
        }));
    }
    #[given("Streams assignment epoch 12 completed at timestamp {timestamp}")]
    fn streams_target_epoch(streams_epochs: &mut StreamsEpochCodec, timestamp: i64) {
        streams_epochs.value = Some(StreamsEpochRecord::Target(
            StreamsGroupTargetAssignmentMetadataValue {
                assignment_epoch: 12,
                assignment_timestamp_ms: timestamp,
            },
        ));
    }
    #[given("Kafka represents that Streams epoch record with bytes {hex}")]
    fn streams_epoch_golden(streams_epochs: &mut StreamsEpochCodec, hex: String) {
        streams_epochs.golden = wire_bytes(&[&hex]);
    }
    #[when("the coordinator writes the Streams epoch record and reads Kafka bytes")]
    fn streams_epoch_codec(streams_epochs: &mut StreamsEpochCodec) {
        let (encoded, decoded) = match streams_epochs
            .value
            .as_ref()
            .expect("epoch record prepared")
        {
            StreamsEpochRecord::Group(value) => (
                value.encode(),
                StreamsEpochRecord::Group(
                    StreamsGroupMetadataValue::decode(&streams_epochs.golden)
                        .expect("Kafka group epoch"),
                ),
            ),
            StreamsEpochRecord::Target(value) => (
                value.encode(),
                StreamsEpochRecord::Target(
                    StreamsGroupTargetAssignmentMetadataValue::decode(&streams_epochs.golden)
                        .expect("Kafka assignment epoch"),
                ),
            ),
        };
        streams_epochs.encoded = Some(encoded);
        streams_epochs.decoded = Some(decoded);
    }
    #[then(
        "the Streams epoch bytes match Kafka and all optional tags preserve their original values"
    )]
    fn streams_epoch_layout(streams_epochs: &StreamsEpochCodec) {
        assert!(
            &streams_epochs.encoded.as_ref().expect("epoch written")[..]
                == &streams_epochs.golden[..]
        );
        assert!(streams_epochs.decoded == streams_epochs.value);
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/streams/persistence/epochs.feature",fixtures=[streams_epochs:StreamsEpochCodec]);
}
