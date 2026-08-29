//! Fixture builders that more than one of the produce handler's unit-test
//! modules needs, kept in one place so each of them builds the same image,
//! topic override, and record batch.

use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};
use krabka_metadata::{
    MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord,
};
use krabka_protocol::records::RecordBatch;
use uuid::Uuid;

use super::framing::{FramedPartition, FramedTopic, PartitionPayload};
use crate::config_keys::MIN_INSYNC_REPLICAS;

pub(super) fn image_with_topic(topic: &str, isr: &[u64]) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    img.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: topic.into(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: i16::try_from(isr.len().max(1)).unwrap(),
    }));
    img.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.into(),
        partition: 0,
        leader: krabka_audit::NodeId(*isr.first().unwrap_or(&1)),
        replicas: isr.iter().copied().map(krabka_audit::NodeId).collect(),
        isr: isr.iter().copied().map(krabka_audit::NodeId).collect(),
        leader_epoch: krabka_metadata::LeaderEpoch(0),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }));
    img
}

pub(super) fn set_min_isr(img: &mut MetadataImage, topic: &str, n: i32) {
    let mut o = BTreeMap::new();
    o.insert(MIN_INSYNC_REPLICAS.into(), n.to_string());
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.into(),
        overrides: o,
    }));
}

pub(super) fn set_qos_tier(img: &mut MetadataImage, topic: &str, tier: &str) {
    let mut o = BTreeMap::new();
    o.insert(crate::config_keys::QOS_TIER.into(), tier.into());
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.into(),
        overrides: o,
    }));
}

pub(super) fn framed_topic(name: &str, payload_lens: &[usize]) -> FramedTopic {
    FramedTopic {
        name: name.into(),
        topic_id: krabka_protocol::primitives::uuid::Uuid::ZERO,
        partition_data: payload_lens
            .iter()
            .enumerate()
            .map(|(idx, len)| FramedPartition {
                index: i32::try_from(idx).unwrap(),
                payload: PartitionPayload::Slice(Bytes::from(vec![0; *len])),
            })
            .collect(),
    }
}

pub(super) fn encode_batch(batch: &RecordBatch) -> Bytes {
    let mut buf = BytesMut::new();
    batch.encode(&mut buf).expect("encode record batch");
    buf.freeze()
}
