//! Assembly of the wire response: the resolved reads are grouped back into
//! per-topic entries, down-converted for a v0-v3 fetcher, and metered.

use krabka_protocol::{
    owned::fetch_response::{FetchableTopicResponse, PartitionData},
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};

use super::plan::PendingRead;
use crate::broker::Broker;

/// Group the resolved `PendingRead`s back into per-topic response entries, and
/// keep the order in which the topics first appeared in the request.
///
/// The function also returns the per-topic `cpu_micros` accumulators. They
/// line up by position with the returned `Vec`, so `cpu_micros[ti][pi]`
/// matches `responses[ti].partitions[pi]`. The caller can then attribute CPU
/// without a re-key by topic name.
pub(super) type GroupedResponses = (Vec<FetchableTopicResponse>, Vec<Vec<u64>>);

pub(super) fn group_into_topic_responses(pending: Vec<PendingRead>) -> GroupedResponses {
    let mut topic_order: Vec<String> = Vec::new();
    // Value: (topic_id, partitions, cpu_micros) — the trailing Vec mirrors
    // `partitions` positionally.
    let mut by_topic: std::collections::HashMap<String, (WireUuid, Vec<PartitionData>, Vec<u64>)> =
        std::collections::HashMap::new();
    for p in pending {
        let entry = by_topic
            .entry(p.topic_name.clone())
            .or_insert_with(|| (p.topic_id, Vec::new(), Vec::new()));
        entry.1.push(p.out);
        entry.2.push(p.cpu_micros);
        if !topic_order.iter().any(|t| t == &p.topic_name) {
            topic_order.push(p.topic_name);
        }
    }
    let mut responses = Vec::with_capacity(topic_order.len());
    let mut cpu_micros = Vec::with_capacity(topic_order.len());
    for name in topic_order {
        let (topic_id, parts, micros) = by_topic.remove(&name).expect("topic order populated");
        responses.push(FetchableTopicResponse {
            topic: name,
            topic_id,
            partitions: parts,
            ..Default::default()
        });
        cpu_micros.push(micros);
    }
    (responses, cpu_micros)
}

pub(super) fn downconvert_legacy_responses(
    broker: &Broker,
    version: i16,
    responses: &mut [FetchableTopicResponse],
) {
    if version >= 4 {
        return;
    }
    for topic in responses {
        for partition in &mut topic.partitions {
            let Some(payload) = partition.records.take() else {
                continue;
            };
            match crate::handlers::fetch_downconvert::down_convert_payload_for_fetch(
                &payload, version,
            ) {
                Ok(Some(converted)) => {
                    if converted.payload_len() > 0 {
                        partition.records = Some(converted);
                    }
                    if !topic.topic.is_empty() {
                        broker.metrics.record_fetch_message_conversion(&topic.topic);
                    }
                }
                Ok(None) => {}
                Err(error_code) => partition.error_code = error_code,
            }
        }
    }
}

pub(super) fn record_fetch_metrics(
    broker: &Broker,
    responses: &[FetchableTopicResponse],
    cpu_micros_by_index: &[Vec<u64>],
    is_follower_fetch: bool,
) {
    for (topic_index, topic) in responses.iter().enumerate() {
        if topic.topic.is_empty() {
            continue;
        }
        let mut topic_bytes = 0;
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            let bytes = partition
                .records
                .as_ref()
                .map_or(0, RecordsPayload::payload_len) as u64;
            broker
                .metrics
                .record_partition_fetch(&topic.topic, partition.partition_index, bytes);
            if partition.error_code != 0 {
                broker.metrics.record_failed_fetch(&topic.topic);
            }
            if is_follower_fetch {
                broker.metrics.record_replication_out(
                    &topic.topic,
                    partition.partition_index,
                    bytes,
                );
            }
            if let Some(micros) = cpu_micros_by_index
                .get(topic_index)
                .and_then(|partitions| partitions.get(partition_index))
            {
                broker.metrics.record_partition_cpu_micros(
                    &topic.topic,
                    partition.partition_index,
                    *micros,
                );
            }
            topic_bytes += bytes;
        }
        broker.metrics.record_fetch(&topic.topic, topic_bytes);
    }
}
