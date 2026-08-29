//! Reading the broker's audit topic back out over the Kafka wire.
//!
//! Both helpers here fetch `AUDIT_TOPIC` partition 0 from offset zero and
//! decode the record batches, one into the `seq` header of every
//! non-checkpoint record and the other into the JSON body of every record.
//! They live beside the polling wrappers in the parent module rather than in
//! it, because between them they are the only code in `support` that speaks
//! `FetchRequest` directly.

use krabka_broker::coordinator::AUDIT_TOPIC;
use krabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};

/// Fetch the audit topic and return the `seq` header value (parsed as `u64`)
/// from each non-checkpoint record, in order.
pub async fn audit_record_seqs(client: &krabka_client_core::Client) -> Vec<u64> {
    let topic_id = super::topic_id_for(client, AUDIT_TOPIC).await;
    let fr = client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("FetchRequest for audit topic");

    let mut seqs = Vec::new();
    if let Some(part) = fr.responses.first().and_then(|r| r.partitions.first())
        && let Some(batches) = part.records.as_ref().and_then(|r| r.as_v2())
    {
        for batch in batches {
            for rec in &batch.records {
                // Skip checkpoint records — they have no `seq` header.
                let is_checkpoint = rec
                    .headers
                    .iter()
                    .any(|h| h.key == "event_class" && h.value.as_deref() == Some(b"checkpoint"));
                if is_checkpoint {
                    continue;
                }
                if let Some(seq_val) = rec
                    .headers
                    .iter()
                    .find(|h| h.key == "seq")
                    .and_then(|h| h.value.as_ref())
                    .and_then(|v| std::str::from_utf8(v).ok())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    seqs.push(seq_val);
                }
            }
        }
    }
    seqs
}

pub async fn consume_audit_records(client: &krabka_client_core::Client) -> Vec<serde_json::Value> {
    let topic_id = super::topic_id_for(client, AUDIT_TOPIC).await;
    let fr = client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("FetchRequest for audit topic");

    let mut records = Vec::new();
    if let Some(part) = fr.responses.first().and_then(|r| r.partitions.first())
        && let Some(batches) = part.records.as_ref().and_then(|r| r.as_v2())
    {
        for batch in batches {
            for rec in &batch.records {
                if let Some(value) = &rec.value
                    && let Ok(j) = serde_json::from_slice::<serde_json::Value>(value)
                {
                    records.push(j);
                }
            }
        }
    }
    records
}
