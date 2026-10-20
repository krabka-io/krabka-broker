//! Kafka-native audit sink.
//!
//! The sink appends OCSF records to this broker's partition of the internal
//! audit topic. This is the broker-affinity write path.

use {
    crate::{
        metrics::BrokerMetrics, partition::ProduceBatchError,
        partition_registry::PartitionRegistry, time_util,
    },
    async_trait::async_trait,
    bytes::Bytes,
    krabka_audit::{AuditError, AuditRecord, AuditSink},
    krabka_ids::PartitionIndex,
    krabka_protocol::records::{Record, RecordBatch, RecordHeader},
    std::sync::Arc,
};

/// Writes audit records to a single partition of the audit topic that this
/// broker leads.
///
/// Audit sink construction: the sink resolves the partition index once at construction.
#[derive(derive_more::Debug)]
pub struct KafkaTopicAuditSink {
    #[debug(skip)]
    partitions: Arc<PartitionRegistry>,
    topic: String,
    partition_index: PartitionIndex,
    #[debug(skip)]
    node_id: krabka_raft::NodeId,
    #[debug(skip)]
    metrics: BrokerMetrics,
}

impl KafkaTopicAuditSink {
    #[must_use]
    pub(crate) fn new(
        partitions: Arc<PartitionRegistry>,
        topic: String,
        partition_index: PartitionIndex,
        node_id: krabka_raft::NodeId,
        metrics: BrokerMetrics,
    ) -> Self {
        Self {
            partitions,
            topic,
            partition_index,
            node_id,
            metrics,
        }
    }
}

#[async_trait]
impl AuditSink for KafkaTopicAuditSink {
    async fn write(&self, record: AuditRecord, durable: bool) -> Result<(), AuditError> {
        let Some(partition) = self.partitions.get(&self.topic, self.partition_index) else {
            self.metrics.audit_write_failures.inc();
            return Err(AuditError::Sink(format!(
                "audit partition {}-{} not local",
                self.topic, self.partition_index
            )));
        };
        let leadership = partition.lock_produce_transition().await;
        if leadership.leader_node_id != self.node_id {
            self.metrics.audit_write_failures.inc();
            return Err(AuditError::Sink(format!(
                "audit partition {}-{} is led by node {}, not this node {}",
                self.topic, self.partition_index, leadership.leader_node_id.0, self.node_id.0
            )));
        }

        let batch = audit_batch(record, time_util::now_ms());

        let result = if durable {
            partition.produce_batch_durable_outcome(batch).await
        } else {
            partition.produce_batch_outcome(batch).await
        };
        if let Err(error) = result {
            self.metrics.audit_write_failures.inc();
            return Err(match error {
                ProduceBatchError::Rejected(error) => AuditError::Sink(error.to_string()),
                ProduceBatchError::Indeterminate(error) => AuditError::Indeterminate(error),
            });
        }
        self.metrics.audit_events.inc();
        Ok(())
    }
}

/// The single-record batch that carries `record`, stamped with `now_ms`.
///
/// The audit topic keeps `CreateTime`, so the batch timestamps are the ones the
/// log stores. Time retention and time-based segment rolls read them. A batch
/// left at timestamp 0 looks older than any `retention.ms`, and the next
/// retention pass deletes its segment.
fn audit_batch(record: AuditRecord, now_ms: i64) -> RecordBatch {
    let headers = record
        .headers
        .into_iter()
        .map(|(k, v)| RecordHeader {
            key: k,
            value: Some(Bytes::from(v)),
        })
        .collect();
    // `offset_delta`/`key` are left at their `Record::default()` values (0 /
    // None) — a single audit record with no key. Spelling them out would only
    // create equivalent "delete field" mutants.
    RecordBatch {
        base_timestamp: now_ms,
        max_timestamp: now_ms,
        last_offset_delta: 0,
        records: vec![Record {
            value: Some(Bytes::from(record.value)),
            headers,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{partition::Partition, test_support::open_partition as fixture_partition},
        assert2::assert,
        krabka_log::Offset,
        krabka_units::mebibytes,
        std::sync::Arc,
    };

    /// The `__audit` partition and the registry that holds it.
    struct AuditPartition {
        partition: Arc<Partition>,
        partitions: Arc<PartitionRegistry>,
    }

    fn audit_partition(path: &std::path::Path) -> AuditPartition {
        let partitions = Arc::new(PartitionRegistry::new());
        let partition = fixture_partition().log_dir(path).topic("__audit").call();
        partitions.insert("__audit".into(), PartitionIndex(0), Arc::clone(&partition));
        AuditPartition {
            partition,
            partitions,
        }
    }

    fn audit_sink(partitions: Arc<PartitionRegistry>) -> KafkaTopicAuditSink {
        KafkaTopicAuditSink::new(
            partitions,
            "__audit".to_string(),
            PartitionIndex(0),
            krabka_raft::NodeId(0),
            BrokerMetrics::new(),
        )
    }

    #[tokio::test]
    async fn write_appends_record_value_and_headers_to_local_partition() {
        let dir = tempfile::tempdir().expect("tempdir");
        let AuditPartition {
            partition,
            partitions,
        } = audit_partition(dir.path());
        let sink = audit_sink(partitions);

        sink.write(
            AuditRecord {
                class: krabka_audit::AuditEventClass::ApiActivity,
                value: b"{\"ok\":true}".to_vec(),
                headers: vec![("event_class".to_string(), b"admin".to_vec())],
            },
            true,
        )
        .await
        .expect("write audit record");

        let out = partition
            .read_log(Offset(0), mebibytes(1))
            .expect("read audit partition");
        let records: Vec<_> = out.batches.iter().flat_map(|b| &b.records).collect();

        assert!(
            (
                records.len(),
                records[0].value.as_deref(),
                records[0].headers.len(),
                records[0].headers[0].key.as_str(),
                records[0].headers[0].value.as_deref(),
            ) == (
                1,
                Some(&b"{\"ok\":true}"[..]),
                1,
                "event_class",
                Some(&b"admin"[..]),
            )
        );
    }

    #[test]
    fn audit_batch_carries_the_write_time() {
        let batch = audit_batch(
            AuditRecord {
                class: krabka_audit::AuditEventClass::ApiActivity,
                value: b"v".to_vec(),
                headers: Vec::new(),
            },
            1_724_500_000_000,
        );

        assert!(
            (
                batch.base_timestamp,
                batch.max_timestamp,
                batch.records.len()
            ) == (1_724_500_000_000, 1_724_500_000_000, 1)
        );
    }

    /// A stored audit batch carries the time it was written. At timestamp 0,
    /// time retention deleted every closed audit segment at its next pass.
    #[tokio::test]
    async fn write_stores_the_write_time_as_the_batch_timestamp() {
        let dir = tempfile::tempdir().expect("tempdir");
        let AuditPartition {
            partition,
            partitions,
        } = audit_partition(dir.path());
        let sink = audit_sink(partitions);

        let before = time_util::now_ms();
        sink.write(
            AuditRecord {
                class: krabka_audit::AuditEventClass::ApiActivity,
                value: b"{}".to_vec(),
                headers: Vec::new(),
            },
            true,
        )
        .await
        .expect("write audit record");
        let after = time_util::now_ms();

        let out = partition
            .read_log(Offset(0), mebibytes(1))
            .expect("read audit partition");
        let stamped = out.batches[0].max_timestamp;
        assert!((before..=after).contains(&stamped), "stamped {stamped}");
    }

    #[tokio::test]
    async fn write_refuses_a_partition_after_leadership_moves_away() {
        let dir = tempfile::tempdir().expect("tempdir");
        let AuditPartition {
            partition,
            partitions,
        } = audit_partition(dir.path());
        partition.install_leader_change(1, 1).await;
        let sink = audit_sink(partitions);

        let error = sink
            .write(
                AuditRecord {
                    class: krabka_audit::AuditEventClass::ApiActivity,
                    value: b"{}".to_vec(),
                    headers: Vec::new(),
                },
                true,
            )
            .await
            .expect_err("former leader must reject audit write");

        assert!(error.to_string().contains("led by node 1"));
        assert!(partition.log_end_offset() == Offset(0));
    }
}
