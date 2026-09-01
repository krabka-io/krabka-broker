//! [`KafkaMetadataEventLog`]: the production [`MetadataEventLog`]
//! adapter that persists events in the internal `__remote_log_metadata`
//! Kafka topic.
//!
//! Writes flow through a [`krabka_client_producer::Producer`] with
//! explicit per-record partition pinning. Reads come back through one
//! cancellable manual-`Fetch` task per assigned partition. Each task
//! drives its own dedicated [`krabka_client_core::Connection`] and emits
//! [`MetadataEventRecord`](crate::log::MetadataEventRecord)s into a shared
//! mpsc. There is **no consumer group and no broker-side offset commit**.
//! The RLMM owns the read position. The manager assigns all partitions from
//! offset 0 today, then resumes from snapshot offsets and restricts the
//! consumed set.
//!
//! A dedicated connection per partition is necessary because the broker is
//! serial per-connection. A long-`max_wait_ms` fetch would
//! head-of-line-block any other RPC that shares the socket.
//!
//! Topic provisioning runs once at [`KafkaMetadataEventLog::start`] through
//! the [`krabka_client_admin::AdminClient`]. It reuses an existing topic, and
//! the topic's actual partition count then overrides the configured
//! `num_partitions`. It creates an absent topic with the configured cleanup
//! policy and `retention.ms=-1`. The same admin round-trip surfaces the topic's
//! `Uuid`, which the manual `Fetch` path needs, because Fetch v≥13 carries
//! `topic_id` and not the name.
//!
//! One `ListOffsets(timestamp=-1)` over the raw
//! [`krabka_client_core::Client`] pulls the high-water marks, rather than a
//! consumer. [`MetadataEventLog::high_water_marks`] therefore does not need
//! any fetch task to have made progress.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{StreamExt, unfold};
use krabka_client_core::{Client, ClientFrameMax, ConnectionDispatchQueueCapacity};
use krabka_client_producer::{Acks, Producer, ProducerRecord};
use krabka_protocol::{
    owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    primitives::uuid::Uuid as WireUuid,
};
use krabka_units::prelude::{ByteSize, Time};
use tracing::{instrument, warn};

mod config;
mod consumer;
mod topic;

pub use self::config::{
    DEFAULT_METADATA_EVENT_QUEUE_CAPACITY, DEFAULT_METADATA_FETCH_MAX_BYTES,
    DEFAULT_METADATA_FETCH_MAX_WAIT, DEFAULT_METADATA_FETCH_RETRY_BACKOFF,
    DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION,
    KafkaMetadataLogConfig, METADATA_TOPIC, MetadataEventQueueCapacity,
};
use self::{
    consumer::{ConsumerState, KafkaAssignmentHandle, metadata_event_channel},
    topic::ensure_topic,
};
use crate::{
    error::MetadataLogError,
    log::{AssignmentHandle, MetadataEventLog, MetadataEventStream, PartitionStart},
};

/// Production [`MetadataEventLog`] backed by an internal Kafka topic.
pub struct KafkaMetadataEventLog {
    producer: Producer,
    client: Client,
    topic: String,
    topic_id: WireUuid,
    partition_count: i32,
    bootstrap: String,
    client_id: String,
    security: Option<krabka_client_core::security::ClientSecurity>,
    fetch_max_wait: Time,
    fetch_max_bytes: ByteSize,
    fetch_retry_backoff: Time,
    event_queue_capacity: MetadataEventQueueCapacity,
    dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    frame_max: ClientFrameMax,
    subscriptions: tokio::sync::Mutex<Vec<Arc<ConsumerState>>>,
}

impl KafkaMetadataEventLog {
    /// Provision the topic if it is missing, connect the producer and the
    /// raw client, learn the topic id, and return the log.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError::Other`] on admin / producer /
    /// client construction failures.
    #[instrument(skip_all, fields(topic = %cfg.topic, bootstrap = %cfg.bootstrap), err)]
    pub async fn start(cfg: KafkaMetadataLogConfig) -> Result<Arc<Self>, MetadataLogError> {
        cfg.validate()
            .map_err(|error| MetadataLogError::Other(format!("invalid config: {error}")))?;

        // 1. Provision the topic, learn its partition count and id. The
        //    manual Fetch path needs the topic Uuid (Fetch v≥13 carries
        //    topic_id, not the name).
        let (partition_count, topic_id) = ensure_topic(&cfg).await?;

        // 2. Producer with acks=All and idempotence on. Read-your-writes
        //    depends on the broker durably acking the publish.
        let producer = Producer::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-producer", cfg.client_id))
            .dispatch_queue_capacity(cfg.dispatch_queue_capacity.get())
            .frame_max(cfg.frame_max.size())
            .acks(Acks::All)
            .enable_idempotence(true)
            .maybe_security(cfg.security.clone())
            .build()
            .await
            .map_err(|e| MetadataLogError::Other(format!("producer build failed: {e}")))?;

        // 3. Raw client for ListOffsets and any future low-level queries.
        let client = Client::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-client", cfg.client_id))
            .dispatch_queue_capacity(cfg.dispatch_queue_capacity.get())
            .frame_max(cfg.frame_max.size())
            .maybe_security(cfg.security.clone())
            .build()
            .await
            .map_err(|e| MetadataLogError::Other(format!("client build failed: {e}")))?;

        Ok(Arc::new(Self {
            producer,
            client,
            topic: cfg.topic,
            topic_id,
            partition_count,
            bootstrap: cfg.bootstrap,
            client_id: cfg.client_id,
            security: cfg.security,
            fetch_max_wait: cfg.fetch_max_wait,
            fetch_max_bytes: cfg.fetch_max_bytes,
            fetch_retry_backoff: cfg.fetch_retry_backoff,
            event_queue_capacity: cfg.event_queue_capacity,
            dispatch_queue_capacity: cfg.dispatch_queue_capacity,
            frame_max: cfg.frame_max,
            subscriptions: tokio::sync::Mutex::new(Vec::new()),
        }))
    }

    /// Cancel the fetch tasks of every active subscription. A drop also
    /// cancels them.
    pub async fn shutdown(&self) {
        let mut subs = self.subscriptions.lock().await;
        for state in subs.drain(..) {
            state.cancel_all();
        }
    }
}

impl Drop for KafkaMetadataEventLog {
    fn drop(&mut self) {
        if let Ok(mut subs) = self.subscriptions.try_lock() {
            for state in subs.drain(..) {
                state.cancel_all();
            }
        }
    }
}

#[async_trait]
impl MetadataEventLog for KafkaMetadataEventLog {
    fn partition_count(&self) -> i32 {
        self.partition_count
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(topic = %self.topic, partition, len = event.len()),
        err
    )]
    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
        self.publish_record(partition, None, Some(event)).await
    }

    async fn publish_keyed(
        &self,
        partition: i32,
        key: Bytes,
        event: Option<Bytes>,
    ) -> Result<i64, MetadataLogError> {
        self.publish_record(partition, Some(key), event).await
    }

    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        let (tx, rx) = metadata_event_channel(self.event_queue_capacity);
        let state = Arc::new(ConsumerState {
            bootstrap: self.bootstrap.clone(),
            client_id: format!("{}-consumer", self.client_id),
            security: self.security.clone(),
            topic: self.topic.clone(),
            topic_id: self.topic_id,
            tx,
            fetch_max_wait: self.fetch_max_wait,
            fetch_max_bytes: self.fetch_max_bytes,
            fetch_retry_backoff: self.fetch_retry_backoff,
            dispatch_queue_capacity: self.dispatch_queue_capacity,
            frame_max: self.frame_max,
            tasks: StdMutex::new(HashMap::new()),
        });
        for ps in assignment {
            state.spawn_partition(ps);
        }
        if let Ok(mut subs) = self.subscriptions.try_lock() {
            subs.push(Arc::clone(&state));
        } else {
            warn!("KafkaMetadataEventLog: could not track subscription state");
        }
        let stream = unfold(rx, |mut rx| async move { rx.recv().await.map(|r| (r, rx)) }).boxed();
        let handle: Arc<dyn AssignmentHandle> = Arc::new(KafkaAssignmentHandle { state });
        (stream, handle)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(topic = %self.topic, partition_count = self.partition_count),
        err
    )]
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        let partitions = (0..self.partition_count)
            .map(|p| ListOffsetsPartition {
                partition_index: p,
                current_leader_epoch: -1,
                timestamp: -1, // LATEST
                ..Default::default()
            })
            .collect();
        let req = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                name: self.topic.clone(),
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = self
            .client
            .send(req)
            .await
            .map_err(|e| MetadataLogError::Other(format!("ListOffsets failed: {e}")))?;
        let mut hwms = vec![0i64; usize_count(self.partition_count)?];
        for topic in &resp.topics {
            if topic.name != self.topic {
                continue;
            }
            for p in &topic.partitions {
                if p.error_code != 0 {
                    return Err(MetadataLogError::Other(format!(
                        "ListOffsets partition {} error {}",
                        p.partition_index, p.error_code
                    )));
                }
                if let Ok(idx) = usize::try_from(p.partition_index)
                    && idx < hwms.len()
                {
                    hwms[idx] = p.offset;
                }
            }
        }
        Ok(hwms)
    }
}

impl KafkaMetadataEventLog {
    async fn publish_record(
        &self,
        partition: i32,
        key: Option<Bytes>,
        event: Option<Bytes>,
    ) -> Result<i64, MetadataLogError> {
        let record = producer_record(&self.topic, self.partition_count, partition, key, event)?;
        let ack = self.producer.send(record).await;
        let meta = ack
            .await
            .map_err(|_| MetadataLogError::Publish("producer dropped before ack".into()))?
            .map_err(|e| MetadataLogError::Publish(e.to_string()))?;
        Ok(meta.offset)
    }
}

fn producer_record(
    topic: &str,
    partition_count: i32,
    partition: i32,
    key: Option<Bytes>,
    event: Option<Bytes>,
) -> Result<ProducerRecord, MetadataLogError> {
    if partition < 0 || partition >= partition_count {
        return Err(MetadataLogError::PartitionOutOfRange {
            partition,
            count: partition_count,
        });
    }
    Ok(ProducerRecord {
        topic: topic.to_owned(),
        partition: Some(partition),
        key,
        value: event,
        ..Default::default()
    })
}

fn usize_count(n: i32) -> Result<usize, MetadataLogError> {
    usize::try_from(n).map_err(|_| MetadataLogError::Other(format!("partition_count {n} negative")))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::convert::TimeExt as _;

    use super::*;

    #[tokio::test]
    async fn start_rejects_invalid_policy_before_connecting() {
        let cfg = KafkaMetadataLogConfig {
            topic_create_timeout: Time::ZERO,
            ..KafkaMetadataLogConfig::new("not a socket address")
        };

        let Err(error) = KafkaMetadataEventLog::start(cfg).await else {
            panic!("invalid policy must fail before network I/O");
        };
        assert!(error.to_string().contains("topic_create_timeout"));
    }

    #[test]
    fn keyed_tombstone_record_is_partitioned_and_preserves_the_null_value() {
        let record = producer_record(
            "__diskless_wal_index",
            3,
            2,
            Some(Bytes::from_static(b"range")),
            None,
        )
        .unwrap();

        assert!(record.topic == "__diskless_wal_index");
        assert!(record.partition == Some(2));
        assert!(record.key.as_deref() == Some(b"range".as_slice()));
        assert!(record.value.is_none());
    }

    #[test]
    fn keyed_record_rejects_out_of_range_partitions() {
        for partition in [-1, 3] {
            let error = producer_record("index", 3, partition, None, None).unwrap_err();
            assert!(matches!(
                error,
                MetadataLogError::PartitionOutOfRange {
                    partition: got,
                    count: 3
                } if got == partition
            ));
        }
    }
}
