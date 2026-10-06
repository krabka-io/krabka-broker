//! The setup every `DescribeProducers` test shares: creating a topic, reading
//! back its id, claiming a producer id, and building the record batches the
//! produce calls send.
//!
//! Both batch builders live here because the transactional one is the
//! idempotent one with the transactional attribute bit set.

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        find_coordinator_request::FindCoordinatorRequest,
        init_producer_id_request::InitProducerIdRequest,
    },
    records::{Attributes, Record, RecordBatch},
};

use crate::support;
pub(crate) use crate::support::topic_id_for;

pub(crate) async fn create_topic(client: &Client, name: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "{name} create: {resp:?}");
}

pub(crate) async fn init_producer(p: &support::InProcess) -> (i64, i16) {
    let init = p
        .client
        .send(InitProducerIdRequest {
            // A null transactional id is what makes this an idempotent
            // producer; Kafka rejects an empty one with INVALID_REQUEST.
            transactional_id: None,
            ..Default::default()
        })
        .await
        .expect("InitProducerId");
    (init.producer_id, init.producer_epoch)
}

pub(crate) async fn init_transactional_producer(
    p: &support::InProcess,
    transactional_id: &str,
) -> (i64, i16) {
    let coordinator = p
        .client
        .send(FindCoordinatorRequest {
            key: transactional_id.into(),
            key_type: 1,
            coordinator_keys: vec![transactional_id.into()],
            ..Default::default()
        })
        .await
        .expect("transactional FindCoordinator");
    assert!(
        coordinator.error_code == 0
            || coordinator
                .coordinators
                .iter()
                .all(|entry| entry.error_code == 0),
        "transactional FindCoordinator: {coordinator:?}"
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let init = p
            .client
            .send(InitProducerIdRequest {
                transactional_id: Some(transactional_id.into()),
                transaction_timeout_ms: 60_000,
                ..Default::default()
            })
            .await
            .expect("transactional InitProducerId");
        if init.error_code == 0 {
            return (init.producer_id, init.producer_epoch);
        }
        assert!(
            matches!(init.error_code, 15 | 16) && tokio::time::Instant::now() < deadline,
            "transactional InitProducerId: {init:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub(crate) fn batch(pid: i64, epoch: i16, base_seq: i32, values: &[&str]) -> RecordBatch {
    let n = i32::try_from(values.len()).expect("values.len fits i32");
    let records = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            offset_delta: i32::try_from(i).expect("index fits i32"),
            value: Some(bytes::Bytes::from(v.to_string())),
            ..Default::default()
        })
        .collect();
    RecordBatch {
        producer_id: pid,
        producer_epoch: epoch,
        base_sequence: base_seq,
        last_offset_delta: n - 1,
        max_timestamp: i64::from(n),
        records,
        ..Default::default()
    }
}

pub(crate) fn transactional_batch(
    pid: i64,
    epoch: i16,
    base_seq: i32,
    values: &[&str],
) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        ..batch(pid, epoch, base_seq, values)
    }
}
