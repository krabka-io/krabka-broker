//! The two transaction-scoped fields of a producer row,
//! `current_txn_start_offset` and `coordinator_epoch`, tracked across an open
//! transaction, the `WriteTxnMarkers` that completes it, and the next one.

use assert2::{assert, check};
use krabka_protocol::owned::{
    describe_producers_request::{DescribeProducersRequest, TopicRequest},
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    write_txn_markers_request::{
        WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
    },
};

use crate::{
    producers_harness::{
        create_topic, init_transactional_producer, topic_id_for, transactional_batch,
    },
    support,
};

#[tokio::test]
async fn transactional_fields_follow_open_and_completed_transactions() {
    let p = support::start().await;
    create_topic(&p.client, "transactions", 1).await;
    let topic_id = topic_id_for(&p, "transactions").await;
    let (pid, epoch) = init_transactional_producer(&p, "describe-producers-tid").await;

    let produce_response = p
        .client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "transactions".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(transactional_batch(pid, epoch, 0, &["first"]).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("transactional Produce");
    assert!(produce_response.responses[0].partition_responses[0].error_code == 0);

    let describe = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "transactions".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers during first transaction");
    let producer_row = &describe.topics[0].partitions[0].active_producers[0];
    check!(producer_row.current_txn_start_offset == 0);
    check!(producer_row.coordinator_epoch == -1);

    let marker = p
        .client
        .send(WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: pid,
                producer_epoch: epoch,
                transaction_result: true,
                coordinator_epoch: 17,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: "transactions".into(),
                    partition_indexes: vec![0],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("WriteTxnMarkers");
    assert!(marker.markers[0].topics[0].partitions[0].error_code == 0);

    let describe = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "transactions".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers after marker");
    let producer_row = &describe.topics[0].partitions[0].active_producers[0];
    check!(producer_row.current_txn_start_offset == -1);
    check!(producer_row.coordinator_epoch == 17);

    let produce_response = p
        .client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "transactions".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(transactional_batch(pid, epoch, 1, &["second"]).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("second transactional Produce");
    assert!(produce_response.responses[0].partition_responses[0].error_code == 0);

    let describe = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "transactions".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers during second transaction");
    let producer_row = &describe.topics[0].partitions[0].active_producers[0];
    check!(producer_row.current_txn_start_offset == 2);
    check!(producer_row.coordinator_epoch == 17);

    p.broker.shutdown().await;
}
