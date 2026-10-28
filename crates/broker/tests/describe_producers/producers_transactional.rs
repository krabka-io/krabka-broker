//! The two transaction-scoped fields of a producer row,
//! `current_txn_start_offset` and `coordinator_epoch`, tracked across an open
//! transaction, the `WriteTxnMarkers` that completes it, and the next one.

use {
    crate::{
        producers_harness::{
            ProducerSnapshot, batch, init_transactional_producer, snapshot, topic_id_for,
            transactional,
        },
        support::{
            produce::{SinglePartitionProduceSetup, batch_request},
            records::{ProducerSequence, ProducerValuesSetup},
            transactions,
        },
    },
    assert2::{assert, check},
    krabka_protocol::{
        owned::{
            produce_request::ProduceRequest, write_txn_markers_request::WriteTxnMarkersRequest,
        },
        primitives::uuid::Uuid,
    },
};

fn transaction_values_request(topic_id: Uuid, values: ProducerValuesSetup<'_>) -> ProduceRequest {
    ProduceRequest {
        transactional_id: Some("describe-producers-tid".into()),
        ..batch_request(
            transactional(
                batch()
                    .pid(values.pid.0)
                    .epoch(values.epoch.0)
                    .base_seq(values.base_seq.0)
                    .values(values.values)
                    .call(),
            ),
            SinglePartitionProduceSetup {
                topic: ("transactions").into(),
                topic_id,
                ..SinglePartitionProduceSetup::replicated()
            },
        )
    }
}

#[rstest_bdd_macros::given(
    "a transactional producer describe-producers-tid is initialized for transactions"
)]
async fn transaction_snapshot_identity(snapshot: &mut ProducerSnapshot) {
    let process = snapshot
        .process
        .as_ref()
        .expect("standalone broker started");
    snapshot.topic_id = Some(topic_id_for(&process.client, "transactions").await);
    snapshot.transactional =
        Some(init_transactional_producer(process, "describe-producers-tid").await);
}
#[rstest_bdd_macros::when("the transactional producer appends {value} at base sequence {sequence}")]
async fn transaction_snapshot_append(
    snapshot: &mut ProducerSnapshot,
    value: String,
    sequence: i32,
) {
    let producer = snapshot
        .transactional
        .expect("transactional producer initialized");
    snapshot.produced = Some(
        snapshot
            .client()
            .send(transaction_values_request(
                snapshot.topic_id.expect("topic id known"),
                ProducerValuesSetup {
                    pid: producer.id,
                    epoch: producer.epoch,
                    base_seq: ProducerSequence(sequence),
                    values: &[value.as_str()],
                },
            ))
            .await
            .expect("transactional Produce"),
    );
}
#[rstest_bdd_macros::then("the transactional append succeeds")]
fn transaction_snapshot_appended(snapshot: &ProducerSnapshot) {
    assert!(
        snapshot
            .produced
            .as_ref()
            .expect("transactional append attempted")
            .responses[0]
            .partition_responses[0]
            .error_code
            == 0
    );
}
#[rstest_bdd_macros::when(
    "a version 1 transaction marker completes the transaction at coordinator epoch 17"
)]
async fn transaction_snapshot_marker(snapshot: &mut ProducerSnapshot) {
    let producer = snapshot
        .transactional
        .expect("transactional producer initialized");
    let marker = snapshot
        .client()
        .send(WriteTxnMarkersRequest {
            markers: vec![
                transactions::transaction_marker()
                    .producer(producer)
                    .coordinator_epoch(17)
                    .transaction_version(1)
                    .topics(vec![transactions::marker_topic(
                        "transactions".into(),
                        vec![0],
                    )])
                    .call(),
            ],
            ..Default::default()
        })
        .await
        .expect("WriteTxnMarkers");
    snapshot.marker = Some(marker);
}
#[rstest_bdd_macros::then("the transaction marker succeeds")]
fn transaction_snapshot_marker_written(snapshot: &ProducerSnapshot) {
    assert!(
        snapshot.marker.as_ref().expect("marker attempted").markers[0].topics[0].partitions[0]
            .error_code
            == 0
    );
}
#[rstest_bdd_macros::then(
    "the active producer reports transaction start {start} and coordinator epoch {coordinator}"
)]
fn transaction_snapshot_fields(snapshot: &ProducerSnapshot, start: i64, coordinator: i32) {
    let describe = snapshot.response.as_ref().expect("producers described");
    let row = &describe.topics[0].partitions[0].active_producers[0];
    check!(row.current_txn_start_offset == start);
    check!(row.coordinator_epoch == coordinator);
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_producers/producers_transactional.feature",
    name = "transactional fields follow open and completed transactions"
)]
#[tokio::test]
async fn transactional_fields_follow_open_and_completed_transactions(snapshot: ProducerSnapshot) {
    snapshot.shutdown().await;
}
